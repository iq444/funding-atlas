"""Identity: client order ids and event sequence numbers.

Both exist to answer a question asked *after* something went wrong, so both
have to survive the thing that went wrong.

`cloid` is the idempotency key for an order. A send that times out tells us
nothing about whether the venue accepted it — only that the response was lost.
The retry is therefore sent with the same cloid, and the venue collapses the
duplicate. Without that, a flaky connection is how a hedge becomes a doubled
position.

`seq` is the per-account ordering of the audit trail. Timestamps are not enough:
two events written in the same microsecond, or written by a clock that was
stepped by NTP, cannot be ordered by time alone. A sequence number can.
"""

from __future__ import annotations

import hashlib
import re
import threading
from typing import Final

#: Hyperliquid cloids are 128-bit, hex, `0x`-prefixed, lower case.
CLOID_RE: Final = re.compile(r"^0x[0-9a-f]{32}$")

_CLOID_BYTES: Final = 16
_CLOID_PERSON: Final = b"fa-cloid"

#: `seq` starts at 1 so that 0 can mean "not yet assigned" — the placeholder an
#: `Event` carries between construction and `EventLog.append`.
FIRST_SEQ: Final = 1


def _frame(*parts: str) -> bytes:
    """Length-prefix each field before hashing.

    Joining with a separator only moves the ambiguity into whichever byte was
    chosen as the separator: with `:`, the tags `a:b` + `c` and `a` + `b:c`
    collide, and two different orders sharing one cloid is the exact failure
    this module exists to prevent.
    """
    out = bytearray()
    for p in parts:
        raw = p.encode("utf-8")
        out += len(raw).to_bytes(4, "big")
        out += raw
    return bytes(out)


def cloid(account_id: str, tag: str, order_key: str) -> str:
    """Derive a client order id from the order's logical identity.

    Derived, not allocated. The alternative — allocate a random id once, persist
    it, carry it through the retry loop — is stable only as long as the carrier
    is: a process that dies between "id written to disk" and "order sent" leaves
    an id with no order, and a process that dies the other way round leaves an
    order with an id nobody remembers. Recovery then has to reason about which
    happened.

    A pure function of (account, strategy tag, logical order key) has no such
    state. Any process, at any time, asked for the same logical order, produces
    the same cloid; a crashed engine recomputes it from the intent it is
    replaying and asks the venue what became of it.

    The cost of that choice is that `order_key` must be genuinely unique per
    intended order — a caller that reuses `("acct", "carry", "BTC-leg1")` for
    tomorrow's rebalance gets yesterday's cloid and the venue correctly refuses
    the order as a duplicate. So the key carries whatever makes the order
    distinct: a cycle id, a bar timestamp, a rebalance number.
    """
    digest = hashlib.blake2b(
        _frame(account_id, tag, order_key),
        digest_size=_CLOID_BYTES,
        person=_CLOID_PERSON,
    ).digest()
    return "0x" + digest.hex()


def is_cloid(value: str) -> bool:
    return CLOID_RE.fullmatch(value) is not None


class SequenceAllocator:
    """Per-account monotonic sequence numbers.

    Deliberately synchronous and mutex-guarded rather than `async`. An `await`
    between taking a number and writing the line would be a scheduling point:
    another producer could take the next number and reach the file first, and
    the log would be out of seq order on disk. A plain lock has no such window,
    and it is also what makes the allocator usable from a worker thread.

    The lock is held for one dict lookup and one addition, so contention is not
    a consideration at decision-plane rates.
    """

    __slots__ = ("_lock", "_next")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next: dict[str, int] = {}

    def next(self, account_id: str) -> int:
        with self._lock:
            seq = self._next.get(account_id, FIRST_SEQ)
            self._next[account_id] = seq + 1
            return seq

    def peek(self, account_id: str) -> int:
        """The number `next()` would return. For assertions and for recovery."""
        with self._lock:
            return self._next.get(account_id, FIRST_SEQ)

    def resume_after(self, account_id: str, last_seq: int) -> None:
        """Continue after a sequence already on disk, following a restart.

        Takes the maximum rather than assigning, so a late recovery scan can
        never hand back a number that has already been issued in this process.
        Reusing a seq would make two different events indistinguishable in the
        audit trail, which is worse than skipping a few.
        """
        with self._lock:
            self._next[account_id] = max(self._next.get(account_id, FIRST_SEQ), last_seq + 1)

    def known_accounts(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._next)
