"""The append-only event log.

This is the decision plane's storage: a handful of records per minute, every one
of which has to still be there, and still be unedited, when someone asks months
later why the bot did what it did. Throughput is irrelevant here; durability and
immutability are the entire point. Market data goes the other way, straight to
Parquet with no per-record validation (see `recorder.writers`).

Three rules, enforced structurally rather than by convention:

* **Nothing is ever edited.** `append` does not write into the caller's event —
  it returns a sealed copy carrying the assigned `seq`. On disk, a correction is
  a later event, never a rewritten line.
* **Files are only ever appended to or created.** Rotation happens on the UTC
  date boundary and nowhere else, so a closed partition is finished forever and
  can be offloaded to object storage without coordination.
* **Order is `seq`, not time.** Clocks step; sequence numbers do not.

Disk is the source of truth. The Postgres sink (`EventSink`) is a projection
built for querying, and is wired in after the line is durable so that a database
being down cannot cost us an audit record.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, Self, runtime_checkable

from funding_atlas.core.clock import Clock
from funding_atlas.core.events import DataGap, Event, EventType
from funding_atlas.core.ids import FIRST_SEQ, SequenceAllocator

_NS_PER_S = 1_000_000_000
_NS_PER_DAY = 86_400 * _NS_PER_S

#: `account_id` becomes a directory name, so it is restricted rather than
#: escaped. An id containing `..` or `/` is a path traversal, and an id that
#: differs from another only by case is two partitions on Linux and one on
#: macOS — both are better rejected at the boundary than normalised silently.
_ACCOUNT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


class EventLogError(Exception):
    pass


class CorruptEventLog(EventLogError):
    """A partition contains a line that is not a valid event.

    Raised rather than skipped: a bad line in the middle of a file means the
    file is not what we think it is, and a replay that quietly drops it would
    produce a plausible, wrong answer. An unparsable *final* line is different —
    that is a torn write from a crash, and is tolerated.
    """


@runtime_checkable
class EventSink(Protocol):
    """Secondary destination for events — Postgres, in practice.

    Defined here, implemented in `store/pg.py`. The contract:

    * `emit` is called only after the JSONL line is durable on disk, and
      receives the exact line that was written. A sink that stores those bytes
      cannot drift from the file through a serialisation change.
    * `emit` must be idempotent on `(account_id, seq)`. Replay after a restart
      will re-offer events the sink may already hold, and the pair is the
      natural primary key.
    * Raising is allowed. The log records the failure and keeps going; it will
      not fail a write to disk because a projection is unavailable.
    """

    async def emit(self, event: Event, line: str) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class FsyncPolicy:
    """When to force the OS to put the tail of the log on the platter.

    The default is one fsync per event, which is the conservative end on
    purpose. The decision plane writes a few records per minute, so a few
    milliseconds of fsync per record is not a cost anyone can measure — while
    the thing being bought is the only copy of the record of an order we have
    already sent to a venue. A lost tail is the specific failure that leaves
    real money in a position nothing in our own history explains.

    `relaxed()` exists for bulk backfill, where the data is reproducible and
    the records are not evidence. Note that every append flushes the process
    buffer regardless of policy: relaxing fsync widens the window for a *host*
    crash (power loss, kernel panic), never for our own process dying.
    """

    every_n_events: int = 1
    every_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.every_n_events < 1:
            raise ValueError("every_n_events must be >= 1")
        if self.every_seconds < 0:
            raise ValueError("every_seconds must be >= 0")

    @classmethod
    def relaxed(cls, every_n_events: int = 256, every_seconds: float = 5.0) -> FsyncPolicy:
        return cls(every_n_events=every_n_events, every_seconds=every_seconds)


@dataclass(slots=True)
class _Partition:
    """One open file: a single account on a single UTC date."""

    day: date
    path: Path
    handle: Any
    unsynced: int = 0
    last_fsync_ns: int = 0


@dataclass(slots=True)
class SinkFailure:
    sink: str
    account_id: str
    seq: int
    error: str


def _json_default(value: object) -> str:
    # Decimal becomes a string, never a float: a price or size that round-trips
    # through a float is no longer the number we sent to the venue, and the log
    # is what reconciliation and tax reporting are derived from.
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not serialisable into the event log")


def encode(event: Event) -> str:
    """Serialise deterministically.

    `sort_keys` and the tight separators mean the same event always produces the
    same bytes, on any interpreter, in any dict insertion order. That is what
    makes a line hashable for tamper detection later, and what keeps a diff
    between the file and the Postgres projection meaningful.
    """
    return json.dumps(
        event.model_dump(),
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def partition_dir(root: Path, account_id: str, day: date) -> Path:
    return root / "events" / f"account={account_id}" / f"date={day.isoformat()}"


def utc_day(ts_ns: int) -> date:
    # Integer division, not ns/1e9: a float seconds value loses nanosecond
    # precision and can land an event in the wrong partition at midnight.
    return datetime.fromtimestamp(ts_ns // _NS_PER_S, UTC).date()


class EventLog:
    """Append-only JSONL event log, safe for concurrent producers.

    One lock guards sequence allocation, the write and the fsync together.
    Per-account locks would allow more parallelism, but at a few records per
    minute there is nothing to win, and a single lock makes "the file is in seq
    order" true by construction instead of true by argument.
    """

    def __init__(
        self,
        root: Path | str,
        clock: Clock,
        *,
        fsync: FsyncPolicy | None = None,
        sinks: Sequence[EventSink] = (),
    ) -> None:
        self._root = Path(root)
        self._clock = clock
        self._fsync = fsync or FsyncPolicy()
        self._sinks = tuple(sinks)
        self._seq = SequenceAllocator()
        self._lock = asyncio.Lock()
        self._open: dict[str, _Partition] = {}
        self._recovered: set[str] = set()
        self._sink_failures: list[SinkFailure] = []
        # Two processes appending to one file can interleave a torn line that
        # belongs to neither of them. A per-instance token gives each writer its
        # own file inside the partition, so a torn tail is attributable and the
        # rest of the partition stays trustworthy.
        self._writer_id = secrets.token_hex(4)

    @property
    def root(self) -> Path:
        return self._root

    @property
    def sink_failures(self) -> tuple[SinkFailure, ...]:
        """Sinks that rejected an event. Surfaced so a supervisor can alert;
        the events themselves are already safe on disk."""
        return tuple(self._sink_failures)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def append(self, event: Event) -> Event:
        """Assign the next `seq` for the account, write, return the sealed event.

        The caller's `event` is not touched — `Event` is frozen, and the `seq` it
        was constructed with (conventionally 0) stays 0. What was written is what
        comes back, and that is the object to hand onwards.
        """
        account_id = event.account_id
        _check_account(account_id)
        async with self._lock:
            await self._recover(account_id)
            sealed = event.model_copy(update={"seq": self._seq.next(account_id)})
            line = encode(sealed)
            part = await self._partition(account_id)
            await asyncio.to_thread(_write_line, part.handle, line)
            part.unsynced += 1
            await self._maybe_fsync(part)
        # Outside the lock: a slow sink must not stall other producers, and by
        # here the record is already durable.
        await self._fan_out(sealed, line)
        return sealed

    async def gap(self, account_id: str, venue: str, data_gap: DataGap) -> Event:
        """Record a hole in the market-data history.

        `ts_event_ns` is the end of the gap: the moment we know the hole closed,
        which is also the earliest point from which data is trustworthy again.
        Research reads these to refuse a backtest that crosses one.
        """
        _check_account(account_id)
        return await self.append(
            Event(
                seq=0,
                type=EventType.DATA_GAP,
                account_id=account_id,
                venue=venue,
                ts_event_ns=data_gap.to_ns,
                ts_ingest_ns=self._clock.now_ns(),
                payload=data_gap.model_dump(),
            )
        )

    async def read(
        self,
        account_id: str,
        since_ns: int | None = None,
        until_ns: int | None = None,
    ) -> AsyncIterator[Event]:
        """Replay in `seq` order. `[since_ns, until_ns)` on `ts_ingest_ns`.

        Filtered on our own clock rather than the venue's: `seq` order agrees
        with ingest order by construction, while venue timestamps can arrive out
        of order and would make a time window and a seq window disagree.

        Partitions are read whole and sorted, because several writer files in one
        day are interleaved by seq. That bounds memory to one account-day, which
        at decision-plane rates is kilobytes.
        """
        _check_account(account_id)
        acct_dir = self._root / "events" / f"account={account_id}"
        if not await asyncio.to_thread(acct_dir.is_dir):
            return
        for day_dir in sorted(await asyncio.to_thread(_list_dirs, acct_dir)):
            day = _day_from_dirname(day_dir.name)
            if day is None or not _day_in_range(day, since_ns, until_ns):
                continue
            records = await asyncio.to_thread(_load_partition, day_dir)
            records.sort(key=lambda obj: obj["seq"])
            for obj in records:
                event = Event.model_validate(obj)
                if since_ns is not None and event.ts_ingest_ns < since_ns:
                    continue
                if until_ns is not None and event.ts_ingest_ns >= until_ns:
                    continue
                yield event

    async def sync(self) -> None:
        """Force every open partition to disk. For a supervisor's idle tick: a
        time-based policy only fires on the next append, so an engine that goes
        quiet holding unsynced bytes needs someone to say so."""
        async with self._lock:
            for part in self._open.values():
                await asyncio.to_thread(_fsync, part.handle)
                part.unsynced = 0
                part.last_fsync_ns = self._clock.now_ns()

    async def close(self) -> None:
        async with self._lock:
            for part in self._open.values():
                await asyncio.to_thread(_close, part.handle)
            self._open.clear()
        for sink in self._sinks:
            await sink.close()

    # -- internals ---------------------------------------------------------

    async def _partition(self, account_id: str) -> _Partition:
        day = utc_day(self._clock.now_ns())
        part = self._open.get(account_id)
        if part is not None and part.day == day:
            return part
        if part is not None:
            # Date boundary. The old file is complete and is never reopened,
            # which is what makes cold-partition offload safe.
            await asyncio.to_thread(_close, part.handle)
        directory = partition_dir(self._root, account_id, day)
        path = directory / f"events-{day.isoformat()}-{self._writer_id}.jsonl"
        handle = await asyncio.to_thread(_open_append, path)
        fresh = _Partition(day=day, path=path, handle=handle, last_fsync_ns=self._clock.now_ns())
        self._open[account_id] = fresh
        return fresh

    async def _maybe_fsync(self, part: _Partition) -> None:
        policy = self._fsync
        due = part.unsynced >= policy.every_n_events
        if not due and policy.every_seconds > 0:
            elapsed_ns = self._clock.now_ns() - part.last_fsync_ns
            due = elapsed_ns >= policy.every_seconds * _NS_PER_S
        if not due:
            return
        await asyncio.to_thread(_fsync, part.handle)
        part.unsynced = 0
        part.last_fsync_ns = self._clock.now_ns()

    async def _recover(self, account_id: str) -> None:
        """Continue the sequence after whatever is already on disk.

        Without this, a restart would begin at 1 again and the account's history
        would contain two events with the same seq — the one thing that makes the
        audit trail ambiguous. Runs once per account per process; it reads the
        account's partitions, which is cheap at these volumes.
        """
        if account_id in self._recovered:
            return
        acct_dir = self._root / "events" / f"account={account_id}"
        last_seq = await asyncio.to_thread(_max_seq, acct_dir)
        if last_seq >= FIRST_SEQ:
            self._seq.resume_after(account_id, last_seq)
        self._recovered.add(account_id)

    async def _fan_out(self, event: Event, line: str) -> None:
        for sink in self._sinks:
            try:
                await sink.emit(event, line)
            except Exception as exc:  # noqa: BLE001 - a projection must not break the log
                self._sink_failures.append(
                    SinkFailure(
                        sink=type(sink).__name__,
                        account_id=event.account_id,
                        seq=event.seq,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )


def _check_account(account_id: str) -> None:
    if not _ACCOUNT_RE.fullmatch(account_id):
        raise ValueError(
            f"unusable account_id {account_id!r}: must match {_ACCOUNT_RE.pattern} "
            "(it is a partition directory name)"
        )


def _open_append(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    # `newline=""` so nothing translates the terminator: a line written on one
    # platform and read on another must be the same bytes.
    return path.open("a", encoding="utf-8", newline="")


def _write_line(handle: Any, line: str) -> None:
    handle.write(line + "\n")
    # Always leave the process buffer, whatever the fsync policy. This costs
    # nothing and means our own crash — the common one — loses no events; the
    # policy only governs exposure to the host dying.
    handle.flush()


def _fsync(handle: Any) -> None:
    os.fsync(handle.fileno())


def _close(handle: Any) -> None:
    handle.flush()
    os.fsync(handle.fileno())
    handle.close()


def _list_dirs(directory: Path) -> list[Path]:
    return [p for p in directory.iterdir() if p.is_dir()]


def _day_from_dirname(name: str) -> date | None:
    if not name.startswith("date="):
        return None
    try:
        return date.fromisoformat(name[len("date=") :])
    except ValueError:
        return None


def _day_in_range(day: date, since_ns: int | None, until_ns: int | None) -> bool:
    """Prune partitions by date, with a day of slack on each side.

    An event's partition comes from the clock at write time, which can differ
    from its own `ts_ingest_ns` across a midnight boundary. The slack makes the
    prune an optimisation only; the per-event filter is what decides.
    """
    start_ns = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * _NS_PER_S
    end_ns = start_ns + _NS_PER_DAY
    starts_after = until_ns is not None and start_ns - _NS_PER_DAY >= until_ns
    ends_before = since_ns is not None and end_ns + _NS_PER_DAY <= since_ns
    return not (starts_after or ends_before)


def _load_partition(day_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(day_dir.glob("*.jsonl")):
        records.extend(_load_file(path))
    return records


def _load_file(path: Path) -> Iterable[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError as exc:
            # Only the final line may be torn — that is a crash mid-write, and
            # everything before it is intact. Anywhere else, the file is not
            # what we think it is and a replay must not guess.
            if index == len(lines) - 1:
                break
            raise CorruptEventLog(f"{path}: unparsable line {index + 1}") from exc
    return out


def _max_seq(acct_dir: Path) -> int:
    if not acct_dir.is_dir():
        return 0
    highest = 0
    for path in acct_dir.glob("date=*/*.jsonl"):
        for obj in _load_file(path):
            seq = obj.get("seq", 0)
            if isinstance(seq, int) and seq > highest:
                highest = seq
    return highest
