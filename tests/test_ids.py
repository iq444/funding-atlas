from __future__ import annotations

import threading

import pytest

from funding_atlas.core.ids import FIRST_SEQ, SequenceAllocator, cloid, is_cloid


def test_cloid_format_is_hyperliquid_128_bit_hex() -> None:
    value = cloid("acct-1", "carry", "BTC:2026-10-13T00:00:00Z:leg0")
    assert value.startswith("0x")
    assert len(value) == 34
    assert is_cloid(value)


def test_cloid_is_stable_across_repeated_calls() -> None:
    # The property the retry path depends on: same logical order, same id, with
    # nothing carried between the calls.
    args = ("acct-1", "carry", "BTC:cycle-42:leg0")
    assert cloid(*args) == cloid(*args) == cloid(*args)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        (("acct-1", "carry", "k"), ("acct-2", "carry", "k")),
        (("acct-1", "carry", "k"), ("acct-1", "basis", "k")),
        (("acct-1", "carry", "k"), ("acct-1", "carry", "k2")),
        # Field boundaries must not be ambiguous: a separator-joined scheme
        # would collide on these two.
        (("a", "bc", "k"), ("ab", "c", "k")),
    ],
)
def test_distinct_logical_orders_get_distinct_cloids(
    a: tuple[str, str, str], b: tuple[str, str, str]
) -> None:
    assert cloid(*a) != cloid(*b)


def test_sequence_starts_at_first_seq_and_is_per_account() -> None:
    alloc = SequenceAllocator()
    assert alloc.peek("a") == FIRST_SEQ
    assert alloc.next("a") == 1
    assert alloc.next("a") == 2
    assert alloc.next("b") == 1
    assert alloc.peek("a") == 3


def test_sequence_is_unique_under_concurrent_threads() -> None:
    alloc = SequenceAllocator()
    per_thread = 500
    threads_n = 8
    results: list[list[int]] = [[] for _ in range(threads_n)]

    def worker(slot: int) -> None:
        results[slot] = [alloc.next("acct-1") for _ in range(per_thread)]

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    issued = [seq for slot in results for seq in slot]
    total = per_thread * threads_n
    assert sorted(issued) == list(range(1, total + 1))
    # Each thread saw its own numbers strictly increasing.
    for slot in results:
        assert slot == sorted(slot)


def test_resume_after_never_lowers_the_sequence() -> None:
    alloc = SequenceAllocator()
    alloc.resume_after("a", 100)
    assert alloc.next("a") == 101
    alloc.resume_after("a", 50)  # a late recovery scan must not reissue 51
    assert alloc.next("a") == 102


def test_known_accounts_tracks_allocation() -> None:
    alloc = SequenceAllocator()
    alloc.next("a")
    alloc.resume_after("b", 7)
    assert alloc.known_accounts() == frozenset({"a", "b"})
