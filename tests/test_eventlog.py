from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from funding_atlas.core.clock import SimClock
from funding_atlas.core.eventlog import (
    CorruptEventLog,
    EventLog,
    EventSink,
    FsyncPolicy,
    encode,
    partition_dir,
)
from funding_atlas.core.events import DataGap, Event, EventType

NS_PER_S = 1_000_000_000


def at(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp()) * NS_PER_S


def intent(clock: SimClock, account_id: str = "acct-1", **payload: object) -> Event:
    now = clock.now_ns()
    return Event(
        seq=0,
        type=EventType.ORDER_INTENT,
        account_id=account_id,
        venue="hyperliquid",
        ts_event_ns=now,
        ts_ingest_ns=now,
        payload=dict(payload),
    )


@pytest.fixture
def clock() -> SimClock:
    return SimClock(at(2026, 10, 13, 12))


@pytest.fixture
async def log(tmp_path: Path, clock: SimClock):
    async with EventLog(tmp_path, clock) as instance:
        yield instance


async def test_append_assigns_seq_without_touching_the_callers_event(
    log: EventLog, clock: SimClock
) -> None:
    original = intent(clock, asset="BTC")
    sealed = await log.append(original)

    assert original.seq == 0  # the caller's object is untouched
    assert sealed.seq == 1
    assert sealed is not original
    assert sealed.payload == original.payload


async def test_event_is_frozen(log: EventLog, clock: SimClock) -> None:
    sealed = await log.append(intent(clock))
    with pytest.raises(ValidationError):
        sealed.seq = 99  # type: ignore[misc]


async def test_seq_is_unique_and_monotonic_under_concurrent_appends(
    log: EventLog, clock: SimClock
) -> None:
    total = 200
    sealed = await asyncio.gather(*(log.append(intent(clock, i=i)) for i in range(total)))

    assert sorted(e.seq for e in sealed) == list(range(1, total + 1))

    # And the file itself is in seq order, which is what a replay relies on.
    replayed = [e async for e in log.read("acct-1")]
    assert [e.seq for e in replayed] == list(range(1, total + 1))


async def test_sequence_is_per_account(log: EventLog, clock: SimClock) -> None:
    a = await log.append(intent(clock, account_id="acct-1"))
    b = await log.append(intent(clock, account_id="acct-2"))
    assert (a.seq, b.seq) == (1, 1)
    assert partition_dir(log.root, "acct-1", date(2026, 10, 13)).is_dir()
    assert partition_dir(log.root, "acct-2", date(2026, 10, 13)).is_dir()


async def test_replay_returns_what_was_written_in_order(log: EventLog, clock: SimClock) -> None:
    written = []
    for i in range(5):
        written.append(await log.append(intent(clock, i=i)))
        clock.set_now_ns(clock.now_ns() + NS_PER_S)

    replayed = [e async for e in log.read("acct-1")]
    assert replayed == written


async def test_replay_window_is_half_open_on_ingest_time(log: EventLog, clock: SimClock) -> None:
    stamps = []
    for i in range(5):
        stamps.append(clock.now_ns())
        await log.append(intent(clock, i=i))
        clock.set_now_ns(clock.now_ns() + NS_PER_S)

    window = [e async for e in log.read("acct-1", since_ns=stamps[1], until_ns=stamps[3])]
    assert [e.payload["i"] for e in window] == [1, 2]


async def test_replay_of_unknown_account_is_empty(log: EventLog) -> None:
    assert [e async for e in log.read("nobody")] == []


async def test_rotation_happens_on_the_utc_date_boundary(tmp_path: Path) -> None:
    clock = SimClock(at(2026, 10, 13, 23, 59))
    async with EventLog(tmp_path, clock) as log:
        first = await log.append(intent(clock, side="before"))
        clock.set_now_ns(at(2026, 10, 14, 0, 1))
        second = await log.append(intent(clock, side="after"))

    day_one = partition_dir(tmp_path, "acct-1", date(2026, 10, 13))
    day_two = partition_dir(tmp_path, "acct-1", date(2026, 10, 14))
    assert len(list(day_one.glob("*.jsonl"))) == 1
    assert len(list(day_two.glob("*.jsonl"))) == 1

    # Rotation does not restart the sequence, and replay crosses the boundary.
    assert (first.seq, second.seq) == (1, 2)
    async with EventLog(tmp_path, clock) as reader:
        assert [e.payload["side"] async for e in reader.read("acct-1")] == ["before", "after"]


async def test_no_rotation_within_a_day(tmp_path: Path) -> None:
    clock = SimClock(at(2026, 10, 13, 1))
    async with EventLog(tmp_path, clock) as log:
        for hour in (1, 7, 19, 23):
            clock.set_now_ns(at(2026, 10, 13, hour))
            await log.append(intent(clock))
    files = list(partition_dir(tmp_path, "acct-1", date(2026, 10, 13)).glob("*.jsonl"))
    assert len(files) == 1
    assert len(files[0].read_text().splitlines()) == 4


async def test_existing_records_are_never_rewritten(tmp_path: Path, clock: SimClock) -> None:
    async with EventLog(tmp_path, clock) as log:
        await log.append(intent(clock, i=0))
        path = next(partition_dir(tmp_path, "acct-1", date(2026, 10, 13)).glob("*.jsonl"))
        after_first = path.read_bytes()

        for i in range(1, 4):
            await log.append(intent(clock, i=i))
        after_more = path.read_bytes()

    # Strictly an extension of the earlier bytes: no line edited, none removed.
    assert after_more.startswith(after_first)
    assert len(after_more.splitlines()) == 4


async def test_restart_continues_the_sequence(tmp_path: Path, clock: SimClock) -> None:
    async with EventLog(tmp_path, clock) as first:
        for _ in range(3):
            await first.append(intent(clock))
    async with EventLog(tmp_path, clock) as second:
        resumed = await second.append(intent(clock))
    assert resumed.seq == 4

    async with EventLog(tmp_path, clock) as reader:
        assert [e.seq async for e in reader.read("acct-1")] == [1, 2, 3, 4]


async def test_gap_appends_a_data_gap_event(log: EventLog, clock: SimClock) -> None:
    data_gap = DataGap(
        stream="l2",
        asset="BTC",
        from_ns=at(2026, 10, 13, 11),
        to_ns=at(2026, 10, 13, 12),
        reason="disconnect",
        detail="ws closed 1006",
    )
    event = await log.gap("acct-1", "hyperliquid", data_gap)

    assert event.type is EventType.DATA_GAP
    assert event.ts_event_ns == data_gap.to_ns  # stamped at the end of the hole
    assert event.ts_ingest_ns == clock.now_ns()
    assert event.payload["reason"] == "disconnect"
    assert event.payload["asset"] == "BTC"

    replayed = [e async for e in log.read("acct-1")]
    assert replayed == [event]
    assert DataGap.model_validate(replayed[0].payload) == data_gap


async def test_decimal_payload_survives_as_a_string_never_a_float(
    log: EventLog, clock: SimClock
) -> None:
    price = Decimal("64123.123456789012345")
    sealed = await log.append(intent(clock, price=price, size=Decimal("0.0001")))
    (replayed,) = [e async for e in log.read("acct-1")]

    assert replayed.payload["price"] == str(price)
    assert Decimal(replayed.payload["price"]) == price
    assert isinstance(replayed.payload["size"], str)
    assert sealed.payload["price"] == price  # in memory it is still a Decimal


def test_encode_is_deterministic() -> None:
    now = at(2026, 10, 13, 12)
    kwargs: dict[str, object] = {
        "seq": 1,
        "type": EventType.SIGNAL,
        "account_id": "acct-1",
        "venue": "hyperliquid",
        "ts_event_ns": now,
        "ts_ingest_ns": now,
    }
    one = Event(**kwargs, payload={"a": 1, "b": {"y": 2, "x": 3}})
    two = Event(**kwargs, payload={"b": {"x": 3, "y": 2}, "a": 1})
    assert encode(one) == encode(two)
    assert json.loads(encode(one))["type"] == "signal"


async def test_unusable_account_id_is_rejected(log: EventLog, clock: SimClock) -> None:
    with pytest.raises(ValueError, match="unusable account_id"):
        await log.append(intent(clock, account_id="../escape"))


async def test_torn_final_line_is_tolerated_but_mid_file_corruption_is_not(
    tmp_path: Path, clock: SimClock
) -> None:
    async with EventLog(tmp_path, clock) as log:
        await log.append(intent(clock, i=0))
        await log.append(intent(clock, i=1))

    path = next(partition_dir(tmp_path, "acct-1", date(2026, 10, 13)).glob("*.jsonl"))
    good = path.read_text().splitlines()

    path.write_text("\n".join(good) + '\n{"seq":3,"type":"sig')
    async with EventLog(tmp_path, clock) as reader:
        assert [e.payload["i"] async for e in reader.read("acct-1")] == [0, 1]

    path.write_text("not json\n" + "\n".join(good) + "\n")
    async with EventLog(tmp_path, clock) as reader:
        with pytest.raises(CorruptEventLog):
            _ = [e async for e in reader.read("acct-1")]


async def test_sink_receives_the_exact_line_and_a_failure_cannot_lose_an_event(
    tmp_path: Path, clock: SimClock
) -> None:
    class Recorder:
        def __init__(self) -> None:
            self.seen: list[tuple[Event, str]] = []
            self.closed = False

        async def emit(self, event: Event, line: str) -> None:
            self.seen.append((event, line))
            if event.seq == 2:
                raise RuntimeError("pg down")

        async def close(self) -> None:
            self.closed = True

    sink = Recorder()
    assert isinstance(sink, EventSink)

    async with EventLog(tmp_path, clock, sinks=[sink]) as log:
        for i in range(3):
            await log.append(intent(clock, i=i))
        replayed = [e async for e in log.read("acct-1")]

    assert [e.seq for e in replayed] == [1, 2, 3]  # the rejected event is still on disk
    assert [line for _, line in sink.seen] == [encode(e) for e in replayed]
    assert [f.seq for f in log.sink_failures] == [2]
    assert sink.closed


async def test_relaxed_fsync_policy_still_writes_every_event(
    tmp_path: Path, clock: SimClock
) -> None:
    async with EventLog(tmp_path, clock, fsync=FsyncPolicy.relaxed()) as log:
        for i in range(10):
            await log.append(intent(clock, i=i))
        await log.sync()
        assert [e.payload["i"] async for e in log.read("acct-1")] == list(range(10))


def test_fsync_policy_rejects_nonsense() -> None:
    assert FsyncPolicy().every_n_events == 1  # conservative by default
    with pytest.raises(ValueError):
        FsyncPolicy(every_n_events=0)
    with pytest.raises(ValueError):
        FsyncPolicy(every_seconds=-1)
