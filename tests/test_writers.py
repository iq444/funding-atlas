"""Rotation, crash-safety and shutdown tests for ParquetWriter.

Time is driven by `SimClock`, so the hour-scale bounds in config/recorder.yaml
are tested at their real values instead of being shrunk to something a test can
wait for.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from funding_atlas.core.clock import SimClock
from funding_atlas.recorder.writers import ParquetWriter, RotationPolicy
from funding_atlas.store import parquet as store
from funding_atlas.venues.records import BookLevel, BookSnapshot, FundingSample, Mid, Trade

VENUE = "hyperliquid"
DAY_NS = 86_400 * 1_000_000_000
# 2026-10-13T00:00:00Z — a UTC midnight, so date-boundary arithmetic is exact.
MIDNIGHT = int(dt.datetime(2026, 10, 13, tzinfo=dt.UTC).timestamp()) * 1_000_000_000
NOON = MIDNIGHT + 12 * 3600 * 1_000_000_000


def trade(ts_ns: int, asset: str = "BTC", price: str = "112345.678901234567") -> Trade:
    return Trade(
        venue=VENUE,
        asset=asset,
        ts_event_ns=ts_ns,
        ts_ingest_ns=ts_ns + 1_000,
        price=Decimal(price),
        size=Decimal("0.5"),
        side="buy",
        trade_id=f"t-{ts_ns}",
    )


def parquet_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet"))


def rows_in(root: Path) -> int:
    return sum(pq.read_metadata(p).num_rows for p in parquet_files(root))


def writer(tmp_path: Path, clock: SimClock, **policy: object) -> ParquetWriter:
    return ParquetWriter(tmp_path, RotationPolicy(**policy), clock=clock)  # type: ignore[arg-type]


async def test_buffers_until_max_rows(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=3, max_seconds=3600)

    await w.append(trade(NOON))
    await w.append(trade(NOON + 1))
    assert parquet_files(tmp_path) == []
    assert w.pending_rows == 2
    assert w.rows_written == {}

    await w.append(trade(NOON + 2))
    files = parquet_files(tmp_path)
    assert len(files) == 1
    assert pq.read_metadata(files[0]).num_rows == 3
    assert w.pending_rows == 0
    assert w.rows_written["trades"] == 3


async def test_rotates_by_row_count_repeatedly(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=10, max_seconds=3600)
    for i in range(25):
        clock.set_now_ns(NOON + i)
        await w.append(trade(NOON + i))
    await w.close()

    assert len(parquet_files(tmp_path)) == 3  # 10 + 10 + 5
    assert rows_in(tmp_path) == 25
    assert w.rows_written["trades"] == 25
    # Unique names inside one partition directory, in write order.
    names = [p.name for p in parquet_files(tmp_path)]
    assert len(set(names)) == 3


async def test_rotates_by_time_on_a_quiet_stream(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=500_000, max_seconds=3600)
    await w.append(trade(NOON))
    assert parquet_files(tmp_path) == []

    # Nothing else arrives: the heartbeat call is what lands the partial file.
    clock.set_now_ns(NOON + 3600 * 1_000_000_000)
    await w.flush_due()
    assert rows_in(tmp_path) == 1
    assert w.last_flush_ns["trades"] == clock.now_ns()


async def test_time_rotation_also_fires_from_append(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=500_000, max_seconds=60)
    await w.append(trade(NOON))
    clock.set_now_ns(NOON + 61 * 1_000_000_000)
    await w.append(trade(clock.now_ns()))
    assert rows_in(tmp_path) == 2
    assert w.pending_rows == 0


async def test_date_boundary_rotation_regardless_of_buffer_state(tmp_path: Path) -> None:
    clock = SimClock(MIDNIGHT + DAY_NS - 1_000_000_000)  # 23:59:59
    w = writer(tmp_path, clock, max_rows=500_000, max_seconds=3600)
    await w.append(trade(clock.now_ns()))

    # One second later it is tomorrow. Yesterday's partition must close now, not
    # an hour later, or a reader cannot tell a finished day from a lagging one.
    clock.set_now_ns(MIDNIGHT + DAY_NS)
    await w.flush_due()
    assert [p.parent.name for p in parquet_files(tmp_path)] == ["date=2026-10-13"]

    await w.append(trade(clock.now_ns()))
    await w.close()
    assert [p.parent.name for p in parquet_files(tmp_path)] == [
        "date=2026-10-13",
        "date=2026-10-14",
    ]


async def test_records_are_partitioned_by_event_date_not_arrival(tmp_path: Path) -> None:
    clock = SimClock(MIDNIGHT + DAY_NS + 1_000_000_000)  # 00:00:01 tomorrow
    w = writer(tmp_path, clock, max_rows=1, max_seconds=3600)
    late = trade(MIDNIGHT + DAY_NS - 1)  # stamped 23:59:59.999999999 yesterday
    await w.append(late)
    assert [p.parent.name for p in parquet_files(tmp_path)] == ["date=2026-10-13"]


async def test_streams_and_assets_get_separate_partitions(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=500_000, max_seconds=3600)
    await w.append(trade(NOON, asset="BTC"))
    await w.append(trade(NOON, asset="ETH"))
    await w.append(
        Mid(venue=VENUE, asset="BTC", ts_event_ns=NOON, ts_ingest_ns=NOON, mid=Decimal(1))
    )
    await w.append(
        FundingSample(
            venue=VENUE,
            asset="BTC",
            ts_event_ns=NOON,
            ts_ingest_ns=NOON,
            rate=Decimal("0.0000125"),
            interval_s=3600,
        )
    )
    assert w.pending_rows == 4
    await w.close()

    rels = sorted(str(p.parent.relative_to(tmp_path)) for p in parquet_files(tmp_path))
    assert rels == [
        f"funding/venue={VENUE}/date=2026-10-13",
        f"mids/venue={VENUE}/asset=BTC/date=2026-10-13",
        f"trades/venue={VENUE}/asset=BTC/date=2026-10-13",
        f"trades/venue={VENUE}/asset=ETH/date=2026-10-13",
    ]
    assert w.rows_written == {"trades": 2, "mids": 1, "funding": 1}
    assert set(w.last_flush_ns) == {"trades", "mids", "funding"}


async def test_close_flushes_the_buffer(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=500_000, max_seconds=3600)
    await w.append(trade(NOON))
    await w.close()
    assert rows_in(tmp_path) == 1
    assert w.pending_rows == 0


async def test_context_manager_flushes_even_on_exception(tmp_path: Path) -> None:
    clock = SimClock(NOON)

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with writer(tmp_path, clock, max_rows=500_000, max_seconds=3600) as w:
            await w.append(trade(NOON))
            raise Boom

    # Records captured before the failure are still valid history.
    assert rows_in(tmp_path) == 1


async def test_no_tmp_files_left_behind(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    async with writer(tmp_path, clock, max_rows=5, max_seconds=3600) as w:
        for i in range(12):
            await w.append(trade(NOON + i))
    assert list(tmp_path.rglob("*" + store.TMP_SUFFIX)) == []
    assert rows_in(tmp_path) == 12


async def test_values_read_back_exactly(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    price = Decimal("0.000000000000012345")
    snapshot = BookSnapshot(
        venue=VENUE,
        asset="PEPE",
        ts_event_ns=NOON,
        ts_ingest_ns=NOON + 10,
        bids=(BookLevel(price=price, size=Decimal("1000000.5"), n_orders=4),),
        asks=(BookLevel(price=price * 2, size=Decimal("2.25"), n_orders=None),),
    )
    async with writer(tmp_path, clock, max_rows=500_000, max_seconds=3600) as w:
        await w.append(trade(NOON, asset="PEPE", price=str(price)))
        await w.append(snapshot)

    (trade_file,) = [p for p in parquet_files(tmp_path) if "trades" in p.parts]
    (book_file,) = [p for p in parquet_files(tmp_path) if "l2" in p.parts]

    (got_price,) = pq.read_table(trade_file).column("price").to_pylist()
    assert got_price == price
    (bids,) = pq.read_table(book_file).column("bids").to_pylist()
    assert bids == [{"price": price, "size": Decimal("1000000.5"), "n_orders": 4}]
    assert store.read_schema_version(book_file) == store.SCHEMA_VERSION


async def test_append_during_flush_is_not_lost(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=2, max_seconds=3600)
    # Two appends racing a rotation: the flush of the first pair must not carry
    # away, or drop, the records that arrive while it is writing in a thread.
    await asyncio.gather(
        w.append(trade(NOON)),
        w.append(trade(NOON + 1)),
        w.append(trade(NOON + 2)),
    )
    await w.close()
    assert rows_in(tmp_path) == 3
    assert w.rows_written["trades"] == 3


async def test_flush_does_not_block_the_event_loop(tmp_path: Path) -> None:
    clock = SimClock(NOON)
    w = writer(tmp_path, clock, max_rows=20_000, max_seconds=3600)
    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0)
            ticks += 1

    beat = asyncio.create_task(heartbeat())
    try:
        for i in range(20_000):
            await w.append(trade(NOON + i))
    finally:
        beat.cancel()
    # The write ran in a thread, so the loop kept scheduling other work through it.
    assert rows_in(tmp_path) == 20_000
    assert ticks > 0


def test_rotation_policy_reads_the_real_config() -> None:
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "config/recorder.yaml").read_text()
    )
    policy = RotationPolicy.from_config(config)
    assert policy.max_rows == 500_000
    assert policy.max_seconds == 3600.0
    # Same result from the subtree alone, and string values survive.
    assert RotationPolicy.from_config(config["rotation"]) == policy
    assert RotationPolicy.from_config({"max_rows": "250_000"}).max_rows == 250_000
    assert RotationPolicy() == RotationPolicy(max_rows=500_000, max_seconds=3600.0)


async def test_unknown_record_type_is_rejected(tmp_path: Path) -> None:
    w = writer(tmp_path, SimClock(NOON), max_rows=1, max_seconds=3600)
    with pytest.raises(TypeError):
        await w.append(object())
