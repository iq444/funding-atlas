"""Supervisor tests, with the emphasis where the data loss is: shutdown.

No network and no wall-clock waiting. The venue adapter is a fake `MarketFeed`
yielding canned records, which is also what proves the lazy feed factory does
its job — `venues/hyperliquid/ws.py` need not exist for any of this to run.

Time comes from `YieldClock`: a `SimClock` whose `sleep` hands control to the
event loop without advancing the clock. That combination is what the loops under
test need. Advancing would make every heartbeat tick 30 simulated seconds and
trip `max_silence_seconds` within three ticks, so the assertions would be racing
silence detection instead of measuring the thing under test; not yielding at all
would let a loop driven by `clock.sleep` spin without ever letting another task
run. Tests that need time to move move it explicitly with `set_now_ns`.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from funding_atlas.core.clock import SimClock
from funding_atlas.core.config import RecorderConfig
from funding_atlas.core.events import EventType
from funding_atlas.recorder import main as recorder_main
from funding_atlas.recorder.main import EXIT_CRASH, EXIT_OK, Recorder
from funding_atlas.venues.base import AssetSpec
from funding_atlas.venues.records import FundingSample, Mid, Trade

S = 1_000_000_000
VENUE = "hyperliquid"
# A UTC midnight, so partition-date arithmetic in the writers is exact.
MIDNIGHT = int(dt.datetime(2026, 10, 13, tzinfo=dt.UTC).timestamp()) * S
NOON = MIDNIGHT + 12 * 3600 * S


class YieldClock(SimClock):
    """Time stands still, but other tasks get to run. See the module note."""

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(0)


class FakeFeed:
    """Stands in for `HyperliquidFeed`.

    Yields canned records and then, unless `finish` is set, parks forever —
    which is what a live socket does between messages and is the state the
    supervisor has to be able to shut down from.
    """

    def __init__(self, items: list[Any], *, raises: Exception | None = None, finish: bool = True):
        self.items = items
        self.raises = raises
        self.finish = finish
        self.drained = asyncio.Event()
        self.starts = 0

    async def specs(self) -> dict[str, AssetSpec]:
        return {}

    async def stream(self) -> AsyncIterator[Any]:
        self.starts += 1
        for item in self.items:
            yield item
        self.drained.set()
        if self.raises is not None:
            raise self.raises
        if not self.finish:
            await asyncio.Event().wait()


class BrokenWriter:
    """A writer whose flush fails. Stands in for a full disk at shutdown.

    Delegates everything else, so the supervisor sees a writer that behaves
    normally right up to the moment the records have to leave memory.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def close(self) -> None:
        raise OSError("disk full")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def trade(ts_ns: int = NOON, asset: str = "BTC") -> Trade:
    return Trade(
        venue=VENUE,
        asset=asset,
        ts_event_ns=ts_ns,
        ts_ingest_ns=ts_ns,
        price=Decimal("112345.5"),
        size=Decimal("0.25"),
        side="buy",
        trade_id=f"t-{ts_ns}",
    )


def mid(ts_ns: int = NOON, asset: str = "BTC") -> Mid:
    return Mid(venue=VENUE, asset=asset, ts_event_ns=ts_ns, ts_ingest_ns=ts_ns, mid=Decimal("3.5"))


def funding(ts_ns: int = NOON, asset: str = "BTC") -> FundingSample:
    return FundingSample(
        venue=VENUE,
        asset=asset,
        ts_event_ns=ts_ns,
        ts_ingest_ns=ts_ns,
        rate=Decimal("0.0000125"),
        interval_s=3600,
    )


def config_for(tmp_path: Path, **over: Any) -> RecorderConfig:
    raw: dict[str, Any] = {
        "venue": VENUE,
        "network": "testnet",
        "data_dir": str(tmp_path),
        "rotation": {"max_rows": 500_000, "max_seconds": 3600},
        "universe": {"mode": "explicit", "explicit": ["BTC", "ETH"]},
        "streams": {
            "funding": {"enabled": True, "poll_seconds": 60},
            "trades": {"enabled": True},
            "mids": {"enabled": True, "sample_seconds": 1},
            "l2": {"enabled": False, "assets": [], "sample_ms": 500},
            "open_interest": {"enabled": False},
        },
        "integrity": {"heartbeat_seconds": 30, "max_silence_seconds": 90},
    }
    raw.update(over)
    return RecorderConfig.model_validate(raw)


def build(
    tmp_path: Path, feed: FakeFeed, *, config: RecorderConfig | None = None, **kwargs: Any
) -> tuple[Recorder, YieldClock]:
    clock = YieldClock(NOON)

    async def factory(config: RecorderConfig, _clock: Any, _sink: Any) -> FakeFeed:
        # The signature the real factory has. A feed built here never imports
        # `venues.hyperliquid`, which is the whole point of the seam.
        assert config.venue == VENUE
        return feed

    recorder = Recorder(
        config if config is not None else config_for(tmp_path),
        clock,
        feed_factory=factory,
        drain_timeout_s=0.05,
        restart_backoff_s=0.0,
        **kwargs,
    )
    return recorder, clock


async def events(recorder: Recorder) -> list[Any]:
    return [e async for e in recorder.event_log.read("recorder")]


def parquet_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet"))


# -- routing ---------------------------------------------------------------


async def test_records_reach_the_right_writer_and_the_monitor(tmp_path: Path) -> None:
    feed = FakeFeed([trade(), trade(asset="ETH"), mid(), funding()])
    recorder, _ = build(tmp_path, feed)

    assert await recorder.run() == EXIT_OK

    assert dict(recorder.consumer.handled) == {"trades": 2, "mids": 1, "funding": 1}
    assert recorder.consumer.unroutable == {}
    # Each record landed in its own stream's writer, not in a shared one.
    assert recorder.writers["trades"].rows_written["trades"] == 2
    assert recorder.writers["mids"].rows_written["mids"] == 1
    assert recorder.writers["funding"].rows_written["funding"] == 1

    # The monitor's asset granularity follows the writers': per asset where the
    # partitions are, venue-wide where they are not.
    assert recorder.monitor.state("trades", "BTC") is not None
    assert recorder.monitor.state("trades", "ETH") is not None
    assert recorder.monitor.state("funding", None) is not None
    assert recorder.monitor.state("funding", "BTC") is None


async def test_envelope_from_the_feed_is_accepted(tmp_path: Path) -> None:
    # `MarketFeed.stream()` is typed as yielding dicts; the consumer takes
    # either a bare record or a mapping carrying one, so the venue adapter's
    # choice cannot silently record nothing.
    feed = FakeFeed([{"stream": "trades", "record": trade()}, {"data": mid()}])
    recorder, _ = build(tmp_path, feed)

    assert await recorder.run() == EXIT_OK
    assert dict(recorder.consumer.handled) == {"trades": 1, "mids": 1}


async def test_a_disabled_stream_is_counted_not_fatal(tmp_path: Path) -> None:
    from funding_atlas.venues.records import OpenInterest

    oi = OpenInterest(
        venue=VENUE,
        asset="BTC",
        ts_event_ns=NOON,
        ts_ingest_ns=NOON,
        open_interest=Decimal(12),
    )
    feed = FakeFeed([oi, trade()])
    recorder, _ = build(tmp_path, feed)

    assert await recorder.run() == EXIT_OK
    assert recorder.consumer.unroutable == {"open_interest": 1}
    assert recorder.consumer.handled["trades"] == 1


# -- shutdown --------------------------------------------------------------


async def test_graceful_shutdown_flushes_records_a_gap_and_closes_the_log(
    tmp_path: Path,
) -> None:
    feed = FakeFeed([trade(), mid(), funding()], finish=False)
    recorder, _ = build(tmp_path, feed)
    task = asyncio.create_task(recorder.run())
    await feed.drained.wait()

    # Nothing is on disk yet: the rotation bounds are hours away, so everything
    # recorded so far exists only in a buffer. This is the loss shutdown has to
    # prevent, and asserting it here is what makes the next assertion mean
    # something.
    assert parquet_files(tmp_path) == []

    recorder.request_stop()  # what SIGTERM does
    assert await task == EXIT_OK

    assert len(parquet_files(tmp_path)) == 3
    assert recorder.writers["trades"].pending_rows == 0

    gaps = [e for e in await events(recorder) if e.type == EventType.DATA_GAP]
    assert gaps, "a stop that is not recorded is indistinguishable from full coverage"
    assert {g.payload["reason"] for g in gaps} == {"shutdown"}
    # Every stream we were watching gets its hole written, including the ones
    # that never delivered a single message.
    assert {g.payload["stream"] for g in gaps} == {"trades", "mids", "funding"}

    # Closed, not merely flushed: no partition handle is left open.
    assert recorder.event_log._open == {}


async def test_shutdown_flushes_every_writer_even_when_one_fails(tmp_path: Path) -> None:
    feed = FakeFeed([trade(), mid()], finish=False)
    recorder, _ = build(tmp_path, feed)

    recorder.writers["trades"] = BrokenWriter(recorder.writers["trades"])  # type: ignore[assignment]

    task = asyncio.create_task(recorder.run())
    await feed.drained.wait()
    recorder.request_stop()
    assert await task == EXIT_OK

    # The broken stream cost its own buffer and nothing else: mids still landed,
    # and the shutdown gap was still written.
    assert recorder.writers["mids"].rows_written["mids"] == 1
    gaps = [e for e in await events(recorder) if e.type == EventType.DATA_GAP]
    assert {g.payload["stream"] for g in gaps} == {"trades", "mids", "funding"}


async def test_second_signal_forces_exit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import signal as signal_mod

    feed = FakeFeed([trade()], finish=False)
    recorder, _ = build(tmp_path, feed)
    forced: list[int] = []
    monkeypatch.setattr(recorder_main.os, "_exit", forced.append)

    # First signal asks for the graceful path and does not exit.
    recorder._on_signal(signal_mod.SIGTERM)
    assert forced == []
    assert recorder._stop.is_set()

    # Second one gives up on it.
    recorder._on_signal(signal_mod.SIGINT)
    assert forced == [recorder_main.EXIT_FORCED]


# -- feed failure ----------------------------------------------------------


async def test_feed_exception_is_recorded_not_swallowed(tmp_path: Path) -> None:
    boom = RuntimeError("socket closed mid-frame")
    feed = FakeFeed([trade(), mid()], raises=boom)
    recorder, _ = build(tmp_path, feed, max_feed_restarts=0)

    assert await recorder.run() == EXIT_CRASH
    assert recorder.failure is boom

    logged = await events(recorder)
    errors = [e for e in logged if e.type == EventType.ERROR]
    assert len(errors) == 1
    assert "socket closed mid-frame" in errors[0].payload["error"]

    # The streams were marked down for the right cause, so the hole is
    # attributable afterwards rather than being relabelled a clean stop.
    down = [e for e in logged if e.type == EventType.STREAM_DOWN]
    assert {e.payload["reason"] for e in down} == {"error"}
    gaps = [e for e in logged if e.type == EventType.DATA_GAP]
    assert {g.payload["reason"] for g in gaps} == {"error"}
    # And the records captured before it blew up are still on disk.
    assert len(parquet_files(tmp_path)) == 2


async def test_feed_is_restarted_before_the_supervisor_gives_up(tmp_path: Path) -> None:
    feed = FakeFeed([trade()], raises=RuntimeError("flap"))
    recorder, _ = build(tmp_path, feed, max_feed_restarts=2)

    assert await recorder.run() == EXIT_CRASH
    # One original attempt plus two restarts, then the supervisor chooses exit.
    assert feed.starts == 3


# -- heartbeat -------------------------------------------------------------


async def test_heartbeat_lands_an_idle_streams_partial_file(tmp_path: Path) -> None:
    feed = FakeFeed([trade()], finish=False)
    recorder, clock = build(tmp_path, feed)
    task = asyncio.create_task(recorder.run())
    await feed.drained.wait()
    assert parquet_files(tmp_path) == []

    # The stream goes quiet. `max_seconds` only fires from inside `append`, so
    # without the heartbeat this partial file would never land.
    clock.set_now_ns(NOON + 3601 * S)
    health = await recorder.heartbeat_once()

    assert len(parquet_files(tmp_path)) == 1
    assert health["rows_written"]["trades"] == 1
    assert health["pending_rows"] == 0
    assert health["last_flush_age_s"]["trades"] == 0.0
    assert health["last_flush_age_s"]["mids"] is None

    recorder.request_stop()
    assert await task == EXIT_OK


async def test_the_run_loop_ticks_the_heartbeat(tmp_path: Path) -> None:
    feed = FakeFeed([trade()], finish=False)
    recorder, clock = build(tmp_path, feed, heartbeat_max_ticks=1)
    task = asyncio.create_task(recorder.run())
    await feed.drained.wait()
    clock.set_now_ns(NOON + 3601 * S)

    # The loop is driven by `clock.sleep`, which yields; one tick is enough to
    # prove the supervisor calls `flush_due` without anyone poking it.
    for _ in range(100):
        if parquet_files(tmp_path):
            break
        await asyncio.sleep(0)
    assert len(parquet_files(tmp_path)) == 1

    recorder.request_stop()
    assert await task == EXIT_OK


async def test_health_reports_streams_the_monitor_considers_down(tmp_path: Path) -> None:
    feed = FakeFeed([mid()], finish=False)
    recorder, clock = build(tmp_path, feed)
    task = asyncio.create_task(recorder.run())
    await feed.drained.wait()

    assert recorder.health()["down"] == []
    clock.set_now_ns(NOON + 91 * S)
    await recorder.monitor.check()

    # Every regular stream we were watching is now silent — including the two
    # that were registered from config and never delivered anything, which is
    # the point of registering them. `trades` is market-driven and must never
    # be judged by silence, so it stays out of the list.
    assert recorder.health()["down"] == ["funding:*", "mids:BTC", "mids:ETH"]

    recorder.request_stop()
    assert await task == EXIT_OK


# -- wiring ----------------------------------------------------------------


async def test_startup_line_uses_the_redacted_config(tmp_path: Path) -> None:
    config = config_for(tmp_path, pg_dsn="postgresql://atlas:hunter2@db/atlas")
    redacted = config.redacted()
    assert redacted["pg_dsn"] == "***redacted***"
    assert "hunter2" not in repr(redacted)


async def test_build_feed_rejects_an_unknown_venue() -> None:
    from funding_atlas.core.config import ConfigError

    config = RecorderConfig.model_validate(
        {"venue": "binance", "rotation": {"max_rows": 10, "max_seconds": 10}}
    )
    with pytest.raises(ConfigError, match="binance"):
        await recorder_main.build_feed(config, SimClock(NOON), object())  # type: ignore[arg-type]


async def test_only_enabled_streams_get_a_writer(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    assert recorder_main.enabled_streams(config) == ("trades", "mids", "funding")
    assert set(recorder_main.build_writers(config, SimClock(NOON))) == {
        "trades",
        "mids",
        "funding",
    }


async def test_cadences_follow_the_config(tmp_path: Path) -> None:
    cadences = recorder_main.cadences_for(config_for(tmp_path))
    # Trades are market-driven: a quiet hour on a thin perp is not an outage.
    assert cadences["trades"] is None
    assert cadences["mids"] == 1.0
    assert cadences["funding"] == 60.0


async def test_asset_specs_is_registered_end_to_end(tmp_path: Path) -> None:
    """Enabling the stream must be enough: a writer, a cadence and a watched
    monitor entry, with no further edit here. The failure this guards against is
    a stream that is configured, polled, and then dropped on the floor."""
    config = config_for(
        tmp_path,
        streams={
            "funding": {"enabled": True, "poll_seconds": 60},
            "trades": {"enabled": True},
            "mids": {"enabled": False},
            "l2": {"enabled": False, "assets": []},
            "open_interest": {"enabled": False},
            "asset_specs": {"enabled": True, "poll_seconds": 900},
        },
    )
    assert "asset_specs" in recorder_main.enabled_streams(config)
    assert "asset_specs" in recorder_main.build_writers(config, SimClock(NOON))
    # We set the cadence, so silence on it is evidence of failure — unlike trades.
    assert recorder_main.cadences_for(config)["asset_specs"] == 900.0

    recorder, _clock = build(tmp_path, FakeFeed([]), config=config)
    recorder._register_streams()
    state = recorder.monitor.state("asset_specs")
    assert state is not None
    assert state.is_regular
