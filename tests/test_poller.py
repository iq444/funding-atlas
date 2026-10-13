"""The REST poller carries funding and open interest.

Without it the recorder looks healthy while recording nothing of what the
project is for, so these tests mostly assert that the records actually arrive
and that a failure stays visible.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from funding_atlas.core.clock import SimClock
from funding_atlas.core.config import load_config
from funding_atlas.core.events import EventType
from funding_atlas.recorder.poller import BACKOFF_FLOOR_S, ContextPoller
from funding_atlas.venues.records import FundingSample, Mid, OpenInterest


@dataclass(frozen=True)
class FakeContext:
    funding: FundingSample
    open_interest: OpenInterest
    mark: Mid | None


def _context(asset: str, ts: int) -> FakeContext:
    return FakeContext(
        funding=FundingSample(
            venue="hyperliquid",
            asset=asset,
            ts_event_ns=ts,
            ts_ingest_ns=ts,
            rate=Decimal("0.0000125"),
            interval_s=3600,
            ts_event_synthetic=True,
        ),
        open_interest=OpenInterest(
            venue="hyperliquid",
            asset=asset,
            ts_event_ns=ts,
            ts_ingest_ns=ts,
            open_interest=Decimal("1234.5"),
            ts_event_synthetic=True,
        ),
        mark=Mid(
            venue="hyperliquid",
            asset=asset,
            ts_event_ns=ts,
            ts_ingest_ns=ts,
            mid=Decimal("112345.5"),
            ts_event_synthetic=True,
        ),
    )


class FakeSource:
    def __init__(self, *, fail_times: int = 0) -> None:
        self.calls = 0
        self._fail_times = fail_times

    async def asset_contexts(self) -> list[Any]:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError("venue said no")
        return [_context("BTC", 1), _context("ETH", 2)]


class FakeConsumer:
    def __init__(self) -> None:
        self.records: list[Any] = []

    async def handle(self, item: Any) -> None:
        self.records.append(item)


class FakeSink:
    def __init__(self) -> None:
        self.events: list[tuple[EventType, dict[str, Any]]] = []

    async def emit(self, event_type: EventType, payload: dict[str, Any]) -> None:
        self.events.append((event_type, payload))


def _poller(source: Any, consumer: Any, sink: Any, clock: SimClock) -> ContextPoller:
    return ContextPoller(source, load_config("config/recorder.yaml"), clock, consumer, sink)


async def test_funding_and_open_interest_reach_the_consumer() -> None:
    consumer, sink = FakeConsumer(), FakeSink()
    poller = _poller(FakeSource(), consumer, sink, SimClock(1))
    await poller.run(max_ticks=1)

    kinds = [type(r).__name__ for r in consumer.records]
    assert kinds.count("FundingSample") == 2
    assert kinds.count("OpenInterest") == 2


async def test_mark_is_not_filed_as_a_mid() -> None:
    """A mark is the venue's valuation, not a mid. Filing it into the `mids`
    stream beside websocket mids would make that column mean two things."""
    consumer, sink = FakeConsumer(), FakeSink()
    poller = _poller(FakeSource(), consumer, sink, SimClock(1))
    await poller.run(max_ticks=1)

    assert not any(isinstance(r, Mid) for r in consumer.records)


async def test_funding_keeps_the_hourly_interval() -> None:
    consumer, sink = FakeConsumer(), FakeSink()
    poller = _poller(FakeSource(), consumer, sink, SimClock(1))
    await poller.run(max_ticks=1)

    funding = [r for r in consumer.records if isinstance(r, FundingSample)]
    assert {f.interval_s for f in funding} == {3600}


async def test_a_failed_poll_is_reported_and_retried_not_fatal() -> None:
    source = FakeSource(fail_times=1)
    consumer, sink = FakeConsumer(), FakeSink()
    poller = _poller(source, consumer, sink, SimClock(1))
    await poller.run(max_ticks=2)

    assert poller.failures == 1
    assert any(e is EventType.ERROR for e, _ in sink.events)
    # It recovered rather than ending the run.
    assert any(isinstance(r, FundingSample) for r in consumer.records)


async def test_failure_backs_off_from_the_documented_floor() -> None:
    clock = SimClock(1)
    poller = _poller(FakeSource(fail_times=1), FakeConsumer(), FakeSink(), clock)
    before = clock.now_ns()
    await poller.run(max_ticks=1)

    waited_s = (clock.now_ns() - before) / 1e9
    assert waited_s >= BACKOFF_FLOOR_S


async def test_stop_ends_the_loop() -> None:
    stop = asyncio.Event()
    stop.set()
    poller = _poller(FakeSource(), FakeConsumer(), FakeSink(), SimClock(1))
    await asyncio.wait_for(poller.run(stop=stop), timeout=1)


async def test_disabled_when_neither_stream_is_enabled() -> None:
    config = load_config("config/recorder.yaml").model_copy(
        update={
            "streams": load_config("config/recorder.yaml").streams.model_copy(
                update={
                    "funding": load_config("config/recorder.yaml").streams.funding.model_copy(
                        update={"enabled": False}
                    ),
                    "open_interest": load_config(
                        "config/recorder.yaml"
                    ).streams.open_interest.model_copy(update={"enabled": False}),
                }
            )
        }
    )
    poller = ContextPoller(FakeSource(), config, SimClock(1), FakeConsumer(), FakeSink())
    assert not poller.enabled
    await poller.run(max_ticks=5)  # returns immediately


@pytest.mark.parametrize("ticks", [1, 3])
async def test_every_tick_polls_once(ticks: int) -> None:
    source = FakeSource()
    poller = _poller(source, FakeConsumer(), FakeSink(), SimClock(1))
    await poller.run(max_ticks=ticks)
    assert source.calls == ticks
