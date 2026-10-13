"""The REST poller carries funding, open interest and asset specs.

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
from funding_atlas.venues.base import AssetSpec
from funding_atlas.venues.records import AssetSpecSample, FundingSample, Mid, OpenInterest


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


def _spec(asset: str, *, max_leverage: str = "40", is_delisted: bool = False) -> AssetSpec:
    return AssetSpec(
        asset=asset,
        price_tick=Decimal("0.1"),
        price_sig_figs=5,
        size_lot=Decimal("0.00001"),
        min_notional=Decimal(10),
        max_leverage=Decimal(max_leverage),
        is_perp=True,
        is_delisted=is_delisted,
    )


class FakeSource:
    """Both halves of the polled seam. `asset_specs` is a separate request from
    `asset_contexts`, so the two are counted separately here for the same reason
    the poller counts them separately."""

    def __init__(self, *, fail_times: int = 0, specs: dict[str, AssetSpec] | None = None) -> None:
        self.calls = 0
        self.spec_calls = 0
        self._fail_times = fail_times
        self._specs = specs if specs is not None else {"BTC": _spec("BTC"), "ETH": _spec("ETH")}

    async def asset_contexts(self) -> list[Any]:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError("venue said no")
        return [_context("BTC", 1), _context("ETH", 2)]

    async def asset_specs(self) -> dict[str, AssetSpec]:
        self.spec_calls += 1
        return dict(self._specs)


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


def _config(**streams: bool) -> Any:
    """Shipped config with the named polled streams forced off."""
    base = load_config("config/recorder.yaml")
    updates = {
        name: getattr(base.streams, name).model_copy(update={"enabled": enabled})
        for name, enabled in streams.items()
    }
    return base.model_copy(update={"streams": base.streams.model_copy(update=updates)})


async def test_disabled_when_no_polled_stream_is_enabled() -> None:
    config = _config(funding=False, open_interest=False, asset_specs=False)
    source = FakeSource()
    poller = ContextPoller(source, config, SimClock(1), FakeConsumer(), FakeSink())
    assert not poller.enabled
    await poller.run(max_ticks=5)  # returns immediately
    assert (source.calls, source.spec_calls) == (0, 0)


@pytest.mark.parametrize("ticks", [1, 3])
async def test_every_tick_polls_once(ticks: int) -> None:
    source = FakeSource()
    poller = _poller(source, FakeConsumer(), FakeSink(), SimClock(1))
    await poller.run(max_ticks=ticks)
    assert source.calls == ticks


# --- asset specs -------------------------------------------------------------
#
# The venue serves only its *current* `meta`, so every sample this stream misses
# is a sample that can never be recovered. These tests are about the two ways
# that could happen quietly: the samples never being filed at all, and being
# filed on the wrong (funding's) cadence.


async def test_asset_specs_reach_the_consumer_one_sample_per_asset() -> None:
    consumer, sink = FakeConsumer(), FakeSink()
    poller = _poller(FakeSource(), consumer, sink, SimClock(1))
    await poller.run(max_ticks=1)

    samples = [r for r in consumer.records if isinstance(r, AssetSpecSample)]
    assert {s.asset for s in samples} == {"BTC", "ETH"}
    assert len(samples) == 2


async def test_asset_spec_fields_survive_the_conversion_exactly() -> None:
    consumer = FakeConsumer()
    source = FakeSource(specs={"kPEPE": _spec("kPEPE", max_leverage="3", is_delisted=True)})
    poller = _poller(source, consumer, FakeSink(), SimClock(1))
    await poller.run(max_ticks=1)

    (sample,) = [r for r in consumer.records if isinstance(r, AssetSpecSample)]
    assert sample.venue == "hyperliquid"
    assert sample.asset == "kPEPE"
    assert sample.price_tick == Decimal("0.1")
    assert sample.price_sig_figs == 5
    assert sample.size_lot == Decimal("0.00001")
    assert sample.min_notional == Decimal(10)
    assert sample.max_leverage == Decimal(3)
    assert sample.is_perp is True
    # The one field that says the rules stopped applying, which is the whole
    # point of keeping a history of them.
    assert sample.is_delisted is True


async def test_one_timestamp_for_the_whole_poll_and_it_is_ours() -> None:
    """`meta` carries no time, so the stamp is our clock's and says so — and it
    is one stamp per poll, not one per asset: the venue described one instant."""
    consumer = FakeConsumer()
    poller = _poller(FakeSource(), consumer, FakeSink(), SimClock(1))
    await poller.run(max_ticks=1)

    samples = [r for r in consumer.records if isinstance(r, AssetSpecSample)]
    assert len({s.ts_event_ns for s in samples}) == 1
    assert all(s.ts_event_synthetic for s in samples)
    assert all(s.ts_event_ns == s.ts_ingest_ns for s in samples)


async def test_specs_poll_on_their_own_cadence_not_fundings() -> None:
    """Shipped config: funding every 60s, specs every 900s. Fifteen funding
    polls must not drag fifteen `meta` requests along with them — the two are
    separate requests and the rate-limit weight is spent either way."""
    source = FakeSource()
    consumer = FakeConsumer()
    poller = _poller(source, consumer, FakeSink(), SimClock(1))
    await poller.run(max_ticks=5)

    assert source.calls == 5  # funding/OI, every 60s tick
    assert source.spec_calls == 1  # specs, only the first tick inside the 900s
    assert len([r for r in consumer.records if isinstance(r, AssetSpecSample)]) == 2
    assert len([r for r in consumer.records if isinstance(r, FundingSample)]) == 10


async def test_a_specs_only_poller_never_asks_for_contexts() -> None:
    """The converse of the cadence test: being due is what makes the request
    happen, so a disabled funding stream costs no `metaAndAssetCtxs` call."""
    source = FakeSource()
    consumer = FakeConsumer()
    poller = ContextPoller(
        source,
        _config(funding=False, open_interest=False, asset_specs=True),
        SimClock(1),
        consumer,
        FakeSink(),
    )
    await poller.run(max_ticks=2)

    assert source.calls == 0
    assert source.spec_calls == 2  # tick is now 900s, so both ticks are due
    assert not any(isinstance(r, FundingSample | OpenInterest) for r in consumer.records)


async def test_every_poll_is_written_even_when_nothing_changed() -> None:
    """Change-only rows would make a missing row mean either "nothing changed"
    or "the recorder was down". That ambiguity is the thing this stream exists
    to remove, so an unchanged universe still produces a full sample."""
    source = FakeSource()
    consumer = FakeConsumer()
    clock = SimClock(1)
    poller = ContextPoller(
        source, _config(funding=False, open_interest=False), clock, consumer, FakeSink()
    )
    await poller.run(max_ticks=3)

    samples = [r for r in consumer.records if isinstance(r, AssetSpecSample)]
    assert len(samples) == 6  # 2 assets x 3 identical polls
    # Distinguishable only by time, which is exactly what makes "the recorder
    # was up and nothing changed" a statement the data can make.
    assert len({s.ts_event_ns for s in samples}) == 3
