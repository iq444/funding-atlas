"""REST polling for the streams the websocket does not carry.

Funding and open interest arrive on Hyperliquid through `activeAssetCtx`, which
cannot be subscribed per asset under `universe.mode: all_perps` — the venue caps
subscriptions at 1000 per IP. They are therefore polled over REST.

Asset specs have no socket stream at all: `meta` is a request, the recorder
already makes it once at startup to build its subscriptions, and then throws the
answer away. Polling it on a slow cadence is what turns the venue's current
trading rules into a history of them — which is the only form a backtest can
size orders against, and one the venue will not sell back later.

Without this module the recorder runs, writes trades, mids and books, reports
itself healthy, and records funding as an unbroken series of gaps: the one
dataset the whole project exists to collect would be the one thing missing, and
nothing would look wrong. That failure mode is the reason this file is wired
into the supervisor rather than left as a later improvement.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol

import structlog

from funding_atlas.core.events import EventType
from funding_atlas.venues.records import AssetSpecSample

if TYPE_CHECKING:
    from funding_atlas.core.clock import Clock
    from funding_atlas.core.config import RecorderConfig
    from funding_atlas.recorder.integrity import EventSink
    from funding_atlas.recorder.streams import StreamConsumer
    from funding_atlas.venues.base import AssetSpec
    from funding_atlas.venues.hyperliquid.rest import AssetContext

log = structlog.get_logger(__name__)

#: Failures back off from here, doubling to the ceiling. The venue's own weight
#: limiter is inside the REST client; this only governs how hard we retry after
#: it has already said no.
BACKOFF_FLOOR_S = 5.0
BACKOFF_CEILING_S = 120.0


class ContextSource(Protocol):
    """The funding/OI slice of `HyperliquidRest`. Narrow on purpose: the poller
    is venue-agnostic and a second venue only has to supply this one call."""

    async def asset_contexts(self) -> list[AssetContext]: ...


class SpecSource(Protocol):
    """The trading-rules slice. Same shape as `hyperliquid.ws.AssetSpecSource`,
    declared again here rather than imported: this module must not depend on a
    venue package, and the two seams are free to diverge.

    Kept separate from `ContextSource` because they are separate requests with
    separate costs and cadences — `meta` is one call for the whole universe,
    billed on its own, and is polled a quarter-hour apart rather than a minute.
    """

    async def asset_specs(self) -> dict[str, AssetSpec]: ...


class PollSource(ContextSource, SpecSource, Protocol):
    """What one venue must supply for the whole polled side.

    Required together rather than optionally: a source that cannot answer
    `asset_specs` while the stream is enabled would record nothing and report
    healthy, which is the failure mode this module exists to prevent. A type
    error at the wiring point is the cheap version of that discovery.
    """


def specs_to_samples(specs: dict[str, AssetSpec], venue: str, ts_ns: int) -> list[AssetSpecSample]:
    """Stamp one poll's specs with one timestamp.

    One timestamp for the whole batch, not one per record: the venue returned a
    single `meta` document describing a single instant, and stamping each asset
    as it is converted would invent a spread across the universe that says
    nothing but our own loop time — and would make "the rules at time T" a range
    query instead of an equality one.

    `ts_event_synthetic` is left at the record's default of True. `meta` carries
    no time of its own (API doc §4), so there is nothing else it could be.
    """
    return [
        AssetSpecSample(
            venue=venue,
            asset=spec.asset,
            ts_event_ns=ts_ns,
            ts_ingest_ns=ts_ns,
            price_tick=spec.price_tick,
            price_sig_figs=spec.price_sig_figs,
            size_lot=spec.size_lot,
            min_notional=spec.min_notional,
            max_leverage=spec.max_leverage,
            is_perp=spec.is_perp,
            is_delisted=spec.is_delisted,
        )
        for spec in specs.values()
    ]


class ContextPoller:
    """Polls the venue's REST surface and files funding, open interest and specs."""

    def __init__(
        self,
        source: PollSource,
        config: RecorderConfig,
        clock: Clock,
        consumer: StreamConsumer,
        sink: EventSink,
    ) -> None:
        self._source = source
        self._clock = clock
        self._consumer = consumer
        self._sink = sink

        funding = config.streams.funding
        oi = config.streams.open_interest
        specs = config.streams.asset_specs
        self._venue = config.venue
        self._emit_funding = funding.enabled
        self._emit_oi = oi.enabled
        self._emit_specs = specs.enabled
        self._funding_every_s = float(funding.poll_seconds)
        self._oi_every_s = float(oi.poll_seconds)
        self._specs_every_s = float(specs.poll_seconds)

        # One `metaAndAssetCtxs` call returns funding and OI together, so the
        # loop ticks at the finest cadence and each stream is filed only when its
        # own interval is due. Emitting both at the fast cadence would be free in
        # requests but not in disk, and disk is the binding constraint here.
        #
        # Asset specs are a *different* request, so being due is also what
        # decides whether that request is made at all: fetching `meta` every
        # minute to discard 14 of every 15 answers would spend rate-limit weight
        # on nothing.
        intervals = [
            s
            for s, on in (
                (self._funding_every_s, self._emit_funding),
                (self._oi_every_s, self._emit_oi),
                (self._specs_every_s, self._emit_specs),
            )
            if on
        ]
        self._tick_s = min(intervals) if intervals else 0.0
        self._next_funding_ns = 0
        self._next_oi_ns = 0
        self._next_specs_ns = 0

        self.polls = 0
        #: Counted separately from `polls`: the two are different requests on
        #: different cadences, and one number would hide a spec poll that stopped
        #: happening behind a funding poll that did not.
        self.spec_polls = 0
        self.failures = 0

    @property
    def enabled(self) -> bool:
        return self._tick_s > 0

    async def run(self, *, stop: asyncio.Event | None = None, max_ticks: int | None = None) -> None:
        if not self.enabled:
            log.info("poller.disabled")
            return

        backoff = BACKOFF_FLOOR_S
        ticks = 0
        while stop is None or not stop.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — a poll failure must not end the recorder
                self.failures += 1
                # Reported, never swallowed: the monitor turns the resulting
                # silence into a gap, so a REST outage is visible in history
                # rather than only in a log nobody reads.
                log.warning("poller.failed", error=str(exc), backoff_s=backoff)
                await self._sink.emit(
                    EventType.ERROR,
                    {"component": "context_poller", "error": str(exc), "backoff_s": backoff},
                )
                await self._clock.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_CEILING_S)
            else:
                backoff = BACKOFF_FLOOR_S
                await self._clock.sleep(self._tick_s)

            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                return

    async def _poll_once(self) -> None:
        # Due-ness is decided before any request, because on this stream it
        # decides whether the request happens. Taken once so that two streams
        # sharing a tick cannot disagree about what time it is.
        now = self._clock.now_ns()
        due_funding = self._emit_funding and now >= self._next_funding_ns
        due_oi = self._emit_oi and now >= self._next_oi_ns
        due_specs = self._emit_specs and now >= self._next_specs_ns

        contexts: list[AssetContext] = []
        if due_funding or due_oi:
            contexts = await self._source.asset_contexts()
            self.polls += 1

        for ctx in contexts:
            if due_funding:
                await self._consumer.handle(ctx.funding)
            if due_oi:
                await self._consumer.handle(ctx.open_interest)
            # `ctx.mark` is deliberately not filed. It would land in the `mids`
            # stream beside websocket mid prices, and a mark is the venue's own
            # valuation rather than a mid — mixing them would make the column
            # mean two things. It is already carried as `FundingSample.mark_price`,
            # alongside the oracle price it is quoted against.

        if due_specs:
            specs = await self._source.asset_specs()
            self.spec_polls += 1
            # Stamped after the response, from our clock: `meta` carries no time
            # and the fetch is where the latency is.
            ts_ns = self._clock.now_ns()
            for sample in specs_to_samples(specs, self._venue, ts_ns):
                # Written for every asset on every poll, unchanged rows
                # included. A change-only stream makes a missing row mean either
                # "nothing changed" or "the recorder was down", and that is the
                # distinction the whole integrity layer exists to keep.
                await self._consumer.handle(sample)

        if due_funding:
            self._next_funding_ns = now + int(self._funding_every_s * 1_000_000_000)
        if due_oi:
            self._next_oi_ns = now + int(self._oi_every_s * 1_000_000_000)
        if due_specs:
            self._next_specs_ns = now + int(self._specs_every_s * 1_000_000_000)
