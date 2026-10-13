"""REST polling for the streams the websocket does not carry.

Funding and open interest arrive on Hyperliquid through `activeAssetCtx`, which
cannot be subscribed per asset under `universe.mode: all_perps` — the venue caps
subscriptions at 1000 per IP. They are therefore polled over REST.

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

if TYPE_CHECKING:
    from funding_atlas.core.clock import Clock
    from funding_atlas.core.config import RecorderConfig
    from funding_atlas.recorder.integrity import EventSink
    from funding_atlas.recorder.streams import StreamConsumer
    from funding_atlas.venues.hyperliquid.rest import AssetContext

log = structlog.get_logger(__name__)

#: Failures back off from here, doubling to the ceiling. The venue's own weight
#: limiter is inside the REST client; this only governs how hard we retry after
#: it has already said no.
BACKOFF_FLOOR_S = 5.0
BACKOFF_CEILING_S = 120.0


class ContextSource(Protocol):
    """The slice of `HyperliquidRest` this needs. Narrow on purpose: the poller
    is venue-agnostic and a second venue only has to supply this one call."""

    async def asset_contexts(self) -> list[AssetContext]: ...


class ContextPoller:
    """Polls asset contexts and files funding and open interest."""

    def __init__(
        self,
        source: ContextSource,
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
        self._emit_funding = funding.enabled
        self._emit_oi = oi.enabled
        self._funding_every_s = float(funding.poll_seconds)
        self._oi_every_s = float(oi.poll_seconds)

        # One REST call returns both, so the loop ticks at the finer cadence and
        # each stream is filed only when its own interval is due. Emitting both
        # at the fast cadence would be free in requests but not in disk, and
        # disk is the binding constraint here.
        intervals = [
            s
            for s, on in (
                (self._funding_every_s, self._emit_funding),
                (self._oi_every_s, self._emit_oi),
            )
            if on
        ]
        self._tick_s = min(intervals) if intervals else 0.0
        self._next_funding_ns = 0
        self._next_oi_ns = 0

        self.polls = 0
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
        contexts = await self._source.asset_contexts()
        now = self._clock.now_ns()
        self.polls += 1

        due_funding = self._emit_funding and now >= self._next_funding_ns
        due_oi = self._emit_oi and now >= self._next_oi_ns

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

        if due_funding:
            self._next_funding_ns = now + int(self._funding_every_s * 1_000_000_000)
        if due_oi:
            self._next_oi_ns = now + int(self._oi_every_s * 1_000_000_000)
