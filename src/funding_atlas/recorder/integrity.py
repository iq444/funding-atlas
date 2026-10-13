"""Stream integrity: liveness, silence detection, gap bookkeeping.

Order-book and funding history cannot be backfilled. Ten minutes of lost socket
is ten minutes that will never exist, so a gap is not a transient error to retry
past — it is a durable fact. This module's job is to notice the loss, bound it
in time, attribute a cause to it, and hand it to the event log so that research
can later refuse a backtest that would otherwise run across invented liquidity.

Two consequences shape the design:

* Detection is pessimistic on the regular streams. A sampled stream (mids, L2,
  funding and asset-spec polls) has a known cadence, so silence is evidence of
  failure and we act on it. The cadences span three orders of magnitude, so the
  threshold is per-stream — see `SILENCE_CADENCE_FACTOR`.

* Detection is *silent* on the irregular streams. Trades are market-driven: a
  quiet hour on a thin perp produces no messages at all, and declaring that
  stream down would both lie in the event log and trigger a pointless
  reconnect that drops the subscriptions that *are* working. Irregular streams
  are therefore only ever marked down by evidence from the transport —
  `record_disconnect` / `record_error` / `record_backpressure`.

* Bookkeeping survives a clean stop. A deliberate shutdown leaves exactly the
  same hole in history as a crash; hiding it because we meant to do it is how a
  dataset acquires silent holes.

Gap bounds always start at the last message actually recorded, not at the moment
detection fired. Everything after that last message is unrecorded by
definition, even though we only found out `max_silence_seconds` later.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from funding_atlas.core.clock import Clock
from funding_atlas.core.events import DataGap, EventType
from funding_atlas.venues.records import STREAMS

NS_PER_S = 1_000_000_000

#: Expected sampling cadence per stream, seconds. `None` marks a stream whose
#: arrival times are set by the market rather than by us — see the module note
#: on why those must never be judged by silence. Defaults mirror
#: `config/recorder.yaml`; a caller holding the parsed config should override.
DEFAULT_CADENCE_S: dict[str, float | None] = {
    "trades": None,
    "mids": 1.0,
    "l2": 0.5,
    "funding": 60.0,
    "open_interest": 60.0,
    # Polled on a cadence we set, like funding — not market-driven, so silence
    # is failure. Slow (15 min) because the venue's rules change on a timescale
    # of days; the cadence exists to make a gap detectable, not to catch a
    # change quickly.
    "asset_specs": 900.0,
}

#: A regular stream is judged silent after `max_silence_seconds` *or* this many
#: of its own sampling intervals, whichever is longer.
#:
#: The global bound was chosen for streams sampled every 0.5-60 s, where 90 s is
#: several missed samples. Applied unchanged to a 15-minute poll it fires long
#: before the first sample is even due, so the monitor would declare a perfectly
#: healthy stream down on every heartbeat and fill the ledger with gaps that
#: never happened — which costs more than missing a real one, because a reader
#: who learns the gap ledger cries wolf stops refusing backtests over it.
#:
#: 1.5 rather than 1.0 for the reason `IntegrityConfig` demands slack over the
#: heartbeat: a poll landing one scheduling jitter late is not an outage.
SILENCE_CADENCE_FACTOR = 1.5


class EventSink(Protocol):
    """The narrow seam to `core/eventlog.py`.

    Deliberately one method. Integrity has no business knowing about sequence
    numbers, accounts or storage, and the event log has no business knowing how
    a gap was detected.
    """

    async def emit(self, event_type: EventType, payload: dict[str, Any]) -> None: ...


#: Called when a stream must be torn down and resubscribed. The monitor never
#: reconnects anything itself; it only knows that the data stopped.
ReconnectHook = Callable[[str, str | None], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class StreamKey:
    stream: str
    asset: str | None = None  # None for venue-wide streams with no per-asset split


@dataclass(slots=True)
class StreamState:
    key: StreamKey
    cadence_s: float | None
    last_message_ns: int
    message_count: int = 0
    up: bool = True
    #: Set while down, so recovery knows what to blame the gap on.
    down_reason: str | None = None
    down_detail: str = ""
    down_since_ns: int | None = None

    @property
    def is_regular(self) -> bool:
        return self.cadence_s is not None

    def silent_for_ns(self, now_ns: int) -> int:
        return now_ns - self.last_message_ns


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Answer to "may I run over this window?".

    `covered` is the whole question for the backtest engine; the rest is for the
    message it prints when it refuses. `gaps` are clipped to the requested
    window so the caller reports the hole it actually cares about rather than
    its full historical extent.
    """

    stream: str
    asset: str | None
    from_ns: int
    to_ns: int
    gaps: tuple[DataGap, ...] = ()

    @property
    def covered(self) -> bool:
        return not self.gaps

    @property
    def missing_ns(self) -> int:
        # Gaps are kept non-overlapping by construction, so a plain sum is exact.
        return sum(g.to_ns - g.from_ns for g in self.gaps)

    @property
    def missing_s(self) -> float:
        return self.missing_ns / 1e9

    def __bool__(self) -> bool:
        return self.covered


class StreamMonitor:
    """Per-(stream, asset) liveness and the gap ledger that falls out of it."""

    def __init__(
        self,
        clock: Clock,
        sink: EventSink,
        *,
        max_silence_seconds: float = 90.0,
        heartbeat_seconds: float = 30.0,
        cadences: dict[str, float | None] | None = None,
    ) -> None:
        self._clock = clock
        self._sink = sink
        self._max_silence_ns = int(max_silence_seconds * NS_PER_S)
        self._heartbeat_seconds = heartbeat_seconds
        self._cadences = dict(cadences or DEFAULT_CADENCE_S)
        self._states: dict[StreamKey, StreamState] = {}
        self._gaps: list[DataGap] = []

    # -- registration ----------------------------------------------------

    def register(self, stream: str, asset: str | None = None) -> StreamState:
        """Start watching a stream. Idempotent, so a resubscribe is harmless."""
        if stream not in STREAMS:
            raise ValueError(f"unknown stream {stream!r}; expected one of {sorted(STREAMS)}")
        key = StreamKey(stream, asset)
        state = self._states.get(key)
        if state is None:
            # A freshly registered stream counts as alive from now: it has had no
            # opportunity to be silent yet, and seeding `last_message_ns` with the
            # current time is what stops startup from looking like a 90s outage.
            state = StreamState(
                key=key,
                cadence_s=self._cadences.get(stream),
                last_message_ns=self._clock.now_ns(),
            )
            self._states[key] = state
        return state

    def state(self, stream: str, asset: str | None = None) -> StreamState | None:
        return self._states.get(StreamKey(stream, asset))

    def is_up(self, stream: str, asset: str | None = None) -> bool:
        state = self._states.get(StreamKey(stream, asset))
        return state is not None and state.up

    @property
    def gaps(self) -> tuple[DataGap, ...]:
        return tuple(self._gaps)

    def load_gaps(self, gaps: Iterable[DataGap]) -> None:
        """Hydrate the ledger from the event log.

        A monitor only witnesses the gaps of its own process, while a backtest
        window spans months and many recorder restarts. `coverage` is therefore
        only as complete as what has been loaded here — the caller owns reading
        `DATA_GAP` events out of the event log.
        """
        self._gaps.extend(gaps)
        self._gaps.sort(key=lambda g: (g.stream, g.asset or "", g.from_ns))

    # -- ingestion -------------------------------------------------------

    async def record_message(
        self, stream: str, asset: str | None = None, ts_ns: int | None = None
    ) -> None:
        """Note a message. The only thing that proves a stream alive.

        `ts_ns` is our ingest time, not the venue's event time: a gap describes
        when *we* were blind, and a venue replaying stale timestamps after a
        reconnect must not be able to shrink it.
        """
        state = self.register(stream, asset)
        now_ns = ts_ns if ts_ns is not None else self._clock.now_ns()
        state.message_count += 1
        if not state.up:
            await self._mark_up(state, now_ns)
        state.last_message_ns = now_ns

    async def record_disconnect(
        self, stream: str, asset: str | None = None, detail: str = ""
    ) -> None:
        await self._mark_down(stream, asset, "disconnect", detail)

    async def record_error(self, stream: str, asset: str | None = None, detail: str = "") -> None:
        await self._mark_down(stream, asset, "error", detail)

    async def record_backpressure(
        self, stream: str, asset: str | None = None, detail: str = ""
    ) -> None:
        """Writer could not keep up, so records were dropped at our end.

        Our own fault rather than the venue's, but identically unrecoverable,
        and the distinct reason is what makes it visible in a post-mortem.
        """
        await self._mark_down(stream, asset, "backpressure", detail)

    # -- silence detection ------------------------------------------------

    def silence_limit_ns(self, state: StreamState) -> int:
        """How long this stream may be quiet before it is called down.

        Per-stream rather than one global number, because the streams differ by
        three orders of magnitude in cadence. See `SILENCE_CADENCE_FACTOR`.
        """
        if state.cadence_s is None:
            return self._max_silence_ns
        return max(self._max_silence_ns, int(state.cadence_s * SILENCE_CADENCE_FACTOR * NS_PER_S))

    async def check(self, now_ns: int | None = None) -> tuple[StreamKey, ...]:
        """Declare over-silent regular streams down. Returns what needs a reconnect.

        Includes streams already down for transport reasons: a socket that
        closed still needs resubscribing, and the caller should not have to
        track that separately.
        """
        now_ns = now_ns if now_ns is not None else self._clock.now_ns()
        needs_reconnect: list[StreamKey] = []
        for state in self._states.values():
            if state.up:
                # Irregular streams are skipped here and nowhere else: this is the
                # single branch that keeps a quiet market from being logged as an
                # outage. Their liveness comes from the transport, which is why the
                # supervision loop pairs with the venue's own ws heartbeat.
                if not state.is_regular:
                    continue
                limit_ns = self.silence_limit_ns(state)
                if state.silent_for_ns(now_ns) < limit_ns:
                    continue
                await self._mark_down(
                    state.key.stream,
                    state.key.asset,
                    "silence",
                    f"no message for {state.silent_for_ns(now_ns) / 1e9:.3f}s "
                    f"(limit {limit_ns / 1e9:.3f}s)",
                    now_ns=now_ns,
                )
            needs_reconnect.append(state.key)
        return tuple(needs_reconnect)

    # -- shutdown ---------------------------------------------------------

    async def shutdown(self, detail: str = "clean shutdown") -> tuple[DataGap, ...]:
        """Close the books on a deliberate stop.

        The hole is real whether or not we intended it, so it is written with
        reason `shutdown`: a reader can then distinguish maintenance from
        failure without either being invisible. The gap ends now; the next
        process's first message does not extend it, because that process has no
        way to know this one's last message time.
        """
        now_ns = self._clock.now_ns()
        closed: list[DataGap] = []
        for state in list(self._states.values()):
            if state.up:
                gap = await self._emit_gap(state, state.last_message_ns, now_ns, "shutdown", detail)
                await self._sink.emit(
                    EventType.STREAM_DOWN,
                    {
                        "stream": state.key.stream,
                        "asset": state.key.asset,
                        "reason": "shutdown",
                        "detail": detail,
                        "last_message_ns": state.last_message_ns,
                        "message_count": state.message_count,
                    },
                )
                state.up = False
                state.down_reason = "shutdown"
                state.down_detail = detail
                state.down_since_ns = now_ns
            else:
                # Already down for another cause; that gap stays open and keeps its
                # original reason rather than being relabelled as a shutdown.
                gap = await self._emit_gap(
                    state,
                    state.last_message_ns,
                    now_ns,
                    state.down_reason or "error",
                    state.down_detail,
                )
            closed.append(gap)
        return tuple(closed)

    # -- supervision ------------------------------------------------------

    async def supervise(
        self,
        on_reconnect: ReconnectHook | None = None,
        *,
        stop: asyncio.Event | None = None,
        max_ticks: int | None = None,
    ) -> None:
        """Heartbeat loop for `recorder/main.py`.

        Driven by the `Clock`, so a SimClock test runs it instantly and in data
        order. `max_ticks` bounds the loop for exactly that case: under a
        SimClock `sleep` never yields to the event loop, so a test needs a
        termination condition that does not depend on wall time.
        """
        ticks = 0
        while not (stop is not None and stop.is_set()):
            await self._clock.sleep(self._heartbeat_seconds)
            for key in await self.check():
                if on_reconnect is not None:
                    await on_reconnect(key.stream, key.asset)
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                return

    # -- coverage ---------------------------------------------------------

    def coverage(self, stream: str, asset: str | None, from_ns: int, to_ns: int) -> CoverageReport:
        """What the backtest engine calls before it agrees to run.

        Signature is shaped for that caller: it holds a stream, the instrument
        it intends to trade and a window, and it wants one boolean plus enough
        detail to explain a refusal. Venue-wide gaps (`asset is None`) count
        against every asset of that stream, since nothing was recorded for any
        of them.
        """
        if to_ns < from_ns:
            raise ValueError(f"empty range: {from_ns} -> {to_ns}")

        clipped: list[DataGap] = []
        for gap in self._candidate_gaps(stream, asset):
            lo, hi = max(gap.from_ns, from_ns), min(gap.to_ns, to_ns)
            if hi <= lo:
                continue
            clipped.append(gap.model_copy(update={"from_ns": lo, "to_ns": hi}))
        clipped.sort(key=lambda g: g.from_ns)
        return CoverageReport(
            stream=stream, asset=asset, from_ns=from_ns, to_ns=to_ns, gaps=tuple(clipped)
        )

    def _candidate_gaps(self, stream: str, asset: str | None) -> Sequence[DataGap]:
        out = [
            g
            for g in self._gaps
            if g.stream == stream and (g.asset is None or asset is None or g.asset == asset)
        ]
        # A stream that is down right now has an open-ended hole that has not been
        # written yet. Omitting it would let a run start over data we are currently
        # failing to record.
        now_ns = self._clock.now_ns()
        for state in self._states.values():
            if state.up or state.key.stream != stream:
                continue
            if not (state.key.asset is None or asset is None or state.key.asset == asset):
                continue
            out.append(
                DataGap(
                    stream=stream,
                    asset=state.key.asset,
                    from_ns=state.last_message_ns,
                    to_ns=now_ns,
                    reason=_as_reason(state.down_reason),
                    detail=state.down_detail or "stream currently down",
                )
            )
        return out

    # -- internals --------------------------------------------------------

    async def _mark_down(
        self,
        stream: str,
        asset: str | None,
        reason: str,
        detail: str,
        *,
        now_ns: int | None = None,
    ) -> None:
        state = self.register(stream, asset)
        if not state.up:
            return  # already accounted for; the first cause is the true one
        now_ns = now_ns if now_ns is not None else self._clock.now_ns()
        state.up = False
        state.down_reason = reason
        state.down_detail = detail
        state.down_since_ns = now_ns
        await self._sink.emit(
            EventType.STREAM_DOWN,
            {
                "stream": stream,
                "asset": asset,
                "reason": reason,
                "detail": detail,
                "last_message_ns": state.last_message_ns,
                "silent_for_s": state.silent_for_ns(now_ns) / 1e9,
                "message_count": state.message_count,
            },
        )

    async def _mark_up(self, state: StreamState, now_ns: int) -> None:
        reason = _as_reason(state.down_reason)
        gap = await self._emit_gap(state, state.last_message_ns, now_ns, reason, state.down_detail)
        await self._sink.emit(
            EventType.STREAM_UP,
            {
                "stream": state.key.stream,
                "asset": state.key.asset,
                "reason": reason,
                "gap_from_ns": gap.from_ns,
                "gap_to_ns": gap.to_ns,
                "gap_duration_s": gap.duration_s,
            },
        )
        state.up = True
        state.down_reason = None
        state.down_detail = ""
        state.down_since_ns = None

    async def _emit_gap(
        self, state: StreamState, from_ns: int, to_ns: int, reason: str, detail: str
    ) -> DataGap:
        gap = DataGap(
            stream=state.key.stream,
            asset=state.key.asset,
            from_ns=from_ns,
            to_ns=to_ns,
            reason=_as_reason(reason),
            detail=detail,
        )
        self._gaps.append(gap)
        await self._sink.emit(EventType.DATA_GAP, gap.model_dump())
        return gap


def _as_reason(reason: str | None) -> Any:
    # A stream can only come back from being down, so a missing reason means the
    # bookkeeping itself failed. Recording that as `error` is honest; guessing
    # `disconnect` would invent a cause.
    return reason or "error"
