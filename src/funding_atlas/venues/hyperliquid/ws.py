"""Hyperliquid WebSocket market-data feed.

A `MarketFeed` for a process that is expected to stay up for months. Everything
unusual in here follows from that: the venue's published caps are treated as
hard constraints rather than advice, every way of losing data emits an event,
and no code path can lose the stream because of one surprising message.

The four facts from `docs/hyperliquid-api.md` that shape this module:

* **The 60 s server timeout is on messages *we* send** (docs §1). A connection
  where our ping loop is healthy but the venue has stopped publishing stays
  open and reports nothing. Socket state is therefore not evidence of liveness;
  inbound silence is tracked separately, per stream, and reported through the
  sink. This is the failure mode `integrity.max_silence_seconds` exists for.

* **30 new connections per minute, per IP** (docs §5). Reconnect backoff is
  derived from that number instead of a guessed constant, and a rolling-window
  budget makes the cap unbreachable even if the backoff maths is wrong. Getting
  locked out happens exactly when reconnecting matters most.

* **1000 subscriptions per IP** (docs §5). Checked against the planned
  subscription set *before the first connect*, so an over-wide config fails at
  startup with a message naming the numbers rather than halfway through
  subscribing, with some assets silently missing.

* **Nothing is resubscribed by the server** (docs §1). Every reconnect replays
  the whole set, and the window between the last message and a completed
  resubscribe is written as a gap.

Backpressure is the one data-loss path that is our own fault rather than the
venue's: the queue is bounded, and a drop emits a `DATA_GAP` with reason
`backpressure`. An unbounded queue would convert a slow writer into an
out-of-memory kill, which loses the same data and the process with it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections import Counter, deque
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

from funding_atlas.core.clock import Clock
from funding_atlas.core.events import DataGap, EventType
from funding_atlas.venues.base import AssetSpec
from funding_atlas.venues.hyperliquid import codec
from funding_atlas.venues.records import BookSnapshot, Mid, Trade

if TYPE_CHECKING:
    from funding_atlas.core.config import RecorderConfig
    from funding_atlas.recorder.integrity import EventSink

__all__ = [
    "BACKOFF_CEILING_S",
    "BACKOFF_FLOOR_S",
    "MAX_NEW_CONNECTIONS_PER_MINUTE",
    "MAX_SUBSCRIPTIONS_PER_IP",
    "MIN_CONNECT_SPACING_S",
    "PING_INTERVAL_S",
    "WS_URLS",
    "HyperliquidFeed",
    "SubscriptionLimitError",
]

NS_PER_S: Final = 1_000_000_000

WS_URLS: Final[dict[str, str]] = {
    "mainnet": "wss://api.hyperliquid.xyz/ws",
    "testnet": "wss://api.hyperliquid-testnet.xyz/ws",
}

#: Application-level ping period. The server closes a connection it has not
#: *received* on for 60 s (docs §1); the official SDK uses 50 s and so do we. A
#: 60 s period would race the cutoff it is meant to avoid.
PING_INTERVAL_S: Final = 50.0

#: docs §5, WebSocket limits, per IP.
MAX_SUBSCRIPTIONS_PER_IP: Final = 1000
MAX_NEW_CONNECTIONS_PER_MINUTE: Final = 30
CONNECTION_WINDOW_S: Final = 60.0

#: Minimum *average* spacing between connection attempts that the cap permits:
#: 60 s / 30 = 2 s. Everything about the backoff is derived from this rather
#: than from a round number that happens to look safe.
MIN_CONNECT_SPACING_S: Final = CONNECTION_WINDOW_S / MAX_NEW_CONNECTIONS_PER_MINUTE

#: Full-jitter backoff sleeps `uniform(0, window)`, whose *mean* is `window / 2`.
#: To keep the long-run attempt rate inside the cap, the smallest window must
#: therefore be twice the minimum spacing: 2 x 2 s = 4 s, mean 2 s. Using 2 s as
#: the window would halve that to a mean of 1 s and sustain 60 attempts a
#: minute — double the cap — while looking like a correct jittered backoff.
BACKOFF_FLOOR_S: Final = 2.0 * MIN_CONNECT_SPACING_S

#: Upper bound on the window. A venue-side outage should be retried about once a
#: minute forever, not with hour-long sleeps that turn a 5-minute outage into a
#: 40-minute gap.
BACKOFF_CEILING_S: Final = 60.0

#: How often the sink is told about a backpressure drop while it continues. The
#: drops are all counted; only the eventing is coalesced, because a saturated
#: queue would otherwise emit thousands of events per second into the very
#: writer that is already behind.
_DROP_REPORT_INTERVAL_S: Final = 1.0

#: Streams whose silence is evidence of failure. `trades` is excluded on
#: purpose: it is market-driven, and a quiet hour on a thin perp would otherwise
#: be logged as an outage and trigger a reconnect that drops the subscriptions
#: which *are* working. Same rule as `recorder/integrity.py`.
_REGULAR_STREAMS: Final[frozenset[str]] = frozenset({"mids", "l2"})

MarketRecord = Trade | Mid | BookSnapshot


class SubscriptionLimitError(Exception):
    """The configured subscription set cannot fit in the venue's per-IP cap."""


class AssetSpecSource(Protocol):
    """The only part of `HyperliquidRest` this feed is allowed to know about.

    Narrow by design: `rest.py` is written independently, and a one-method
    surface means the two can be developed and tested against each other
    without either importing the other's internals.
    """

    async def asset_specs(self) -> dict[str, AssetSpec]: ...


class WsConnection(Protocol):
    """A connected socket, as much of `websockets` as we use.

    Extracted so the tests can drive the full reconnect, resubscribe, ping and
    backpressure logic from a fake transport with no network and no sleeping.
    """

    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


#: Takes a URL, returns a connected socket.
Connector = Callable[[str], Awaitable[WsConnection]]


@dataclass(frozen=True, slots=True)
class _Stop:
    """Sentinel: the connection task finished. `error` is None on a clean stop."""

    error: BaseException | None = None


class _ConnectionBudget:
    """Hard guard on the 30-new-connections-per-minute cap.

    The jittered backoff already keeps the *expected* rate inside the cap, but
    an expectation is not a guarantee: a run of small jitter draws, or a caller
    that recreates feeds, can still cluster attempts. This counts actual
    attempts in a rolling 60 s window and refuses to exceed them, so the cap
    holds regardless of how the backoff behaves. Being slow to reconnect costs
    a bounded gap; being locked out costs an unbounded one.
    """

    __slots__ = ("_attempts", "_clock", "_limit", "_window_ns")

    def __init__(
        self,
        clock: Clock,
        *,
        limit: int = MAX_NEW_CONNECTIONS_PER_MINUTE,
        window_s: float = CONNECTION_WINDOW_S,
    ) -> None:
        self._clock = clock
        self._limit = limit
        self._window_ns = int(window_s * NS_PER_S)
        self._attempts: deque[int] = deque()

    def wait_s(self, now_ns: int) -> float:
        """Seconds to wait before another attempt is within the cap."""
        self._expire(now_ns)
        if len(self._attempts) < self._limit:
            return 0.0
        # The oldest attempt in the window is the one that has to age out.
        return max(0.0, (self._attempts[0] + self._window_ns - now_ns) / NS_PER_S)

    async def acquire(self) -> None:
        while True:
            now_ns = self._clock.now_ns()
            wait = self.wait_s(now_ns)
            if wait <= 0.0:
                self._attempts.append(now_ns)
                return
            await self._clock.sleep(wait)

    def _expire(self, now_ns: int) -> None:
        cutoff = now_ns - self._window_ns
        while self._attempts and self._attempts[0] <= cutoff:
            self._attempts.popleft()


class HyperliquidFeed:
    """`MarketFeed` over the Hyperliquid WebSocket API.

    Single-consumer: `stream()` may be iterated once. Two consumers of one
    socket would each see an arbitrary half of the tape, which is worse than an
    error because it looks like data.
    """

    def __init__(
        self,
        config: RecorderConfig,
        clock: Clock,
        rest: AssetSpecSource,
        sink: EventSink,
        *,
        connect: Connector | None = None,
        max_queue: int = 10_000,
        rng: random.Random | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._rest = rest
        self._sink = sink
        self._connect = connect if connect is not None else _websockets_connector
        # Not a cryptographic use: this randomness only spreads reconnect
        # attempts, and it is injectable so tests can pin the draw.
        self._rng = rng if rng is not None else random.Random()
        self._url = WS_URLS[config.network]

        self._budget = _ConnectionBudget(clock)
        # Bounded on purpose; see the module docstring. `max_queue` is records,
        # not bytes, and 10k is a few seconds of the whole mainnet tape.
        self._queue: asyncio.Queue[MarketRecord | _Stop] = asyncio.Queue(maxsize=max_queue)

        self._universe: frozenset[str] | None = None
        self._conn: WsConnection | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False
        self._closing = False

        # -- observability counters, read by the recorder's status line --
        self.connects = 0
        self.disconnects = 0
        self.messages = 0
        self.dropped = 0
        self.undecodable: Counter[str] = Counter()

        self._last_inbound_ns = clock.now_ns()
        self._last_stream_ns: dict[tuple[str, str | None], int] = {}
        #: Last record per stream the consumer actually received. A gap starts
        #: here, not at the moment the queue filled: `_last_stream_ns` moves on
        #: every inbound message, including the ones we then failed to hand on.
        self._last_enqueued_ns: dict[tuple[str, str | None], int] = {}
        self._down: set[tuple[str, str | None]] = set()
        self._reported_undecodable: set[tuple[str | None, str]] = set()
        self._last_drop_report_ns = 0
        self._last_emit_ns: dict[tuple[str, str], int] = {}

    # -- MarketFeed -------------------------------------------------------

    async def specs(self) -> dict[str, AssetSpec]:
        """Trading rules. Delegated: `meta` is a REST concern and the WS socket
        has no business holding the authoritative copy."""
        return await self._rest.asset_specs()

    def stream(self) -> AsyncIterator[MarketRecord]:
        return self._iterate()

    # -- startup ----------------------------------------------------------

    async def _resolve_universe(self) -> frozenset[str]:
        """Assets to record. `all_perps` means whatever the venue lists now.

        Cached for the life of the feed because it is also the `allMids` filter,
        and docs §8 warns that `allMids` carries ~1300 keys of which only the
        plain-named ones are perps. Filtering is by membership in this set, not
        by key shape.
        """
        if self._universe is not None:
            return self._universe
        if self._config.universe.mode == "explicit":
            self._universe = frozenset(self._config.universe.explicit)
        else:
            self._universe = frozenset(await self.specs())
        return self._universe

    def plan_subscriptions(self, assets: Collection[str]) -> tuple[dict[str, Any], ...]:
        """The exact subscription set, checked against the per-IP cap.

        Raises `SubscriptionLimitError` rather than truncating. A truncated set
        records a subset of the configured universe while reporting healthy,
        which is the failure mode this whole module is built to avoid.

        Funding and open interest are absent by design: `activeAssetCtx` is
        per-coin (docs §2), so under `all_perps` it alone would be ~190
        subscriptions, and `metaAndAssetCtxs` returns the same data for every
        asset in one REST call. `config/recorder.yaml` polls it instead.
        """
        ordered = sorted(assets)
        streams = self._config.streams
        subs: list[dict[str, Any]] = []

        if streams.trades.enabled:
            subs.extend({"type": "trades", "coin": asset} for asset in ordered)

        if streams.l2.enabled:
            # Intersected with the universe: an l2 asset outside it would be a
            # subscription whose records are filtered away downstream, i.e.
            # paid-for bandwidth producing nothing.
            wanted = [a for a in sorted(streams.l2.assets) if a in set(ordered)]
            # All three of `nSigFigs`, `mantissa` and `fast` stay unset: the
            # default is full precision and 20 levels per side, which is what
            # `depth: 20` asks for. `nSigFigs` would coarsen recorded prices
            # irreversibly (docs §2).
            subs.extend({"type": "l2Book", "coin": asset} for asset in wanted)

        if streams.mids.enabled:
            # One subscription for every asset on the venue. `dex` is left unset
            # so it covers the first perp dex, which is the recorded universe.
            subs.append({"type": "allMids"})

        if len(subs) > MAX_SUBSCRIPTIONS_PER_IP:
            raise SubscriptionLimitError(
                f"configuration needs {len(subs)} websocket subscriptions "
                f"({sum(1 for s in subs if s['type'] == 'trades')} trades + "
                f"{sum(1 for s in subs if s['type'] == 'l2Book')} l2Book + "
                f"{sum(1 for s in subs if s['type'] == 'allMids')} allMids) but Hyperliquid "
                f"allows {MAX_SUBSCRIPTIONS_PER_IP} per IP; reduce universe.explicit or "
                "streams.l2.assets, or split the recorder across hosts"
            )
        if not subs:
            raise SubscriptionLimitError(
                "no websocket subscriptions result from this configuration; the feed would "
                "connect and record nothing while reporting healthy"
            )
        return tuple(subs)

    # -- consumer side ----------------------------------------------------

    async def _iterate(self) -> AsyncIterator[MarketRecord]:
        if self._started:
            raise RuntimeError("HyperliquidFeed.stream() is single-consumer; iterate it once")
        self._started = True

        # Resolved and validated before anything is connected, so a config that
        # cannot work fails here — at startup, to the caller — rather than
        # mid-run with part of the universe missing.
        assets = await self._resolve_universe()
        subs = self.plan_subscriptions(assets)

        self._tasks.append(asyncio.create_task(self._run(subs), name="hl-ws-connection"))
        self._tasks.append(asyncio.create_task(self._silence_loop(), name="hl-ws-silence"))
        try:
            while True:
                item = await self._queue.get()
                if isinstance(item, _Stop):
                    if item.error is not None:
                        raise item.error
                    return
                yield item
        finally:
            await self.close()

    async def close(self) -> None:
        """Stop the socket and the supervision tasks. Safe to call twice."""
        if self._closing:
            return
        self._closing = True
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        await self._close_conn()
        # A deliberate stop leaves the same hole in history as a crash, so it is
        # recorded as one. Hiding it because we meant to do it is how a dataset
        # acquires invisible gaps.
        await self._emit(
            EventType.STREAM_DOWN,
            {
                "stream": "all",
                "asset": None,
                "reason": "shutdown",
                "detail": "feed closed",
                "last_message_ns": self._last_inbound_ns,
                "messages": self.messages,
                "dropped": self.dropped,
            },
        )

    # -- connection lifecycle ---------------------------------------------

    async def _run(self, subs: tuple[dict[str, Any], ...]) -> None:
        attempt = 0
        try:
            while not self._closing:
                # The budget guard comes before the connect, not after a
                # failure: a *successful* connection counts against the cap too.
                await self._budget.acquire()
                productive = False
                try:
                    conn = await self._connect(self._url)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - every failure is a gap, not a crash
                    await self._report_down("error", f"connect failed: {type(exc).__name__}: {exc}")
                else:
                    self._conn = conn
                    self.connects += 1
                    try:
                        productive = await self._session(conn, subs)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        self.disconnects += 1
                        await self._report_down(
                            "disconnect", f"session ended: {type(exc).__name__}: {exc}"
                        )
                    else:
                        self.disconnects += 1
                        await self._report_down("disconnect", "socket closed by peer")
                    finally:
                        await self._close_conn()

                if self._closing:
                    break
                # Backoff is reset only by a connection that actually delivered
                # data. A socket that accepts us and closes immediately would
                # otherwise reset the backoff on every cycle and hammer the
                # 30/minute cap at full speed — which is precisely the state in
                # which the venue is least willing to let us back in.
                attempt = 1 if productive else attempt + 1
                await self._clock.sleep(self.backoff_delay(attempt))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - hand it to the consumer, do not vanish
            await self._put(_Stop(exc))
            return
        await self._put(_Stop())

    def backoff_window_s(self, attempt: int) -> float:
        """Upper bound of the jitter window for `attempt` (1-based)."""
        if attempt <= 0:
            return 0.0
        return min(BACKOFF_CEILING_S, BACKOFF_FLOOR_S * (2 ** (attempt - 1)))

    def backoff_delay(self, attempt: int) -> float:
        """Full jitter: `uniform(0, window)`.

        Full rather than equal or decorrelated jitter because several recorder
        processes behind one IP share the 30/minute cap, and full jitter is the
        variant that spreads a synchronised thundering herd widest. The floor
        inside `backoff_window_s` is what keeps its *mean* inside the cap.
        """
        return self._rng.uniform(0.0, self.backoff_window_s(attempt))

    async def _session(self, conn: WsConnection, subs: tuple[dict[str, Any], ...]) -> bool:
        """Subscribe, then read until the socket ends. True if data arrived.

        Resubscription is unconditional on every connection: the server restores
        nothing (docs §1), and the subscription set is replayed in full rather
        than diffed against what we think the server has.
        """
        for sub in subs:
            await conn.send(json.dumps({"method": "subscribe", "subscription": sub}))
        await self._emit(
            EventType.STREAM_UP,
            {
                "stream": "all",
                "asset": None,
                "reason": "resubscribed",
                "subscriptions": len(subs),
                "last_message_ns": self._last_inbound_ns,
                "gap_from_ns": self._last_inbound_ns,
                "gap_to_ns": self._clock.now_ns(),
            },
        )
        # Seeded after the subscribes so the silence monitor does not count the
        # connect handshake against the first message's deadline.
        self._last_inbound_ns = self._clock.now_ns()
        self._down.clear()

        ping = asyncio.create_task(self._ping_loop(conn), name="hl-ws-ping")
        received = 0
        try:
            while not self._closing:
                raw = await conn.recv()
                if raw is None:  # a fake or a transport signalling end-of-stream
                    break
                received += 1
                await self._handle(raw)
        finally:
            ping.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await ping
        return received > 0

    async def _ping_loop(self, conn: WsConnection) -> None:
        """`{"method":"ping"}` every 50 s (docs §1).

        Note what this does *not* prove. The venue's 60 s timeout is on traffic
        it receives, so a faithful ping keeps the socket open even when no market
        data is coming back. Liveness is `_silence_loop`'s job; this loop only
        stops the server hanging up on us.
        """
        while not self._closing:
            await self._clock.sleep(PING_INTERVAL_S)
            if self._closing:
                return
            try:
                await conn.send(json.dumps({"method": "ping"}))
            except Exception as exc:  # noqa: BLE001
                # A failed ping means this socket is finished; let the read loop
                # discover it by closing, rather than reconnecting from here and
                # racing it.
                await self._report_down("error", f"ping failed: {type(exc).__name__}: {exc}")
                with contextlib.suppress(Exception):
                    await conn.close()
                return

    async def _close_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()

    # -- inbound ----------------------------------------------------------

    async def _handle(self, raw: str | bytes) -> None:
        now_ns = self._clock.now_ns()
        self._last_inbound_ns = now_ns
        outcome = codec.decode_message(
            raw,
            venue=self._config.venue,
            clock=self._clock,
            assets=self._universe,
        )
        if isinstance(outcome, codec.Control):
            # Protocol traffic proves the socket is alive but not that any
            # subscription is: it must not refresh a per-stream clock.
            return
        if isinstance(outcome, codec.Undecodable):
            await self._report_undecodable(outcome)
            return

        self.messages += 1
        for record in outcome.records:
            stream = codec.stream_of(record)
            if not isinstance(record, (Trade, Mid, BookSnapshot)):
                # Funding and open interest come from the REST poller, not this
                # socket (see `plan_subscriptions`). Reaching here means we are
                # subscribed to something we did not plan, which is a bug worth
                # counting rather than a record worth yielding out of a stream
                # whose contract says Trade | Mid | BookSnapshot.
                self.undecodable["unexpected_record"] += 1
                continue
            asset = getattr(record, "asset", None)
            self._last_stream_ns[(stream, asset)] = now_ns
            # Data arriving is the only thing that clears a down flag, so a
            # recovered stream can be reported down again if it stalls twice.
            self._down.discard((stream, asset))
            if not self._should_emit(stream, record, now_ns):
                continue
            await self._offer(record, stream, asset, now_ns)

    def _should_emit(self, stream: str, record: Any, now_ns: int) -> bool:
        """Config-driven downsampling of the regular streams.

        `mids.sample_seconds` and `l2.sample_ms` are the recorded cadence, and
        the venue pushes faster than both (`l2Book` fires per block). Dropping
        here is intentional sampling, not loss, so it is deliberately *not*
        reported as a gap — unlike a backpressure drop, which is.

        `trades` is never sampled: every fill is a distinct event and a sampled
        tape is not a tape.
        """
        if stream == "mids":
            interval_ns = self._config.streams.mids.sample_seconds * NS_PER_S
        elif stream == "l2":
            interval_ns = self._config.streams.l2.sample_ms * 1_000_000
        else:
            return True
        if interval_ns <= 0:
            return True
        key = (stream, str(getattr(record, "asset", "")))
        last = self._last_emit_ns.get(key)
        if last is not None and now_ns - last < interval_ns:
            return False
        self._last_emit_ns[key] = now_ns
        return True

    async def _offer(
        self, record: MarketRecord, stream: str, asset: str | None, now_ns: int
    ) -> None:
        """Enqueue, or count and report a drop. Never blocks the read loop.

        Blocking here would stall every other subscription on the socket behind
        the slowest one and then let the venue's own 60 s timeout kill the
        connection, turning one slow writer into a total outage. Dropping the
        newest record keeps the oldest, so the consumer sees a contiguous prefix
        of the stream with an explicit gap at the end rather than a shuffled mix.
        """
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self.dropped += 1
            await self._report_drop(stream, asset, now_ns)
        else:
            self._last_enqueued_ns[(stream, asset)] = now_ns

    async def _put(self, item: MarketRecord | _Stop) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            # A sentinel must never be dropped, or the consumer waits forever on
            # a feed that has already stopped.
            await self._queue.put(item)

    # -- liveness ---------------------------------------------------------

    async def _silence_loop(self) -> None:
        """Inbound-silence watchdog; the counterpart to the ping loop.

        Evaluated per (stream, asset), not per connection: a dead `l2Book` on a
        socket whose `trades` is flowing trips no connection-level check (docs
        §8). Only the regular streams are judged — see `_REGULAR_STREAMS`.
        """
        heartbeat = float(self._config.integrity.heartbeat_seconds)
        max_silence_ns = int(self._config.integrity.max_silence_seconds * NS_PER_S)
        while not self._closing:
            await self._clock.sleep(heartbeat)
            if self._closing:
                return
            await self.check_silence(max_silence_ns)

    async def check_silence(self, max_silence_ns: int | None = None) -> bool:
        """Report over-silent streams. True if the socket was torn down.

        Reconnecting is the only remedy available: the venue offers no way to
        re-ask for one subscription's flow, and a socket that is silent on a
        stream we are subscribed to has already failed in a way it will not
        report.
        """
        if max_silence_ns is None:
            max_silence_ns = int(self._config.integrity.max_silence_seconds * NS_PER_S)
        now_ns = self._clock.now_ns()
        silent: list[tuple[str, str | None]] = []

        for key, last_ns in self._last_stream_ns.items():
            stream, _asset = key
            if stream not in _REGULAR_STREAMS or key in self._down:
                continue
            if now_ns - last_ns >= max_silence_ns:
                silent.append(key)

        # A connection that has delivered nothing at all since subscribing has
        # no per-stream entries yet, so it needs its own check or a socket that
        # goes mute immediately would never be noticed.
        whole_socket_mute = (
            self._conn is not None and now_ns - self._last_inbound_ns >= max_silence_ns
        )

        for stream, asset in silent:
            self._down.add((stream, asset))
            await self._emit(
                EventType.STREAM_DOWN,
                {
                    "stream": stream,
                    "asset": asset,
                    "reason": "silence",
                    "detail": (
                        f"no inbound message for "
                        f"{(now_ns - self._last_stream_ns[(stream, asset)]) / 1e9:.3f}s while "
                        "the socket stayed open and our pings were accepted"
                    ),
                    "last_message_ns": self._last_stream_ns[(stream, asset)],
                },
            )
        if whole_socket_mute:
            await self._emit(
                EventType.STREAM_DOWN,
                {
                    "stream": "all",
                    "asset": None,
                    "reason": "silence",
                    "detail": (
                        f"no inbound message on the connection for "
                        f"{(now_ns - self._last_inbound_ns) / 1e9:.3f}s"
                    ),
                    "last_message_ns": self._last_inbound_ns,
                },
            )

        if silent or whole_socket_mute:
            # Closing hands control back to `_run`, which resubscribes
            # everything. Nothing here reconnects directly: two reconnect paths
            # race each other and burn the connection cap twice as fast.
            await self._close_conn()
            return True
        return False

    # -- reporting --------------------------------------------------------

    async def _report_down(self, reason: str, detail: str) -> None:
        self._down.add(("all", None))
        await self._emit(
            EventType.STREAM_DOWN,
            {
                "stream": "all",
                "asset": None,
                "reason": reason,
                "detail": detail,
                "last_message_ns": self._last_inbound_ns,
                "connects": self.connects,
                "disconnects": self.disconnects,
            },
        )

    async def _report_drop(self, stream: str, asset: str | None, now_ns: int) -> None:
        if now_ns - self._last_drop_report_ns < _DROP_REPORT_INTERVAL_S * NS_PER_S:
            return  # still counted in `self.dropped`; only the eventing coalesces
        self._last_drop_report_ns = now_ns
        gap = DataGap(
            stream=stream,
            asset=asset,
            # The hole starts at the last record the consumer actually got for
            # this stream, not at the moment the queue filled: everything after
            # that point is what we failed to hand on.
            from_ns=self._last_enqueued_ns.get((stream, asset), now_ns),
            to_ns=now_ns,
            reason="backpressure",
            detail=(
                f"consumer slower than the socket; {self.dropped} record(s) dropped from a "
                f"bounded queue of {self._queue.maxsize}"
            ),
        )
        await self._emit(EventType.DATA_GAP, gap.model_dump())

    async def _report_undecodable(self, outcome: codec.Undecodable) -> None:
        """Count every one, event the first of each kind.

        Both halves matter: the count makes a slow drift visible, and the event
        makes the first occurrence findable in permanent history. Eventing every
        occurrence would let one changed field flood the log at tape rate.
        """
        self.undecodable[str(outcome.reason)] += 1
        key = (outcome.channel, str(outcome.reason))
        if key in self._reported_undecodable:
            return
        self._reported_undecodable.add(key)
        await self._emit(
            EventType.ERROR,
            {
                "source": "hyperliquid.ws.codec",
                "channel": outcome.channel,
                "reason": str(outcome.reason),
                "detail": outcome.detail,
                "raw": outcome.raw,
                "count": self.undecodable[str(outcome.reason)],
            },
        )

    async def _emit(self, event_type: EventType, payload: dict[str, Any]) -> None:
        # The sink is the recorder's event log. A sink that throws must not take
        # the feed down with it: losing the explanation of a gap is bad, losing
        # the data as well is worse.
        try:
            await self._sink.emit(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001, S110
            pass


async def _websockets_connector(url: str) -> WsConnection:
    """Default transport. Imported lazily so the module is importable, and the
    codec and policy testable, without `websockets` installed."""
    import websockets  # deliberately lazy; see the docstring

    # `ping_interval=None`: Hyperliquid's keepalive is the application-level
    # `{"method":"ping"}` message (docs §1), and a WS control-frame ping does
    # not satisfy the 60 s receive timeout. Running both would just add traffic.
    return await websockets.connect(  # type: ignore[return-value]
        url, ping_interval=None, max_size=2**22, open_timeout=10
    )


def iter_subscription_names(subs: Iterable[dict[str, Any]]) -> list[str]:
    """Readable form of a subscription set, for the startup log line."""
    return [f"{s['type']}:{s.get('coin', '*')}" for s in subs]
