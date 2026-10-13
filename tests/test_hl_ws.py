"""Feed tests.

No sockets and no wall-clock sleeps. The transport is a fake that replays
scripted messages, and time comes from `SimClock` subclasses, so every
assertion about *when* the feed does something is exact rather than
approximate. A test that slept would be both slow and flaky about the one
property that matters here — a recorder's behaviour over time.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import random
from decimal import Decimal
from typing import Any

import pytest

# Same captures as the codec tests, imported rather than re-typed: two copies
# of a wire fixture drift, and then one test suite is testing a fiction.
from test_hl_codec import ALL_MIDS_MSG, L2_MSG, TRADES_MSG

from funding_atlas.core.clock import SimClock
from funding_atlas.core.config import RecorderConfig
from funding_atlas.core.events import EventType
from funding_atlas.venues.base import AssetSpec
from funding_atlas.venues.hyperliquid.ws import (
    BACKOFF_CEILING_S,
    BACKOFF_FLOOR_S,
    CONNECTION_WINDOW_S,
    MAX_NEW_CONNECTIONS_PER_MINUTE,
    MAX_SUBSCRIPTIONS_PER_IP,
    MIN_CONNECT_SPACING_S,
    PING_INTERVAL_S,
    HyperliquidFeed,
    SubscriptionLimitError,
    _ConnectionBudget,
)
from funding_atlas.venues.records import BookSnapshot, Mid, Trade

S = 1_000_000_000
T0 = 1_791_900_000 * S


# -- doubles ---------------------------------------------------------------


class RecordingClock(SimClock):
    """SimClock that logs every sleep and either advances or parks forever.

    `advance=False` is how a test freezes the feed's background loops: the ping
    and reconnect sleeps never return, so nothing fires except what the test
    drives explicitly. It is a block, not a wait — no wall time passes.
    """

    def __init__(self, start_ns: int = T0, *, advance: bool = True) -> None:
        super().__init__(start_ns)
        self.sleeps: list[float] = []
        self._advance = advance

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if not self._advance:
            await asyncio.Future()  # parked for the life of the test
        self._now_ns += int(seconds * S)
        await asyncio.sleep(0)  # hand control back without consuming wall time


class MaxJitter(random.Random):
    """Full-jitter draw pinned to its upper bound, so delays are exact."""

    def uniform(self, a: float, b: float) -> float:
        return b


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[tuple[EventType, dict[str, Any]]] = []

    async def emit(self, event_type: EventType, payload: dict[str, Any]) -> None:
        self.events.append((event_type, payload))

    def of(self, event_type: EventType) -> list[dict[str, Any]]:
        return [p for t, p in self.events if t == event_type]


class FakeRest:
    """The one method the feed is allowed to use from `rest.py`."""

    def __init__(self, assets: tuple[str, ...] = ("BTC", "ETH")) -> None:
        self.calls = 0
        self._specs = {
            name: AssetSpec(
                asset=name,
                price_tick=Decimal("0.1"),
                price_sig_figs=5,
                size_lot=Decimal("0.00001"),
                min_notional=Decimal(10),
                max_leverage=Decimal(40),
                is_perp=True,
            )
            for name in assets
        }

    async def asset_specs(self) -> dict[str, AssetSpec]:
        self.calls += 1
        return dict(self._specs)


class FakeConn:
    def __init__(self, script: list[Any]) -> None:
        self.sent: list[str] = []
        self.closed = False
        self._script = list(script)

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | None:
        await asyncio.sleep(0)
        if not self._script:
            return None  # end of stream: the session ends, like a closed socket
        item = self._script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if item is ...:
            await asyncio.Future()  # hold the session open forever
        return item

    async def close(self) -> None:
        self.closed = True

    def subscriptions(self) -> list[dict[str, Any]]:
        return [
            json.loads(m)["subscription"]
            for m in self.sent
            if json.loads(m).get("method") == "subscribe"
        ]


class FakeTransport:
    """Hands out scripted connections, then parks.

    Parking rather than raising on exhaustion keeps the reconnect loop from
    spinning after the scripted part of a test is over.
    """

    def __init__(self, *scripts: list[Any], fail: int = 0) -> None:
        self.scripts = list(scripts)
        self.conns: list[FakeConn] = []
        self.urls: list[str] = []
        self._fail = fail

    async def __call__(self, url: str) -> FakeConn:
        self.urls.append(url)
        if self._fail:
            raise ConnectionRefusedError("refused")
        if not self.scripts:
            await asyncio.Future()
        conn = FakeConn(self.scripts.pop(0))
        self.conns.append(conn)
        return conn


def make_config(**overrides: Any) -> RecorderConfig:
    raw: dict[str, Any] = {
        "venue": "hyperliquid",
        "network": "mainnet",
        "rotation": {"max_rows": 500_000, "max_seconds": 3600},
        "universe": {"mode": "explicit", "explicit": ["BTC", "ETH"]},
        "streams": {
            "funding": {"enabled": True, "poll_seconds": 60},
            "trades": {"enabled": True},
            # Sampling off by default in these tests: a throttle would hide
            # which records the feed actually produced.
            "mids": {"enabled": True, "sample_seconds": 1},
            "l2": {"enabled": True, "assets": ["BTC"], "sample_ms": 500},
            "open_interest": {"enabled": True, "poll_seconds": 60},
        },
        "integrity": {"heartbeat_seconds": 30, "max_silence_seconds": 90},
    }
    raw.update(overrides)
    return RecorderConfig.model_validate(raw)


def make_feed(
    *,
    config: RecorderConfig | None = None,
    clock: SimClock | None = None,
    transport: FakeTransport | None = None,
    rest: FakeRest | None = None,
    max_queue: int = 100,
    rng: random.Random | None = None,
) -> tuple[HyperliquidFeed, RecordingSink, FakeTransport, SimClock]:
    cfg = config if config is not None else make_config()
    the_clock = clock if clock is not None else RecordingClock(advance=False)
    sink = RecordingSink()
    tr = transport if transport is not None else FakeTransport()
    feed = HyperliquidFeed(
        cfg,
        the_clock,
        rest if rest is not None else FakeRest(),
        sink,
        connect=tr,
        max_queue=max_queue,
        rng=rng if rng is not None else MaxJitter(),
    )
    return feed, sink, tr, the_clock


# -- specs -----------------------------------------------------------------


async def test_specs_delegates_to_rest() -> None:
    rest = FakeRest(("BTC", "ETH", "SOL"))
    feed, *_ = make_feed(rest=rest)
    specs = await feed.specs()
    assert set(specs) == {"BTC", "ETH", "SOL"}
    assert rest.calls == 1


def test_the_real_rest_client_satisfies_the_feed_s_narrow_surface() -> None:
    """Contract test for the seam between this module and `rest.py`.

    The feed depends on exactly one method. Checking it here means a rename on
    the REST side fails a test instead of failing at runtime in a recorder that
    has already been deployed.
    """
    rest_module = pytest.importorskip("funding_atlas.venues.hyperliquid.rest")
    assert inspect.iscoroutinefunction(rest_module.HyperliquidRest.asset_specs)
    signature = inspect.signature(rest_module.HyperliquidRest.asset_specs)
    assert list(signature.parameters) == ["self"]


# -- subscription planning and the per-IP cap ------------------------------


def test_subscription_plan_matches_the_configured_streams() -> None:
    feed, *_ = make_feed()
    subs = feed.plan_subscriptions({"BTC", "ETH"})
    assert list(subs) == [
        {"type": "trades", "coin": "BTC"},
        {"type": "trades", "coin": "ETH"},
        # l2 only for the assets on `streams.l2.assets`, not the whole universe.
        {"type": "l2Book", "coin": "BTC"},
        # allMids is one subscription for every asset on the venue.
        {"type": "allMids"},
    ]
    # No aggregation parameters: `nSigFigs` would coarsen recorded prices
    # irreversibly (docs §2).
    assert all("nSigFigs" not in s and "mantissa" not in s for s in subs)


async def test_subscription_cap_is_refused_at_startup_not_discovered_mid_run() -> None:
    assets = [f"A{i}" for i in range(600)]
    cfg = make_config(
        universe={"mode": "explicit", "explicit": assets},
        streams={
            "trades": {"enabled": True},
            "mids": {"enabled": True, "sample_seconds": 1},
            "l2": {"enabled": True, "assets": assets, "sample_ms": 500},
        },
    )
    feed, _sink, transport, _clock = make_feed(config=cfg)

    with pytest.raises(SubscriptionLimitError) as excinfo:
        feed.plan_subscriptions(assets)
    message = str(excinfo.value)
    assert "1201" in message  # 600 trades + 600 l2Book + 1 allMids
    assert str(MAX_SUBSCRIPTIONS_PER_IP) in message

    # The refusal must come from `stream()` too, and before any socket is
    # opened: discovering it mid-run means an unknown subset of the universe is
    # silently unrecorded.
    with pytest.raises(SubscriptionLimitError):
        async for _ in feed.stream():
            break
    assert transport.urls == []


async def test_subscription_set_exactly_at_the_cap_is_allowed() -> None:
    # The cap is a limit, not a budget to stay clear of: refusing at 1000 would
    # make a correctly-sized deployment unstartable.
    assets = [f"A{i}" for i in range(999)]
    cfg = make_config(
        universe={"mode": "explicit", "explicit": assets},
        streams={"trades": {"enabled": True}, "mids": {"enabled": True, "sample_seconds": 1}},
    )
    feed, *_ = make_feed(config=cfg)
    assert len(feed.plan_subscriptions(assets)) == MAX_SUBSCRIPTIONS_PER_IP


async def test_a_configuration_that_subscribes_to_nothing_is_refused() -> None:
    cfg = make_config(
        streams={
            "trades": {"enabled": False},
            "mids": {"enabled": False},
            "l2": {"enabled": False},
            "funding": {"enabled": True, "poll_seconds": 60},
        }
    )
    feed, *_ = make_feed(config=cfg)
    with pytest.raises(SubscriptionLimitError):
        feed.plan_subscriptions({"BTC"})


# -- backoff derived from the connection cap -------------------------------


def test_backoff_floor_is_derived_from_the_connection_cap() -> None:
    # 30 new connections / 60 s => one every 2 s is the fastest the cap allows.
    assert MIN_CONNECT_SPACING_S == CONNECTION_WINDOW_S / MAX_NEW_CONNECTIONS_PER_MINUTE == 2.0
    # Full jitter sleeps uniform(0, window), mean window/2, so the smallest
    # window must be twice the spacing for the *mean* rate to stay inside it.
    assert BACKOFF_FLOOR_S == 2 * MIN_CONNECT_SPACING_S == 4.0


def test_backoff_mean_delay_respects_the_cap() -> None:
    feed, *_ = make_feed(rng=random.Random(20_251_013))
    draws = [feed.backoff_delay(1) for _ in range(20_000)]
    assert all(0.0 <= d <= BACKOFF_FLOOR_S for d in draws)
    mean = sum(draws) / len(draws)
    # The mean is the long-run attempt spacing; it must not fall under what the
    # cap permits. A 2 s window (the tempting choice) would land at 1.0 here and
    # sustain twice the permitted connection rate.
    assert mean == pytest.approx(MIN_CONNECT_SPACING_S, rel=0.05)


def test_backoff_windows_double_and_are_capped() -> None:
    feed, *_ = make_feed()
    windows = [feed.backoff_window_s(a) for a in range(1, 8)]
    assert windows[:4] == [4.0, 8.0, 16.0, 32.0]
    assert all(w <= BACKOFF_CEILING_S for w in windows)
    assert windows[-1] == BACKOFF_CEILING_S


async def test_connection_budget_never_exceeds_the_documented_rate() -> None:
    clock = RecordingClock()
    budget = _ConnectionBudget(clock)
    stamps: list[int] = []

    # A caller that ignores the backoff entirely: the rolling-window budget
    # alone has to hold the cap, because an expectation is not a guarantee.
    for _ in range(100):
        await budget.acquire()
        stamps.append(clock.now_ns())

    for i, start in enumerate(stamps):
        in_window = [t for t in stamps[i:] if t < start + int(CONNECTION_WINDOW_S * S)]
        assert len(in_window) <= MAX_NEW_CONNECTIONS_PER_MINUTE


# -- reconnect, resubscribe, ping ------------------------------------------


async def test_every_connection_replays_the_whole_subscription_set() -> None:
    clock = RecordingClock()
    transport = FakeTransport(
        [TRADES_MSG, ConnectionResetError("peer went away")],
        [TRADES_MSG, ...],
    )
    feed, sink, _tr, _clock = make_feed(clock=clock, transport=transport)
    subs = feed.plan_subscriptions({"BTC", "ETH"})

    task = asyncio.create_task(feed._run(subs))
    await _until(lambda: len(transport.conns) == 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Nothing is restored by the server (docs §1), so the second socket must
    # carry the identical set — not a diff against what we assume it has.
    assert transport.conns[0].subscriptions() == list(subs)
    assert transport.conns[1].subscriptions() == list(subs)

    down = sink.of(EventType.STREAM_DOWN)
    assert any(e["reason"] == "disconnect" for e in down)
    # The hole between the last message and the completed resubscribe is written
    # as a gap, not assumed to be instantaneous.
    up = sink.of(EventType.STREAM_UP)
    assert len(up) == 2
    assert up[1]["gap_to_ns"] >= up[1]["gap_from_ns"]


async def test_backoff_grows_while_connecting_keeps_failing() -> None:
    clock = RecordingClock()
    transport = FakeTransport(fail=1)
    feed, sink, _tr, _clock = make_feed(clock=clock, transport=transport)

    task = asyncio.create_task(feed._run(feed.plan_subscriptions({"BTC"})))
    await _until(lambda: len(_backoffs(clock)) >= 4)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # MaxJitter pins each draw to the window's upper bound, so the doubling is
    # visible exactly.
    assert _backoffs(clock)[:4] == [4.0, 8.0, 16.0, 32.0]
    assert all(e["reason"] == "error" for e in sink.of(EventType.STREAM_DOWN))


async def test_an_unproductive_connection_does_not_reset_the_backoff() -> None:
    clock = RecordingClock()
    # Three sockets that accept us and immediately close without sending
    # anything. Resetting the backoff on each would hammer the 30/minute cap at
    # full speed in exactly the state where the venue is least forgiving.
    transport = FakeTransport([], [], [])
    feed, *_ = make_feed(clock=clock, transport=transport)

    task = asyncio.create_task(feed._run(feed.plan_subscriptions({"BTC"})))
    await _until(lambda: len(_backoffs(clock)) >= 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert _backoffs(clock)[:3] == [4.0, 8.0, 16.0]


async def test_a_productive_connection_resets_the_backoff() -> None:
    clock = RecordingClock()
    transport = FakeTransport([TRADES_MSG], [TRADES_MSG], [TRADES_MSG])
    feed, *_ = make_feed(clock=clock, transport=transport)

    task = asyncio.create_task(feed._run(feed.plan_subscriptions({"BTC"})))
    await _until(lambda: len(_backoffs(clock)) >= 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Data arrived each time, so each retry starts from the floor again.
    assert _backoffs(clock)[:3] == [4.0, 4.0, 4.0]


async def test_ping_uses_the_documented_period_and_payload() -> None:
    clock = RecordingClock()
    feed, *_ = make_feed(clock=clock)
    conn = FakeConn([])

    task = asyncio.create_task(feed._ping_loop(conn))
    await _until(lambda: len(conn.sent) >= 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert json.loads(conn.sent[0]) == {"method": "ping"}
    # 50 s, not 60: the server's cutoff is 60 s on messages it receives, so a
    # 60 s period races the thing it exists to avoid.
    assert clock.sleeps[:2] == [PING_INTERVAL_S, PING_INTERVAL_S]
    assert PING_INTERVAL_S < 60.0


# -- inbound silence -------------------------------------------------------


async def test_inbound_silence_is_detected_although_the_socket_looks_alive() -> None:
    clock = RecordingClock(advance=False)
    feed, sink, _tr, _clock = make_feed(clock=clock)
    conn = FakeConn([])
    feed._conn = conn

    await feed._handle(L2_MSG)
    assert feed.messages == 1

    # 89 s: inside the window. Our pings are still being accepted and the socket
    # is open, which is exactly the state that proves nothing.
    clock.set_now_ns(T0 + 89 * S)
    assert await feed.check_silence() is False
    assert sink.of(EventType.STREAM_DOWN) == []

    clock.set_now_ns(T0 + 90 * S)
    assert await feed.check_silence() is True
    down = sink.of(EventType.STREAM_DOWN)
    assert [(e["stream"], e["asset"], e["reason"]) for e in down] == [
        ("l2", "BTC", "silence"),
        ("all", None, "silence"),
    ]
    assert down[0]["last_message_ns"] == T0
    # The only remedy the venue offers is a fresh socket and a full resubscribe.
    assert conn.closed is True


async def test_quiet_trades_are_not_mistaken_for_an_outage() -> None:
    clock = RecordingClock(advance=False)
    feed, sink, _tr, _clock = make_feed(clock=clock)

    await feed._handle(TRADES_MSG)
    clock.set_now_ns(T0 + 600 * S)
    # No connection attached, so only the per-stream rule is under test: a thin
    # perp with no fills for ten minutes is a quiet market, not a failure, and
    # declaring it down would trigger a reconnect that drops the subscriptions
    # which *are* working.
    assert await feed.check_silence() is False
    assert sink.of(EventType.STREAM_DOWN) == []


async def test_data_clears_a_silence_flag_so_a_second_stall_is_reported() -> None:
    clock = RecordingClock(advance=False)
    feed, sink, _tr, _clock = make_feed(clock=clock)
    feed._conn = FakeConn([])

    await feed._handle(L2_MSG)
    clock.set_now_ns(T0 + 100 * S)
    await feed.check_silence()
    assert len(sink.of(EventType.STREAM_DOWN)) == 2

    await feed._handle(L2_MSG)
    clock.set_now_ns(T0 + 300 * S)
    feed._conn = FakeConn([])
    await feed.check_silence()
    assert len([e for e in sink.of(EventType.STREAM_DOWN) if e["stream"] == "l2"]) == 2


# -- backpressure ----------------------------------------------------------


async def test_a_dropped_message_under_backpressure_is_reported_as_a_gap() -> None:
    clock = RecordingClock(advance=False)
    # Queue of one: the consumer is slower than the socket by construction.
    feed, sink, _tr, _clock = make_feed(clock=clock, max_queue=1)

    await feed._handle(TRADES_MSG)
    assert feed.dropped == 0

    clock.set_now_ns(T0 + 5 * S)
    await feed._handle(TRADES_MSG)
    assert feed.dropped == 1

    gaps = sink.of(EventType.DATA_GAP)
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["reason"] == "backpressure"
    assert gap["stream"] == "trades"
    assert gap["asset"] == "BTC"
    # The hole starts at the last record the consumer actually received, not at
    # the instant the queue filled.
    assert gap["from_ns"] == T0
    assert gap["to_ns"] == T0 + 5 * S
    assert "dropped" in gap["detail"]


async def test_continuing_drops_are_all_counted_but_eventing_is_coalesced() -> None:
    clock = RecordingClock(advance=False)
    feed, sink, _tr, _clock = make_feed(clock=clock, max_queue=1)

    for _ in range(50):
        await feed._handle(TRADES_MSG)
    # Every drop is in the counter; the event log is not flooded at tape rate
    # into the writer that is already behind.
    assert feed.dropped == 49
    assert len(sink.of(EventType.DATA_GAP)) == 1


# -- undecodable messages --------------------------------------------------


async def test_undecodable_messages_are_counted_and_evented_once() -> None:
    feed, sink, _tr, _clock = make_feed()

    for _ in range(5):
        await feed._handle('{"channel": "somethingNew", "data": {}}')
    await feed._handle("not json")

    assert feed.undecodable["unknown_channel"] == 5
    assert feed.undecodable["not_json"] == 1
    # One event per kind: enough to find the first occurrence in permanent
    # history, not enough to drown it.
    errors = sink.of(EventType.ERROR)
    assert {e["reason"] for e in errors} == {"unknown_channel", "not_json"}
    assert len(errors) == 2
    assert feed.messages == 0


async def test_a_bad_message_does_not_stop_the_read_loop() -> None:
    clock = RecordingClock(advance=False)
    transport = FakeTransport(['{"channel": "trades", "data": "nonsense"}', TRADES_MSG, ...])
    feed, _sink, _tr, _clock = make_feed(clock=clock, transport=transport)

    received: list[Any] = []

    async def consume() -> None:
        async for record in feed.stream():
            received.append(record)
            break

    task = asyncio.create_task(consume())
    await _until(lambda: bool(received))
    await _finish(task)

    assert isinstance(received[0], Trade)
    assert feed.undecodable["bad_payload"] == 1


# -- end to end through stream() -------------------------------------------


async def test_stream_yields_records_for_every_subscribed_channel() -> None:
    clock = RecordingClock(advance=False)
    transport = FakeTransport([TRADES_MSG, L2_MSG, ALL_MIDS_MSG, ...])
    feed, _sink, _tr, _clock = make_feed(clock=clock, transport=transport)

    received: list[Any] = []

    async def consume() -> None:
        async for record in feed.stream():
            received.append(record)
            if len(received) == 3:
                break

    task = asyncio.create_task(consume())
    await _until(lambda: len(received) >= 3)
    await _finish(task)

    assert isinstance(received[0], Trade)
    assert isinstance(received[1], BookSnapshot)
    assert isinstance(received[2], Mid)
    # `allMids` carries ~1300 keys live; only the recorded universe survives.
    assert received[2].asset in {"BTC", "ETH"}
    assert received[0].price == Decimal("83245.0")


async def test_mids_are_downsampled_to_the_configured_cadence() -> None:
    clock = RecordingClock(advance=False)
    feed, _sink, _tr, _clock = make_feed(clock=clock)
    feed._universe = frozenset({"BTC"})

    await feed._handle(ALL_MIDS_MSG)
    await feed._handle(ALL_MIDS_MSG)  # same instant: inside sample_seconds: 1
    assert feed._queue.qsize() == 1

    clock.set_now_ns(T0 + 1 * S)
    await feed._handle(ALL_MIDS_MSG)
    assert feed._queue.qsize() == 2


async def test_stream_is_single_consumer() -> None:
    clock = RecordingClock(advance=False)
    transport = FakeTransport([...])
    feed, _sink, _tr, _clock = make_feed(clock=clock, transport=transport)

    first = feed.stream()
    task = asyncio.create_task(_anext(first))
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError):
        await _anext(feed.stream())

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_close_records_the_shutdown_hole() -> None:
    feed, sink, _tr, _clock = make_feed()
    await feed.close()
    down = sink.of(EventType.STREAM_DOWN)
    # A deliberate stop leaves the same hole as a crash; hiding it because we
    # meant to do it is how a dataset acquires invisible gaps.
    assert [e["reason"] for e in down] == ["shutdown"]
    await feed.close()  # idempotent
    assert len(sink.of(EventType.STREAM_DOWN)) == 1


# -- helpers ---------------------------------------------------------------


async def _finish(task: asyncio.Task[None]) -> None:
    """Let a consumer that has broken out of the stream shut the feed down."""
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _backoffs(clock: RecordingClock) -> list[float]:
    """Reconnect sleeps only. The ping loop shares the clock, and its fixed 50 s
    period would otherwise be read as a backoff window."""
    return [s for s in clock.sleeps if s != PING_INTERVAL_S]


async def _anext(iterator: Any) -> Any:
    return await iterator.__anext__()


async def _until(predicate: Any, *, ticks: int = 2000) -> None:
    """Spin the event loop until `predicate` holds. No wall-clock waiting."""
    for _ in range(ticks):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition never became true")
