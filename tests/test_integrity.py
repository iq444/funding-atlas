"""Integrity tests.

All of them run on `SimClock`, so time moves only when a test moves it. A test
that waits on wall time would be both slow and flaky, and the thing under test
is precisely *when* a judgement is made — that has to be exact, not approximate.
"""

from __future__ import annotations

from typing import Any

import pytest

from funding_atlas.core.clock import SimClock
from funding_atlas.core.events import EventType
from funding_atlas.recorder.integrity import StreamMonitor

S = 1_000_000_000
T0 = 1_700_000_000 * S


class RecordingSink:
    """Stands in for `core/eventlog.py` — the whole seam is one method."""

    def __init__(self) -> None:
        self.events: list[tuple[EventType, dict[str, Any]]] = []

    async def emit(self, event_type: EventType, payload: dict[str, Any]) -> None:
        self.events.append((event_type, payload))

    def of(self, event_type: EventType) -> list[dict[str, Any]]:
        return [p for t, p in self.events if t == event_type]


def make(
    *, max_silence_seconds: float = 90.0, heartbeat_seconds: float = 30.0
) -> tuple[SimClock, RecordingSink, StreamMonitor]:
    clock = SimClock(T0)
    sink = RecordingSink()
    monitor = StreamMonitor(
        clock,
        sink,
        max_silence_seconds=max_silence_seconds,
        heartbeat_seconds=heartbeat_seconds,
    )
    return clock, sink, monitor


async def test_silence_fires_at_the_threshold_and_not_before() -> None:
    clock, sink, monitor = make(max_silence_seconds=90.0)
    await monitor.record_message("mids", "BTC")

    clock.set_now_ns(T0 + 89 * S + 999_000_000)
    await monitor.check()
    assert sink.of(EventType.STREAM_DOWN) == []
    assert monitor.is_up("mids", "BTC")

    clock.set_now_ns(T0 + 90 * S)
    needs_reconnect = await monitor.check()

    down = sink.of(EventType.STREAM_DOWN)
    assert len(down) == 1
    assert down[0]["reason"] == "silence"
    assert down[0]["last_message_ns"] == T0
    assert not monitor.is_up("mids", "BTC")
    # The caller is told to resubscribe; the monitor never does it itself.
    assert [(k.stream, k.asset) for k in needs_reconnect] == [("mids", "BTC")]


async def test_quiet_trades_stream_is_not_declared_down() -> None:
    # A thin perp can genuinely trade nothing for an hour. Treating that as an
    # outage would write a false gap and tear down a healthy subscription.
    clock, sink, monitor = make(max_silence_seconds=90.0)
    await monitor.record_message("trades", "DOGE")

    clock.set_now_ns(T0 + 3600 * S)
    await monitor.check()

    assert sink.of(EventType.STREAM_DOWN) == []
    assert sink.of(EventType.DATA_GAP) == []
    assert monitor.is_up("trades", "DOGE")


async def test_trades_still_go_down_on_transport_evidence() -> None:
    # The flip side: silence proves nothing about trades, but a closed socket does.
    clock, sink, monitor = make()
    await monitor.record_message("trades", "DOGE")

    clock.set_now_ns(T0 + 10 * S)
    await monitor.record_disconnect("trades", "DOGE", detail="ws closed 1006")
    assert not monitor.is_up("trades", "DOGE")
    assert sink.of(EventType.STREAM_DOWN)[0]["reason"] == "disconnect"

    clock.set_now_ns(T0 + 25 * S)
    await monitor.record_message("trades", "DOGE")

    (gap,) = sink.of(EventType.DATA_GAP)
    assert gap["reason"] == "disconnect"
    assert (gap["from_ns"], gap["to_ns"]) == (T0, T0 + 25 * S)


async def test_recovery_emits_correctly_bounded_gap() -> None:
    clock, sink, monitor = make(max_silence_seconds=90.0)
    await monitor.record_message("l2", "BTC")

    clock.set_now_ns(T0 + 120 * S)
    await monitor.check()

    clock.set_now_ns(T0 + 200 * S)
    await monitor.record_message("l2", "BTC")

    (gap,) = sink.of(EventType.DATA_GAP)
    # Starts at the last recorded message, not at detection: nothing after T0 was
    # ever written, even though we only noticed 90s later.
    assert gap["from_ns"] == T0
    assert gap["to_ns"] == T0 + 200 * S
    assert gap["reason"] == "silence"
    assert gap["stream"] == "l2"
    assert gap["asset"] == "BTC"

    up = sink.of(EventType.STREAM_UP)
    assert len(up) == 1
    assert up[0]["gap_duration_s"] == pytest.approx(200.0)
    assert monitor.is_up("l2", "BTC")
    assert monitor.gaps[0].duration_s == pytest.approx(200.0)


async def test_clean_shutdown_still_records_a_gap() -> None:
    clock, sink, monitor = make()
    await monitor.record_message("funding", "ETH")

    clock.set_now_ns(T0 + 300 * S)
    (gap,) = await monitor.shutdown(detail="SIGTERM")

    assert gap.reason == "shutdown"
    assert (gap.from_ns, gap.to_ns) == (T0, T0 + 300 * S)
    assert sink.of(EventType.DATA_GAP)[0]["reason"] == "shutdown"
    assert sink.of(EventType.STREAM_DOWN)[0]["reason"] == "shutdown"


async def test_shutdown_keeps_the_original_cause_of_an_open_gap() -> None:
    clock, _sink, monitor = make()
    await monitor.record_message("mids", "SOL")
    clock.set_now_ns(T0 + 10 * S)
    await monitor.record_backpressure("mids", "SOL", detail="writer queue full")

    clock.set_now_ns(T0 + 60 * S)
    (gap,) = await monitor.shutdown()
    assert gap.reason == "backpressure"


async def test_coverage_straddling_a_gap() -> None:
    clock, _sink, monitor = make(max_silence_seconds=90.0)
    await monitor.record_message("l2", "BTC")

    clock.set_now_ns(T0 + 100 * S)
    await monitor.check()
    clock.set_now_ns(T0 + 160 * S)
    await monitor.record_message("l2", "BTC")
    clock.set_now_ns(T0 + 600 * S)

    straddling = monitor.coverage("l2", "BTC", T0 + 60 * S, T0 + 300 * S)
    assert not straddling.covered
    assert not straddling
    # Clipped to the requested window, not the gap's full extent.
    assert [(g.from_ns, g.to_ns) for g in straddling.gaps] == [(T0 + 60 * S, T0 + 160 * S)]
    assert straddling.missing_s == pytest.approx(100.0)

    clean = monitor.coverage("l2", "BTC", T0 + 200 * S, T0 + 300 * S)
    assert clean.covered
    assert clean.missing_ns == 0

    # Another asset on the same stream was never affected.
    assert monitor.coverage("l2", "ETH", T0 + 60 * S, T0 + 300 * S).covered


async def test_coverage_reports_a_stream_that_is_down_right_now() -> None:
    clock, _sink, monitor = make(max_silence_seconds=90.0)
    await monitor.record_message("mids", "BTC")
    clock.set_now_ns(T0 + 90 * S)
    await monitor.check()
    clock.set_now_ns(T0 + 200 * S)

    report = monitor.coverage("mids", "BTC", T0, T0 + 200 * S)
    assert not report.covered
    assert [(g.from_ns, g.to_ns) for g in report.gaps] == [(T0, T0 + 200 * S)]


async def test_supervision_loop_detects_and_asks_for_reconnect() -> None:
    clock, sink, monitor = make(max_silence_seconds=90.0, heartbeat_seconds=30.0)
    await monitor.record_message("mids", "BTC")
    asked: list[tuple[str, str | None]] = []

    async def on_reconnect(stream: str, asset: str | None) -> None:
        asked.append((stream, asset))

    # Four heartbeat ticks drive the SimClock to T0+120s, past the 90s threshold.
    await monitor.supervise(on_reconnect, max_ticks=4)

    assert clock.now_ns() == T0 + 120 * S
    assert sink.of(EventType.STREAM_DOWN)[0]["reason"] == "silence"
    # Ticks at 30/60 are healthy and ask for nothing; 90 and 120 both do.
    assert asked == [("mids", "BTC"), ("mids", "BTC")]


async def test_message_counts_and_unknown_stream() -> None:
    _clock, _sink, monitor = make()
    for _ in range(3):
        await monitor.record_message("trades", "BTC")
    state = monitor.state("trades", "BTC")
    assert state is not None
    assert state.message_count == 3
    assert state.is_regular is False
    assert monitor.state("mids", "BTC") is None

    with pytest.raises(ValueError, match="unknown stream"):
        monitor.register("l2_book", "BTC")
