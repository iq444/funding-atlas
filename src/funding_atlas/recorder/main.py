"""The recorder supervisor — the process this repository exists to run.

Everything else here is replaceable. A backtest can be re-run, a metric
recomputed, a dashboard rebuilt; an hour of order book that was never written
down is gone for good, and no API will sell it back. So this module is written
around one question: what happens when it stops?

Shutdown, in order, and the reason for each step:

  1. **Stop consuming.** The loop finishes the record it already took off the
     feed — nothing will offer that one again — and then returns.
  2. **Flush every writer.** Each one independently, each failure logged and
     the next one still attempted. A buffer dropped here is history, and the
     cheapest possible way to lose it is to let one stream's broken flush skip
     the other four.
  3. **Record the `shutdown` gap.** A planned stop leaves exactly the hole a
     crash does. Writing it with reason `shutdown` is what lets a reader tell
     maintenance from failure — while *not* writing it makes a restart
     indistinguishable from complete coverage, which is the worse lie: research
     would then backtest across minutes that never existed.
  4. **Flush and close the event log.** Last, because step 3 writes into it.

A second signal arriving mid-shutdown forces an exit rather than hanging
forever, but only after the first path has had its chance — and the whole of
steps 2-4 is bounded by `SHUTDOWN_TIMEOUT_S`, which is under the 30s
`stop_grace_period` the compose file grants, so the flush finishes on our terms
rather than being SIGKILLed halfway through a Parquet rename.

Exit codes distinguish the cases a restart policy has to tell apart: 0 is a
stop we chose, 1 is a crash, 2 is a config that never started, 130 is a forced
kill with buffers still in memory.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any, cast

import structlog

from funding_atlas.core.clock import Clock, LiveClock
from funding_atlas.core.config import ConfigError, RecorderConfig, load_config
from funding_atlas.core.eventlog import EventLog
from funding_atlas.core.events import Event, EventType
from funding_atlas.recorder.integrity import EventSink, StreamMonitor
from funding_atlas.recorder.poller import ContextPoller
from funding_atlas.recorder.streams import StreamConsumer
from funding_atlas.recorder.writers import ParquetWriter, RotationPolicy
from funding_atlas.store.parquet import ASSET_PARTITIONED
from funding_atlas.venues.base import MarketFeed
from funding_atlas.venues.records import STREAMS

__all__ = [
    "EXIT_CONFIG",
    "EXIT_CRASH",
    "EXIT_FORCED",
    "EXIT_OK",
    "EventLogSink",
    "Recorder",
    "build_feed",
    "build_poller",
    "cli",
    "configure_logging",
    "enabled_streams",
]

log = structlog.get_logger("recorder")

EXIT_OK = 0
EXIT_CRASH = 1
EXIT_CONFIG = 2
#: 128 + SIGINT, the shell convention. A forced exit means buffered rows were
#: abandoned, so it must not be reported as a clean stop.
EXIT_FORCED = 130

#: Under docker-compose's `stop_grace_period: 30s`, with room for the runtime to
#: tear down. Past this the flush is not going to finish and a bounded loss now
#: beats a SIGKILL arriving in the middle of a rename.
SHUTDOWN_TIMEOUT_S = 25.0

#: The recorder is not a trading account, but the event log partitions by one.
#: A fixed id keeps every process's gaps in a single, replayable stream.
RECORDER_ACCOUNT = "recorder"

_NS_PER_S = 1_000_000_000


def configure_logging(level: str = "INFO") -> None:
    """Structured logging, JSON when not a terminal.

    A gap investigation happens months later from a log aggregator, so the
    machine-readable form is the default and the pretty renderer is the
    exception for someone watching a console.
    """
    renderer: Any = (
        structlog.dev.ConsoleRenderer()
        if sys.stderr.isatty()
        else structlog.processors.JSONRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )


class EventLogSink:
    """Adapts integrity's one-method sink onto the append-only event log.

    The seam exists so that `integrity.py` knows nothing about sequence numbers
    or accounts, and the event log knows nothing about how a gap was detected.
    This class is the only place the two vocabularies meet.
    """

    __slots__ = ("_account_id", "_clock", "_log", "_venue")

    def __init__(
        self, event_log: EventLog, clock: Clock, venue: str, account_id: str = RECORDER_ACCOUNT
    ) -> None:
        self._log = event_log
        self._clock = clock
        self._venue = venue
        self._account_id = account_id

    async def emit(self, event_type: EventType, payload: dict[str, Any]) -> None:
        # A gap's `to_ns` is its event time: the moment the hole closed, which is
        # also the earliest point from which data is trustworthy again. Falling
        # back to the clock keeps the field meaningful for the other event types
        # rather than leaving it zero.
        candidate = payload.get("to_ns")
        ts_event_ns = candidate if isinstance(candidate, int) else self._clock.now_ns()
        await self._log.append(
            Event(
                seq=0,
                type=event_type,
                account_id=self._account_id,
                venue=self._venue,
                ts_event_ns=ts_event_ns,
                ts_ingest_ns=self._clock.now_ns(),
                payload=payload,
            )
        )


#: How the supervisor obtains a feed. Behind a factory so the wiring is testable
#: against a fake `MarketFeed`, and so a second venue is a new branch here
#: rather than a change to the supervisor.
FeedFactory = Callable[[RecorderConfig, Clock, EventSink], Awaitable[MarketFeed]]
PollerFactory = Callable[
    [RecorderConfig, Clock, StreamConsumer, EventSink], Awaitable["ContextPoller | None"]
]


async def build_feed(config: RecorderConfig, clock: Clock, sink: EventSink) -> MarketFeed:
    """Construct the configured venue's feed.

    The imports are deliberately inside the function. `venues/hyperliquid`
    pulls in a WebSocket client and the venue's REST metadata layer, and binding
    that at module import would make the supervisor untestable without it — and
    would make a venue package that fails to import take down a process that may
    not even be configured to use it.
    """
    if config.venue == "hyperliquid":
        from funding_atlas.venues.hyperliquid.rest import HyperliquidRest
        from funding_atlas.venues.hyperliquid.ws import HyperliquidFeed

        rest = HyperliquidRest(config.network, clock)
        # `MarketFeed.stream()` is declared as yielding `dict`, while every
        # adapter in fact yields the canonical records from `venues/records.py`
        # — which is what the writers' schemas are defined over. The cast
        # records that mismatch here rather than papering over it; the fix is a
        # narrowed annotation in `venues/base.py`, which this module does not own.
        return cast("MarketFeed", HyperliquidFeed(config, clock, rest, sink))
    raise ConfigError(f"no feed implementation for venue {config.venue!r}")


async def build_poller(
    config: RecorderConfig, clock: Clock, consumer: StreamConsumer, sink: EventSink
) -> ContextPoller | None:
    """Construct the REST poller that carries funding and open interest.

    Lazily imported for the same reason as `build_feed`. Returns None when the
    venue has no poller or neither polled stream is enabled, so the supervisor
    simply runs one fewer task.
    """
    if config.venue != "hyperliquid":
        return None
    from funding_atlas.venues.hyperliquid.rest import HyperliquidRest

    poller = ContextPoller(HyperliquidRest(config.network, clock), config, clock, consumer, sink)
    return poller if poller.enabled else None


def enabled_streams(config: RecorderConfig) -> tuple[str, ...]:
    """Enabled stream names, in `records.STREAMS` order.

    Driven off `STREAMS` rather than a literal list so that a stream added to
    the records module and to `StreamsConfig` is recorded without a third edit
    here — the kind of omission that produces a stream nobody notices is missing
    until someone queries for it a year later.
    """
    return tuple(name for name in STREAMS if getattr(config.streams, name).enabled)


def cadences_for(config: RecorderConfig) -> dict[str, float | None]:
    """Expected arrival cadence per stream, for silence detection.

    `None` means "judged by the market, not by us" — see the note in
    `integrity.py` about why a quiet trades stream must never be called down.
    """
    streams = config.streams
    return {
        "trades": None,
        "mids": _positive(streams.mids.sample_seconds),
        "l2": _positive(streams.l2.sample_ms / 1000.0),
        "funding": _positive(streams.funding.poll_seconds),
        "open_interest": _positive(streams.open_interest.poll_seconds),
    }


def _positive(value: float) -> float | None:
    # A zero interval only survives config validation on a disabled stream.
    # Treating it as "no cadence" is safer than as "expect a message instantly".
    return float(value) if value > 0 else None


def build_writers(
    config: RecorderConfig, clock: Clock, streams: Sequence[str] | None = None
) -> dict[str, ParquetWriter]:
    """One writer per enabled stream.

    Separate instances rather than one shared writer, for two reasons that both
    show up at shutdown: a flush that raises on the L2 stream cannot take the
    funding stream's buffer with it, and `rows_written` / `last_flush_ns` are
    then attributable to a single stream without any bookkeeping of our own.
    """
    policy = RotationPolicy(
        max_rows=config.rotation.max_rows,
        max_seconds=float(config.rotation.max_seconds),
    )
    names = tuple(streams) if streams is not None else enabled_streams(config)
    return {name: ParquetWriter(config.data_dir, policy, clock=clock) for name in names}


class Recorder:
    """Owns the loops, and owns the shutdown.

    Construction does no IO: the event log, monitor and writers are built here
    so a test can inspect them, while the feed is built inside `run` because it
    needs a running loop and may have to be rebuilt after a failure.
    """

    def __init__(
        self,
        config: RecorderConfig,
        clock: Clock | None = None,
        *,
        feed_factory: FeedFactory = build_feed,
        poller_factory: PollerFactory = build_poller,
        event_log: EventLog | None = None,
        max_feed_restarts: int = 5,
        restart_backoff_s: float = 5.0,
        heartbeat_max_ticks: int | None = None,
        drain_timeout_s: float = 2.0,
    ) -> None:
        self._config = config
        self._clock = clock or LiveClock()
        self._feed_factory = feed_factory
        self._poller_factory = poller_factory
        self._event_log = event_log or EventLog(config.data_dir, self._clock)
        self._sink = EventLogSink(self._event_log, self._clock, config.venue)
        self._monitor = StreamMonitor(
            self._clock,
            self._sink,
            max_silence_seconds=float(config.integrity.max_silence_seconds),
            heartbeat_seconds=float(config.integrity.heartbeat_seconds),
            cadences=cadences_for(config),
        )
        self._streams = enabled_streams(config)
        self._writers = build_writers(config, self._clock, self._streams)
        self._consumer = StreamConsumer(self._writers, self._monitor)
        self._stop = asyncio.Event()
        self._max_feed_restarts = max_feed_restarts
        self._restart_backoff_s = restart_backoff_s
        self._heartbeat_max_ticks = heartbeat_max_ticks
        self._drain_timeout_s = drain_timeout_s
        self._registered: set[tuple[str, str | None]] = set()
        self._signals = 0
        self._failure: BaseException | None = None

    # -- accessors (for the health endpoint and for tests) -------------------

    @property
    def event_log(self) -> EventLog:
        return self._event_log

    @property
    def monitor(self) -> StreamMonitor:
        return self._monitor

    @property
    def writers(self) -> dict[str, ParquetWriter]:
        return self._writers

    @property
    def consumer(self) -> StreamConsumer:
        return self._consumer

    @property
    def failure(self) -> BaseException | None:
        """The exception that ended the run, if it ended badly."""
        return self._failure

    def request_stop(self) -> None:
        """Ask for a graceful stop. Idempotent; safe from a signal handler."""
        self._stop.set()

    # -- run ----------------------------------------------------------------

    async def run(self) -> int:
        # `redacted()`, never the config object: `pg_dsn` and `s3_secret` are
        # SecretStr precisely so a startup line cannot leak them, and an
        # f-string of the model would undo that.
        log.info(
            "recorder.start",
            config=self._config.redacted(),
            streams=list(self._streams),
            pid=os.getpid(),
        )
        self._install_signal_handlers()
        self._register_streams()

        consume = asyncio.create_task(self._consume_with_restarts(), name="consume")
        supervise = asyncio.create_task(
            self._monitor.supervise(stop=self._stop, max_ticks=self._heartbeat_max_ticks),
            name="supervise",
        )
        heartbeat = asyncio.create_task(self._heartbeat(), name="heartbeat")

        # Funding and open interest do not arrive on the socket, so without this
        # task they are recorded as an unbroken run of gaps while everything
        # else looks healthy. See recorder/poller.py.
        poller = await self._poller_factory(self._config, self._clock, self._consumer, self._sink)
        poll = asyncio.create_task(self._poll(poller), name="poll") if poller is not None else None
        if poller is None:
            log.warning("recorder.poller.absent", venue=self._config.venue)

        try:
            await self._stop.wait()
        finally:
            # Step 1 of the shutdown sequence: consumption stops before anything
            # is flushed, so no writer is appended to while it is being drained.
            #
            # The consume task is given a moment to return on its own first. It
            # checks `stop` after every record, so an actively draining feed
            # exits here within one record — whereas cancelling immediately
            # could land in the middle of `writer.append` and lose the record
            # already taken off the socket. A feed merely *waiting* for data has
            # nothing in flight, which is what bounds the wait.
            self._stop.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(consume), timeout=self._drain_timeout_s)
            running = [t for t in (consume, supervise, heartbeat, poll) if t is not None]
            for task in running:
                task.cancel()
            for task in running:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await self._shut_down("feed failure" if self._failure is not None else "clean shutdown")

        if self._failure is not None:
            log.error("recorder.exit", code=EXIT_CRASH, error=repr(self._failure))
            return EXIT_CRASH
        log.info("recorder.exit", code=EXIT_OK)
        return EXIT_OK

    async def _shut_down(self, reason: str = "clean shutdown") -> None:
        """Steps 2-4. Bounded, and every step attempted even if an earlier one failed."""
        try:
            await asyncio.wait_for(self._shut_down_steps(reason), timeout=SHUTDOWN_TIMEOUT_S)
        except TimeoutError:
            # Deliberately not re-raised: the exit code already reflects how the
            # run ended, and there is nothing left to try.
            log.error("recorder.shutdown.timeout", timeout_s=SHUTDOWN_TIMEOUT_S)

    async def _shut_down_steps(self, reason: str) -> None:
        for name, writer in self._writers.items():
            pending = writer.pending_rows
            try:
                await writer.close()
            except Exception as exc:  # noqa: BLE001 - one stream must not strand the rest
                log.error("recorder.flush.failed", stream=name, pending=pending, error=repr(exc))
            else:
                log.info("recorder.flush", stream=name, flushed=pending)

        try:
            gaps = await self._monitor.shutdown(reason)
        except Exception as exc:  # noqa: BLE001 - the log still has to be closed
            log.error("recorder.gap.failed", error=repr(exc))
        else:
            log.info(
                "recorder.gap",
                reason=reason,
                gaps=[{"stream": g.stream, "asset": g.asset, "s": g.duration_s} for g in gaps],
            )

        try:
            await self._event_log.sync()
        except Exception as exc:  # noqa: BLE001 - close still fsyncs each partition
            log.error("recorder.eventlog.sync_failed", error=repr(exc))
        await self._event_log.close()
        log.info("recorder.stopped", root=str(self._event_log.root))

    # -- loops ---------------------------------------------------------------

    async def _consume_with_restarts(self) -> None:
        """Drain the feed; on failure, record it and decide restart vs exit.

        A feed that raises must never take the process down quietly: the
        exception becomes a `STREAM_DOWN` per known stream plus an `ERROR` event,
        so the gap it caused is attributable afterwards even if the restart
        succeeds and the stream looks healthy again by morning.
        """
        attempt = 0
        try:
            while not self._stop.is_set():
                try:
                    feed = await self._feed_factory(self._config, self._clock, self._sink)
                    await self._consumer.run(feed, stop=self._stop)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - classified below, never swallowed
                    await self._record_feed_failure(exc)
                    if attempt >= self._max_feed_restarts:
                        self._failure = exc
                        log.error("recorder.feed.giving_up", attempts=attempt, error=repr(exc))
                        return
                    attempt += 1
                    log.warning(
                        "recorder.feed.restarting",
                        attempt=attempt,
                        backoff_s=self._restart_backoff_s,
                        error=repr(exc),
                    )
                    await self._clock.sleep(self._restart_backoff_s)
                    continue
                # The feed ended without raising. For a live socket that is
                # itself unusual, but it is not a loss event, so it is a clean
                # stop rather than a crash.
                log.info("recorder.feed.exhausted", handled=dict(self._consumer.handled))
                return
        finally:
            # Whatever happened, the supervisor must wake up and run shutdown.
            self._stop.set()

    async def _poll(self, poller: ContextPoller) -> None:
        """Run the REST poller until stop.

        A failing poll is handled inside the poller (reported, backed off), so
        reaching here means something unrecoverable. It is recorded like a feed
        failure rather than ending the process: trades, mids and books are still
        worth recording even while funding is unavailable, and the monitor turns
        the funding silence into a gap that says so.
        """
        try:
            await poller.run(stop=self._stop)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("recorder.poller.stopped", error=repr(exc))
            await self._sink.emit(
                EventType.ERROR, {"component": "context_poller", "error": repr(exc), "fatal": True}
            )

    async def _heartbeat(self) -> None:
        """Periodic `flush_due` plus the health line.

        The flush is the point: a stream that goes quiet still holds a partial
        buffer, and `max_seconds` only fires from inside `append`. Without this
        tick an idle perp's last file never lands.

        `max_ticks` mirrors `StreamMonitor.supervise`: under a SimClock `sleep`
        does not yield, so a test needs a termination condition that is not wall
        time.
        """
        ticks = 0
        interval = float(self._config.integrity.heartbeat_seconds)
        while not self._stop.is_set():
            await self._clock.sleep(interval)
            if self._stop.is_set():
                return
            await self.heartbeat_once()
            ticks += 1
            if self._heartbeat_max_ticks is not None and ticks >= self._heartbeat_max_ticks:
                return

    async def heartbeat_once(self) -> dict[str, Any]:
        """One tick: flush what is due, then report. Returns the health snapshot."""
        for name, writer in self._writers.items():
            try:
                await writer.flush_due()
            except Exception as exc:  # noqa: BLE001 - a wedged stream must not stop the tick
                log.error("recorder.flush_due.failed", stream=name, error=repr(exc))
        health = self.health()
        log.info("recorder.health", **health)
        return health

    def health(self) -> dict[str, Any]:
        """What the health line and `api/routes/health.py` both need.

        Last-flush *age* rather than the raw timestamp: a stream whose age stops
        growing is fine, and one whose age passes `rotation.max_seconds` is
        wedged — a judgement nobody can make from an epoch value at a glance.
        """
        now_ns = self._clock.now_ns()
        rows: dict[str, int] = {}
        # Per stream, not just a total. `rows_written` counts only what has been
        # flushed, and rotation is hourly — so a perfectly healthy recorder
        # reports zeros for an hour, which reads exactly like one that is
        # recording nothing. The buffered counts are what show it is alive.
        buffered: dict[str, int] = {}
        ages: dict[str, float | None] = {}
        pending = 0
        for name, writer in self._writers.items():
            rows[name] = writer.rows_written.get(name, 0)
            last = writer.last_flush_ns.get(name)
            ages[name] = None if last is None else round((now_ns - last) / _NS_PER_S, 3)
            buffered[name] = writer.pending_rows
            pending += writer.pending_rows
        down = sorted(
            f"{stream}:{asset or '*'}"
            for stream, asset in self._known_keys()
            if not self._monitor.is_up(stream, asset)
        )
        return {
            "rows_written": rows,
            "buffered_rows": buffered,
            "last_flush_age_s": ages,
            "pending_rows": pending,
            "down": down,
            "unroutable": dict(self._consumer.unroutable),
        }

    # -- internals -----------------------------------------------------------

    def _known_keys(self) -> set[tuple[str, str | None]]:
        return self._registered | self._consumer.keys

    def _register_streams(self) -> None:
        """Watch the streams we can name before the first message arrives.

        Registration is otherwise lazy, which would mean a stream that never
        delivers anything is never judged silent and never produces a shutdown
        gap — the hole would simply not be in the ledger. Under
        `universe.mode: all_perps` the asset set is whatever the venue lists at
        runtime and is genuinely unknowable here, so those arrive via
        `record_message` instead.
        """
        explicit = tuple(a.upper() for a in self._config.universe.explicit)
        for name in self._streams:
            if name not in ASSET_PARTITIONED:
                self._monitor.register(name)
                self._registered.add((name, None))
                continue
            assets = self._config.streams.l2.assets if name == "l2" else explicit
            for asset in assets:
                self._monitor.register(name, asset)
                self._registered.add((name, asset))

    async def _record_feed_failure(self, exc: BaseException) -> None:
        detail = f"{type(exc).__name__}: {exc}"
        for stream, asset in sorted(self._known_keys(), key=lambda k: (k[0], k[1] or "")):
            with contextlib.suppress(Exception):
                await self._monitor.record_error(stream, asset, detail)
        with contextlib.suppress(Exception):
            # The event log is best-effort here only because the alternative is
            # losing the exception entirely; the loss itself is already in the
            # monitor's ledger above.
            await self._sink.emit(
                EventType.ERROR,
                {"source": "feed", "venue": self._config.venue, "error": detail},
            )
        log.error("recorder.feed.failed", error=detail, exc_info=exc)

    def _install_signal_handlers(self) -> None:
        """SIGTERM (docker stop) and SIGINT (a human at a console).

        The second signal is the escape hatch, not the policy: the first one has
        already been given its chance at the flush, and someone hitting Ctrl-C
        twice has decided they would rather lose the buffer than wait.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except (NotImplementedError, RuntimeError, ValueError):
                # No signal support (Windows, or a non-main thread). Not fatal:
                # the supervisor still stops on feed exhaustion or failure.
                log.warning("recorder.signal.unsupported", signal=sig.name)

    def _on_signal(self, sig: signal.Signals) -> None:
        self._signals += 1
        if self._signals == 1:
            log.warning("recorder.signal", signal=sig.name, action="graceful shutdown")
            self.request_stop()
            return
        # `os._exit`, not `sys.exit`: we are inside a signal callback on a loop
        # that is already mid-shutdown, and raising SystemExit here would simply
        # be swallowed by the task that is trying to flush.
        log.error("recorder.signal.forced", signal=sig.name, pending=self._pending_rows())
        os._exit(EXIT_FORCED)

    def _pending_rows(self) -> int:
        return sum(writer.pending_rows for writer in self._writers.values())


def cli(argv: Sequence[str] | None = None) -> int:
    """`fa-record` entry point. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog="fa-record", description="Record market data continuously."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="path to recorder.yaml (default: $FA_CONFIG, else config/recorder.yaml)",
    )
    args = parser.parse_args(argv)

    # Logging is configured twice on purpose: once at a default level so a
    # config failure is not silent, then again at the configured level.
    configure_logging()
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log.error("recorder.config.invalid", path=str(args.config or ""), error=str(exc))
        return EXIT_CONFIG
    except Exception as exc:  # noqa: BLE001 - pydantic validation, surfaced not traced
        log.error("recorder.config.invalid", path=str(args.config or ""), error=repr(exc))
        return EXIT_CONFIG

    configure_logging(config.log_level)
    return asyncio.run(Recorder(config, LiveClock()).run())


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(cli())
