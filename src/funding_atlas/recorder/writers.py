"""Buffered, rotating Parquet writers for the recorder.

The recorder runs 24/7 and its one irreplaceable output is history: anything in
a buffer that never reaches disk is gone, and no API will sell it back. So the
rules here are about loss, not throughput tuning:

  * every exit path flushes (`close`, async context manager, exception);
  * files appear atomically or not at all (`store.parquet.write_table`);
  * a buffer never straddles a UTC date boundary, so a partition is complete
    once its date has passed;
  * Arrow conversion and disk IO happen off the event loop, because a 500k-row
    flush that blocks the loop is a stalled WebSocket, which is a data gap.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import Any, Self

from funding_atlas.core.clock import Clock, LiveClock
from funding_atlas.store.parquet import (
    ASSET_PARTITIONED,
    date_for_ns,
    partition_path,
    records_to_table,
    stream_of,
    write_table,
)

_NS_PER_S = 1_000_000_000


# Mirrors config/recorder.yaml `rotation`, so a writer constructed without a
# config behaves like the deployed one rather than like a test fixture.
DEFAULT_MAX_ROWS = 500_000
DEFAULT_MAX_SECONDS = 3600.0


def _digits(value: Any) -> str:
    # YAML resolves `500_000` to an int, but a config hand-edited to "500000"
    # as a quoted string should not take the recorder down on startup.
    return str(value).replace("_", "")


@dataclass(frozen=True, slots=True)
class RotationPolicy:
    """When to close a file. Defaults mirror config/recorder.yaml.

    Both bounds exist for different failures: `max_rows` keeps a single file
    scannable and bounds the loss window on a busy stream, `max_seconds` bounds
    it on a quiet one — without it, a thinly traded perp could hold an hour of
    trades in RAM and lose them to a restart.
    """

    max_rows: int = DEFAULT_MAX_ROWS
    max_seconds: float = DEFAULT_MAX_SECONDS

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> RotationPolicy:
        """Accepts the whole recorder config or just its `rotation` subtree."""
        section = config.get("rotation", config)
        return cls(
            max_rows=int(_digits(section.get("max_rows", DEFAULT_MAX_ROWS))),
            max_seconds=float(_digits(section.get("max_seconds", DEFAULT_MAX_SECONDS))),
        )


@dataclass(frozen=True, slots=True)
class _Key:
    """One open file's worth of records: a single partition directory."""

    stream: str
    venue: str
    asset: str | None
    day: date


@dataclass(slots=True)
class _Buffer:
    records: list[Any] = field(default_factory=list)
    opened_ns: int = 0


class ParquetWriter:
    """Buffers records per partition and flushes on the rotation policy.

    Not thread-safe, and does not need to be: it belongs to the recorder's event
    loop. `append` is the only hot path; it does no IO unless a bound trips.
    """

    __slots__ = (
        "_buffers",
        "_clock",
        "_day",
        "_day_hi",
        "_day_lo",
        "_due_check_ns",
        "_last_flush_ns",
        "_policy",
        "_root",
        "_rows_written",
        "_seq",
    )

    def __init__(
        self,
        root: Path | str,
        policy: RotationPolicy | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._root = Path(root)
        self._policy = policy or RotationPolicy()
        self._clock = clock or LiveClock()
        self._buffers: dict[_Key, _Buffer] = {}
        self._rows_written: dict[str, int] = {}
        self._last_flush_ns: dict[str, int] = {}
        self._seq = 0
        self._due_check_ns = 0
        # Cached UTC day as an integer range. `append` needs the partition date
        # of every record; comparing two ints beats constructing a datetime per
        # record on a stream doing thousands per second.
        self._day: date | None = None
        self._day_lo = 0
        self._day_hi = 0

    # -- health ---------------------------------------------------------------

    @property
    def rows_written(self) -> Mapping[str, int]:
        """Rows durably on disk per stream. Counted after rename, not after
        buffering, so the number never claims history the disk does not have."""
        return MappingProxyType(self._rows_written)

    @property
    def last_flush_ns(self) -> Mapping[str, int]:
        """Last successful flush per stream. A stream whose value stops moving is
        either dead upstream or wedged here, and the health check cannot tell the
        difference from rows alone."""
        return MappingProxyType(self._last_flush_ns)

    @property
    def pending_rows(self) -> int:
        """Rows buffered in memory — the window that a crash would lose."""
        return sum(len(buf.records) for buf in self._buffers.values())

    # -- writing --------------------------------------------------------------

    async def append(self, record: Any, stream: str | None = None) -> None:
        key = self._key_for(record, stream)
        buf = self._buffers.get(key)
        if buf is None:
            buf = self._buffers[key] = _Buffer(opened_ns=self._clock.now_ns())
        buf.records.append(record)

        if len(buf.records) >= self._policy.max_rows:
            await self._flush_key(key)
            return
        # Time- and date-based rotation are checked here too, so an active
        # recorder needs no external timer; `flush_due` covers the quiet case.
        # Rate-limited because the check scans every open partition and the
        # hot path runs thousands of times a second — a one-second lag on an
        # hour-scale bound is irrelevant, a scan per record is not.
        now_ns = self._clock.now_ns()
        if now_ns >= self._due_check_ns:
            self._due_check_ns = now_ns + _NS_PER_S
            await self.flush_due()

    async def append_many(self, records: Iterable[Any], stream: str | None = None) -> None:
        for record in records:
            await self.append(record, stream)

    async def flush_due(self) -> None:
        """Flush buffers past `max_seconds` or belonging to a past UTC date.

        The recorder calls this on its heartbeat (config `integrity.heartbeat_seconds`)
        so that a stream which goes silent still lands its last partial file, and
        so yesterday's partition is closed shortly after midnight rather than
        whenever the asset next trades.
        """
        now_ns = self._clock.now_ns()
        today = self._day_of(now_ns)
        deadline = now_ns - int(self._policy.max_seconds * _NS_PER_S)
        due = [
            key
            for key, buf in self._buffers.items()
            if key.day != today or buf.opened_ns <= deadline
        ]
        for key in due:
            await self._flush_key(key)

    async def flush(self) -> None:
        """Flush everything buffered, due or not."""
        for key in list(self._buffers):
            await self._flush_key(key)

    async def close(self) -> None:
        await self.flush()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Flushes even when the recorder is unwinding from an exception: the
        # records already captured are valid history regardless of what failed.
        await self.close()

    # -- internals ------------------------------------------------------------

    def _key_for(self, record: Any, stream: str | None) -> _Key:
        name = stream if stream is not None else stream_of(record)
        asset = record.asset if name in ASSET_PARTITIONED else None
        # Partition by the venue's event time, not arrival time: a record that
        # arrives at 00:00:01 for an event at 23:59:59 belongs to yesterday, or
        # a reader scanning `date=` has to widen every range by a fudge factor.
        day = self._day_of(record.ts_event_ns)
        return _Key(stream=name, venue=record.venue, asset=asset, day=day)

    def _day_of(self, ts_ns: int) -> date:
        if self._day is None or not (self._day_lo <= ts_ns < self._day_hi):
            day = date_for_ns(ts_ns)
            start = datetime.combine(day, time.min, tzinfo=UTC)
            self._day = day
            self._day_lo = int(start.timestamp()) * _NS_PER_S
            self._day_hi = self._day_lo + 86_400 * _NS_PER_S
        return self._day

    async def _flush_key(self, key: _Key) -> None:
        buf = self._buffers.pop(key, None)
        if buf is None or not buf.records:
            return
        # Buffer is detached before the await, so records appended while the
        # thread is writing accumulate into a fresh buffer instead of being
        # written twice or dropped.
        records = buf.records
        self._seq += 1
        path = partition_path(
            self._root,
            key.stream,
            key.venue,
            key.asset,
            key.day,
            self._clock.now_ns(),
            self._seq,
        )
        await asyncio.to_thread(_write, key.stream, records, path)
        self._rows_written[key.stream] = self._rows_written.get(key.stream, 0) + len(records)
        self._last_flush_ns[key.stream] = self._clock.now_ns()


def _write(stream: str, records: list[Any], path: Path) -> None:
    write_table(records_to_table(stream, records), path)


__all__ = ["ParquetWriter", "RotationPolicy"]
