"""Per-stream consumption: take records off a feed, file them, prove liveness.

This module is plumbing on purpose. Every judgement it could make belongs
somewhere else and is already made there: rotation and durability in
`recorder/writers.py`, silence and gap bookkeeping in `recorder/integrity.py`,
schemas in `venues/records.py`. What is left is routing, and routing is the one
thing that must not be clever — a mis-filed record is a silent corruption of a
partition that nothing downstream can detect.

Two ordering rules carry all the weight:

* **Write, then report liveness.** `StreamMonitor.record_message` is the only
  evidence a stream is alive. Calling it before the record is buffered would let
  a writer that raises on every append look perfectly healthy, which is the
  exact failure the monitor exists to catch.

* **An unroutable record is counted, never dropped silently and never fatal.**
  A venue can push a stream we did not enable (an extra subscription, a
  server-side default). Taking the process down for that would cost real
  history on the streams that *are* working; pretending it never happened would
  hide a config mistake. So it is counted and surfaced in the health line.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Mapping
from typing import Any

from funding_atlas.recorder.integrity import StreamMonitor
from funding_atlas.recorder.writers import ParquetWriter
from funding_atlas.store.parquet import ASSET_PARTITIONED
from funding_atlas.venues.base import MarketFeed
from funding_atlas.venues.records import STREAMS

__all__ = ["StreamConsumer", "classify"]

#: Keys an envelope may use for the record it wraps. `MarketFeed.stream()` is
#: typed as yielding `dict`, while the writers and the schemas are defined over
#: the canonical dataclasses in `venues/records.py`. Rather than pick one and
#: make the venue adapter's choice load-bearing, both shapes are accepted: a
#: bare canonical record, or a mapping carrying one. The alternative is a seam
#: where a plausible-looking adapter silently records nothing.
_RECORD_KEYS = ("record", "data", "payload")

_RECORD_TYPES = {rtype: name for name, rtype in STREAMS.items()}


def classify(item: Any) -> tuple[str, Any, str | None]:
    """Resolve a feed item into `(stream, record, monitor asset)`.

    The monitor's asset granularity deliberately matches the writers'
    (`ASSET_PARTITIONED`): "the mids stream for BTC is down" and "the BTC mids
    partition has a hole in it" then refer to the same thing, instead of being
    two notions a reader has to reconcile. Venue-wide streams register as
    `None`, which also keeps the monitor from holding one state per perp for a
    stream that has a single arrival cadence anyway.
    """
    record = item
    stream: str | None = None
    if isinstance(item, Mapping):
        for key in _RECORD_KEYS:
            if key in item:
                record = item[key]
                break
        else:
            raise TypeError(
                f"feed yielded a mapping with no record under {_RECORD_KEYS}: {sorted(item)}"
            )
        raw = item.get("stream")
        stream = str(raw) if raw is not None else None

    # The record's own type wins over any label the envelope carries: the type
    # is what decides the Parquet schema, so a disagreement resolved the other
    # way would write the wrong columns.
    resolved = _RECORD_TYPES.get(type(record))
    if resolved is None:
        raise TypeError(f"feed yielded {type(record).__name__}, which is not a recorded stream")
    if stream is not None and stream != resolved:
        raise ValueError(
            f"feed labelled a {type(record).__name__} as stream {stream!r}; expected {resolved!r}"
        )

    asset = record.asset if resolved in ASSET_PARTITIONED else None
    return resolved, record, asset


class StreamConsumer:
    """Drains one `MarketFeed` into the writers and the monitor."""

    __slots__ = ("_monitor", "_writers", "handled", "keys", "unroutable")

    def __init__(self, writers: Mapping[str, ParquetWriter], monitor: StreamMonitor) -> None:
        self._writers = dict(writers)
        self._monitor = monitor
        self.handled: Counter[str] = Counter()
        self.unroutable: Counter[str] = Counter()
        # Every (stream, asset) this consumer has filed a record for. The
        # monitor has no iteration API, and under `universe.mode: all_perps` the
        # asset set is only known once the venue has sent something — so the
        # health line needs someone to remember what it should ask about.
        self.keys: set[tuple[str, str | None]] = set()

    async def handle(self, item: Any) -> None:
        stream, record, asset = classify(item)
        writer = self._writers.get(stream)
        if writer is None:
            # Not an error we can fix at runtime, and not worth losing the other
            # streams over. See the module note.
            self.unroutable[stream] += 1
            return
        await writer.append(record, stream)
        self.handled[stream] += 1
        self.keys.add((stream, asset))
        await self._monitor.record_message(stream, asset)

    async def run(self, feed: MarketFeed, *, stop: asyncio.Event | None = None) -> None:
        """Consume until the feed ends or `stop` is set.

        `stop` is checked per record rather than raced against with a timeout:
        the loop must not abandon a record it has already taken off the feed,
        because nothing will offer it again.
        """
        async for item in feed.stream():
            await self.handle(item)
            if stop is not None and stop.is_set():
                return
