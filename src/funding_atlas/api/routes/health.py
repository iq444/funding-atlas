"""`/health` and `/stats` — is the recorder alive, and what does it hold?

The distinction these two keep apart: `/health` is about *now* (how far behind
is each stream), `/stats` is about the archive (how much history exists, in how
many files, over what range). An operator pages on the first; a researcher sizes
a query with the second.

Health deliberately has three states rather than two. A recorder that started
sixty seconds ago has written nothing, and reporting that as `degraded` trains
whoever is on call to ignore this endpoint — which is worse than having no
endpoint at all. So no data is `empty`, data that has stopped arriving is
`degraded`, and only live streams are `ok`.
"""

from __future__ import annotations

import shutil
from typing import Literal

from fastapi import APIRouter

from funding_atlas.api.coverage import FLUSH_INTERVAL_S, FLUSH_SLACK, MAX_SILENCE_S, cadence_for
from funding_atlas.api.deps import SettingsDep, StoreDep, uptime_s
from funding_atlas.api.duck import NS_PER_S, DuckStore
from funding_atlas.api.models import HealthOut, StatsOut, StreamHealth, StreamStats
from funding_atlas.core.clock import LiveClock
from funding_atlas.venues.records import STREAMS

router = APIRouter(tags=["health"])

_CLOCK = LiveClock()


def _lag_limit_s(stream: str) -> float:
    """How stale a stream may be before `/health` calls it degraded.

    Two terms, and leaving out the second is a mistake worth naming: this
    endpoint measures the age of data **on disk**, while the recorder buffers
    for up to a full rotation before writing. Judging flushed data by the
    stream's sampling cadence alone marks every stream degraded within minutes
    of a healthy recorder's last flush, and a status page that is permanently
    red is one nobody reads — which defeats the purpose of publishing it.

    So the limit is the recorder's own silence threshold *plus* the time data
    may legitimately sit in a buffer. Market-driven streams (trades) still get
    no limit at all: a quiet perp is silent for legitimate reasons and must
    never turn the page red, the same rule `recorder/integrity.py` applies.
    """
    cadence = cadence_for(stream)
    if cadence is None:
        return float("inf")
    return MAX_SILENCE_S + FLUSH_INTERVAL_S * FLUSH_SLACK


@router.get("/health", response_model=HealthOut)
def health(store: StoreDep) -> HealthOut:
    now_ns = _CLOCK.now_ns()
    streams: dict[str, StreamHealth] = {}
    degraded = False
    for stream in STREAMS:
        last_ns, rows = store.last_write_ns(stream)
        if last_ns is None:
            # Stream never wrote. Reported with nulls rather than omitted, so a
            # dashboard renders a row for a stream that is configured-but-silent
            # instead of silently dropping it off the page.
            streams[stream] = StreamHealth(last_write_ns=None, lag_s=None, rows=rows)
            continue
        lag_s = (now_ns - last_ns) / NS_PER_S
        streams[stream] = StreamHealth(last_write_ns=last_ns, lag_s=lag_s, rows=rows)
        if lag_s > _lag_limit_s(stream):
            degraded = True

    status: Literal["ok", "degraded", "empty"]
    if not any(s.rows for s in streams.values()):
        status = "empty"
    elif degraded:
        status = "degraded"
    else:
        status = "ok"

    return HealthOut(
        status=status,
        streams=streams,
        disk_free_bytes=_disk_free_bytes(store),
        uptime_s=uptime_s(),
    )


def _disk_free_bytes(store: DuckStore) -> int:
    """Free space on the volume holding the data directory.

    Falls back to the nearest existing parent: the recorder creates partition
    directories lazily, so `FA_DATA_DIR` itself may not exist yet on a host that
    has only ever run the API. Zero is returned rather than raising — a health
    endpoint that 500s because it could not stat a path has failed at its one
    job.
    """
    path = store.root
    while not path.exists() and path != path.parent:
        path = path.parent
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 0


@router.get("/stats", response_model=StatsOut)
def stats(store: StoreDep, settings: SettingsDep) -> StatsOut:
    streams: dict[str, StreamStats] = {}
    for stream in STREAMS:
        first_ns, last_ns, rows = store.span(stream)
        files = store.files_for(stream)
        streams[stream] = StreamStats(
            rows=rows,
            first_ns=first_ns,
            last_ns=last_ns,
            files=files.files,
            bytes=files.bytes,
        )
    return StatsOut(streams=streams, venue=settings.venue, network=settings.network)
