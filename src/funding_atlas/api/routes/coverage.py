"""`/coverage` — may I trust this window?

The endpoint a backtest calls before it agrees to run, and the one that makes
the rest of this API safe to build research on. It answers with the same type
the recorder's own `integrity.coverage()` answers with, so the API and the
backtest engine cannot disagree about what "covered" means.

Gaps come from two places, unioned and merged — see `api/coverage.py` for why
neither source is sufficient alone.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from funding_atlas.api.coverage import coverage_out, coverage_report
from funding_atlas.api.deps import StoreDep
from funding_atlas.api.duck import NS_PER_S
from funding_atlas.api.models import CoverageOut
from funding_atlas.core.clock import LiveClock
from funding_atlas.venues.records import STREAMS

router = APIRouter(tags=["coverage"])

_CLOCK = LiveClock()

#: Default stream. `funding` because it is the series this API is built around
#: and the one every other endpoint's honesty depends on.
DEFAULT_STREAM = "funding"
DEFAULT_SPAN_NS = 86_400 * NS_PER_S


@router.get("/coverage", response_model=CoverageOut)
def coverage(
    store: StoreDep,
    stream: str = DEFAULT_STREAM,
    asset: Annotated[str | None, Query()] = None,
    from_ns: Annotated[int | None, Query(ge=0)] = None,
    to_ns: Annotated[int | None, Query(ge=0)] = None,
) -> CoverageOut:
    """Gaps in `[from_ns, to_ns]` for one stream, optionally one asset.

    An unknown stream is a 400 rather than an empty answer: `covered: true` for
    a typo'd stream name is the single most dangerous response this API could
    give, because it is the answer that lets a backtest run.
    """
    if stream not in STREAMS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unknown stream {stream!r}; known: {sorted(STREAMS)}",
        )
    to_ns = to_ns if to_ns is not None else _CLOCK.now_ns()
    from_ns = from_ns if from_ns is not None else to_ns - DEFAULT_SPAN_NS
    if from_ns > to_ns:
        from_ns, to_ns = to_ns, from_ns
    return coverage_out(coverage_report(store, stream, asset, from_ns, to_ns))
