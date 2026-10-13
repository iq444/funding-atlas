"""Does this window have holes in it?

The reason this module exists at the API boundary rather than in research: a
chart drawn straight across missing data is indistinguishable from a chart of a
flat market, and the consumer has no way to tell. So every response that spans
time carries a `CoverageOut`, and the caller has to work to ignore it.

Two independent sources of truth, unioned:

* **The event log.** `recorder/integrity.py` wrote a `DATA_GAP` event every time
  a stream went silent, errored or shut down. These are authoritative — they
  carry a *reason* — but only cover what some recorder process witnessed and
  managed to record.
* **The data itself.** A regular stream has a known cadence, so a hole between
  two consecutive samples is evidence of loss regardless of what any process
  logged. This catches the gaps the event log cannot have: a recorder that was
  never running, a host that was off, a partition deleted or not yet restored
  from cold storage.

Neither source subsumes the other, which is why both are read and the result is
merged into non-overlapping intervals (`CoverageReport.missing_ns` sums them and
is only exact if they do not overlap).

Irregular streams (`trades`) are deliberately not judged by silence: a quiet
hour on a thin perp produces no trades and is not a gap. That is the same rule
`integrity.py` applies, and for the same reason — see its module docstring.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from funding_atlas.api.duck import NS_PER_S, DuckStore
from funding_atlas.api.models import CoverageOut, GapOut
from funding_atlas.core.events import DataGap, EventType
from funding_atlas.recorder.integrity import DEFAULT_CADENCE_S, CoverageReport

__all__ = ["cadence_for", "coverage_out", "coverage_report", "event_log_gaps", "threshold_ns"]

#: How many consecutive missed samples count as a hole. Three rather than one
#: because a sampled stream jitters: a 1s mids stream routinely delivers two
#: samples 1.4s apart, and reporting that as a gap would make the field useless
#: by crying wolf on every window.
MISSED_SAMPLES = 3.0

#: Mirrors `integrity.max_silence_seconds` in config/recorder.yaml — the point
#: at which the recorder itself declares a stream down. Used as a ceiling on the
#: cadence-derived threshold so that a hole the *recorder* would have called a
#: gap is never reported as healthy by the API, whatever the stream's cadence.
MAX_SILENCE_S = 90.0

#: How long the recorder may buffer a stream before its partition reaches disk.
#: Mirrors `rotation.max_seconds` in `config/recorder.yaml`; override with
#: FA_FLUSH_INTERVAL_S when that changes.
FLUSH_INTERVAL_S = float(os.environ.get("FA_FLUSH_INTERVAL_S", "3600"))

#: Slack over one rotation, so a flush running slightly late is not a hole.
FLUSH_SLACK = 1.5


def settled_before_ns(now_ns: int) -> int:
    """The newest instant for which absence from disk is evidence of absence.

    Everything after this is simply too recent to judge: the recorder buffers
    for up to a rotation, so a stream that is being recorded perfectly has
    nothing on disk for it yet. Inferring a gap there invents a hole that never
    happened — and for a project whose credibility rests on publishing its own
    holes, a false one is as damaging as a hidden one, because a researcher
    discards good data over it.

    Inside this window the event log is the only authority: a real shutdown or
    disconnect is still reported, because it was witnessed rather than inferred.
    """
    return now_ns - int(FLUSH_INTERVAL_S * FLUSH_SLACK * NS_PER_S)


def cadence_for(stream: str) -> float | None:
    """Expected sampling interval, or None for market-driven streams."""
    return DEFAULT_CADENCE_S.get(stream)


def threshold_ns(stream: str) -> int | None:
    """Sample spacing above which a hole is reported, or None if unjudgeable.

    The tighter of "three missed samples" and "the recorder would have called
    this silence". Both bounds matter: the first catches a broken 500ms book
    stream in 1.5s, the second stops a slow stream from needing 4.5 minutes of
    silence before anyone is told.
    """
    cadence = cadence_for(stream)
    if cadence is None:
        return None
    return int(min(cadence * MISSED_SAMPLES, MAX_SILENCE_S) * NS_PER_S)


def event_log_gaps(root: Path, stream: str, asset: str | None) -> list[DataGap]:
    """`DATA_GAP` events from the JSONL event log, if one is on this disk.

    Read directly rather than through `core.eventlog.EventLog`, which is async
    and keyed by a single `account_id`; the API wants every account's view of
    the data plane and must not open the log for append. A torn final line (a
    crash mid-write) is tolerated exactly as the event log tolerates it; a
    malformed line elsewhere is skipped rather than raised, because a coverage
    answer that is merely incomplete still beats a 500 on a health page.
    """
    gaps: list[DataGap] = []
    events_dir = root / "events"
    if not events_dir.is_dir():
        return gaps
    for path in sorted(events_dir.glob("account=*/date=*/*.jsonl")):
        for payload in _data_gap_payloads(path):
            if payload.get("stream") != stream:
                continue
            # A venue-wide gap (`asset is None`) blinded every asset of that
            # stream, so it counts against whichever one was asked about.
            gap_asset = payload.get("asset")
            if not (gap_asset is None or asset is None or gap_asset == asset):
                continue
            parsed = _as_gap(payload)
            if parsed is not None:
                gaps.append(parsed)
    return gaps


def _as_gap(payload: dict[str, Any]) -> DataGap | None:
    # A payload that does not validate is data we cannot interpret, not a bug in
    # this process: an incomplete coverage answer beats a 500 on a health page.
    try:
        return DataGap.model_validate(payload)
    except ValidationError:
        return None


def _data_gap_payloads(path: Path) -> Iterator[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for line in lines:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or event.get("type") != EventType.DATA_GAP.value:
            continue
        payload = event.get("payload")
        if isinstance(payload, dict):
            yield payload


def coverage_report(
    store: DuckStore, stream: str, asset: str | None, from_ns: int, to_ns: int
) -> CoverageReport:
    """Everything known about holes in `[from_ns, to_ns]` for one stream.

    Reuses `integrity.CoverageReport` rather than defining a parallel shape: the
    backtest engine already refuses runs on that type, and an API that disagreed
    with it about what "covered" means would be worse than no API.
    """
    if to_ns < from_ns:
        raise ValueError(f"empty range: {from_ns} -> {to_ns}")

    found: list[DataGap] = [
        gap.model_copy(update={"from_ns": lo, "to_ns": hi})
        for gap in event_log_gaps(store.root, stream, asset)
        if (lo := max(gap.from_ns, from_ns)) < (hi := min(gap.to_ns, to_ns))
    ]
    found.extend(_derived_gaps(store, stream, asset, from_ns, to_ns))
    merged = _merge(found)
    return CoverageReport(
        stream=stream, asset=asset, from_ns=from_ns, to_ns=to_ns, gaps=tuple(merged)
    )


def _derived_gaps(
    store: DuckStore, stream: str, asset: str | None, from_ns: int, to_ns: int
) -> list[DataGap]:
    limit = threshold_ns(stream)
    if limit is None:
        # Market-driven stream: silence is not evidence. See the module note.
        return []

    # Absence is only evidence for the part of the window old enough to have
    # been flushed. See `settled_before_ns`.
    judge_to = min(to_ns, settled_before_ns(time.time_ns()))
    if judge_to <= from_ns:
        # The whole window is too recent to judge from files alone.
        return []

    first, last, count = store.sample_bounds(stream, asset, from_ns, judge_to)
    if count == 0 or first is None or last is None:
        # Nothing at all in the judgeable window. The whole of it is unrecorded —
        # also the honest answer for a dataset that is still empty, and why an
        # empty dataset reports `covered: false` rather than failing.
        return [_gap(stream, asset, from_ns, judge_to, "no samples recorded in range")]

    gaps = [
        _gap(stream, asset, prev, nxt, f"{(nxt - prev) / NS_PER_S:.3f}s between samples")
        for prev, nxt in store.sample_gaps(stream, asset, from_ns, judge_to, limit)
    ]
    # Edges are gaps too, and the trailing one is the common case in production:
    # a window that runs to "now" over a recorder that died ten minutes ago is
    # exactly the chart nobody should draw.
    if first - from_ns > limit:
        gaps.append(_gap(stream, asset, from_ns, first, "no samples before first in range"))
    if judge_to - last > limit:
        gaps.append(_gap(stream, asset, last, judge_to, "no samples after last in range"))
    return gaps


def _gap(stream: str, asset: str | None, from_ns: int, to_ns: int, detail: str) -> DataGap:
    # Reason `silence`: derived from absence, which is the same evidence
    # `integrity.check()` acts on. Anything more specific would be invented —
    # the event log is where a real cause comes from.
    return DataGap(
        stream=stream, asset=asset, from_ns=from_ns, to_ns=to_ns, reason="silence", detail=detail
    )


def _merge(gaps: Iterable[DataGap]) -> list[DataGap]:
    """Collapse overlaps, keeping the attributed reason where there is one.

    Required, not cosmetic: the event log and the cadence check will usually
    both see the same outage, and `CoverageReport.missing_ns` is a plain sum
    that would double-count it.
    """
    ordered = sorted(gaps, key=lambda g: (g.from_ns, g.to_ns))
    out: list[DataGap] = []
    for gap in ordered:
        if out and gap.from_ns <= out[-1].to_ns:
            previous = out[-1]
            # Prefer a reason that came from the event log: `silence` is what
            # the cadence check can infer, anything else was witnessed.
            keep = previous if previous.reason != "silence" else gap
            out[-1] = keep.model_copy(
                update={
                    "from_ns": previous.from_ns,
                    "to_ns": max(previous.to_ns, gap.to_ns),
                }
            )
            continue
        out.append(gap)
    return out


def coverage_out(report: CoverageReport) -> CoverageOut:
    return CoverageOut(
        covered=report.covered,
        missing_s=report.missing_s,
        gaps=[
            GapOut(
                from_ns=gap.from_ns, to_ns=gap.to_ns, reason=gap.reason, duration_s=gap.duration_s
            )
            for gap in report.gaps
        ],
    )
