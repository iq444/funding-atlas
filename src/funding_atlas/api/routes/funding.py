"""`/funding/*` — current cross-section, history, and the carry screener.

Normalisation happens exactly once in this package, in `screener.hourly_rate`,
and every number served from here goes through it. The venue's `interval_s`
travels with its `rate` from the Parquet row to that call, so the hourly figure
is always derived from the interval the sample was quoted at. Hyperliquid quotes
per hour (docs/hyperliquid-api.md §6), which makes the conversion an identity
today — and that is the whole point of writing it as a conversion: a venue that
moves to 8-hour funding gets divided by 8 here rather than inflating every
number on the dashboard by 8x with nothing to show it happened.
"""

from __future__ import annotations

from decimal import Decimal, localcontext
from typing import Annotated, Literal

from fastapi import APIRouter, Query

from funding_atlas.api import screener as sc
from funding_atlas.api.coverage import coverage_out, coverage_report
from funding_atlas.api.deps import StoreDep
from funding_atlas.api.duck import NS_PER_S, BucketRow, DuckStore
from funding_atlas.api.models import (
    CoverageOut,
    FundingCurrentOut,
    FundingCurrentRow,
    FundingHistoryOut,
    FundingHistoryPoint,
    ScreenerAssumptions,
    ScreenerOut,
    ScreenerRow,
)
from funding_atlas.core.clock import LiveClock

router = APIRouter(prefix="/funding", tags=["funding"])

_CLOCK = LiveClock()

#: Chart buckets. Only two, because these are the two a funding chart is read
#: at: the hour is the settlement period, the day is the carry horizon. Adding
#: `1m` would invite a chart finer than the recorder's 60s poll, which would
#: plot the poll cadence rather than the market.
BUCKET_NS: dict[str, int] = {
    "1h": 3600 * NS_PER_S,
    "1d": 86_400 * NS_PER_S,
}

#: Default history window per bucket: enough points to be a chart, few enough to
#: be one response.
DEFAULT_SPAN_NS: dict[str, int] = {
    "1h": 7 * 86_400 * NS_PER_S,
    "1d": 180 * 86_400 * NS_PER_S,
}

Bucket = Literal["1h", "1d"]


#: Delisted perps keep reporting a funding rate of exactly zero forever. Left in,
#: they are a quarter of the cross-section and every one of them is untradeable,
#: so a reader scanning for opportunity is scanning mostly noise. They are hidden
#: by default and available with `include_delisted=true`, because a historical
#: question — what was this market doing before it died — is a real one.
INCLUDE_DELISTED_DESC = "Include perps the venue has delisted. They are untradeable."


@router.get("/current", response_model=FundingCurrentOut)
def current(
    store: StoreDep,
    include_delisted: Annotated[bool, Query(description=INCLUDE_DELISTED_DESC)] = False,
) -> FundingCurrentOut:
    """Latest funding per asset, sorted by APR descending.

    One row per asset from its own most recent sample, so a perp whose poll
    landed a few seconds later is not dropped from the cross-section. `as_of_ns`
    is the newest of those timestamps — the caller can compare it against a row's
    staleness itself, which is better than this endpoint silently dropping rows
    it judges old.
    """
    samples = store.latest_funding()
    if not include_delisted:
        delisted = store.delisted_assets()
        samples = [s for s in samples if s.asset not in delisted]
    if not samples:
        return FundingCurrentOut(as_of_ns=None, rows=[])

    oi_by_asset = store.latest_open_interest()
    rows: list[FundingCurrentRow] = []
    for sample in samples:
        hourly = sc.hourly_rate(sample.rate, sample.interval_s)
        open_interest = oi_by_asset.get(sample.asset)
        rows.append(
            FundingCurrentRow(
                asset=sample.asset,
                rate_hourly=hourly,
                apr=sc.annualise(hourly),
                mark=sample.mark_price,
                open_interest=open_interest,
                oi_notional=_oi_notional(open_interest, sample.mark_price),
            )
        )
    rows.sort(key=lambda row: row.apr, reverse=True)
    return FundingCurrentOut(as_of_ns=max(s.ts_event_ns for s in samples), rows=rows)


def _oi_notional(open_interest: Decimal | None, mark: Decimal | None) -> Decimal | None:
    """Open interest in dollars.

    `openInterest` is in **coin units** (docs/hyperliquid-api.md §8), so it is
    meaningless until multiplied by a price, and a series that mixes the two is
    unusable in a way no single sample reveals. Null rather than zero when
    either side is missing: zero would sort as a real, tiny market.
    """
    if open_interest is None or mark is None:
        return None
    return open_interest * mark


@router.get("/history", response_model=FundingHistoryOut)
def history(
    store: StoreDep,
    asset: Annotated[str, Query(min_length=1)],
    from_ns: Annotated[int | None, Query(ge=0)] = None,
    to_ns: Annotated[int | None, Query(ge=0)] = None,
    bucket: Bucket = "1h",
) -> FundingHistoryOut:
    """Bucketed funding history for one asset, with its coverage attached.

    The `coverage` block is not decoration. A funding chart is a line, and a line
    drawn between two points an outage apart looks exactly like a line drawn
    through data — so the response states where the holes are and the caller has
    to go out of its way to plot across one.
    """
    bucket_ns = BUCKET_NS[bucket]
    to_ns = to_ns if to_ns is not None else _CLOCK.now_ns()
    from_ns = from_ns if from_ns is not None else to_ns - DEFAULT_SPAN_NS[bucket]
    from_ns, to_ns = _ordered(from_ns, to_ns)

    buckets = store.funding_buckets(asset, from_ns, to_ns, bucket_ns)
    points = [_point(ts, rows) for ts, rows in _group_by_bucket(buckets)]
    return FundingHistoryOut(
        asset=asset,
        bucket=bucket,
        points=points,
        coverage=_funding_coverage(store, asset, from_ns, to_ns),
    )


def _group_by_bucket(buckets: list[BucketRow]) -> list[tuple[int, list[BucketRow]]]:
    grouped: dict[int, list[BucketRow]] = {}
    for row in buckets:
        grouped.setdefault(row.bucket_ns, []).append(row)
    return sorted(grouped.items())


def _point(bucket_ns: int, rows: list[BucketRow]) -> FundingHistoryPoint:
    """Mean hourly rate over a bucket, normalised per cadence before averaging.

    The average is over samples, not time-weighted: the recorder polls on a
    fixed cadence, so the samples are already evenly spaced and a time weighting
    would only differ where a gap exists — and a gap is reported rather than
    smoothed over.

    Each `BucketRow` is one `interval_s` group, so the rates inside a single
    `rate_sum` share a cadence and can be normalised with one multiplication.
    Summing across cadences first and dividing afterwards is the mistake this
    grouping exists to prevent.
    """
    # One shared precision for every money figure this API emits.
    with localcontext(sc.MONEY_CONTEXT):
        total_samples = sum(row.samples for row in rows)
        hourly_total = sum(
            (sc.hourly_rate(row.rate_sum, row.interval_s) for row in rows), Decimal(0)
        )
        hourly = hourly_total / total_samples
        mark_samples = sum(row.mark_samples for row in rows)
        mark_total = sum((row.mark_sum for row in rows if row.mark_sum is not None), Decimal(0))
        # Mark is nullable on disk, so a bucket can legitimately have rates and
        # no prices. Null, not zero: a zero mark would render as a crashed market.
        mark = mark_total / mark_samples if mark_samples else None
        return FundingHistoryPoint(
            ts_ns=bucket_ns,
            rate_hourly=hourly,
            apr=sc.annualise(hourly),
            mark=mark,
        )


@router.get("/screener", response_model=ScreenerOut)
def screener(
    store: StoreDep,
    notional: Annotated[Decimal, Query(gt=0)] = Decimal(1000),
    holding_hours: Annotated[Decimal, Query(gt=0)] = Decimal(24),
    side: Literal["short", "long"] = "short",
    include_delisted: Annotated[bool, Query(description=INCLUDE_DELISTED_DESC)] = False,
) -> ScreenerOut:
    """Rank the universe by what a carry actually nets after fees.

    Sorted by `net_apr` descending rather than by `apr`: gross APR ranks a
    hundred micro-cap perps whose funding cannot pay four fills above the one
    trade that works, which is precisely the mistake every other funding table
    makes. `breakeven_hours` is the field to read.
    """
    samples = store.latest_funding()
    if not include_delisted:
        delisted = store.delisted_assets()
        samples = [s for s in samples if s.asset not in delisted]
    inputs = sc.ScreenerInputs(notional=notional, holding_hours=holding_hours, side=side)
    as_of_ns = max((s.ts_event_ns for s in samples), default=None)
    assumptions = _assumptions(store, inputs, as_of_ns)

    if not samples:
        return ScreenerOut(
            assumptions=assumptions,
            rows=[],
            coverage=_screener_coverage(store, inputs, as_of_ns),
        )

    oi_by_asset = store.latest_open_interest()
    rows: list[ScreenerRow] = []
    for sample in samples:
        result = sc.screen(sample.rate, sample.interval_s, inputs)
        rows.append(
            ScreenerRow(
                asset=sample.asset,
                apr=result.apr,
                gross_pnl=result.gross_pnl,
                fee_cost=result.fee_cost,
                net_pnl=result.net_pnl,
                net_apr=result.net_apr,
                breakeven_hours=result.breakeven_hours,
                oi_notional=_oi_notional(oi_by_asset.get(sample.asset), sample.mark_price),
                covers_fees=result.covers_fees,
            )
        )
    rows.sort(key=lambda row: row.net_apr, reverse=True)
    return ScreenerOut(
        assumptions=assumptions,
        rows=rows,
        coverage=_screener_coverage(store, inputs, as_of_ns),
    )


def _assumptions(
    store: DuckStore, inputs: sc.ScreenerInputs, as_of_ns: int | None
) -> ScreenerAssumptions:
    return ScreenerAssumptions(
        notional=inputs.notional,
        holding_hours=inputs.holding_hours,
        side=inputs.side,
        taker_fee_rate=sc.TAKER_FEE_RATE,
        maker_fee_rate=sc.MAKER_FEE_RATE,
        fee_role="taker",
        legs=sc.NEUTRAL_LEGS,
        fills=inputs.fills,
        fee_model=(
            f"round trip, taker on every fill: {sc.NEUTRAL_LEGS} legs "
            f"(perp + hedge) x {sc.FILLS_PER_LEG} fills (entry + exit) "
            f"= {sc.ROUND_TRIP_FILLS} fills, each charged on full notional"
        ),
        hours_per_year=sc.HOURS_PER_YEAR,
        annualisation=("rate_hourly * 24 * 365, where rate_hourly = rate * 3600 / interval_s"),
        # Read off the data, not asserted: if this comes back as anything other
        # than [3600] the venue changed cadence and the caller should know the
        # normalisation above did real work.
        funding_interval_s=store.funding_intervals(),
        fee_source=sc.FEE_SOURCE,
        as_of_ns=as_of_ns,
    )


def _funding_coverage(store: DuckStore, asset: str, from_ns: int, to_ns: int) -> CoverageOut:
    return coverage_out(coverage_report(store, "funding", asset, from_ns, to_ns))


def _screener_coverage(
    store: DuckStore, inputs: sc.ScreenerInputs, as_of_ns: int | None
) -> CoverageOut:
    """Coverage of the window the screener extrapolates from.

    The rows are built from one sample per asset, but the claim they make is
    about the next `holding_hours`, justified by the recent past. So the window
    reported is the last `holding_hours` of funding across the venue, ending
    now: if the recorder was down for half of it, the carry shown is an
    extrapolation from a hole and the caller is told so.
    """
    now_ns = _CLOCK.now_ns()
    window_ns = int(inputs.holding_hours * 3600) * NS_PER_S
    from_ns, to_ns = _ordered(now_ns - window_ns, now_ns)
    return coverage_out(coverage_report(store, "funding", None, from_ns, to_ns))


def _ordered(from_ns: int, to_ns: int) -> tuple[int, int]:
    """Swap a reversed range rather than rejecting it.

    A dashboard that sends `from` and `to` the wrong way round wants the window
    between them; a 422 for an argument order mistake helps nobody.
    """
    return (from_ns, to_ns) if from_ns <= to_ns else (to_ns, from_ns)
