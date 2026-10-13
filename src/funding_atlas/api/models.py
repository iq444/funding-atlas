"""Response models for the public Data API.

One rule dominates this module: **money leaves as a JSON string.** Every price,
rate, size and PnL figure on disk is `decimal128(38,18)` (see
`store/parquet.py`), and emitting it as a JSON number hands it to a consumer's
float parser, where a funding rate of `0.0000125` and a mark of `112345.5` can
no longer both be exact. A dashboard that rounds a rate by one part in 1e16 is
harmless; a backtest or an invoice built on the same response is not, and the
API cannot tell which caller it is talking to. So `DecimalStr` serialises to a
string and the decision is taken away from the consumer.

Timestamps stay raw int64 nanoseconds for the same reason they do on disk: they
are the integers the venue sent, and a float seconds value cannot hold them.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, PlainSerializer

__all__ = [
    "AssetOut",
    "AssetsOut",
    "CoverageOut",
    "DecimalStr",
    "FundingCurrentOut",
    "FundingCurrentRow",
    "FundingHistoryOut",
    "FundingHistoryPoint",
    "GapOut",
    "HealthOut",
    "Nanos",
    "OptNanos",
    "ScreenerAssumptions",
    "ScreenerOut",
    "ScreenerRow",
    "StatsOut",
    "StreamHealth",
    "StreamStats",
    "decimal_str",
    "nanos_str",
]


def decimal_str(value: Decimal) -> str:
    """Shortest exact fixed-point rendering of `value`.

    DuckDB returns every decimal column padded to the column's scale, so a rate
    arrives as `0.000012500000000000`. The padding carries no information, and
    `normalize()` drops it without changing the value. `format(..., "f")` then
    forbids the exponent form `normalize()` can produce for integers (`1E+2`),
    because `"1E+2"` is a string many JSON consumers will not read back as a
    number at all.

    Note: stripping the padding does not lose the venue's quoted precision —
    that is already gone, because the Parquet column has a fixed scale of 18 and
    a mark quoted as `3.4210` is stored as `3.421000000000000000`. The digit
    count the venue used is not recoverable here, so preserving the padding
    would assert a precision we do not have. `price_tick` from the asset specs
    is the honest source for that question.
    """
    return format(value.normalize(), "f")


def nanos_str(value: int | None) -> str | None:
    """Nanosecond timestamps serialise as JSON strings, like the decimals.

    A current ns timestamp is ~1.8e18 while JavaScript's Number.MAX_SAFE_INTEGER
    is 9.007e15, so a JSON *number* is silently rounded to roughly 256ns by every
    browser client — and a value echoed back as a `from_ns` query parameter is
    then simply wrong. This is the same precision failure the project refuses for
    prices, in the layer built to display them; it was found by the first
    consumer, not by a test.

    Query parameters still accept plain integers: the loss is in the JSON number
    type, not in the decimal digits of a string.
    """
    return None if value is None else str(value)


#: An int that serialises as a JSON string. Use for every `*_ns` field.
Nanos = Annotated[int, PlainSerializer(nanos_str, return_type=str, when_used="json")]
OptNanos = Annotated[
    int | None, PlainSerializer(nanos_str, return_type=str | None, when_used="json")
]


#: A `Decimal` that serialises as a JSON string. See the module docstring.
DecimalStr = Annotated[Decimal, PlainSerializer(decimal_str, return_type=str, when_used="json")]


class _Out(BaseModel):
    # Frozen: a response object is a value, and nothing downstream of
    # construction has any business editing one.
    model_config = ConfigDict(frozen=True)


class GapOut(_Out):
    """A hole in the record, clipped to the window that was asked about."""

    from_ns: Nanos
    to_ns: Nanos
    reason: str
    duration_s: float


class CoverageOut(_Out):
    """Whether a window can be trusted.

    Attached to every response that spans time, because a chart drawn straight
    across missing data is a lie that looks exactly like data.
    """

    covered: bool
    missing_s: float
    gaps: list[GapOut] = []


class StreamHealth(_Out):
    #: None when the stream has no rows yet — a fresh recorder, not an error.
    last_write_ns: OptNanos
    lag_s: float | None
    rows: int


class HealthOut(_Out):
    #: `empty` is distinct from `degraded`: a recorder that started a minute ago
    #: has written nothing and is perfectly healthy, and paging on that is how
    #: an operator learns to ignore the health endpoint.
    status: Literal["ok", "degraded", "empty"]
    streams: dict[str, StreamHealth]
    disk_free_bytes: int
    uptime_s: float


class AssetOut(_Out):
    asset: str
    # Nullable because these are venue *metadata*, and the recorder does not
    # persist a metadata stream — see `routes/assets.py` for what is derived
    # from observed data and what cannot be.
    price_tick: DecimalStr | None
    price_sig_figs: int | None
    size_lot: DecimalStr | None
    min_notional: DecimalStr | None
    #: A string like every other quantity that multiplies notional. `price_sig_figs`
    #: stays an int because it counts digits rather than measuring anything — the
    #: rule is "numbers you do money arithmetic with are strings".
    max_leverage: DecimalStr | None
    first_seen_ns: OptNanos
    last_seen_ns: OptNanos
    has_l2: bool
    #: Where the trading rules came from. "recorded" is the venue's own `meta`
    #: as it was at that time; "inferred" is reconstructed from observed trade
    #: sizes and is a bound, not a rule. A consumer sizing an order must not
    #: treat the two alike.
    source: Literal["recorded", "inferred"] = "inferred"


class AssetsOut(_Out):
    assets: list[AssetOut] = []
    #: Latest observation across the universe, so a caller can tell an empty
    #: dataset from a stale one.
    as_of_ns: OptNanos = None


class FundingCurrentRow(_Out):
    asset: str
    rate_hourly: DecimalStr
    apr: DecimalStr
    mark: DecimalStr | None
    open_interest: DecimalStr | None
    oi_notional: DecimalStr | None


class FundingCurrentOut(_Out):
    as_of_ns: OptNanos = None
    rows: list[FundingCurrentRow] = []


class FundingHistoryPoint(_Out):
    ts_ns: Nanos
    rate_hourly: DecimalStr
    apr: DecimalStr
    mark: DecimalStr | None


class FundingHistoryOut(_Out):
    asset: str
    bucket: str
    points: list[FundingHistoryPoint] = []
    coverage: CoverageOut


class ScreenerAssumptions(_Out):
    """Everything the numbers in `rows` rest on, stated rather than implied.

    A carry screener is a pile of assumptions wearing a table's clothing. If the
    fee tier or the fill model is wrong, every row is wrong by the same large
    factor and nothing in the output would reveal it — so the inputs ship with
    the output.
    """

    notional: DecimalStr
    holding_hours: DecimalStr
    side: Literal["short", "long"]
    taker_fee_rate: DecimalStr
    maker_fee_rate: DecimalStr
    fee_role: Literal["taker", "maker"]
    #: Stated as zero rather than left out. An unmodelled cost that is simply
    #: absent from the assumptions reads as "not a factor", when in fact it is
    #: the largest unknown on a thin book. Silence is itself an assumption, so
    #: it is given a name and a number a caller can argue with.
    slippage_bps: DecimalStr = Decimal(0)
    #: Funding is assumed to persist at the current rate for the whole holding
    #: period. It does not; this is the screener's weakest premise.
    rate_is_assumed_constant: bool = True
    legs: int
    fills: int
    fee_model: str
    hours_per_year: int
    annualisation: str
    #: Distinct `interval_s` values seen in the samples these rows came from.
    #: Anything other than `[3600]` means the venue changed cadence and the
    #: hourly normalisation did real work.
    funding_interval_s: list[int]
    fee_source: str
    as_of_ns: OptNanos = None


class ScreenerRow(_Out):
    asset: str
    apr: DecimalStr
    gross_pnl: DecimalStr
    fee_cost: DecimalStr
    net_pnl: DecimalStr
    net_apr: DecimalStr
    #: None when funding runs against this side: no holding period ever pays
    #: the fees back, and `0` or a huge number would both read as a real answer.
    breakeven_hours: DecimalStr | None
    oi_notional: DecimalStr | None
    covers_fees: bool


class ScreenerOut(_Out):
    assumptions: ScreenerAssumptions
    rows: list[ScreenerRow] = []
    coverage: CoverageOut


class StreamStats(_Out):
    rows: int
    first_ns: OptNanos
    last_ns: OptNanos
    files: int
    bytes: int


class StatsOut(_Out):
    streams: dict[str, StreamStats] = {}
    venue: str
    network: str
