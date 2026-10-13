"""Canonical market-data records.

The contract every venue adapter normalises *into* and every writer persists
*from*. Defined before the adapters so that storage, gap detection and research
can be built against a stable shape rather than against Hyperliquid's wire
format.

Rules that apply to all of them:

* Prices and sizes are `Decimal`. Never float. Float rounding on a two-leg
  hedge produces a position that is nearly neutral, which is the same thing as
  directional.
* Two timestamps: `ts_event_ns` as stamped by the venue, `ts_ingest_ns` from our
  clock. The difference is observable latency and is kept as data, not
  normalised away.
* Some venue streams carry no time at all (on Hyperliquid, `allMids` and
  `activeAssetCtx`). There `ts_event_ns` is filled from our clock and
  `ts_event_synthetic` is set, so research can tell a venue timestamp from our
  guess instead of silently measuring our own latency against itself. The field
  is never dropped to keep the column non-nullable: a nullable timestamp would
  push the decision onto every consumer, and partitioning needs a value.
* Records are immutable. History is append-only; a correction is a new record,
  never an edit.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

Side = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class Trade:
    """One execution on the public tape."""

    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    price: Decimal
    size: Decimal
    side: Side  # aggressor side
    #: Venue trade id. NOT globally unique on Hyperliquid — `tid` there is a
    #: 50-bit hash, so dedupe on (venue, asset, ts_event_ns, trade_id).
    trade_id: str
    #: True when the venue sent no timestamp and `ts_event_ns` is our own.
    ts_event_synthetic: bool = False


@dataclass(frozen=True, slots=True)
class Mid:
    """Sampled mid price. Regular cadence, used for marking and features."""

    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    mid: Decimal
    #: True when the venue sent no timestamp and `ts_event_ns` is our own.
    ts_event_synthetic: bool = False


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    size: Decimal
    n_orders: int | None = None


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Top-N of both sides at an instant.

    Snapshots rather than a reconstructed incremental book: a snapshot that
    arrives late is still correct, while a dropped increment silently corrupts
    every subsequent state. Given that gaps are expected, correctness under loss
    matters more than fidelity between samples.
    """

    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    #: True when the venue sent no timestamp and `ts_event_ns` is our own.
    ts_event_synthetic: bool = False

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None


@dataclass(frozen=True, slots=True)
class FundingSample:
    """Funding state at a point in time.

    `rate` is the venue's quoted rate for its own interval — not annualised.
    Annualisation is a research concern and is done where the interval is known,
    so that a change in venue cadence cannot silently rescale history.
    """

    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    rate: Decimal
    interval_s: int
    premium: Decimal | None = None
    oracle_price: Decimal | None = None
    mark_price: Decimal | None = None
    #: True when the venue sent no timestamp and `ts_event_ns` is our own.
    ts_event_synthetic: bool = False


@dataclass(frozen=True, slots=True)
class OpenInterest:
    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    open_interest: Decimal
    #: True when the venue sent no timestamp and `ts_event_ns` is our own.
    ts_event_synthetic: bool = False


@dataclass(frozen=True, slots=True)
class AssetSpecSample:
    """The venue's trading rules for one asset, as they stood at one instant.

    Recorded rather than derived at read time because the rules *change*: perps
    are listed and delisted, `szDecimals` is widened, `maxLeverage` is cut after
    a volatility event. A backtest must size and round its orders by the rules
    that applied at the simulated instant, not by the ones the venue happens to
    publish on the day the backtest runs — sizing a 2024 trade with 2026's lot
    step produces an order that could never have been placed, which is the
    quietest way to manufacture a profitable fill.

    Like the order book and unlike funding, this cannot be backfilled: `meta`
    serves only the current rules and the venue publishes no history of them. A
    sample not taken is a sample lost.

    The fields mirror `venues.base.AssetSpec` — this is that spec with a
    timestamp on it — so a consumer rebuilding the spec for an instant maps the
    columns across one-to-one rather than re-deriving anything.

    `meta` carries no timestamp of its own (API doc §4), so `ts_event_ns` is
    always our clock's and `ts_event_synthetic` is always set. It defaults to
    True here, the opposite way round from the pushed streams, because there is
    no code path on which this record could carry a venue stamp.
    """

    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    #: Finest price step ever legal for this asset. Necessary but not
    #: sufficient: `price_sig_figs` binds instead at large magnitudes, so both
    #: are stored. See `AssetSpec` for why neither subsumes the other.
    price_tick: Decimal
    #: None means the venue imposes no significant-figure rule, which is not
    #: the same as "we did not look".
    price_sig_figs: int | None
    size_lot: Decimal
    min_notional: Decimal
    max_leverage: Decimal
    is_perp: bool
    #: Still listed in the venue's universe but no longer tradable. Kept as a
    #: flag rather than as an absent row: an asset that stops appearing would be
    #: indistinguishable from a poll we never made.
    is_delisted: bool = False
    #: Always True for this stream — see the class docstring.
    ts_event_synthetic: bool = True


#: Stream name -> record type. Used by writers for partition paths and schemas,
#: and by integrity checks to know which streams must be alive.
STREAMS: dict[str, type] = {
    "trades": Trade,
    "mids": Mid,
    "l2": BookSnapshot,
    "funding": FundingSample,
    "open_interest": OpenInterest,
    "asset_specs": AssetSpecSample,
}
