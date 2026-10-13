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
    side: Side          # aggressor side
    trade_id: str


@dataclass(frozen=True, slots=True)
class Mid:
    """Sampled mid price. Regular cadence, used for marking and features."""

    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    mid: Decimal


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


@dataclass(frozen=True, slots=True)
class OpenInterest:
    venue: str
    asset: str
    ts_event_ns: int
    ts_ingest_ns: int
    open_interest: Decimal


#: Stream name -> record type. Used by writers for partition paths and schemas,
#: and by integrity checks to know which streams must be alive.
STREAMS: dict[str, type] = {
    "trades": Trade,
    "mids": Mid,
    "l2": BookSnapshot,
    "funding": FundingSample,
    "open_interest": OpenInterest,
}
