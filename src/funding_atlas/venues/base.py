"""Venue interfaces.

The whole architecture rests on these three protocols. A strategy is written
once against them; the mode is chosen by which implementations get injected:

    backtest   ParquetFeed      + SimBroker
    paper      HyperliquidFeed  + SimBroker
    testnet    HyperliquidFeed  + HyperliquidBroker(testnet)
    live       HyperliquidFeed  + HyperliquidBroker(mainnet)

There is no separate "simulator" codebase, which is what normally causes a
backtest and a live run to diverge.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from funding_atlas.venues.records import (
    AssetSpecSample,
    BookSnapshot,
    FundingSample,
    Mid,
    OpenInterest,
    Trade,
)

#: Everything an adapter may emit. The writers' schemas are defined over these.
type MarketRecord = Trade | Mid | BookSnapshot | FundingSample | OpenInterest | AssetSpecSample


@dataclass(frozen=True, slots=True)
class AssetSpec:
    """Venue trading rules. Strategies must respect these, not assume them.

    These constraints are not cosmetic: a $10 minimum order is what decides
    whether a hedge can be sized accurately on small capital, and tick/lot
    rounding silently turns a delta-neutral pair into a directional bet if
    ignored.

    A price must satisfy **both** `price_tick` and `price_sig_figs`. On
    Hyperliquid the two bind at different magnitudes — at 83245 the significant
    figures rule decides, at 0.001234 the decimal places do — so neither can be
    expressed in terms of the other. A validator that checks only the tick
    accepts prices the venue rejects, which is precisely the kind of order the
    risk gate exists to stop before it is sent.
    """

    asset: str
    price_tick: Decimal
    #: Maximum significant figures in a price. Integer prices are exempt on
    #: Hyperliquid. None means the venue imposes no such rule.
    price_sig_figs: int | None
    size_lot: Decimal
    min_notional: Decimal
    max_leverage: Decimal
    is_perp: bool
    #: Listed in the venue's universe but no longer tradable. Carried here
    #: rather than dropped so the `asset_specs` history can say *when* an asset
    #: stopped trading; a spec silently vanishing from the mapping would be
    #: indistinguishable from a poll that never happened.
    is_delisted: bool = False


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """What the strategy wants. Not yet an order.

    `cloid` is the idempotency key, generated before the first send attempt and
    reused on every retry. It is the only thing standing between a flaky
    connection and a doubled position.
    """

    cloid: str
    account_id: str
    asset: str
    side: str  # "buy" | "sell"
    size: Decimal
    price: Decimal | None  # None => market
    reduce_only: bool = False
    post_only: bool = False
    tag: str = ""  # strategy id / leg id, carried into the event log


class MarketFeed(Protocol):
    """Read side. Identical shape whether the bytes come from a socket or a file."""

    async def specs(self) -> dict[str, AssetSpec]: ...

    def stream(self) -> AsyncIterator[MarketRecord]:
        """Yield normalised market events in non-decreasing time order.

        Typed as the canonical records rather than `dict`: the writers' Parquet
        schemas are defined over exactly these, so an adapter yielding anything
        else is a mismatch worth catching at type-check time rather than at the
        writer.
        """
        ...


class Broker(Protocol):
    """Write side.

    Implementations must be idempotent on `cloid`: submitting the same cloid
    twice results in one order. This is a requirement, not a nicety.
    """

    async def submit(self, intent: OrderIntent) -> dict: ...

    async def cancel(self, account_id: str, cloid: str) -> dict: ...

    async def cancel_all(self, account_id: str) -> dict:
        """Used by the kill switch. Must be safe to call repeatedly."""
        ...

    async def positions(self, account_id: str) -> dict:
        """Venue-side truth. Reconciliation compares local state against this,
        never the other way round."""
        ...

    async def open_orders(self, account_id: str) -> dict: ...
