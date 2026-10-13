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


@dataclass(frozen=True, slots=True)
class AssetSpec:
    """Venue trading rules. Strategies must respect these, not assume them.

    These constraints are not cosmetic: a $10 minimum order is what decides
    whether a hedge can be sized accurately on small capital, and tick/lot
    rounding silently turns a delta-neutral pair into a directional bet if
    ignored.
    """

    asset: str
    price_tick: Decimal
    size_lot: Decimal
    min_notional: Decimal
    max_leverage: Decimal
    is_perp: bool


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

    def stream(self) -> AsyncIterator[dict]:
        """Yield normalised market events in non-decreasing time order."""
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
