"""Carry screener arithmetic: the one view this API exists to serve.

Everything else here reports what the recorder saw. This module makes a claim —
"holding this position for N hours nets you X" — and a wrong claim is worse than
no endpoint, because it looks exactly like a right one. So the arithmetic is
spelled out, every constant is named, and every assumption is shipped to the
caller in the response rather than living only in this file.

The three ways a funding screener is normally wrong:

1. **The interval.** On Hyperliquid the quoted rate is already **per hour**
   (docs/hyperliquid-api.md §6 — verified against `fundingHistory`, which
   returned the 8-hour interest floor divided by 8). Annualising a per-hour rate
   as though it were per-8-hours understates it by 8x; the reverse overstates it
   by 8x. Nothing here hardcodes the cadence: `hourly_rate` reads `interval_s`
   off the sample and normalises from it, so a venue that moves to 8-hour
   funding changes the numbers correctly instead of silently rescaling the page.
2. **The fees.** A round trip on a delta-neutral carry is four fills, not one
   and not two. `fee_cost` says so and `assumptions.fills` publishes it.
3. **The headline number.** `apr` is a yield, and a yield says nothing about
   whether the trade clears its costs. `breakeven_hours` does, which is why it
   is computed per row even though it is more expensive to explain.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, localcontext
from typing import Literal

__all__ = [
    "FEE_SOURCE",
    "FILLS_PER_LEG",
    "HOURS_PER_YEAR",
    "MAKER_FEE_RATE",
    "MONEY_CONTEXT",
    "NEUTRAL_LEGS",
    "ROUND_TRIP_FILLS",
    "SECONDS_PER_HOUR",
    "TAKER_FEE_RATE",
    "ScreenerInputs",
    "ScreenerResult",
    "Side",
    "annualise",
    "fee_cost",
    "hourly_rate",
    "screen",
    "side_sign",
]

Side = Literal["short", "long"]

# Hyperliquid's base tier, per side, per fill. 0.045% taker / 0.015% maker.
# Source: docs/hyperliquid-api.md (fees section) — the same document §6 of which
# establishes that funding is quoted per hour. Expressed as a rate, not bps, so
# no factor of 10_000 can go missing between here and a response.
#
# These are *base tier*. A caller with volume discounts, a staking tier or a
# maker rebate has lower fees and a shorter breakeven, which is why the rates
# are published in `assumptions` instead of being buried: a screener whose fee
# assumption is invisible is a screener nobody can correct.
TAKER_FEE_RATE = Decimal("0.00045")
MAKER_FEE_RATE = Decimal("0.00015")
FEE_SOURCE = "docs/hyperliquid-api.md"

#: A delta-neutral carry is two positions: the perp paying/receiving funding and
#: the hedge against it.
NEUTRAL_LEGS = 2
#: Entry and exit. A "round trip" that charges once is the most common way a
#: carry screener doubles its own net PnL.
FILLS_PER_LEG = 2
ROUND_TRIP_FILLS = NEUTRAL_LEGS * FILLS_PER_LEG  # 4

SECONDS_PER_HOUR = 3600
#: 24 * 365. Not 24 * 365.25: the venue settles funding on hours, the quoted
#: convention is a plain year, and inventing a leap-day correction would make
#: this API's APR disagree with every other venue's by 0.07% for no gain.
HOURS_PER_YEAR = 24 * 365

# Divisions below are exact-until-rounded, so the precision is pinned here
# rather than inherited from whatever the calling thread's context happens to
# be. 40 digits is far beyond any funding figure and keeps results identical
# across processes.
MONEY_CONTEXT = Context(prec=40)


@dataclass(frozen=True, slots=True)
class ScreenerInputs:
    notional: Decimal
    holding_hours: Decimal
    side: Side
    fee_rate: Decimal = TAKER_FEE_RATE
    fills: int = ROUND_TRIP_FILLS


@dataclass(frozen=True, slots=True)
class ScreenerResult:
    """Per-asset screener arithmetic. All `Decimal`; no float ever touches it."""

    #: Hourly funding as it accrues to *this* side: positive means received.
    carry_hourly: Decimal
    apr: Decimal
    gross_pnl: Decimal
    fee_cost: Decimal
    net_pnl: Decimal
    net_apr: Decimal
    #: None when funding runs against this side at any holding period.
    breakeven_hours: Decimal | None
    covers_fees: bool


def hourly_rate(rate: Decimal, interval_s: int) -> Decimal:
    """Normalise a venue-quoted rate to per hour using its own interval.

    `rate * 3600 / interval_s`. On Hyperliquid `interval_s` is 3600 and this is
    the identity — which is the point: the identity is *derived*, so a venue
    that quotes an 8-hour rate (`interval_s = 28800`) is divided by 8 here
    instead of being annualised 8x too high somewhere downstream.
    """
    if interval_s <= 0:
        raise ValueError(f"interval_s must be positive, got {interval_s}")
    with localcontext(MONEY_CONTEXT):
        return rate * SECONDS_PER_HOUR / interval_s


def annualise(hourly: Decimal) -> Decimal:
    """`rate_hourly * 24 * 365`. Simple, not compounded.

    Uncompounded on purpose: funding is paid out in cash hourly rather than
    accruing to the position, and a compounded figure would claim a reinvestment
    the venue does not perform.
    """
    with localcontext(MONEY_CONTEXT):
        return hourly * HOURS_PER_YEAR


def side_sign(side: Side) -> int:
    """+1 for a short, -1 for a long.

    Positive funding means longs pay shorts, so a short receives it. A negative
    rate is a real, tradeable opportunity for the other side, not a row to
    filter out — flipping the sign is what makes the screener symmetric instead
    of quietly hiding half the market.
    """
    return 1 if side == "short" else -1


def fee_cost(notional: Decimal, fee_rate: Decimal, fills: int) -> Decimal:
    """`notional * fee_rate * fills` — the whole round trip.

    Fees are charged on notional per fill, and both legs of a neutral pair are
    sized at the same notional, so one multiplication covers all four fills.
    """
    with localcontext(MONEY_CONTEXT):
        return notional * fee_rate * fills


def screen(rate: Decimal, interval_s: int, inputs: ScreenerInputs) -> ScreenerResult:
    """The full per-asset computation.

        rate_hourly    = rate * 3600 / interval_s
        carry_hourly   = rate_hourly * (+1 short | -1 long)
        apr            = carry_hourly * 24 * 365
        gross_pnl      = notional * carry_hourly * holding_hours
        fee_cost       = notional * taker_fee * 4          (2 legs x entry+exit)
        net_pnl        = gross_pnl - fee_cost
        net_apr        = (net_pnl / notional) * (24 * 365 / holding_hours)
        breakeven_h    = fee_cost / (notional * carry_hourly)   if carry > 0
        covers_fees    = holding_hours >= breakeven_h

    `net_apr` annualises the *net* return of this specific holding period, so a
    6-hour hold that barely clears its fees shows a small net APR while its
    gross APR is enormous. That difference is the entire output of this
    endpoint.
    """
    if inputs.holding_hours <= 0:
        raise ValueError("holding_hours must be positive")
    if inputs.notional <= 0:
        raise ValueError("notional must be positive")

    with localcontext(MONEY_CONTEXT):
        carry = hourly_rate(rate, interval_s) * side_sign(inputs.side)
        apr = annualise(carry)
        gross = inputs.notional * carry * inputs.holding_hours
        fees = fee_cost(inputs.notional, inputs.fee_rate, inputs.fills)
        net = gross - fees
        net_apr = (net / inputs.notional) * HOURS_PER_YEAR / inputs.holding_hours
        # Breakeven only exists while funding accrues *towards* this side. At
        # zero or negative carry no holding period repays the fees, and
        # reporting a negative or infinite number would read as an answer.
        breakeven = fees / (inputs.notional * carry) if carry > 0 else None
        return ScreenerResult(
            carry_hourly=carry,
            apr=apr,
            gross_pnl=gross,
            fee_cost=fees,
            net_pnl=net,
            net_apr=net_apr,
            breakeven_hours=breakeven,
            covers_fees=breakeven is not None and inputs.holding_hours >= breakeven,
        )
