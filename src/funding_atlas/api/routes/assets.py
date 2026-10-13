"""`/assets` — the recorded universe and what is known about each instrument.

An honest caveat belongs at the top of this file, because the response looks
more authoritative than it is: **the recorder does not persist venue metadata.**
`records.STREAMS` is trades, mids, l2, funding and open_interest — there is no
`meta` stream, so `szDecimals`, `maxLeverage` and the margin tables are fetched
at startup, used to build subscriptions, and never written down.

What that means per field:

* `first_seen_ns` / `last_seen_ns` / `has_l2` — observed. Exact.
* `price_sig_figs` / `min_notional` — venue *rules*, not per-asset data. Fixed
  by docs/hyperliquid-api.md §7 (5 significant figures, $10 minimum) and
  returned as the constants they are.
* `price_tick` / `size_lot` — derived from `szDecimals`, which is recovered from
  the widest size observed on the tape (see `DuckStore.observed_size_scale`).
  A lower bound, and therefore an *upper* bound on the true lot size: an asset
  whose recent trades all happened to be round numbers will report a coarser lot
  than the venue allows. Good enough to render a price column, not good enough
  to size an order — which is what `null` for an asset with no trades is saying.
* `max_leverage` — `null`, always, today. It exists only in the `meta` response
  and nothing on disk implies it. A plausible guess here would be indistinguish-
  able from a real value to every consumer, so there is none.

The fix is a `meta`/universe stream in the recorder. Until then this endpoint
says what it knows and marks the rest absent.
"""

from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter

from funding_atlas.api.deps import StoreDep
from funding_atlas.api.duck import NS_PER_S, DuckStore
from funding_atlas.api.models import AssetOut, AssetsOut
from funding_atlas.venues.records import STREAMS

router = APIRouter(tags=["assets"])

#: docs/hyperliquid-api.md §7: at most 5 significant figures, and at most
#: `MAX_DECIMALS - szDecimals` decimal places with MAX_DECIMALS = 6 for perps.
PRICE_SIG_FIGS = 5
PERP_MAX_PRICE_DECIMALS = 6
#: "Order must have minimum value of $10." (§7). Flagged there as possibly
#: lowered for HIP-4 markets but unconfirmed from any venue source, so the
#: documented value is what ships.
MIN_NOTIONAL = Decimal(10)

#: Window for recovering `szDecimals` from observed trade sizes. One day, because
#: it is a full scan of the tape and the answer only changes on a relisting.
SIZE_SCALE_LOOKBACK_NS = 86_400 * NS_PER_S


@router.get("/assets", response_model=AssetsOut)
def assets(store: StoreDep) -> AssetsOut:
    spans = _spans(store)
    if not spans:
        # Empty universe is a valid state: a recorder that has not flushed its
        # first partition yet. `as_of_ns` is null rather than "now", so a caller
        # cannot mistake an empty response for a fresh one.
        return AssetsOut(assets=[], as_of_ns=None)

    with_l2 = store.assets_with_l2()
    size_scale = store.observed_size_scale(SIZE_SCALE_LOOKBACK_NS)

    rows = [
        AssetOut(
            asset=asset,
            price_tick=_price_tick(size_scale.get(asset)),
            price_sig_figs=PRICE_SIG_FIGS,
            size_lot=_size_lot(size_scale.get(asset)),
            min_notional=MIN_NOTIONAL,
            max_leverage=None,  # not on disk; see the module docstring
            first_seen_ns=first_ns,
            last_seen_ns=last_ns,
            has_l2=asset in with_l2,
        )
        for asset, (first_ns, last_ns) in sorted(spans.items())
    ]
    return AssetsOut(assets=rows, as_of_ns=max(last for _, last in spans.values()))


def _spans(store: DuckStore) -> dict[str, tuple[int, int]]:
    """Widest `(first_seen, last_seen)` per asset across every stream.

    Unioned across streams rather than read from one: an asset with funding but
    no trades yet is still in the universe, and an asset whose L2 subscription
    started late was first seen before its book.
    """
    spans: dict[str, tuple[int, int]] = {}
    for stream in STREAMS:
        for span in store.asset_spans(stream):
            current = spans.get(span.asset)
            if current is None:
                spans[span.asset] = (span.first_ns, span.last_ns)
            else:
                spans[span.asset] = (
                    min(current[0], span.first_ns),
                    max(current[1], span.last_ns),
                )
    return spans


def _size_lot(sz_decimals: int | None) -> Decimal | None:
    """`10 ** -szDecimals`. None when no trade has been seen for the asset."""
    if sz_decimals is None:
        return None
    return Decimal(1).scaleb(-sz_decimals)


def _price_tick(sz_decimals: int | None) -> Decimal | None:
    """Finest legal price increment: `10 ** -(6 - szDecimals)` for perps (§7).

    Clamped at 1 because the decimal allowance cannot go negative: an asset with
    more than six size decimals would imply sub-integer prices being illegal,
    and integer prices are always valid.
    """
    if sz_decimals is None:
        return None
    decimals = max(PERP_MAX_PRICE_DECIMALS - sz_decimals, 0)
    return Decimal(1).scaleb(-decimals)
