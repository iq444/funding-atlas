"""`/assets` — the recorded universe and what is known about each instrument.

Two sources, in order of authority:

1. **The `asset_specs` stream.** The venue's own rules, polled from `meta` and
   written to disk, so a historical question gets the rules that applied *then*
   rather than the ones that apply now. This is the authoritative source and is
   used whenever the stream has data.

2. **Inference from the tape**, as a fallback for data recorded before that
   stream existed. `szDecimals` is recovered from the widest size observed in
   trades, which is a lower bound on the venue's precision and therefore an
   *upper* bound on lot fineness: an asset whose recent trades were all round
   numbers reports a coarser lot than the venue allows, and `max_leverage` is
   simply unknowable. Good enough to render a price column, not to size an
   order.

`source` on each row says which was used, because a consumer sizing an order
needs to know whether it is reading a recorded rule or a guess.

`price_sig_figs` and `min_notional` are venue-wide *rules* rather than per-asset
data (docs/hyperliquid-api.md §7: five significant figures, $10 minimum) and are
returned as the constants they are when no recorded spec is available.
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
    recorded = store.latest_asset_specs()
    # Only scanned when something is actually missing from the recorded stream.
    # It is a tape scan over a day of trades, and paying for it to answer a
    # question the specs stream already answers would be waste.
    size_scale = (
        store.observed_size_scale(SIZE_SCALE_LOOKBACK_NS)
        if any(a not in recorded for a in spans)
        else {}
    )

    rows = []
    for asset, (first_ns, last_ns) in sorted(spans.items()):
        spec = recorded.get(asset)
        if spec is not None:
            _, tick, sig_figs, lot, min_notional, max_lev, _is_perp, _delisted, _ts = spec
            rows.append(
                AssetOut(
                    asset=asset,
                    price_tick=tick,
                    price_sig_figs=sig_figs,
                    size_lot=lot,
                    min_notional=min_notional,
                    max_leverage=max_lev,
                    first_seen_ns=first_ns,
                    last_seen_ns=last_ns,
                    has_l2=asset in with_l2,
                    source="recorded",
                )
            )
        else:
            rows.append(
                AssetOut(
                    asset=asset,
                    price_tick=_price_tick(size_scale.get(asset)),
                    price_sig_figs=PRICE_SIG_FIGS,
                    size_lot=_size_lot(size_scale.get(asset)),
                    min_notional=MIN_NOTIONAL,
                    max_leverage=None,
                    first_seen_ns=first_ns,
                    last_seen_ns=last_ns,
                    has_l2=asset in with_l2,
                    source="inferred",
                )
            )
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
