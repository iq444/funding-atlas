"""Hyperliquid REST (`POST /info`) client.

Coded against `docs/hyperliquid-api.md`, which was produced from live mainnet
captures. That document is authoritative here; where it carries a
`TODO(verify)` marker the code below is deliberately defensive and says so at
the point of the decision.

Three properties of this API drive the whole module:

* **Everything numeric is a JSON string** (§ "Two invariants"). Every value is
  fed straight to `Decimal`. A float anywhere in this path would quantise
  funding rates of the order 1e-6 and turn a neutral hedge into a directional
  one — see the note in `records.py`.
* **Everything temporal is milliseconds**, and `metaAndAssetCtxs` has no
  timestamp at all. So the poll path stamps `ts_event_ns` from the injected
  `Clock` and sets `ts_event_synthetic=True`; research can then tell our guess
  from a venue stamp instead of measuring our own latency against itself.
* **`metaAndAssetCtxs` joins by index.** The context objects carry no `coin`
  field (API doc §4), so a length mismatch between `universe` and `assetCtxs`
  would attribute BTC's funding to ETH and produce plausible-looking garbage
  for every asset. That is checked on every poll and is fatal, per the
  "Positional joins" gotcha.

Why REST at all for funding: `activeAssetCtx` is per-coin and the WS
subscription cap is 1000 per IP (§5), which `mode: all_perps` would approach.
One `metaAndAssetCtxs` poll covers funding *and* open interest for every perp.
"""

from __future__ import annotations

import asyncio
import random
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

from funding_atlas.core.clock import Clock
from funding_atlas.venues.base import AssetSpec
from funding_atlas.venues.records import FundingSample, Mid, OpenInterest

__all__ = [
    "AssetContext",
    "HyperliquidHTTPError",
    "HyperliquidRest",
    "HyperliquidRestError",
    "HyperliquidShapeError",
]

VENUE = "hyperliquid"

NS_PER_MS = 1_000_000

#: Hyperliquid settles funding hourly and the rate on the wire is *already* the
#: per-hour figure (API doc §6: `fundingRate` 0.0000125 for BTC is the 8-hour
#: 0.0001 interest floor divided by 8). Dividing by 8 here would double-apply a
#: conversion the venue has already done.
FUNDING_INTERVAL_S = 3600

INFO_URLS: dict[str, str] = {
    "mainnet": "https://api.hyperliquid.xyz/info",
    "testnet": "https://api.hyperliquid-testnet.xyz/info",
}

# --- Precision rules (API doc §7) -------------------------------------------
# There is no `tickSize` field. A valid perp price must satisfy two rules at
# once: at most 5 significant figures, and at most `MAX_DECIMALS - szDecimals`
# decimal places with MAX_DECIMALS = 6 for perps (8 for spot; this client only
# reads the perp `meta`). Integer prices are always valid, which is what makes
# BTC at 83245 legal despite the significant-figure rule.
PERP_MAX_PRICE_DECIMALS = 6
MAX_PRICE_SIG_FIGS = 5

#: Minimum order value, USD (API doc §7, `MinTradeNtl`). A per-asset field does
#: not exist; the venue applies one flat floor to every perp.
MIN_ORDER_NOTIONAL = Decimal(10)

# --- Rate limiting (API doc §5) ---------------------------------------------
#: Aggregate REST weight budget per IP, per minute.
WEIGHT_LIMIT = 1200
WEIGHT_WINDOW_S = 60.0

#: Per-request weights. The cheap set is enumerated in the doc; everything else
#: documented costs 20.
WEIGHT_CHEAP = frozenset(
    {
        "l2Book",
        "allMids",
        "clearinghouseState",
        "orderStatus",
        "spotClearinghouseState",
        "exchangeStatus",
    }
)
WEIGHT_DEFAULT = 20
WEIGHT_USER_ROLE = 60

#: Endpoints that add weight per page of results returned, and their page size
#: (doc §5: `fundingHistory` per 20 items, `candleSnapshot` per 60).
PAGE_SIZE: dict[str, int] = {"fundingHistory": 20, "recentTrades": 20, "candleSnapshot": 60}

# --- Retry policy -----------------------------------------------------------
#: A hung request must never stall a polling loop, so every request carries a
#: total timeout well under the recorder's 60 s poll period.
REQUEST_TIMEOUT_S = 10.0
CONNECT_TIMEOUT_S = 5.0

MAX_ATTEMPTS = 5
BACKOFF_BASE_S = 0.5
BACKOFF_CAP_S = 20.0


class HyperliquidRestError(RuntimeError):
    """Base for every failure this client raises."""


class HyperliquidHTTPError(HyperliquidRestError):
    """Non-200 that we either exhausted retries on or must not retry."""

    def __init__(self, status_code: int, body: str, request_type: str) -> None:
        super().__init__(f"{request_type}: HTTP {status_code}: {body[:200]}")
        self.status_code = status_code
        self.body = body
        self.request_type = request_type


class HyperliquidShapeError(HyperliquidRestError):
    """The response did not have the shape the API doc records.

    Raised rather than coerced. The positional `universe`/`assetCtxs` join is
    the motivating case: zipping the shorter of two mismatched lists would
    mislabel every asset's funding and look healthy while doing it.
    """


@dataclass(frozen=True, slots=True)
class AssetContext:
    """One asset's slice of a `metaAndAssetCtxs` poll, already labelled.

    The label is the whole point: the wire format is positional, so this type
    exists so that no consumer downstream ever has to re-derive the join.
    """

    funding: FundingSample
    open_interest: OpenInterest
    mark: Mid | None


def _dec(value: Any) -> Decimal:
    """Exact decimal from a wire value.

    `Decimal(str(...))` rather than `Decimal(value)` because the doc guarantees
    strings today but an int would arrive here silently if the venue ever
    changed, and `Decimal(float)` would bake in binary rounding. Going through
    `str` costs nothing and cannot lose a digit.
    """
    return Decimal(str(value))


def _opt_dec(value: Any) -> Decimal | None:
    """Nullable wire value. `midPx`, `premium` and `impactPxs` can be null on
    HIP-3 markets (doc §3), so absent and explicit-null must both survive."""
    return None if value is None else _dec(value)


def _weight_for(request_type: str) -> int:
    if request_type in WEIGHT_CHEAP:
        return 2
    if request_type == "userRole":
        return WEIGHT_USER_ROLE
    return WEIGHT_DEFAULT


class _WeightLimiter:
    """Client-side sliding-window weight budget.

    The doc's §5 `TODO(verify)` is explicit that the *consequence* of breaching
    the IP weight limit is undocumented — 429, connection close and silent drop
    are all possible. A 24/7 recorder therefore must not discover the limit
    empirically: it throttles itself and never finds out which one it is.

    Time comes from the `Clock`, so a simulated clock makes this instant and
    deterministic in tests rather than sleeping on wall time.
    """

    __slots__ = ("_clock", "_limit", "_lock", "_spent", "_window_ns")

    def __init__(self, clock: Clock, limit: int = WEIGHT_LIMIT, window_s: float = WEIGHT_WINDOW_S):
        self._clock = clock
        self._limit = limit
        self._window_ns = int(window_s * 1e9)
        self._spent: deque[tuple[int, int]] = deque()
        # Serialised so that two concurrent pollers cannot both read spare
        # capacity and both spend it.
        self._lock = asyncio.Lock()

    def _prune(self, now_ns: int) -> int:
        horizon = now_ns - self._window_ns
        while self._spent and self._spent[0][0] <= horizon:
            self._spent.popleft()
        return sum(w for _, w in self._spent)

    async def acquire(self, weight: int) -> None:
        if weight > self._limit:
            raise HyperliquidRestError(
                f"request weight {weight} exceeds the per-minute budget {self._limit}"
            )
        async with self._lock:
            while True:
                now_ns = self._clock.now_ns()
                used = self._prune(now_ns)
                if used + weight <= self._limit:
                    self._spent.append((now_ns, weight))
                    return
                # Wait exactly until the oldest charge leaves the window.
                oldest_ns = self._spent[0][0]
                wait_s = (oldest_ns + self._window_ns - now_ns) / 1e9
                await self._clock.sleep(max(wait_s, 0.001))

    def charge(self, weight: int) -> None:
        """Record weight already consumed, without waiting.

        Paged endpoints bill per page *returned*, which is only knowable after
        the fact. Charging it retroactively makes the *next* caller pay the
        wait, which is the best an honest client-side limiter can do.
        """
        if weight <= 0:
            return
        self._spent.append((self._clock.now_ns(), weight))


class HyperliquidRest:
    """Polled market-data reads: asset specs, funding + OI contexts, backfill.

    Owns no state beyond the HTTP client and the rate limiter. Each method is a
    single request, so a caller can be killed between polls without leaving
    anything half-applied.
    """

    __slots__ = ("_client", "_clock", "_limiter", "_owns_client", "_url")

    def __init__(self, network: str, clock: Clock, *, client: httpx.AsyncClient | None = None):
        try:
            self._url = INFO_URLS[network]
        except KeyError:
            raise ValueError(
                f"unknown network {network!r}; expected one of {sorted(INFO_URLS)}"
            ) from None
        self._clock = clock
        self._limiter = _WeightLimiter(clock)
        # Ownership, not just a reference: an injected client belongs to the
        # caller's lifecycle (often a shared pool), and closing it from here
        # would break every other user of it.
        self._owns_client = client is None
        self._client = client if client is not None else self._make_client()

    @staticmethod
    def _make_client() -> httpx.AsyncClient:
        timeout = httpx.Timeout(REQUEST_TIMEOUT_S, connect=CONNECT_TIMEOUT_S)
        return httpx.AsyncClient(timeout=timeout, headers={"Content-Type": "application/json"})

    # --- transport ---------------------------------------------------------

    async def _post(self, body: dict[str, Any]) -> Any:
        """One info request, rate-limited, timed out and retried.

        Retries cover 429, 5xx, timeouts and transport errors — the failures
        that a later attempt can plausibly fix. Every other 4xx is a malformed
        request: retrying it burns weight budget to receive the same answer, so
        it is raised immediately.
        """
        request_type = str(body.get("type", "")) or str(body.get("req", {}).get("type", ""))
        await self._limiter.acquire(_weight_for(request_type))

        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                # Full jitter: the recorder is one of many clients on one IP and
                # a fixed backoff would have them all retry in lockstep.
                ceiling = min(BACKOFF_CAP_S, BACKOFF_BASE_S * 2**attempt)
                await self._clock.sleep(random.uniform(0.0, ceiling))
            try:
                # Timeout passed per request so it also applies to an injected
                # client that was built without one.
                response = await self._client.post(
                    self._url,
                    json=body,
                    timeout=httpx.Timeout(REQUEST_TIMEOUT_S, connect=CONNECT_TIMEOUT_S),
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                continue

            if response.status_code == 200:
                payload = response.json()
                self._charge_pages(request_type, payload)
                return payload

            if response.status_code == 429 or response.status_code >= 500:
                last_error = HyperliquidHTTPError(response.status_code, response.text, request_type)
                continue

            raise HyperliquidHTTPError(response.status_code, response.text, request_type)

        assert last_error is not None
        raise HyperliquidRestError(
            f"{request_type}: giving up after {MAX_ATTEMPTS} attempts: {last_error}"
        ) from last_error

    def _charge_pages(self, request_type: str, payload: Any) -> None:
        page_size = PAGE_SIZE.get(request_type)
        if page_size is None or not isinstance(payload, list):
            return
        # The doc states these endpoints "add weight per 20 items returned" but
        # not how much per page. Charging the base weight per extra page is the
        # conservative reading: overcharging slows us down, undercharging walks
        # into an undocumented failure mode.
        extra_pages = max(0, (len(payload) - 1) // page_size)
        self._limiter.charge(WEIGHT_DEFAULT * extra_pages)

    # --- public surface -----------------------------------------------------

    async def asset_specs(self) -> dict[str, AssetSpec]:
        """Trading rules per perp, derived from `meta` (API doc §4 and §7)."""
        payload = await self._post({"type": "meta", "dex": ""})
        universe = self._universe_of(payload)
        # Delisted assets are kept: funding rows can still arrive for them in
        # the same poll, and a record with no spec is harder to reason about
        # than a spec for something nobody trades. Universe selection is the
        # recorder's job, not this client's.
        return {entry["name"]: _spec_from_universe(entry) for entry in universe}

    async def asset_contexts(self) -> list[AssetContext]:
        """Funding + OI + mark for every perp, in one request.

        Returned in `universe` order, which is also asset-id order (doc §4),
        so the index of a result is the venue's asset id for that poll.
        """
        payload = await self._post({"type": "metaAndAssetCtxs", "dex": ""})
        if not isinstance(payload, list) or len(payload) != 2:
            shape = (
                f"list of length {len(payload)}"
                if isinstance(payload, list)
                else type(payload).__name__
            )
            raise HyperliquidShapeError(
                f"metaAndAssetCtxs: expected a two-element [meta, assetCtxs] array, got {shape}"
            )
        universe = self._universe_of(payload[0])
        contexts = payload[1]
        if not isinstance(contexts, list):
            raise HyperliquidShapeError(
                f"metaAndAssetCtxs: assetCtxs is {type(contexts).__name__}, expected a list"
            )
        # The join is positional and the contexts carry no `coin` field, so a
        # mismatch is unrecoverable rather than partial: zipping would silently
        # shift every asset's funding by one. Fatal, per doc §8.
        if len(universe) != len(contexts):
            raise HyperliquidShapeError(
                "metaAndAssetCtxs positional join broken: "
                f"len(universe)={len(universe)} != len(assetCtxs)={len(contexts)}; "
                "contexts carry no 'coin' field, so they cannot be re-labelled"
            )

        # One timestamp for the whole poll. The response carries none (doc §4),
        # so it is our clock's and every record says so.
        ts_ns = self._clock.now_ns()
        return [
            _context_from_ctx(entry["name"], ctx, ts_ns)
            for entry, ctx in zip(universe, contexts, strict=True)
        ]

    async def funding_history(
        self, asset: str, start_ms: int, end_ms: int | None = None
    ) -> list[FundingSample]:
        """Settled hourly funding, for gap repair on the `funding` stream.

        The only funding data recoverable after an outage (doc §4). `startTime`
        is inclusive and required; `endTime` defaults to now venue-side, so it
        is omitted rather than filled in from our clock — our idea of "now" is
        not the venue's.
        """
        body: dict[str, Any] = {
            "type": "fundingHistory",
            "coin": asset,
            "startTime": int(start_ms),
        }
        if end_ms is not None:
            body["endTime"] = int(end_ms)
        rows = await self._post(body)
        if not isinstance(rows, list):
            raise HyperliquidShapeError(
                f"fundingHistory: expected a list of rows, got {type(rows).__name__}"
            )
        ts_ingest_ns = self._clock.now_ns()
        return [_funding_from_history_row(row, asset, ts_ingest_ns) for row in rows]

    async def close(self) -> None:
        """Release the HTTP client, but only if we created it."""
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def _universe_of(meta: Any) -> list[dict[str, Any]]:
        if not isinstance(meta, dict) or not isinstance(meta.get("universe"), list):
            raise HyperliquidShapeError("meta: missing or non-list 'universe'")
        universe: list[dict[str, Any]] = meta["universe"]
        for entry in universe:
            if not isinstance(entry, dict) or "name" not in entry:
                raise HyperliquidShapeError(f"meta.universe entry without a 'name': {entry!r}")
        return universe


def _spec_from_universe(entry: dict[str, Any]) -> AssetSpec:
    """Derive `AssetSpec` from one `meta.universe` entry.

    Derivation, all from API doc §7:

    * `size_lot = 10 ** -szDecimals`. Sizes are rounded to `szDecimals`
      exactly, so the lot *is* that decimal step (szDecimals 3 ⇒ 1.001 valid,
      1.0001 not).
    * `price_tick = 10 ** -(6 - szDecimals)`, i.e. `MAX_DECIMALS - szDecimals`
      with MAX_DECIMALS = 6 for perps. This is the decimal-places half of the
      price rule. The other half — at most 5 significant figures, with integer
      prices exempt — is *price-dependent* and so cannot be expressed as a
      single tick: at 83 245 the binding constraint is sig figs, at 0.001234 it
      is decimals. `price_tick` is therefore the finest step that is ever legal
      for this asset, and an order router must additionally round to
      `MAX_PRICE_SIG_FIGS` significant figures. Treating the tick as sufficient
      would produce prices the venue rejects; treating sig figs as sufficient
      would produce prices with too many decimals. Both rules apply at once.
    * `min_notional = $10` flat (`MinTradeNtl`). Per-asset minimums do not
      exist on this venue.
    """
    sz_decimals = int(entry["szDecimals"])
    if sz_decimals < 0:
        raise HyperliquidShapeError(f"{entry.get('name')!r}: negative szDecimals {sz_decimals}")
    # Defensive clamp: szDecimals > MAX_DECIMALS would make the allowance
    # negative, which the doc neither forbids nor shows. An integer-only tick
    # is the conservative reading of "no fractional prices are permitted".
    price_decimals = max(0, PERP_MAX_PRICE_DECIMALS - sz_decimals)
    return AssetSpec(
        asset=str(entry["name"]),
        price_tick=Decimal(1).scaleb(-price_decimals),
        price_sig_figs=MAX_PRICE_SIG_FIGS,
        size_lot=Decimal(1).scaleb(-sz_decimals),
        min_notional=MIN_ORDER_NOTIONAL,
        # Integer on the wire, unlike the market-data numerics — but still
        # routed through Decimal so leverage arithmetic never meets a float.
        max_leverage=_dec(entry["maxLeverage"]),
        # This client only ever requests the perp `meta` (`dex: ""`); spot has
        # its own endpoint and a different MAX_DECIMALS of 8.
        is_perp=True,
        # Present only on removed assets (doc §4), so absence means listed.
        # Read rather than ignored: it is the one field that says an asset's
        # rules no longer apply, which the `asset_specs` history has to record.
        is_delisted=bool(entry.get("isDelisted", False)),
    )


def _context_from_ctx(asset: str, ctx: dict[str, Any], ts_ns: int) -> AssetContext:
    if not isinstance(ctx, dict):
        raise HyperliquidShapeError(
            f"{asset}: assetCtx is {type(ctx).__name__}, expected an object"
        )
    funding = FundingSample(
        venue=VENUE,
        asset=asset,
        ts_event_ns=ts_ns,
        ts_ingest_ns=ts_ns,
        rate=_dec(ctx["funding"]),
        interval_s=FUNDING_INTERVAL_S,
        premium=_opt_dec(ctx.get("premium")),
        oracle_price=_opt_dec(ctx.get("oraclePx")),
        mark_price=_opt_dec(ctx.get("markPx")),
        ts_event_synthetic=True,
    )
    open_interest = OpenInterest(
        venue=VENUE,
        asset=asset,
        ts_event_ns=ts_ns,
        ts_ingest_ns=ts_ns,
        # Coin units, not USD notional (doc §8). Stored as received; whoever
        # wants notional multiplies by a price and says which one.
        open_interest=_dec(ctx["openInterest"]),
        ts_event_synthetic=True,
    )
    # `markPx` rather than `midPx`: the mark is the venue's own valuation and is
    # present even on markets where `midPx` is null, and funding is quoted
    # against the oracle/mark pair rather than the book mid. Still optional,
    # because the doc only guarantees the field for standard perps.
    mark_px = _opt_dec(ctx.get("markPx"))
    mark = (
        None
        if mark_px is None
        else Mid(
            venue=VENUE,
            asset=asset,
            ts_event_ns=ts_ns,
            ts_ingest_ns=ts_ns,
            mid=mark_px,
            ts_event_synthetic=True,
        )
    )
    return AssetContext(funding=funding, open_interest=open_interest, mark=mark)


def _funding_from_history_row(row: Any, asset: str, ts_ingest_ns: int) -> FundingSample:
    if not isinstance(row, dict):
        raise HyperliquidShapeError(
            f"fundingHistory[{asset}]: row is {type(row).__name__}, expected an object"
        )
    return FundingSample(
        venue=VENUE,
        # Rows carry their own `coin`; prefer it so a venue-side alias cannot be
        # overwritten by the name we happened to ask with.
        asset=str(row.get("coin", asset)),
        # Settlement time, ms with a few tens of ms of block jitter. Real venue
        # stamp, so not synthetic — but consumers must bucket to the hour rather
        # than match exact equality (doc §4).
        ts_event_ns=int(row["time"]) * NS_PER_MS,
        ts_ingest_ns=ts_ingest_ns,
        rate=_dec(row["fundingRate"]),
        interval_s=FUNDING_INTERVAL_S,
        premium=_opt_dec(row.get("premium")),
        # Settled rows carry neither price; nulls here are honest absence, not a
        # lookup we skipped.
        oracle_price=None,
        mark_price=None,
        ts_event_synthetic=False,
    )
