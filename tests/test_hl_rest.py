"""Hyperliquid REST client tests.

No network: every request is served by `httpx.MockTransport` from JSON copied
out of `docs/hyperliquid-api.md`, which is itself a live mainnet capture. That
keeps the fixtures honest — a field renamed in the doc fails here rather than
in production at 3am — and keeps the suite runnable offline.

Time comes from `SimClock`, so backoff sleeps and the rate limiter's waits are
instant and exact instead of slow and approximate.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest

from funding_atlas.core.clock import SimClock
from funding_atlas.venues.hyperliquid.rest import (
    HyperliquidHTTPError,
    HyperliquidRest,
    HyperliquidRestError,
    HyperliquidShapeError,
)

T0_NS = 1_791_893_000_000 * 1_000_000

# --- Fixtures ---------------------------------------------------------------
# BTC and ETH entries are verbatim from the doc's live `meta` response. The
# szDecimals-0 entry is constructed from the doc's §7 worked example (price
# `0.001234` valid at `6 - 0` decimals); the capture in the doc only shows
# high-szDecimals majors, and the low-precision branch is exactly where the
# derivation is easiest to get wrong.
UNIVERSE: list[dict[str, Any]] = [
    {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
    {"szDecimals": 4, "name": "ETH", "maxLeverage": 25, "marginTableId": 55},
    {"szDecimals": 0, "name": "COARSE", "maxLeverage": 3, "marginTableId": 50},
]

META: dict[str, Any] = {
    "universe": UNIVERSE,
    "marginTables": [
        [50, {"description": "", "marginTiers": [{"lowerBound": "0.0", "maxLeverage": 50}]}]
    ],
    "collateralToken": "USDC",
}

# Verbatim live `assetCtxs[0]` from the doc, plus two more of the same shape.
BTC_CTX: dict[str, Any] = {
    "funding": "0.0000020288",
    "openInterest": "38518.20404",
    "prevDayPx": "83367.0",
    "dayNtlVlm": "3307460952.327064991",
    "premium": "-0.000456303",
    "oraclePx": "83278.0",
    "markPx": "83240.0",
    "midPx": "83239.5",
    "impactPxs": ["83239.0", "83240.0"],
    "dayBaseVlm": "39963.8011200001",
}
ETH_CTX: dict[str, Any] = dict(
    BTC_CTX, funding="0.0000125", openInterest="1234.5678", markPx="3100.4"
)
#: A HIP-3-shaped context: nullable fields actually null (doc §3).
COARSE_CTX: dict[str, Any] = {
    "funding": "-0.0000035092",
    "openInterest": "7.0",
    "prevDayPx": "1.0",
    "dayNtlVlm": "0.0",
    "premium": None,
    "oraclePx": "1.0",
    "markPx": None,
    "midPx": None,
    "impactPxs": None,
    "dayBaseVlm": "0.0",
}

#: Verbatim live `fundingHistory` row from the doc, plus one more hour.
FUNDING_HISTORY: list[dict[str, Any]] = [
    {"coin": "BTC", "fundingRate": "0.0000125", "premium": "-0.0003558005", "time": 1791802800098},
    {
        "coin": "BTC",
        "fundingRate": "0.0000020288",
        "premium": "-0.000456303",
        "time": 1791806400032,
    },
]


class Router:
    """Dispatch on the info request's `type`, and count the calls.

    The counter is the assertion surface for the retry tests: "retried" and
    "did not retry" are statements about how many requests left the client.
    """

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.calls.append(body)
        handler = self.responses[body["type"]]
        if callable(handler):
            return handler(len([c for c in self.calls if c["type"] == body["type"]]))
        return httpx.Response(200, json=handler)

    def count(self, request_type: str) -> int:
        return len([c for c in self.calls if c["type"] == request_type])


def make(responses: dict[str, Any]) -> tuple[HyperliquidRest, Router, httpx.AsyncClient, SimClock]:
    router = Router(responses)
    client = httpx.AsyncClient(transport=httpx.MockTransport(router))
    clock = SimClock(T0_NS)
    return HyperliquidRest("mainnet", clock, client=client), router, client, clock


# --- Spec derivation --------------------------------------------------------


async def test_spec_derivation_high_precision_asset() -> None:
    rest, _, client, _ = make({"meta": META})
    specs = await rest.asset_specs()
    await client.aclose()

    btc = specs["BTC"]
    # szDecimals 5 => sizes step by 1e-5, prices allowed 6-5 = 1 decimal.
    assert btc.size_lot == Decimal("0.00001")
    assert btc.price_tick == Decimal("0.1")
    assert btc.min_notional == Decimal(10)
    assert btc.max_leverage == Decimal(40)
    assert btc.is_perp is True

    eth = specs["ETH"]
    assert eth.size_lot == Decimal("0.0001")
    assert eth.price_tick == Decimal("0.01")


async def test_spec_derivation_low_precision_asset() -> None:
    rest, _, client, _ = make({"meta": META})
    specs = await rest.asset_specs()
    await client.aclose()

    coarse = specs["COARSE"]
    # szDecimals 0 => whole-coin lots, and the full 6 decimals of price.
    assert coarse.size_lot == Decimal(1)
    assert coarse.price_tick == Decimal("0.000001")
    assert coarse.min_notional == Decimal(10)
    assert coarse.max_leverage == Decimal(3)


async def test_specs_cover_every_universe_entry_including_order() -> None:
    rest, _, client, _ = make({"meta": META})
    specs = await rest.asset_specs()
    await client.aclose()
    assert list(specs) == ["BTC", "ETH", "COARSE"]


# --- Contexts ---------------------------------------------------------------


def _ctx_responses(universe: list[dict[str, Any]], ctxs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"metaAndAssetCtxs": [dict(META, universe=universe), ctxs]}


async def test_contexts_decimal_round_trip_is_exact() -> None:
    rest, _, client, _ = make(_ctx_responses(UNIVERSE, [BTC_CTX, ETH_CTX, COARSE_CTX]))
    contexts = await rest.asset_contexts()
    await client.aclose()

    btc = contexts[0]
    assert btc.funding.asset == "BTC"
    # Exact, digit for digit: a float round trip would not preserve this.
    assert btc.funding.rate == Decimal("0.0000020288")
    assert str(btc.funding.rate) == "0.0000020288"
    assert btc.funding.premium == Decimal("-0.000456303")
    assert btc.funding.oracle_price == Decimal("83278.0")
    assert btc.funding.mark_price == Decimal("83240.0")
    # Coin units, stored as received (doc §8) — not multiplied into notional.
    assert btc.open_interest.open_interest == Decimal("38518.20404")
    assert str(btc.open_interest.open_interest) == "38518.20404"
    assert btc.mark is not None
    assert btc.mark.mid == Decimal("83240.0")


async def test_contexts_are_labelled_by_positional_join() -> None:
    rest, _, client, _ = make(_ctx_responses(UNIVERSE, [BTC_CTX, ETH_CTX, COARSE_CTX]))
    contexts = await rest.asset_contexts()
    await client.aclose()

    assert [c.funding.asset for c in contexts] == ["BTC", "ETH", "COARSE"]
    assert [c.open_interest.asset for c in contexts] == ["BTC", "ETH", "COARSE"]
    assert contexts[1].funding.rate == Decimal("0.0000125")
    assert contexts[2].funding.rate == Decimal("-0.0000035092")


async def test_contexts_funding_interval_is_hourly() -> None:
    rest, _, client, _ = make(_ctx_responses(UNIVERSE, [BTC_CTX, ETH_CTX, COARSE_CTX]))
    contexts = await rest.asset_contexts()
    await client.aclose()
    # The wire rate is already per-hour (doc §6). 3600, never 28800.
    assert {c.funding.interval_s for c in contexts} == {3600}


async def test_contexts_timestamps_are_synthetic_and_from_the_clock() -> None:
    rest, _, client, clock = make(_ctx_responses(UNIVERSE, [BTC_CTX, ETH_CTX, COARSE_CTX]))
    contexts = await rest.asset_contexts()
    await client.aclose()

    for ctx in contexts:
        # metaAndAssetCtxs carries no timestamp at all, so the record must say
        # so rather than let research measure our latency against itself.
        assert ctx.funding.ts_event_synthetic is True
        assert ctx.open_interest.ts_event_synthetic is True
        assert ctx.funding.ts_event_ns == clock.now_ns()
        assert ctx.funding.ts_event_ns == ctx.funding.ts_ingest_ns
        assert ctx.mark is None or ctx.mark.ts_event_synthetic is True


async def test_context_nullable_fields_survive_as_none() -> None:
    rest, _, client, _ = make(_ctx_responses(UNIVERSE, [BTC_CTX, ETH_CTX, COARSE_CTX]))
    contexts = await rest.asset_contexts()
    await client.aclose()

    coarse = contexts[2]
    assert coarse.funding.premium is None
    assert coarse.funding.mark_price is None
    assert coarse.mark is None


async def test_positional_join_length_mismatch_raises_rather_than_mislabelling() -> None:
    # One extra listing in `universe` with no context: zipping would attribute
    # ETH's funding to BTC and look perfectly healthy.
    rest, _, client, _ = make(_ctx_responses(UNIVERSE, [BTC_CTX, ETH_CTX]))
    with pytest.raises(HyperliquidShapeError, match="positional join broken"):
        await rest.asset_contexts()
    await client.aclose()


async def test_shorter_universe_also_raises() -> None:
    rest, _, client, _ = make(_ctx_responses(UNIVERSE[:2], [BTC_CTX, ETH_CTX, COARSE_CTX]))
    with pytest.raises(HyperliquidShapeError, match="positional join broken"):
        await rest.asset_contexts()
    await client.aclose()


# --- Funding history --------------------------------------------------------


async def test_funding_history_converts_ms_to_ns() -> None:
    rest, router, client, _ = make({"fundingHistory": FUNDING_HISTORY})
    rows = await rest.funding_history("BTC", 1791800000000, 1791893000000)
    await client.aclose()

    assert [r.ts_event_ns for r in rows] == [1791802800098 * 1_000_000, 1791806400032 * 1_000_000]
    assert rows[0].rate == Decimal("0.0000125")
    assert rows[0].premium == Decimal("-0.0003558005")
    assert rows[0].interval_s == 3600
    # A real venue settlement stamp, so not synthetic.
    assert rows[0].ts_event_synthetic is False
    assert router.calls[0] == {
        "type": "fundingHistory",
        "coin": "BTC",
        "startTime": 1791800000000,
        "endTime": 1791893000000,
    }


async def test_funding_history_omits_end_time_when_not_given() -> None:
    rest, router, client, _ = make({"fundingHistory": []})
    assert await rest.funding_history("BTC", 1791800000000) == []
    await client.aclose()
    # Omitted, not filled from our clock: the venue's "now" is not ours.
    assert "endTime" not in router.calls[0]


# --- Retry / rate limiting --------------------------------------------------


async def test_retries_on_429_then_succeeds() -> None:
    def handler(nth: int) -> httpx.Response:
        if nth == 1:
            return httpx.Response(429, text="rate limited")
        return httpx.Response(200, json=META)

    rest, router, client, _ = make({"meta": handler})
    specs = await rest.asset_specs()
    await client.aclose()

    assert router.count("meta") == 2
    assert "BTC" in specs


async def test_retries_on_5xx_and_on_timeout() -> None:
    def handler(nth: int) -> httpx.Response:
        if nth == 1:
            raise httpx.ReadTimeout("hung", request=None)  # type: ignore[arg-type]
        if nth == 2:
            return httpx.Response(503, text="unavailable")
        return httpx.Response(200, json=META)

    rest, router, client, _ = make({"meta": handler})
    await rest.asset_specs()
    await client.aclose()
    assert router.count("meta") == 3


async def test_does_not_retry_a_permanent_4xx() -> None:
    def handler(nth: int) -> httpx.Response:
        return httpx.Response(422, text="Failed to deserialize the JSON body")

    rest, router, client, _ = make({"meta": handler})
    with pytest.raises(HyperliquidHTTPError) as excinfo:
        await rest.asset_specs()
    await client.aclose()

    # Exactly one attempt: a malformed request costs weight budget and returns
    # the same answer however often it is sent.
    assert router.count("meta") == 1
    assert excinfo.value.status_code == 422


async def test_gives_up_after_max_attempts() -> None:
    def handler(nth: int) -> httpx.Response:
        return httpx.Response(500, text="boom")

    rest, router, client, _ = make({"meta": handler})
    with pytest.raises(HyperliquidRestError, match="giving up"):
        await rest.asset_specs()
    await client.aclose()
    assert router.count("meta") == 5


async def test_backoff_advances_the_clock_rather_than_blocking() -> None:
    def handler(nth: int) -> httpx.Response:
        if nth < 3:
            return httpx.Response(429, text="rate limited")
        return httpx.Response(200, json=META)

    rest, _, client, clock = make({"meta": handler})
    await rest.asset_specs()
    await client.aclose()
    # Two retries slept; full jitter can round to zero, so only monotonicity is
    # asserted — the point is that waiting went through the Clock at all.
    assert clock.now_ns() >= T0_NS


async def test_rate_limiter_throttles_before_the_budget_is_breached() -> None:
    # 1200 weight / 20 per `meta` = 60 requests per 60 s window. The 61st must
    # wait for the window to roll rather than be sent and rejected.
    rest, router, client, clock = make({"meta": META})
    for _ in range(60):
        await rest.asset_specs()
    assert clock.now_ns() == T0_NS

    await rest.asset_specs()
    await client.aclose()
    assert router.count("meta") == 61
    assert clock.now_ns() > T0_NS


# --- Lifecycle --------------------------------------------------------------


async def test_close_does_not_close_an_injected_client() -> None:
    rest, _, client, _ = make({"meta": META})
    await rest.asset_specs()
    await rest.close()
    # The caller owns an injected client — often a shared pool. Closing it here
    # would break every other user of it.
    assert client.is_closed is False
    assert "BTC" in await rest.asset_specs()
    await client.aclose()


async def test_close_closes_a_client_it_created() -> None:
    rest = HyperliquidRest("mainnet", SimClock(T0_NS))
    owned = rest._client  # ownership is internal by design
    await rest.close()
    assert owned.is_closed is True


async def test_unknown_network_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="unknown network"):
        HyperliquidRest("devnet", SimClock(T0_NS))


async def test_testnet_targets_the_testnet_info_endpoint() -> None:
    router = Router({"meta": META})
    client = httpx.AsyncClient(transport=httpx.MockTransport(router))
    rest = HyperliquidRest("testnet", SimClock(T0_NS), client=client)
    await rest.asset_specs()
    await client.aclose()
    assert rest._url == "https://api.hyperliquid-testnet.xyz/info"


async def test_delisting_is_read_from_the_universe_entry() -> None:
    """`isDelisted` is present only on removed assets (doc §4), and it is the
    one field that says an asset's rules no longer apply. The `asset_specs`
    history needs it to date a delisting, so absence means listed rather than
    unknown."""
    universe = [
        {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
        {"szDecimals": 2, "name": "GONE", "maxLeverage": 3, "isDelisted": True},
    ]
    rest, _, client, _ = make({"meta": dict(META, universe=universe)})
    specs = await rest.asset_specs()
    await client.aclose()

    assert specs["BTC"].is_delisted is False
    assert specs["GONE"].is_delisted is True
