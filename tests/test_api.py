"""Tests for the public Data API.

The dataset under test is built with the real writers from `store/parquet.py`,
not hand-rolled files. That matters more than it looks: the schemas there encode
`decimal128(38,18)` and raw int64 nanoseconds, and a hand-built fixture would
quietly test the API against float prices — which is the exact failure the whole
storage layer is shaped to prevent. Going through `records_to_table` also means
a schema change breaks these tests instead of silently changing the API's
output.

The fixture contains a deliberate one-hour hole in the funding series so that
gap reporting is exercised against a known interval rather than against
whatever the recorder happened to do.
"""

from __future__ import annotations

import datetime as dt
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from funding_atlas.api import deps
from funding_atlas.api import screener as sc
from funding_atlas.api.main import create_app
from funding_atlas.store.parquet import date_for_ns, partition_path, records_to_table, write_table
from funding_atlas.venues.records import (
    BookLevel,
    BookSnapshot,
    FundingSample,
    OpenInterest,
    Trade,
)

VENUE = "hyperliquid"
NS_PER_S = 1_000_000_000
MINUTE_NS = 60 * NS_PER_S
HOUR_NS = 3600 * NS_PER_S

# 2026-10-13T00:00:00Z. A UTC midnight, so every partition and bucket boundary
# in these tests is exact rather than approximately right.
T0 = int(dt.datetime(2026, 10, 13, tzinfo=dt.UTC).timestamp()) * NS_PER_S

#: Funding is sampled every minute for six hours, except for one hour that is
#: missing on purpose. The hole runs from the sample at minute 120 to the sample
#: at minute 180, so its reported bounds are exactly these.
GAP_FROM_NS = T0 + 120 * MINUTE_NS
GAP_TO_NS = T0 + 180 * MINUTE_NS
SPAN_END_NS = T0 + 360 * MINUTE_NS
SAMPLE_MINUTES = [*range(121), *range(180, 360)]

#: Per-asset funding. BTC and ETH are quoted hourly like the real venue; SOL is
#: quoted on an 8-hour interval so the normalisation in `screener.hourly_rate`
#: has something to actually do. WIF carries a full-scale rate and mark, to
#: prove nothing rounds on the way out.
#:
#:   asset -> (rate, interval_s, mark)
ASSETS: dict[str, tuple[Decimal, int, Decimal]] = {
    "BTC": (Decimal("0.0000125"), 3600, Decimal("112345.5")),
    "ETH": (Decimal("-0.00002"), 3600, Decimal(3500)),
    "SOL": (Decimal("0.0002"), 28800, Decimal(150)),
    "WIF": (Decimal("0.000000000000000123"), 3600, Decimal("112345.678901234567890123")),
}

#: Hourly rate each asset's sample normalises to, hand-computed:
#:   BTC 0.0000125 * 3600 / 3600  = 0.0000125
#:   SOL 0.0002    * 3600 / 28800 = 0.000025   <- the 8x the interval is worth
HOURLY: dict[str, Decimal] = {
    "BTC": Decimal("0.0000125"),
    "ETH": Decimal("-0.00002"),
    "SOL": Decimal("0.000025"),
    "WIF": Decimal("0.000000000000000123"),
}

BTC_OPEN_INTEREST = Decimal("1234.5")
#: szDecimals is not on disk, so the API recovers it from observed trade sizes.
#: These two have different scales (5 and 4) to prove it is per-asset.
TRADE_SIZES: dict[str, Decimal] = {"BTC": Decimal("0.00123"), "ETH": Decimal("0.1234")}


def _write(root: Path, stream: str, records: list[Any], asset: str | None, seq: int) -> None:
    write_table(
        records_to_table(stream, records),
        partition_path(root, stream, VENUE, asset, date_for_ns(T0), T0, seq),
    )


def build_dataset(root: Path) -> None:
    """Write a small but real Parquet tree: funding, OI, trades and one book."""
    funding = [
        FundingSample(
            venue=VENUE,
            asset=asset,
            ts_event_ns=T0 + minute * MINUTE_NS,
            ts_ingest_ns=T0 + minute * MINUTE_NS + 1_000,
            rate=rate,
            interval_s=interval_s,
            mark_price=mark,
        )
        for asset, (rate, interval_s, mark) in ASSETS.items()
        for minute in SAMPLE_MINUTES
    ]
    _write(root, "funding", funding, None, 1)

    # OI for BTC only: the cross-section must tolerate an asset with funding and
    # no open interest, which is the normal state while a poll is in flight.
    _write(
        root,
        "open_interest",
        [
            OpenInterest(
                venue=VENUE,
                asset="BTC",
                ts_event_ns=T0 + minute * MINUTE_NS,
                ts_ingest_ns=T0 + minute * MINUTE_NS + 1_000,
                open_interest=BTC_OPEN_INTEREST,
            )
            for minute in SAMPLE_MINUTES
        ],
        None,
        2,
    )

    for seq, (asset, size) in enumerate(TRADE_SIZES.items(), start=3):
        _write(
            root,
            "trades",
            [
                Trade(
                    venue=VENUE,
                    asset=asset,
                    ts_event_ns=T0,
                    ts_ingest_ns=T0 + 1_000,
                    price=ASSETS[asset][2],
                    size=size,
                    side="buy",
                    trade_id=f"t-{asset}",
                )
            ],
            asset,
            seq,
        )

    # One book snapshot, for BTC only: `has_l2` must reflect what is on disk
    # rather than what the config asked for.
    _write(
        root,
        "l2",
        [
            BookSnapshot(
                venue=VENUE,
                asset="BTC",
                ts_event_ns=T0,
                ts_ingest_ns=T0 + 1_000,
                bids=(BookLevel(Decimal("112345.0"), Decimal("1.5"), 3),),
                asks=(BookLevel(Decimal("112346.0"), Decimal("2.5"), 4),),
            )
        ],
        "BTC",
        9,
    )


def _client(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("FA_DATA_DIR", str(data_dir))
    # Point FA_CONFIG at nothing: the API must stand up without the recorder's
    # YAML, which is the deployment this endpoint actually has.
    monkeypatch.setenv("FA_CONFIG", str(data_dir / "no-such-config.yaml"))
    monkeypatch.delenv("FA_API_KEYS", raising=False)
    deps.reset_state()
    with TestClient(create_app()) as client:
        yield client
    deps.reset_state()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    root = tmp_path / "data"
    build_dataset(root)
    yield from _client(root, monkeypatch)


@pytest.fixture
def empty_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """A recorder that has just started: the directory exists and holds nothing."""
    root = tmp_path / "empty"
    root.mkdir()
    yield from _client(root, monkeypatch)


def rows_by_asset(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["asset"]: row for row in rows}


# -- health / stats ---------------------------------------------------------


def test_health_reports_last_write_and_rows_per_stream(client: TestClient) -> None:
    body = client.get("/health").json()

    assert body["status"] == "degraded"  # the fixture is historical, so lag is large
    funding = body["streams"]["funding"]
    assert funding["rows"] == len(SAMPLE_MINUTES) * len(ASSETS)
    assert funding["last_write_ns"] == str(T0 + 359 * MINUTE_NS + 1_000)
    assert funding["lag_s"] > 0
    # A configured-but-silent stream is reported with nulls, not omitted.
    assert body["streams"]["mids"] == {"last_write_ns": None, "lag_s": None, "rows": 0}
    assert body["disk_free_bytes"] > 0
    assert body["uptime_s"] >= 0


def test_stats_reports_span_files_and_bytes(client: TestClient) -> None:
    body = client.get("/stats").json()

    funding = body["streams"]["funding"]
    assert funding["first_ns"] == str(T0)
    assert funding["last_ns"] == str(T0 + 359 * MINUTE_NS)
    assert funding["files"] == 1
    assert funding["bytes"] > 0
    assert body["streams"]["mids"] == {
        "rows": 0,
        "first_ns": None,
        "last_ns": None,
        "files": 0,
        "bytes": 0,
    }
    assert body["venue"] == VENUE
    assert body["network"] == "mainnet"


# -- assets -----------------------------------------------------------------


def test_assets_derives_specs_from_observed_data(client: TestClient) -> None:
    body = client.get("/assets").json()
    assets = rows_by_asset(body["assets"])

    assert set(assets) == set(ASSETS)
    assert body["as_of_ns"] == str(T0 + 359 * MINUTE_NS)

    btc = assets["BTC"]
    # szDecimals recovered as 5 from a size of 0.00123, so lot = 1e-5 and the
    # price allowance is 6 - 5 = 1 decimal.
    assert btc["size_lot"] == "0.00001"
    assert btc["price_tick"] == "0.1"
    assert btc["price_sig_figs"] == 5
    assert btc["min_notional"] == "10"
    assert btc["has_l2"] is True
    assert btc["first_seen_ns"] == str(T0)
    # Venue metadata the recorder does not persist stays null rather than guessed.
    assert btc["max_leverage"] is None

    # ETH trades at 0.1234 -> szDecimals 4, so a coarser lot and a finer tick.
    assert assets["ETH"]["size_lot"] == "0.0001"
    assert assets["ETH"]["price_tick"] == "0.01"
    assert assets["ETH"]["has_l2"] is False

    # No trades for SOL, so there is nothing to derive precision from.
    assert assets["SOL"]["size_lot"] is None
    assert assets["SOL"]["price_tick"] is None


# -- funding/current --------------------------------------------------------


def test_current_annualises_from_interval_s_and_sorts_by_apr(client: TestClient) -> None:
    body = client.get("/funding/current").json()

    assert body["as_of_ns"] == str(T0 + 359 * MINUTE_NS)
    # SOL's rate is numerically 16x BTC's but quoted on an 8-hour interval, so
    # it ranks 2x higher — not 16x. Hardcoding an interval anywhere would put
    # these two in the wrong order.
    assert [row["asset"] for row in body["rows"]] == ["SOL", "BTC", "WIF", "ETH"]

    rows = rows_by_asset(body["rows"])
    for asset, hourly in HOURLY.items():
        assert Decimal(rows[asset]["rate_hourly"]) == hourly
        assert Decimal(rows[asset]["apr"]) == hourly * 24 * 365

    assert Decimal(rows["SOL"]["apr"]) == Decimal("0.219")
    assert Decimal(rows["BTC"]["apr"]) == Decimal("0.1095")
    # Negative funding is kept, not dropped: it is a long's opportunity.
    assert Decimal(rows["ETH"]["apr"]) == Decimal("-0.1752")


def test_current_open_interest_is_notional_against_mark(client: TestClient) -> None:
    rows = rows_by_asset(client.get("/funding/current").json()["rows"])

    assert Decimal(rows["BTC"]["open_interest"]) == BTC_OPEN_INTEREST
    # openInterest is in coin units, so notional needs the mark.
    assert Decimal(rows["BTC"]["oi_notional"]) == BTC_OPEN_INTEREST * ASSETS["BTC"][2]
    # No OI recorded for ETH: null, not zero. Zero would sort as a real market.
    assert rows["ETH"]["open_interest"] is None
    assert rows["ETH"]["oi_notional"] is None


def test_decimals_arrive_as_strings_with_no_precision_loss(client: TestClient) -> None:
    rows = rows_by_asset(client.get("/funding/current").json()["rows"])
    wif = rows["WIF"]

    # Strings, not JSON numbers: a consumer must not be able to round these by
    # accident on the way in.
    assert isinstance(wif["rate_hourly"], str)
    assert isinstance(wif["mark"], str)

    rate, _, mark = ASSETS["WIF"]
    assert wif["rate_hourly"] == "0.000000000000000123"
    assert Decimal(wif["rate_hourly"]) == rate
    assert Decimal(wif["mark"]) == mark
    # The same value through a float would not survive: that is the whole point.
    assert Decimal(str(float(wif["mark"]))) != mark
    assert Decimal(wif["apr"]) == rate * 24 * 365


# -- funding/history --------------------------------------------------------


def test_history_buckets_hourly_and_reports_the_gap(client: TestClient) -> None:
    body = client.get(
        "/funding/history",
        params={"asset": "BTC", "from_ns": str(T0), "to_ns": str(SPAN_END_NS), "bucket": "1h"},
    ).json()

    assert body["asset"] == "BTC"
    assert body["bucket"] == "1h"
    # Six hours of buckets minus the two whose samples are missing entirely;
    # the hole spans minutes 121-179, so hour 2 keeps its first sample.
    ts = [point["ts_ns"] for point in body["points"]]
    assert ts == [str(T0 + hour * HOUR_NS) for hour in (0, 1, 2, 3, 4, 5)]
    # Bucket edges land on real UTC hours, not on the first sample seen.
    assert all(int(point) % HOUR_NS == 0 for point in ts)

    for point in body["points"]:
        assert Decimal(point["rate_hourly"]) == HOURLY["BTC"]
        assert Decimal(point["mark"]) == ASSETS["BTC"][2]

    # The coverage block is the point of this endpoint: the chart above has a
    # hole in it, and the response says exactly where.
    coverage = body["coverage"]
    assert coverage["covered"] is False
    assert coverage["gaps"] == [
        {
            "from_ns": str(GAP_FROM_NS),
            "to_ns": str(GAP_TO_NS),
            "reason": "silence",
            "duration_s": 3600.0,
        }
    ]
    assert coverage["missing_s"] == 3600.0


def test_history_reports_covered_on_a_clean_range(client: TestClient) -> None:
    body = client.get(
        "/funding/history",
        params={"asset": "BTC", "from_ns": str(T0), "to_ns": T0 + 60 * MINUTE_NS, "bucket": "1h"},
    ).json()

    assert body["coverage"] == {"covered": True, "missing_s": 0.0, "gaps": []}


def test_history_daily_bucket_averages_every_sample(client: TestClient) -> None:
    body = client.get(
        "/funding/history",
        params={"asset": "SOL", "from_ns": str(T0), "to_ns": str(SPAN_END_NS), "bucket": "1d"},
    ).json()

    assert len(body["points"]) == 1
    point = body["points"][0]
    assert point["ts_ns"] == str(T0)  # T0 is a UTC midnight
    # Every sample carries the same rate, so the mean is that rate normalised
    # from its own 8-hour interval.
    assert Decimal(point["rate_hourly"]) == HOURLY["SOL"]


# -- funding/screener -------------------------------------------------------


def test_screener_arithmetic_matches_hand_computed_values(client: TestClient) -> None:
    body = client.get(
        "/funding/screener", params={"notional": "1000", "holding_hours": "24", "side": "short"}
    ).json()
    rows = rows_by_asset(body["rows"])

    # BTC, short, $1000, 24h, hand-computed:
    #   rate_hourly = 0.0000125                       (interval_s 3600)
    #   apr         = 0.0000125 * 8760       = 0.1095
    #   gross_pnl   = 1000 * 0.0000125 * 24  = 0.30
    #   fee_cost    = 1000 * 0.00045 * 4     = 1.80   (2 legs x entry+exit)
    #   net_pnl     = 0.30 - 1.80            = -1.50
    #   net_apr     = (-1.50/1000) * 8760/24 = -0.5475
    #   breakeven_h = 1.80 / (1000 * 0.0000125) = 144
    btc = rows["BTC"]
    assert Decimal(btc["apr"]) == Decimal("0.1095")
    assert Decimal(btc["gross_pnl"]) == Decimal("0.30")
    assert Decimal(btc["fee_cost"]) == Decimal("1.80")
    assert Decimal(btc["net_pnl"]) == Decimal("-1.50")
    assert Decimal(btc["net_apr"]) == Decimal("-0.5475")
    assert Decimal(btc["breakeven_hours"]) == Decimal(144)
    # 24h of this carry does not pay for four fills, which is the answer the
    # headline APR hides.
    assert btc["covers_fees"] is False

    # SOL: 8-hour quote, so its hourly carry is 0.000025 and its breakeven is
    #   1.80 / (1000 * 0.000025) = 72h. Derived from interval_s, not assumed.
    sol = rows["SOL"]
    assert Decimal(sol["gross_pnl"]) == Decimal("0.60")
    assert Decimal(sol["breakeven_hours"]) == Decimal(72)
    assert Decimal(sol["net_apr"]) == Decimal("-0.438")

    # Ranked by net APR, not gross: the question is what clears its costs.
    assert [row["asset"] for row in body["rows"]] == ["SOL", "BTC", "WIF", "ETH"]
    assert Decimal(rows["SOL"]["net_apr"]) > Decimal(rows["BTC"]["net_apr"])


def test_screener_fee_cost_is_a_four_fill_round_trip(client: TestClient) -> None:
    body = client.get("/funding/screener", params={"notional": "1000"}).json()
    assumptions = body["assumptions"]

    assert assumptions["fills"] == 4
    assert assumptions["legs"] == 2
    assert assumptions["fee_role"] == "taker"
    assert Decimal(assumptions["taker_fee_rate"]) == sc.TAKER_FEE_RATE
    assert Decimal(assumptions["maker_fee_rate"]) == sc.MAKER_FEE_RATE
    assert assumptions["hours_per_year"] == 8760
    # The interval is read off the data, so a caller can see that the hourly
    # normalisation was derived rather than assumed.
    assert assumptions["funding_interval_s"] == [3600, 28800]
    assert "docs/hyperliquid-api.md" in assumptions["fee_source"]

    for row in body["rows"]:
        assert Decimal(row["fee_cost"]) == Decimal(1000) * sc.TAKER_FEE_RATE * 4


def test_screener_covers_fees_once_the_holding_period_clears_breakeven(
    client: TestClient,
) -> None:
    body = client.get(
        "/funding/screener", params={"notional": "1000", "holding_hours": "200"}
    ).json()
    btc = rows_by_asset(body["rows"])["BTC"]

    # 200h > the 144h breakeven, so the carry is real at this horizon:
    #   gross = 1000 * 0.0000125 * 200 = 2.50, net = 2.50 - 1.80 = 0.70
    assert Decimal(btc["breakeven_hours"]) == Decimal(144)
    assert Decimal(btc["net_pnl"]) == Decimal("0.70")
    assert Decimal(btc["net_apr"]) == Decimal("0.03066")
    assert btc["covers_fees"] is True


def test_screener_side_long_flips_the_sign_without_dropping_rows(client: TestClient) -> None:
    short = rows_by_asset(client.get("/funding/screener", params={"side": "short"}).json()["rows"])
    long = client.get(
        "/funding/screener", params={"side": "long", "notional": "1000", "holding_hours": "24"}
    ).json()
    rows = rows_by_asset(long["rows"])

    assert long["assumptions"]["side"] == "long"
    # Every asset survives the flip: a short's loss is a long's carry.
    assert set(rows) == set(short)
    for asset in rows:
        assert Decimal(rows[asset]["apr"]) == -Decimal(short[asset]["apr"])
        assert Decimal(rows[asset]["gross_pnl"]) == -Decimal(short[asset]["gross_pnl"])

    # ETH funding is negative, so the long is the side that gets paid:
    #   carry = +0.00002/h, gross = 1000 * 0.00002 * 24 = 0.48,
    #   breakeven = 1.80 / (1000 * 0.00002) = 90h
    eth = rows["ETH"]
    assert Decimal(eth["apr"]) == Decimal("0.1752")
    assert Decimal(eth["gross_pnl"]) == Decimal("0.48")
    assert Decimal(eth["breakeven_hours"]) == Decimal(90)
    assert eth["covers_fees"] is False

    # BTC funding is positive, so no holding period ever repays a long's fees.
    assert rows["BTC"]["breakeven_hours"] is None
    assert rows["BTC"]["covers_fees"] is False


def test_screener_rejects_a_nonsensical_holding_period(client: TestClient) -> None:
    assert client.get("/funding/screener", params={"holding_hours": "0"}).status_code == 422
    assert client.get("/funding/screener", params={"notional": "-5"}).status_code == 422


# -- coverage ---------------------------------------------------------------


def test_coverage_reports_a_gap_on_a_range_that_straddles_the_hole(client: TestClient) -> None:
    body = client.get(
        "/coverage",
        params={
            "stream": "funding",
            "asset": "BTC",
            "from_ns": GAP_FROM_NS - 10 * MINUTE_NS,
            "to_ns": GAP_TO_NS + 10 * MINUTE_NS,
        },
    ).json()

    assert body["covered"] is False
    assert body["gaps"] == [
        {
            "from_ns": str(GAP_FROM_NS),
            "to_ns": str(GAP_TO_NS),
            "reason": "silence",
            "duration_s": 3600.0,
        }
    ]
    assert body["missing_s"] == 3600.0


def test_coverage_is_clean_on_a_range_inside_the_recorded_window(client: TestClient) -> None:
    body = client.get(
        "/coverage",
        params={
            "stream": "funding",
            "from_ns": T0 + 10 * MINUTE_NS,
            "to_ns": T0 + 110 * MINUTE_NS,
        },
    ).json()

    assert body == {"covered": True, "missing_s": 0.0, "gaps": []}


def test_coverage_clips_a_gap_to_the_window_asked_about(client: TestClient) -> None:
    # Half of the hole. The caller should be told about its own window, not the
    # gap's full historical extent.
    midpoint = GAP_FROM_NS + 30 * MINUTE_NS
    body = client.get(
        "/coverage", params={"stream": "funding", "from_ns": str(midpoint), "to_ns": GAP_TO_NS}
    ).json()

    assert body["gaps"] == [
        {
            "from_ns": str(midpoint),
            "to_ns": str(GAP_TO_NS),
            "reason": "silence",
            "duration_s": 1800.0,
        }
    ]


def test_coverage_does_not_judge_trades_by_silence(client: TestClient) -> None:
    # A market-driven stream has no cadence, so a quiet hour is not a gap. The
    # fixture holds exactly one trade and the window is six hours wide.
    body = client.get(
        "/coverage",
        params={"stream": "trades", "asset": "BTC", "from_ns": str(T0), "to_ns": SPAN_END_NS},
    ).json()

    assert body == {"covered": True, "missing_s": 0.0, "gaps": []}


def test_coverage_rejects_an_unknown_stream(client: TestClient) -> None:
    response = client.get("/coverage", params={"stream": "fundng"})

    # 400, not an empty "covered" answer: `covered: true` for a typo is the one
    # response that would let a backtest run over nothing.
    assert response.status_code == 400
    assert "unknown stream" in response.json()["detail"]


# -- empty dataset ----------------------------------------------------------


def test_health_on_an_empty_dataset_is_empty_not_degraded(empty_client: TestClient) -> None:
    body = empty_client.get("/health").json()

    assert body["status"] == "empty"
    assert all(stream["rows"] == 0 for stream in body["streams"].values())
    assert all(stream["last_write_ns"] is None for stream in body["streams"].values())
    assert body["disk_free_bytes"] > 0


def test_every_endpoint_serves_an_empty_dataset_without_failing(empty_client: TestClient) -> None:
    # A recorder that started a minute ago has written nothing. That is a valid
    # state, and none of these may 500 over it.
    assert empty_client.get("/stats").json()["streams"]["funding"]["rows"] == 0
    assert empty_client.get("/assets").json() == {"assets": [], "as_of_ns": None}
    assert empty_client.get("/funding/current").json() == {"as_of_ns": None, "rows": []}

    history = empty_client.get("/funding/history", params={"asset": "BTC"}).json()
    assert history["points"] == []
    # Honest, not cheerful: nothing is recorded, so nothing is covered.
    assert history["coverage"]["covered"] is False

    screener = empty_client.get("/funding/screener").json()
    assert screener["rows"] == []
    assert screener["assumptions"]["funding_interval_s"] == []
    assert screener["assumptions"]["as_of_ns"] is None

    assert empty_client.get("/coverage").json()["covered"] is False


# -- read-only / auth seam --------------------------------------------------


def test_api_never_writes_to_the_data_directory(client: TestClient) -> None:
    root = Path(deps.get_settings().data_dir)
    before = {path: path.stat().st_mtime_ns for path in sorted(root.rglob("*")) if path.is_file()}

    for url, params in (
        ("/health", None),
        ("/stats", None),
        ("/assets", None),
        ("/funding/current", None),
        ("/funding/history", {"asset": "BTC"}),
        ("/funding/screener", None),
        ("/coverage", None),
    ):
        assert client.get(url, params=params).status_code == 200

    after = {path: path.stat().st_mtime_ns for path in sorted(root.rglob("*")) if path.is_file()}
    assert after == before


def test_api_key_dependency_enforces_only_when_keys_are_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    build_dataset(root)
    monkeypatch.setenv("FA_DATA_DIR", str(root))
    monkeypatch.setenv("FA_CONFIG", str(root / "no-such-config.yaml"))
    monkeypatch.setenv("FA_API_KEYS", "secret-one,secret-two")
    deps.reset_state()
    try:
        with TestClient(create_app()) as client:
            # The seam is live on every router without a route body knowing.
            assert client.get("/health").status_code == 401
            assert client.get("/funding/current", headers={"X-API-Key": "nope"}).status_code == 401
            ok = client.get("/health", headers={"X-API-Key": "secret-one"})
            assert ok.status_code == 200
    finally:
        deps.reset_state()


def test_cors_allows_the_configured_dashboard_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    build_dataset(root)
    monkeypatch.setenv("FA_DATA_DIR", str(root))
    monkeypatch.setenv("FA_CONFIG", str(root / "no-such-config.yaml"))
    monkeypatch.setenv("FA_API_CORS_ORIGINS", "https://atlas.example")
    deps.reset_state()
    try:
        with TestClient(create_app()) as client:
            response = client.get("/health", headers={"Origin": "https://atlas.example"})
            assert response.headers["access-control-allow-origin"] == "https://atlas.example"
            other = client.get("/health", headers={"Origin": "https://evil.example"})
            assert "access-control-allow-origin" not in other.headers
    finally:
        deps.reset_state()


# -- recorded asset specs ---------------------------------------------------


@pytest.fixture
def specs_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """A dataset that also has the `asset_specs` stream the recorder now writes."""
    from funding_atlas.venues.records import AssetSpecSample

    root = tmp_path / "data"
    build_dataset(root)
    _write(
        root,
        "asset_specs",
        [
            AssetSpecSample(
                venue=VENUE,
                asset="BTC",
                ts_event_ns=T0 + 100,
                ts_ingest_ns=T0 + 100,
                price_tick=Decimal("0.1"),
                price_sig_figs=5,
                size_lot=Decimal("0.00001"),
                min_notional=Decimal(10),
                max_leverage=Decimal(40),
                is_perp=True,
                is_delisted=False,
                ts_event_synthetic=True,
            )
        ],
        None,
        1,
    )
    yield from _client(root, monkeypatch)


def test_assets_prefers_recorded_specs_over_tape_inference(specs_client: TestClient) -> None:
    """A recorded rule beats a reconstruction, and says which it is.

    Inference from observed trade sizes is a bound, not a rule — an asset traded
    in round numbers looks coarser than the venue allows. A consumer sizing an
    order has to be able to tell the two apart.
    """
    rows = rows_by_asset(specs_client.get("/assets").json()["assets"])

    btc = rows["BTC"]
    assert btc["source"] == "recorded"
    assert btc["max_leverage"] == "40"  # unobtainable from the tape at any price
    assert btc["price_tick"] == "0.1"
    assert btc["size_lot"] == "0.00001"

    # An asset with no recorded spec still answers, flagged as reconstructed.
    others = [r for a, r in rows.items() if a != "BTC"]
    assert others, "fixture should contain an asset without a recorded spec"
    assert all(r["source"] == "inferred" for r in others)
    assert all(r["max_leverage"] is None for r in others)


def test_a_healthy_recorder_is_not_degraded_merely_for_buffering(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Flushed data is older than live data by up to a rotation.

    The fixture is historical, so every stream is hours stale — which is what a
    freshly flushed partition looks like from here. With the flush interval
    accounted for, that must not read as an incident, or the status page is red
    forever and stops meaning anything.
    """
    from funding_atlas.api.routes import health as health_route

    limit = health_route._lag_limit_s("funding")
    assert limit > health_route.MAX_SILENCE_S, "flush interval must widen the limit"
    assert limit >= health_route.FLUSH_INTERVAL_S

    # A market-driven stream still has no limit: a quiet perp cannot turn the
    # page red, however long it has been silent.
    assert health_route._lag_limit_s("trades") == float("inf")


def test_coverage_does_not_invent_a_hole_in_the_unflushed_window(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A recorder that is recording but has not flushed has no gap.

    Found live: the dashboard reported a 22-minute hole where the recorder's own
    event log recorded 45 seconds. The difference was buffered data — present,
    simply not yet on disk. Publishing a hole that did not happen is as damaging
    as hiding one that did, because a researcher discards good data over it.
    """
    from funding_atlas.api import coverage as cov

    # Real wall-clock now: the clipping is defined against it, so a fixture
    # timestamp from hours ago is already settled and would be judged normally.
    now = time.time_ns()
    settled = cov.settled_before_ns(now)
    assert now - settled >= cov.FLUSH_INTERVAL_S * NS_PER_S, (
        "the unjudgeable window must cover at least one rotation"
    )

    # A range entirely inside the unflushed window yields no inferred gap at all.
    body = client.get(
        "/coverage",
        params={
            "stream": "funding",
            "from_ns": str(now - 60 * 10**9),
            "to_ns": str(now),
        },
    ).json()
    assert all(g["reason"] != "silence" for g in body["gaps"]), (
        "absence from disk in the last rotation is not evidence of absence"
    )
