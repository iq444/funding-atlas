"""Schema, partition path and atomic-write tests for the Parquet store.

The assertions that matter are about exactness: a Decimal that survives a
round-trip as "almost" the same number is the bug this module exists to make
impossible.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from funding_atlas.store import parquet as store
from funding_atlas.venues.records import (
    STREAMS,
    AssetSpecSample,
    BookLevel,
    BookSnapshot,
    FundingSample,
    Mid,
    OpenInterest,
    Trade,
)

TS = 1_760_000_000_123_456_789  # 2025-10-09T08:53:20Z, a mid-day instant
VENUE = "hyperliquid"

# Both ends of the range the decimal type has to cover without rounding.
MEMECOIN_PRICE = Decimal("0.000000000000012345")
BTC_PRICE = Decimal("112345.678901234567")


def _trade() -> Trade:
    return Trade(
        venue=VENUE,
        asset="BTC",
        ts_event_ns=TS,
        ts_ingest_ns=TS + 1_000_000,
        price=BTC_PRICE,
        size=Decimal("0.00012345"),
        side="buy",
        trade_id="t-1",
    )


def _snapshot() -> BookSnapshot:
    return BookSnapshot(
        venue=VENUE,
        asset="ETH",
        ts_event_ns=TS,
        ts_ingest_ns=TS + 500_000,
        bids=(
            BookLevel(price=Decimal("3999.12"), size=Decimal("1.5"), n_orders=3),
            BookLevel(price=Decimal("3999.11"), size=Decimal("2.25"), n_orders=None),
        ),
        asks=(BookLevel(price=Decimal("3999.13"), size=Decimal("0.75"), n_orders=1),),
    )


SAMPLES: dict[str, list[object]] = {
    "trades": [
        _trade(),
        Trade(
            venue=VENUE,
            asset="PEPE",
            ts_event_ns=TS + 1,
            ts_ingest_ns=TS + 2,
            price=MEMECOIN_PRICE,
            size=Decimal("1234567890.123456789"),
            side="sell",
            trade_id="t-2",
        ),
    ],
    "mids": [Mid(venue=VENUE, asset="BTC", ts_event_ns=TS, ts_ingest_ns=TS + 5, mid=BTC_PRICE)],
    "l2": [_snapshot()],
    "funding": [
        FundingSample(
            venue=VENUE,
            asset="BTC",
            ts_event_ns=TS,
            ts_ingest_ns=TS + 7,
            rate=Decimal("0.0000125"),
            interval_s=3600,
            premium=Decimal("-0.000004321"),
            oracle_price=BTC_PRICE,
            mark_price=None,
        )
    ],
    "open_interest": [
        OpenInterest(
            venue=VENUE,
            asset="BTC",
            ts_event_ns=TS,
            ts_ingest_ns=TS + 9,
            open_interest=Decimal("12345.6789"),
        )
    ],
    # Both rows share one `ts_event_ns`: the venue describes one instant per
    # poll. The second carries the awkward states — a sub-satoshi tick, a
    # delisted asset, and no significant-figure rule at all.
    "asset_specs": [
        AssetSpecSample(
            venue=VENUE,
            asset="BTC",
            ts_event_ns=TS,
            ts_ingest_ns=TS + 11,
            price_tick=Decimal("0.1"),
            price_sig_figs=5,
            size_lot=Decimal("0.00001"),
            min_notional=Decimal(10),
            max_leverage=Decimal(40),
            is_perp=True,
        ),
        AssetSpecSample(
            venue=VENUE,
            asset="kPEPE",
            ts_event_ns=TS,
            ts_ingest_ns=TS + 11,
            price_tick=MEMECOIN_PRICE,
            price_sig_figs=None,
            size_lot=Decimal("0.000000000000000001"),
            min_notional=Decimal("10.000000000000000001"),
            max_leverage=Decimal(3),
            is_perp=True,
            is_delisted=True,
        ),
    ],
}


def test_every_stream_has_a_schema_and_a_sample() -> None:
    assert set(SAMPLES) == set(STREAMS)
    for stream in STREAMS:
        assert store.schema_for(stream) is not None


def test_decimal_type_spans_memecoins_and_bitcoin() -> None:
    assert store.DECIMAL_SCALE >= 18
    field = store.schema_for("mids").field("mid")
    assert field.type.scale == store.DECIMAL_SCALE
    assert field.type.precision == store.DECIMAL_PRECISION
    # Integer headroom must cover a large notional as well as a tiny tick.
    assert store.DECIMAL_PRECISION - store.DECIMAL_SCALE >= 12


@pytest.mark.parametrize("stream", sorted(SAMPLES))
def test_round_trip_preserves_values_exactly(stream: str, tmp_path: Path) -> None:
    records = SAMPLES[stream]
    path = tmp_path / f"{stream}.parquet"
    store.write_table(store.records_to_table(stream, records), path)

    table = pq.read_table(path)
    assert table.schema.remove_metadata().equals(store.schema_for(stream))
    rows = table.to_pylist()
    assert len(rows) == len(records)

    for row, record in zip(rows, records, strict=True):
        for name in table.column_names:
            got = row[name]
            want = getattr(record, name)
            if stream == "l2" and name in ("bids", "asks"):
                assert [tuple(lv.values()) for lv in got] == [
                    (lv.price, lv.size, lv.n_orders) for lv in want
                ]
                continue
            if isinstance(want, Decimal):
                # Equal *and* same scale-independent value: Decimal("1.5") ==
                # Decimal("1.500") so compare normalised, then insist the
                # magnitude is bit-identical rather than merely close.
                assert isinstance(got, Decimal)
                assert got == want, f"{stream}.{name}"
                assert got.normalize() == want.normalize()
            else:
                assert got == want, f"{stream}.{name}"


def test_decimal_round_trip_is_not_float_rounding(tmp_path: Path) -> None:
    # A value that float64 cannot represent: if the column were float, this
    # assertion is the one that fails.
    price = Decimal("0.100000000000000005")
    mid = Mid(venue=VENUE, asset="X", ts_event_ns=TS, ts_ingest_ns=TS, mid=price)
    path = tmp_path / "mids.parquet"
    store.write_table(store.records_to_table("mids", [mid]), path)
    (got,) = pq.read_table(path).column("mid").to_pylist()
    assert got == price
    assert got != Decimal(float(price))


def test_more_fractional_digits_than_scale_raises_rather_than_truncating(tmp_path: Path) -> None:
    too_fine = Decimal("1." + "0" * store.DECIMAL_SCALE + "1")
    mid = Mid(venue=VENUE, asset="X", ts_event_ns=TS, ts_ingest_ns=TS, mid=too_fine)
    # pyarrow.lib.ArrowInvalid, a ValueError: "Rescaling Decimal value would
    # cause data loss". Loud beats rounded.
    with pytest.raises(ValueError, match="data loss"):
        store.records_to_table("mids", [mid])


def test_schema_version_in_file_metadata(tmp_path: Path) -> None:
    path = tmp_path / "trades.parquet"
    store.write_table(store.records_to_table("trades", SAMPLES["trades"]), path)
    meta = pq.read_schema(path).metadata
    assert meta[store.SCHEMA_VERSION_KEY] == str(store.SCHEMA_VERSION).encode()
    assert meta[store.STREAM_KEY] == b"trades"
    assert store.read_schema_version(path) == store.SCHEMA_VERSION


def test_partition_paths_match_documented_layout(tmp_path: Path) -> None:
    day = dt.date(2026, 10, 13)
    path = store.partition_path(tmp_path, "trades", VENUE, "BTC", day, TS, 7)
    assert path.parent == (tmp_path / "trades" / f"venue={VENUE}" / "asset=BTC" / "date=2026-10-13")
    assert path.name == f"{TS}-000007.parquet"

    # funding / open_interest / asset_specs are deliberately not partitioned by
    # asset: all three are cross-sectional by nature, and asset_specs most of
    # all — "what were the rules for the whole universe at time T" must be one
    # file, not 200 directories.
    assert store.partition_dir(tmp_path, "funding", VENUE, "BTC", day) == (
        tmp_path / "funding" / f"venue={VENUE}" / "date=2026-10-13"
    )
    assert store.partition_dir(tmp_path, "asset_specs", VENUE, None, day) == (
        tmp_path / "asset_specs" / f"venue={VENUE}" / "date=2026-10-13"
    )
    assert "asset_specs" not in store.ASSET_PARTITIONED
    assert store.partition_dir(tmp_path, "l2", VENUE, "ETH", day).parts[-2] == "asset=ETH"


def test_asset_required_for_asset_partitioned_streams(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        store.partition_dir(tmp_path, "trades", VENUE, None, dt.date(2026, 1, 1))
    with pytest.raises(KeyError):
        store.partition_dir(tmp_path, "nope", VENUE, "BTC", dt.date(2026, 1, 1))


def test_partition_date_is_utc() -> None:
    # 23:59:59.999999999Z and the nanosecond after it must land on either side.
    end_of_day = 1_760_399_999_999_999_999  # 2025-10-13T23:59:59.999999999Z
    assert store.date_for_ns(end_of_day) == dt.date(2025, 10, 13)
    assert store.date_for_ns(end_of_day + 1) == dt.date(2025, 10, 14)


def test_stream_of_maps_record_types() -> None:
    assert store.stream_of(_trade()) == "trades"
    assert store.stream_of(_snapshot()) == "l2"
    with pytest.raises(TypeError):
        store.stream_of(object())


def test_write_is_atomic_and_leaves_no_tmp(tmp_path: Path) -> None:
    path = tmp_path / "a" / "b" / "trades.parquet"
    store.write_table(store.records_to_table("trades", SAMPLES["trades"]), path)
    assert path.exists()
    assert list(tmp_path.rglob("*" + store.TMP_SUFFIX)) == []


def test_failed_write_leaves_neither_target_nor_tmp(tmp_path: Path, monkeypatch) -> None:
    table = store.records_to_table("mids", SAMPLES["mids"])
    path = tmp_path / "mids.parquet"

    class Boom(Exception):
        pass

    def explode(*args: object, **kw: object) -> None:
        raise Boom

    monkeypatch.setattr(store.pq, "ParquetWriter", explode)
    with pytest.raises(Boom):
        store.write_table(table, path)
    # The failure mode that matters: research globs *.parquet, so a partial file
    # under the final name would read as data.
    assert not path.exists()
    assert list(tmp_path.rglob("*" + store.TMP_SUFFIX)) == []


def test_duckdb_reads_back_the_same_decimals(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    day = store.date_for_ns(TS)
    for seq, record in enumerate(SAMPLES["trades"], start=1):
        path = store.partition_path(tmp_path, "trades", VENUE, record.asset, day, TS + seq, seq)
        store.write_table(store.records_to_table("trades", [record]), path)

    glob = str(tmp_path / "trades" / "**" / "*.parquet")
    con = duckdb.connect()
    rows = con.execute(
        "select asset, price from read_parquet(?) order by ts_event_ns", [glob]
    ).fetchall()
    assert rows == [("BTC", BTC_PRICE), ("PEPE", MEMECOIN_PRICE)]
    assert all(isinstance(price, Decimal) for _, price in rows)

    # Hive keys are recoverable from the paths and agree with the columns kept
    # inside the files, so a partition pruned by directory is not a different
    # answer from one filtered by column.
    hive = con.execute(
        "select asset, venue, date from read_parquet(?, hive_partitioning=true) "
        "where asset = 'PEPE'",
        [glob],
    ).fetchall()
    assert hive == [("PEPE", VENUE, dt.date(2025, 10, 9))]


def test_duckdb_reads_book_lists(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "l2.parquet"
    store.write_table(store.records_to_table("l2", SAMPLES["l2"]), path)
    con = duckdb.connect()
    # The query shape the layout is chosen for: spread straight off one row.
    spread, depth = con.execute(
        "select asks[1].price - bids[1].price, "
        "list_sum(list_transform(bids, b -> b.size)) from read_parquet(?)",
        [str(path)],
    ).fetchone()
    assert spread == Decimal("0.01")
    assert depth == Decimal("3.75")


def test_asset_spec_decimals_and_flags_round_trip(tmp_path: Path) -> None:
    """The specs are what an order is rounded by, so an approximate lot step is
    a rejected order at best and a mis-sized hedge at worst. `price_sig_figs` is
    nullable because "the venue has no such rule" is a real state."""
    path = tmp_path / "asset_specs.parquet"
    store.write_table(store.records_to_table("asset_specs", SAMPLES["asset_specs"]), path)

    rows = pq.read_table(path).to_pylist()
    btc, pepe = rows
    assert btc["price_sig_figs"] == 5
    assert pepe["price_sig_figs"] is None
    assert pepe["price_tick"] == MEMECOIN_PRICE
    assert pepe["size_lot"] == Decimal("0.000000000000000001")
    # 20 integer digits of headroom, so a $10 floor plus one wei survives whole.
    assert pepe["min_notional"] == Decimal("10.000000000000000001")
    assert (btc["is_delisted"], pepe["is_delisted"]) == (False, True)
    # The flag is always set on this stream: `meta` carries no timestamp.
    assert all(r["ts_event_synthetic"] for r in rows)
    # One poll, one instant.
    assert len({r["ts_event_ns"] for r in rows}) == 1
