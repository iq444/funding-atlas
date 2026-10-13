"""Parquet schemas, partition paths and atomic file writes.

This is the data plane (see docs/architecture.md). Records arrive already
normalised by the venue adapters, so nothing here validates them a second time;
the only job is to turn batches of records into columnar files that DuckDB can
scan years from now without a decoder ring.

Three choices are load-bearing and are argued at their definitions below:
decimal encoding (`DECIMAL_PRECISION`), book snapshot layout (`_L2_SCHEMA`) and
the partition key (`ASSET_PARTITIONED`).
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from funding_atlas.venues.records import STREAMS

#: Bumped whenever a stream's Arrow schema changes shape or type. Written into
#: every file's key-value metadata, because the alternative is a research query
#: silently unioning two incompatible generations of a column and reporting a
#: number nobody can reproduce. A reader that finds an unexpected version can
#: refuse the partition instead of guessing.
#:
#: Adding a *new* stream does not bump it: no existing file's columns change, so
#: every file already on disk still reads exactly as it was written. Bumping for
#: a new stream would instead make every historical partition look stale and
#: force a reader to special-case a version difference that means nothing.
SCHEMA_VERSION = 1

SCHEMA_VERSION_KEY = b"funding_atlas.schema_version"
STREAM_KEY = b"funding_atlas.stream"

# Prices and sizes: fixed-point decimal, never float.
#
# Scale 18 because crypto spans ~25 orders of magnitude in a single table:
# a memecoin quoted at 0.00000000012 and BTC at 112_345.5 are both "price".
# Float64 has 15-17 significant digits total, so it cannot hold a sub-satoshi
# tick and a six-figure notional without rounding one of them — and rounding a
# price is how a two-leg hedge quietly becomes directional.
#
# 18 fractional digits also matches the EVM token convention (wei), so on-chain
# quantities round-trip exactly rather than approximately. Precision 38 is the
# maximum for Arrow's 128-bit decimal and leaves 20 integer digits, which is
# more than any notional, size or open-interest figure in token units can reach.
# Both are fixed at 16 bytes per value; Parquet's byte-stream splitting and
# compression make the unused headroom nearly free on disk.
#
# A value with more than 18 fractional digits raises at conversion time rather
# than being truncated. That is deliberate: a loud failure on a venue that
# changes its tick format is recoverable, a silently rounded history is not.
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 18

_DEC = pa.decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)

# Timestamps stay as raw int64 nanoseconds, the same integers the venue sent and
# our clock stamped. Arrow's timestamp('ns') would overflow in 2262 and, worse,
# invites readers to assume a timezone; the difference between the two columns is
# measured latency and must survive untouched. DuckDB turns them into instants on
# demand (`make_timestamp_ns`).
_TS_FIELDS = [
    pa.field("ts_event_ns", pa.int64(), nullable=False),
    pa.field("ts_ingest_ns", pa.int64(), nullable=False),
    # Some venue streams send no timestamp (Hyperliquid's allMids and
    # activeAssetCtx), so ts_event_ns is filled from our clock. Without this
    # flag a latency study would quietly measure our own clock against itself
    # and report zero.
    pa.field("ts_event_synthetic", pa.bool_(), nullable=False),
]

_KEY_FIELDS = [
    # Kept as columns even though they are also partition keys: a file that is
    # copied out of its directory tree must still be self-describing.
    pa.field("venue", pa.string(), nullable=False),
    pa.field("asset", pa.string(), nullable=False),
]

_TRADES_SCHEMA = pa.schema(
    [
        *_KEY_FIELDS,
        *_TS_FIELDS,
        pa.field("price", _DEC, nullable=False),
        pa.field("size", _DEC, nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("trade_id", pa.string(), nullable=False),
    ]
)

_MIDS_SCHEMA = pa.schema(
    [
        *_KEY_FIELDS,
        *_TS_FIELDS,
        pa.field("mid", _DEC, nullable=False),
    ]
)

_LEVEL_TYPE = pa.struct(
    [
        pa.field("price", _DEC, nullable=False),
        pa.field("size", _DEC, nullable=False),
        pa.field("n_orders", pa.int32(), nullable=True),
    ]
)

# Book layout: one row per snapshot, each side a list<struct<price, size,
# n_orders>>, rather than one row per level.
#
# Why lists win here, given 20 levels per side at 500ms:
#
#   * Atomicity. A snapshot is the unit of truth (see BookSnapshot's docstring).
#     As one row it cannot be half-read, half-filtered or re-assembled wrongly;
#     flattened, every query that wants a book has to GROUP BY (ts, side) and
#     trust that all 40 rows landed in the same file.
#   * Compactness. Flattening repeats venue/asset/ts_event_ns/ts_ingest_ns 40x
#     per snapshot — the metadata outweighs the payload it describes. Run-length
#     and dictionary encoding soften that but do not remove the row count, and
#     row count is what drives scan cost on the volume-dominant stream.
#   * Queries stay natural. The questions actually asked of a book are
#     per-snapshot: spread (`asks[1].price - bids[1].price`), depth within k bps,
#     imbalance (`list_sum(list_transform(bids, b -> b.size))`). All are single-row
#     expressions in DuckDB, and `UNNEST(bids)` recovers the flat shape for the
#     rarer per-level scan.
#
# The cost, stated plainly: a query that only wants best bid still decodes the
# whole list column, and tools with weak nested-type support (naive pandas
# pipelines, some BI front-ends) need an explicit unnest first. That is paid by
# research code, which can be fixed; the flattened alternative's costs are paid
# in disk and in snapshot integrity, which cannot.
_L2_SCHEMA = pa.schema(
    [
        *_KEY_FIELDS,
        *_TS_FIELDS,
        pa.field("bids", pa.list_(pa.field("level", _LEVEL_TYPE, nullable=False))),
        pa.field("asks", pa.list_(pa.field("level", _LEVEL_TYPE, nullable=False))),
    ]
)

_FUNDING_SCHEMA = pa.schema(
    [
        *_KEY_FIELDS,
        *_TS_FIELDS,
        pa.field("rate", _DEC, nullable=False),
        # The venue's own interval, stored per sample: annualising with today's
        # cadence would rescale history the moment a venue changes it.
        pa.field("interval_s", pa.int32(), nullable=False),
        pa.field("premium", _DEC, nullable=True),
        pa.field("oracle_price", _DEC, nullable=True),
        pa.field("mark_price", _DEC, nullable=True),
    ]
)

_OPEN_INTEREST_SCHEMA = pa.schema(
    [
        *_KEY_FIELDS,
        *_TS_FIELDS,
        pa.field("open_interest", _DEC, nullable=False),
    ]
)

# Asset specs: the venue's trading rules, resampled on a cadence. One row per
# (asset, poll), every poll, including the polls where nothing changed — see the
# note on ASSET_PARTITIONED for why this stream is not partitioned per asset,
# and `records.AssetSpecSample` for why it is recorded at all.
_ASSET_SPECS_SCHEMA = pa.schema(
    [
        *_KEY_FIELDS,
        *_TS_FIELDS,
        # Decimal, not float, for the same reason as every other money-shaped
        # column: a lot step read back as 0.0009999999 rounds a size down to the
        # previous lot and the venue rejects the order — or worse, accepts a
        # smaller one and leaves a hedge short.
        pa.field("price_tick", _DEC, nullable=False),
        # Nullable, and the only nullable field here: a venue with no
        # significant-figure rule is a real state, distinct from one we failed
        # to read. int32 because the value is a digit count, not a quantity.
        pa.field("price_sig_figs", pa.int32(), nullable=True),
        pa.field("size_lot", _DEC, nullable=False),
        pa.field("min_notional", _DEC, nullable=False),
        # Leverage is a ratio rather than a price, but it multiplies notionals
        # in every margin calculation, so it stays out of float too.
        pa.field("max_leverage", _DEC, nullable=False),
        pa.field("is_perp", pa.bool_(), nullable=False),
        pa.field("is_delisted", pa.bool_(), nullable=False),
    ]
)


_SCHEMAS: dict[str, pa.Schema] = {
    "trades": _TRADES_SCHEMA,
    "mids": _MIDS_SCHEMA,
    "l2": _L2_SCHEMA,
    "funding": _FUNDING_SCHEMA,
    "open_interest": _OPEN_INTEREST_SCHEMA,
    "asset_specs": _ASSET_SPECS_SCHEMA,
}

# Streams partitioned down to the asset, matching README's "Storage layout".
#
# trades/mids/l2 are per-asset by volume and are nearly always queried one asset
# at a time, so the directory is the cheapest possible filter. funding and
# open_interest are a handful of rows per asset per minute: giving each of ~200
# perps its own directory would produce thousands of kilobyte files, and the
# cross-sectional question ("funding across the universe right now") would have
# to open all of them. They keep `asset` as a column and partition by date only,
# so one file answers the dispersion query.
#
# asset_specs is the strongest case of all for date-only: the question it exists
# to answer is explicitly cross-sectional — "what were the rules for every asset
# at time T" — because an order router validating a basket needs the whole
# universe's ticks and lots at one instant, not one asset's history. It is also
# tiny and uniform (~200 rows every 15 minutes, every asset in every poll), so
# per-asset directories would turn 22k daily rows into 200 files of a hundred
# rows each. `asset` stays a column and one file answers the whole question.
ASSET_PARTITIONED: frozenset[str] = frozenset({"trades", "mids", "l2"})

assert set(_SCHEMAS) == set(STREAMS), "every stream in records.STREAMS needs a schema"


def schema_for(stream: str) -> pa.Schema:
    try:
        return _SCHEMAS[stream]
    except KeyError:
        raise KeyError(f"unknown stream {stream!r}; known: {sorted(_SCHEMAS)}") from None


def date_for_ns(ts_ns: int) -> date:
    """Partition date of a timestamp. UTC, always.

    Local time would make partition boundaries move with DST and make two hosts
    disagree about which file a record belongs in. Integer division rather than
    `ts_ns / 1e9`, because float seconds lose nanosecond resolution past 2^53
    and would misfile records within a microsecond of midnight.
    """
    return datetime.fromtimestamp(ts_ns // 1_000_000_000, tz=UTC).date()


def partition_dir(root: Path | str, stream: str, venue: str, asset: str | None, day: date) -> Path:
    """`<root>/<stream>/venue=<v>[/asset=<a>]/date=<YYYY-MM-DD>`."""
    if stream not in _SCHEMAS:
        raise KeyError(f"unknown stream {stream!r}")
    path = Path(root) / stream / f"venue={venue}"
    if stream in ASSET_PARTITIONED:
        if asset is None:
            raise ValueError(f"stream {stream!r} is partitioned by asset; asset is required")
        path = path / f"asset={asset}"
    return path / f"date={day.isoformat()}"


def file_name(ts_ns: int, seq: int) -> str:
    """`<ts>-<seq>.parquet`.

    The timestamp sorts files in write order for a reader that lists a
    directory; `seq` keeps names unique when a rotation fires twice inside the
    same nanosecond tick, and after a restart a fresh process cannot collide
    because its timestamps are strictly later.
    """
    return f"{ts_ns}-{seq:06d}.parquet"


def partition_path(
    root: Path | str,
    stream: str,
    venue: str,
    asset: str | None,
    day: date,
    ts_ns: int,
    seq: int,
) -> Path:
    return partition_dir(root, stream, venue, asset, day) / file_name(ts_ns, seq)


def _levels(levels: Sequence[Any]) -> list[dict[str, Any]]:
    return [{"price": lv.price, "size": lv.size, "n_orders": lv.n_orders} for lv in levels]


def _columns(stream: str, records: Sequence[Any]) -> dict[str, list[Any]]:
    # Built column-at-a-time: Arrow consumes columns, and one pass per field over
    # a 500k-record buffer beats materialising half a million intermediate dicts.
    cols: dict[str, list[Any]] = {
        "venue": [r.venue for r in records],
        "asset": [r.asset for r in records],
        "ts_event_ns": [r.ts_event_ns for r in records],
        "ts_ingest_ns": [r.ts_ingest_ns for r in records],
        "ts_event_synthetic": [r.ts_event_synthetic for r in records],
    }
    if stream == "trades":
        cols["price"] = [r.price for r in records]
        cols["size"] = [r.size for r in records]
        cols["side"] = [r.side for r in records]
        cols["trade_id"] = [r.trade_id for r in records]
    elif stream == "mids":
        cols["mid"] = [r.mid for r in records]
    elif stream == "l2":
        cols["bids"] = [_levels(r.bids) for r in records]
        cols["asks"] = [_levels(r.asks) for r in records]
    elif stream == "funding":
        cols["rate"] = [r.rate for r in records]
        cols["interval_s"] = [r.interval_s for r in records]
        cols["premium"] = [r.premium for r in records]
        cols["oracle_price"] = [r.oracle_price for r in records]
        cols["mark_price"] = [r.mark_price for r in records]
    elif stream == "open_interest":
        cols["open_interest"] = [r.open_interest for r in records]
    elif stream == "asset_specs":
        cols["price_tick"] = [r.price_tick for r in records]
        cols["price_sig_figs"] = [r.price_sig_figs for r in records]
        cols["size_lot"] = [r.size_lot for r in records]
        cols["min_notional"] = [r.min_notional for r in records]
        cols["max_leverage"] = [r.max_leverage for r in records]
        cols["is_perp"] = [r.is_perp for r in records]
        cols["is_delisted"] = [r.is_delisted for r in records]
    else:  # pragma: no cover - guarded by schema_for
        raise KeyError(f"unknown stream {stream!r}")
    return cols


def stream_of(record: Any) -> str:
    """Stream a record belongs to, by type."""
    for name, rtype in STREAMS.items():
        if type(record) is rtype:
            return name
    raise TypeError(f"{type(record).__name__} is not a recorded stream type")


def records_to_table(stream: str, records: Sequence[Any]) -> pa.Table:
    schema = schema_for(stream)
    cols = _columns(stream, records)
    table = pa.Table.from_pydict(cols, schema=schema)
    return table.replace_schema_metadata(
        {
            SCHEMA_VERSION_KEY: str(SCHEMA_VERSION).encode(),
            STREAM_KEY: stream.encode(),
        }
    )


TMP_SUFFIX = ".tmp"


def write_table(table: pa.Table, path: Path) -> None:
    """Write `table` to `path` atomically.

    Research globs `*.parquet`, so a half-written file under that name is worse
    than a missing one: it reads as data. The payload goes to `<name>.tmp`, is
    fsynced, and only then renamed — `os.replace` is atomic within a filesystem,
    so a kill at any instant leaves either nothing or a complete file. The
    orphaned `.tmp` a crash leaves behind is outside the glob and is safe to
    delete.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + TMP_SUFFIX)
    try:
        with pq.ParquetWriter(tmp, table.schema, compression="zstd") as writer:
            writer.write_table(table)
        # Rename only orders the directory entry; without fsync the bytes may
        # still be in page cache when the host loses power.
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def read_schema_version(path: Path) -> int | None:
    """Schema generation a file was written with, or None if it predates the key."""
    meta = pq.read_schema(path).metadata or {}
    raw = meta.get(SCHEMA_VERSION_KEY)
    return int(raw) if raw is not None else None


__all__ = [
    "ASSET_PARTITIONED",
    "DECIMAL_PRECISION",
    "DECIMAL_SCALE",
    "SCHEMA_VERSION",
    "SCHEMA_VERSION_KEY",
    "STREAM_KEY",
    "TMP_SUFFIX",
    "date_for_ns",
    "file_name",
    "partition_dir",
    "partition_path",
    "read_schema_version",
    "records_to_table",
    "schema_for",
    "stream_of",
    "write_table",
]
