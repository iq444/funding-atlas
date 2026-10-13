"""DuckDB query layer over the recorder's Parquet tree.

The API is a *reader*. The recorder owns `FA_DATA_DIR` and is the only process
allowed to create a byte in it, so everything here is `SELECT` over
`read_parquet(...)` and the connection is pointed at an in-memory database with
its spill directory forced outside the tree — a query that spills must not be
able to drop a file next to the history it is scanning.

Two behaviours of DuckDB shape this module:

* `read_parquet` on a glob that matches nothing raises `IOException`. An empty
  dataset is the normal state of a recorder that started five minutes ago, so
  every entry point checks for files first and returns an empty result instead
  of letting a 500 escape (`_SQL_EMPTY` is never reached, the guard is).
* Decimal columns come back as `Decimal` padded to scale 18. That is exact;
  `models.decimal_str` handles the cosmetics.

The connection is cached per worker process (see `deps.get_store`) and guarded
by a lock: a DuckDB connection is not safe to use from two threads at once, and
FastAPI runs these sync routes in a threadpool. Serialising queries costs
concurrency the dashboard will never notice and removes a class of crash that
only shows up under load.
"""

from __future__ import annotations

import tempfile
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from funding_atlas.store.parquet import ASSET_PARTITIONED
from funding_atlas.venues.records import STREAMS

__all__ = [
    "AssetSpan",
    "BucketRow",
    "DuckStore",
    "FundingRow",
    "StreamFiles",
]

NS_PER_S = 1_000_000_000


@dataclass(frozen=True, slots=True)
class FundingRow:
    """One funding sample, straight off disk with nothing normalised yet.

    `interval_s` travels with `rate` for the whole journey to the response, so
    that the hourly normalisation happens once, in one place, against the
    interval the venue actually quoted — not against an interval someone
    remembered.
    """

    asset: str
    ts_event_ns: int
    rate: Decimal
    interval_s: int
    mark_price: Decimal | None


@dataclass(frozen=True, slots=True)
class BucketRow:
    """Pre-aggregated funding for one chart bucket.

    Grouped by `interval_s` as well as by bucket so that the sum of `rate` is
    only ever a sum of rates on the same cadence. Mixing cadences inside a SUM
    and dividing afterwards would be wrong by whatever ratio the venue changed
    by, which is exactly the silent rescaling this codebase keeps refusing.
    """

    bucket_ns: int
    interval_s: int
    samples: int
    rate_sum: Decimal
    mark_sum: Decimal | None
    mark_samples: int


@dataclass(frozen=True, slots=True)
class AssetSpan:
    asset: str
    first_ns: int
    last_ns: int
    rows: int


@dataclass(frozen=True, slots=True)
class StreamFiles:
    files: int
    bytes: int


class DuckStore:
    """Read-only, connection-cached access to one venue's Parquet tree."""

    def __init__(self, data_dir: Path | str, venue: str) -> None:
        self._root = Path(data_dir)
        self._venue = venue
        self._lock = threading.Lock()
        self._con: duckdb.DuckDBPyConnection | None = None

    @property
    def root(self) -> Path:
        return self._root

    @property
    def venue(self) -> str:
        return self._venue

    # -- connection ----------------------------------------------------------

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            con = duckdb.connect(":memory:")
            # Spill outside the data directory. The API must never write there,
            # and DuckDB's default temp location follows the database file.
            con.execute("SET temp_directory = ?", [tempfile.gettempdir()])
            self._con = con
        return self._con

    def close(self) -> None:
        with self._lock:
            if self._con is not None:
                self._con.close()
                self._con = None

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        with self._lock:
            return self._connection().execute(sql, list(params)).fetchall()

    # -- file layout ---------------------------------------------------------

    def glob(self, stream: str) -> str:
        """Glob covering every partition of `stream`, matching README's layout."""
        if stream not in STREAMS:
            raise KeyError(f"unknown stream {stream!r}; known: {sorted(STREAMS)}")
        return str(self._root / stream / "**" / "*.parquet")

    def has_files(self, stream: str) -> bool:
        """Whether anything has been written for `stream` yet.

        Checked in Python rather than by catching DuckDB's `IOException`,
        because an empty dataset is an expected state and should not travel as
        an exception. Short-circuits on the first match, so it does not walk a
        tree of a million files to answer "any?".
        """
        directory = self._root / stream
        if not directory.is_dir():
            return False
        return next(directory.rglob("*.parquet"), None) is not None

    def streams_present(self) -> list[str]:
        return [s for s in STREAMS if self.has_files(s)]

    def files_for(self, stream: str) -> StreamFiles:
        count = 0
        total = 0
        directory = self._root / stream
        if directory.is_dir():
            for path in directory.rglob("*.parquet"):
                count += 1
                total += path.stat().st_size
        return StreamFiles(files=count, bytes=total)

    # -- health / stats ------------------------------------------------------

    def last_write_ns(self, stream: str) -> tuple[int | None, int]:
        """`(max ts_ingest_ns, row count)` for a stream.

        Ingest time, not event time: health is a question about *our* pipeline,
        and a venue replaying stale event timestamps after a reconnect must not
        be able to make the recorder look behind — or ahead.
        """
        if not self.has_files(stream):
            return None, 0
        rows = self.query(
            "SELECT max(ts_ingest_ns), count(*) FROM read_parquet(?) WHERE venue = ?",
            [self.glob(stream), self._venue],
        )
        last, count = rows[0]
        return (int(last) if last is not None else None), int(count or 0)

    def span(self, stream: str) -> tuple[int | None, int | None, int]:
        """`(min, max, count)` of `ts_event_ns` — the range research can ask for."""
        if not self.has_files(stream):
            return None, None, 0
        rows = self.query(
            "SELECT min(ts_event_ns), max(ts_event_ns), count(*) "
            "FROM read_parquet(?) WHERE venue = ?",
            [self.glob(stream), self._venue],
        )
        lo, hi, count = rows[0]
        return (
            int(lo) if lo is not None else None,
            int(hi) if hi is not None else None,
            int(count or 0),
        )

    # -- assets --------------------------------------------------------------

    def asset_spans(self, stream: str) -> list[AssetSpan]:
        if not self.has_files(stream):
            return []
        rows = self.query(
            "SELECT asset, min(ts_event_ns), max(ts_event_ns), count(*) "
            "FROM read_parquet(?) WHERE venue = ? GROUP BY asset",
            [self.glob(stream), self._venue],
        )
        return [AssetSpan(str(a), int(lo), int(hi), int(n)) for a, lo, hi, n in rows]

    def delisted_assets(self) -> set[str]:
        """Assets the venue has delisted, per the most recent recorded spec.

        Empty when the `asset_specs` stream has not been recorded yet, which
        means the caller sees everything — the honest failure direction, since
        hiding an asset we are not sure about is worse than showing a dead one.
        """
        return {asset for asset, row in self.latest_asset_specs().items() if bool(row[7])}

    def latest_asset_specs(self) -> dict[str, tuple[Any, ...]]:
        """Most recent recorded spec per asset, from the `asset_specs` stream.

        Returns the venue's own rules rather than anything inferred from the
        tape. Empty when the stream has not been recorded yet — the recorder
        gained it after the first deployments, so historical data directories
        legitimately have none and the caller falls back.
        """
        if not self.has_files("asset_specs"):
            return {}
        rows = self.query(
            """
            SELECT asset, price_tick, price_sig_figs, size_lot, min_notional,
                   max_leverage, is_perp, is_delisted, ts_ingest_ns
            FROM read_parquet(?)
            WHERE venue = ?
            QUALIFY row_number() OVER (PARTITION BY asset ORDER BY ts_ingest_ns DESC) = 1
            """,
            (self.glob("asset_specs"), self.venue),
        )
        return {str(r[0]): r for r in rows}

    def assets_with_l2(self) -> set[str]:
        """Assets that actually have book snapshots on disk.

        `config/recorder.yaml` lists which assets *should* have L2, but the
        config is not what the API serves — a configured asset whose
        subscription never landed has no book, and saying otherwise would send
        a dashboard looking for a depth chart that does not exist.
        """
        if "l2" not in ASSET_PARTITIONED or not self.has_files("l2"):
            return set()
        rows = self.query(
            "SELECT DISTINCT asset FROM read_parquet(?) WHERE venue = ?",
            [self.glob("l2"), self._venue],
        )
        return {str(a) for (a,) in rows}

    def observed_size_scale(self, lookback_ns: int) -> dict[str, int]:
        """Largest number of significant size decimals seen per asset, recently.

        This is how `szDecimals` is recovered: it is venue metadata that the
        recorder does not persist, but a trade size cannot carry more decimals
        than the lot rule allows, so the widest size observed is a *lower bound*
        on the venue's precision and the best available estimate. Bounded to a
        recent window because it is a full scan of the tape and the answer only
        changes when the venue relists an asset.
        """
        if not self.has_files("trades"):
            return {}
        pattern = self.glob("trades")
        rows = self.query(
            "SELECT asset, max(length(rtrim(split_part(CAST(size AS VARCHAR), '.', 2), '0'))) "
            "FROM read_parquet(?) WHERE venue = ? AND ts_event_ns >= ("
            "  SELECT coalesce(max(ts_event_ns), 0) - ? FROM read_parquet(?) WHERE venue = ?"
            ") GROUP BY asset",
            [pattern, self._venue, lookback_ns, pattern, self._venue],
        )
        return {str(a): int(scale or 0) for a, scale in rows}

    # -- funding -------------------------------------------------------------

    def latest_funding(self) -> list[FundingRow]:
        """Most recent funding sample per asset.

        `ROW_NUMBER` rather than a `max(ts)` join: the join form duplicates a
        row whenever two samples share a timestamp, which happens on every
        reconnect (see docs/hyperliquid-api.md §8 on expected duplicates).
        """
        if not self.has_files("funding"):
            return []
        rows = self.query(
            "WITH f AS ("
            "  SELECT asset, ts_event_ns, rate, interval_s, mark_price,"
            "         row_number() OVER (PARTITION BY asset ORDER BY ts_event_ns DESC) AS rn"
            "  FROM read_parquet(?) WHERE venue = ?"
            ") SELECT asset, ts_event_ns, rate, interval_s, mark_price FROM f WHERE rn = 1",
            [self.glob("funding"), self._venue],
        )
        return [
            FundingRow(str(a), int(ts), rate, int(interval), mark)
            for a, ts, rate, interval, mark in rows
        ]

    def latest_open_interest(self) -> dict[str, Decimal]:
        """Most recent open interest per asset, in **coin units** (§8)."""
        if not self.has_files("open_interest"):
            return {}
        rows = self.query(
            "WITH o AS ("
            "  SELECT asset, open_interest,"
            "         row_number() OVER (PARTITION BY asset ORDER BY ts_event_ns DESC) AS rn"
            "  FROM read_parquet(?) WHERE venue = ?"
            ") SELECT asset, open_interest FROM o WHERE rn = 1",
            [self.glob("open_interest"), self._venue],
        )
        return {str(a): oi for a, oi in rows}

    def funding_buckets(
        self, asset: str, from_ns: int, to_ns: int, bucket_ns: int
    ) -> list[BucketRow]:
        """Funding aggregated into fixed buckets, half-open `[from_ns, to_ns)`.

        Bucket edges come from integer modulo on the raw nanoseconds, so they
        land on real UTC hour and day boundaries and two calls with different
        windows agree on where a bucket starts.
        """
        if not self.has_files("funding"):
            return []
        rows = self.query(
            "SELECT ts_event_ns - (ts_event_ns % ?) AS bucket_ns, interval_s, count(*),"
            "       sum(rate), sum(mark_price), count(mark_price) "
            "FROM read_parquet(?) "
            "WHERE venue = ? AND asset = ? AND ts_event_ns >= ? AND ts_event_ns < ? "
            "GROUP BY 1, 2 ORDER BY 1, 2",
            [bucket_ns, self.glob("funding"), self._venue, asset, from_ns, to_ns],
        )
        return [
            BucketRow(int(b), int(interval), int(n), rate_sum, mark_sum, int(n_mark or 0))
            for b, interval, n, rate_sum, mark_sum, n_mark in rows
        ]

    def funding_intervals(self) -> list[int]:
        """Distinct `interval_s` on disk — the proof the API did not hardcode it."""
        if not self.has_files("funding"):
            return []
        rows = self.query(
            "SELECT DISTINCT interval_s FROM read_parquet(?) WHERE venue = ? ORDER BY 1",
            [self.glob("funding"), self._venue],
        )
        return [int(i) for (i,) in rows]

    # -- cadence / gaps ------------------------------------------------------

    def sample_bounds(
        self, stream: str, asset: str | None, from_ns: int, to_ns: int
    ) -> tuple[int | None, int | None, int]:
        """`(first, last, count)` of `ts_event_ns` inside a window."""
        if not self.has_files(stream):
            return None, None, 0
        clause, params = self._asset_clause(stream, asset)
        rows = self.query(
            "SELECT min(ts_event_ns), max(ts_event_ns), count(*) FROM read_parquet(?) "
            f"WHERE venue = ? {clause} AND ts_event_ns >= ? AND ts_event_ns <= ?",
            [self.glob(stream), self._venue, *params, from_ns, to_ns],
        )
        lo, hi, count = rows[0]
        return (
            int(lo) if lo is not None else None,
            int(hi) if hi is not None else None,
            int(count or 0),
        )

    def sample_gaps(
        self, stream: str, asset: str | None, from_ns: int, to_ns: int, threshold_ns: int
    ) -> list[tuple[int, int]]:
        """Consecutive samples more than `threshold_ns` apart.

        Computed in SQL with `LAG` rather than by pulling timestamps into
        Python: on `l2` at 500ms a one-week window is a million rows, and the
        answer is a handful of intervals.

        `DISTINCT` first, because duplicates across a reconnect are expected and
        a duplicated timestamp would otherwise produce a zero-length interval
        that hides nothing but costs a row.
        """
        if not self.has_files(stream):
            return []
        clause, params = self._asset_clause(stream, asset)
        rows = self.query(
            "WITH t AS ("
            "  SELECT DISTINCT ts_event_ns AS ts FROM read_parquet(?) "
            f"  WHERE venue = ? {clause} AND ts_event_ns >= ? AND ts_event_ns <= ?"
            "), s AS (SELECT ts, lag(ts) OVER (ORDER BY ts) AS prev FROM t) "
            "SELECT prev, ts FROM s WHERE prev IS NOT NULL AND ts - prev > ? ORDER BY prev",
            [self.glob(stream), self._venue, *params, from_ns, to_ns, threshold_ns],
        )
        return [(int(prev), int(ts)) for prev, ts in rows]

    def _asset_clause(self, stream: str, asset: str | None) -> tuple[str, list[Any]]:
        if asset is None:
            return "", []
        return "AND asset = ?", [asset]
