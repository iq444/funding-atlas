/**
 * The funding-atlas data API contract, as consumed by this dashboard.
 *
 * Monetary and rate fields are typed `Numeric` (= `string | number`) rather
 * than `string`: the contract says strings, and strings are what the parsing
 * layer prefers, but typing them as `string | number` means a field that
 * arrives as a JSON number renders a slightly-less-precise value instead of
 * crashing the page. Timestamps are `NsTimestamp` for the same reason — see
 * `src/lib/time.ts` for why nanoseconds-as-JSON-number is lossy.
 */

import type { Numeric } from "@/lib/num";
import type { NsTimestamp } from "@/lib/time";

export type StreamName = string;

/* ---------------------------------------------------------- GET /health --- */

export interface StreamHealth {
  last_write_ns: NsTimestamp | null;
  lag_s: Numeric | null;
  rows: Numeric;
}

export interface HealthResponse {
  status: string;
  streams: Record<StreamName, StreamHealth>;
  disk_free_bytes: Numeric;
  uptime_s: Numeric;
}

/* ---------------------------------------------------------- GET /assets --- */

export interface AssetMeta {
  asset: string;
  /** Smallest price increment. Hyperliquid has no tick field; see `price_sig_figs`. */
  price_tick: Numeric | null;
  /** At most this many significant figures in a price (5 on Hyperliquid perps). */
  price_sig_figs: Numeric | null;
  size_lot: Numeric | null;
  min_notional: Numeric | null;
  max_leverage: Numeric | null;
  first_seen_ns: NsTimestamp | null;
  last_seen_ns: NsTimestamp | null;
  has_l2: boolean;
}

export interface AssetsResponse {
  assets: AssetMeta[];
  /** Latest observation across the universe, where the API reports it. */
  as_of_ns?: NsTimestamp | null;
}

/* ------------------------------------------------- GET /funding/current --- */

export interface FundingCurrentRow {
  asset: string;
  /** Per-hour funding rate as a fraction. Positive = longs pay shorts. */
  rate_hourly: Numeric;
  /** Annualised: rate_hourly x 24 x 365. */
  apr: Numeric;
  /** Null where the recorder has a funding row but no mark for the bucket. */
  mark: Numeric | null;
  /** Open interest in COIN units. */
  open_interest: Numeric | null;
  /** Open interest in USD. */
  oi_notional: Numeric | null;
}

export interface FundingCurrentResponse {
  as_of_ns: NsTimestamp | null;
  rows: FundingCurrentRow[];
}

/* ------------------------------------------------- GET /funding/history --- */

export type HistoryBucket = "1h" | "1d";

export interface FundingHistoryPoint {
  ts_ns: NsTimestamp;
  rate_hourly: Numeric;
  apr: Numeric;
  mark: Numeric | null;
}

export interface FundingHistoryResponse {
  asset: string;
  bucket: HistoryBucket;
  points: FundingHistoryPoint[];
  /**
   * Coverage of the requested window, when the API embeds it. Preferred over a
   * separate /coverage call, since it is guaranteed to describe exactly the
   * window these points came from.
   */
  coverage?: CoverageResponse;
}

/* ------------------------------------------------ GET /funding/screener --- */

export type Side = "short" | "long";

/**
 * Free-form by contract (`assumptions:{...}`). The dashboard renders whatever
 * keys it is given, and labels the ones it recognises.
 */
export type AssumptionValue = Numeric | boolean | null | Numeric[];
export type ScreenerAssumptions = Record<string, AssumptionValue>;

export interface ScreenerRow {
  asset: string;
  apr: Numeric;
  gross_pnl: Numeric;
  fee_cost: Numeric;
  net_pnl: Numeric;
  net_apr: Numeric;
  /** Hours of funding needed to pay off round-trip fees. */
  breakeven_hours: Numeric | null;
  oi_notional: Numeric | null;
  covers_fees: boolean;
}

export interface ScreenerResponse {
  assumptions: ScreenerAssumptions;
  rows: ScreenerRow[];
  /** Coverage of the window the rates were drawn from, when the API sends it. */
  coverage?: CoverageResponse;
}

export interface ScreenerQuery {
  notional: number;
  holding_hours: number;
  side: Side;
}

/* -------------------------------------------------------- GET /coverage --- */

export interface CoverageGap {
  from_ns: NsTimestamp;
  to_ns: NsTimestamp;
  reason: string;
  duration_s: Numeric;
}

export interface CoverageResponse {
  /**
   * Whether the window is fully covered.
   *
   * The API emits a boolean; a fraction in 0..1 is also accepted, because the
   * published contract did not say which and both are reasonable readings of
   * the field name. `coverageIsComplete` in `@/components/gap-notice` is the
   * only place that interprets it.
   */
  covered: boolean | Numeric;
  /** Seconds of the requested window with no data. */
  missing_s: Numeric;
  gaps: CoverageGap[];
}

export interface CoverageQuery {
  stream: string;
  asset?: string;
  from_ns: string;
  to_ns: string;
}

/* ----------------------------------------------------------- GET /stats --- */

export interface StreamStats {
  rows: Numeric;
  first_ns: NsTimestamp | null;
  last_ns: NsTimestamp | null;
  files: Numeric;
  bytes: Numeric;
}

export interface StatsResponse {
  streams: Record<StreamName, StreamStats>;
  venue: string;
  network: string;
}
