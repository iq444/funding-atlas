/**
 * Mock fixture layer.
 *
 * Purpose: let the dashboard be built, reviewed and tested before the data API
 * exists, and let a reviewer clone the repo and see every state — including
 * the awkward ones (a stale stream, a recorder gap mid-chart, carries that do
 * not cover fees) — without a running recorder.
 *
 * Properties this layer deliberately has:
 *   - Deterministic. Values derive from a seeded PRNG and from the current
 *     hour, not from Math.random(), so two renders agree and a test can assert.
 *   - Shaped like the wire. Monetary values are emitted as STRINGS with
 *     plausible per-asset precision, so the string-parsing path is the one
 *     under test rather than a convenient number path.
 *   - Honest. Nothing here resembles a real quote closely enough to be
 *     mistaken for one, and the UI stamps every page while mock mode is on.
 */

import type {
  AssetMeta,
  AssetsResponse,
  CoverageGap,
  CoverageQuery,
  CoverageResponse,
  FundingCurrentResponse,
  FundingCurrentRow,
  FundingHistoryResponse,
  HealthResponse,
  ScreenerQuery,
  ScreenerResponse,
  ScreenerRow,
  StatsResponse,
} from "./types";
import type { FundingAtlasApi, FundingHistoryQuery } from "./client";
import { msToNs, nsToMs, HOUR_MS, DAY_MS } from "@/lib/time";

/* ------------------------------------------------------------- plumbing --- */

function hashSeed(text: string): number {
  let h = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

/** mulberry32 — small, fast, deterministic. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Current hour, so a page re-render does not reshuffle the numbers. */
function currentHour(): number {
  return Math.floor(Date.now() / HOUR_MS);
}

async function settle<T>(value: T, signal?: AbortSignal): Promise<T> {
  // A short delay so loading and skeleton states are actually exercised.
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, 120);
    signal?.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(new DOMException("aborted", "AbortError"));
    });
  });
  return value;
}

/* --------------------------------------------------------------- universe -- */

interface MockAsset {
  asset: string;
  /** Reference level only — a plausible magnitude, not a quote. */
  level: number;
  pxDecimals: number;
  szDecimals: number;
  maxLeverage: number;
  hasL2: boolean;
  /** Hourly funding bias as a fraction; the interest floor is 0.0000125. */
  bias: number;
  oiCoins: number;
  firstSeenDaysAgo: number;
}

const UNIVERSE: MockAsset[] = [
  { asset: "BTC", level: 68000, pxDecimals: 1, szDecimals: 5, maxLeverage: 40, hasL2: true, bias: 0.0000180, oiCoins: 4120, firstSeenDaysAgo: 96 },
  { asset: "ETH", level: 3200, pxDecimals: 2, szDecimals: 4, maxLeverage: 25, hasL2: true, bias: 0.0000165, oiCoins: 58400, firstSeenDaysAgo: 96 },
  { asset: "SOL", level: 148, pxDecimals: 3, szDecimals: 2, maxLeverage: 20, hasL2: true, bias: 0.0000240, oiCoins: 612000, firstSeenDaysAgo: 96 },
  { asset: "HYPE", level: 24, pxDecimals: 4, szDecimals: 2, maxLeverage: 10, hasL2: true, bias: 0.0000410, oiCoins: 1840000, firstSeenDaysAgo: 71 },
  { asset: "XRP", level: 0.58, pxDecimals: 5, szDecimals: 0, maxLeverage: 20, hasL2: false, bias: 0.0000125, oiCoins: 38400000, firstSeenDaysAgo: 96 },
  { asset: "DOGE", level: 0.124, pxDecimals: 6, szDecimals: 0, maxLeverage: 10, hasL2: false, bias: 0.0000205, oiCoins: 214000000, firstSeenDaysAgo: 96 },
  { asset: "SUI", level: 1.84, pxDecimals: 4, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: 0.0000330, oiCoins: 9120000, firstSeenDaysAgo: 88 },
  { asset: "AVAX", level: 27.4, pxDecimals: 3, szDecimals: 2, maxLeverage: 10, hasL2: false, bias: -0.0000090, oiCoins: 412000, firstSeenDaysAgo: 96 },
  { asset: "LINK", level: 13.2, pxDecimals: 4, szDecimals: 2, maxLeverage: 10, hasL2: false, bias: 0.0000140, oiCoins: 884000, firstSeenDaysAgo: 96 },
  { asset: "ARB", level: 0.41, pxDecimals: 5, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: -0.0000220, oiCoins: 21400000, firstSeenDaysAgo: 96 },
  { asset: "OP", level: 1.12, pxDecimals: 4, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: 0.0000115, oiCoins: 6240000, firstSeenDaysAgo: 96 },
  { asset: "TIA", level: 2.08, pxDecimals: 4, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: 0.0000610, oiCoins: 3180000, firstSeenDaysAgo: 82 },
  { asset: "INJ", level: 11.8, pxDecimals: 4, szDecimals: 2, maxLeverage: 10, hasL2: false, bias: -0.0000480, oiCoins: 288000, firstSeenDaysAgo: 96 },
  { asset: "APT", level: 5.4, pxDecimals: 4, szDecimals: 2, maxLeverage: 10, hasL2: false, bias: 0.0000095, oiCoins: 1420000, firstSeenDaysAgo: 96 },
  { asset: "SEI", level: 0.26, pxDecimals: 5, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: 0.0000720, oiCoins: 28400000, firstSeenDaysAgo: 74 },
  { asset: "JUP", level: 0.72, pxDecimals: 5, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: 0.0000260, oiCoins: 7120000, firstSeenDaysAgo: 68 },
  { asset: "kPEPE", level: 0.0082, pxDecimals: 7, szDecimals: 0, maxLeverage: 10, hasL2: false, bias: 0.0000840, oiCoins: 1240000000, firstSeenDaysAgo: 96 },
  { asset: "WIF", level: 1.42, pxDecimals: 4, szDecimals: 1, maxLeverage: 10, hasL2: false, bias: 0.0000530, oiCoins: 4280000, firstSeenDaysAgo: 91 },
  { asset: "LTC", level: 68.2, pxDecimals: 3, szDecimals: 2, maxLeverage: 10, hasL2: false, bias: -0.0000035, oiCoins: 94000, firstSeenDaysAgo: 96 },
  { asset: "BNB", level: 560, pxDecimals: 2, szDecimals: 3, maxLeverage: 10, hasL2: false, bias: 0.0000105, oiCoins: 18400, firstSeenDaysAgo: 96 },
  { asset: "TAO", level: 412, pxDecimals: 2, szDecimals: 3, maxLeverage: 10, hasL2: false, bias: 0.0000385, oiCoins: 12800, firstSeenDaysAgo: 79 },
  { asset: "ENA", level: 0.34, pxDecimals: 5, szDecimals: 0, maxLeverage: 10, hasL2: false, bias: -0.0000610, oiCoins: 32400000, firstSeenDaysAgo: 64 },
];

const BOOT_MS = Date.now() - 31 * HOUR_MS - 14 * 60_000;

/** Hyperliquid perps: at most 6 - szDecimals decimal places, and 5 sig figs. */
const PERP_MAX_DECIMALS = 6;

function priceTick(a: MockAsset): string {
  const decimals = Math.min(a.pxDecimals, PERP_MAX_DECIMALS - a.szDecimals);
  return decimals <= 0 ? "1" : `0.${"0".repeat(decimals - 1)}1`;
}

function sizeLot(a: MockAsset): string {
  return a.szDecimals === 0 ? "1" : `0.${"0".repeat(a.szDecimals - 1)}1`;
}

/** A mark with exactly the precision the venue's tick rules would allow. */
function markFor(a: MockAsset, hour: number): string {
  const rand = prng(hashSeed(`${a.asset}:mark:${hour}`));
  const drift = 1 + (rand() - 0.5) * 0.06;
  const decimals = Math.max(0, Math.min(a.pxDecimals, PERP_MAX_DECIMALS - a.szDecimals));
  return (a.level * drift).toFixed(decimals);
}

function rateFor(a: MockAsset, hour: number): number {
  const rand = prng(hashSeed(`${a.asset}:rate:${hour}`));
  const noise = (rand() - 0.45) * 0.00006;
  // Hyperliquid clamps interest - premium to +/-0.05% and caps total at 4%/h.
  return Math.max(-0.0005, Math.min(0.0005, a.bias + noise));
}

/** Rates are quoted to 8 decimal places on the wire, like fundingHistory. */
function rateString(rate: number): string {
  return rate.toFixed(9).replace(/0+$/, "").replace(/\.$/, ".0");
}

function aprString(rate: number): string {
  return (rate * 24 * 365).toFixed(6);
}

/* ------------------------------------------------------------------ gaps --- */

interface MockGap {
  stream: string;
  /** Restricted to one asset, or undefined for a whole-stream outage. */
  asset?: string;
  startMs: number;
  durationS: number;
  reason: string;
}

/**
 * Fixed, deliberate holes. These exist so the gap-reporting paths are visible
 * in development rather than discovered in production.
 */
const MOCK_GAPS: MockGap[] = [
  {
    stream: "funding",
    startMs: BOOT_MS - 2 * DAY_MS,
    durationS: 1_847,
    reason: "ws_disconnect: no message for 90s, resubscribe window",
  },
  {
    stream: "funding",
    startMs: BOOT_MS - 9 * DAY_MS - 5 * HOUR_MS,
    durationS: 10_412,
    reason: "process_restart: recorder redeployed",
  },
  {
    stream: "funding",
    asset: "HYPE",
    startMs: BOOT_MS - 4 * DAY_MS,
    durationS: 4_260,
    reason: "listing_churn: asset absent from meta.universe",
  },
  {
    stream: "l2",
    asset: "BTC",
    startMs: BOOT_MS - 31 * HOUR_MS,
    durationS: 3_140,
    reason: "disk_pressure: writer paused below free-space floor",
  },
  {
    stream: "trades",
    startMs: BOOT_MS - 16 * HOUR_MS,
    durationS: 612,
    reason: "rate_limit: REST weight exhausted, backoff",
  },
];

function gapsFor(stream: string, asset: string | undefined, fromMs: number, toMs: number) {
  return MOCK_GAPS.filter((g) => {
    if (g.stream !== stream) return false;
    // A gap affecting one asset still belongs in the stream-wide answer: a
    // whole-stream coverage figure that silently omits per-asset outages is
    // the kind of flattering number this project exists to avoid.
    if (g.asset && asset && g.asset !== asset) return false;
    const endMs = g.startMs + g.durationS * 1000;
    return endMs > fromMs && g.startMs < toMs;
  }).map((g) => (g.asset && !asset ? { ...g, reason: `${g.asset}: ${g.reason}` } : g));
}

/* ------------------------------------------------------------- endpoints --- */

const STREAMS = ["funding", "trades", "mids", "l2", "open_interest"] as const;

function health(): HealthResponse {
  const now = Date.now();
  const streams: HealthResponse["streams"] = {};
  const lagByStream: Record<string, number> = {
    funding: 12,
    trades: 0.4,
    mids: 1.1,
    // Deliberately stale, so the degraded path on /status is visible.
    l2: 214,
    open_interest: 38,
  };
  const rowsByStream: Record<string, number> = {
    funding: 3_184_920,
    trades: 418_204_118,
    mids: 212_480_664,
    l2: 1_842_118_904,
    open_interest: 3_184_920,
  };
  for (const stream of STREAMS) {
    const lag = lagByStream[stream] ?? 1;
    streams[stream] = {
      last_write_ns: msToNs(now - lag * 1000),
      lag_s: lag.toFixed(2),
      rows: rowsByStream[stream] ?? 0,
    };
  }
  return {
    status: "degraded",
    streams,
    disk_free_bytes: "98236764160",
    uptime_s: Math.round((now - BOOT_MS) / 1000),
  };
}

function assets(): AssetsResponse {
  const now = Date.now();
  const list: AssetMeta[] = UNIVERSE.map((a) => ({
    asset: a.asset,
    price_tick: priceTick(a),
    price_sig_figs: 5,
    size_lot: sizeLot(a),
    min_notional: "10",
    max_leverage: a.maxLeverage,
    first_seen_ns: msToNs(now - a.firstSeenDaysAgo * DAY_MS),
    last_seen_ns: msToNs(now - 12_000),
    has_l2: a.hasL2,
  }));
  return { assets: list, as_of_ns: msToNs(now - 12_000) };
}

function fundingCurrent(): FundingCurrentResponse {
  const hour = currentHour();
  const rows: FundingCurrentRow[] = UNIVERSE.map((a) => {
    const rate = rateFor(a, hour);
    const mark = markFor(a, hour);
    const oiCoins = a.oiCoins * (0.9 + prng(hashSeed(`${a.asset}:oi:${hour}`))() * 0.2);
    const oiNotional = oiCoins * Number(mark);
    return {
      asset: a.asset,
      rate_hourly: rateString(rate),
      apr: aprString(rate),
      mark,
      open_interest: oiCoins.toFixed(a.szDecimals),
      oi_notional: oiNotional.toFixed(2),
    };
  });
  return { as_of_ns: msToNs(Date.now() - 12_000), rows };
}

function fundingHistory(q: FundingHistoryQuery): FundingHistoryResponse {
  const a = UNIVERSE.find((u) => u.asset === q.asset);
  const fromMs = nsToMs(q.from_ns) ?? Date.now() - 7 * DAY_MS;
  const toMs = nsToMs(q.to_ns) ?? Date.now();
  if (!a) return { asset: q.asset, bucket: q.bucket, points: [] };

  const stepMs = q.bucket === "1d" ? DAY_MS : HOUR_MS;
  const gaps = gapsFor("funding", q.asset, fromMs, toMs).concat(
    gapsFor("funding", undefined, fromMs, toMs),
  );
  const points: FundingHistoryResponse["points"] = [];

  const firstBucket = Math.ceil(fromMs / stepMs) * stepMs;
  for (let ts = firstBucket; ts <= toMs; ts += stepMs) {
    // A bucket whose window overlaps a recorded gap is simply absent, which is
    // what the real API does: a missing bucket is not a zero.
    const insideGap = gaps.some(
      (g) => ts >= g.startMs && ts < g.startMs + g.durationS * 1000,
    );
    if (insideGap) continue;

    const hour = Math.floor(ts / HOUR_MS);
    const rand = prng(hashSeed(`${a.asset}:hist:${hour}`));
    const season = Math.sin((ts / DAY_MS) * Math.PI * 0.6) * 0.000022;
    const rate = Math.max(
      -0.0005,
      Math.min(0.0005, a.bias + season + (rand() - 0.5) * 0.00004),
    );
    const mark = markFor({ ...a, level: a.level * (0.94 + rand() * 0.12) }, hour);
    points.push({ ts_ns: msToNs(ts), rate_hourly: rateString(rate), apr: aprString(rate), mark });
  }

  return {
    asset: q.asset,
    bucket: q.bucket,
    points,
    coverage: coverage({
      stream: "funding",
      asset: q.asset,
      from_ns: q.from_ns,
      to_ns: q.to_ns,
    }),
  };
}

/** Fee model the mock screener assumes. The real API owns these numbers. */
const MOCK_TAKER_BPS = 4.5;
const MOCK_MAKER_BPS = 1.5;
const MOCK_SLIPPAGE_BPS = 1.0;

function screener(q: ScreenerQuery): ScreenerResponse {
  const hour = currentHour();
  const notional = q.notional;
  const holdingHours = q.holding_hours;
  // Round trip: taker in, taker out, plus an assumed slippage allowance.
  const feeRate = ((MOCK_TAKER_BPS * 2 + MOCK_SLIPPAGE_BPS * 2) / 10_000);
  const feeCost = notional * feeRate;

  const rows: ScreenerRow[] = UNIVERSE.map((a) => {
    const rate = rateFor(a, hour);
    // A short is paid when funding is positive; a long is paid when negative.
    const signedRate = q.side === "short" ? rate : -rate;
    const gross = notional * signedRate * holdingHours;
    const net = gross - feeCost;
    const mark = markFor(a, hour);
    const oiCoins = a.oiCoins * (0.9 + prng(hashSeed(`${a.asset}:oi:${hour}`))() * 0.2);
    const perHour = notional * signedRate;
    const breakeven = perHour > 0 ? feeCost / perHour : null;
    return {
      asset: a.asset,
      apr: aprString(signedRate),
      gross_pnl: gross.toFixed(2),
      fee_cost: feeCost.toFixed(2),
      net_pnl: net.toFixed(2),
      net_apr: ((net / notional) * (8760 / holdingHours)).toFixed(6),
      breakeven_hours: breakeven === null ? null : breakeven.toFixed(3),
      oi_notional: (oiCoins * Number(mark)).toFixed(2),
      covers_fees: net > 0,
    };
  }).sort((x, y) => Number(y.net_apr) - Number(x.net_apr));

  return {
    assumptions: {
      notional: notional.toFixed(2),
      holding_hours: holdingHours.toFixed(2),
      side: q.side,
      taker_fee_rate: (MOCK_TAKER_BPS / 10_000).toFixed(8),
      maker_fee_rate: (MOCK_MAKER_BPS / 10_000).toFixed(8),
      fee_role: "taker",
      legs: 1,
      fills: 2,
      fee_model: `taker in, taker out, plus ${MOCK_SLIPPAGE_BPS} bps slippage allowance per fill`,
      hours_per_year: 8760,
      annualisation: "net_pnl / notional x (hours_per_year / holding_hours)",
      funding_interval_s: [3600],
      fee_source: "mock fixture constants",
      as_of_ns: msToNs(Date.now() - 12_000),
    } satisfies ScreenerResponse["assumptions"],
    rows,
    coverage: coverage({
      stream: "funding",
      from_ns: msToNs(Date.now() - HOUR_MS),
      to_ns: msToNs(Date.now()),
    }),
  };
}

function coverage(q: CoverageQuery): CoverageResponse {
  const fromMs = nsToMs(q.from_ns) ?? 0;
  const toMs = nsToMs(q.to_ns) ?? Date.now();
  const windowS = Math.max(1, (toMs - fromMs) / 1000);

  const gaps: CoverageGap[] = gapsFor(q.stream, q.asset, fromMs, toMs).map((g) => {
    const startMs = Math.max(g.startMs, fromMs);
    const endMs = Math.min(g.startMs + g.durationS * 1000, toMs);
    return {
      from_ns: msToNs(startMs),
      to_ns: msToNs(endMs),
      reason: g.reason,
      duration_s: Math.round((endMs - startMs) / 1000),
    };
  });

  // Window predating the recorder's first byte is missing data too, and saying
  // otherwise would be the exact dishonesty this dashboard is built against.
  const recorderStartMs = Date.now() - 96 * DAY_MS;
  const preHistoryS = fromMs < recorderStartMs ? (Math.min(recorderStartMs, toMs) - fromMs) / 1000 : 0;
  if (preHistoryS > 0) {
    gaps.unshift({
      from_ns: msToNs(fromMs),
      to_ns: msToNs(Math.min(recorderStartMs, toMs)),
      reason: "before_recorder_start: no data exists for this period",
      duration_s: Math.round(preHistoryS),
    });
  }

  const missingS = gaps.reduce((sum, g) => sum + Number(g.duration_s), 0);
  void windowS;
  // `covered` is a boolean on the wire, as the API emits it.
  return { covered: missingS === 0, missing_s: Math.round(missingS), gaps };
}

function stats(): StatsResponse {
  const now = Date.now();
  const h = health();
  const streams: StatsResponse["streams"] = {};
  const bytesByStream: Record<string, string> = {
    funding: "412884992",
    trades: "28412880896",
    mids: "9841262592",
    l2: "214884208640",
    open_interest: "388104192",
  };
  const filesByStream: Record<string, number> = {
    funding: 96,
    trades: 2112,
    mids: 1920,
    l2: 4288,
    open_interest: 96,
  };
  for (const stream of STREAMS) {
    streams[stream] = {
      rows: h.streams[stream]?.rows ?? 0,
      first_ns: msToNs(now - 96 * DAY_MS),
      last_ns: h.streams[stream]?.last_write_ns ?? null,
      files: filesByStream[stream] ?? 0,
      bytes: bytesByStream[stream] ?? "0",
    };
  }
  return { streams, venue: "hyperliquid", network: "mainnet" };
}

export const mockApi: FundingAtlasApi = {
  health: (signal) => settle(health(), signal),
  assets: (signal) => settle(assets(), signal),
  fundingCurrent: (signal) => settle(fundingCurrent(), signal),
  fundingHistory: (q, signal) => settle(fundingHistory(q), signal),
  screener: (q, signal) => settle(screener(q), signal),
  coverage: (q, signal) => settle(coverage(q), signal),
  stats: (signal) => settle(stats(), signal),
};

/** Exported for the asset route's static params and for tests. */
export const MOCK_ASSET_SYMBOLS = UNIVERSE.map((a) => a.asset);
