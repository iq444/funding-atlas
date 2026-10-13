/**
 * Typed client for the funding-atlas data API.
 *
 * Two sources of data are wired behind one interface:
 *   - the live API at NEXT_PUBLIC_API_BASE_URL
 *   - the mock fixture layer in ./mock.ts, used when NEXT_PUBLIC_MOCK_API=1
 *
 * Mock mode exists so that the whole dashboard can be built, reviewed and
 * tested before the API is deployed. Every page renders a loud banner while it
 * is on, because a dashboard showing synthetic numbers without saying so is
 * exactly the failure this project is supposed to be the opposite of.
 */

import type {
  AssetsResponse,
  CoverageQuery,
  CoverageResponse,
  FundingCurrentResponse,
  FundingHistoryResponse,
  HealthResponse,
  HistoryBucket,
  ScreenerQuery,
  ScreenerResponse,
  StatsResponse,
} from "./types";
import * as mock from "./mock";

export const API_BASE_URL = (
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000"
).replace(/\/+$/, "");

export const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_API === "1";

export class ApiError extends Error {
  readonly status: number;
  readonly url: string;

  constructor(message: string, status: number, url: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.url = url;
  }
}

/** Where the data on screen came from. Rendered in the footer of every page. */
export interface DataSource {
  kind: "live" | "mock";
  label: string;
  detail: string;
}

export const DATA_SOURCE: DataSource = MOCK_MODE
  ? {
      kind: "mock",
      label: "Mock fixtures",
      detail:
        "Synthetic data generated in the browser. No number on this page came from an exchange.",
    }
  : { kind: "live", label: "Live API", detail: API_BASE_URL };

type QueryValue = string | number | boolean | undefined | null;

function buildUrl(path: string, params?: Record<string, QueryValue>): string {
  const url = new URL(API_BASE_URL + path);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value === undefined || value === null || value === "") continue;
    // Values are stringified by the caller where precision matters (ns
    // timestamps), so never re-serialise them through Number here.
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

async function get<T>(
  path: string,
  params?: Record<string, QueryValue>,
  signal?: AbortSignal,
): Promise<T> {
  const url = buildUrl(path, params);
  let response: Response;
  try {
    response = await fetch(url, {
      signal,
      headers: { accept: "application/json" },
      cache: "no-store",
    });
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
    throw new ApiError(
      `Cannot reach the data API at ${API_BASE_URL}. Is it running?`,
      0,
      url,
    );
  }

  if (!response.ok) {
    let detail = "";
    try {
      detail = (await response.text()).slice(0, 300);
    } catch {
      /* body unreadable; the status is the whole story */
    }
    throw new ApiError(
      `${path} returned ${response.status} ${response.statusText}${detail ? ` — ${detail}` : ""}`,
      response.status,
      url,
    );
  }

  return (await response.json()) as T;
}

export interface FundingHistoryQuery {
  asset: string;
  from_ns: string;
  to_ns: string;
  bucket: HistoryBucket;
}

export interface FundingAtlasApi {
  health(signal?: AbortSignal): Promise<HealthResponse>;
  assets(signal?: AbortSignal): Promise<AssetsResponse>;
  fundingCurrent(signal?: AbortSignal): Promise<FundingCurrentResponse>;
  fundingHistory(q: FundingHistoryQuery, signal?: AbortSignal): Promise<FundingHistoryResponse>;
  screener(q: ScreenerQuery, signal?: AbortSignal): Promise<ScreenerResponse>;
  coverage(q: CoverageQuery, signal?: AbortSignal): Promise<CoverageResponse>;
  stats(signal?: AbortSignal): Promise<StatsResponse>;
}

const liveApi: FundingAtlasApi = {
  health: (signal) => get<HealthResponse>("/health", undefined, signal),
  assets: (signal) => get<AssetsResponse>("/assets", undefined, signal),
  fundingCurrent: (signal) => get<FundingCurrentResponse>("/funding/current", undefined, signal),
  fundingHistory: (q, signal) =>
    get<FundingHistoryResponse>(
      "/funding/history",
      { asset: q.asset, from_ns: q.from_ns, to_ns: q.to_ns, bucket: q.bucket },
      signal,
    ),
  screener: (q, signal) =>
    get<ScreenerResponse>(
      "/funding/screener",
      { notional: q.notional, holding_hours: q.holding_hours, side: q.side },
      signal,
    ),
  coverage: (q, signal) =>
    get<CoverageResponse>(
      "/coverage",
      { stream: q.stream, asset: q.asset, from_ns: q.from_ns, to_ns: q.to_ns },
      signal,
    ),
  stats: (signal) => get<StatsResponse>("/stats", undefined, signal),
};

export const api: FundingAtlasApi = MOCK_MODE ? mock.mockApi : liveApi;
export { liveApi };
