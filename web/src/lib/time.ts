/**
 * Nanosecond timestamps.
 *
 * The API's `*_ns` fields are nanoseconds since the Unix epoch. A current
 * nanosecond timestamp is ~1.8e18, which is far beyond Number.MAX_SAFE_INTEGER
 * (9.007e15) — so if such a value arrives as a JSON *number* it has already
 * been rounded to the nearest ~256 ns by the time JavaScript sees it. That is
 * harmless for display at second resolution and corrupting if the value is
 * ever echoed back as a query parameter.
 *
 * Everything here therefore accepts `string | number`, does the division with
 * BigInt whenever the value is a string, and round-trips query parameters as
 * the original text rather than as a parsed double.
 */

export type NsTimestamp = string | number;

const NS_PER_MS = 1_000_000n;

/** Nanoseconds -> milliseconds, exactly when given a string. */
export function nsToMs(ns: NsTimestamp | null | undefined): number | null {
  if (ns === null || ns === undefined) return null;
  if (typeof ns === "number") {
    return Number.isFinite(ns) ? Math.floor(ns / 1e6) : null;
  }
  const text = ns.trim();
  if (!/^[+-]?\d+$/.test(text)) {
    const asNumber = Number(text);
    return Number.isFinite(asNumber) ? Math.floor(asNumber / 1e6) : null;
  }
  try {
    return Number(BigInt(text) / NS_PER_MS);
  } catch {
    return null;
  }
}

/** Milliseconds -> a nanosecond string safe to put in a query parameter. */
export function msToNs(ms: number): string {
  return (BigInt(Math.trunc(ms)) * NS_PER_MS).toString();
}

/** Seconds since epoch, as Lightweight Charts wants them. */
export function nsToUnixSeconds(ns: NsTimestamp | null | undefined): number | null {
  const ms = nsToMs(ns);
  return ms === null ? null : Math.floor(ms / 1000);
}

export function nsToDate(ns: NsTimestamp | null | undefined): Date | null {
  const ms = nsToMs(ns);
  return ms === null ? null : new Date(ms);
}

const UTC_DATE_TIME = new Intl.DateTimeFormat("en-GB", {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const UTC_DATE_HOUR = new Intl.DateTimeFormat("en-GB", {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const UTC_DATE = new Intl.DateTimeFormat("en-GB", {
  year: "numeric",
  month: "short",
  day: "2-digit",
  timeZone: "UTC",
});

/** Absolute UTC timestamp. Every time in this app is UTC and says so. */
export function formatUtc(ns: NsTimestamp | null | undefined, fallback = "—"): string {
  const date = nsToDate(ns);
  return date ? `${UTC_DATE_TIME.format(date)} UTC` : fallback;
}

export function formatUtcShort(ns: NsTimestamp | null | undefined, fallback = "—"): string {
  const date = nsToDate(ns);
  return date ? UTC_DATE_HOUR.format(date) : fallback;
}

export function formatUtcDate(ns: NsTimestamp | null | undefined, fallback = "—"): string {
  const date = nsToDate(ns);
  return date ? UTC_DATE.format(date) : fallback;
}

/** "14s ago" / "3m ago" / "2h ago", relative to now. */
export function formatAgo(ns: NsTimestamp | null | undefined, now = Date.now()): string {
  const ms = nsToMs(ns);
  if (ms === null) return "—";
  const delta = Math.max(0, Math.round((now - ms) / 1000));
  if (delta < 2) return "just now";
  if (delta < 60) return `${delta}s ago`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`;
  return `${Math.floor(delta / 86400)}d ago`;
}

export const HOUR_MS = 3_600_000;
export const DAY_MS = 86_400_000;

export interface NsRange {
  from_ns: string;
  to_ns: string;
  /** Milliseconds, kept alongside for chart maths. */
  fromMs: number;
  toMs: number;
}

export function rangeEndingNow(spanMs: number, now = Date.now()): NsRange {
  const toMs = now;
  const fromMs = now - spanMs;
  return { from_ns: msToNs(fromMs), to_ns: msToNs(toMs), fromMs, toMs };
}
