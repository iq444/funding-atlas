/**
 * Decimal handling.
 *
 * The data API sends monetary and rate values as JSON strings so that no
 * precision is lost in transport. The rule this module enforces is that a
 * value is never *displayed* to more digits than the string actually carried:
 * a mark of "3.4210" is four decimal places of real information, and printing
 * it as 3.42100000 invents a digit the venue never quoted.
 *
 * So every value keeps its original text (`raw`) alongside the parsed double
 * (`value`, used only for sorting and arithmetic) and the number of decimals
 * the venue sent (`decimals`, the ceiling for formatting).
 */

/** What the wire may hand us. Strings are preferred; numbers are tolerated. */
export type Numeric = string | number;

export interface Dec {
  /** Exactly the characters the API sent (trimmed). */
  raw: string;
  /** Double precision view. For comparison and arithmetic, not for display. */
  value: number;
  /** Decimal places present in `raw`. Formatting never exceeds this. */
  decimals: number;
  /** True when the source was a JSON number rather than a string. */
  lossy: boolean;
}

const DEC_RE = /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/;

export function parseDec(input: Numeric | null | undefined): Dec | null {
  if (input === null || input === undefined) return null;

  if (typeof input === "number") {
    if (!Number.isFinite(input)) return null;
    const raw = String(input);
    return { raw, value: input, decimals: decimalsOf(raw), lossy: true };
  }

  const raw = input.trim();
  if (raw === "" || !DEC_RE.test(raw)) return null;
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  return { raw, value, decimals: decimalsOf(raw), lossy: false };
}

/** Parsed value, or 0 — for sorting only, never for display. */
export function decValue(input: Numeric | null | undefined): number {
  return parseDec(input)?.value ?? 0;
}

function decimalsOf(raw: string): number {
  const exp = raw.match(/[eE]([+-]?\d+)/);
  const dot = raw.indexOf(".");
  const mantissaDecimals =
    dot === -1 ? 0 : (exp ? raw.slice(dot + 1, exp.index) : raw.slice(dot + 1)).length;
  if (!exp) return mantissaDecimals;
  const shift = Number(exp[1]);
  return Math.max(0, mantissaDecimals - shift);
}

function groupThousands(intPart: string): string {
  const neg = intPart.startsWith("-");
  const digits = neg ? intPart.slice(1) : intPart;
  return (neg ? "-" : "") + digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export interface FormatOptions {
  /** Hard ceiling on decimals shown. Clamped down to the value's own precision. */
  maxDecimals?: number;
  /** Pad with zeros up to this many decimals (never beyond `raw`'s precision). */
  minDecimals?: number;
  /** Thousands separators. Default true. */
  group?: boolean;
  /** Always render an explicit + for positive values. */
  signed?: boolean;
  /** What to render when the value is missing or unparseable. */
  fallback?: string;
}

/**
 * Format a wire value for display without ever adding precision it lacks.
 */
export function formatDec(input: Numeric | null | undefined, opts: FormatOptions = {}): string {
  const d = parseDec(input);
  if (!d) return opts.fallback ?? "—";

  const { group = true, signed = false } = opts;
  // The value's own precision is the ceiling; a caller asking for more gets less.
  const ceiling = d.decimals;
  const max = Math.min(opts.maxDecimals ?? ceiling, ceiling);
  const min = Math.min(opts.minDecimals ?? 0, max);

  let text = d.value.toFixed(max);
  if (min < max) {
    // Drop trailing zeros that are not required by `min`.
    text = text.replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
    const dot = text.indexOf(".");
    const have = dot === -1 ? 0 : text.length - dot - 1;
    if (have < min) text = d.value.toFixed(min);
  }

  const [intPart = "0", fracPart] = text.split(".");
  let out = group ? groupThousands(intPart) : intPart;
  if (fracPart) out += "." + fracPart;
  if (signed && d.value > 0) out = "+" + out;
  return out;
}

/** A fraction (0.0000125) as a percentage string ("0.00125%"). */
export function formatPct(
  input: Numeric | null | undefined,
  opts: { decimals?: number; signed?: boolean; fallback?: string } = {},
): string {
  const d = parseDec(input);
  if (!d) return opts.fallback ?? "—";
  const pct = d.value * 100;
  const decimals = opts.decimals ?? 4;
  const body = Math.abs(pct).toFixed(decimals);
  const sign = d.value < 0 ? "-" : opts.signed ? "+" : "";
  return `${sign}${groupThousands(body.split(".")[0] ?? "0")}${
    body.includes(".") ? "." + body.split(".")[1] : ""
  }%`;
}

/**
 * Large notionals, abbreviated. Abbreviation is a display choice, so the
 * caller is expected to put the exact `formatDec` text in a title attribute.
 */
export function formatCompactUsd(
  input: Numeric | null | undefined,
  opts: { fallback?: string; signed?: boolean } = {},
): string {
  const d = parseDec(input);
  if (!d) return opts.fallback ?? "—";
  const sign = d.value < 0 ? "-" : opts.signed && d.value > 0 ? "+" : "";
  const abs = Math.abs(d.value);
  const units: [number, string][] = [
    [1e12, "T"],
    [1e9, "B"],
    [1e6, "M"],
    [1e3, "k"],
  ];
  for (const [scale, suffix] of units) {
    if (abs >= scale) {
      const scaled = abs / scale;
      return `${sign}$${scaled.toFixed(scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2)}${suffix}`;
    }
  }
  return `${sign}$${formatDec(d.raw, { maxDecimals: 2, minDecimals: 2 })}`;
}

/** Exact dollars, to the precision the API sent (capped at 2dp for readability). */
export function formatUsd(
  input: Numeric | null | undefined,
  opts: { maxDecimals?: number; signed?: boolean; fallback?: string } = {},
): string {
  const d = parseDec(input);
  if (!d) return opts.fallback ?? "—";
  const sign = d.value < 0 ? "-" : opts.signed && d.value > 0 ? "+" : "";
  return `${sign}$${formatDec(Math.abs(d.value).toString(), {
    maxDecimals: opts.maxDecimals ?? 2,
    minDecimals: Math.min(2, opts.maxDecimals ?? 2),
  })}`;
}

/** Hours, as "13.4 h" / "18 h" / "9.2 d" once the number stops being useful in hours. */
export function formatHours(input: Numeric | null | undefined, fallback = "—"): string {
  const d = parseDec(input);
  if (!d) return fallback;
  if (!Number.isFinite(d.value)) return "never";
  if (d.value >= 72) return `${(d.value / 24).toFixed(1)} d`;
  if (d.value >= 10) return `${d.value.toFixed(1)} h`;
  return `${d.value.toFixed(2)} h`;
}

/** Seconds as a compact human duration: 45s, 12m 03s, 3h 12m, 2d 04h. */
export function formatDuration(seconds: Numeric | null | undefined, fallback = "—"): string {
  const d = parseDec(seconds);
  if (!d) return fallback;
  const s = Math.max(0, Math.round(d.value));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  const days = Math.floor(h / 24);
  return `${days}d ${String(h % 24).padStart(2, "0")}h`;
}

export function formatBytes(input: Numeric | null | undefined, fallback = "—"): string {
  const d = parseDec(input);
  if (!d) return fallback;
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = d.value;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(i === 0 ? 0 : v >= 100 ? 0 : 1)} ${units[i]}`;
}

export function formatCount(input: Numeric | null | undefined, fallback = "—"): string {
  const d = parseDec(input);
  if (!d) return fallback;
  return groupThousands(Math.round(d.value).toString());
}
