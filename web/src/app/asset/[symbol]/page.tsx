"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import { api } from "@/lib/api/client";
import type { AssetMeta, FundingCurrentRow, HistoryBucket } from "@/lib/api/types";
import { useApiQuery } from "@/lib/use-api";
import { useMountedNow } from "@/lib/use-now";
import {
  decValue,
  formatCompactUsd,
  formatCount,
  formatDec,
  formatPct,
  parseDec,
} from "@/lib/num";
import { DAY_MS, HOUR_MS, formatUtc, rangeEndingNow } from "@/lib/time";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { ErrorState } from "@/components/states";
import { FundingChart, type ChartMetric } from "@/components/funding-chart";
import { GapNotice } from "@/components/gap-notice";
import { PaymentDirectionCallout, signClass, signOf } from "@/components/funding-sign";
import { Stat } from "@/components/stat";

const RANGES = [
  { label: "24 hours", days: 1, bucket: "1h" as HistoryBucket },
  { label: "7 days", days: 7, bucket: "1h" as HistoryBucket },
  { label: "30 days", days: 30, bucket: "1h" as HistoryBucket },
  { label: "90 days", days: 90, bucket: "1d" as HistoryBucket },
] as const;

const METRICS: { value: ChartMetric; label: string }[] = [
  { value: "apr", label: "Annualised (APR)" },
  { value: "rate_hourly", label: "Hourly rate" },
  { value: "mark", label: "Mark price" },
];

export default function AssetPage() {
  const params = useParams<{ symbol: string }>();
  const symbol = decodeURIComponent(
    Array.isArray(params.symbol) ? (params.symbol[0] ?? "") : (params.symbol ?? ""),
  );

  const [rangeDays, setRangeDays] = useState<number>(7);
  const [bucket, setBucket] = useState<HistoryBucket>("1h");
  const [metric, setMetric] = useState<ChartMetric>("apr");

  const now = useMountedNow();
  const range = useMemo(
    () => (now === null ? null : rangeEndingNow(rangeDays * DAY_MS, now)),
    [now, rangeDays],
  );

  const current = useApiQuery((signal) => api.fundingCurrent(signal), [], { pollMs: 60_000 });
  const assets = useApiQuery((signal) => api.assets(signal), []);
  const history = useApiQuery(
    (signal) =>
      api.fundingHistory(
        {
          asset: symbol,
          from_ns: range?.from_ns ?? "0",
          to_ns: range?.to_ns ?? "0",
          bucket,
        },
        signal,
      ),
    [symbol, range?.from_ns, range?.to_ns, bucket],
    { enabled: range !== null },
  );
  const coverage = useApiQuery(
    (signal) =>
      api.coverage(
        {
          stream: "funding",
          asset: symbol,
          from_ns: range?.from_ns ?? "0",
          to_ns: range?.to_ns ?? "0",
        },
        signal,
      ),
    [symbol, range?.from_ns, range?.to_ns],
    { enabled: range !== null },
  );

  const row: FundingCurrentRow | null =
    current.data?.rows.find((candidate) => candidate.asset === symbol) ?? null;
  const meta: AssetMeta | null =
    assets.data?.assets.find((candidate) => candidate.asset === symbol) ?? null;

  const points = useMemo(() => history.data?.points ?? [], [history.data]);

  // Where the API embeds coverage in the history response, that is the
  // authoritative answer: it describes exactly the window these points came
  // from. The standalone /coverage call is the fallback.
  const windowCoverage = history.data?.coverage ?? coverage.data;
  const stepMs = bucket === "1d" ? DAY_MS : HOUR_MS;

  const windowStats = useMemo(() => {
    if (points.length === 0) return null;
    let sum = 0;
    let positive = 0;
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    for (const point of points) {
      const apr = decValue(point.apr);
      sum += apr;
      if (decValue(point.rate_hourly) > 0) positive += 1;
      min = Math.min(min, apr);
      max = Math.max(max, apr);
    }
    if (!range) return null;
    const expected = Math.max(1, Math.floor((range.toMs - range.fromMs) / stepMs));
    return {
      meanApr: sum / points.length,
      positiveShare: positive / points.length,
      min,
      max,
      observed: points.length,
      expected,
      missing: Math.max(0, expected - points.length),
    };
  }, [points, range, stepMs]);

  const rangeLabel = RANGES.find((option) => option.days === rangeDays)?.label ?? `${rangeDays} d`;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <Link
            href="/"
            className="inline-flex items-center gap-1 text-[11px] text-ink-faint hover:text-ink"
          >
            <ArrowLeft className="size-3" />
            All perps
          </Link>
          <h1 className="mt-1 flex items-center gap-2 text-lg font-semibold tracking-tight">
            {symbol}
            {meta?.has_l2 ? (
              <Badge variant="accent" title="L2 book snapshots are recorded for this asset">
                L2 recorded
              </Badge>
            ) : (
              <Badge variant="outline" title="Book depth is opt-in per asset; this one is not in the subset">
                no L2 depth
              </Badge>
            )}
          </h1>
          <p className="mt-0.5 text-xs text-ink-muted">
            Hyperliquid perpetual. Funding settles hourly; all times UTC.
          </p>
        </div>

        <div className="flex flex-wrap items-end gap-2">
          <div className="space-y-1">
            <Label htmlFor="range">Range</Label>
            <Select
              id="range"
              className="w-32"
              value={String(rangeDays)}
              onChange={(event) => {
                const days = Number(event.target.value);
                setRangeDays(days);
                const next = RANGES.find((option) => option.days === days);
                if (next) setBucket(next.bucket);
              }}
            >
              {RANGES.map((option) => (
                <option key={option.days} value={option.days}>
                  {option.label}
                </option>
              ))}
            </Select>
          </div>
          <div className="space-y-1">
            <Label htmlFor="bucket">Bucket</Label>
            <Select
              id="bucket"
              className="w-24"
              value={bucket}
              onChange={(event) => setBucket(event.target.value as HistoryBucket)}
            >
              <option value="1h">1 hour</option>
              <option value="1d">1 day</option>
            </Select>
          </div>
          <div className="space-y-1">
            <Label htmlFor="metric">Series</Label>
            <Select
              id="metric"
              className="w-44"
              value={metric}
              onChange={(event) => setMetric(event.target.value as ChartMetric)}
            >
              {METRICS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </Select>
          </div>
        </div>
      </div>

      {current.data && !row ? (
        <div className="rounded-card border border-warn/40 bg-warn-dim/25 px-4 py-3 text-xs text-warn">
          <span className="font-semibold">{symbol}</span> is not in the current funding snapshot.
          It may have been delisted, or it may never have been listed. Historical rows below, if
          any, are what the recorder captured while it existed.
        </div>
      ) : null}

      {/* Current state */}
      <div className="grid gap-2 lg:grid-cols-[minmax(0,1fr)_280px]">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-5">
          <Stat
            label="Rate / hour"
            value={
              <span className={signClass(signOf(row?.rate_hourly))}>
                {formatPct(row?.rate_hourly, { decimals: 5, signed: true })}
              </span>
            }
            hint="for the hour in progress"
          />
          <Stat
            label="APR"
            value={
              <span className={signClass(signOf(row?.rate_hourly))}>
                {formatPct(row?.apr, { decimals: 2, signed: true })}
              </span>
            }
            hint="rate x 24 x 365"
          />
          <Stat label="Mark" value={formatDec(row?.mark)} hint="as the venue quotes it" />
          <Stat
            label="Open interest"
            value={formatDec(row?.open_interest, { maxDecimals: 2 })}
            hint="coin units"
          />
          <Stat
            label="OI notional"
            value={formatCompactUsd(row?.oi_notional)}
            title={row ? `$${formatDec(row.oi_notional, { maxDecimals: 2 })}` : undefined}
            hint="OI x price"
          />
        </div>
        <PaymentDirectionCallout rate={row?.rate_hourly} />
      </div>

      {/* Chart */}
      <Card>
        <CardHeader className="flex-col items-start gap-1 sm:flex-row sm:items-center sm:justify-between">
          <CardTitle>Funding history — {rangeLabel}</CardTitle>
          <span className="num text-[11px] text-ink-faint">
            {range ? `${formatUtc(range.from_ns)} → ${formatUtc(range.to_ns)}` : "—"}
          </span>
        </CardHeader>
        <CardContent>
          {history.error ? (
            <ErrorState error={history.error} onRetry={history.reload} what="funding history" />
          ) : range ? (
            <FundingChart
              points={points}
              gaps={windowCoverage?.gaps ?? []}
              metric={metric}
              bucket={bucket}
              fromMs={range.fromMs}
              toMs={range.toMs}
              height={340}
            />
          ) : (
            <div className="h-[340px]" />
          )}
          {history.loading && points.length === 0 && !history.error ? (
            <p className="mt-2 text-[11px] text-ink-faint">Loading history…</p>
          ) : null}
        </CardContent>
      </Card>

      {/* Coverage for exactly the window the chart draws */}
      {coverage.error ? (
        <ErrorState error={coverage.error} onRetry={coverage.reload} what="data coverage" />
      ) : windowCoverage ? (
        <GapNotice
          coverage={windowCoverage}
          windowLabel={`the ${rangeLabel} plotted above`}
          windowS={range ? (range.toMs - range.fromMs) / 1000 : undefined}
        />
      ) : null}

      <div className="grid gap-4 lg:grid-cols-2">
        {/* Window statistics, with the sample size stated */}
        <Card>
          <CardHeader>
            <CardTitle>Over the plotted window</CardTitle>
          </CardHeader>
          <CardContent>
            {windowStats ? (
              <>
                <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                  <Stat
                    label="Mean APR"
                    value={
                      <span className={windowStats.meanApr >= 0 ? "text-pos" : "text-neg"}>
                        {formatPct(windowStats.meanApr, { decimals: 2, signed: true })}
                      </span>
                    }
                  />
                  <Stat
                    label="Min APR"
                    value={formatPct(windowStats.min, { decimals: 2, signed: true })}
                    tone="muted"
                  />
                  <Stat
                    label="Max APR"
                    value={formatPct(windowStats.max, { decimals: 2, signed: true })}
                    tone="muted"
                  />
                  <Stat
                    label="Hours positive"
                    value={`${(windowStats.positiveShare * 100).toFixed(1)}%`}
                    hint="longs paying"
                  />
                </div>
                <p
                  className={cn(
                    "mt-3 text-[11px] leading-relaxed",
                    windowStats.missing > 0 ? "text-warn" : "text-ink-faint",
                  )}
                >
                  Computed over <span className="num">{formatCount(windowStats.observed)}</span>{" "}
                  recorded buckets out of{" "}
                  <span className="num">{formatCount(windowStats.expected)}</span> the window
                  would hold.
                  {windowStats.missing > 0 ? (
                    <>
                      {" "}
                      <span className="num">{formatCount(windowStats.missing)}</span> buckets are
                      missing, so these are averages over the data that exists — not over the
                      window.
                    </>
                  ) : (
                    " No buckets are missing."
                  )}
                </p>
              </>
            ) : (
              <p className="text-xs text-ink-faint">
                No recorded buckets in this window, so there is nothing to average.
              </p>
            )}
          </CardContent>
        </Card>

        {/* Venue trading rules */}
        <Card>
          <CardHeader>
            <CardTitle>Venue trading rules</CardTitle>
          </CardHeader>
          <CardContent>
            {assets.error ? (
              <ErrorState error={assets.error} onRetry={assets.reload} what="asset metadata" />
            ) : meta ? (
              <>
                <dl className="grid grid-cols-1 gap-x-6 gap-y-1.5 sm:grid-cols-2">
                  <Rule label="Price tick" value={formatDec(meta.price_tick, { maxDecimals: 10 })} />
                  <Rule
                    label="Price significant figures"
                    value={meta.price_sig_figs == null ? "—" : formatDec(meta.price_sig_figs)}
                  />
                  <Rule label="Size lot" value={formatDec(meta.size_lot, { maxDecimals: 10 })} />
                  <Rule
                    label="Minimum order value"
                    value={
                      parseDec(meta.min_notional)
                        ? `$${formatDec(meta.min_notional, { maxDecimals: 2 })}`
                        : "—"
                    }
                  />
                  <Rule
                    label="Max leverage"
                    value={meta.max_leverage == null ? "—" : `${formatDec(meta.max_leverage)}x`}
                  />
                  <Rule label="L2 depth recorded" value={meta.has_l2 ? "yes" : "no"} />
                  <Rule label="First seen" value={formatUtc(meta.first_seen_ns)} />
                  <Rule label="Last seen" value={formatUtc(meta.last_seen_ns)} />
                </dl>
                <p className="mt-3 text-[11px] leading-relaxed text-ink-faint">
                  Hyperliquid has no single tick field: a valid perp price must satisfy both
                  rules at once — at most{" "}
                  <span className="num">{formatDec(meta.price_sig_figs ?? 5)}</span> significant
                  figures, and at most <span className="num">6 &minus; szDecimals</span> decimal
                  places. Integer prices are always valid. The tick above is the decimal-place
                  limit expressed as an increment; it does not relax the significant-figure rule.
                </p>
              </>
            ) : (
              <p className="text-xs text-ink-faint">
                {assets.loading ? "Loading venue rules…" : `No venue metadata recorded for ${symbol}.`}
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Reading this page</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-[11px] leading-relaxed text-ink-muted">
          <p>
            The hourly rate shown at the top is the venue&apos;s running estimate for the hour in
            progress, which moves as the premium index accumulates. The chart plots the rate
            recorded for each completed bucket. Those are different quantities and the difference
            is the point of recording at 60-second resolution.
          </p>
          <p>
            Open interest is reported by the venue in{" "}
            <span className="text-ink">coin units</span>; the notional column multiplies it by
            price. A series that mixes the two is unusable, and the error is invisible in any
            single sample.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}

function Rule({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-line/60 pb-1">
      <dt className="text-[11px] text-ink-faint">{label}</dt>
      <dd className="num text-xs text-ink">{value}</dd>
    </div>
  );
}
