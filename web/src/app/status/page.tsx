"use client";

import { useMemo, useState } from "react";
import { Activity, HardDrive, RefreshCw } from "lucide-react";

import { api } from "@/lib/api/client";
import type { CoverageResponse } from "@/lib/api/types";
import { useApiQuery } from "@/lib/use-api";
import { useMountedNow } from "@/lib/use-now";
import { decValue, formatBytes, formatCount, formatDuration, formatPct } from "@/lib/num";
import { DAY_MS, formatAgo, formatUtc, rangeEndingNow } from "@/lib/time";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ErrorState, TableSkeleton } from "@/components/states";
import { coverageFraction, coverageIsComplete } from "@/components/gap-notice";
import { Stat } from "@/components/stat";

const WINDOWS = [
  { label: "24 hours", days: 1 },
  { label: "7 days", days: 7 },
  { label: "30 days", days: 30 },
  { label: "90 days", days: 90 },
] as const;

/** Lag thresholds for the status badge. A stream is late long before it is dead. */
function lagTone(lagS: number, stream: string): "positive" | "warning" | "negative" {
  // Funding is sampled every 60 s and settles hourly, so 120 s of lag is fine;
  // trades and mids are continuous and 120 s of silence is an outage.
  const warnAt = stream === "funding" || stream === "open_interest" ? 120 : 30;
  const failAt = stream === "funding" || stream === "open_interest" ? 600 : 120;
  if (lagS >= failAt) return "negative";
  if (lagS >= warnAt) return "warning";
  return "positive";
}

export default function StatusPage() {
  const [windowDays, setWindowDays] = useState<number>(7);

  const health = useApiQuery((signal) => api.health(signal), [], { pollMs: 30_000 });
  const stats = useApiQuery((signal) => api.stats(signal), []);

  const streamNames = useMemo(() => {
    const names = new Set<string>();
    for (const key of Object.keys(health.data?.streams ?? {})) names.add(key);
    for (const key of Object.keys(stats.data?.streams ?? {})) names.add(key);
    return [...names].sort();
  }, [health.data, stats.data]);

  const now = useMountedNow();
  const range = useMemo(
    () => (now === null ? null : rangeEndingNow(windowDays * DAY_MS, now)),
    [now, windowDays],
  );
  const streamKey = streamNames.join(",");

  const coverage = useApiQuery(
    async (signal) => {
      const entries = await Promise.all(
        streamNames.map(async (stream) => {
          const result = await api.coverage(
            { stream, from_ns: range?.from_ns ?? "0", to_ns: range?.to_ns ?? "0" },
            signal,
          );
          return [stream, result] as const;
        }),
      );
      return Object.fromEntries(entries) as Record<string, CoverageResponse>;
    },
    [streamKey, range?.from_ns, range?.to_ns],
    { enabled: range !== null && streamNames.length > 0 },
  );

  const allGaps = useMemo(() => {
    const out: { stream: string; gap: CoverageResponse["gaps"][number] }[] = [];
    for (const [stream, result] of Object.entries(coverage.data ?? {})) {
      for (const gap of result.gaps) out.push({ stream, gap });
    }
    return out.sort((a, b) => Number(decValue(b.gap.from_ns)) - Number(decValue(a.gap.from_ns)));
  }, [coverage.data]);

  const totalMissingS = useMemo(
    () =>
      Object.values(coverage.data ?? {}).reduce((sum, result) => sum + decValue(result.missing_s), 0),
    [coverage.data],
  );

  const windowS = windowDays * 24 * 3600;

  const statusTone =
    health.data?.status === "ok"
      ? "positive"
      : health.data?.status === "degraded"
        ? "warning"
        : "negative";

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Recorder status</h1>
          <p className="mt-0.5 max-w-3xl text-xs text-ink-muted">
            Live health of the recorder, and every hole in the record it knows about. Order-book
            and funding history cannot be backfilled: a gap here is permanent, and publishing it
            is the only way anyone can tell what this dataset actually contains.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={health.reload} disabled={health.loading}>
          <RefreshCw className={health.loading ? "animate-spin" : undefined} />
          Refresh
        </Button>
      </div>

      {health.error ? (
        <ErrorState error={health.error} onRetry={health.reload} what="recorder health" />
      ) : null}

      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
        <Stat
          label="Status"
          value={health.data?.status ?? "—"}
          tone={health.data ? statusTone : "muted"}
          hint={health.data ? `checked ${formatAgo(msOrNull(health.fetchedAt))}` : undefined}
        />
        <Stat
          label="Uptime"
          value={health.data ? formatDuration(health.data.uptime_s) : "—"}
          hint="since process start"
        />
        <Stat
          label="Disk free"
          value={health.data ? formatBytes(health.data.disk_free_bytes) : "—"}
          hint="on the recording host"
        />
        <Stat label="Venue" value={stats.data?.venue ?? "—"} hint={stats.data?.network} />
        <Stat
          label="Streams"
          value={streamNames.length || "—"}
          hint="recorded continuously"
        />
        <Stat
          label={`Missing, ${windowDays}d`}
          value={coverage.data ? formatDuration(totalMissingS) : "—"}
          tone={totalMissingS > 0 ? "warning" : "positive"}
          hint="summed across streams"
        />
      </div>

      <Card>
        <CardHeader className="flex-row items-center gap-2">
          <Activity className="size-3.5 text-ink-faint" />
          <CardTitle>Per-stream health</CardTitle>
        </CardHeader>
        {health.loading && !health.data ? (
          <TableSkeleton cols={6} rows={5} />
        ) : (
          <div className="overflow-x-auto scrollbar-thin">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Stream</TableHead>
                  <TableHead className="text-right">Lag</TableHead>
                  <TableHead>Last write</TableHead>
                  <TableHead className="text-right">Rows</TableHead>
                  <TableHead className="text-right">Files</TableHead>
                  <TableHead className="text-right">On disk</TableHead>
                  <TableHead>First row</TableHead>
                  <TableHead className="text-right">
                    Coverage, {windowDays}d
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {streamNames.map((stream) => {
                  const streamHealth = health.data?.streams[stream];
                  const streamStats = stats.data?.streams[stream];
                  const streamCoverage = coverage.data?.[stream];
                  const lagS = decValue(streamHealth?.lag_s ?? null);
                  const tone = streamHealth?.lag_s == null ? "neutral" : lagTone(lagS, stream);
                  return (
                    <TableRow key={stream}>
                      <TableCell className="font-medium text-ink">{stream}</TableCell>
                      <TableCell className="text-right">
                        <Badge
                          variant={
                            tone === "positive"
                              ? "positive"
                              : tone === "warning"
                                ? "warning"
                                : tone === "negative"
                                  ? "negative"
                                  : "neutral"
                          }
                          className="num"
                        >
                          {streamHealth?.lag_s == null
                            ? "unknown"
                            : formatDuration(streamHealth.lag_s)}
                        </Badge>
                      </TableCell>
                      <TableCell className="num text-xs text-ink-muted">
                        {formatUtc(streamHealth?.last_write_ns)}
                        <span className="ml-1.5 text-ink-faint">
                          ({formatAgo(streamHealth?.last_write_ns)})
                        </span>
                      </TableCell>
                      <TableCell className="num text-right text-ink-muted">
                        {formatCount(streamHealth?.rows ?? streamStats?.rows ?? null)}
                      </TableCell>
                      <TableCell className="num text-right text-ink-faint">
                        {formatCount(streamStats?.files ?? null)}
                      </TableCell>
                      <TableCell className="num text-right text-ink-faint">
                        {formatBytes(streamStats?.bytes ?? null)}
                      </TableCell>
                      <TableCell className="num text-xs text-ink-faint">
                        {formatUtc(streamStats?.first_ns)}
                      </TableCell>
                      <TableCell
                        className={cn(
                          "num text-right",
                          streamCoverage
                            ? coverageIsComplete(streamCoverage)
                              ? "text-pos"
                              : "text-warn"
                            : "text-ink-faint",
                        )}
                      >
                        {streamCoverage
                          ? renderCoverage(streamCoverage, windowS)
                          : coverage.loading
                            ? "…"
                            : "—"}
                      </TableCell>
                    </TableRow>
                  );
                })}
                {streamNames.length === 0 && !health.loading ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={8} className="py-6 text-center text-xs text-ink-faint">
                      The recorder reported no streams.
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>
        )}
        <div className="border-t border-line px-4 py-2 text-[11px] text-ink-faint">
          Lag is the age of the newest row written for that stream. Funding is sampled every 60 s
          and open interest alongside it, so a lag of a minute or so is normal there; trades and
          mids are continuous, and the same lag on those is an outage.
        </div>
      </Card>

      <Card>
        <CardHeader className="flex-col items-start gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-2">
            <HardDrive className="size-3.5 text-ink-faint" />
            <CardTitle>Known gaps</CardTitle>
            {coverage.data ? (
              <Badge variant={allGaps.length === 0 ? "positive" : "warning"}>
                {allGaps.length} in the last {windowDays} d
              </Badge>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            <Label htmlFor="gap-window">Window</Label>
            <Select
              id="gap-window"
              className="w-36"
              value={String(windowDays)}
              onChange={(event) => setWindowDays(Number(event.target.value))}
            >
              {WINDOWS.map((option) => (
                <option key={option.days} value={option.days}>
                  {option.label}
                </option>
              ))}
            </Select>
          </div>
        </CardHeader>

        {coverage.error ? (
          <CardContent>
            <ErrorState error={coverage.error} onRetry={coverage.reload} what="coverage" />
          </CardContent>
        ) : coverage.loading && !coverage.data ? (
          <TableSkeleton cols={4} rows={4} />
        ) : allGaps.length === 0 ? (
          <CardContent>
            <div className="rounded-card border border-pos/30 bg-pos-dim/20 px-3 py-2 text-xs text-pos">
              No gaps recorded in any stream over the last {windowDays} days. This is a claim the
              recorder makes about its own log — it is checkable against the per-stream row
              counts and first/last timestamps above, and it says nothing about the period before{" "}
              <span className="num">{formatUtc(earliestFirstNs(stats.data?.streams))}</span>.
            </div>
          </CardContent>
        ) : (
          <div className="overflow-x-auto scrollbar-thin">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Stream</TableHead>
                  <TableHead>From (UTC)</TableHead>
                  <TableHead>To (UTC)</TableHead>
                  <TableHead className="text-right">Duration</TableHead>
                  <TableHead>Recorded cause</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {allGaps.map(({ stream, gap }, index) => (
                  <TableRow key={`${stream}-${String(gap.from_ns)}-${index}`}>
                    <TableCell className="font-medium text-ink">{stream}</TableCell>
                    <TableCell className="num text-xs text-warn/90">
                      {formatUtc(gap.from_ns)}
                    </TableCell>
                    <TableCell className="num text-xs text-warn/90">
                      {formatUtc(gap.to_ns)}
                    </TableCell>
                    <TableCell className="num text-right text-ink-muted">
                      {formatDuration(gap.duration_s)}
                    </TableCell>
                    <TableCell className="text-xs text-ink-muted">{gap.reason}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
        <div className="border-t border-line px-4 py-2 text-[11px] leading-relaxed text-ink-faint">
          A gap is written to the event log with its cause and duration at the moment it is
          detected, not reconstructed afterwards. Research that crosses one is expected to refuse
          to run rather than interpolate across it.
        </div>
      </Card>
    </div>
  );
}

function renderCoverage(result: CoverageResponse, windowS: number): string {
  const fraction = coverageFraction(result, windowS);
  if (fraction === null) return coverageIsComplete(result) ? "complete" : "incomplete";
  return formatPct(fraction, { decimals: 3 });
}

function msOrNull(ms: number | null): string | null {
  if (ms === null) return null;
  return (BigInt(ms) * 1_000_000n).toString();
}

function earliestFirstNs(
  streams: Record<string, { first_ns: string | number | null }> | undefined,
): string | number | null {
  if (!streams) return null;
  let earliest: string | number | null = null;
  let earliestValue = Number.POSITIVE_INFINITY;
  for (const stream of Object.values(streams)) {
    if (stream.first_ns == null) continue;
    const value = Number(stream.first_ns);
    if (Number.isFinite(value) && value < earliestValue) {
      earliestValue = value;
      earliest = stream.first_ns;
    }
  }
  return earliest;
}
