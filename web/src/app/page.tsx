"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { RefreshCw } from "lucide-react";

import { api } from "@/lib/api/client";
import type { FundingCurrentRow } from "@/lib/api/types";
import { useApiQuery } from "@/lib/use-api";
import { useMountedNow } from "@/lib/use-now";
import { decValue, formatCompactUsd, formatDec, formatPct } from "@/lib/num";
import { DAY_MS, formatAgo, formatUtc, msToNs, rangeEndingNow } from "@/lib/time";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
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
import { FundingExplainer } from "@/components/funding-explainer";
import { GapNotice } from "@/components/gap-notice";
import { PaymentFlow, SignLegend, signClass, signOf } from "@/components/funding-sign";
import { SortHeader, type SortState } from "@/components/sort-header";
import { Stat } from "@/components/stat";

type SortKey = "asset" | "rate_hourly" | "apr" | "open_interest" | "oi_notional" | "mark";
type SignFilter = "all" | "positive" | "negative";

const COVERAGE_WINDOW_MS = DAY_MS;

export default function FundingOverviewPage() {
  const [query, setQuery] = useState("");
  const [signFilter, setSignFilter] = useState<SignFilter>("all");
  const [minOi, setMinOi] = useState("");
  const [sort, setSort] = useState<SortState<SortKey>>({ key: "apr", direction: "desc" });

  const current = useApiQuery((signal) => api.fundingCurrent(signal), [], { pollMs: 60_000 });

  // The window the table describes is "now", but the record behind it is the
  // last day of the funding stream — so its coverage is stated here too.
  const now = useMountedNow();
  const coverageRange = useMemo(
    () => (now === null ? null : rangeEndingNow(COVERAGE_WINDOW_MS, now)),
    [now],
  );
  const coverage = useApiQuery(
    (signal) =>
      api.coverage(
        {
          stream: "funding",
          from_ns: coverageRange?.from_ns ?? "0",
          to_ns: coverageRange?.to_ns ?? "0",
        },
        signal,
      ),
    [coverageRange?.from_ns, coverageRange?.to_ns],
    { enabled: coverageRange !== null },
  );

  const rows = useMemo(() => current.data?.rows ?? [], [current.data]);

  const filtered = useMemo(() => {
    const needle = query.trim().toUpperCase();
    const floor = minOi.trim() === "" ? null : Number(minOi);
    return rows.filter((row) => {
      if (needle && !row.asset.toUpperCase().includes(needle)) return false;
      const sign = signOf(row.rate_hourly);
      if (signFilter === "positive" && sign !== "positive") return false;
      if (signFilter === "negative" && sign !== "negative") return false;
      if (floor !== null && Number.isFinite(floor) && decValue(row.oi_notional) < floor) {
        return false;
      }
      return true;
    });
  }, [rows, query, signFilter, minOi]);

  const sorted = useMemo(() => {
    const factor = sort.direction === "asc" ? 1 : -1;
    const pick = (row: FundingCurrentRow): number | string => {
      switch (sort.key) {
        case "asset":
          return row.asset;
        case "rate_hourly":
          return decValue(row.rate_hourly);
        case "apr":
          return decValue(row.apr);
        case "mark":
          return decValue(row.mark);
        case "open_interest":
          return decValue(row.open_interest);
        case "oi_notional":
          return decValue(row.oi_notional);
      }
    };
    return [...filtered].sort((a, b) => {
      const left = pick(a);
      const right = pick(b);
      if (typeof left === "string" || typeof right === "string") {
        return factor * String(left).localeCompare(String(right));
      }
      return factor * (left - right);
    });
  }, [filtered, sort]);

  const summary = useMemo(() => {
    if (rows.length === 0) return null;
    let positive = 0;
    let negative = 0;
    let oiTotal = 0;
    let highest: FundingCurrentRow | null = null;
    let lowest: FundingCurrentRow | null = null;
    for (const row of rows) {
      const sign = signOf(row.rate_hourly);
      if (sign === "positive") positive += 1;
      if (sign === "negative") negative += 1;
      oiTotal += decValue(row.oi_notional);
      if (!highest || decValue(row.apr) > decValue(highest.apr)) highest = row;
      if (!lowest || decValue(row.apr) < decValue(lowest.apr)) lowest = row;
    }
    return { positive, negative, oiTotal, highest, lowest, count: rows.length };
  }, [rows]);

  const onSort = (key: SortKey) =>
    setSort((prev) =>
      prev.key === key
        ? { key, direction: prev.direction === "desc" ? "asc" : "desc" }
        : { key, direction: key === "asset" ? "asc" : "desc" },
    );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Funding — all perps</h1>
          <p className="mt-0.5 text-xs text-ink-muted">
            Per-hour funding rate for every listed Hyperliquid perpetual, as of{" "}
            <span className="num">{formatUtc(current.data?.as_of_ns)}</span>
            {current.data ? (
              <span className="text-ink-faint"> ({formatAgo(current.data.as_of_ns)})</span>
            ) : null}
            .
          </p>
        </div>
        <div className="flex items-center gap-2">
          {current.fetchedAt ? (
            <span className="num text-[11px] text-ink-faint">
              refreshed {formatAgo(msToNs(current.fetchedAt))}
            </span>
          ) : null}
          <Button variant="outline" size="sm" onClick={current.reload} disabled={current.loading}>
            <RefreshCw className={current.loading ? "animate-spin" : undefined} />
            Refresh
          </Button>
        </div>
      </div>

      {coverage.data ? (
        <GapNotice
          coverage={coverage.data}
          windowLabel="the last 24 h of the funding stream"
          windowS={COVERAGE_WINDOW_MS / 1000}
          showList={coverage.data.gaps.length <= 4}
        />
      ) : null}

      {summary ? (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-5">
          <Stat label="Perps listed" value={summary.count} />
          <Stat
            label="Longs paying"
            value={summary.positive}
            hint="positive funding"
            tone="positive"
          />
          <Stat
            label="Shorts paying"
            value={summary.negative}
            hint="negative funding"
            tone="negative"
          />
          <Stat
            label="Highest APR"
            value={
              summary.highest ? (
                <span className={signClass(signOf(summary.highest.rate_hourly))}>
                  {formatPct(summary.highest.apr, { decimals: 2, signed: true })}
                </span>
              ) : (
                "—"
              )
            }
            hint={summary.highest?.asset}
          />
          <Stat
            label="Lowest APR"
            value={
              summary.lowest ? (
                <span className={signClass(signOf(summary.lowest.rate_hourly))}>
                  {formatPct(summary.lowest.apr, { decimals: 2, signed: true })}
                </span>
              ) : (
                "—"
              )
            }
            hint={summary.lowest?.asset}
          />
        </div>
      ) : null}

      <Card>
        <CardHeader className="gap-3">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:w-auto">
              <div className="space-y-1">
                <Label htmlFor="filter-asset">Asset</Label>
                <Input
                  id="filter-asset"
                  value={query}
                  placeholder="BTC"
                  onChange={(event) => setQuery(event.target.value)}
                  autoComplete="off"
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="filter-sign">Direction</Label>
                <Select
                  id="filter-sign"
                  value={signFilter}
                  onChange={(event) => setSignFilter(event.target.value as SignFilter)}
                >
                  <option value="all">All</option>
                  <option value="positive">Positive — longs pay shorts</option>
                  <option value="negative">Negative — shorts pay longs</option>
                </Select>
              </div>
              <div className="space-y-1">
                <Label htmlFor="filter-oi">Min OI notional $</Label>
                <Input
                  id="filter-oi"
                  value={minOi}
                  inputMode="numeric"
                  placeholder="0"
                  onChange={(event) => setMinOi(event.target.value)}
                  autoComplete="off"
                />
              </div>
            </div>
            <SignLegend className="lg:justify-end" />
          </div>
        </CardHeader>

        {current.error ? (
          <CardContent>
            <ErrorState error={current.error} onRetry={current.reload} what="current funding" />
          </CardContent>
        ) : current.loading && rows.length === 0 ? (
          <TableSkeleton cols={7} />
        ) : (
          <div className="overflow-x-auto scrollbar-thin">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <SortHeader columnKey="asset" sort={sort} onSort={onSort}>
                    Asset
                  </SortHeader>
                  <SortHeader
                    columnKey="rate_hourly"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Funding rate for the hour in progress, as a percentage of notional"
                  >
                    Rate / h
                  </SortHeader>
                  <SortHeader
                    columnKey="apr"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Hourly rate x 24 x 365. An extrapolation, not a realised return."
                  >
                    APR
                  </SortHeader>
                  <TableHead>Who pays</TableHead>
                  <SortHeader columnKey="mark" sort={sort} onSort={onSort} align="right">
                    Mark
                  </SortHeader>
                  <SortHeader
                    columnKey="open_interest"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Open interest in coin units, as the venue reports it"
                  >
                    OI (coin)
                  </SortHeader>
                  <SortHeader
                    columnKey="oi_notional"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Open interest x price"
                  >
                    OI notional
                  </SortHeader>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sorted.map((row) => {
                  const sign = signOf(row.rate_hourly);
                  return (
                    <TableRow key={row.asset}>
                      <TableCell className="font-medium">
                        <Link
                          href={`/asset/${encodeURIComponent(row.asset)}`}
                          className="text-ink hover:text-accent hover:underline"
                        >
                          {row.asset}
                        </Link>
                      </TableCell>
                      <TableCell className={`num text-right ${signClass(sign)}`}>
                        {formatPct(row.rate_hourly, { decimals: 5, signed: true })}
                      </TableCell>
                      <TableCell
                        className={`num text-right font-medium ${signClass(sign)}`}
                        title="hourly rate x 24 x 365"
                      >
                        {formatPct(row.apr, { decimals: 2, signed: true })}
                      </TableCell>
                      <TableCell>
                        <PaymentFlow rate={row.rate_hourly} />
                      </TableCell>
                      <TableCell className="num text-right text-ink-muted">
                        {formatDec(row.mark)}
                      </TableCell>
                      <TableCell className="num text-right text-ink-muted">
                        {formatDec(row.open_interest, { maxDecimals: 2 })}
                      </TableCell>
                      <TableCell
                        className="num text-right text-ink-muted"
                        title={`$${formatDec(row.oi_notional, { maxDecimals: 2 })}`}
                      >
                        {formatCompactUsd(row.oi_notional)}
                      </TableCell>
                    </TableRow>
                  );
                })}
                {sorted.length === 0 && !current.loading ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={7} className="py-6 text-center text-xs text-ink-faint">
                      No perp matches these filters.
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>
        )}
        <div className="border-t border-line px-4 py-2 text-[11px] text-ink-faint">
          Showing <span className="num">{sorted.length}</span> of{" "}
          <span className="num">{rows.length}</span> perps. Rates are the figure the venue
          publishes for the hour in progress; the settled rate for a completed hour is on each
          asset page.
        </div>
      </Card>

      <FundingExplainer />
    </div>
  );
}
