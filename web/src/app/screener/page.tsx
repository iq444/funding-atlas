"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { AlertTriangle, Info } from "lucide-react";

import { api } from "@/lib/api/client";
import type { AssumptionValue, ScreenerRow, Side } from "@/lib/api/types";
import { useApiQuery } from "@/lib/use-api";
import {
  decValue,
  formatCompactUsd,
  formatDec,
  formatHours,
  formatPct,
  formatUsd,
  parseDec,
} from "@/lib/num";
import { formatUtc } from "@/lib/time";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { SortHeader, type SortState } from "@/components/sort-header";
import { Stat } from "@/components/stat";

type SortKey = "asset" | "net_apr" | "apr" | "breakeven_hours" | "net_pnl" | "oi_notional";

const NOTIONAL_PRESETS = [1_000, 10_000, 100_000, 1_000_000];
const HOLDING_PRESETS = [1, 8, 24, 72, 168, 720];

/** Keys the dashboard knows how to label; anything else is shown verbatim. */
const ASSUMPTION_LABELS: Record<string, string> = {
  notional: "Notional per position",
  holding_hours: "Holding period",
  side: "Side",
  taker_fee_rate: "Taker fee",
  maker_fee_rate: "Maker fee",
  taker_fee_bps: "Taker fee",
  maker_fee_bps: "Maker fee",
  slippage_bps: "Slippage allowance",
  fee_role: "Fee role assumed",
  legs: "Legs",
  fills: "Fills charged",
  fee_model: "Fee model",
  round_trip_fee_cost: "Round-trip fee cost",
  funding_interval_s: "Funding interval",
  hours_per_year: "Hours per year",
  annualisation: "Annualisation",
  fee_source: "Fee source",
  rate_source: "Rate source",
  rate_as_of_ns: "Rate as of",
  as_of_ns: "Rates as of",
  leverage: "Leverage",
  borrow_cost_included: "Borrow cost included",
};

/**
 * Assumptions are rendered whatever they are: the contract declares the block
 * free-form, so an unrecognised key is shown under its own name rather than
 * dropped. Dropping one would hide exactly the assumption nobody thought to
 * document.
 */
function formatAssumption(key: string, value: AssumptionValue): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) {
    if (value.length === 0) return "—";
    const rendered = value.map((item) => formatDec(item, { maxDecimals: 8 })).join(", ");
    return key.endsWith("_s") ? `${rendered} s` : rendered;
  }
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "string" && !/^[+-]?(\d|\.)/.test(value.trim())) return value;
  if (key.endsWith("_ns")) return formatUtc(value);
  if (key.endsWith("_bps")) return `${formatDec(value, { maxDecimals: 4 })} bps`;
  if (key.endsWith("_fee_rate") || key === "fee_rate") {
    return `${formatPct(value, { decimals: 4 })} (${formatDec(
      String(decValue(value) * 10_000),
      { maxDecimals: 4 },
    )} bps)`;
  }
  if (key === "holding_hours") return formatHours(value);
  if (key === "funding_interval_s") return `${formatDec(value)} s`;
  if (key === "notional" || key.endsWith("_cost") || key.endsWith("_pnl")) return formatUsd(value);
  return formatDec(value, { maxDecimals: 8 });
}

export default function ScreenerPage() {
  const [notionalInput, setNotionalInput] = useState("10000");
  const [holdingInput, setHoldingInput] = useState("24");
  const [side, setSide] = useState<Side>("short");
  const [sort, setSort] = useState<SortState<SortKey>>({ key: "net_apr", direction: "desc" });
  const [onlyViable, setOnlyViable] = useState(false);

  // Debounced so typing a notional does not fire a request per keystroke.
  const [applied, setApplied] = useState({ notional: 10_000, holding_hours: 24 });
  useEffect(() => {
    const notional = Number(notionalInput);
    const holding = Number(holdingInput);
    const timer = setTimeout(() => {
      setApplied({
        notional: Number.isFinite(notional) && notional > 0 ? notional : 10_000,
        holding_hours: Number.isFinite(holding) && holding > 0 ? holding : 24,
      });
    }, 400);
    return () => clearTimeout(timer);
  }, [notionalInput, holdingInput]);

  const screener = useApiQuery(
    (signal) =>
      api.screener(
        { notional: applied.notional, holding_hours: applied.holding_hours, side },
        signal,
      ),
    [applied.notional, applied.holding_hours, side],
  );

  const rows = useMemo(() => screener.data?.rows ?? [], [screener.data]);
  const assumptions = screener.data?.assumptions ?? null;

  const visible = useMemo(
    () => (onlyViable ? rows.filter((row) => row.covers_fees) : rows),
    [rows, onlyViable],
  );

  const sorted = useMemo(() => {
    const factor = sort.direction === "asc" ? 1 : -1;
    const pick = (row: ScreenerRow): number | string => {
      switch (sort.key) {
        case "asset":
          return row.asset;
        case "net_apr":
          return decValue(row.net_apr);
        case "apr":
          return decValue(row.apr);
        case "net_pnl":
          return decValue(row.net_pnl);
        case "oi_notional":
          return decValue(row.oi_notional);
        case "breakeven_hours": {
          const parsed = parseDec(row.breakeven_hours);
          // "Never breaks even" sorts last whichever way the column is pointed.
          return parsed === null || parsed.value < 0
            ? sort.direction === "asc"
              ? Number.POSITIVE_INFINITY
              : Number.NEGATIVE_INFINITY
            : parsed.value;
        }
      }
    };
    return [...visible].sort((a, b) => {
      const left = pick(a);
      const right = pick(b);
      if (typeof left === "string" || typeof right === "string") {
        return factor * String(left).localeCompare(String(right));
      }
      return factor * (left - right);
    });
  }, [visible, sort]);

  const viableCount = rows.filter((row) => row.covers_fees).length;
  const onSort = (key: SortKey) =>
    setSort((prev) =>
      prev.key === key
        ? { key, direction: prev.direction === "desc" ? "asc" : "desc" }
        : { key, direction: key === "asset" || key === "breakeven_hours" ? "asc" : "desc" },
    );

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">Carry screener</h1>
        <p className="mt-0.5 max-w-3xl text-xs text-ink-muted">
          Ranks every perp by what the funding carry is worth{" "}
          <span className="text-ink">after fees</span> for a stated notional, holding period and
          side. The headline APR is not the decision — the{" "}
          <span className="text-ink">breakeven hours</span> is: it is how long the position has
          to survive before funding has paid for the round trip.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Position</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-1">
              <Label htmlFor="notional">Notional (USD)</Label>
              <Input
                id="notional"
                inputMode="decimal"
                value={notionalInput}
                onChange={(event) => setNotionalInput(event.target.value)}
              />
              <div className="flex gap-1">
                {NOTIONAL_PRESETS.map((preset) => (
                  <Button
                    key={preset}
                    size="sm"
                    variant={Number(notionalInput) === preset ? "default" : "subtle"}
                    onClick={() => setNotionalInput(String(preset))}
                  >
                    {formatCompactUsd(preset)}
                  </Button>
                ))}
              </div>
            </div>

            <div className="space-y-1">
              <Label htmlFor="holding">Holding period (hours)</Label>
              <Input
                id="holding"
                inputMode="decimal"
                value={holdingInput}
                onChange={(event) => setHoldingInput(event.target.value)}
              />
              <div className="flex flex-wrap gap-1">
                {HOLDING_PRESETS.map((preset) => (
                  <Button
                    key={preset}
                    size="sm"
                    variant={Number(holdingInput) === preset ? "default" : "subtle"}
                    onClick={() => setHoldingInput(String(preset))}
                  >
                    {preset >= 168 ? `${preset / 168}w` : preset >= 24 ? `${preset / 24}d` : `${preset}h`}
                  </Button>
                ))}
              </div>
            </div>

            <div className="space-y-1">
              <Label htmlFor="side">Side</Label>
              <Select
                id="side"
                value={side}
                onChange={(event) => setSide(event.target.value as Side)}
              >
                <option value="short">Short the perp — paid when funding is positive</option>
                <option value="long">Long the perp — paid when funding is negative</option>
              </Select>
              <p className="text-[11px] text-ink-faint">
                {side === "short"
                  ? "A short collects funding while longs are paying, and carries the price risk of being short."
                  : "A long collects funding while shorts are paying, and carries the price risk of being long."}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {assumptions ? (
        <Card>
          <CardHeader className="flex-row items-center gap-2">
            <Info className="size-3.5 text-ink-faint" />
            <CardTitle>Assumptions behind every number below</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-1 gap-x-6 gap-y-1.5 sm:grid-cols-2 lg:grid-cols-3">
              {Object.entries(assumptions).map(([key, value]) => (
                <div
                  key={key}
                  className="flex items-baseline justify-between gap-3 border-b border-line/60 pb-1"
                >
                  <dt className="text-[11px] text-ink-faint">
                    {ASSUMPTION_LABELS[key] ?? key}
                  </dt>
                  <dd className="num text-right text-xs text-ink">
                    {formatAssumption(key, value)}
                  </dd>
                </div>
              ))}
            </dl>
            <p className="mt-3 text-[11px] leading-relaxed text-ink-faint">
              These come from the API, not from this page. Net figures assume the current
              in-progress hourly rate persists for the whole holding period, which it will not:
              funding is re-set every hour. Price risk, liquidation and borrow costs are not
              modelled here at all.
            </p>
          </CardContent>
        </Card>
      ) : null}

      {screener.data ? (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          <Stat label="Perps screened" value={rows.length} />
          <Stat
            label="Cover fees"
            value={viableCount}
            tone={viableCount > 0 ? "positive" : "muted"}
            hint={`at ${formatHours(applied.holding_hours)}`}
          />
          <Stat
            label="Do not cover fees"
            value={rows.length - viableCount}
            tone="negative"
            hint="net of round trip"
          />
          <Stat
            label="Best net APR"
            value={
              sorted.length > 0 && rows[0]
                ? formatPct(
                    rows.reduce(
                      (best, row) => (decValue(row.net_apr) > decValue(best.net_apr) ? row : best),
                      rows[0],
                    ).net_apr,
                    { decimals: 2, signed: true },
                  )
                : "—"
            }
            tone="positive"
          />
        </div>
      ) : null}

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Ranked by net APR after fees</CardTitle>
          <label className="flex items-center gap-1.5 text-[11px] text-ink-muted">
            <input
              type="checkbox"
              checked={onlyViable}
              onChange={(event) => setOnlyViable(event.target.checked)}
              className="size-3.5 accent-[#5fa8d3]"
            />
            Hide rows that do not cover fees
          </label>
        </CardHeader>

        {screener.error ? (
          <CardContent>
            <ErrorState error={screener.error} onRetry={screener.reload} what="the screener" />
          </CardContent>
        ) : screener.loading && rows.length === 0 ? (
          <TableSkeleton cols={8} />
        ) : (
          <div className="overflow-x-auto scrollbar-thin">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <SortHeader columnKey="asset" sort={sort} onSort={onSort}>
                    Asset
                  </SortHeader>
                  <SortHeader
                    columnKey="breakeven_hours"
                    sort={sort}
                    onSort={onSort}
                    title="Hours of funding required to pay off the round-trip fee"
                  >
                    Breakeven
                  </SortHeader>
                  <SortHeader
                    columnKey="net_apr"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Net P&L annualised over the holding period"
                  >
                    Net APR
                  </SortHeader>
                  <SortHeader
                    columnKey="apr"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Funding APR before fees"
                  >
                    Gross APR
                  </SortHeader>
                  <TableHead className="text-right">Gross P&amp;L</TableHead>
                  <TableHead className="text-right">Fees</TableHead>
                  <SortHeader columnKey="net_pnl" sort={sort} onSort={onSort} align="right">
                    Net P&amp;L
                  </SortHeader>
                  <SortHeader
                    columnKey="oi_notional"
                    sort={sort}
                    onSort={onSort}
                    align="right"
                    title="Open interest notional — a crude capacity proxy"
                  >
                    OI notional
                  </SortHeader>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sorted.map((row) => (
                  <ScreenerTableRow
                    key={row.asset}
                    row={row}
                    holdingHours={applied.holding_hours}
                  />
                ))}
                {sorted.length === 0 && !screener.loading ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={8} className="py-6 text-center text-xs text-ink-faint">
                      {onlyViable
                        ? "No perp covers its round-trip fee at this notional and holding period."
                        : "The screener returned no rows."}
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>
        )}

        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-line px-4 py-2 text-[11px] text-ink-faint">
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-xs border border-neg/50 bg-neg-dim/50" />
            Row does not cover fees over this holding period
          </span>
          <span>
            Breakeven longer than the holding period means the fee is never repaid, however
            attractive the APR looks.
          </span>
        </div>
      </Card>
    </div>
  );
}

function ScreenerTableRow({
  row,
  holdingHours,
}: {
  row: ScreenerRow;
  holdingHours: number;
}) {
  const breakeven = parseDec(row.breakeven_hours);
  const neverBreaksEven = breakeven === null || breakeven.value <= 0;
  const ratio = neverBreaksEven ? Infinity : breakeven.value / Math.max(holdingHours, 1e-9);
  const covers = row.covers_fees;

  return (
    <TableRow
      className={cn(
        covers ? undefined : "bg-neg-dim/20 text-ink-faint",
        !covers && "[&_td]:opacity-80",
      )}
    >
      <TableCell className="font-medium">
        <div className="flex items-center gap-2">
          <Link
            href={`/asset/${encodeURIComponent(row.asset)}`}
            className={cn("hover:underline", covers ? "text-ink hover:text-accent" : "text-ink-muted")}
          >
            {row.asset}
          </Link>
          {!covers ? (
            <Badge variant="negative" title="Net P&L after the round-trip fee is not positive">
              <AlertTriangle className="size-2.5" />
              fees not covered
            </Badge>
          ) : null}
        </div>
      </TableCell>

      {/* Breakeven: the number that decides it, with its own scale bar. */}
      <TableCell className="min-w-[168px]">
        <div className="flex items-center gap-2">
          <span
            className={cn(
              "num w-16 shrink-0 text-sm font-semibold",
              neverBreaksEven ? "text-neg" : ratio <= 0.5 ? "text-pos" : ratio <= 1 ? "text-warn" : "text-neg",
            )}
            title={
              neverBreaksEven
                ? "Funding runs against this side — the fee is never repaid"
                : `${formatDec(row.breakeven_hours, { maxDecimals: 3 })} hours of funding to repay the round-trip fee`
            }
          >
            {neverBreaksEven ? "never" : formatHours(row.breakeven_hours)}
          </span>
          <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-line/70" title={`holding period: ${formatHours(holdingHours)}`}>
            <div
              className={cn(
                "h-full rounded-full",
                neverBreaksEven ? "bg-neg" : ratio <= 0.5 ? "bg-pos" : ratio <= 1 ? "bg-warn" : "bg-neg",
              )}
              style={{ width: `${Math.min(100, neverBreaksEven ? 100 : ratio * 100)}%` }}
            />
          </div>
          <span className="w-10 shrink-0 text-right text-[10px] text-ink-faint">
            {neverBreaksEven ? "" : `${(ratio * 100).toFixed(0)}%`}
          </span>
        </div>
      </TableCell>

      <TableCell
        className={cn(
          "num text-right font-semibold",
          decValue(row.net_apr) > 0 ? "text-pos" : "text-neg",
        )}
      >
        {formatPct(row.net_apr, { decimals: 2, signed: true })}
      </TableCell>
      <TableCell className="num text-right text-ink-muted">
        {formatPct(row.apr, { decimals: 2, signed: true })}
      </TableCell>
      <TableCell className="num text-right text-ink-muted">
        {formatUsd(row.gross_pnl, { signed: true })}
      </TableCell>
      <TableCell className="num text-right text-neg/80">
        -{formatUsd(row.fee_cost).replace(/^-/, "")}
      </TableCell>
      <TableCell
        className={cn(
          "num text-right font-medium",
          decValue(row.net_pnl) > 0 ? "text-pos" : "text-neg",
        )}
      >
        {formatUsd(row.net_pnl, { signed: true })}
      </TableCell>
      <TableCell
        className="num text-right text-ink-faint"
        title={`$${formatDec(row.oi_notional, { maxDecimals: 2 })}`}
      >
        {formatCompactUsd(row.oi_notional)}
      </TableCell>
    </TableRow>
  );
}
