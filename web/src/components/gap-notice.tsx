import { AlertTriangle, CheckCircle2 } from "lucide-react";

import type { CoverageResponse } from "@/lib/api/types";
import { cn } from "@/lib/utils";
import { decValue, formatDuration, formatPct } from "@/lib/num";
import { formatUtc } from "@/lib/time";

/**
 * Is the window whole?
 *
 * `covered` arrives as a boolean from the API and could reasonably have been a
 * 0..1 fraction, so both are handled here and nowhere else. Either way a
 * non-empty gap list or any missing seconds means not whole — the gaps are the
 * primary evidence and the flag is a summary of them.
 */
export function coverageIsComplete(coverage: CoverageResponse | null): boolean {
  if (!coverage) return false;
  if (coverage.gaps.length > 0) return false;
  if (decValue(coverage.missing_s) > 0) return false;
  if (typeof coverage.covered === "boolean") return coverage.covered;
  return decValue(coverage.covered) >= 1;
}

/**
 * Covered fraction for display, 0..1, or null when it cannot be known.
 * Preferring `windowS` (the span the caller actually asked for) over the
 * `covered` field means the figure is derived from the same seconds the gap
 * list is measured in, whichever shape the field takes.
 */
export function coverageFraction(
  coverage: CoverageResponse,
  windowS?: number,
): number | null {
  if (windowS && windowS > 0) {
    return Math.max(0, Math.min(1, 1 - decValue(coverage.missing_s) / windowS));
  }
  if (typeof coverage.covered === "boolean") return coverage.covered ? 1 : null;
  const value = decValue(coverage.covered);
  return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : null;
}

/**
 * Coverage for a window, stated plainly. A range containing a gap says so
 * wherever the range is used — a chart or a table that quietly spans a hole in
 * the record is the one thing this dataset cannot afford to ship.
 */
export function GapNotice({
  coverage,
  windowLabel,
  windowS,
  className,
  showList = true,
}: {
  coverage: CoverageResponse;
  windowLabel: string;
  /** Length of the requested window in seconds, used to derive the percentage. */
  windowS?: number;
  className?: string;
  showList?: boolean;
}) {
  const complete = coverageIsComplete(coverage);
  const fraction = coverageFraction(coverage, windowS);

  if (complete) {
    return (
      <div
        className={cn(
          "flex items-center gap-2 rounded-card border border-pos/30 bg-pos-dim/20 px-3 py-2 text-xs text-pos",
          className,
        )}
      >
        <CheckCircle2 className="size-3.5 shrink-0" />
        <span>
          Continuous record over {windowLabel} — no recorded gaps
          {fraction === null ? null : (
            <>
              , coverage <span className="num">{formatPct(fraction, { decimals: 3 })}</span>
            </>
          )}
          .
        </span>
      </div>
    );
  }

  return (
    <div
      className={cn(
        "rounded-card border border-warn/40 bg-warn-dim/25 px-3 py-2 text-xs",
        className,
      )}
    >
      <div className="flex items-start gap-2 text-warn">
        <AlertTriangle className="mt-0.5 size-3.5 shrink-0" />
        <div>
          <span className="font-semibold">
            {coverage.gaps.length} gap{coverage.gaps.length === 1 ? "" : "s"} in {windowLabel}
          </span>{" "}
          — <span className="num">{formatDuration(coverage.missing_s)}</span> of this window has
          no recorded data
          {fraction === null ? null : (
            <>
              . Coverage <span className="num">{formatPct(fraction, { decimals: 3 })}</span>
            </>
          )}
          .
        </div>
      </div>
      {showList ? (
        <ul className="mt-2 space-y-1 border-t border-warn/25 pt-2">
          {coverage.gaps.map((gap, index) => (
            <li
              key={`${String(gap.from_ns)}-${index}`}
              className="flex flex-col gap-x-2 gap-y-0.5 text-ink-muted sm:flex-row sm:items-baseline"
            >
              <span className="num shrink-0 text-warn/90">
                {formatUtc(gap.from_ns)} &rarr; {formatUtc(gap.to_ns)}
              </span>
              <span className="num shrink-0 text-ink-faint">
                ({formatDuration(gap.duration_s)})
              </span>
              <span className="break-words text-ink-faint">{gap.reason}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
