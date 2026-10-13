import { ArrowRight } from "lucide-react";

import { cn } from "@/lib/utils";
import { decValue, type Numeric } from "@/lib/num";

export type FundingSign = "positive" | "negative" | "flat";

export function signOf(rate: Numeric | null | undefined): FundingSign {
  const value = decValue(rate);
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "flat";
}

/**
 * Who pays whom. This is the single fact a reader must never have to derive:
 *
 *   positive funding -> LONGS PAY SHORTS  (a short position is paid to hold)
 *   negative funding -> SHORTS PAY LONGS  (a long position is paid to hold)
 *
 * Every surface in this app states it in those words rather than relying on
 * the colour or the minus sign alone.
 */
export const SIGN_COPY: Record<FundingSign, { payer: string; receiver: string; earns: string }> = {
  positive: { payer: "longs", receiver: "shorts", earns: "Shorts are paid" },
  negative: { payer: "shorts", receiver: "longs", earns: "Longs are paid" },
  flat: { payer: "—", receiver: "—", earns: "Nobody is paid" },
};

export function signClass(sign: FundingSign): string {
  if (sign === "positive") return "text-pos";
  if (sign === "negative") return "text-neg";
  return "text-ink-faint";
}

/** Compact "longs → shorts" flow label, for table cells. */
export function PaymentFlow({
  rate,
  className,
}: {
  rate: Numeric | null | undefined;
  className?: string;
}) {
  const sign = signOf(rate);
  if (sign === "flat") {
    return <span className={cn("text-xs text-ink-faint", className)}>no payment</span>;
  }
  const copy = SIGN_COPY[sign];
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 text-xs font-medium",
        signClass(sign),
        className,
      )}
      title={`${copy.payer} pay ${copy.receiver}`}
    >
      {copy.payer}
      <ArrowRight className="size-3 opacity-70" aria-hidden />
      {copy.receiver}
    </span>
  );
}

/** The same fact spelled out in full, for page headers and asset pages. */
export function PaymentDirectionCallout({
  rate,
  className,
}: {
  rate: Numeric | null | undefined;
  className?: string;
}) {
  const sign = signOf(rate);
  const copy = SIGN_COPY[sign];
  const tone =
    sign === "positive"
      ? "border-pos/40 bg-pos-dim/30 text-pos"
      : sign === "negative"
        ? "border-neg/40 bg-neg-dim/30 text-neg"
        : "border-line bg-line/30 text-ink-muted";

  return (
    <div className={cn("rounded-card border px-3 py-2", tone, className)}>
      <div className="text-[11px] uppercase tracking-wider opacity-80">
        Funding is {sign === "flat" ? "flat" : sign}
      </div>
      <div className="mt-0.5 text-sm font-semibold">
        {sign === "flat" ? (
          "No funding is changing hands"
        ) : (
          <>
            {copy.payer} pay {copy.receiver}
          </>
        )}
      </div>
      {sign !== "flat" ? (
        <div className="mt-0.5 text-xs opacity-80">
          {copy.earns} to hold the position, every hour, while this persists.
        </div>
      ) : null}
    </div>
  );
}

/** Legend, shown once above any table that colours by sign. */
export function SignLegend({ className }: { className?: string }) {
  return (
    <div className={cn("flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px]", className)}>
      <span className="flex items-center gap-1.5">
        <span className="size-2 rounded-xs bg-pos" aria-hidden />
        <span className="text-pos font-medium">Positive</span>
        <span className="text-ink-faint">longs pay shorts — a short is paid to hold</span>
      </span>
      <span className="flex items-center gap-1.5">
        <span className="size-2 rounded-xs bg-neg" aria-hidden />
        <span className="text-neg font-medium">Negative</span>
        <span className="text-ink-faint">shorts pay longs — a long is paid to hold</span>
      </span>
    </div>
  );
}
