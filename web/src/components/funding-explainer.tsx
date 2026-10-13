import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { SignLegend } from "@/components/funding-sign";

/**
 * Most visitors will not know what funding is. This is the whole explanation,
 * with no promotional claim in it, and the numbers in it are Hyperliquid's
 * published mechanics (hourly settlement, 0.01%/8h interest component,
 * +/-0.05% clamp, 4%/h cap) rather than anything this project measured.
 */
export function FundingExplainer() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>What funding is</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 text-xs leading-relaxed text-ink-muted">
        <p>
          A perpetual future never expires, so nothing forces its price to converge on the
          underlying. <span className="text-ink">Funding</span> is the mechanism that does the
          forcing: a periodic payment between the traders themselves — not to the exchange —
          sized so that whichever side is pushing the perp away from the oracle price pays the
          other side to take the opposite position.
        </p>
        <p>
          On Hyperliquid funding settles <span className="text-ink">every hour</span>, and the
          rate quoted everywhere in this dashboard is the{" "}
          <span className="text-ink">per-hour</span> rate. The annualised figure is simply{" "}
          <span className="num text-ink">rate &times; 24 &times; 365</span>; it is a projection
          of the current rate, not a return anyone received. Payment is on position size times
          the <span className="text-ink">oracle</span> price.
        </p>
        <div className="rounded-md border border-line bg-bg-inset px-3 py-2">
          <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-ink-faint">
            The sign is the whole trade
          </div>
          <SignLegend />
        </div>
        <p>
          Collecting funding is not free money. The rate is paid hourly but the position is
          exposed to price the whole time, you pay fees to get in and out, and a large enough
          move liquidates you regardless of what funding did. The{" "}
          <span className="text-ink">carry screener</span> exists to put the fee side of that
          arithmetic on screen; it cannot put the risk side there.
        </p>
        <p className="text-ink-faint">
          Mechanics, for reference: the rate is the average premium index plus interest, clamped
          to &plusmn;0.05%, capped at 4% per hour, with the premium sampled every 5 seconds over
          the hour. Because the in-progress rate moves as the premium accumulates, the path it
          takes within the hour only exists if something recorded it.
        </p>
      </CardContent>
    </Card>
  );
}
