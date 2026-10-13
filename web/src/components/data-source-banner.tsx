import { DATA_SOURCE } from "@/lib/api/client";

/**
 * Mock mode is a development convenience that must never be mistaken for
 * recorded data, so it announces itself at the top of every page.
 */
export function DataSourceBanner() {
  if (DATA_SOURCE.kind === "live") return null;

  return (
    <div className="border-b border-warn/40 bg-warn-dim/40 px-3 py-1.5 text-center text-[11px] text-warn sm:px-5">
      <span className="font-semibold uppercase tracking-wider">Mock data</span>
      <span className="text-warn/80"> — {DATA_SOURCE.detail}</span>
    </div>
  );
}
