import type { Metadata } from "next";

import "./globals.css";
import { SiteHeader } from "@/components/site-header";
import { DataSourceBanner } from "@/components/data-source-banner";
import { DATA_SOURCE } from "@/lib/api/client";

export const metadata: Metadata = {
  title: {
    default: "funding-atlas — Hyperliquid perpetual funding",
    template: "%s — funding-atlas",
  },
  description:
    "An open, continuously recorded history of Hyperliquid perpetual funding rates, with gaps in the record published alongside the data.",
  robots: { index: true, follow: true },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-dvh bg-bg text-ink antialiased">
        <DataSourceBanner />
        <SiteHeader />
        <main className="mx-auto w-full max-w-[1600px] px-3 py-4 sm:px-5 sm:py-6">
          {children}
        </main>
        <footer className="mx-auto w-full max-w-[1600px] border-t border-line px-3 py-5 text-[11px] leading-relaxed text-ink-faint sm:px-5">
          <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between">
            <span>
              Data source:{" "}
              <span className={DATA_SOURCE.kind === "mock" ? "text-warn" : "text-ink-muted"}>
                {DATA_SOURCE.label}
              </span>{" "}
              <span className="num">({DATA_SOURCE.detail})</span>
            </span>
            <span>Times are UTC. Rates are per hour unless labelled APR.</span>
          </div>
          <p className="mt-2 max-w-3xl">
            funding-atlas is research and data infrastructure. Nothing here is investment advice
            or an offer to trade, no figure on this site is a prediction, and an annualised rate
            is an extrapolation of a rate observed now — not a return anyone received.
          </p>
        </footer>
      </body>
    </html>
  );
}
