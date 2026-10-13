"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { cn } from "@/lib/utils";

const NAV = [
  { href: "/", label: "Funding" },
  { href: "/screener", label: "Carry screener" },
  { href: "/status", label: "Recorder status" },
];

export function SiteHeader() {
  const pathname = usePathname();

  return (
    <header className="sticky top-0 z-30 border-b border-line bg-bg/95 backdrop-blur">
      <div className="mx-auto flex max-w-[1600px] flex-col gap-2 px-3 py-2.5 sm:flex-row sm:items-center sm:justify-between sm:gap-6 sm:px-5">
        <div className="flex items-baseline gap-2.5">
          <Link href="/" className="text-sm font-semibold tracking-tight text-ink">
            funding<span className="text-accent">-</span>atlas
          </Link>
          <span className="hidden text-[11px] text-ink-faint sm:inline">
            Hyperliquid perpetual funding, recorded
          </span>
        </div>
        <nav className="-mx-1 flex items-center gap-1 overflow-x-auto scrollbar-thin">
          {NAV.map((item) => {
            const active =
              item.href === "/"
                ? pathname === "/" || pathname.startsWith("/asset/")
                : pathname.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "whitespace-nowrap rounded-md px-2.5 py-1.5 text-xs font-medium transition-colors",
                  active
                    ? "bg-line/60 text-ink"
                    : "text-ink-muted hover:bg-line/35 hover:text-ink",
                )}
              >
                {item.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
