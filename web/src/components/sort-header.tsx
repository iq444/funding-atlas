"use client";

import { ChevronDown, ChevronUp } from "lucide-react";

import { TableHead } from "@/components/ui/table";
import { cn } from "@/lib/utils";

export type SortDirection = "asc" | "desc";

export interface SortState<K extends string> {
  key: K;
  direction: SortDirection;
}

export function SortHeader<K extends string>({
  columnKey,
  sort,
  onSort,
  align = "left",
  className,
  title,
  children,
}: {
  columnKey: K;
  sort: SortState<K>;
  onSort: (key: K) => void;
  align?: "left" | "right";
  className?: string;
  title?: string;
  children: React.ReactNode;
}) {
  const active = sort.key === columnKey;
  return (
    <TableHead
      className={cn(align === "right" && "text-right", "p-0", className)}
      aria-sort={active ? (sort.direction === "asc" ? "ascending" : "descending") : "none"}
    >
      <button
        type="button"
        onClick={() => onSort(columnKey)}
        title={title}
        className={cn(
          "flex w-full items-center gap-1 px-2.5 py-2 text-[11px] font-semibold uppercase tracking-wider transition-colors hover:text-ink",
          align === "right" && "justify-end",
          active ? "text-ink" : "text-ink-faint",
        )}
      >
        {align === "right" && active ? (
          sort.direction === "asc" ? (
            <ChevronUp className="size-3" />
          ) : (
            <ChevronDown className="size-3" />
          )
        ) : null}
        <span>{children}</span>
        {align === "left" && active ? (
          sort.direction === "asc" ? (
            <ChevronUp className="size-3" />
          ) : (
            <ChevronDown className="size-3" />
          )
        ) : null}
      </button>
    </TableHead>
  );
}
