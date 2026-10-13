import { AlertTriangle, Database } from "lucide-react";

import { ApiError } from "@/lib/api/client";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";

export function ErrorState({
  error,
  onRetry,
  what,
}: {
  error: Error;
  onRetry?: () => void;
  what: string;
}) {
  const status = error instanceof ApiError ? error.status : null;
  return (
    <div className="flex flex-col items-start gap-2 rounded-card border border-neg/40 bg-neg-dim/25 px-4 py-3">
      <div className="flex items-center gap-2 text-sm font-medium text-neg">
        <AlertTriangle className="size-4" />
        Could not load {what}
        {status ? <span className="num text-xs text-neg/70">HTTP {status}</span> : null}
      </div>
      <p className="text-xs text-ink-muted">{error.message}</p>
      {onRetry ? (
        <Button variant="outline" size="sm" onClick={onRetry}>
          Retry
        </Button>
      ) : null}
    </div>
  );
}

export function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 rounded-card border border-line bg-bg-inset px-4 py-6 text-xs text-ink-faint">
      <Database className="size-4" />
      {children}
    </div>
  );
}

export function TableSkeleton({ rows = 8, cols = 6 }: { rows?: number; cols?: number }) {
  return (
    <div className="space-y-1.5 p-3">
      {Array.from({ length: rows }).map((_, rowIndex) => (
        <div key={rowIndex} className="flex gap-2">
          {Array.from({ length: cols }).map((_, colIndex) => (
            <Skeleton
              key={colIndex}
              className="h-5 flex-1"
              style={{ opacity: 1 - rowIndex * 0.08 }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}
