"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export interface QueryState<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
  /** Wall-clock ms when `data` was last replaced. */
  fetchedAt: number | null;
  reload: () => void;
}

/**
 * Minimal fetch-on-mount hook with abort, manual reload and optional polling.
 *
 * The dashboard reads the API from the browser rather than from the Next.js
 * server: it keeps the deployment a static bundle, keeps mock mode a single
 * env var, and means a reader can watch the request in their own devtools —
 * which matters for a dataset whose claim is that it can be checked.
 */
export function useApiQuery<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  deps: readonly unknown[],
  options: { pollMs?: number; enabled?: boolean } = {},
): QueryState<T> {
  const enabled = options.enabled ?? true;
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [fetchedAt, setFetchedAt] = useState<number | null>(null);
  const [nonce, setNonce] = useState(0);

  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const reload = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    let cancelled = false;
    setLoading(true);

    fetcherRef
      .current(controller.signal)
      .then((result) => {
        if (cancelled) return;
        setData(result);
        setError(null);
        setFetchedAt(Date.now());
      })
      .catch((cause: unknown) => {
        if (cancelled) return;
        if (cause instanceof DOMException && cause.name === "AbortError") return;
        setError(cause instanceof Error ? cause : new Error(String(cause)));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce, enabled]);

  const { pollMs } = options;
  useEffect(() => {
    if (!pollMs || !enabled) return;
    const timer = setInterval(() => setNonce((n) => n + 1), pollMs);
    return () => clearInterval(timer);
  }, [pollMs, enabled]);

  return { data, error, loading, fetchedAt, reload };
}
