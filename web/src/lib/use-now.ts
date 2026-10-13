"use client";

import { useEffect, useState } from "react";

/**
 * Wall clock, available only after mount.
 *
 * Every range in this app ends at "now", and reading the clock during
 * server rendering produces a timestamp that is already stale by the time the
 * client hydrates — React then reports a hydration mismatch, and worse, the
 * server and client would be describing two different windows of the record.
 * So the clock is read once on the client and the ranges derive from that.
 */
export function useMountedNow(): number | null {
  const [now, setNow] = useState<number | null>(null);
  useEffect(() => {
    setNow(Date.now());
  }, []);
  return now;
}
