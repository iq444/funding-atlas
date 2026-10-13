"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  ColorType,
  LineStyle,
  LineType,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type UTCTimestamp,
  type WhitespaceData,
} from "lightweight-charts";

import type { CoverageGap, FundingHistoryPoint, HistoryBucket } from "@/lib/api/types";
import { decValue } from "@/lib/num";
import { DAY_MS, HOUR_MS, formatUtc, msToNs, nsToMs } from "@/lib/time";
import { cn } from "@/lib/utils";

export type ChartMetric = "apr" | "rate_hourly" | "mark";

interface GapBand {
  key: string;
  startMs: number;
  endMs: number;
  reason: string;
  durationS: number;
}

interface PositionedBand extends GapBand {
  left: number;
  width: number;
}

const METRIC_LABEL: Record<ChartMetric, string> = {
  apr: "Annualised funding (APR %)",
  rate_hourly: "Hourly funding rate (%)",
  mark: "Mark price",
};

function metricValue(point: FundingHistoryPoint, metric: ChartMetric): number {
  if (metric === "mark") return decValue(point.mark);
  // Rates are fractions on the wire; the axis is in percent.
  return decValue(metric === "apr" ? point.apr : point.rate_hourly) * 100;
}

/**
 * Funding history.
 *
 * Two things this chart does that a default line chart does not:
 *
 *  1. The series is built over the complete bucket grid for the requested
 *     range, with a *whitespace* point wherever the recorder has no row. The
 *     line therefore breaks at a gap instead of being interpolated across it.
 *  2. Every recorded gap overlapping the range is drawn as a hatched band, on
 *     the chart itself, labelled with its cause. A smooth line across missing
 *     data would be a false claim about what was observed.
 */
export function FundingChart({
  points,
  gaps,
  metric,
  bucket,
  fromMs,
  toMs,
  height = 320,
  className,
}: {
  points: FundingHistoryPoint[];
  gaps: CoverageGap[];
  metric: ChartMetric;
  bucket: HistoryBucket;
  fromMs: number;
  toMs: number;
  height?: number;
  className?: string;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const [bands, setBands] = useState<PositionedBand[]>([]);
  const [hover, setHover] = useState<{ ms: number; value: number } | null>(null);

  const stepMs = bucket === "1d" ? DAY_MS : HOUR_MS;

  const gapBands = useMemo<GapBand[]>(
    () =>
      gaps
        .map((gap, index) => {
          const gapStart = nsToMs(gap.from_ns);
          const gapEnd = nsToMs(gap.to_ns);
          if (gapStart === null || gapEnd === null) return null;
          const startMs = Math.max(gapStart, fromMs);
          const endMs = Math.min(gapEnd, toMs);
          if (endMs <= startMs) return null;
          return {
            key: `${gapStart}-${index}`,
            startMs,
            endMs,
            reason: gap.reason,
            durationS: decValue(gap.duration_s),
          };
        })
        .filter((band): band is GapBand => band !== null),
    [gaps, fromMs, toMs],
  );

  /**
   * The full bucket grid. Missing buckets become whitespace, which both breaks
   * the line and keeps the time axis linear in time (Lightweight Charts spaces
   * points by index, so an absent bucket would otherwise compress the gap away
   * to nothing and hide it).
   */
  const seriesData = useMemo<(LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>)[]>(() => {
    const byBucket = new Map<number, FundingHistoryPoint>();
    for (const point of points) {
      const ms = nsToMs(point.ts_ns);
      if (ms === null) continue;
      byBucket.set(Math.floor(ms / stepMs) * stepMs, point);
    }

    const out: (LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>)[] = [];
    const first = Math.ceil(fromMs / stepMs) * stepMs;
    const last = Math.floor(toMs / stepMs) * stepMs;
    // Guard against an absurd range producing millions of grid points.
    const maxBuckets = 20_000;
    if ((last - first) / stepMs > maxBuckets) {
      for (const point of points) {
        const ms = nsToMs(point.ts_ns);
        if (ms === null) continue;
        out.push({
          time: Math.floor(ms / 1000) as UTCTimestamp,
          value: metricValue(point, metric),
        });
      }
      return out;
    }

    for (let ts = first; ts <= last; ts += stepMs) {
      const time = Math.floor(ts / 1000) as UTCTimestamp;
      const point = byBucket.get(ts);
      if (point) {
        out.push({ time, value: metricValue(point, metric) });
      } else {
        out.push({ time });
      }
    }
    return out;
  }, [points, metric, stepMs, fromMs, toMs]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart = createChart(container, {
      height,
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#8b90a0",
        fontSize: 11,
        fontFamily:
          'ui-monospace, "SF Mono", SFMono-Regular, Menlo, Monaco, Consolas, monospace',
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.045)" },
        horzLines: { color: "rgba(255,255,255,0.045)" },
      },
      rightPriceScale: { borderColor: "rgba(255,255,255,0.12)" },
      timeScale: {
        borderColor: "rgba(255,255,255,0.12)",
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        vertLine: { color: "#5fa8d3", width: 1, style: LineStyle.Dotted, labelVisible: true },
        horzLine: { color: "#5fa8d3", width: 1, style: LineStyle.Dotted },
      },
      handleScale: { axisPressedMouseMove: { price: false, time: true } },
    });

    const series = chart.addLineSeries({
      color: "#5fa8d3",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerRadius: 3,
      lineType: LineType.Simple,
    });
    series.createPriceLine({
      price: 0,
      color: "rgba(255,255,255,0.22)",
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      axisLabelVisible: false,
      title: "",
    });

    chartRef.current = chart;
    seriesRef.current = series;

    chart.subscribeCrosshairMove((param) => {
      const value = param.seriesData.get(series) as LineData<UTCTimestamp> | undefined;
      if (!param.time || !value || value.value === undefined) {
        setHover(null);
        return;
      }
      setHover({ ms: (param.time as number) * 1000, value: value.value });
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [height]);

  useEffect(() => {
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (!series || !chart) return;
    series.setData(seriesData);
    series.applyOptions({ color: metric === "mark" ? "#c3a0f0" : "#5fa8d3" });
    chart.timeScale().fitContent();
  }, [seriesData, metric]);

  // Position the hatched gap bands over the plot area, and keep them pinned to
  // the time axis as the user pans or zooms.
  useEffect(() => {
    const chart = chartRef.current;
    const container = containerRef.current;
    if (!chart || !container) return;

    const reposition = () => {
      const timeScale = chart.timeScale();
      const width = container.clientWidth;
      const next: PositionedBand[] = [];
      for (const band of gapBands) {
        const startX = timeScale.timeToCoordinate(
          Math.floor(band.startMs / 1000) as UTCTimestamp,
        );
        const endX = timeScale.timeToCoordinate(Math.floor(band.endMs / 1000) as UTCTimestamp);
        // A band whose endpoints are not on the visible axis is skipped rather
        // than guessed at; the gap is still listed in text beside the chart.
        if (startX === null || endX === null) continue;
        const left = Math.max(0, Math.min(startX, endX));
        const right = Math.min(width, Math.max(startX, endX));
        if (right <= 0 || left >= width) continue;
        next.push({ ...band, left, width: Math.max(2, right - left) });
      }
      setBands(next);
    };

    reposition();
    const timeScale = chart.timeScale();
    timeScale.subscribeVisibleTimeRangeChange(reposition);
    const observer = new ResizeObserver(reposition);
    observer.observe(container);

    return () => {
      timeScale.unsubscribeVisibleTimeRangeChange(reposition);
      observer.disconnect();
    };
  }, [gapBands, seriesData]);

  const hasData = seriesData.some((d) => "value" in d);

  return (
    <div className={cn("relative", className)}>
      <div className="mb-1.5 flex flex-wrap items-baseline justify-between gap-2 px-0.5">
        <span className="text-[11px] uppercase tracking-wider text-ink-faint">
          {METRIC_LABEL[metric]}
          <span className="ml-1.5 text-ink-faint/70">· {bucket} buckets · UTC</span>
        </span>
        {hover ? (
          <span className="num text-[11px] text-ink">
            {formatUtc(msToNs(hover.ms))}
            <span className="ml-2 text-accent">
              {metric === "mark" ? hover.value.toFixed(4) : `${hover.value.toFixed(4)}%`}
            </span>
          </span>
        ) : gapBands.length > 0 ? (
          <span className="text-[11px] font-medium text-warn">
            {gapBands.length} gap{gapBands.length === 1 ? "" : "s"} in this range — hatched below
          </span>
        ) : (
          <span className="text-[11px] text-pos">no gaps in this range</span>
        )}
      </div>

      <div className="relative" style={{ height }}>
        <div ref={containerRef} className="absolute inset-0" />

        {/* Gap overlay. Pointer-events pass through except on the label. */}
        <div className="pointer-events-none absolute inset-0 overflow-hidden">
          {bands.map((band) => (
            <div
              key={band.key}
              className="gap-hatch absolute top-0 bottom-[22px]"
              style={{ left: band.left, width: band.width }}
            >
              <span className="pointer-events-auto absolute top-1 left-1 hidden whitespace-nowrap rounded bg-bg/90 px-1 py-0.5 text-[10px] font-medium text-warn sm:inline">
                no data
              </span>
            </div>
          ))}
        </div>

        {!hasData ? (
          <div className="absolute inset-0 flex items-center justify-center text-xs text-ink-faint">
            No recorded funding rows in this range.
          </div>
        ) : null}
      </div>

      {gapBands.length > 0 ? (
        <ul className="mt-2 space-y-0.5 border-t border-line pt-2 text-[11px] text-ink-faint">
          {gapBands.map((band) => (
            <li key={`legend-${band.key}`} className="flex flex-wrap items-baseline gap-x-2">
              <span className="inline-block h-2 w-3 shrink-0 border border-warn/70 bg-warn/20" />
              <span className="num text-warn/90">
                {formatUtc(msToNs(band.startMs))} &rarr;{" "}
                {formatUtc(msToNs(band.endMs))}
              </span>
              <span className="break-words">{band.reason}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
