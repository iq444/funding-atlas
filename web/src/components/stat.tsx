import { cn } from "@/lib/utils";

export function Stat({
  label,
  value,
  hint,
  tone = "default",
  title,
  className,
}: {
  label: string;
  value: React.ReactNode;
  hint?: React.ReactNode;
  tone?: "default" | "positive" | "negative" | "warning" | "muted";
  title?: string;
  className?: string;
}) {
  const toneClass = {
    default: "text-ink",
    positive: "text-pos",
    negative: "text-neg",
    warning: "text-warn",
    muted: "text-ink-muted",
  }[tone];

  return (
    <div
      className={cn("rounded-card border border-line bg-bg-raised px-3 py-2", className)}
      title={title}
    >
      <div className="text-[11px] uppercase tracking-wider text-ink-faint">{label}</div>
      <div className={cn("num mt-0.5 text-lg leading-tight", toneClass)}>{value}</div>
      {hint ? <div className="mt-0.5 text-[11px] text-ink-faint">{hint}</div> : null}
    </div>
  );
}
