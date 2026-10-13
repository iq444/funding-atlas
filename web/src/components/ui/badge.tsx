import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[11px] font-medium leading-none whitespace-nowrap",
  {
    variants: {
      variant: {
        neutral: "border-line-strong bg-line/40 text-ink-muted",
        positive: "border-pos/40 bg-pos-dim/50 text-pos",
        negative: "border-neg/40 bg-neg-dim/50 text-neg",
        warning: "border-warn/40 bg-warn-dim/50 text-warn",
        accent: "border-accent/40 bg-accent-dim/50 text-accent",
        outline: "border-line-strong text-ink-faint",
      },
    },
    defaultVariants: { variant: "neutral" },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge, badgeVariants };
