import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * A native select, styled to match. Native is deliberate: on a phone it opens
 * the platform picker, which beats any custom popover for a dense data tool.
 */
const Select = React.forwardRef<HTMLSelectElement, React.SelectHTMLAttributes<HTMLSelectElement>>(
  ({ className, children, ...props }, ref) => (
    <select
      ref={ref}
      className={cn(
        "h-9 w-full appearance-none rounded-md border border-line-strong bg-bg-inset px-2.5 pr-8 text-sm text-ink",
        "bg-[url('data:image/svg+xml;utf8,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 16 16%22 fill=%22none%22 stroke=%22%238b90a0%22 stroke-width=%221.5%22><path d=%22M4 6.5l4 4 4-4%22/></svg>')] bg-[length:14px] bg-[position:right_0.5rem_center] bg-no-repeat",
        "focus-visible:border-accent disabled:opacity-50",
        className,
      )}
      {...props}
    >
      {children}
    </select>
  ),
);
Select.displayName = "Select";

export { Select };
