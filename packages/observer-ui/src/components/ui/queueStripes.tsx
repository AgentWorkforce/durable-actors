import type { ComponentProps } from "react"

import { cn } from "../../lib/utils.js"

export function QueueStripes({ className, ...props }: ComponentProps<"span">) {
    return (
        <span
            data-slot="queue-stripes"
            className={cn("bg-[repeating-linear-gradient(135deg,var(--chart-1)_0_2px,transparent_2px_4px)] shadow-[inset_0_0_0_1px_color-mix(in_oklab,var(--chart-1)_50%,transparent)]", className)}
            {...props}
        />
    )
}
