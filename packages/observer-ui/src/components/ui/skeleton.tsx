import type { ComponentProps } from "react"

import { cn } from "../../lib/utils.js"

export function Skeleton({ className, ...props }: ComponentProps<"div">) {
    return <div data-slot="skeleton" className={cn("animate-pulse rounded-md bg-muted motion-reduce:animate-none", className)} {...props} />
}
