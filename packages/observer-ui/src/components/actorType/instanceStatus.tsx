import type { ReactNode } from "react"

import { cn } from "../../lib/utils.js"

export function InstanceStatus({ live, className, children }: { live: boolean; className?: string; children: ReactNode }) {
    return (
        <span className={cn("inline-flex items-center gap-1.25", live ? "text-success" : "text-muted-foreground", className)}>
            {live && <span className="size-1.5 rounded-full bg-success" />}
            {children}
        </span>
    )
}
