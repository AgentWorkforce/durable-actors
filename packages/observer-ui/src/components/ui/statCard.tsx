import { type ReactNode, useId } from "react"

import { cn } from "../../lib/utils.js"

export type StatCardProps = {
    label: string
    aside?: ReactNode
    value?: ReactNode
    unit?: ReactNode
    busy?: boolean
    children?: ReactNode
}

export function StatCard({ label, aside, value, unit, busy, children }: StatCardProps) {
    const labelId = useId()
    return (
        <article
            aria-labelledby={labelId}
            aria-busy={busy || undefined}
            className="flex min-h-59 min-w-0 flex-col rounded-xl border bg-card px-5.5 pt-5 pb-4.5 text-card-foreground shadow-(--shadow-control)"
        >
            <div className="flex items-center justify-between gap-2 text-[13px] font-medium text-muted-foreground">
                <span id={labelId} className="truncate">
                    {label}
                </span>
                {aside}
            </div>
            {value !== undefined && <StatFigure value={value} unit={unit} className="mt-3.5" />}
            {children}
        </article>
    )
}

export function StatFigure({ value, unit, className }: { value: ReactNode; unit?: ReactNode; className?: string }) {
    return (
        <div className={cn("flex items-baseline gap-2 text-[44px] leading-none font-bold tracking-[-0.035em] tabular-nums", className)}>
            {value}
            {unit !== undefined && <div className="text-[15px] font-medium tracking-normal text-muted-foreground">{unit}</div>}
        </div>
    )
}
