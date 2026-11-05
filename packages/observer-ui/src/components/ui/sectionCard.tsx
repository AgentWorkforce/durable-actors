import { type ReactNode, useId } from "react"

import { cn } from "../../lib/utils.js"

export type SectionCardProps = {
    id?: string
    title: string
    description?: ReactNode
    aside?: ReactNode
    busy?: boolean
    className?: string
    children: ReactNode
}

export function SectionCard({ id, title, description, aside, busy, className, children }: SectionCardProps) {
    const titleId = useId()
    return (
        <section id={id} aria-labelledby={titleId} aria-busy={busy || undefined} className={cn("rounded-xl border bg-card px-5.5 pt-5 text-card-foreground shadow-(--shadow-control)", className)}>
            <header className="flex flex-wrap items-end justify-between gap-3">
                <div>
                    <h2 id={titleId} className="text-base font-bold tracking-tight">
                        {title}
                    </h2>
                    {description && <p className="mt-0.75 text-[13px] text-muted-foreground">{description}</p>}
                </div>
                {aside}
            </header>
            {children}
        </section>
    )
}
