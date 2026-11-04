import type { ReactNode } from "react"
import { Link, type RelativeRoutingType } from "react-router"

import { ArrowLeft } from "lucide-react"

export type PageHeaderProps = {
    titleId: string
    title: string
    back: { to: string; label: string; relative?: RelativeRoutingType }
    badge?: ReactNode
    meta: ReactNode
    aside?: ReactNode
}

export function PageHeader({ titleId, title, back, badge, meta, aside }: PageHeaderProps) {
    return (
        <header className="flex flex-wrap items-end justify-between gap-4">
            <div className="min-w-0">
                <Link to={back.to} relative={back.relative} className="inline-flex items-center gap-1.5 text-[13px] font-medium text-muted-foreground hover:text-foreground">
                    <ArrowLeft className="size-3.5" aria-hidden />
                    {back.label}
                </Link>
                <div className="mt-2.5 flex flex-wrap items-center gap-3">
                    <h1 id={titleId} translate="no" className="truncate text-[26px] font-medium tracking-tight">
                        {title}
                    </h1>
                    {badge}
                </div>
                <div className="mt-1 flex flex-wrap items-center gap-2 text-[13px] text-muted-foreground tabular-nums">{meta}</div>
            </div>
            {aside}
        </header>
    )
}
