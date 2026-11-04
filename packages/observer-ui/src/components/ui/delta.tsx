import type { ReactNode } from "react"

export function Delta({ value, children }: { value: number; children: ReactNode }) {
    const tone = value < 0 ? "bg-danger/15 text-danger" : "bg-success/15 text-success"
    return <span className={`rounded-sm px-1.5 py-px text-xs font-medium tabular-nums ${tone}`}>{children}</span>
}
