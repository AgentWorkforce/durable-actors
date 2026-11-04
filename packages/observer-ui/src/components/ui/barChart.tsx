import { cn } from "../../lib/utils.js"

export type BarChartProps = {
    values: number[]
    label: string
    highlighted?: number
    onSelect?: (index: number) => void
    barLabel?: (index: number) => string
    className?: string
}

export function BarChart({ values, label, highlighted, onSelect, barLabel, className }: BarChartProps) {
    const peak = Math.max(...values, 1)
    if (!onSelect) {
        return (
            <div role="img" aria-label={label} className={cn("flex items-end gap-1", className)}>
                {values.map((value, index) => (
                    <Bar key={index} share={value / peak} highlighted={index === highlighted} />
                ))}
            </div>
        )
    }
    return (
        <div role="group" aria-label={label} className={cn("flex items-end gap-0.5", className)}>
            {values.map((value, index) => (
                <button
                    key={index}
                    type="button"
                    aria-label={barLabel?.(index)}
                    aria-pressed={index === highlighted}
                    onClick={() => onSelect(index)}
                    className={cn(
                        "group relative flex h-full flex-1 items-end rounded-[2px] outline-none focus-visible:ring-2 focus-visible:ring-ring",
                        index === highlighted && "after:absolute after:inset-x-0 after:-bottom-1 after:h-0.5 after:rounded-full after:bg-foreground"
                    )}
                >
                    <Bar share={value / peak} highlighted={index === highlighted} />
                </button>
            ))}
        </div>
    )
}

function Bar({ share, highlighted }: { share: number; highlighted: boolean }) {
    return (
        <span
            className={cn("block flex-1 rounded-t-[1px]", share ? "bg-muted-foreground/40 group-hover:bg-muted-foreground/70" : "bg-border", highlighted && "bg-foreground group-hover:bg-foreground")}
            style={{ height: share ? `${share * 100}%` : "1px" }}
        />
    )
}
