import type { ReactNode } from "react"

export type SegmentedOption<T extends string> = {
    value: T
    label: string
    icon?: ReactNode
    badge?: ReactNode
}

export type SegmentedControlProps<T extends string> = {
    label: string
    options: SegmentedOption<T>[]
    value: T
    onChange: (value: T) => void
}

export function SegmentedControl<T extends string>({ label, options, value, onChange }: SegmentedControlProps<T>) {
    return (
        <div role="group" aria-label={label} className="flex gap-0.5 rounded-lg border bg-background/40 p-0.5">
            {options.map(option => (
                <button
                    key={option.value}
                    type="button"
                    aria-pressed={option.value === value}
                    onClick={() => onChange(option.value)}
                    className="flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-xs font-medium text-muted-foreground transition-colors outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring aria-pressed:bg-muted aria-pressed:text-foreground aria-pressed:shadow-[inset_0_0_0_1px_var(--border)]"
                >
                    {option.icon}
                    {option.label}
                    {option.badge !== undefined && <span className="text-[11px] tabular-nums opacity-75">{option.badge}</span>}
                </button>
            ))}
        </div>
    )
}
