import { formatCount, formatPlural } from "../../format.js"

export type ExpandFooterProps = {
    shown: number
    total: number
    noun: string
    expandLabel: string
    expanded: boolean
    onToggle: () => void
}

export function ExpandFooter({ shown, total, noun, expandLabel, expanded, onToggle }: ExpandFooterProps) {
    return (
        <div className="flex justify-between border-t pt-3 text-xs text-muted-foreground tabular-nums">
            <span>
                Showing {formatCount(shown)} of {formatPlural(total, noun)}
            </span>
            {(expanded || shown < total) && (
                <button type="button" onClick={onToggle} className="font-medium text-primary hover:underline">
                    {expanded ? "Show Fewer" : expandLabel}
                </button>
            )}
        </div>
    )
}
