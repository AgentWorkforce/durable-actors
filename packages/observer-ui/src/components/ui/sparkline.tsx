import { type CSSProperties, useId, useState } from "react"

import { Tooltip, TooltipContent, TooltipTrigger } from "./tooltip.js"

const width = 300
const height = 64
const topInset = 8
const bottomInset = 16

type Point = { x: number; y: number }

export type PointDescription = { at: string; value: string }

export type SparklineProps = {
    values: number[]
    color: string
    label: string
    describePoint?: (index: number) => PointDescription
}

export function Sparkline({ values, color, label, describePoint }: SparklineProps) {
    const gradientId = useId()
    const [active, setActive] = useState<number | null>(null)
    const points = plot(values)
    const line = points.map(({ x, y }, index) => `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ")
    const activePoint = active === null ? undefined : points[active]
    return (
        <div className="relative mt-auto h-16 w-full">
            <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" role="img" aria-label={label} className="block h-full w-full overflow-visible">
                <defs>
                    <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0" style={{ stopColor: color, stopOpacity: 0.16 }} />
                        <stop offset="1" style={{ stopColor: color, stopOpacity: 0 }} />
                    </linearGradient>
                </defs>
                <line x1="0" y1={height - 0.5} x2={width} y2={height - 0.5} className="stroke-border" vectorEffect="non-scaling-stroke" />
                <path d={`${line} L${width},${height} L0,${height} Z`} fill={`url(#${gradientId})`} />
                <path d={line} fill="none" style={{ stroke: color }} strokeWidth={1.75} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
                {activePoint && <line x1={activePoint.x} x2={activePoint.x} y1={0} y2={height} className="stroke-muted-foreground/50" strokeDasharray="2 2" vectorEffect="non-scaling-stroke" />}
            </svg>
            {activePoint && <ActiveDot point={activePoint} color={color} />}
            {describePoint &&
                points.map((_, index) => (
                    <PointTooltip
                        key={index}
                        style={hoverColumn(index, points.length)}
                        point={describePoint(index)}
                        onOpenChange={open => setActive(current => (open ? index : current === index ? null : current))}
                    />
                ))}
        </div>
    )
}

function ActiveDot({ point, color }: { point: Point; color: string }) {
    return (
        <span
            aria-hidden
            className="pointer-events-none absolute size-2 -translate-1/2 rounded-full border-2 border-background"
            style={{ left: `${(point.x / width) * 100}%`, top: `${(point.y / height) * 100}%`, backgroundColor: color }}
        />
    )
}

function PointTooltip({ style, point, onOpenChange }: { style: CSSProperties; point: PointDescription; onOpenChange: (open: boolean) => void }) {
    return (
        <Tooltip onOpenChange={onOpenChange}>
            <TooltipTrigger asChild>
                <span className="absolute inset-y-0" style={style} />
            </TooltipTrigger>
            <TooltipContent className="tabular-nums">
                <div className="opacity-80">{point.at}</div>
                <div className="font-medium">{point.value}</div>
            </TooltipContent>
        </Tooltip>
    )
}

function plot(values: number[]): Point[] {
    const min = Math.min(...values)
    const span = Math.max(...values) - min || 1
    const step = width / Math.max(values.length - 1, 1)
    const plotHeight = height - topInset - bottomInset
    return values.map((value, index) => ({ x: index * step, y: topInset + (1 - (value - min) / span) * plotHeight }))
}

function hoverColumn(index: number, count: number): CSSProperties {
    const center = count > 1 ? index / (count - 1) : 0.5
    const half = 0.5 / Math.max(count - 1, 1)
    const left = Math.max(0, center - half)
    const right = Math.min(1, center + half)
    return { left: `${left * 100}%`, width: `${(right - left) * 100}%` }
}
