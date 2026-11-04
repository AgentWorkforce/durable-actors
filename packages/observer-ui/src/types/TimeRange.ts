export type TimeRange = { mode: "live"; durationMs: number } | { mode: "static"; fromMs: number; toMs: number }

export type TimeBounds = { fromMs: number; toMs: number }

export type TimeSpan = { fromMs: number; toMs: number }

export type TimeWindow = { fromMs: number; toMs: number; stepMs: number; live: boolean }
