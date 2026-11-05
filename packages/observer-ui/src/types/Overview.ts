export interface ActorResidencyMetrics {
    live: number
    dormant: number
}

export interface RequestRateMetrics {
    current: number
    average: number
    peak: number
    peakAt: string
    series: number[]
}

export interface RequestCountMetrics {
    total: number
    perSecond: number
    peak: number
    peakAt: string
    series: number[]
}

export interface SocketMetrics {
    open: number
    change: number
    perLiveActor: number
    series: number[]
}
