export interface StorageUsage {
    bytes: number
    tables: number
    rows: number
    dailyBytes: number[]
}

export interface MemoryUsage {
    bytes: number
    limitBytes: number
}

export interface InstanceResources {
    storage: StorageUsage
    memory: MemoryUsage | null
}
