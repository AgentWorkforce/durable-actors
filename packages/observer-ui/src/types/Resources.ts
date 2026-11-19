/** Requests and limits of a sandbox; an absent limit leaves that resource uncapped. */
export interface ResourceLimits {
    cpuMillis: number
    cpuLimitMillis?: number
    memoryMib: number
    memoryLimitMib?: number
}

/** A sandbox's resources from when it was claimed; a sandbox keeps them for its whole life. */
export interface SandboxResources extends ResourceLimits {
    claimedAtMs: number
}

export interface UsagePoint {
    atMs: number
    cpuCores: number | null
    memoryBytes: number | null
}

export interface DatabasePoint {
    atMs: number
    bytes: number
}

/** One instance's usage. Read points hold the highest sample of a bucket; live points are single samples. */
export interface ResourceUsage {
    /** The resources in force over the range: the sandbox running at its start, then each later one. */
    sandboxes: SandboxResources[]
    usage: UsagePoint[]
    database: DatabasePoint[]
    /** The latest reported size, however long ago it was reported. */
    databaseBytes: number | null
}
