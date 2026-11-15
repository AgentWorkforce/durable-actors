const COLLECT_AFTER_BYTES = 8 * 1024 * 1024

// What a large payload leaves behind, the values it was read into and the text it was sent as, is garbage within
// milliseconds. The engine collects on its own schedule, which does not come round in that time, so the garbage of
// one request is still resident under the next: repeating a 12 MB request took the process from 147 MiB to 213 MiB.
// Collecting after each 8 MiB of payload frees it first.
class GarbageCollection {
    private passed = 0

    constructor(private readonly collect: () => void = collectNow) {}

    payloadPassed(bytes: number): void {
        this.passed += bytes
        if (this.passed < COLLECT_AFTER_BYTES) return
        this.passed = 0
        this.collect()
    }
}

function collectNow(): void {
    Bun.gc(true)
}

// Bun's published types redefine Node's globals for the whole program, so only the call used here is declared.
declare const Bun: { gc(synchronous: boolean): void }

export { GarbageCollection }
