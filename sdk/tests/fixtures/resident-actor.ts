import type { Invocation } from "../../src/host/invocation.js"

/** What a command reaches in the Rust host. Nothing is behind it unless the test puts it there. */
export function command(id: number, reach: Partial<Invocation> = {}): Invocation {
    return { id, allowNext() {}, publish() {}, connections: async () => [], ...reach }
}
