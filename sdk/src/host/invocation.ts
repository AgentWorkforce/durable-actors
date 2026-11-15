import { AsyncLocalStorage } from "node:async_hooks"

import type { HostSocketEffect, SocketLookup, SocketQuery } from "../actor/socketProtocol.js"

/** What a running command reaches in the Rust host. */
interface Invocation {
    /** The command's message ID, which names its database, its logs and its socket output to the Rust host. */
    readonly id: number
    allowNext(): void
    publish(effects: readonly HostSocketEffect[]): void
    connections(query?: SocketQuery): Promise<SocketLookup>
}

const invocations = new AsyncLocalStorage<Invocation>()

function runInvocation<T>(invocation: Invocation, operation: () => T): T {
    return invocations.run(invocation, operation)
}

/** The command that started the running code, which may since have replied. */
function currentInvocation(): Invocation | undefined {
    return invocations.getStore()
}

export { currentInvocation, runInvocation }
export type { Invocation }
