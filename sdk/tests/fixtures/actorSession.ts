import { Actor, Persisted } from "durable-actors"
import type { ActorSocket } from "durable-actors"

class SessionCounter extends Actor {
    @Persisted private count = 0

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async incrementAtomic(): Promise<number> {
        return this.db.transaction(async () => {
            return this.incrementTwice()
        })
    }

    async incrementTwice(): Promise<number> {
        this.count++
        this.count++
        return this.count
    }

    async failAfterWrite(): Promise<void> {
        this.count++
        throw new Error("failed after write")
    }

    async failAtomic(): Promise<void> {
        return this.db.transaction(async () => {
            return this.failAfterWrite()
        })
    }

    async read(): Promise<number> {
        return this.count
    }

    async onConnect(socket: ActorSocket): Promise<void> {
        socket.send({ type: "frames", frames: this.db.exec<{ html: string }>("SELECT html FROM frames") })
    }

    async stream(): Promise<number> {
        this.broadcast({ delta: "first" })
        this.broadcast({ delta: "last" })
        return this.count
    }
}

export { SessionCounter }
