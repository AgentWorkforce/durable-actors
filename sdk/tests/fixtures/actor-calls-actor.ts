import { Actor, Persisted } from "durable-actors"

export class DocIndex extends Actor {
    @Persisted ids: string[] = []

    async add(id: string): Promise<string[]> {
        if (!this.ids.includes(id)) this.ids.push(id)
        return this.ids
    }

    async list(): Promise<string[]> {
        return this.ids
    }
}

export class Document extends Actor {
    @Persisted indexed: string[] | null = null

    async publish(): Promise<void> {
        this.alarm.set(Date.now())
    }

    async read(): Promise<string[] | null> {
        return this.indexed
    }

    async onAlarm(): Promise<void> {
        this.indexed = await DocIndex.get("main").add(this.id)
    }
}
