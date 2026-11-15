import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, type ActorClass, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime } from "../fixtures/actor-runtime.js"
import { wireEffects } from "../fixtures/detached.js"
import { resultOf } from "../fixtures/reply.js"

function deferred() {
    let resolve!: () => void
    const promise = new Promise<void>(done => {
        resolve = done
    })
    return { promise, resolve }
}

async function setup(actorClass: ActorClass, interleaveMethods: string[], allowNextInvocation = () => {}) {
    const definition = registerActorClass(actorClass, {
        actorName: actorClass.name,
        fields: [{ name: "count", persistence: Persistence.Persisted }],
        ...{ interleaveMethods }
    })
    const runtime = new ActorRuntime(definition, allowNextInvocation)
    const invoke = (method: string) =>
        runtime.handle({
            type: "invoke",
            request_id: method,
            actor: { project_id: "test", actor_name: actorClass.name, actor_id: "one" },
            method,
            args: []
        })
    return { runtime, invoke }
}

test("only entering a reentrant invocation grants admission; nested calls inherit their caller", async () => {
    const gate = deferred()
    let admissions = 0
    class AdmissionProbe extends Actor {
        count = 0
        async reentrant() {
            await gate.promise
        }
        async ordinary() {
            await this.reentrant()
        }
    }
    const { invoke } = await setup(AdmissionProbe, ["reentrant"], () => {
        admissions++
    })
    const ordinary = invoke("ordinary")
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(admissions, 0)
    gate.resolve()
    await ordinary
    await invoke("reentrant")
    assert.equal(admissions, 1)
})

test("a failed reentrant invocation cannot erase an overlapping successful mutation", async () => {
    const gate = deferred()
    class ReentrantFailure extends Actor {
        count = 0
        async hold() {
            this.count++
            await gate.promise
            throw new Error("model failed")
        }
        async increment() {
            return ++this.count
        }
        async read() {
            return this.count
        }
    }
    const { invoke } = await setup(ReentrantFailure, ["hold"])
    const holding = invoke("hold")
    await invoke("increment")
    gate.resolve()
    assert.equal((await holding).type, "failed")
    const reply = await invoke("read")
    assert.equal(resultOf(reply), 2)
})

test("socket context survives another invocation finishing during a reentrant await", async () => {
    const gate = deferred()
    class ReentrantSockets extends Actor {
        count = 0
        async hold() {
            this.broadcast("before")
            await gate.promise
            this.broadcast("after")
        }
        async read() {
            this.broadcast("other")
            return this.count
        }
    }
    const { invoke } = await setup(ReentrantSockets, ["hold"])
    const holding = invoke("hold")
    await invoke("read")
    gate.resolve()
    const reply = await holding
    assert.notEqual(reply.type, "failed")
    assert.ok("effects" in reply)
    assert.deepEqual(
        wireEffects(reply.effects ?? []).map(effect => effect.type === "broadcast" && effect.message.data),
        ['"before"', '"after"']
    )
})

test("reentrant continuations may resume during an undecorated await and nested calls inherit their caller", async () => {
    const generation = deferred()
    const settings = deferred()
    const entered = deferred()
    class NativeInterleaving extends Actor {
        count = 0
        async helper() {
            await generation.promise
            this.count++
        }
        async hold() {
            await this.helper()
            return this.count
        }
        async update() {
            entered.resolve()
            await settings.promise
            return this.count
        }
    }
    const { invoke } = await setup(NativeInterleaving, ["hold"])
    const holding = invoke("hold")
    const updating = invoke("update")
    await entered.promise
    generation.resolve()
    const result = await holding
    assert.equal(resultOf(result), 1)
    settings.resolve()
    await updating
})

test("an undecorated failure on an opted-in actor cannot roll back a reentrant success", async () => {
    const release = deferred()
    const entered = deferred()
    class OrdinaryFailure extends Actor {
        count = 0
        async background() {
            await entered.promise
            return ++this.count
        }
        async fail() {
            entered.resolve()
            await release.promise
            throw new Error("failed")
        }
        async read() {
            return this.count
        }
    }
    const { invoke } = await setup(OrdinaryFailure, ["background"])
    const background = invoke("background")
    const failed = invoke("fail")
    await background
    release.resolve()
    assert.equal((await failed).type, "failed")
    const result = await invoke("read")
    assert.equal(resultOf(result), 1)
})

test("emittable changes compare consecutive completed snapshots, including a value restored by a continuation", async () => {
    const gate = deferred()
    class ReentrantEmission extends Actor {
        count = 0
        async hold() {
            await gate.promise
            this.count = 0
        }
        async increment() {
            this.count++
        }
    }
    const definition = registerActorClass(ReentrantEmission, {
        actorName: "ReentrantEmission",
        interleaveMethods: ["hold"],
        fields: [{ name: "count", persistence: Persistence.Persisted, emittable: true }]
    })
    const runtime = new ActorRuntime(definition, () => {})
    const command = {
        type: "invoke" as const,
        request_id: "hold",
        actor: { project_id: "test", actor_name: "ReentrantEmission", actor_id: "one" },
        args: []
    }
    const holding = runtime.handle({ ...command, method: "hold" })
    await runtime.handle({ ...command, method: "increment" })
    gate.resolve()
    const reply = await holding
    assert.ok("effects" in reply)
    assert.deepEqual(reply.effects, [{ type: "state_update", changes: { count: 0 }, removed: [] }])
})
