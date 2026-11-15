# TypeScript guide

## Before you start

```bash
bun install
bun run --bun --cwd sdk build
cd examples/bank-typescript
cp .env.example .env
```

## Defining an actor

Each bank account is an actor. An actor has fields and methods. The `@Persisted` property decorator saves a field each time you assign it.

To track a bank balance, define `src/actors.ts`:

```typescript
import { Actor, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted private balance = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
}
```

## Generating a client

Start the local dev server from `examples/bank-typescript`. The first run downloads the runtime:

```bash
bun run --bun dev
```

Wait for `Ready`, then generate the client in a second terminal:

```bash
cd examples/bank-typescript
bun run --bun generate
```

This creates a `generated` folder with typed stubs for your actors. Each actor instance is addressed by an ID. Create a `client.ts` that gets the `demo` account and calls a method:

```typescript
import { actors } from "./generated/index.js"

const account = actors.BankAccount.get("demo")
console.log(await account.getBalance())
```

Run it with `bun run --bun client`, which runs `bun client.ts`. Bun loads `.env`, so the client connects to port 7111.

An actor starts when one of its methods is called, stays in memory while it is busy, and shuts down after sitting idle. Regenerate the client after changing an actor's methods.

## Ephemeral fields

Next, count the deposits made since the actor last woke up. The `@Ephemeral` decorator keeps a field in memory only while the actor is awake. It resets when the actor shuts down.

```typescript
import { Actor, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted private balance = 0
    @Ephemeral private depositsSinceWake = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        return this.balance
    }
}
```

## WebSocket messages

Actors handle WebSocket connections directly. The three type parameters of `Actor` describe each connection. Payloads are JSON.

- **Metadata** identifies the client. In this example, it is the `userId` passed when the client connects.
- **Incoming** messages are what a client can send. Here, `{ type: "ping" }`, which `onMessage` answers with the balance.
- **Outgoing** messages are what the actor sends, through either `socket.send` or `broadcast`. Here, `{ type: "balance", balance }`.

The example also uses:

- **Tags.** `setTags` labels one socket. `onConnect` tags each socket `"customer"`.
- **Automatic responses.** The gateway answers a raw text `ping` with `pong` without waking the actor. `onConnect` registers the pair. A JSON `{ type: "ping" }` still runs `onMessage`.
- **Broadcasts.** `broadcast` sends to every connected socket. `deposit` limits it to sockets tagged `"customer"`, and `onDisconnect` uses `except` to skip the socket that just left.
- **Individual sends.** `socket.send` sends to one socket. `onConnect` and `onMessage` use it to send that client the current balance.

```typescript
import { Actor, type ActorSocket, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor<Metadata, Incoming, Outgoing> {
    @Persisted private balance = 0
    @Ephemeral private depositsSinceWake = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        this.broadcast({ type: "balance", balance: this.balance }, { tags: ["customer"] })
        return this.balance
    }
    async onConnect(socket: Socket): Promise<void> {
        socket.setTags("customer")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        socket.send({ type: "balance", balance: this.balance })
    }
    async onMessage(socket: Socket): Promise<void> {
        socket.send({ type: "balance", balance: this.balance })
    }
    async onDisconnect(socket: Socket): Promise<void> {
        this.broadcast({ type: "balance", balance: this.balance }, { except: socket })
    }
}

type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
```

## Emitting state

`@Emittable` publishes a public `@Persisted` field to every connected client, so `balance` becomes public:

- When a client connects, it receives a `state` message with the current emitted fields, here `message.state.balance`.
- After a successful call changes the field, clients receive a `state_update` message with the changes.

```typescript
import { Actor, type ActorSocket, Emittable, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor<Metadata, Incoming, Outgoing> {
    @Persisted @Emittable balance = 0
    @Ephemeral private depositsSinceWake = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        this.broadcast({ type: "balance", balance: this.balance }, { tags: ["customer"] })
        return this.balance
    }
    async onConnect(socket: Socket): Promise<void> {
        socket.setTags("customer")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        socket.send({ type: "balance", balance: this.balance })
    }
    async onMessage(socket: Socket): Promise<void> {
        socket.send({ type: "balance", balance: this.balance })
    }
    async onDisconnect(socket: Socket): Promise<void> {
        this.broadcast({ type: "balance", balance: this.balance }, { except: socket })
    }
}

type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
```

To connect, your backend prepares a WebSocket grant for the actor, and the client opens its `websocketUrl`. A real backend checks the user's access before issuing a grant; the [chatroom example](../../examples/chat/README.md) shows a browser doing this.

Open the connection before the grant's `connectByMs` deadline. Authorization is checked on connection; an accepted socket remains authorized until it closes, including while the actor sleeps. Reconnects require a valid grant. To revoke an existing connection, explicitly close it.

```typescript
import { actors } from "./generated/index.js"

const { websocketUrl } = await actors.BankAccount.prepareWebsocket({ actorId: "demo", metadata: { userId: "ada" } })
const socket = new WebSocket(websocketUrl)
socket.addEventListener("message", event => {
    const message = JSON.parse(event.data)
    if (message.type === "state") console.log(message.state.balance)
    else if (message.type === "state_update") console.log(message.changes.balance)
    else console.log(message)
})
```

## Using @Resource

It's common to tie a Durable Actor with other pieces of infra such as a Sandbox or a Coding Harness. The problem, is that those have a different lifecycle (ex: sandboxes can be up for days). In order to simplify the core logic of your actor, you can create Resources and tie them to the lifecycle of the Durable Actor.

First, create you resource:

```typescript
class ModalSandbox implements ActorResource {
    private _sandbox: Sandbox | undefined;

    get sandbox(): Sandbox {
        if (this._sandbox === undefined) {
            throw new Error("Sandbox not created")
        }
        return this._sandbox
    }

    constructor(private readonly db: ActorDatabase) {
        this.modal = new ModalClient({
            tokenId: process.env.MODAL_TOKEN_ID,
            tokenSecret: process.env.MODAL_TOKEN_SECRET,
        });

        this.resourceKV = new ResourceKV(this.db, "modal_sandbox")
    }

    async onResume(): Promise<void> {
        // check if sandbox is up, start a new one if not
    }

    async onGracefulShutdown(): Promise<void> {
        // nice to have cleanup, not called when actor crashes.
    }
}
```

Then link it to your actor:

```typescript
export class PiAgent extends Actor<null, string, Outgoing> {
    @Resource private sandbox = new ModalSandbox(this.db)

    async snapshot() {
        this.sandbox.snapshot() // this is guaranteed to be available, no need to check and handle the edge case every time.
    }
}
```

## Scheduling an alarm

Each actor has one alarm. Setting it again replaces the previous timestamp. When the timestamp is due, the runtime wakes the actor and runs `onAlarm`.

```typescript
import { Actor, Persisted } from "durable-actors"

export class Reminder extends Actor {
    @Persisted private sent = false

    async schedule(delayMs: number): Promise<void> {
        this.alarm.set(Date.now() + delayMs)
    }

    async onAlarm(): Promise<void> {
        this.sent = true
    }
}
```

## Interleaving calls

An actor runs one call at a time, and a call holds the actor until it finishes, including across an `await`. `@Interleave` lets other calls run while that method awaits.

Add a second actor for wire transfers to `src/actors.ts`:

```typescript
import { Actor, Emittable, Interleave, Persisted } from "durable-actors"

const clearingNetwork = {
    async submit(reference: string): Promise<string> {
        return `receipt:${reference}`
    }
}

export class Wire extends Actor {
    @Persisted @Emittable status = "requested"

    async getStatus(): Promise<string> {
        return this.status
    }
    @Interleave
    async submit(reference: string): Promise<string> {
        const receipt = await clearingNetwork.submit(reference)
        this.status = "submitted"
        return receipt
    }
}
```

`getStatus` can run while `submit` waits on the clearing network.

Once any method in a class uses `@Interleave`, `this.db.transaction` throws in that class, because overlapping calls may already have used the changes it would roll back.

## Using SQLite

Each actor has its own SQLite database at `this.db`. `this.db.exec(sql, ...bindings)` runs one statement and returns its rows. Here, `deposit` records each deposit in a ledger table:

```typescript
import { Actor, Emittable, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted @Emittable balance = 0

    async deposit(amount: number): Promise<number> {
        this.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        this.balance += amount
        this.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        return this.balance
    }
}
```

- Each SQL write and each `@Persisted` assignment is saved as it happens, so progress survives a crash mid-method. If the method throws, writes made before the error stay saved.
- `await this.db.transaction(async () => { ... })` rolls back only the SQL and `@Persisted` writes inside it when it throws, and rethrows; the method can catch the error and continue. Transactions nest. Writes inside a transaction are saved together when it completes.
- Assigning a `@Persisted` field writes its JSON value through the same SQL execution path as `this.db.exec()`.
- Reassign objects and arrays to persist changes: `this.messages = [...this.messages, message]`. Nested mutations such as `this.messages.push(message)` only change memory; they are not saved, even at method completion. Assign `undefined` to remove a saved field value rather than using `delete`.
- Saved fields are restored before persistence tracking begins. Defaults are inserted only for fields missing from SQLite, so adding a persisted property initializes existing actors without replacing their saved values. Initialization does not delete existing rows.
- Bind values with `?` placeholders. Values can be strings, numbers, bigints, byte arrays, or `null`.
- `exec` keys each row by column name, so a result with duplicate names (such as `SELECT a.id, b.id` from a join) keeps only the last. `this.db.query(sql, ...bindings)` returns `{ columns, rows }` with each row as an array in column order. ORM drivers that read rows by position build on `query`; for Drizzle, use [`durable-actors/drizzle`](#using-drizzle).
- Actor constructors must accept the opaque `ActorContext` and pass it to `super(context)` before using `this`. Classes without a constructor forward the context automatically.
- `this.db` and `this.id` are available after `super(context)` in constructors and in field initializers, methods, and socket hooks. SQLite is restored before construction; `@Persisted` fields are hydrated afterward.
- Keep constructor and field-initializer SQL synchronous and repeatable (for example, `CREATE TABLE IF NOT EXISTS`): both run again when actors are restored. Their writes commit with the next successful method or socket hook; a throwing constructor or initializer rolls them back.

Here, `withdraw` rejects an overdraft without failing the call. The trigger aborts the insert, the transaction rolls back the ledger row and the balance change, and the method returns the unchanged balance:

```typescript
import { Actor, Persisted } from "durable-actors"

export class Account extends Actor {
    @Persisted balance = 100

    async withdraw(amount: number): Promise<{ ok: boolean; balance: number }> {
        this.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER CHECK (amount <= 100))")
        try {
            await this.db.transaction(async () => {
                this.balance -= amount
                this.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
            })
            return { ok: true, balance: this.balance }
        } catch {
            return { ok: false, balance: this.balance }
        }
    }
}
```

```typescript
import { Actor, type ActorContext } from "durable-actors"

export class Ledger extends Actor {
    constructor(context: ActorContext) {
        super(context)
        this.db.exec("CREATE TABLE IF NOT EXISTS entries (amount INTEGER)")
    }
}
```

- The runtime owns transactions and the database file, so `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `ATTACH`, `DETACH`, and `VACUUM` are rejected. Use `this.db.transaction` for a nested transaction.
- The only allowed PRAGMAs are `table_info`, `table_xinfo`, `index_info`, `index_xinfo`, `index_list`, `foreign_key_list`, `foreign_key_check`, `integrity_check`, `quick_check`, and `user_version`.
- Names beginning with `__terse_` or `_litestream_` are reserved.

## Using Drizzle

`durable-actors/drizzle` runs [Drizzle ORM](https://orm.drizzle.team) over `this.db`. Drizzle builds and maps every query; the adapter executes its SQL with `this.db.query` and runs its transactions with `this.db.transaction`. Install Drizzle in the actor project:

```bash
bun add drizzle-orm
bun add -d drizzle-kit
```

Generate migrations with drizzle-kit's `durable-sqlite` driver, which bundles them into `drizzle/migrations.js`:

```typescript
// drizzle.config.ts
import { defineConfig } from "drizzle-kit"

export default defineConfig({ dialect: "sqlite", driver: "durable-sqlite", schema: "./src/schema.ts", out: "./drizzle" })
```

Run `bunx drizzle-kit generate` after each schema change. Declare the generated module's type once, in `drizzle/migrations.d.ts`:

```typescript
import type { DrizzleMigrations } from "durable-actors/drizzle"
declare const migrations: DrizzleMigrations
export default migrations
```

Apply migrations in the constructor, then query with Drizzle:

```typescript
import { Actor, type ActorContext, Ephemeral } from "durable-actors"
import { drizzle, migrate } from "durable-actors/drizzle"
import { eq } from "drizzle-orm"

import migrations from "../drizzle/migrations.js"
import * as schema from "./schema.js"

type Author = typeof schema.authors.$inferSelect
type Book = typeof schema.books.$inferSelect

export class Library extends Actor {
    @Ephemeral private readonly orm = drizzle(this.db, { schema })

    constructor(context: ActorContext) {
        super(context)
        migrate(this.db, migrations)
    }

    async add(author: string, title: string): Promise<number | null> {
        try {
            return await this.orm.transaction(async tx => {
                const [written] = await tx.insert(schema.authors).values({ name: author }).returning()
                const [book] = await tx.insert(schema.books).values({ authorId: written!.id, title }).returning()
                return book!.id
            })
        } catch {
            return null
        }
    }

    async byAuthor(name: string): Promise<(Author & { books: Book[] }) | null> {
        const author = await this.orm.query.authors.findFirst({
            where: eq(schema.authors.name, name),
            with: { books: true }
        })
        return author ?? null
    }
}
```

- `migrate` applies pending migrations once, in journal order, and records them in `__drizzle_migrations`. It is synchronous, so the constructor rule above applies: its writes commit with the next successful method.
- `transaction` is `this.db.transaction`: a throwing transaction rolls back its SQL and `@Persisted` writes, and the method continues. Transactions nest, and `tx.rollback()` rolls back the enclosing transaction. Classes with `@Interleave` cannot open them.
- `batch` applies all of its statements or none.
- Give public methods explicit return types built from `$inferSelect`. Drizzle's inferred query result types carry symbol keys, which the actor contract rejects.
- Raw queries follow Drizzle's `sqlite-proxy` driver: ``db.all(sql`...`)`` and ``db.get(sql`...`)`` return rows as arrays. Use `this.db.exec` for rows keyed by column name.
- Actor builds bundle `durable-actors/drizzle` with the project's own `drizzle-orm` and load `.sql` imports as text.

## Configuring actor resources

`@Compute` overrides the deployment's default resources for one actor class:

```typescript
import { Actor, Compute, Emittable, Persisted } from "durable-actors"

@Compute({ cpu: 0.5, cpuLimit: 2, memoryMiB: 512, idleTimeoutMs: 60_000 })
export class BankAccount extends Actor {
    @Persisted @Emittable balance = 0
}
```

`cpu` and `memoryMiB` are requests; `cpuLimit` and `memoryLimitMiB` cap them and must be at least the request. An omitted limit inherits the server default, which may leave CPU uncapped, and is raised to fit your request. The server rejects deployments outside its configured range. Actors without `@Compute` resources start on prewarmed sandboxes, as do sizes the server keeps a spare tier for; other sizes start a new sandbox.


## Using Unchecked<> 

A lof of the types found in Pi-durable, Vercel ai-sdk etc..are serializable but their type information is very complex.

You can opt out of type generation for these types to make your generated client mark them as `unknown`. 

This is actually how pi-durable suggests getting their `op` messages across the the wire.

```typescript
import { Op } from "@earendil-works/chord/delta";
import { ConversationView } from "@earendil-works/pi-durable"

type Outgoing =
| { type: "view"; view: Unchecked<ConversationView> }
| { type: "ops"; ops: Unchecked<readonly Op[]> }

export class PiAgent extends Actor<null, string, Outgoing> {}

```

And then on your client:

```typescript
import { applyImmutable, type Op } from "@earendil-works/chord/delta";
import type { ConversationView } from "@earendil-works/pi-durable";

socket.onmessage = ({ data }) => {
    if (current === this.generation) this.receive(JSON.parse(data));
};

receive(event: Outgoing) {
    if ("status" in event) {
      this.update({ preview: event });
    } else if (event.type === "view") {
      this.update({ connection: "connected", view: event.view as ConversationView });
    } else if (this.snapshot.view) {
        this.update({ view: applyImmutable(this.snapshot.view, event.ops as readonly Op[]) });
    }
}

```

This **only** impacts the client generation process. You still get full type-safety within the actor!