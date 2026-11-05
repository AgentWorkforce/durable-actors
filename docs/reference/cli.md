# CLI

`durable-actors` runs durable actors locally or in the cloud. TypeScript projects use the Bun CLI (`bunx durable-actors`); Python projects install `durable-actors[cli]` and use `uv run durable-actors`. Python tooling requires no JavaScript runtime.

Both CLIs expose `init`, `dev`, `generate`, `inspect`, `observe`, and `start`. The flags below describe the Bun CLI; Python `init` always creates a Python project and Python `generate` always emits Python clients.

```sh
durable-actors [options] [command]
```

| Option          | Description  |
| --------------- | ------------ |
| `-h, --help`    | Show help    |
| `-V, --version` | Show version |

| Command    | Description                                                     |
| ---------- | --------------------------------------------------------------- |
| `init`     | Create a sample actor project                                   |
| `dev`      | Run local actors and reload code changes                        |
| `generate` | Generate actor clients from the running server or a source file |
| `inspect`  | Print actor inventory and the latest persisted state            |
| `observe`  | Open the local observability UI                                 |
| `start`    | Start the production control plane server                       |

Other settings are environment variables. See [Configuration](configuration.md).

## durable-actors init

Create a sample actor project. The project includes the `durable-actors-quickstart` agent skill in `.agents/skills` and `.claude/skills`.

```sh
durable-actors init <directory> [--template <name>]
```

| Argument    | Required | Description           |
| ----------- | -------- | --------------------- |
| `directory` | Yes      | Destination directory |

| Flag                | Default | Description                                                            |
| ------------------- | ------- | ---------------------------------------------------------------------- |
| `--template <name>` | `actor` | Project template: `actor`, `chat`, `ai-chat`, or `documents` |

```sh
durable-actors init my-project
uvx --from 'durable-actors[cli]' durable-actors init my-project
```

## durable-actors dev

Run local actors and reload code changes. Run it from the actor project directory. It discovers `actors.ts` or `actors.py` in `src/` or the project root. If more than one exists, set `DURABLE_ACTORS_ENTRYPOINT` to choose; an explicit setting always wins.

```sh
durable-actors dev [--port <number>] [--no-watch]
```

| Flag              | Default | Description                                                                                                      |
| ----------------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| `--port <number>` | `7100`  | Localhost port. `0` selects a free port. Integer from 0 to 65535. `DURABLE_ACTORS_PORT` supplies the same value. |
| `--no-watch`      |         | Disable automatic code reload                                                                                    |

Optional `.env` overrides:

| Variable                    | Default                            | Description                                |
| --------------------------- | ---------------------------------- | ------------------------------------------ |
| `DURABLE_ACTORS_PROJECT`    | Current directory                  | Actor project directory                    |
| `DURABLE_ACTORS_ENTRYPOINT` | Auto-detected | Actor source file, relative to the project; discovers `actors.ts` or `actors.py` in `src/` or the project root |
| `DURABLE_ACTORS_PYTHON`     | Project `.venv`                    | Python interpreter                         |

Python projects use the interpreter running the CLI. The `DURABLE_ACTORS_PYTHON` override applies to the Bun CLI and native runtime.

```sh
durable-actors dev
durable-actors dev --port 0
durable-actors dev --no-watch
```

## durable-actors generate

Generate actor clients from the running server, or from an explicit source file.

```sh
durable-actors generate [entrypoint] [--out-dir <directory>] [--language <language>] [--config <file>] [--control-plane-url <url>]
```

| Argument     | Required | Description                                                                   |
| ------------ | -------- | ----------------------------------------------------------------------------- |
| `entrypoint` | No       | Actor source file to compile instead of fetching the contract from the server |

| Flag                        | Default     | Description                                                         |
| --------------------------- | ----------- | ------------------------------------------------------------------- |
| `--out-dir <directory>`     | `generated` | Generated client directory                                          |
| `--language <language>`     | Inferred    | `typescript` or `python`. Inferred from the contract when omitted.  |
| `--config <file>`           |             | TypeScript configuration file. Local TypeScript source only.        |
| `--control-plane-url <url>` |             | Control-plane origin. Overrides `DURABLE_ACTORS_CONTROL_PLANE_URL`. |

`--config` requires a TypeScript entrypoint. `--control-plane-url` applies when the contract is fetched from the server.

Connection settings come from the environment or `.env`:

| Variable                           | Default                                                      |
| ---------------------------------- | ------------------------------------------------------------ |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost                                         |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                                      |
| `DURABLE_ACTORS_SECRET`            | Unset. Must match the server when authentication is enabled. |

```sh
durable-actors generate
durable-actors generate src/actors.ts --out-dir generated
durable-actors generate src/actors.py --language python
```

## durable-actors inspect

Print actor inventory and the latest persisted state. It uses the same connection settings as `generate`. Without a selector, inspect lists every actor instance and its status. With `ActorName/actorId`, it prints that instance's status and latest persisted state as JSON.

```sh
durable-actors inspect [ActorName/actorId] [--json] [--control-plane-url <url>]
```

| Argument            | Required | Description                                              |
| ------------------- | -------- | -------------------------------------------------------- |
| `ActorName/actorId` | No       | Actor class and instance identity, separated by a slash |

| Flag                        | Description                                                         |
| --------------------------- | ------------------------------------------------------------------- |
| `--json`                    | Print machine-readable JSON                                         |
| `--control-plane-url <url>` | Control-plane origin. Overrides `DURABLE_ACTORS_CONTROL_PLANE_URL`. |

When the control plane has not finished gathering socket metadata, inspect prints a warning on stderr. `--json` still writes a JSON document to stdout.

```sh
durable-actors inspect
durable-actors inspect Counter/one
durable-actors inspect Counter/one --json
```

## durable-actors observe

Open the local observability UI. It uses the same connection settings as `generate`.

```sh
durable-actors observe [--no-open]
```

| Flag        | Description                                |
| ----------- | ------------------------------------------ |
| `--no-open` | Print the UI URL without opening a browser |

```sh
durable-actors observe
durable-actors observe --no-open
```

## durable-actors start

Start the production control plane server.

```sh
durable-actors start
```
