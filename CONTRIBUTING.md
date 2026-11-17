# Contributing to Durable Actors

We welcome bug reports, documentation improvements, examples, and code contributions. Start by opening a [GitHub issue](https://github.com/TerseAI/durable-actors/issues/new/choose). External contributors do not need a Linear account or the lit CLI. Discuss larger changes before starting implementation.

New GitHub issues sync to our Durable Actors team in Linear. During triage, a maintainer adds the corresponding ticket ID and link to the GitHub issue, for example `Tracked as [DUR-123](https://linear.app/terse-inc/issue/DUR-123)`. Use that ID in your branch; you can continue the discussion entirely on GitHub. If the ID is missing, ask a maintainer on the issue. The Linear link may require team access, but the visible DUR ID is all you need.

Please follow our [code of conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately through [SECURITY.md](SECURITY.md).

## Set up the repository

To use Durable Actors in your own application, start with the [language quickstarts](README.md#languages). To develop this repository, install:

- Bun 1.4.2 (the pinned runtime and package manager).
- Rust 1.91+ with Cargo and rustfmt, and a native C/C++ build toolchain.
- Helm 3 for changes to the Kubernetes chart.
- Python 3.11+ and uv for changes to `sdk-python`.
- PostgreSQL 16 for database tests. Docker is an optional way to run it.

Fork the repository, then clone your fork and install the workspace dependencies:

```sh
git clone https://github.com/YOUR_USERNAME/durable-actors.git
cd durable-actors
bun install --frozen-lockfile
bun run --bun --cwd sdk build
cargo build --locked
```

Repository builds set the pinned Google SDK’s Rapid append opt-in in `.cargo/config.toml`. When building outside the checkout (including `cargo install`), set `RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"`.

The SDK build includes the observer UI and actor template. A full native bundle can be built with `bun run --bun build`.

Cargo fetches the Rust `terse-litestream` and `terse-ltx` crates at the revisions pinned in `Cargo.toml` and `Cargo.lock`. No submodule checkout is needed. `third_party/` contains their license notices for distribution in native bundles and runtime images.

## Find your way around

| Directory               | Contents                                            |
| ----------------------- | --------------------------------------------------- |
| `src/`                  | Rust control plane, host, storage, and runtime      |
| `sdk-python/`           | Python actors, executor, and generated clients |
| `sdk/`                  | TypeScript SDK, compiler, generated client, and CLI |
| `packages/observer-ui/` | Actor observability UI                              |
| `charts/durable-actors/`          | GKE hosting with Standard GCS and optional Rapid          |
| `tests/`                | Rust tests and repository script tests              |
| `examples/`             | Chat, AI chat, and collaborative documents          |
| `docs/`                 | API and configuration references                    |

## Make a change

Keep changes focused and follow the [engineering conventions](AGENTS.md). For behavior changes, add a failing test first, implement the change, then refactor. Put tests and fixtures in the relevant project's `tests/` directory; CLI tests belong in `sdk/tests/cli/`. Rust unit tests use `#[path]` declarations to retain private access.

Update documentation and examples when changing a public API, configuration, or CLI behavior.

## Run checks

Rust SQLite test fixtures use the upstream Go Litestream CLI to create and restore compatible backups. Install Go 1.27.1 and the pinned test tool, then add Go's binary directory to your PATH:

```sh
go install github.com/benbjohnson/litestream/cmd/litestream@v0.5.17
export PATH="$(go env GOPATH)/bin:$PATH"
```

The production runtime embeds the Rust crate and does not need this Go executable.

From the repository root:

```sh
bun run --bun format:check
bun run --bun test
bun run --bun docs:check
```

`bun run --bun test` covers the observer UI, repository scripts, Rust tests, and SDK. PostgreSQL tests skip their database work unless `DURABLE_ACTORS_TEST_POSTGRES_URL` is set. Use a dedicated test database; the suite creates and removes test schemas. For example, with Docker:

```sh
docker run --name durable-actors-test-postgres --rm -d \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=durable_actors_test \
  -p 127.0.0.1:5432:5432 postgres:16
docker exec durable-actors-test-postgres pg_isready -U postgres -d durable_actors_test
export DURABLE_ACTORS_TEST_POSTGRES_URL='postgresql://postgres:postgres@127.0.0.1:5432/durable_actors_test?sslmode=disable'
bun run --bun test
```

Wait until `pg_isready` reports that PostgreSQL is accepting connections before running the tests. Stop the disposable database with `docker stop durable-actors-test-postgres` when finished.

For runtime and integration changes, build the SDK and runtime, then run the opt-in tests with Bun on your PATH:

```sh
bun run --bun --cwd sdk build
cargo build --locked
cargo test --locked -- --ignored
```

For Kubernetes deployment changes:

```sh
helm lint charts/durable-actors -f charts/durable-actors/tests/values.yaml
bun test --timeout 60000 charts/durable-actors/tests/chart.test.mjs
```

For Python SDK changes, build the runtime and run from `sdk-python`:

```sh
uv sync --locked --all-extras --no-install-package durable-actors-runtime
uv run --no-sync ruff check src tests
uv run --no-sync ruff format --check src tests
uv run --no-sync mypy src/durable_actors
uv run --no-sync pyright
DURABLE_ACTORS_TEST_RUNTIME="$(cd .. && pwd)/target/debug/durable-actors" uv run --no-sync pytest -q -n auto
uv build --no-sources
```

The `native-build.yml` workflow builds `durable-actors-runtime` wheels with Maturin for Linux/macOS on x64/ARM64. It packages the same compiled executable as the native CLI tarball with `bun scripts/build-runtime.mjs --binary target/release/durable-actors-native`; both executable names use `src/main.rs`. It builds the observer UI once per wheel, includes the prebuilt assets and third-party license notices, then installs both wheels in a clean environment. CLI acceptance tests clear PATH to verify initialization, development, reloads, persistence, code generation, and the observer without Bun or Node. The release publishes runtime wheels before the SDK; configure the new runtime project's trusted publisher before its first release.

Only the release workflow runs `native-build.yml`. Pull request and main CI run tests only, and superseded pull request runs are cancelled. Releases do not rerun the test suites: publishing waits for main CI to pass on the tagged commit.

To build and test the native Python package locally, from the repository root:

```sh
bun install --frozen-lockfile
bun run --bun --cwd packages/observer-ui build
mkdir -p python-runtime/durable_actors_runtime/observer
cp -R packages/observer-ui/dist/standalone/. python-runtime/durable_actors_runtime/observer/
uvx --from 'maturin>=1.15,<2' maturin build --release --out dist
uv pip install --python sdk-python/.venv/bin/python dist/durable_actors_runtime-*.whl
sdk-python/.venv/bin/python -m pytest sdk-python/tests/test_cli.py -q
```

Building the UI from source requires Bun; installing and running the published Python packages requires only Python/uv. SDK clients and production actor images do not install `[cli]` or its observer dependencies.

Python integration tests require `DURABLE_ACTORS_TEST_RUNTIME`; they skip without it. To run the shared CLI's Python tests from the repository root after building the SDK:

```sh
bun run --bun --cwd sdk tsc -p tsconfig.test.json
DURABLE_ACTORS_TEST_PYTHON="$PWD/sdk-python/.venv/bin/python" DURABLE_ACTORS_TEST_RUNTIME="$PWD/target/debug/durable-actors" bun test --timeout 60000 ./sdk/.test-dist/tests/cli/python.test.js
```
The release workflow requires a [PyPI trusted publisher](https://docs.pypi.org/trusted-publishers/adding-a-publisher/) for both `durable-actors` and `durable-actors-runtime`: owner `TerseAI`, repository `durable-actors`, workflow `release.yml`, environment `pypi`.

For SDK packaging or documentation changes, run the relevant checks:

```sh
bun run --bun --cwd sdk package:check
bun run --bun --cwd packages/observer-ui package:check
bun run --bun docs:build
```

The [CI workflow](.github/workflows/ci.yml) is the source of truth for toolchain versions and the full validation matrix.

The Dockerfile builds three production images from a shared Rust build. Validate a target with:

```sh
docker build --target typescript -t durable-actors:typescript .
bash scripts/test-runtime-image.sh durable-actors:typescript typescript
```

Repeat for `python` and `control-plane`. Releases build and check each target on amd64 and arm64; pull request and main CI run tests only, and the WebSocket and restart integration tests run on main. The TypeScript execution package bundles only the actor and host modules, collects dependency licenses, and is checked by `sdk package:check`. Examples, the CLI, compiler, and observer UI are excluded from that image. SQL migrations and `docs/reference/openapi.yaml` are Rust build inputs embedded in the executable; their source directories are not copied into the final images.

## Open a pull request

Include exactly one Durable Actors ticket ID in the source branch, for example `your-name/DUR-123-fix-reconnect`, or as `Fixes DUR-123` in the PR description. The `Ticket branch` check rejects missing, malformed, ambiguous, conflicting, or other-team IDs. Release PRs follow the same rule. This checks the identifier format without accessing Linear. If you do not have Linear access, open the PR anyway; a maintainer will add the corresponding `Fixes DUR-123` line to the description. Always include it in the description so the integration can link and complete the ticket.

Also include `Closes #123` for the corresponding GitHub issue. A maintainer applies exactly one release label: `release:feature`, `release:fix`, or `release:skip` for changes with no user-visible effect, including version-only release PRs. The same required check enforces this choice. Write the PR title as a concise description of the user-visible change: it becomes a bullet under **Features** or **Fixes** in the release notes. Put detailed migration guidance in the linked PR. External contributors can open a PR before a maintainer applies the label.

Describe the problem, the resulting behavior, and how you verified it. Link related issues and include screenshots for UI changes. Note any checks you could not run. Maintainers will review the change and arrange releases through the existing [release workflow](.github/workflows/release.yml).

## Release notes

Prepare the notes for the current `origin/main` commit:

```sh
node scripts/release-notes.mjs
```

The command fetches `origin/main`, reads the latest stable GitHub release, and prints **Features** and **Fixes** for pull requests merged after that release. The same commit and release list produce the same notes. A pull request without `release:feature`, `release:fix`, or `release:skip` stops the command. `bun run release:notes` runs the same script.

Contributions are made under the repository's [MIT license](LICENSE.md). See also the [Contributor License Agreement](CLA.md).

Published chart packages include all three image digests from the same release. The release job uploads the chart to GitHub Releases and `ghcr.io/terseai/charts/durable-actors`. Configure that GHCR package for public access when first published.
