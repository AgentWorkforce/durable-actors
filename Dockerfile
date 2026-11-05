FROM rust:1.91.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY .cargo ./.cargo
COPY migrations ./migrations
COPY proto ./proto
COPY src ./src
COPY docs/reference/openapi.yaml ./docs/reference/openapi.yaml
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release \
    && mkdir -p /out \
    && cp target/release/durable-actors /out/durable-actors

FROM oven/bun:1.4.2 AS sdk-builder
WORKDIR /build
COPY package.json bun.lock bunfig.toml ./
COPY sdk/package.json ./sdk/package.json
COPY sdk/bin ./sdk/bin
COPY packages/observer-ui/package.json ./packages/observer-ui/package.json
RUN bun install --frozen-lockfile --filter './sdk' --filter './packages/observer-ui'
COPY sdk/src ./sdk/src
COPY sdk/scripts/build-host-runtime.mjs ./sdk/scripts/build-host-runtime.mjs
COPY sdk/LICENSE.md ./sdk/LICENSE.md
RUN bun sdk/scripts/build-host-runtime.mjs /out/sdk

FROM python:3.13-slim-bookworm AS python-sdk
WORKDIR /build
COPY sdk-python/pyproject.toml sdk-python/README.md sdk-python/LICENSE.md ./
COPY sdk-python/src ./src
RUN pip install --no-cache-dir .

FROM oven/bun:1.4.2 AS bun

FROM golang:1.26.0-bookworm AS collector-builder
WORKDIR /build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.163.0
COPY collector ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 builder --config builder-config.yaml

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /customer \
    && useradd --uid 10000 --create-home --home-dir /home/runtime runtime
COPY --from=builder /out/durable-actors /usr/local/bin/durable-actors
COPY LICENSE.md /usr/share/licenses/durable-actors/
COPY third_party/terse-ltx/LICENSE /usr/share/licenses/terse-ltx/
COPY third_party/terse-litestream/LICENSE third_party/terse-litestream/NOTICE /usr/share/licenses/terse-litestream/
USER 10000:10000
ENV RUST_LOG=warn,durable_actors=info
ENTRYPOINT ["/usr/local/bin/durable-actors"]

FROM runtime-base AS typescript
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
COPY --from=sdk-builder /out/sdk /node_modules/durable-actors
ENV DURABLE_ACTORS_SDK_HOST=/node_modules/durable-actors/host.js
ENV DURABLE_ACTORS_EXECUTOR_RUNTIME=typescript

FROM python:3.13-slim-bookworm AS python
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /customer \
    && useradd --uid 10000 --create-home --home-dir /home/runtime runtime
COPY --from=python-sdk /usr/local /usr/local
COPY --from=runtime-base /usr/local/bin/durable-actors /usr/local/bin/durable-actors
COPY --from=runtime-base /usr/share/licenses /usr/share/licenses
USER 10000:10000
ENV RUST_LOG=warn,durable_actors=info
ENV DURABLE_ACTORS_EXECUTOR_RUNTIME=python
ENTRYPOINT ["/usr/local/bin/durable-actors"]

FROM debian:bookworm-slim AS collector
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=collector-builder /build/build/otelcol-durable-actors /usr/local/bin/otelcol
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/otelcol"]

FROM runtime-base AS control-plane
ENV DURABLE_ACTORS_PROCESS_ROLE=control_plane
