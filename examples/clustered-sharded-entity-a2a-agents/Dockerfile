# Multi-stage build for the clustered sharded-entity A2A example.
#
# Build from the workspace root:
#
#   docker build -f examples/clustered-sharded-entity-a2a-agents/Dockerfile \
#     -t rakka-clustered-a2a-agents:0.1.0 .
#
# The image is built with the optional `postgres` feature so Kubernetes
# deployments can use a shared PostgreSQL durable store.

FROM rust:1.85-bookworm AS builder
WORKDIR /workspace
# `protoc` is required at build time: etcd-client's build script (and other
# proto codegen in the dependency graph) invoke it. Installed before COPY so
# the layer caches independently of source changes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release \
    -p rakka-example-clustered-sharded-entity-a2a-agents \
    --features postgres

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder \
    /workspace/target/release/rakka-example-clustered-sharded-entity-a2a-agents \
    /usr/local/bin/rakka-a2a-agent
# private remoting / public A2A HTTP
EXPOSE 25580 35580
ENTRYPOINT ["/usr/local/bin/rakka-a2a-agent"]
