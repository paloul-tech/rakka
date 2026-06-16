# Clustered Counter gRPC Example

This example starts one Rakka node per process, joins nodes with a simple file discovery directory, exposes a generated gRPC `CounterService`, and routes every request to a persistent sharded `Counter` entity by name.

All Rakka APIs are imported through the top-level `rakka` facade crate. The only non-Rakka runtime dependencies are `tonic`/`prost` for the generated gRPC contract and `serde_json` for the example-local file stores.

## Run

Start two terminals with a shared discovery directory and different Rakka/gRPC ports:

```sh
RAKKA_DISCOVERY_DIR=/tmp/rakka-counter-demo RAKKA_TCP_PORT=25520 RAKKA_GRPC_PORT=50051 \
  cargo run -p rakka-example-clustered-counter-grpc

RAKKA_DISCOVERY_DIR=/tmp/rakka-counter-demo RAKKA_TCP_PORT=25521 RAKKA_GRPC_PORT=50052 \
  cargo run -p rakka-example-clustered-counter-grpc
```

Then call either node. The counter name is the stable entity id, so `orders` routes to the same sharded entity regardless of which gRPC node receives the call:

```sh
cargo run -p rakka-example-clustered-counter-grpc -- client initiate orders 10
RAKKA_GRPC_ENDPOINT=http://127.0.0.1:50052 \
  cargo run -p rakka-example-clustered-counter-grpc -- client increase orders 5
RAKKA_GRPC_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p rakka-example-clustered-counter-grpc -- client decrease orders 3
RAKKA_GRPC_ENDPOINT=http://127.0.0.1:50052 \
  cargo run -p rakka-example-clustered-counter-grpc -- client get orders
```

## External gRPC Client

You can interact with the running processes from Postman or any other gRPC client. Start one or more server processes with the commands above, then connect to either gRPC port. The example serves plaintext gRPC, so use `127.0.0.1:50051` or `127.0.0.1:50052` without TLS.

In Postman:

1. Create a new gRPC request.
2. Set the server URL to `127.0.0.1:50051` or `127.0.0.1:50052`.
3. Import the proto file from `examples/clustered-counter-grpc/proto/rakka/examples/clustered_counter/v1/counter.proto`.
4. Select `rakka.examples.clustered_counter.v1.CounterService`.

Call the methods with JSON message bodies like these:

```json
{
  "name": "orders",
  "initial_value": 10
}
```

Use that body with `Initiate`.

```json
{
  "name": "orders"
}
```

Use that body with `Get` to read the current count without changing it.

```json
{
  "name": "orders",
  "amount": 5
}
```

Use that body with `Increase`.

```json
{
  "name": "orders",
  "amount": 3
}
```

Use that body with `Decrease`.

Every request may be sent to either node. Rakka routes the command to the sharded counter entity identified by `name`, creating the persistent counter on first reference. A response looks like this:

```json
{
  "name": "orders",
  "value": 12,
  "revision": 3,
  "initialized": true,
  "created": false,
  "owner_node": "counter-node-25520#uid-25520"
}
```

The exact `value`, `revision`, `created`, and `owner_node` fields depend on the calls made so far and which Rakka node owns the shard.

Useful environment variables:

- `RAKKA_TCP_PORT`: Rakka TCP remoting port for this process.
- `RAKKA_GRPC_PORT`: public gRPC port for this process. Defaults to `RAKKA_TCP_PORT + 10000`.
- `RAKKA_DISCOVERY_DIR`: shared directory used by this example's file discovery. Defaults to `/tmp/rakka-clustered-counter-grpc/discovery`.
- `RAKKA_COUNTER_STORE_DIR`: shared directory for counter durable state. Defaults to `/tmp/rakka-clustered-counter-grpc/counter-state`.
- `RAKKA_BIND_HOST`: local IP address to bind. Defaults to `127.0.0.1`.
- `RAKKA_ADVERTISE_HOST`: host written into Rakka node discovery records. Defaults to `RAKKA_BIND_HOST`.
- `RAKKA_NODE_LOGICAL_ID` and `RAKKA_NODE_INCARNATION`: override the generated node id.

## Current Gaps Documented By The Example

- Rakka does not yet provide a turnkey generated gRPC service facade. The `rakka::grpc` crate provides adapters and timeout/error helpers, while this example owns the `.proto`, `build.rs`, and tonic service implementation.
- Rakka's cluster node runtime is discovery-snapshot driven. This example supplies a tiny file-based discovery loop so local processes can join as they start. Production code should plug in a real discovery provider such as Kubernetes DNS, Consul, or a control plane.
- The example-local file durable store is for local demos. It persists counter state across actor restarts and local process restarts that share the same directory, but it is not a production distributed CAS store. For real multi-host durability, use the PostgreSQL persistence plugin or another external `DurableStateStore`.
- There is no `rakka::grpc` helper that maps `RemoteEntityAskError` directly into a tonic status yet. The service maps the common cases locally.
