# Clustered Counter HTTP Example

This example starts one Rakka node per process, joins nodes with a simple file discovery directory, exposes a REST/JSON counter API, and routes every request to a persistent sharded `Counter` entity by name.

All Rakka APIs are imported through the top-level `rakka` facade crate. The REST routes use `rakka::http::Router`/`HttpError` with Axum extractors for path parameters.

## Run

Start two terminals with a shared discovery directory and different Rakka/HTTP ports:

```sh
RAKKA_DISCOVERY_DIR=/tmp/rakka-counter-http-demo \
  RAKKA_TCP_PORT=25520 RAKKA_HTTP_PORT=50051 \
  cargo run -p rakka-example-clustered-counter-http

RAKKA_DISCOVERY_DIR=/tmp/rakka-counter-http-demo \
  RAKKA_TCP_PORT=25521 RAKKA_HTTP_PORT=50052 \
  cargo run -p rakka-example-clustered-counter-http
```

Then call either node. The counter name is the stable entity id, so `orders` routes to the same sharded entity regardless of which HTTP node receives the call:

```sh
cargo run -p rakka-example-clustered-counter-http -- client initiate orders 10
RAKKA_HTTP_ENDPOINT=http://127.0.0.1:50052 \
  cargo run -p rakka-example-clustered-counter-http -- client increase orders 5
RAKKA_HTTP_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p rakka-example-clustered-counter-http -- client decrease orders 3
RAKKA_HTTP_ENDPOINT=http://127.0.0.1:50052 \
  cargo run -p rakka-example-clustered-counter-http -- client get orders
```

The equivalent REST calls are:

```sh
curl -s -X POST http://127.0.0.1:50051/counters/orders/initiate \
  -H 'content-type: application/json' -d '{"initial_value":10}'
curl -s -X POST http://127.0.0.1:50052/counters/orders/increase \
  -H 'content-type: application/json' -d '{"amount":5}'
curl -s -X POST http://127.0.0.1:50051/counters/orders/decrease \
  -H 'content-type: application/json' -d '{"amount":3}'
curl -s http://127.0.0.1:50052/counters/orders
```

## External HTTP Client

You can interact with the running processes from Postman or any other REST client. Start one or more server processes with the commands above, then send requests to either HTTP port.

In Postman:

1. Create a new HTTP request.
2. Set the method to `POST` for mutations or `GET` for the current count.
3. For `POST` requests, set the `Content-Type` header to `application/json`.
4. Use one of the URLs and JSON bodies below.

Initiate a counter named `orders`:

```http
POST http://127.0.0.1:50051/counters/orders/initiate
Content-Type: application/json
```

```json
{
  "initial_value": 10
}
```

Increase the same counter through another node:

```http
POST http://127.0.0.1:50052/counters/orders/increase
Content-Type: application/json
```

```json
{
  "amount": 5
}
```

Decrease it through either node:

```http
POST http://127.0.0.1:50051/counters/orders/decrease
Content-Type: application/json
```

```json
{
  "amount": 3
}
```

Get the current count without changing it:

```http
GET http://127.0.0.1:50052/counters/orders
Accept: application/json
```

Every request may be sent to either node. Rakka routes the command to the sharded counter entity identified by the `{name}` path segment, creating the persistent counter on first reference. A response looks like this:

```json
{
  "name": "orders",
  "value": 12,
  "revision": 3,
  "initialized": true,
  "created": false,
  "owner_node": "counter-node-25520#uid-25520-..."
}
```

The exact `value`, `revision`, `created`, and `owner_node` fields depend on the calls made so far and which Rakka node owns the shard.

## Failover Behavior

If a process that owns a counter shard exits, the remaining processes continue polling discovery and advancing Rakka membership failure detection. Once the old owner is no longer routable, shard ownership is recalculated and the counter can be started on a live node.

During the detection window, requests for counters owned by the exited process may time out. With the default settings, a graceful Ctrl-C exit is typically detected after the 10 second membership failure timeout. A hard-killed process can take longer because its stale discovery file is retained until the 30 second discovery TTL expires. Counter values recover on the new owner only when the processes share the same `RAKKA_COUNTER_STORE_DIR`.

When a process restarts, the example keeps the same default logical node id for the port but generates a fresh default node incarnation. Do not reuse a fixed `RAKKA_NODE_INCARNATION` across restarts unless you deliberately want to model the same cluster incarnation; a member that was already marked down is not resurrected by reusing its old incarnation.

Useful environment variables:

- `RAKKA_TCP_PORT`: Rakka TCP remoting port for this process.
- `RAKKA_HTTP_PORT`: public HTTP port for this process. Defaults to `RAKKA_TCP_PORT + 10000`.
- `RAKKA_DISCOVERY_DIR`: shared directory used by this example's file discovery. Defaults to `/tmp/rakka-clustered-counter-http/discovery`.
- `RAKKA_COUNTER_STORE_DIR`: shared directory for counter durable state. Defaults to `/tmp/rakka-clustered-counter-http/counter-state`.
- `RAKKA_BIND_HOST`: local IP address to bind. Defaults to `127.0.0.1`.
- `RAKKA_ADVERTISE_HOST`: host written into Rakka node discovery records. Defaults to `RAKKA_BIND_HOST`.
- `RAKKA_NODE_LOGICAL_ID`: override the stable logical node id. Defaults to `counter-node-<RAKKA_TCP_PORT>`.
- `RAKKA_NODE_INCARNATION`: override the per-process node incarnation. Defaults to a unique value generated at process start.

## Current Gaps Documented By The Example

- Rakka's HTTP facade provides useful fixed-path adapters, but REST path parameters are application-owned today. This example uses `rakka::http::Router` and maps path-aware handlers itself.
- Rakka's cluster node runtime is discovery-snapshot driven. This example supplies a tiny file-based discovery loop so local processes can join as they start. Production code should plug in a real discovery provider such as Kubernetes DNS, Consul, or a control plane.
- The example uses deterministic in-memory shard coordination for local demonstration. Production multi-process deployments should configure a shared durable shard coordinator store and lease, such as the PostgreSQL sharding plugin, so there is one fenced ownership authority.
- The example-local file durable store is for local demos. It persists counter state across actor restarts and local process restarts that share the same directory, but it is not a production distributed CAS store. For real multi-host durability, use the PostgreSQL persistence plugin or another external `DurableStateStore`.
- There is no `rakka::http` helper that maps `RemoteEntityAskError` directly into an HTTP error yet. The service maps the common cases locally.
