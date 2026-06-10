# Rakka V1 Generated Contract Example

The generated-contract example demonstrates how application-owned service contracts can sit on top of Rakka adapters without relying on hand-written test structs.

## Contract Boundary

The contract lives in:

- `examples/generated-contracts/proto/rakka/examples/contracts/v1/store.proto`

The generated Rust code is produced at build time by:

- `examples/generated-contracts/build.rs`

The generated module is included with `tonic::include_proto!("rakka.examples.contracts.v1")`. Generated code is not checked into the repository; `cargo build`, `cargo test`, and `cargo run` regenerate it into Cargo's build output.

## Adapter Boundary

The hand-written adapter code lives in:

- `examples/generated-contracts/src/lib.rs`

That file implements generated tonic service traits and maps contract messages into Rakka surfaces:

- `CounterService.Add`: generated unary gRPC client/server path into `rakka_grpc::unary_actor_ask`.
- `CartService.AddItem`: generated unary gRPC path into `rakka_grpc::unary_entity_ask`.
- `CatalogService.List`: generated server-streaming path into `rakka_grpc::server_streaming_service`.
- `IngestService.Upload`: generated client-streaming path into `rakka_grpc::client_streaming_service`.
- `CartLiveService.Watch`: generated bidirectional-streaming path into `rakka_grpc::bidi_streaming_service`.
- `WorkflowService.Submit`: generated unary gRPC path into `rakka_workflow::DurableInbox`.
- `LegacyService.Increment`: generated unary gRPC path into a `rakka_process` line-json child process.

The same protobuf message types are reused by mirrored HTTP routes:

- JSON actor ask route for `CounterDelta`.
- JSON entity ask route for `CartItem`.
- JSON workflow route for `WorkflowCommand`.
- JSON process-backed route for `LegacyRequest`.
- Binary protobuf route for `CounterDelta` to `CounterValue`.

This keeps the contract vocabulary in one `.proto` while showing both gRPC and HTTP adapter wiring.

## Run It

```sh
cargo run -p rakka-example-generated-contracts
```

Expected output includes:

```text
Generated gRPC CounterService returned value 7.
Generated gRPC CartService accepted book and CatalogService returned ["book", "box"].
Generated gRPC streaming accepted 2 upload item(s) and 2 bidi ack(s).
Generated gRPC WorkflowService revision 1 and LegacyService result 42.
Mirrored HTTP JSON returned counter 12, cart pencil, workflow revision 2, legacy 100; binary counter 23.
```

## Test It

The integration test calls generated tonic clients over a local loopback server and exercises the mirrored in-process HTTP routes:

```sh
cargo test -p rakka-example-generated-contracts --test generated_contracts -- --nocapture
```

The test launches a local line-json child process through `rakka-process`. In restricted sandboxes this may require unsandboxed execution, because the OS can block child-process launch or loopback binding.

## Non-Goals

- The example is not a project template generator.
- The generated code is intentionally not committed.
- The HTTP routes do not include OpenAPI generation in v1.
- The workflow endpoint demonstrates durable inbox acceptance, not a full workflow orchestration engine.
