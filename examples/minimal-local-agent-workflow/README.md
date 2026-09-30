# Minimal Local Agent Workflow

The smallest useful path through the agent-workflow kernel, in one process
with in-memory stores: define and register a workflow, build a `StartRun`
command with durability metadata, accept it through the durable inbox, rebuild
the facade from durable state to show recovery, execute one deterministic step,
and mark the inbox item completed. It mirrors the kernel's first integration
test and exists so the durable command boundary can be seen from a terminal.

It is deliberately not a workflow engine: the runner is a few lines so that the
boundary — a command is durable before anything acts on it, and recovery reads
the same record — stays visible.

## Run

```sh
cargo run -p rakka-example-minimal-local-agent-workflow
```

## Expected stdout

```text
Accepted StartRun as durable inbox message command-start-local-1 at revision 1.
Recovered 1 inbox item(s) from in-memory durable state.
Executed deterministic step; run run-local-1 is completed.
Completed payload: deterministic-plan-complete
Recoverable inbox items after completion: 0.
Recorded 1 bounded command acceptance metric(s).
```

The durable agent domain built on this kernel is described in
[`docs/rakka-agents.md`](../../docs/rakka-agents.md); the other agent examples
listed there walk it end to end.
