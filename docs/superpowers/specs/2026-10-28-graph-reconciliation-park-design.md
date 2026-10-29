# Graph reconciliation park: a waiting node that waits on a person, decisions on it, and a re-offer under a new effect generation

Status: written 2026-10-28, awaiting the owner's review. Branch `graph-reconciliation-park`, off `main` at `dfe426c`.

Brief: issue #110 (the rakka.host graph-reconciliation brief, narrowing #71 item 3), its addendum (keep `Cancelling` across a non-terminal graph update), and its optional ask (a node's declared `maxAttempts`). The brief was drafted against `8445ee4`; every site below is re-cited at `dfe426c`, which is also the host's pin after batch 1. This is the host's batch 2, on its own, with its rollout plan.

Owner decision (2026-10-28): the park is a **wait reason**, not a new top-level node status. A parked node reads "still waiting, now on a person". #71 item 3 named `AgentGraphNodeStatus::Indeterminate`; this design supersedes that name for the reason the owner gave: a waiting node composes with everything that already understands waiting (blocked successors, run cancel, the waiting-node transitions), and the status stays truthful.

Calls made by the author, each marked where it lands; the owner can veto any of them:

1. The park is entered through the existing `EffectFailed` command with a fourth failure disposition, `Reconcile`, so the dispatcher keeps its one ingress (section 5.1).
2. The decisions are one new inbox command kind carrying a graph-level decision type with two members, `NotExecuted` and `Abandon`; `Completed` is not built (section 5.2).
3. The safety class that selects the idempotency-key rule is read from the node target's attributes under `rakka-agent`'s existing vocabulary; an absent class is treated as non-idempotent (section 5.3).
4. The declared `maxAttempts` travels as a target attribute under `rakka-agent`'s existing vocabulary, declared in the plan or set per schedule request (section 5.4).
5. The graph state schema version moves to 2; a reader refuses a newer version with a typed error; the version-2 values are written only on the host's explicit calls (section 6).

Calls made after the builder check (2026-10-28), each marked where it lands; the owner can veto any of them:

6. A command keyed by an effect id of a generation that is no longer the node's current one is a no-op, including a late result for a call the person declared not executed; the dispatcher may report that late result, but it moves no graph state (section 5.3, "generation fencing").
7. A real answer for the current generation resolves a park: a late `EffectCompleted` completes the parked node, a definitive `EffectFailed` fails it, so the person is not asked about a call whose outcome has become known (section 5.1).
8. `Reconcile` is refused unless the effect's outbox entry is terminal and never due (`Exhausted` or `Cancelled`), so a parked node never has a live retry under it and a re-offer never leaves two generations live (section 5.1).
9. Recovery re-links every outbox entry that names a graph node, not only the due ones, so a doubtful call dispatched and exhausted before the run actor recovered can still be parked (section 5.7).
10. The attempt bound is read leniently by the shared outbox conversion and validated strictly by the bridge, so `rakka-agent`'s dispatch path, which overrides the policy after conversion, is untouched (section 5.4).

## 1. Intent

A compiled workflow node calls a tool that must not be called twice. The call goes out and its answer is lost: a timeout after the send, or a crash before the reply landed. Nobody can say whether it ran. Today the graph model has no state for "a person has to decide", so the host derives one from three facts (`Waiting` on `Effect`, a target that may not be re-invoked, an `Exhausted` entry naming the doubt), rides two outbox mutators that check nothing, and can offer "it never ran, send it again" at most once per step because a re-send spends the same entry's budget.

This change gives the park a native form in the graph state, a decision command that resolves it, and a re-offer that opens a fresh effect generation with its own budget. The host then retires its derivation, its mutator pin, and its once-per-step cap, and moves its pin. Two small asks ride along: a non-terminal graph update must not overwrite a requested cancel, and a node's declared attempt bound must reach its outbox entry.

## 2. The code at `dfe426c`, verified

Paths are under `crates/`.

- **No state for the park.** `AgentGraphNodeStatus` (`rakka-agent-workflow/src/graph_state.rs:397`) and `AgentGraphWaitReason` (`:463`) have no member for it. Both derive `Serialize`/`Deserialize` with kebab-case names and neither is `#[non_exhaustive]`. `AgentGraphNodeState` (`:256`) carries `status`, `wait_reason: Option<_>`, `error_code: Option<String>`, `scheduled_effect_ids: Vec<AgentEffectId>`, `attempt: u32`, all later fields `#[serde(default)]`.
- **A failed effect fails the run.** `EffectFailed` carries a disposition attribute (`effect_bridge.rs:929`, `AgentGraphEffectFailureDisposition` at `:644`: `RetryScheduled`, `Exhausted`, `Terminal`). `apply_effect_failed_command` (`:1392`) requires the node to be `Waiting` on the effect's own wait reason (`:1419-1426`; `apply_effect_completed_command` has the same guard at `:1372-1379`), records the code on `RetryScheduled`, and on `Exhausted`/`Terminal` calls `fail_waiting_node` (`graph_scheduler.rs:647`), which marks the node `Failed`, sets `terminal_status = Failed`, and cancels every unresolved node. There is no "fail this node and continue" in the graph model. The `accept_and_apply_*` entry points (`:996-1046`) apply the command whatever the inbox's acceptance, duplicate or not, so a replayed command must be made harmless by the apply rules, not by dedupe.
- **Nothing fences a command by generation.** The guards above check only "waiting on the kind's reason and the id is listed", so once a node lists two effect ids nothing stops a late command for the older one from moving the node.
- **Recovery re-links only due effects.** `reconcile_recovered_graph_effects` (`runtime.rs:795-799`) feeds `reconcile_recovered_effects` the inbox's `due_effects()` (`outbox.rs:277-283`), and an `Exhausted` entry is never due (`model.rs:612-620`). A doubtful call dispatched and exhausted before the run actor recovered from a crash in the outbox-write-to-graph-persist window is never linked to its node; that is the brief's own "crash before the reply landed" case, and today it ends `UnknownEffect`.
- **One identity per node, no generation.** `effect_from_node` (`effect_bridge.rs:1680`) builds the effect id `graph-effect:{identity}`, the dedupe key `graph-effect-dedupe:{identity}` and the idempotency key `graph-effect-idempotency:{identity}` from `effect_identity` (`:1989`: run, plan fingerprint, node, loop instance, kind, target class). The identity is a plain string, not a hash. `ensure_node_can_schedule_effect` (`:1588`) admits a `Running` node, or a `Waiting` one on the same wait reason whose `scheduled_effect_ids` already lists the id. A second schedule of the same node is therefore a dedupe hit on the same outbox entry.
- **The bridge knows no safety class.** Nothing in `rakka-agent-workflow` reads a safety class; the brief's rule ("bare key for a non-idempotent call, `#g{generation}` for an idempotent one") is the agent-run path's (`rakka-agent/src/effect.rs:540-552`, `:2213-2222`), and that path gives the bare key to every class but `Idempotent`. `rakka-agent` defines the attribute vocabulary a dispatch ticket carries: `ATTR_AGENT_EFFECT_SAFETY_CLASS = "agent_effect_safety_class"` with the class labels (`read-only`, `idempotent`, `non-idempotent`, `reconcileable`), `ATTR_AGENT_EFFECT_MAX_ATTEMPTS = "agent_effect_max_attempts"`, `ATTR_AGENT_EFFECT_GENERATION` (`effect.rs:2194-2209`).
- **No attempt bound reaches the outbox.** `agent_effect_to_outbox_command` (`rakka-agent-workflow/src/outbox.rs:309`) sets no retry policy, so every node effect gets `RetryPolicy::default()`, three attempts (`rakka-workflow/src/model.rs:414`). The compiled node carries `retry_policy_ref: Option<ArtifactRef>` (`compiled_plan.rs:296`), a reference the bridge cannot resolve, not a value. The brief's "declared `maxAttempts`" exists nowhere in the plan model today.
- **The outbox mutators are unguarded, as the brief says.** `record_definitive_failure` (`model.rs:671`) and `record_failure` (`:646`) check no prior status. This design does not tighten them; section 5.1 says why.
- **`update_graph_state` writes `Running` over `Cancelling`** (`runner.rs:880`, the `None` arm at `:914-917`), exactly as the addendum states; `cancellation` survives because the write copies the record.
- **The schema version is written and never read.** `CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION` is 1 (`graph_state.rs:20`), stamped by `initialize_state` (`graph_scheduler.rs:180`) through `AgentGraphRunState::new` (`graph_state.rs:86`), and no reader compares it. `docs/rakka-compatibility.md:198` lists it, held by `crates/rakka-agent/tests/compatibility_currency.rs:152`; `crates/rakka-agent-workflow/tests/api_compatibility.rs:331-437` round-trips it.
- **The command kind is persisted as opaque bytes.** `AgentCommandKind` (`facade.rs:194`) is serialized into the inbox entry's `payload: Vec<u8>` on accept (`rakka-agent-workflow/src/inbox.rs:313-322`), and loading `WorkflowState` never decodes a stored payload; the one in-tree reader of stored payloads (`rakka-a2a/src/runsync.rs:303-314`) warns and skips an entry it cannot decode. So a new command kind is a persisted value, but not one an older binary fails on; the wait reason, inline in `AgentRunState.graph_state` (`domain.rs:346`), is the only value this change adds that an older binary cannot decode. The rollout plan says so.
- **The shared outbox conversion has a second caller.** `agent_effect_to_outbox_command` is also called by `rakka-agent/src/dispatch.rs:475-491`, which reads `agent_effect_max_attempts` after the conversion and overrides the policy with its own backoff (default one attempt). `rakka-agent` depends on `rakka-agent-workflow`, not the reverse (`rakka-agent/Cargo.toml:31`), so attribute names the bridge reads must be defined in `rakka-agent-workflow` and matched by `rakka-agent`.
- **The query index copies the wait reason label.** `postgres_query.rs:734` writes `wait_reason` as text and `:1276` filters by it; `graph_wait_reason_label` (`:1575`) is `as_label()`. A new label needs no schema change.

## 3. Goals, non-goals, anti-goals

Goals:

- A node whose effect may have run can be parked in the graph state itself: still `Waiting`, with a wait reason that says a person decides, the cause recorded, the effect still listed, the run still `Running`, successors blocked, and nothing due.
- A person's decision resolves the park through the durable inbox: "not executed" re-offers the node, "abandon" fails it under a bounded code.
- A re-offer opens a new effect generation on the node: a new effect id, a new outbox entry with a fresh attempt budget, the first entry left as the record of the doubtful call, and an idempotency key that still deduplicates a non-idempotent re-send against the first call.
- A rollout plan under which a mixed-version fleet never meets a value it cannot decode, and a future version can be refused with a typed error rather than a decode failure.
- A requested cancel survives any non-terminal graph write.
- A node's declared attempt bound governs its automatic retries.

Non-goals:

- `Completed` as a decision. What a completed node puts on its output port is undefined; the decision type is exhaustive so adding it later is a compile-time event.
- A new top-level node status (owner decision).
- Tightening `record_definitive_failure` or `record_failure`. The host's A20 reads them until it moves its pin; after that the first stays the way a dispatcher records a doubtful call and the second is no longer needed for a re-offer.
- Any change to `rakka-agent`'s run-effect reconciliation (`AgentReconciliationDecision`, `rakka-agent/src/checkpoints.rs:552`), which stays the agent-run decision set. The graph decision type lives in the graph model, as #71 item 3 asks.
- A new `AgentRunActorCommand` arm. The host drives the graph through the bridge and `update_graph_state` on its own entity; the actor is untouched (#71 item 1 is separate).
- Compensation as a decision (the brief excludes it).

Anti-goals:

- A re-send that double-executes a non-idempotent call because its key changed.
- A park that a run cancel, a terminal transition, or a snapshot count cannot see.
- A version-2 value written by any automatic path. Only the host's explicit `Reconcile` disposition and decision command write one.
- Resolving an effect id to the wrong generation.
- Changing an existing effect id, dedupe key or idempotency key for generation 0: every stored id stays what it is.

## 4. Constraints

- MSRV 1.88, workspace lints: every public item documented, clippy clean under `-D warnings`.
- `AgentGraphNodeStatus`, `AgentGraphWaitReason`, `AgentGraphEffectFailureDisposition`, `AgentCommandKind` and the new decision type stay exhaustive (not `#[non_exhaustive]`): a consumer's exhaustive match breaks at compile time, which the brief calls the right failure. Every addition is named as source-breaking in the changelog.
- Stable codes do not change. New codes are named in section 5.
- Every new persisted field has a serde default; every new persisted enum value is listed in section 6 with who writes it.
- The host's classifier of a lost compare-and-set (batch 1) is untouched.

## 5. Design

### 5.1 The park

`AgentGraphWaitReason::Reconciliation` (serde and label `reconciliation`). A parked node is `Waiting` with `wait_reason = Some(Reconciliation)`, `error_code = Some(cause)`, its effect id still in `scheduled_effect_ids`, `attempt` unchanged. Nothing else on the node or the run moves: `terminal_status` stays `None`, the run stays `Running`, successors stay blocked because the node is unresolved, `cancel_graph_run` cancels it like any waiting node (`cancel_unresolved_nodes`, `graph_scheduler.rs:1131`), and the snapshot's waiting count includes it.

**Entry.** `AgentGraphEffectFailureDisposition::Reconcile` (label `reconcile`). On `Reconcile`, `apply_effect_failed_command` calls a new scheduler transition, `park_waiting_node(plan, state, node_id, from_reason, cause_code, now)`: `Waiting(from_reason)` to `Waiting(Reconciliation)`, `error_code = Some(cause_code)`, `scheduler_revision += 1`, `blocked_reason = None`. The cause code is the `error_code` the `EffectFailed` command already carries: the dispatcher's own outcome-unknown code (what the host puts in the entry's `last_error` today). It must be non-empty; the builder applies the same length bound the crate uses for other error codes, if it has one, and otherwise leaves it unbounded in length but documents the recommended codes.

**Precondition on the entry (call 8).** The bridge reads the effect's outbox entry from the inbox's own `WorkflowState` (`AgentRunInbox::inner().state()`, `rakka-agent-workflow/src/inbox.rs:269`; the entries live in the same record) and refuses `Reconcile` unless the entry exists and is terminal and never due, `Exhausted` or `Cancelled`, with a new typed error `EffectNotTerminal { effect_id, status }`. A park over a live entry would let the outbox keep retrying the doubtful call under the person's feet and, after a re-offer, leave two generations live. The park itself never touches the entry: the dispatcher owns entry status, the bridge owns node status, as today. Two contract points for the dispatcher, stated in the docs: an outcome-unknown exhaustion is reported with `Reconcile` as its disposition, never as `Exhausted` first (an `Exhausted` fails the run irrevocably and the later `Reconcile` finds a failed node), and the entry is recorded exhausted before the command is sent.

**What the other commands do to a parked node (call 7).** The guard on `apply_effect_failed_command` and `apply_effect_completed_command` widens from "waiting on the kind's own reason" to "waiting on the kind's own reason or on `Reconciliation`", for the current generation only (section 5.3):

- `EffectCompleted`: completes the parked node with its output, as it completes a node waiting on the effect. The doubt is resolved by the real answer; the person is no longer asked.
- `EffectFailed` with `Exhausted` or `Terminal`: fails the parked node, as it fails a node waiting on the effect. A definitive failure resolves the doubt the other way.
- `EffectFailed` with `RetryScheduled`: a no-op on a parked node; the entry was terminal when the park was accepted, so a retry report is stale.
- `EffectFailed` with `Reconcile`: a no-op when the cause is the same; the cause is replaced and the revision moves when it differs.
- `Failed` and `Cancelled` nodes stay no-ops, as today.

### 5.2 The decisions

A graph-level decision type in `rakka-agent-workflow`:

```rust
pub enum AgentGraphReconciliationDecision {
    /// The call never ran: re-offer the node under a new effect generation.
    NotExecuted,
    /// Give the node up under a bounded code; the run fails as any failed node fails it.
    Abandon { error_code: String },
}
```

A new inbox command kind, `AgentCommandKind::GraphReconciliationDecided { effect_id: AgentEffectId, decision: AgentGraphReconciliationDecision }`, keyed by effect id like `EffectFailed`, with a bridge builder `reconciliation_decision_command(metadata, effect_id, decision)` and an apply path `accept_and_apply_reconciliation_decision(plan, state, command, inbox)` shaped like `accept_and_apply_effect_failed` (`effect_bridge.rs:1021`): accept into the inbox first (durable, deduplicated by the command's id), then apply.

Apply rules, resolving the node with `node_id_for_effect` (`:1488`) and fencing by generation (section 5.3):

- The effect id belongs to a superseded generation: a no-op transition, whatever the node's status. A replayed decision after a re-offer lands here.
- Current generation, `Waiting(Reconciliation)`: apply.
- Current generation, `Completed`, `Failed` or `Cancelled`: a no-op, the node has resolved.
- Current generation, `Running` or `Waiting` on another reason (never parked, or the park was resolved by a real answer and the node is running again): `InvalidNodeStatus`.
- An effect id the node never carried: `UnknownEffect`, as today.

`NotExecuted` runs a new scheduler transition, `resume_parked_node(plan, state, node_id, now)`: `Waiting(Reconciliation)` to `Running`, `wait_reason = None`, `error_code = None`, `effect_generation += 1`, `attempt` and `started_at` unchanged, revision moved. The node is now a running node the host drives as it drives any other: its next `schedule_node_effect` mints the next generation (section 5.3).

`Abandon { error_code }` calls `fail_waiting_node(plan, state, node_id, Reconciliation, error_code, now)`: the node is `Failed` with the code, the run's `terminal_status` is `Failed`, unresolved nodes are cancelled. The docs recommend `reconciliation-abandoned` as the code; the host chooses.

`Completed` is not built. The type stays exhaustive so adding it later breaks every match at compile time, which is what the host asked for.

### 5.3 The generation

`AgentGraphNodeState.effect_generation: u32`, `#[serde(default)]`, zero for every node that exists today. It moves only in `resume_parked_node`.

Identity. For generation 0 nothing changes: the effect id, dedupe key and idempotency key are the strings they are today, so no stored id moves. For generation `g >= 1`, `effect_identity` gains the suffix `;gen={g}`, so the effect id is `graph-effect:{identity};gen={g}` and the dedupe key `graph-effect-dedupe:{identity};gen={g}`: a new outbox entry with a fresh `RetryPolicy`, while the generation-0 entry stays `Exhausted` as the record of the doubtful call, listed on the node beside the new id (`scheduled_effect_ids` keeps both; `node_id_for_effect` resolves either).

The idempotency key follows the agent-run rule, by the target's safety class read from the node target's attributes under `agent_effect_safety_class`:

- `idempotent`: `graph-effect-idempotency:{identity};gen={g}`, a fresh key, so a target that deduplicates by key executes the re-send.
- `non-idempotent`, `reconcileable`, `read-only`, or no class attribute: `graph-effect-idempotency:{identity}`, the bare generation-0 key, so a target that deduplicates by key refuses to run the call a second time. An absent class is treated as non-idempotent, the safe default (call 3).
- Any other value: the schedule is refused with `InvalidCompiledPlan` naming the attribute.

**Generation fencing (call 6).** The generation of an effect id is read back from the id itself, which the bridge minted: `;gen={g}` at the end, or 0 when absent. A command keyed by an effect id whose generation is not the node's `effect_generation` is superseded and applies as a no-op transition, for `EffectCompleted`, every `EffectFailed` disposition, and the decision. That covers a replayed `Reconcile` or `Exhausted` for generation 0 while generation 1 is in flight, which would otherwise re-park or fail the run, and a late result for the call the person declared not executed, which is reported by the dispatcher if it wants to but moves no graph state. The window between `resume_parked_node` and the next schedule is fenced the same way: the node's generation has already moved, so a generation-0 command in that window is superseded. Only one generation is ever live.

The bridge derives the generation from the node state inside `schedule_node_effect` (which has the state); the schedule request does not carry it, so a caller cannot mint a generation the node has not opened. The builder decides whether the public `effect_from_node(plan, request)` gains a state parameter or a sibling that takes one; either way the generation-0 behaviour of the existing signature is unchanged. `ensure_node_can_schedule_effect` keeps its shape: a `Running` node may schedule, and a re-schedule of the current generation is still the dedupe hit it is today. `reconcile_recovered_effects` (`:770`) must re-link a generation-suffixed id to its node after a crash in the window between the outbox write and the graph persist; the builder proves it.

### 5.4 The declared attempt bound

The bound travels as the target attribute `agent_effect_max_attempts` (`rakka-agent`'s vocabulary), in two ways that end in the same place:

- declared in the plan on the node target's attributes, which the host sets at compile time;
- set per schedule by a new `AgentGraphEffectScheduleRequest::max_attempts(u32)`, which the bridge writes onto the effect target's attributes and which wins over the plan's value.

`agent_effect_to_outbox_command` reads the attribute leniently: a positive integer sets the outbox command's retry policy to `RetryPolicy::new(max_attempts, <default base delay>, <default max delay>)`, keeping the default delays and replacing only the attempt count; an absent or unparsable value leaves today's default of three. The bridge validates strictly before it schedules: a value that is not a positive integer is refused with a typed error naming the attribute, so a graph effect never reaches the conversion with a bad bound (call 10). `rakka-agent`'s dispatch path, which calls the same conversion and then overrides the policy with its own backoff, is unchanged in effect. The attribute names are defined in `rakka-agent-workflow` (the dependency runs from `rakka-agent` to it) and `rakka-agent` keeps matching strings; the builder decides whether `rakka-agent` re-exports or equality-tests them. The attribute is a string on an already-persisted map, so it is harmless to any reader.

### 5.5 Keep `Cancelling` across a non-terminal graph update

In `update_graph_state`'s `None` arm: `next.status` stays `Cancelling` when `previous_status` is `Cancelling`, otherwise `Running`, as today. A terminal update keeps its present meaning: `Completed`, `Failed` or `Cancelled` by the graph's terminal status. `cancellation` is untouched either way.

### 5.6 What the host retires, named

The derived park (three facts), the host test pinning the two outbox mutators, the once-per-step cap on "not executed", and the two-reader `Cancelling` workaround. The report-back names each with the commit that makes it unnecessary.

### 5.7 Recovery re-links every graph effect, not only the due ones (call 9)

`reconcile_recovered_graph_effects` feeds recovery every outbox entry that names a graph node (the self-describing attributes the re-link already reads), whether or not the entry is due, so an effect that was dispatched and exhausted while the run actor was down is linked to its node and can be parked. Linking an entry that is already terminal only records the id on the node; it does not change the node's status, and a node that has resolved is left alone. The brief's "crash before the reply landed" is otherwise the one case the native park could not reach.

## 6. Rollout

The persisted values this change adds, and who writes each:

| Value | Where it is persisted | Written by |
|---|---|---|
| `AgentGraphWaitReason::Reconciliation` | `AgentRunState.graph_state` node states | only `EffectFailed` with disposition `Reconcile` |
| `AgentCommandKind::GraphReconciliationDecided` | durable inbox entries in `WorkflowState` | only the decision command |
| `AgentGraphNodeState.effect_generation` | node states, `#[serde(default)]` | `resume_parked_node`; absent means 0 |
| effect ids and dedupe keys with `;gen={g}` | outbox entries, node `scheduled_effect_ids` | the first schedule after a re-offer |
| target attribute `agent_effect_max_attempts` | effect target attributes | a plan that declares it or a request that sets it |

Only the first is a value an older binary cannot decode: `AgentGraphWaitReason` is an exhaustive enum inline in the run state. The command kind is stored as an opaque payload that loading never decodes (section 2), so it fails only a consumer that decodes stored inbox payloads itself, and the host should say whether it does. The other three are additive and harmless. Neither of the first two is ever written by an automatic path: a timer, a recovery, a retry or a scheduler pass never produces them. So a fleet that only reads version-2 binaries holds no version-2 value until a caller sends one.

Versioning. `CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION` moves to 2. `initialize_state` stamps 2 on every new graph. A graph state created at 1 keeps saying 1 until a park or a resume writes it, which raises it to 2 at that write. Every reader that validates a graph state (`validate_plan_state` in the scheduler, `graph_scheduler.rs:732`, and in the bridge, `effect_bridge.rs:1102`; `update_graph_state` before it persists) refuses a state whose version is greater than the current one with a new typed error, `AgentGraphSchedulerError::UnsupportedSchemaVersion { found, supported }` (code `graph-state-schema-unsupported`), instead of letting an unknown value surface as a decode failure. That gate does nothing for this transition's older binaries, which have no gate; it is what the next bump will rely on.

Staging for the host, two pin bumps:

1. Move every replica to a binary that reads version 2. The host keeps its derived park, sends no `Reconcile` disposition and no decision; nothing version-2 is written, and a version-1 replica that still exists decodes every record.
2. When every replica reads version 2, the host starts sending `Reconcile` and the decisions, declares its attempt bounds, and retires the derivation, the mutator pin, the cap and the `Cancelling` workaround.

`docs/rakka-compatibility.md` gets the version row at 2 and a bullet under "what each bump requires" stating the two-step and the table above; `compatibility_currency.rs` holds the row.

## 7. Proofs

Each is a deterministic test that fails on `dfe426c` and passes after.

Scheduler and bridge (`crates/rakka-agent-workflow/tests/graph_scheduler.rs`, `effect_bridge.rs`):

- `EffectFailed` with `Reconcile` parks the node: `Waiting`, `Reconciliation`, the cause in `error_code`, the effect id listed, run not terminal, successors not runnable, the outbox entry untouched; a second `Reconcile` with the same cause is a no-op and one with another cause replaces it; `RetryScheduled`, `Exhausted`, `Terminal` on an unparked node behave as today.
- `Reconcile` over an entry that is `Pending`, `Failed` with budget, `Dispatching` or `Dispatched` is refused `EffectNotTerminal`; over an `Exhausted` or `Cancelled` entry it parks.
- On a parked node: `EffectCompleted` completes it with its output; `Exhausted` and `Terminal` fail it and the run; `RetryScheduled` is a no-op.
- Generation fencing: after `NotExecuted` and the generation-1 schedule, a replayed generation-0 `Reconcile`, `Exhausted`, `EffectCompleted` and decision are each a no-op that leaves the node `Waiting` on generation 1; a generation-0 command in the window between the resume and the next schedule is a no-op too; generation 1's own completion and failure apply.
- Recovery after a crash between the outbox write and the graph persist re-links the effect to its node when the entry is still due and when it was exhausted in the meantime; in the second case a following `Reconcile` parks the node instead of answering `UnknownEffect`.
- `NotExecuted` resumes the node `Running` with generation 1 and no cause; the next schedule returns `Scheduled` (not `Duplicate`) for an effect id ending in `;gen=1`, a new entry with three attempts by default, while the generation-0 entry stays `Exhausted` and not due; both ids are listed on the node.
- The idempotency key per safety class: suffixed for `idempotent`; bare for `non-idempotent`, `reconcileable`, `read-only` and absent; an unknown label is refused.
- `Abandon` fails the node with its code and the run with `Failed`; unresolved nodes are cancelled.
- A decision on a node that was never parked is `InvalidNodeStatus`; a replayed decision after the node resolved is a no-op; a decision naming an effect the node never carried is `UnknownEffect`.
- `cancel_graph_run` cancels a parked node.
- A crash between the generation-1 outbox write and the graph persist re-links the effect to its node on recovery.
- A graph state at version 3 is refused with the typed error by the scheduler, the bridge and `update_graph_state`; a version-1 state parks and is persisted at 2; a version-1 JSON fixture without `effect_generation` decodes with generation 0.
- The attempt bound: declared on the target, the outbox entry carries that many attempts; set on the request, the request wins; a non-positive value is refused.
- The snapshot's waiting count and the query model's wait-reason label (`reconciliation`) see a parked node.

Runner (`crates/rakka-agent-workflow/tests/step_runner.rs`): after `request_cancellation`, a non-terminal `update_graph_state` leaves `status == Cancelling` and `cancellation` set; a terminal update still ends the run.

Compatibility (`api_compatibility.rs`, `compatibility_currency.rs`): the version round-trips at 2 and the doc row matches.

## 8. Left to the builder

- Exact names of the two scheduler transitions and the error variant; where the cause-code bound lives.
- Whether `effect_from_node` gains a state parameter or a sibling; the generation-0 path must keep its signature's behaviour.
- The shape of `agent_effect_to_outbox_command`'s attribute parsing and its error.
- Test fixtures, including a plan whose target declares a safety class and an attempt bound.
- Module docs for `graph_state`, `effect_bridge` and `graph_scheduler` that describe the park, the decisions and the generation; there is no separate product document for the graph model, so `docs/rakka-compatibility.md`'s Agent Domain section and `CHANGELOG.md` carry the user-facing statements.

## 9. Report-back obligations

For each ask: what changed, the commit, the tests, and where the code at `main` contradicted the brief. Known contradictions to name: the bridge has no safety class, so the rule needs the class on the target's attributes; `maxAttempts` exists nowhere in the plan model, so it travels as a target attribute; abandoning a node fails the run, because the graph model has no per-node failure that lets a run continue; recovery re-linked only due effects, so the brief's own crash case could not be parked; the host's two outbox mutators are not tightened, but `Reconcile` now requires the entry to be terminal, which the host's A20 already guarantees in order. Also name the dispatcher contract (one `Reconcile`, never `Exhausted` first), what the host retires (section 5.6), the two-step rollout, and the question the host must answer for the rollout: whether it decodes stored inbox payloads itself.
