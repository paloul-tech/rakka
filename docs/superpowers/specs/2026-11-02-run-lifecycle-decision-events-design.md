# Run lifecycle decision events: checkpoints, unknown outcomes, waits and the run's end on the run's own decision stream

Status: written 2026-11-02, approved by the owner 2026-11-03. Branch `run-lifecycle-decision-events`, off `main` at `47f31ec`.

Brief: issue #119 (the rakka.host run-events brief, answering Studio's ask H7). It was drafted against `bc0c224`, the host's pin. Nothing between `bc0c224` and `47f31ec` touches `rakka-agent`, so every site below is cited at `47f31ec` and holds at the pin too.

Owner decision (2026-11-02): **a staged switch.** Rakka can read every new kind as soon as this ships. It writes them only when the host turns them on with a setting on the run entity. The host turns it on once every process that loads the run store or reads the sink runs a binary that reads them. Until then, no process meets an event it can't decode.

Owner ruling (2026-11-03): **an ended run's ambiguous promotion or claim append records nothing.** Nothing in Rakka can settle that park (section 2), so an event would open a "Needs You" item no one can close. The defect is filed as #120, and the event belongs with its fix.

Approved in conversation (2026-11-02), as played back to the owner:

- the kind vocabulary in section 5.1: four existing kinds reused, six new ones;
- one optional checkpoint reference, one optional bound-effect reference, an optional outcome and an optional run status on the event (section 5.2);
- `request-reconciliation` answers ask 2 (narrowed by the 2026-11-03 ruling to live runs, which always have a checkpoint);
- no resume events;
- the ring stays at 32 (section 5.7);
- one branch and one PR, with one commit and its own proof per ask.

Calls made by the author, each marked where it lands; the owner can veto any of them:

1. Transitions record into a non-persisted draft list. One new hook on the exchange participant, which the host calls on every write path, turns the drafts into events. The switch is applied there, once, instead of being threaded through about 25 functions (section 5.4).
2. The four existing recording sites move onto the same mechanism, and the `decision_events: bool` parameter leaves the five functions that carry it (section 5.4).
3. Waits are derived at the transition boundary, by comparing the state before and after, instead of being recorded at each of the 12 `turn_rest` call sites (section 5.4).
4. An event's schema version is the version of the vocabulary that wrote it: `Version1` stamps 1 on every event, `Version2` stamps 2 on every event. Stamping by kind would keep writing version 1 forever, and the N/N+1 window of the next bump would refuse it (section 5.3). Revised after the builder check.
5. Sources: a decision a principal submits is `human`; a timer, a policy gate, a wind-down and a run's end are `deterministic-policy`. `authorization-service` is not used, because a resolver's `principal_type` is a free string and mapping it would be a guess (section 5.1).
6. The bound effect reuses `AgentCheckpointBoundEffectView`, the shape Studio already reads from the snapshot, rather than a new type (section 5.2).
7. The wait reason has four labels: `budget-top-up`, `awaiting-children`, `awaiting-handoff`, `winding-down` (section 5.1).

Calls made after the builder check (2026-11-03), each marked where it lands; the owner can veto any of them:

8. **`winding-down` means the wind-down began, once per run.** A `Cancelling` run whose effect goes `Indeterminate` rests `WaitingForReconciliation`, then returns to `Cancelling`. Recording each return would re-record the same identity and leave a hole. The predicate is the one `settle_run_disposition` uses, and `terminal_reason` is never cleared, so it is entered once (section 5.5, ask 4).
9. **The owed outbox gets a byte bound as well as its count of 32.** A full ring of the larger events could push a run record past `AGENT_RUN_MATERIALIZED_MAX_BYTES` while the sink is down, and the run would then refuse transitions over telemetry. The bound is sized from measurement to fit the growth reserve's slack, so the admission budget doesn't change (section 5.7).
10. **The `goal-evaluation` slot gains the evaluation effect's slot.** Today a second evaluation in the same turn re-derives the first one's identity: an existing hole this change's proof would otherwise have to exclude (section 5.6).
11. **A draft captures its positional fields when it is pushed:** turn, phase, revisions, context snapshot, telemetry and time. Only the sequence is assigned at `seal`. So `iterate` keeps the turn it was decided in, and a gated call's `request-approval` carries the phase `call-tools` carries (section 5.4).
12. **New events always carry a `reason_code`.** On `resolve-checkpoint` and `reconcile` it repeats the outcome label. The coordination replay (`AgentCoordinationEvent::from_decision`) projects only the kind and `reason_code`, so this is how a reader there tells granted from denied. That path is otherwise out of scope (section 6).
13. **No catch-all `#[serde(other)]` kind.** A run record re-persists the events it owes. A catch-all would silently rewrite a future kind as "unknown", so the staged switch is the mechanism for every future kind too (section 5.3).

## 1. Intent

Studio is building a "Needs You" inbox: one item per run that is waiting on a person. Today the run's decision stream says nothing when a run opens an approval or authorization checkpoint, holds an effect whose outcome is unknown, or ends. Studio learns each from the run snapshot only, by polling every 60 seconds, so a new approval can take a minute to appear. Compiled workflows don't have the gap, because the host drives them and writes their events itself. Agent runs are driven by Rakka's loop, so only the loop can put these events in the run's sequence.

This change makes the loop record one event for each of those transitions, in the run's own sequence, after the transition persists, through the sink the host already implements. It changes no transition. It adds event kinds, so it ships behind a switch the host turns on after its roll.

## 2. The code at `47f31ec`, verified

Paths are under `crates/rakka-agent/src/` unless stated.

**How a decision is recorded today**

- `AgentLoopState::record_decision` (`loop_runtime.rs:573-613`) assigns the next `decision_sequence` and assembles the event. It deduplicates only against events still owed in the outbox, and keeps a ring of 32 (`AGENT_RUN_DECISION_OUTBOX_CAPACITY`, `:137`) that drops the oldest owed event and counts the drop. A draft whose identity can't be formed is dropped and counted too. Either way the sequence is consumed.
- An event's identity is `(tenant, agent, run, "decision", turn, slot)` (`observability.rs:455-465`). The slot must be a valid identity segment: not empty, no control characters, no scope or persistence separator, at most `AGENT_IDENTITY_MAX_LENGTH` (`identity.rs:66-89`).
- `flush_decision_events` (`run.rs:7739`) appends owed events after the transition persists. It treats a `Duplicate` answer as flushed.
  - **Hazard this design must not trigger.** Suppose a later transition re-records an identity whose first copy was already flushed. The run gives it a fresh sequence, but the sink answers `Duplicate` and stores nothing at that sequence. The stream now has a hole, and a replay reader is refused at it.
  - Three of the existing slots fire once per turn by construction: `start`, `tools`, and either `iterate` or `proposal`. The fourth, `goal-evaluation` (`run.rs:6113-6121`), does not. `has_open_goal_evaluation` (`loop_runtime.rs:1292-1304`) refuses only an evaluation that is unreported or still outstanding, so a second evaluation in the same turn re-derives the first one's identity. That is an existing hole, fixed in section 5.6. Every new site must keep the once-only property.
- **The owed events live in the run record** (`decision_outbox`, `loop_runtime.rs:388`), and nothing bounds their bytes.
  - The growth-reserve proof (`the_growth_reserve_covers_the_maximal_working_set`, `run.rs:10689`) doesn't count them against `AGENT_RUN_MATERIALIZED_MAX_BYTES` (176 KiB, `run.rs:160`).
  - With maximal identifiers (`AGENT_IDENTITY_MAX_LENGTH` = 256 per segment), an event runs to kilobytes. That's a reviewer's estimate, not a measurement.
  - So a full ring can approach the bound on its own while the sink is down. The four new fields add to each event.
- **An old binary loads these events whenever it loads the record.** A `bc0c224` binary decodes `AgentDecisionKind` strictly, so it can't load a run record whose outbox still owes an event of a new kind. A `bc0c224` writer that re-persists a record drops the new fields from the events it owes.
- **The switch today** is `self.decisions.is_some()` (`run.rs:7451`, `:7959`). It is passed by hand as `decision_events: bool` into five functions: `advance_once` (`:2621`), the model-consult step (`:2708`), the tool fan-out (`:3291`), `decide_continuation` (`:4191`) and `evaluate_goal` (`:6047`). None of the functions the brief names receives it.
- **Three paths write a run's state,** all through `AgentExchangeHost` (`choreography.rs`):
  - `initiate` (`:2118`), which the run entity's six closures use (`run.rs:7715`, `:7807`, `:8007`, `:8356`, `:8546`, `:8637`);
  - `accept` (`:2148`), which calls `AgentRunParticipant::apply` (`run.rs:6474`);
  - the initiator's settle (`:2264-2266`), which calls `AgentRunParticipant::settle`.

  Some brief sites are reached only through the last two. A run-cancel exchange reaches `cancel` through `accept_run_cancel` (`run.rs:6523`). A refused proposal reaches `terminate` through `settle_proposal` (`:6355`). So a flag set in the entity's closures alone would miss them.

**The event and its kinds**

- `AgentDecisionKind` (`observability.rs:232-253`) has ten variants: `continue`, `call-tools`, `submit-result`, `wait`, `complete`, `fail`, `request-approval`, `request-authorization`, `reconcile`, `evaluate`. It is `#[non_exhaustive]` and kebab-case. Its doc comment says the coordination kinds "arrive with their phases"; they never did.
- `AgentDecisionEvent` (`:391-432`) derives `Deserialize` without `deny_unknown_fields`. Its private `schema_version` is stamped with `CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION` (`schema.rs:163`, value 1). The compatibility doc holds the row at `docs/rakka-compatibility.md:188`.
- `AgentDecisionDraft` (`:320-380`) carries the kind, source, slot, selected tools, safety class and reason code. `reason_code` is truncated at 64 bytes. `record_decision` reads turn, phase, revisions, context snapshot and telemetry from the loop state at the moment it is called. `iterate` relies on that: it is recorded just before `begin_turn` advances the turn and clears the context (`run.rs:4206-4217`).
- The schema policy's default window for a record is N/N+1: the current version and the one before it (`AgentSchemaCompatibility::n_plus_one`, `schema.rs:490`).
- **A second reader of the events is the coordination replay.** `AgentCoordinationEvent::from_decision` (`events.rs:577-600`) projects an event as `AgentCoordinationEventKind::Run(kind)` (`events.rs:368`) with `detail` = `reason_code`, and drops every other field. `replay_run_coordination_events` serves that projection, and the host wires it through `RakkaAgentA2AService::with_decision_events`. So a new kind is also a new value on that A2A wire.

**Checkpoint transitions** (all in `run.rs`)

- **Open:** `open_effect_checkpoint` (`:5449`). It has five callers: the gated tool fan-out (`:4017`), `repair_park` (`:4085`), `record_effect_result` (`:4397`), the re-offer of a gated effect's new generation in `apply_indeterminate_resolution` (`:5393`), and `evaluate_goal` (`:6126`).
  - The checkpoint id is `id_for_effect(effect, generation, kind)` (`checkpoints.rs:800`).
  - The SLA sets `due_at` and `expires_at`. A reconciliation checkpoint never gets `expires_at` (`:5516-5521`).
- **Resolve:** `resolve_checkpoint` (`:5542`). A deduplicated decision returns at `:5571` before any consequence. Otherwise there are six outcomes:
  - `Granted` (`:5583`) and `Denied` (`:5592`) close an approval-family checkpoint;
  - `EffectResolution` (`:5599`), `Compensate` (`:5611`) and `Abandoned` (`:5623`) close a reconciliation checkpoint;
  - `Escalated` (`:5649`), a person's escalation, keeps the checkpoint open.
- **Reconcile without a checkpoint decision:** `resolve_indeterminate_effect` (`:5169`) is the effect-layer command. It applies a confirmed resolution, then retires the bound checkpoint with `drop_reconciliation_checkpoint` (`:5198`).
- **Timers:** `fire_checkpoint_timers` (`:6145`). `AgentCheckpoint::on_timer` (`checkpoints.rs`) expires an approval-family checkpoint past `expires_at`. It escalates a checkpoint only when it is `Open`, past `due_at`, and has an escalation target.
- **Run cancel:** `cancel` (`:5416`) calls `cancel_open_checkpoints` (`loop_runtime.rs:1126`). That marks every waiting approval-family checkpoint `Cancelled` (`AgentCheckpoint::cancel` acts only on a waiting one) and keeps reconciliation checkpoints.

**Unknown outcomes**

- `apply_effect_outcome` sets a generation `Indeterminate` at `:4773` (a handoff send's `Exhausted`) and `:5020` (an `Indeterminate` outcome). Both are reached only from `record_effect_result` (`:4371`).
- On a live run, `record_effect_result` opens the reconciliation checkpoint in the same transition (`:4392-4404`).
- On a run that has already ended, a memory promotion's or claim append's generation can park with no checkpoint (`:4384-4390`; `needs_park_repair` excludes it, `:756`).
  - The comment there says the parked generation is "resolvable by `ResolveIndeterminateEffect`". It isn't: `apply_indeterminate_resolution` refuses a terminal run with `Terminal` (`:5245-5247`).
  - So nothing in Rakka can ever settle that park. This is an existing defect, beyond this brief, filed as #120. `an_indeterminate_post_terminal_outcome_opens_no_checkpoint` (`tests/private_memory_promotion.rs:1568`) builds the shape.

**The run's end and its waits**

- `terminate` (`:1292`) returns early on a run that is already terminal. It has nine callers, including both exchange paths.
  - `AgentRunTerminalReason::status()` (`:530-552`) yields `Completed`, `Failed`, `Cancelled`, `HandedOff` or `Superseded`.
  - `code()` (`:557`) yields the stable reason code.
- **Top-up wait:** `park_for_top_up` (`:1364`) parks on a top-up. The loop state numbers each request (`loop_runtime.rs:1170`, sequence = `top_ups + 1`).
- **Children wait:** `turn_rest` (`:4123`), called from 12 sites, rests a turn in `AwaitingChildren` for a fan-in or for a handoff awaiting its target.
- **Wind-down wait:**
  - `settle_run_disposition` (`:5119`) re-sets `Cancelling` on every pass while children or settlement are outstanding (`:5141-5146`).
  - `cancel` sets `Cancelling` before calling it (`:5432`).
  - A transition can enter `Cancelling` and terminate in the same compare-and-set.
  - **A wind-down can leave `Cancelling` and come back.** `settle_run_disposition` checks for an `Indeterminate` effect before the wind-down branch (`:5128-5131`). So a `Cancelling` run whose in-flight effect goes `Indeterminate` rests `WaitingForReconciliation`, then returns to `Cancelling` once the decision is applied (scenario 57, `tests/effect_dispatch.rs:1451`).
  - `terminal_reason` is set by `cancel` and by every failure that winds a run down, and nothing clears it. The "winding down" predicate, `terminal_reason.is_some() || status == Cancelling`, is restated at `:1771`, `:1989`, `:4465`, `:5134`, `:6058` and `:6368`.
- `AgentRunStatus::WaitingForTimer` is declared (`:312`) and nothing sets it. `AgentOperationalSnapshot::next_wake` (`query.rs:757-761`) still says timer occurrences "join here when the wake controller lands (Phase 3)".
- `AgentCheckpointBoundEffectView` (`query.rs:281-291`) carries `effect_id`, `generation`, `target` and `safety_class`.

**Where the code differs from the brief** (for the report)

1. `handoff` is not a declared kind.
2. A submitted checkpoint decision has six outcomes, not three: also Compensate, Abandon, and a person's Escalate.
3. Two more paths close a checkpoint: the `ResolveIndeterminateEffect` command, and a run cancel, which closes approval and authorization checkpoints as `Cancelled`.
4. Ask 2 is not fully answered by ask 1: on an ended run, a promotion or claim append parks with no checkpoint. By the owner's ruling, that case stays off the stream until #120 makes it answerable.
5. The recording switch reaches none of the sites the brief names.
6. The brief's line citations otherwise hold; `terminate` is at `:1292`, not `:1291`.
7. Beyond the brief: the existing `goal-evaluation` slot can leave a hole; a full outbox ring can approach the run record's byte bound; and an ended run's ambiguous promotion or claim append can't be resolved, despite its comment (#120).

## 3. Goals, non-goals, anti-goals

**Goals**

- Each transition in section 5.1 records exactly one event in the run's sequence, after it persists, through the existing sink.
- An event carries the bounded references a reader needs to act on it: the checkpoint id and kind, its due and expiry times, the bound effect, the outcome, the terminal status, and a stable code.
- A host on the new binary with the switch at its default writes nothing a `bc0c224` replica can't decode. With the switch on, it writes the new kinds.
- The report the brief asks for, with every public name and serde label.

**Non-goals**

- Changing any transition's semantics, or anything a dispatch decision reads.
- Compiled workflows. Trace segments: no segment is added or changed; the existing decision span event follows each new event wherever a segment already reports its transition's decisions.
- A new query, or any change to the snapshot.
- Resume events.
- The coordination kinds the spec lists (`delegate`, `team-operation`, `moderated-turn`).

**Anti-goals** (failures even if the code works)

- A hole in a run's sequence that a replay reader is refused at, caused by re-recording an event the sink already holds.
- A transition that fails, or behaves differently, because of an event: events are never a correctness input (spec 17.13).
- An unbounded label in an event: no summary, roles, resolver identity, arguments or results.
- A run record that grows when no sink is wired.

## 4. Constraints

- Events are recorded inside the transition's compare-and-set and flushed after it persists (spec 17.13). The existing `record_decision` gives both.
- One logical event per transition under re-drive (spec 17.13), and the stricter rule from section 2: a committed transition never records an identity an earlier committed transition already recorded.
- Bounded labels only (spec 17.7, 17.14).
- Workspace lints: every new public item is documented, and validation runs clippy with `-D warnings`.
- Error and record codes are a compatibility surface. A new kind label, once shipped, is stable.
- **No new durable field may carry an event's identity.** During step 1 of the rollout, a `bc0c224` writer that re-persists a run record drops any field it doesn't know. A counter or ordinal kept in a new field would reset, and the next identity would repeat. Every slot is built from durable state `bc0c224` already keeps.
- **The owed outbox must never make a run record refuse a transition.** Its bytes are bounded so that the maximal working set and a full outbox fit `AGENT_RUN_MATERIALIZED_MAX_BYTES` together (section 5.7).

## 5. Design

### 5.1 The vocabulary

| Transition | Kind | New | Source | Carries |
|---|---|---|---|---|
| An approval checkpoint opens | `request-approval` | no | deterministic-policy | checkpoint, effect, safety class |
| A security-authorization checkpoint opens | `request-authorization` | no | deterministic-policy | checkpoint, effect, safety class |
| A live run's generation parks `Indeterminate`, and its reconciliation checkpoint opens in the same transition | `request-reconciliation` | yes | deterministic-policy | checkpoint, effect, safety class, `reason_code` = the effect's recorded error code (e.g. `mcp-outcome-unknown`) |
| An approval or authorization checkpoint is granted or denied | `resolve-checkpoint` | yes | human | checkpoint, effect, safety class, `outcome` = `granted` or `denied`, `reason_code` = the same label |
| An approval or authorization checkpoint expires | `resolve-checkpoint` | yes | deterministic-policy | as above, `outcome` and `reason_code` = `expired` |
| An approval or authorization checkpoint is closed by the run's wind-down | `resolve-checkpoint` | yes | deterministic-policy | as above, `outcome` and `reason_code` = `cancelled` |
| A checkpoint escalates: the SLA timer, or a person's Escalate | `escalate-checkpoint` | yes | deterministic-policy (timer) or human | checkpoint, effect, safety class, `reason_code` = `sla-due` (timer) or `escalated` (a person) |
| A reconciliation decision is applied, through a checkpoint decision or `ResolveIndeterminateEffect` | `reconcile` | no | human | checkpoint (when one was bound), effect, safety class, `outcome` = `confirmed-executed`, `confirmed-not-executed`, `compensate` or `abandon`, `reason_code` = the same label |
| The run ends `Completed` / `Failed` | `complete` / `fail` | no | deterministic-policy | `run_status`, `reason_code` = the terminal reason's code |
| The run ends `Cancelled` / `HandedOff` / `Superseded` | `cancel` / `handoff` / `supersede` | yes | deterministic-policy | `run_status`, `reason_code` = the terminal reason's code |
| The run enters a wait and stays in it | `wait` | no | deterministic-policy | `run_status`, `reason_code` = `budget-top-up`, `awaiting-children`, `awaiting-handoff` or `winding-down` |

Notes:

- **`reconcile` means a reconciliation decision was applied,** as it is declared today. Opening a reconciliation checkpoint is `request-reconciliation`. A person's Escalate on a reconciliation checkpoint is `escalate-checkpoint`, not `reconcile`, because it settles nothing.
- **`handoff` is the run's end by handoff.** The model's choice to hand off is already recorded as `call-tools`. The kind's doc comment says this.
- **Escalate and wait record entry only.**
  - `escalate-checkpoint` is recorded when the checkpoint moves from `Open` to `Escalated`. A second Escalate on an already-escalated checkpoint changes nothing visible and records nothing.
  - `wait` is recorded only by the transition that leaves the run in that wait having started outside it.
  - `winding-down` is recorded once per run, by the transition in which the run begins winding down and doesn't end (section 5.5, ask 4). A transition that begins the wind-down and terminates in the same compare-and-set records the end only.
- **Ask 2.** `request-reconciliation` carries the effect's error code, so it answers ask 2 for live runs, as the brief invites. An ended run's park records nothing (owner ruling, 2026-11-03): nothing can settle it until #120 is fixed, and that fix records the event.
- **`reason_code` on every new event.** For the outcome kinds it repeats the outcome label. A reader of the coordination replay, which keeps only the kind and `reason_code` (section 2), can then still tell the outcomes apart.

### 5.2 The event's new fields

`AgentDecisionEvent` gains four public optional fields. Each is `#[serde(default, skip_serializing_if = "Option::is_none")]`, so an event without them serializes exactly as today:

| Field (serde name) | Type | Set on |
|---|---|---|
| `checkpoint` | `Option<AgentDecisionCheckpoint>` | checkpoint kinds (`request-reconciliation` included), and `reconcile` when a checkpoint was bound |
| `effect` | `Option<AgentCheckpointBoundEffectView>` | checkpoint kinds, `request-reconciliation`, `reconcile` |
| `outcome` | `Option<AgentDecisionOutcome>` | `resolve-checkpoint`, `reconcile` |
| `run_status` | `Option<AgentRunStatus>` | the five end kinds and `wait` |

New public types, in `observability.rs`, re-exported beside `AgentDecisionKind`:

- `AgentDecisionCheckpoint` has four fields: `checkpoint_id: HumanCheckpointId`, `kind: AgentCheckpointKind` (labels `approval`, `security-authorization`, `indeterminate-effect-reconciliation`), and `due_at` and `expires_at`, both `Option<AgentTimestampMillis>`. It is bounded: no summary, roles, resolver or escalation target.
- `AgentDecisionOutcome` is `#[non_exhaustive]` and kebab-case: `granted`, `denied`, `expired`, `cancelled`, `confirmed-executed`, `confirmed-not-executed`, `compensate`, `abandon`.
- `AgentDecisionEventVocabulary` is described in section 5.3.

`effect` reuses `AgentCheckpointBoundEffectView` (`query.rs:281-291`): `effect_id`, `generation`, `target` (`"{target_type}:{name}"`), `safety_class`. The top-level `safety_class` is still set, as today. `AgentDecisionDraft` gains builder methods for the four fields (names left to the builder).

### 5.3 The switch and the schema version

```rust
/// Which decision kinds a run entity writes.
#[non_exhaustive]
pub enum AgentDecisionEventVocabulary {
    /// The ten kinds a `bc0c224` reader decodes; every event is schema version 1. The default.
    Version1,
    /// Adds the six lifecycle kinds; every event is schema version 2.
    Version2,
}
```

- **Builders.** `with_decision_event_vocabulary(AgentDecisionEventVocabulary)` sits beside each `with_decision_events`: on `AgentRunEntityStore` (`run.rs:6887`), `AgentRunEntity` (`:9154`) and `AgentRunEntityShardingSettings` (`:9419`). With no sink wired, nothing is recorded, as today, whatever the vocabulary.
- **Under `Version1`**, a draft of a new kind is discarded before it is given a sequence. That leaves no hole and no drop count.
- **Versioning.**
  - `CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION` moves to 2.
  - The event is stamped with the version of the vocabulary that wrote it: `Version1` stamps 1, `Version2` stamps 2. The four optional fields don't raise it, because a version-1 reader ignores them.
  - Rakka's own readers of stored events accept versions 1 through 2 under the schema policy's N/N+1 window, and refuse a newer one with the existing typed schema error.
  - `tests/operational_query.rs:906-916` asserts that a stored event carries the current version. It changes to assert the vocabulary's version.
  - The compatibility doc records the rule for the next bump: the window must reach back to the oldest vocabulary a deployment can still select. So a bump to 3 either retires `Version1` or widens the window.
- **No catch-all kind.** `AgentDecisionKind` gains no `#[serde(other)]` variant. A run record re-persists the events it owes, and a catch-all would silently rewrite a future kind as "unknown". The staged switch is how every future kind ships.

### 5.4 How a transition records

**Drafts.** A site that records a decision pushes it onto a draft list on `AgentLoopState`.
- **What a pushed draft holds.** It captures everything positional at push time, as `record_decision` does today: turn, phase, task and goal, definition and settings revisions, context snapshot, telemetry, and time. The identity is derived from that turn.
- **What is assigned at `seal`.** Only the sequence.
- **Why the capture matters.** `iterate` keeps the turn it was decided in, though `begin_turn` advances the turn later in the same transition. A gated call's `request-approval` carries the phase `call-tools` carries.

The list:

- is never serialized (`#[serde(skip)]`);
- never counts in equality, so `unchanged_by` (`run.rs`) and every state comparison behave as today;
- is empty on every loaded record.

A site never needs to know whether events are on.

**The seal hook.** `AgentExchangeParticipant` gains one provided method that does nothing by default:

```rust
fn seal(&self, before: &Self::State, after: &mut Self::State, now: AgentTimestampMillis) {}
```

`AgentExchangeHost` calls it on all three write paths, after the transition and before the state is persisted:

- `initiate`: after the closure and `record_owed`;
- `accept`: after `apply`, before the result is journaled;
- the initiator's settle: after `settle`.

`before` is the record state the host cloned. A rejected transition persists nothing, and nothing is sealed. The other four participants keep the default.

**The run participant.** `AgentRunParticipant` becomes a struct holding `Option<AgentDecisionEventVocabulary>`: `None` when no sink is wired. The run entity's builders update the participant the host holds (the accessor is the builder's choice). Its `seal`:

1. clears the drafts and returns, when there is no sink;
2. derives the wait drafts by comparing `before` with `after` (section 5.5, ask 4), taking their positional fields from `after`;
3. discards drafts whose kind the vocabulary doesn't write;
4. gives each survivor the next sequence and stamps the vocabulary's version, then appends it to the ring (section 5.7): site drafts in the order they were pushed, then the derived waits;
5. leaves the list empty.

**The existing sites move too.**
- `start`, `tools`, `iterate`, `proposal` and `goal-evaluation` push drafts like every other site.
- The `decision_events: bool` parameter leaves the five functions in section 2.
- Their events, turns, phases and order are unchanged, and so are their slots except `goal-evaluation`'s (section 5.6). The existing `tests/decision_events.rs` holds that.

### 5.5 The sites, by ask

All in `run.rs` unless stated.

**Ask 1: a checkpoint's transitions**

- **Open:** `open_effect_checkpoint` (`:5449`), after `record_checkpoint`. The kind follows the checkpoint's kind (`request-approval`, `request-authorization` or `request-reconciliation`). One site covers all five callers.
- **Resolve:** `resolve_checkpoint` (`:5542`). There are no drafts on the deduplicated branch (`:5571`).
  - `Granted` and `Denied` record `resolve-checkpoint`.
  - `EffectResolution`, `Compensate` and `Abandoned` record `reconcile` with `confirmed-*`, `compensate` or `abandon`.
  - `Escalated` records `escalate-checkpoint` (human) only if the checkpoint moved from `Open`.
- **The effect-layer command:** `resolve_indeterminate_effect` (`:5169`) records `reconcile`. It references the checkpoint `drop_reconciliation_checkpoint` retires, when there was one.
  - The checkpoint path also reaches the shared core, `apply_indeterminate_resolution`. Each applied resolution records exactly one `reconcile`; whether that happens in the core or at its two callers is the builder's call.
  - A resolution the core refuses (`RedispatchUnaffordable`) rejects the transition, so nothing is recorded.
- **Timers:** `fire_checkpoint_timers` (`:6145`). `Expired` records `resolve-checkpoint` with `expired`; `Escalated` records `escalate-checkpoint` (deterministic-policy).
- **Run cancel:** `cancel` (`:5416`) records `resolve-checkpoint` with `cancelled` for each checkpoint `cancel_open_checkpoints` moved from waiting to `Cancelled`. The loop state method reports which ones.

**Ask 2: an unknown outcome**

- A live run is covered by `request-reconciliation` from the open site above. No other site is added.
- An ended run's park (`record_effect_result`, `:4384-4404`) records nothing, by the owner's ruling. The comment at `:4384-4390` stays as it is; correcting it is part of #120.

**Ask 3: the run's end**

- `terminate` (`:1292`), after its terminal guard. The kind follows `reason.status()`, `run_status` is that status, and `reason_code` is `reason.code()`.
- The guard makes it once per run, however the wind-down is re-driven.

**Ask 4: waits, derived in `seal`**

Each wait is recorded when the `after` state shows it and the `before` state doesn't. None is recorded when `after` is terminal.

| Wait | `after` shows it when | Slot (section 5.6) |
|---|---|---|
| `budget-top-up` | a pending top-up whose request sequence differs from `before`'s | `wait-top-up-{sequence}` |
| `awaiting-children` | phase `AwaitingChildren` with no handoff awaiting its target, and `before` was not `AwaitingChildren` in the same turn | `wait-children` |
| `awaiting-handoff` | phase `AwaitingChildren` with the handoff cell awaiting its target, and `before` was not `AwaitingChildren` in the same turn | `wait-handoff` |
| `winding-down` | the run is winding down and `before` was not. "Winding down" is `settle_run_disposition`'s predicate, `terminal_reason.is_some() \|\| status == Cancelling`, read through one shared helper rather than restated. Because `terminal_reason` is never cleared, this fires once per run, even when the wind-down rests `WaitingForReconciliation` in between. | `wait-winding-down` |

`run_status` is `after`'s status. So a wind-down that began while an effect's outcome was unknown reads `waiting-for-reconciliation` here.

The builder must show that a turn can't enter `AwaitingChildren` twice. If it can, the slot gains an ordinal taken from durable state `bc0c224` already keeps, never from a new field (section 4).

**Ask 5: the stale doc comment**

`query.rs:757-761`: `next_wake` is the earliest durable checkpoint deadline. No agent run enters `WaitingForTimer`; timer wakes belong to the wake controller's epochs, not to the run. A doc change only. `AgentDecisionKind`'s own doc comment (`observability.rs:224-228`) is corrected in the same commit as the new kinds.

### 5.6 Identity: slots and the no-hole rule

An event's identity is `(tenant, agent, run, "decision", turn, slot)`, with `turn` the loop's current turn. Every new slot is built from values that name its transition uniquely within the run:

- **checkpoint and effect events:** the effect's own `turn`, `slot` and `generation`, plus the event's role. For example `approval-request-t{t}-s{s}-g{g}`, `checkpoint-resolve-…`, `checkpoint-escalate-…`, `reconcile-request-…`, `reconcile-…`. The builder confirms that an effect's `(turn, slot)` is unique within a run, post-turn effects included; if it isn't, the builder uses the effect id's own discriminator. The exact strings are the builder's.
- **the end:** `end`. The terminal guard makes it once per run.
- **waits:** as in the table in section 5.5.
- **`goal-evaluation`, fixed.**
  - **The fix.** The slot becomes `goal-evaluation-s{slot}`, from the evaluation effect's slot. It is the one existing slot this change alters.
  - **Why.** The old fixed slot repeats when a turn commits a second evaluation after the first reported (section 2).
  - **Mixed fleet.** The two slot forms never collide: an evaluation is recorded once, by the binary that commits it.

**The rule:** a committed transition never records an identity an earlier committed transition recorded.
- Section 5.1's entry-only rules exist to satisfy it.
- The proof in section 7 drives every new transition, replays and re-drives it, and asserts two things: the sink's sequences are contiguous, and no append answered `Duplicate`.

### 5.7 The ring

The ring keeps its count bound of 32 and gains a byte bound.

- **Worst case per transition:** the gated fan-out records one `call-tools` and one open per gated call, at most `AGENT_MODEL_MAX_TOOL_CALLS` = 8, so 9 events.
- **When it fills:** the outbox is flushed after every pass, so it fills only while the sink is unavailable.
- **The byte bound.**
  - **The rule.** A new constant, `AGENT_RUN_DECISION_OUTBOX_MAX_BYTES`, bounds the serialized size of the owed events. An event that would cross it, or the count, drops the oldest owed events until it fits, and each drop is counted.
  - **Sizing.** The value comes from measurement, not estimate: the largest event under maximal identifiers, and the slack between the measured maximal working set and `AGENT_RUN_STATE_GROWTH_RESERVE_BYTES`. So the admission budget doesn't change.
  - **If it can't fit.** If the slack can't hold one maximal event, the builder stops and brings it to the owner rather than raising the reserve.
- **When it drops:** a drop is counted (`decision_drops`, metric `rakka.agent.decision.drops`) and leaves a hole a replay reader sees, as today.
- **Why not raise the count:** a larger ring would grow the record of every run whose sink lags.

## 6. Rollout, and what a `bc0c224` binary can read

| Change | Wire effect | Can a `bc0c224` binary read it? |
|---|---|---|
| `checkpoint`, `effect`, `outcome`, `run_status` on `AgentDecisionEvent` | additive, optional | Yes. Unknown fields are ignored. An old replica that decodes and re-serializes an event (the session view) shows it without them. |
| `request-approval`, `request-authorization`, `reconcile`, `complete`, `fail`, `wait` events | existing kinds; version 1 | Yes. |
| `request-reconciliation`, `resolve-checkpoint`, `escalate-checkpoint`, `cancel`, `handoff`, `supersede` | new kinds; version 2 | No. The decode fails, and the host fails that page with `decision-schema`. They are written only under `Version2`. |
| `CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION` = 2, stamped by vocabulary | reader window 1..=2 | Not applicable; an old binary has no gate for it. The version is what the next bump relies on. |
| The six new kinds on the coordination replay (`AgentCoordinationEventKind::Run`) | new values on the A2A replay wire | No, for a client that decodes the kind strictly. Under `Version2` only. |
| A run record whose outbox owes an event of a new kind | the record itself | No. The record fails to load. Under `Version2` only. |

**Staging for the host, two pin bumps:**

1. **Move every replica to the new pin with the vocabulary at its default, `Version1`.**
   - The stream gains approval and authorization opens, applied reconciliations, `complete`, `fail` and waits. Every one is readable by a replica still on `bc0c224`.
   - The host's fleet branch for `complete` and `fail` becomes reachable.
   - Approval resolutions, escalations, expiries, cancelled checkpoints, unknown outcomes, and cancelled, handed-off or superseded ends are not yet on the stream, so Studio keeps polling for those.
   - **One loss while the fleet is mixed.** A `bc0c224` replica that takes over a run and re-persists its record drops the four new fields from the events it still owes. So an event a new replica recorded can reach the sink without its checkpoint reference. That costs observability only, and only for events owed across a handover.
2. **When every process that loads the run store or reads the sink runs the new pin, set `Version2`.** The six new kinds start. That covers replicas, and also any tool or job outside the replica set.

**Rolling back past the new pin after step 2 is outside the compatibility window.** That follows the existing rule in `docs/rakka-v1-rolling-update-upgrade.md` ("Rollback is safe only while N remains inside the advertised compatibility window"), as for the run-state terminal-stamp backfill. Two things break:
- **Run records.** A `bc0c224` binary can't load a run record that still owes a version-2 event. That means a run whose sink was behind, or a run that passivated before its last flush. No index lists such records.
- **The sink.** Version-2 events the sink already holds stay undecodable by `bc0c224` until they age out of retention. Rakka's reference sink keeps 256 per run (`AGENT_DECISION_EVENT_RETENTION`); the host's own retention applies to its sink.

Setting `Version1` again stops new version-2 events, but neither of those is undone by it.

`docs/rakka-compatibility.md` gets four changes:
- the `agent-decision-event` row at 2;
- a bullet under "what each bump requires" stating the two steps, this table and the rollback limit;
- a note that a new kind is a wire change for readers, the A2A coordination replay included;
- the next-bump rule from section 5.3.

`compatibility_currency.rs` holds the row.

**The coordination replay is otherwise out of scope.** `AgentCoordinationEvent::from_decision` keeps projecting the kind and `reason_code` only. The new events' `reason_code` (section 5.1) is what that path shows.

## 7. Proofs

Each is a deterministic test that fails at `47f31ec` and passes after, unless it is marked as holding existing behavior. Fixtures run the run entity with a sink wired and `Version2`, unless the test is about the vocabulary.

- **Ask 1**
  - An approval and an authorization checkpoint each open with the right kind, the checkpoint id and kind, `due_at` and `expires_at` under an SLA, the bound effect, and the safety class.
  - Granted and denied each record `resolve-checkpoint` with their outcome.
  - A timer expiry and an SLA escalation record theirs.
  - A person's Escalate records `escalate-checkpoint` once; a second Escalate records nothing.
  - A run cancel records `resolve-checkpoint` `cancelled` for each open approval-family checkpoint and none for a reconciliation one.
  - Each of the four reconciliation outcomes, and `ResolveIndeterminateEffect`, records one `reconcile` with its outcome.
  - A deduplicated decision records nothing.
- **Ask 2**
  - An `Indeterminate` MCP outcome on a live run records `request-reconciliation` with `reason_code` `mcp-outcome-unknown` and the checkpoint reference.
  - On an ended run, a promotion's ambiguous generation records no event (holds existing behavior; it pins the ruling, so #120's fix has a test to change).
- **Ask 3**
  - Each of the five terminal statuses records its kind, `run_status` and reason code.
  - A wind-down re-driven over several passes records one end.
  - A cancel that ends in its own compare-and-set records the end and no wait.
- **Ask 4**
  - A top-up park, a fan-in, a handoff awaiting its target, and a wind-down each record one `wait` with its reason.
  - Settle passes that re-rest the same wait record nothing.
  - A wind-down that rests `WaitingForReconciliation`, is reconciled, and returns to `Cancelling` records one `winding-down` (scenario 57's shape).
- **The vocabulary**
  - Under `Version1`, none of the six new kinds is recorded, the sequence has no gap, and every event is stamped 1.
  - Under `Version2`, they are recorded, and every event, old kinds included, is stamped 2.
  - With no sink, nothing is recorded and the outbox stays empty (holds existing behavior).
- **Existing events keep their positions.** `iterate` keeps its turn and context snapshot, and the turns stay `[1, 1, 1, 2]` (`tests/decision_events.rs:93`). A gated call's `request-approval` carries the phase of its `call-tools`.
- **`goal-evaluation`.** Two evaluations committed in one turn, with a flush between them, record two events with contiguous sequences. This fails at `47f31ec`.
- **The byte bound.**
  - The growth-reserve proof, or a sibling of it, materializes the maximal working set with a full outbox of maximal events and fits `AGENT_RUN_MATERIALIZED_MAX_BYTES`.
  - With the sink down, a run keeps taking transitions after its outbox is full, and counts its drops.
- **What `bc0c224` reads.** A test-local mirror of the `bc0c224` event, with the ten-kind enum and without the new fields, decodes every event `Version1` writes, and fails on each new kind.
- **No hole.** Every scenario above also replays its command, re-runs its settle passes, and re-delivers its exchange. It then asserts that the sink's sequences are contiguous from 1 and that no append answered `Duplicate`.
- **Every write path.** An event recorded on the `accept` path (a run-cancel exchange) and on the settle path (a refused proposal that terminates) reaches the sink.
- **Existing behavior.** `tests/decision_events.rs` passes unchanged except for the `goal-evaluation` slot, along with the span and segment tests. `tests/operational_query.rs:906-916` changes as in section 5.3. Where a segment already reports its transition's decisions, it reports the new ones; the builder notes which segments those are.

## 8. Docs and changelog

- `docs/rakka-compatibility.md` and `compatibility_currency.rs`, as in section 6.
- `docs/rakka-agents.md`, in the decision-event paragraph: the lifecycle kinds, the vocabulary switch and its default.
- `CHANGELOG.md`: the new kinds, the four fields, the switch, and the schema version.
- Doc comments: `AgentDecisionKind`, each new kind, `next_wake`.
- The observability catalogue needs no change: `rakka.agent.decisions` already labels by `decision_kind`, and the new kinds are bounded values.

## 9. Left to the builder

- The exact slot strings, within section 5.6's rule.
- The names of the draft builder methods and of the draft list.
- How the entity's builders reach the participant the host holds.
- Whether `reconcile` is recorded in `apply_indeterminate_resolution` or at its two callers.
- How the loop state reports the checkpoints a cancel closed.
- Whether a turn can enter `AwaitingChildren` twice, and an ordinal from existing durable state if it can.
- The value of `AGENT_RUN_DECISION_OUTBOX_MAX_BYTES`, from measurement, within section 5.7's rule.
- How the tests split across files (one concern per file).
- `AgentRunParticipant` becomes a struct with a field. Two tests build the unit struct and change with it: `tests/escrow_ledger.rs:663` and `tests/exchange_receiver_memoization.rs:431`. The host never constructs it.

## 10. The report to the host

Posted on #119 when the PR merges. For each ask it gives:

- whether it landed and its commit;
- the kinds and fields by their public names and serde labels, from sections 5.1-5.3;
- each new draft site, from section 5.5;
- the brief's section 3 answers: the ring stays at 32 and gains a byte bound (section 5.7), and what a `bc0c224` binary can read, with the two-step staging and the rollback limit (section 6);
- the differences in section 2, including that an ended run's unknown outcome stays off the stream until #120, so Studio still learns of it only from the snapshot.

It also names the one new knob the host turns, `with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2)`, and when to turn it.
