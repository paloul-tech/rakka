# Ended-run reconciliation: an ended run's ambiguous promotion or claim append gets a checkpoint and an answer

**Issue:** paloul-tech/rakka#120, with the host's comment of 2026-11-04 (the host wants the first option). **Status:** design, 2026-11-04. **Code read at:** `main` `0cc9955`. Paths are under `crates/rakka-agent/src/` unless stated.

## 1. Intent

A run that has ended still accepts a memory promotion or a claim append for the post-terminal window. If that effect's attempt may have run and its recovery retry is refused, the generation parks `Indeterminate` with no checkpoint. Nothing can answer it: both resolution paths refuse an ended run, and the settle pass's park repair skips ended runs. The effect stays unresolved on the record for good.

The host reaches this in normal operation. Its memory sweep promotes an ended run's memories, and it terminates agents just after their runs end, so the retry is refused with `agent-terminated`. The cost is the record:
- an outcome nobody can learn;
- a snapshot showing an effect nobody can close;
- a sweep that counts the effect as in flight for good.

This design makes the park answerable the way a live run's is, without moving the run's terminal status.

## 2. The code at `0cc9955`, verified

**How the park arises**
- `settle_undispatchable` (`dispatch.rs:3802`) parks an attempt that was possibly executed when the authority refuses its recovery retry (`park_indeterminate`). A first attempt that is refused is not possibly executed, and fails definitively.
- `record_effect_result` (`run.rs:4416`) accepts a result on an ended run only for the kinds exempt from the wind-down fence (`:4452`). After `apply_effect_outcome` it opens the reconciliation checkpoint only when the run is live (`live && parked`, `:4498-4510`).
  - The comment above it (`:4483-4497`) says the parked generation is "resolvable by `ResolveIndeterminateEffect`". It isn't.
- `record_effect_result` accepts all four fence-exempt kinds on an ended run: compensation, workflow cancel, memory promotion and claim append.
  - The two this issue is about are `AgentRunEffectKind::outside_the_turn()` (`effect.rs:1111`), exactly `MemoryPromotionCall` and `ClaimAppendCall`.
  - A compensation or workflow cancel can also still be outstanding when a run ends. The wind-down waits on it (`blocks_settlement`, `effect.rs:1217`), but some paths call `terminate` without that wait. One is `settle_proposal`'s `Accepted` arm (`:6813`, "acceptance wins the race with a wind-down").
  - So either can, rarely, park checkpoint-less on an ended run too, through the same refused-retry path. By the owner's ruling that stays as it is here, and is tracked in #123.
- **A new generation gets a new idempotency key.** `begin_next_generation` (`effect.rs:2012`) re-derives the external key as `{effect_id}#g{generation}` (`effect.rs:974`).
  - The claim-append executor derives its store operation from that key (`rakka-agent-knowledge-graph/src/append_executor.rs:76-90`). So a re-send after an append that actually landed appends a second claim.
  - A consolidation promotion's re-send is refused on a stale revision.
  - A plain promotion converges (`AlreadyExists`).
  - The live path has the same property today: `ConfirmedNotExecuted` asserts the attempt never ran, and the new generation is a fresh attempt, not a probe.

**Why nothing can answer it**
- `apply_indeterminate_resolution` (`:5399`) refuses an ended run with `Terminal` (`:5411-5413`).
  - It is the shared core of `ResolveIndeterminateEffect` (`resolve_indeterminate_effect`, `:5279`) and of a checkpoint's `EffectResolution`.
- `resolve_checkpoint` (`:5921`) refuses an ended run with `Terminal` before it looks at the checkpoint (`:5931-5936`).
- `needs_park_repair` (`:800`) returns `false` for an ended run, by design (doc comment `:795-799`).
  - So `advance_once`'s repair (`:2757`) never runs for it.
  - Nor does the settle loop's step (`:8515`).
- `fire_checkpoint_timers` (`:6535`) returns early on an ended run (`:6539-6544`).

**What lifting the guard alone would do**
- `terminate` (`:1377`) always sets `terminal_reason`. So every ended run reads as winding down (`winding_down`, `:5414`).
  - `ConfirmedNotExecuted` would take the winding-down arm: settle the generation `Cancelled`, send nothing more.
  - The host asked for a re-send instead (section 5.2).
- The live `ConfirmedNotExecuted` arm and `Granted` both set `run.status = checkpoint_wait_status(...)`. On an ended run that would move the terminal status.
- `settle_run_disposition` (`:5224`) returns early on a terminal run (`:5229-5231`), so it never moves one.
- `open_effect_checkpoint` (`:5637`) doesn't touch the status.

**What already works on an ended run**
- `dispatch_effects` (`run.rs:9072`) flushes a pending exempt-kind effect on an ended run. A promotion accepted in the window rides that path, and so would a re-sent generation.
- `promote_memory` charges its attempts to the run's own budget (`:6315-6321`). Escrow, folded at the terminal transition, is not re-taken.
- `check_post_terminal_memory_window` (`:6209`) gates only the promotion and claim-append commands. Nothing on the dispatch or result path reads the window.
- `SessionMemoryPromotionExecutor` (`dispatch.rs:1265`) refuses a window that selects nothing, definitively (`memory-promotion-selection-empty`). That is what a re-send finds after memory retention has discharged the run's session rows.

**Checkpoints**
- A reconciliation checkpoint's default decisions (`default_decisions`, `checkpoints.rs:1332`) are `confirmed-completed`, `confirmed-not-executed`, `compensate`, `escalate` and `abandon-and-fail`.
- It never gets an `expires_at`, so a timer only escalates it.
- `next_wake` (`query.rs:872`) is the earliest `due_at` or `expires_at` of the run's waiting checkpoints, whatever the run's status.

**Tests**
- Two tests pin today's behaviour, and both are replaced:
  - `an_indeterminate_post_terminal_outcome_opens_no_checkpoint` (`tests/private_memory_promotion.rs:1583`) builds the park and asserts that no checkpoint opens;
  - `an_indeterminate_post_terminal_outcome_records_no_decision_event` (`:1623`) asserts that the park records no event.

## 3. Goals, non-goals, anti-goals

**Goals**
- An ended run's ambiguous promotion or claim append parks behind a reconciliation checkpoint, as a live run's does. This is the host's rule from #106: no park without a checkpoint.
- A person can answer it with either `ResolveCheckpoint` or `ResolveIndeterminateEffect`, at any time after the run ended. The post-terminal window doesn't bound the answer.
- Records already stuck, and records a `bc0c224` replica parks during the roll, become answerable too.
- The decision stream records the open (`request-reconciliation`) and the answer (`reconcile`), which #119 left out for this case on purpose.

**Non-goals**
- Any change to a live run's reconciliation.
- The approval or authorization checkpoints a failed run leaves open on other effects. They stay unanswerable, and the run's end closes them for readers, as #119 documents.
- A compensation or workflow cancel that parks on an ended run (owner ruling, 2026-11-04). It stays as today, with no checkpoint, and is tracked in #123.
- Changing the post-terminal window, or what it admits.
- Host changes. The host's run port refuses a resolve on an ended run before Rakka sees it, and the host changes that at its pin bump.

**Anti-goals** (failures even if the code works)
- An ended run's terminal status, `terminal_reason` or `terminal_at` moving.
- An ended run committing new work other than a re-sent generation of the parked effect.
- A decision event identity re-recorded after the sink holds it (the #119 hole hazard).
- A checkpoint that advertises a decision the run will refuse.

## 4. Constraints

- **The terminal record doesn't move** (specification 13.3: the outcome lands on the terminal record without moving it).
  - Every arm this design opens to an ended run leaves `status`, `terminal_reason` and `terminal_at` as they were.
  - `settle_run_disposition` and `terminate` already hold that, and the two status writes in section 2 must be skipped on an ended run.
- **The scope is exactly the two kinds outside the turn.**
  - On an ended run, only a checkpoint or an `Indeterminate` generation whose effect is `outside_the_turn()` is answerable.
  - Every other refusal stays `Terminal` (`run-terminal`), byte for byte as today.
- **No new error code and no new durable field.** A `bc0c224` replica loads every record this change writes (section 6).
- **No new decision kind.**
  - The events are #119's: `request-reconciliation`, `escalate-checkpoint` and `reconcile`.
  - They are written under the same vocabulary rules, and under `Version1` the stamp clears the four fields.
- Workspace lints: every changed public item stays documented, and validation runs clippy with `-D warnings`.

## 5. Design

### 5.1 The park opens a checkpoint on an ended run

- `record_effect_result` opens the reconciliation checkpoint for a parked generation on an ended run when the effect is `outside_the_turn()`. The `live && parked` condition becomes `parked && (live || outside_the_turn)`.
  - A compensation or workflow cancel that parks on an ended run opens nothing, as today (#123). Opening a checkpoint for it would advertise decisions the run refuses.
- The comment at `:4483-4497` is rewritten to say what is now true: the park opens its checkpoint, it is answerable by either resolution path, and the terminal status doesn't move.
- **No `compensate` on an ended run's checkpoint.**
  - A reconciliation checkpoint opened on an ended run lists the default decisions without `compensate`. That is `confirmed-completed`, `confirmed-not-executed`, `escalate` and `abandon-and-fail`.
  - A `Compensate` decision against it is refused `run-terminal` (section 5.2).
  - So the snapshot never offers a decision the run refuses.
  - A checkpoint opened while the run was live keeps its list. On an ended run it still refuses `Compensate`.
- The checkpoint's SLA `due_at` and escalation target come from the policy, as on a live run.

### 5.2 Both resolution paths answer an ended run

**The guard.** On an ended run:
- **Which refusal wins** (my call, 2026-11-04). The ended-run check runs first and asks one question: does the command name an outside-the-turn effect the run holds, or a checkpoint bound to one?
  - If not, the answer is `Terminal`, as today. That covers an unknown effect, an unknown or dropped checkpoint id, and every other kind.
  - If so, the command gets the same refusals a live run gives that target: `StaleEffectGeneration`, `StaleEffectResult`, and a checkpoint's own decision refusals.
  - So `run-terminal` keeps meaning "this run has ended and the target isn't answerable".
- `apply_indeterminate_resolution` admits a generation whose effect is `outside_the_turn()`, and refuses every other with `Terminal` as today.
- `resolve_checkpoint` admits a checkpoint whose bound effect is `outside_the_turn()`, whatever the checkpoint's kind, and refuses every other with `Terminal` as today.
  - The bound effect is read before the checkpoint is resolved, so a refused decision changes nothing.
  - A gated (approval or authorization) promotion or claim append on an ended run is answerable through the same rule. The builder confirms whether the window's commands can produce one (section 9).

**The decisions, on an ended run** (owner ruling 2026-11-04 for `ConfirmedNotExecuted`; the rest follow the host's comment):

| Decision | What happens | What doesn't move |
|---|---|---|
| `ConfirmedCompleted` (`EffectResolution(ConfirmedExecuted)`) | The outcome is recorded on the generation, through `apply_effect_outcome`, as the window's outcomes already are. | the status |
| `ConfirmedNotExecuted` | **The generation is re-sent.** The live arm runs, not the winding-down one: the effect's attempts are reserved from the run's budget, and `begin_next_generation` mints a new generation that `dispatch_effects` flushes. An unaffordable reservation refuses the decision with `RedispatchUnaffordable`, and the checkpoint stays open. A gated effect opens its gate's checkpoint for the new generation, as on a live run. | The status: the `checkpoint_wait_status` write is skipped. |
| `AbandonAndFail` | The generation fails with `reconciliation-abandoned`, through `apply_effect_outcome`. | The status: `terminate`'s guard already makes the run's failure a no-op. |
| `Escalate` (a person) | The checkpoint escalates and stays open, as on a live run. | everything |
| `Compensate` | Refused, `Terminal` (`run-terminal`). A compensation is new work, and the host didn't ask for it. | everything |
| `Granted` / `Denied` on a gated outside-the-turn effect's approval or authorization checkpoint | As on a live run: the grant is recorded and the effect dispatches, or the denial fails the generation. | The status: `Granted`'s `checkpoint_wait_status` write is skipped. |

**What the re-send means** (owner ruling, 2026-11-04). `ConfirmedNotExecuted` means what it means on a live run: a person asserts the attempt never ran, and the new generation is a fresh attempt under a new idempotency key (section 2). It is not a probe that finds out what happened.
- If the first attempt did land:
  - a re-sent claim append appends a second claim;
  - a re-sent consolidation is refused on a stale revision;
  - a re-sent plain promotion converges.
- A person who doesn't know should choose `ConfirmedCompleted` or `AbandonAndFail`. The docs say so, and so does the report to the host, whose comment on #120 reasoned that the re-send finds out.
- After a true `ConfirmedNotExecuted`, the re-send settles the effect:
  - if the authority still refuses it (the agent is terminated), the new generation's first attempt is refused before it runs, which is not possibly executed, so it fails definitively;
  - if memory retention has discharged the run's session rows, the promotion's re-send is refused `memory-promotion-selection-empty`, also definitively.

**The window.** None of this reads `check_post_terminal_memory_window`.
- The window bounds new memory commands.
- A park found after it closes still needs an answer, and a re-sent generation is not a new command.

**After the answer.**
- `resolve_indeterminate_effect` drops the checkpoint with `drop_reconciliation_checkpoint`, as today.
- A checkpoint decision drops it in its arm, as today.
- `settle_run_disposition` is a no-op on an ended run.

### 5.3 Timers escalate an ended run's checkpoint

`fire_checkpoint_timers` runs on an ended run for checkpoints whose bound effect is `outside_the_turn()`, and leaves every other checkpoint alone.
- An approval-family checkpoint a failed run left open is never expired on an ended run. Expiring one would apply a failure to an effect of the ended run.
- A reconciliation checkpoint never expires, so on an ended run a timer only escalates it.
- A gated outside-the-turn checkpoint expires as on a live run. Its expiry fails that generation only.
- The command's `unchanged_by` precheck needs no change. It compares states, and an ended run with nothing due is unchanged.
- `next_wake` already shows the checkpoint's `due_at`. A timer driver that skips ended runs is the host's choice. Rakka answers the command either way.

### 5.4 The repair covers ended runs

- `needs_park_repair` returns `true` for an ended run that holds an `Indeterminate` outside-the-turn generation with no reconciliation checkpoint. On an ended run it reads nothing else.
  - An ended run's handoff cell is settled by construction, so the repair's handoff half never applies to it.
- `repair_park` opens the checkpoint, as it does for a live run's park.
- The settle loop's `can_advance` check (`:8515`) and `advance_once` (`:2757`) already route a repair-needing run to `repair_park`. The builder confirms that the settle pass reaches an ended run, as `dispatch_effects` does for the window's promotions. If it doesn't, the repair runs on activation, which the builder shows with a test.
- **Two kinds of record get repaired:**
  - records already stuck in a deployment's store;
  - records a `bc0c224` replica parks during the roll, because that replica still opens no checkpoint (section 6).
- The `needs_park_repair` doc comment drops "a terminal run is excluded" and says what it now covers.

### 5.5 Decision events

- The checkpoint's open records `request-reconciliation`, through `open_effect_checkpoint`. #119 put the site there and written only under `Version2`.
  - The repair's open records it the same way.
  - Its slot is `ck-open-reconcile-t{turn}-s{slot}-g{generation}`, from the effect's own turn, slot and generation, which is unique per generation.
- An escalation records `escalate-checkpoint` (`sla-due` from a timer, `escalated` from a person). A decision records `reconcile` with its outcome, or `resolve-checkpoint` for a gated checkpoint's grant or denial.
  - `reconcile` is a `Version1` kind, so a host at its default writes it.
- An event on an ended run carries the run's final turn, like the run's `end`. Every slot comes from the effect's identity, so none collides with `end` or with another generation's.
- **No waits.** The seal records none when the state after the transition is terminal, and that rule stands.
- **No hole.** Each event is recorded by the one transition that makes its change.
  - A replayed decision is answered from the checkpoint's decision ledger, or refused `StaleEffectResult`, and records nothing.
  - A repair runs once, because the checkpoint it opens ends `needs_park_repair`.

### 5.6 What doesn't change

- **A live run:** every path above behaves as today.
- **The snapshot shape:** an ended run's view lists the checkpoint under `open_checkpoints` and, through it, `next_wake`. That is the existing shape.
- **Every other refusal on an ended run:** commands, other checkpoints and other effect kinds still get `Terminal`.

## 6. Rollout, and what a `bc0c224` binary does

The host takes this and #121 in one pin bump, rolling replicas one at a time. No record shape changes: the checkpoint, the generation and the decision ledger are all types `bc0c224` already has.

| During the roll | What happens |
|---|---|
| A `bc0c224` replica records the park | No checkpoint, as today. A new replica's settle pass repairs it (section 5.4). |
| A `bc0c224` replica loads an ended run with this checkpoint open | It loads. Its snapshot shows the checkpoint. Its settle pass leaves the run alone (`needs_park_repair` is `false` there for an ended run). |
| A resolve reaches a `bc0c224` replica | It is refused `run-terminal`, as today, and the decision isn't applied. The host's facade treats `run-terminal` as it does today, and the host's own run port refuses these resolves until its bump. |
| A `bc0c224` replica loads a re-sent generation | It dispatches it. `dispatch_effects` at `bc0c224` already flushes an ended run's exempt kinds, and its `record_effect_result` accepts the outcome. A second park on it opens no checkpoint there, and a new replica repairs it. |

**Rollback to `bc0c224`** is inside the window. What this change wrote stays loadable, and the only loss is that a resolve on an ended run is refused again.

## 7. Proofs

New tests, in `tests/private_memory_promotion.rs` beside the park's builder, or a new `tests/ended_run_reconciliation.rs` (the builder's call). For a promotion and for a claim append:

1. **The park opens its checkpoint.** `an_indeterminate_post_terminal_outcome_opens_no_checkpoint` and `an_indeterminate_post_terminal_outcome_records_no_decision_event` are replaced. The ended run's park now holds one waiting reconciliation checkpoint bound to the generation, without `compensate` in its decisions. The run's status, `terminal_reason` and `terminal_at` are unchanged, and the snapshot lists the checkpoint.
2. **`ConfirmedNotExecuted` re-sends**, through both `ResolveCheckpoint` and `ResolveIndeterminateEffect`.
   - The new generation reaches the dispatcher, and its outcome lands on the ended run.
   - With the agent still terminated, the re-send fails definitively, and the effect is no longer `Indeterminate`.
   - Asserted after the window has closed (the window is shorter than the delay).
3. **`ConfirmedCompleted` records, and `AbandonAndFail` fails the generation only.** `ConfirmedCompleted` goes through both paths. `AbandonAndFail` goes through `ResolveCheckpoint` only, since `AgentEffectResolution` has no abandon. The terminal fields stay unchanged.
4. **`Compensate` is refused** `run-terminal`, and the checkpoint stays open.
5. **Unaffordable re-send.** With the run's budget unable to reserve the attempts, `ConfirmedNotExecuted` is refused `RedispatchUnaffordable` and the checkpoint stays open.
6. **The repair.** A record written checkpoint-less (rewritten through the existing test-kit helpers, as the #106 repair tests do) is repaired by the next settle pass. The checkpoint opens and the park is then answerable.
7. **A timer escalates it.** A reconciliation checkpoint with an SLA and an escalation target escalates on an ended run past `due_at`. An approval left open by a failed run on another effect is not expired.
8. **The guard still refuses everything else.** On an ended run:
   - a resolve against an approval checkpoint bound to a non-memory effect is refused `run-terminal`;
   - so is a `ResolveIndeterminateEffect` naming a non-memory effect;
   - the refusal bytes match today's.
9. **Decision events, no hole.**
   - With `CountingDecisionSink` under `Version2`, the ended run's stream gains `request-reconciliation`, then `reconcile` (and `escalate-checkpoint` in test 7), after its `end`.
   - The test replays the command, re-runs settle passes and re-delivers the exchange, then calls `assert_decision_stream_whole` and asserts presence.
   - Under `Version1`, `reconcile` is written and `assert_bc0c224_round_trips` holds.
10. **Mixed fleet.** The change adds no field, variant or type to any serialized record, so a `bc0c224` replica loads what it writes (section 6). The final review checks the diff for it, since no in-process test can load a run record as `bc0c224` does.

Every new test asserts the run's terminal fields after each step. Each fix is falsified by reverting it and confirming its test fails.

## 8. Docs and changelog

- **`docs/rakka-agents.md`:** where it describes the post-terminal window and reconciliation, an ended run's ambiguous promotion or claim append parks behind a reconciliation checkpoint and is answered by either path, without `compensate`, at any time after the run ended. It also says that `ConfirmedNotExecuted` asserts the attempt never ran and re-sends under a new idempotency key, so it is not the decision to take when unsure.
- **`docs/rakka-compatibility.md`:** a bullet. It covers:
  - the behaviour change: an ended run admits these resolutions;
  - what a `bc0c224` replica does during the roll (section 6);
  - that rollback is inside the window.
- **`CHANGELOG.md`:** under Unreleased, a fix entry naming #120.
- **The #119 docs:** `docs/rakka-agents.md`'s note that "an ended run's ambiguous memory promotion or claim append records no event (issue #120)" is removed. The CHANGELOG #119 entry's matching sentence is replaced by a pointer to the #120 entry.
- **Doc comments:** `record_effect_result`'s park comment, `needs_park_repair`, `apply_indeterminate_resolution`, `resolve_checkpoint` and `fire_checkpoint_timers` say what each now does on an ended run.

## 9. Left to the builder

- Where the `outside_the_turn()` reads live: a helper on `AgentRun`, or inline at each guard.
- How the ended-run checkpoint's decision list is built: a filter on `default_decisions`, or a parameter to the checkpoint constructor.
- **Whether a window command can produce a gated promotion or claim append.** A deployment can set a spec with `with_checkpoint_required()` through `with_memory_promotion_spec` and `with_claim_append_spec`.
  - If it can't, the gated rows in section 5.2 hold by construction and need only a test that they're unreachable.
  - If it can, test 2 also covers the gated re-send.
- The file for the new tests.

## 10. The report to the host

Posted on #120 when the PR merges:
- the commit for each part (5.1-5.5);
- the decisions an ended run accepts, and the refusals that remain;
- that `ConfirmedNotExecuted` is a person's assertion, not a probe. The re-send uses a new idempotency key, so a person who doesn't know what happened chooses `ConfirmedCompleted` or `AbandonAndFail`. This corrects the reasoning in the host's comment;
- #123, the two kinds this leaves out;
- the repair, and that it covers records a `bc0c224` replica parks during the roll;
- the decision events and their slots;
- the rollout table from section 6.
