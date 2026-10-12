# A decision ledger: a resolution's key outlives the operation window

Status: draft, 2026-10-12, for the owner's review.
Brief: issue #96 (rakka.host A18), ask 2, drafted against `96a18b0`. The code was read at `06d2cc7`; `run.rs`, `task.rs`, `goal.rs` and `wake.rs` are unchanged between the two.
Owner rulings (2026-10-12):
- the ledger holds 32 entries;
- it covers every revision-checked task command, not only the four kinds the brief names.

## The gap

The run and the task each remember their last 64 operation ids (`AGENT_RUN_OPERATION_LOG_CAPACITY`, `AGENT_TASK_OPERATION_LOG_CAPACITY`). While a re-post's id is still in that window, it is answered `Duplicate` with the original outcome.

Past the window, the domain fence refuses the re-post. That is safe, because nothing applies twice. But the refusal is the one a first attempt would meet, so a client that lost the answer to a committed decision cannot tell "it applied" from "it was refused":

| Command | Refusal a replay meets past the window |
| --- | --- |
| `ResolveCheckpoint` | `run-unknown-checkpoint` (every resolving outcome drops the checkpoint), or `run-terminal` after a Deny ended the run |
| `ResolveIndeterminateEffect` | `run-terminal`, `run-stale-effect-generation`, `run-stale-effect-result` |
| `Create` | `task-already-created` |
| `Cancel` | `task-terminal` (once the cancellation finished) |
| `ActivateGoal`, `ResumeGoal` | `goal-stale-status-revision`, `goal-terminal` |
| `RecordGoalDecision` | `goal-terminal` (every goal decision is terminal) |
| `ReviseGoalCriteria` | `goal-stale-criteria-revision`, `goal-terminal` |
| `Suspend`/`Resume`/`Renew`/`RetireContinuousGoal` | `wake-stale-lifecycle-revision`, `wake-lifecycle-terminal` |
| `UpdateContinuousSchedule` | `task-schedule-not-monotonic` |

The window fills from ordinary traffic. Every effect result is one entry, so an approved run that goes on to make 64 tool calls has already evicted its approval's id. The host now marks such a refusal `replay_outcome_unknown`. The two stale-effect codes are not terminal for the host, so a re-post that meets one stays `pending` forever (the host's deferred row D2).

There are two precedents in this crate for an answer that outlives the window:
- the conversation's dense turn ledger (slice 5.3);
- the task's human-submission echo (`probe_human_submission_echo`, slice 5.4), which answers a past-window `SubmitHumanResult` from its fingerprint ring before every other guard.

## Decision 1 — a second, smaller log that only decisions write

The run state and the task state each gain an `AgentDecisionLedger`. It has the same shape as the operation log: a bounded queue of (operation id, outcome) entries, oldest first, `#[serde(transparent)]`.

```rust
/// How many decisions a run or a task remembers past its operation window.
pub const AGENT_DECISION_LEDGER_CAPACITY: usize = 32;
```

The run's ledger holds `AgentRunOutcome`, and the task's holds `AgentTaskOutcome`. Each entry stores the same outcome the operation log stores, so a past-window `Duplicate` is the reply an in-window one would have been.

Only decision commands write the ledger, and they write it in the same compare-and-set that records the id in the operation log. Effect results, settle passes, timer firings, exchanges and wake admissions never write it. So 32 later decisions on the same run or task are needed to evict an entry. A decision is human- or operator-paced, so that is far beyond any re-post delay a client has.

**The decision commands:**
- **Run:** `ResolveCheckpoint` and `ResolveIndeterminateEffect`.
- **Task:**
  - the brief's four kinds: `Create` and `Cancel`, `RecordGoalDecision`, and the wake lifecycle commands;
  - every command fenced on a revision (owner ruling): `ActivateGoal`, `ResumeGoal`, `ReviseGoalCriteria`, `SuspendContinuousGoal`, `ResumeContinuousGoal`, `RenewContinuousGoal`, `RetireContinuousGoal` and `UpdateContinuousSchedule`.

Both command enums gain a public `records_decision(&self) -> bool`. The host then derives which refusals are still replay-reachable from the code, rather than restating the list.

**Not decision commands:**
- `RecordEffectResult`: the dispatcher's derived id and the run's effect fence already make it convergent, and it is the traffic that fills the window.
- `AdmitWake` and `CompleteWakeOccurrence`: epoch-fenced and scanner-driven.
- `SubmitHumanResult`: it has its own past-window echo.
- `DeclareDependency`, `RecordDependencyOutcome` and `RecordHandoff`: no revision fence and not in the brief.

The exchange path is out of scope too. A child task's `Creation` exchange is deduplicated by the choreography journal, not by the operation log.

## Decision 2 — answered right after the window, before every other check

In `AgentRunEntityStore::apply_command` and `AgentTaskEntityStore::apply_once`, a miss in the operation log is followed by a ledger lookup. A hit answers `Duplicate { outcome }`, exactly as a window hit does: on the run it also re-drives the settle pass the committed transition may still owe. The lookup runs for every command, keyed on the id alone, as the window lookup is. An id belongs to one intent, so a different command arriving under a decision's id is already an id reuse.

The lookup comes before every guard, the terminal one included. A replayed Deny that ended its run therefore answers `Duplicate`, not `run-terminal`; a replayed `Cancel` on a now-terminal task answers `Duplicate`, not `task-terminal`. It also comes before the task's history-headroom guard, for the same reason the submission echo does: an answer that writes nothing must not be refused for an observability backlog.

A refusal that is re-answered against a re-materialized record (the stale-cache path both entities already have) consults the ledger again on the fresh record. So a decision another writer committed is answered `Duplicate` there too.

## Decision 3 — durable compatibility

The new field is `#[serde(default)]` and is omitted when empty.
- No schema version moves.
- A record written before this change decodes with an empty ledger.
- An older build reading a newer record ignores the field, by the N/N+1 posture in `docs/rakka-compatibility.md`.

**Not covered:** decisions committed before the upgrade. Their ids are only in the operation log.

**Why no schema version moves.** The run state's version 2 exists because an older peer that rewrote a record would have erased `terminal_at` beyond repair. Here, an older peer that rewrites a record during a rolling update drops the ledger, and nothing worse follows: a replay of a decision it dropped meets today's refusal, which is what every caller handles now, and later decisions are recorded again once the new binary writes. Bumping `agent-run-state` to 3 and `agent-task-state` to 2 would instead make older peers fail closed on every record a new peer has touched, for the length of the rollout. That trade is wrong for a guarantee whose loss degrades only to today's answer.

The ledger sits outside `AgentTask` and `AgentRun`, beside the operation log. So it is not charged against `AGENT_TASK_MATERIALIZED_MAX_BYTES` or `AGENT_RUN_MATERIALIZED_MAX_BYTES`, and no admission budget moves. It is bounded as the log is, by entry count: each entry is the pair the log already holds, so the ledger adds at most half the log's size.

## What this does not change

- **No window size changes.** The brief asked for a key that outlives the window, not a bigger window.
- **No refusal code changes meaning.** A first attempt that meets a guard is refused exactly as before.
- **The checkpoint's own `applied_keys` stays.** It deduplicates a decision while the checkpoint is open, which is a different job.
- **Not "after any number of later operations".** It is after any number of non-decision operations, and up to 31 later decisions.

## What rakka.host can change at the pin bump

- `REPLAY_REACHABLE` shrinks to the codes that a covered command can no longer meet on a replay.
  - That is all seven, for decisions committed after the upgrade.
  - The host keeps `replay_outcome_unknown` for decisions from before it, and for one evicted by 32 later decisions.
  - `records_decision` lets the host tell which commands are covered.
- The two stale-effect codes stop leaving a receipt `pending` forever: a replayed `ResolveIndeterminateEffect` now answers `Duplicate`.

## Proof

**One test per covered command**, with the same shape each time:
1. commit the decision;
2. push its id out of the 64-entry window with non-decision operations (effect results on the run; dependency outcomes or wake admissions on the task);
3. re-post the decision;
4. assert it answers `Duplicate` with the original outcome, and that nothing was written.

**Edge cases:**
- **Deny:** the replayed Deny on a run it ended answers `Duplicate`, not `run-terminal`.
- **Eviction:** the 33rd decision evicts the first, whose replay then meets today's refusal.
- **Pre-upgrade record:** one decoded without the field answers as today.
- **Stale cache:** a resident facade whose cache predates the decision answers `Duplicate` after its one re-read.
