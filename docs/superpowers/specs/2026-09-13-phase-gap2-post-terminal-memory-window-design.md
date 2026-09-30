# Phase gap 2, D4 — accept `PromoteMemory` and `AppendClaim` after a run ends

Status: implemented, 2026-09-13. Branch `rakka-agents-phase-gap2`. Brief: the
consumer's gap-slice-2 brief, D4.

Today both handlers refuse first on `status.is_terminal()` (`run-terminal`)
and then on the wind-down (`run-memory-promotion-fenced` /
`run-claim-append-fenced`). A run that starts and ends inside one application
sweep interval is therefore never promoted or claimed at all. The fix is a
bounded window after the terminal transition during which both commands are
accepted, their effects ride the run's ordinary outbox, and their outcomes
land on the terminal record without moving it.

## Decision 1 — the window is a field on `AgentEffectPolicies`

`AgentEffectPolicies::post_terminal_memory_window_ms: u64`, default
`AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS = 600_000`, builder
`with_post_terminal_memory_window_ms`, getter of the same name. `0` disables
the window and restores today's `run-terminal` refusal. **Code over brief:**
`AgentEffectPolicies` is not a serde type — it is projected from the registry
at registration — so there is no `#[serde(default)]` to add; the default lives
in `new()`, which every projection starts from.

## Decision 2 — the fence, in both handlers

```
if run.status.is_terminal() {
    HandedOff | Superseded          -> Terminal (run-terminal), as today: the
                                       task's responsibility moved; the successor promotes.
    Completed | Failed | Cancelled  -> window == 0            -> Terminal (run-terminal)
                                       terminal_at == None    -> MemoryWindowClosed
                                       now - terminal_at > w  -> MemoryWindowClosed
                                       else                   -> accept
}
handoff-pending fence: only when !is_terminal (a terminal run's handoff cell is settled).
wind-down fence (terminal_reason.is_some() || Cancelling): REMOVED for these two commands.
```

One shared code, `run-memory-window-closed` (`AgentRunError::MemoryWindowClosed
{ status, terminal_at, window_ms }`): the rule is one window for both tiers,
the field is named for both, and the command tells the caller which door it
hit. `MemoryPromotionFenced` / `ClaimAppendFenced` stay as variants (their codes
are registered) but nothing answers them any more; the compatibility bullet
says so.

A terminal record with `terminal_at == None` (persisted before schema 2 and
never backfilled) is *outside* the window rather than measured from
`updated_at`: every accepted post-terminal command moves `updated_at`, so it
would be a sliding clock, and retention already answers `TerminalTimeUnknown`
for exactly these records — the backfill repair gives them a stamp.

**Accepted during wind-down: yes** (constraint 2). The effect kinds are exempt
from the fence either way (Decision 3), so the only fences left are
handoff-pending on a live run and the window on a terminal one. A promotion
committed while a run is `Cancelling` dispatches and lands exactly as one
committed after `Cancelled`.

## Decision 3 — the effect rides the ordinary path

- `exempt_from_wind_down_fence` gains `MemoryPromotionCall | ClaimAppendCall`.
  Intended consequence, stated in the doc and the compat bullet: a promotion
  or claim committed on a live run that then winds down still dispatches.
  `fence_unsent_effects` (cancel and the terminal transition), the settle
  pass's flush filter, the dispatcher's claim path, and `fence_run` all key on
  this one predicate and need no other change — except:
- `dispatch_effects` returns early on a *terminal* run today. That return goes:
  a terminal run folds into `winding_down`, so it flushes exempt kinds only,
  exactly as a `Cancelling` run does now. Nothing else in the settle pass needs
  a terminal exception (`advance_loop` cannot advance; the session flush must
  run so the selection is in the store before the executor reads it).
- `record_effect_result` refuses `Terminal` before looking the effect up. It
  will look the effect up first and refuse `Terminal` when the run is terminal
  and the effect is absent *or not exempt* — every answer a non-exempt result
  gets today is unchanged, including `run-terminal` for a result racing
  completion. `apply_effect_outcome` already treats a terminal run as winding
  down (no status, no phase, no resumption); `settle_run_disposition` already
  returns on a terminal run; the wind-down arm already exempts both kinds
  (D1). The budget settle runs as for any generation: the run's own record
  reserves and settles attempts (constraint 5, `*Unaffordable` unchanged), and
  the task's escrow settlement — folded at the terminal transition — is not
  re-taken. Documented as such.
- `fence_run` verified on the next pass: the exempt `continue` precedes every
  read, so a post-terminal `Ready` effect is neither cancelled nor re-read;
  proven by a real-dispatcher test asserting `pass.cancelled == 0` across two
  passes and the memory written.

## Decision 4 — no reconciliation checkpoint on a terminal run

Both kinds are `Idempotent` by default, so an ambiguous attempt retries under
the generation's idempotency key (`recover_ambiguous`, wind-down or not) and
both executors converge per entry/generation. One path can still park an
idempotent effect: the recovery retry of a possibly-executed attempt refused
by the dispatch authority. On a terminal run `record_effect_result` records
the `Indeterminate` outcome on the effect and opens **no** checkpoint — there
is no run to park, and `settle_run_disposition` already refuses to move a
terminal status. The record stays resolvable by `ResolveIndeterminateEffect`
(it retires matching checkpoints, needs none). Spec 13.3/13.4 say: an
ambiguous post-terminal attempt is retried, and a terminal run opens no
reconciliation checkpoint. A deployment that reclassifies either kind as
non-idempotent forfeits the retry on live and terminal runs alike; not
refused, stated.

## Decision 4b — effect identity survives `clear_turn` (found by the proof)

The first post-terminal proof answered `Duplicate`: `next_effect_slot` counted
the effects still held for the turn, and `clear_turn` had dropped the finished
turn's resolved model call, so the promotion derived that call's identity and
its result operation id was already in the operation log. The same shape
exists on a *live* run resting on its result proposal (`clear_turn` without
`begin_turn`), so this was latent at the pin for any promotion or claim
committed there. Fix: `AgentLoopState::next_slot`, a serde-defaulted per-turn
counter that `record_effect` advances, `begin_turn` resets, and `clear_turn`
leaves alone; `next_effect_slot` takes the max of the counter and the held
effects (the pre-counter floor, so an old record allocates as before). The two
handlers additionally skip any slot whose first-generation result the
operation log already holds (`unresolved_effect_slot`), which covers a record
persisted before the counter; the residual — such a record whose terminal
results were evicted by sixty-four later operations — is recorded in the
compatibility bullet. `agent-loop-state` stays at schema version 1.

## Decision 5 — retention and pumping

`memory-promotion-source-missing` is retryable; once the run's session memory
is purged the effect exhausts, and being exempt it ends nothing — the
terminal record, `terminal_at`, and status are untouched (proven). The default
window (10 min) sits well inside the default retention (30 days). Nothing
upstream drives a terminal run's outbox: an application that stops pumping at
terminal never dispatches a post-terminal effect (compat bullet sentence).

## Proof

Entity-level, in `private_memory_promotion.rs` and `communal_claim_append.rs`:
accepted on `Completed`, `Failed`, `Cancelled` inside the window — dispatched,
outcome applied, status and `terminal_at` unchanged; refused past the window
under the new code with nothing committed; refused under `run-terminal` with a
`0` window; command and result replay write once; the exhausted path leaves
the terminal record untouched; an `Indeterminate` outcome on a terminal run
opens no checkpoint; a post-terminal promotion survives every owner loss
(mirror of `memory_promotion_survives_any_owner_loss`). Real-dispatcher, in
`effect_dispatch.rs` (where `DispatchFixture` lives, gaining a claim hook): a
post-terminal effect survives `fence_run` on two passes, and an
`AfterStarted` worker loss on a post-terminal promotion retries and converges
with no checkpoint. No new scenario row: the proofs sit under the rostered
scenario 16 (private half) and scenario 33 files.
