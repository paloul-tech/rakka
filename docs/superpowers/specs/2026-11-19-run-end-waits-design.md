# A run's end waits for the calls it sent (issues #123, #172, #173)

Design and record, 2026-11-19. Branch `end-waits-for-sent-calls`, off `main`
at `70d71b4`.

## 1. What was wrong

A run that winds down does not become terminal over work whose outcome it
does not know: the settle pass waits until nothing blocks settlement
(specification 8.7 and 11.5). A run whose end was decided for it skipped
that wait. Five arms of `settle_proposal` and both budget paths called
`terminate` directly:

- the task accepts the result, refuses the proposal, or cannot be decoded;
- the task rejects with no rejections left;
- the task rejects while the run is cancelling;
- a budget is spent that cannot be topped up, or a top-up grants nothing.

An ended run takes a result only for the four kinds the wind-down fence
exempts, and answers a reconciliation checkpoint only for a memory
promotion or a claim append (issue #120). So whatever else the run held at
that moment stayed on its record for good. The three issues are three
faces of it:

| Issue | What the run held | What was left |
|---|---|---|
| #172 | a goal evaluation, sent | `Ready` for good; the dispatcher's cancellation refused `run-terminal` |
| #173 | a goal evaluation, outcome unknown | its reconciliation checkpoint open, every decision refused |
| #123 | a compensation or a workflow cancel, in flight | its result taken, but a park opened no checkpoint |

#172 and #173 were reproduced when filed. #123 was filed from reading. It
is reproduced now for both kinds: on `main` the run ends with the
compensation, and with the workflow cancel, still out (section 6).

## 2. Approaches

Put to the owner on 2026-11-19 as one question.

- **The end waits.** Chosen by the owner. The run records the reason it was
  given and waits as a run that is winding down waits. It takes the calls'
  results and answers their parks as a live run, then ends under that
  reason. It reuses the wind-down; no decision gains a new meaning.
- **End at once, settle after.** The ended run would take the result of
  anything it sent and answer its parks, extending #120. It needs a ruling
  on what each reconciliation decision means on an ended run for three
  more kinds, widens what an ended run accepts, and has the host driving
  ended runs until they settle.

A spike of the first, since thrown away, passed 1,767 of 1,769 `rakka-agent`
tests before the question was asked. The two failures held a call that was
never sent, which the rule below leaves to the end's own fence.

## 3. The rule

A run does not end while it holds **a call an end waits for**: a call that
is not a memory promotion or a claim append, and that either was sent, has
an unknown outcome, or is of a kind the fence exempts and is still to be
sent (`AgentLoopState::holds_call_an_end_waits_for`).

`end_when_settled(state, reason, now)` is where a decided end arrives.

- The run holds no such call: `terminate`, in the same transition, exactly
  as before.
- It holds one: the reason is recorded, what was never sent is cancelled
  (`fence_unsent`), and `settle_run_disposition` puts the run on
  `WaitingForReconciliation` if one of those calls has an unknown outcome
  and on `Cancelling` otherwise.

`settle_run_disposition` asks first whether the run's recorded reason is a
decided end (`AgentRunTerminalReason::is_decided_end`). If so it ends the
run the moment it holds no call an end waits for, and otherwise sets the
status above. Every transition that settles a call or answers a park
already ends in that function, so the transition that settles the last
call ends the run.

A run that is cancelling and whose proposal is rejected keeps the
wind-down's own reason. If it waits for nothing else it ends there, as
before. Otherwise the answered proposal is dropped from the record and the
wind-down goes on.

## 4. My calls

1. **What holds the end is narrower than what holds a wind-down.** A
   wind-down waits for every outstanding effect, the proposal and the
   children. A decided end waits for none of the last two, and not for a
   memory promotion or a claim append. Each is what such an end did before,
   and each is right: the proposal's answer is what decided the end; a
   promotion and a claim append are owed after the end by design; and an
   accepted run did not wait for a fan-in straggler to finish.
2. **The status while waiting is `Cancelling`,** and not a new one. The run
   is winding down: it dispatches nothing but what the fence exempts and
   takes no turn. `Cancelling` already ends `Completed` when an acceptance
   wins a race with a cancel. A new status would be a durable vocabulary
   change for a state that lasts one dispatch pass in the common case.
3. **A decided end replaces a wind-down's reason,** as `terminate` did when
   it ended the run on the spot. A wind-down's reason never replaces a
   decided end's: `cancel`, a compensation and a failed effect each record
   theirs only when none is set.
4. **The rejected proposal of a cancelling run is dropped from the record,**
   but only when the wind-down goes on. A wind-down waits for a proposal's
   answer, and there is nothing else on the record that says the answer
   came. Where the run ends at once the record is as before.
5. **A cancelling run whose proposal is rejected now waits for its
   children too.** It skipped that wait. Specification 8.7 does not let a
   run project `Cancelled` over a child that has not reported.
6. **One settle-pass repair,** `AgentRun::needs_decided_end`, for mixed
   builds (section 5). It is unreachable under this build alone.
7. **Records an earlier build ended are not repaired.** None is known.
   Each would need its own ruling: what an ended run does with an
   evaluation it cannot know was invoked, and with a park it never offered.

## 5. Mixed builds

An earlier build that loads a waiting run takes the call's result, and then
does not end the run where a proposal is still on the record or a child is
still out: its wind-down waits for both. It writes nothing wrong. The
record it leaves is `Cancelling` with no call out, and this build's next
settle pass ends it. A test holds that. After a rollback such a run waits
until this build returns.

## 6. Tests

`crates/rakka-agent/tests/run_end_waits.rs`, new, ten tests; one more in
`workflow_tool.rs`.

- An accepted run waits for the evaluation it sent, takes the dispatcher's
  cancellation, and ends `Completed` (#172).
- It takes the evaluation's own result too.
- An accepted run parked on an unknown outcome stays parked, answers the
  checkpoint, and ends (#173).
- An accepted run waits for the compensation it sent (#123).
- An accepted run waits for a straggler's workflow cancel; the cancel ends
  unknown, the park opens its checkpoint, a person answers, the run ends
  (#123, in `workflow_tool.rs`).
- A cancelling run whose proposal is rejected goes on winding down.
- A run out of rejections, and a run out of budget, wait and then end
  `Failed` under their own reasons.
- The dispatcher alone settles what an accepted run waits for.
- A waiting run an older build settled is ended by the next settle pass.
- A run that holds nothing ends at once.

Ten of the eleven fail on `main`, each because the run ended at once. The
mixed-build test is one of the ten: it fails there at its first step, and
it failed on this branch too until the repair of call 6 was written. The
eleventh passes on `main` and holds what must not change.

## 7. Not done

- Records already ended in one of these states (call 7).
- A run parked on an unknown outcome for a completed task waits for a
  person. That is the rule's cost, and what the owner chose.
