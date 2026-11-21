# A turn waits for a call of its own whose outcome is unknown (issue #189)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request.

## 1. What was wrong

A turn rests when the last of its calls comes back: the loop then records
the turn, with the results of its calls, and goes on. Every arm that takes an
outcome asked whether a call of the turn was still *out*
(`awaits_turn_effect`). A call whose outcome is unknown is not out. So when
one call of a turn ended unknown and another's result then arrived, that
result rested the turn, with the first call unanswered.

The run was parked, and the park held the loop. The harm came with the
answer. Answered "never executed", the call was sent again, and the run did
not wait for it: it recorded the turn without that call's result and sent the
next turn's model call.

Measured on `main` at `7be7380`, with two tool calls in one turn:

| Step | Status | Phase | Turn |
|---|---|---|---|
| The first call ends unknown | `waiting-for-reconciliation` | `awaiting-tools` | 1 |
| The second call's result lands | `waiting-for-reconciliation` | `recording-turn` | 1 |
| The park is answered "never executed" | `waiting-for-effect` | `awaiting-model` | 2 |

Calls sent go from 3 to 5 at the answer: the first call again, and a model
call for turn 2. Turn 1 is in the session memory with one tool result. The
call's result arrives while turn 2 is in flight.

The same happened beside a fan-in wait. A workflow's start receipt rested
the turn on its children with a tool call unknown. Once the wait was
resolved, by its deadline or by the child's report, "never executed" took
the run to turn 2 with the tool call out.

Where the turn stood depended on which of two outcomes arrived first. Had
the second call's result landed before the first ended unknown, the turn
stayed open at `awaiting-tools`, and the answer was waited for.

Only the answer that sends the call again did harm. An answer that says what
the call returned records the result before the loop takes a step.

## 2. Approaches

1. **Change what rests a turn.** A turn rests when none of its own effects
   is out or unknown. The turn stays open through the park, and the arm that
   takes the answer, or the result of the call the answer sends again, rests
   it.
2. **Leave the turn rested and hold the loop at the record step** while a
   call of the turn is out. The phase would say the turn is over while one of
   its calls is in flight, and a child's report that resolves the wait would
   write `Running` on a run that is waiting for a call.
3. **Open the turn again in the answer**, when "never executed" sends a call
   of a rested turn. Where the turn stands would still depend on the order
   of two outcomes, and every other reader of the phase would still see a
   rested turn with a call unanswered.

I chose the first. The settle pass's own rest of a turn already asks it
(`AgentRun::needs_turn_rest` leaves a turn with an ambiguous effect to its
reconciliation decision, and says why). The arms did not.

## 3. The rule

A turn rests when the last of its own effects has come back. An effect whose
outcome is unknown has not come back: the turn rests when its answer says
what it returned, or, where the answer sends it again, when that call
returns. Where a turn stands does not depend on the order in which two
outcomes arrive.

## 4. The change

- **`AgentLoopState::has_unsettled_turn_effect`.** Whether an effect of a
  kind inside the turn is out or unknown. `awaits_turn_effect`, which is
  public, keeps its meaning: what is out.
- **`rest_turn_if_settled`.** The nine arms that rest a turn each carried
  the same five lines. They are one function now, and it asks the new
  condition. The arms are: a tool result, an A2A send's receipt, a handoff
  send's receipt, a workflow start's receipt, a handoff send its task
  already answered, a handoff send that failed, a fan-out member's send or
  start that failed, and the task's refusal of a handoff.
- **`AgentRun::needs_turn_rest`** asks the same condition. It asked both
  halves already, in two clauses.
- **The settle pass** (`AgentRun::needs_turn_reopen`, `reopen_rested_turn`).
  A live run that is not winding down, whose phase is `RecordingTurn` or
  `AwaitingChildren`, and which holds a call whose outcome moves the run
  that is out or unknown, has its phase put back to `AwaitingTools` before
  the loop's step. That is a record an earlier build left. A parked run
  stays parked. A run that is not parked takes the dispatch wait of a turn
  with a call out.

Nothing durable is added: no field, status, error code or decision kind.

## 5. My calls

1. **Every kind inside the turn holds it while unknown, a goal evaluation
   and a workflow cancel included.** What holds a turn while it is out holds
   it while it is unknown. That is the settle pass's existing rule, and it
   is what makes the order of two outcomes not matter. It changes one thing
   beyond the issue: a turn whose last tool result lands while a goal
   evaluation is unknown used to rest, and after "never executed" went on
   with the evaluation out. It now waits for the evaluation sent again, as
   it waits for one that is out when the tool result lands. Yours to
   reverse; the narrower rule is "a call whose result is recorded with the
   turn".
2. **The nine copies of the rest decision are one function.** The defect
   was the same line nine times. Seven of the nine have no test world that
   puts an unknown call beside them, so as nine copies seven would be
   unheld.
3. **A record an earlier build left is repaired, not held at the record
   step.** The settle pass puts its phase back, and from there the record is
   one this build writes. Such a record lasts until a person answers, so
   they exist.
4. **The repair asks only the kinds whose own arm rests the turn**: the
   model call, a tool call, an A2A send, a workflow start. A goal evaluation
   or a workflow cancel is committed at any rest, so one out or unknown
   beside a rested turn is a state this build writes, and is left.
5. **The repair writes the dispatch wait on a run that is not parked.** One
   record needs it: an earlier build's child report wrote `Running` and the
   turn's phase over a call that had been sent again, and the process
   stopped before its next step.
6. **What is not repaired.** A record whose turn an earlier build already
   recorded without the result: the run is on a later turn with the call of
   the earlier one out, or has taken its late result into the later turn.
   The record of the earlier turn is in the session memory and is not
   rewritten.
7. **The phase a parked run shows changes.** A run parked on a call of its
   turn with nothing else out shows `awaiting-tools` where it showed
   `recording-turn` or `awaiting-children`. An earlier build writes the same
   record whenever the unknown outcome arrives last.
8. **The specification's paragraph on the turn's rest is amended** (9.4):
   `AwaitingTools` holds while an effect of the turn is outstanding or
   ambiguous. Its closing sentence already said so.

## 6. Mixed builds and rollback

An earlier build reads a record this build left parked at `AwaitingTools`
with a call unknown as it reads its own: it writes that record itself when
the unknown outcome is the last to arrive. It waits for the answer, and
rests the turn from the arm that takes it.

This build repairs an earlier build's rested record at its next settle pass.
An earlier build that takes such a record back still has the defect.

## 7. Tests

Fourteen whole-run tests and two unit tests.

In `crates/rakka-agent/tests/turn_unknown_call.rs`, a new file. One turn makes
two tool calls, a charge and a look-up, and the session memory is kept so the
recorded turn can be read:

- **The last result does not rest a turn whose other call is unknown.** The
  reply to the look-up's result says `WaitingForReconciliation` at
  `AwaitingTools`, which holds the transition and not a later pass. Nothing
  is recorded, and a further pass writes and sends nothing.
- **A call answered "never executed" is waited for before the turn is
  recorded.** After the answer the run is `WaitingForEffect` at
  `AwaitingTools` on turn 1, one call more is sent, no model call of turn 2
  exists, and nothing is recorded. When the charge returns, the session
  memory holds turn 1 with both results and turn 2's model call is out.
- **A call answered as executed rests the turn with its result.** This held
  before the change and holds after it.
- **A turn with two unknown calls rests when the second comes back.** The
  answer to the first does not rest the turn.
- **The order of the two outcomes does not decide where the turn stands.**
  The look-up's result first and the charge unknown after it, or the other
  way round: the same status, phase and held results. The first order is
  the one that was already right.
- **A turn an earlier build rested over an unknown call is opened again.**
  The settle pass puts the phase back in one write, sends nothing, and
  measures no turn. From there the answer is waited for, and the turn is
  measured once, when it is recorded.
- **Such a turn waits for a call its first command sends again.** The answer
  is the first thing this build does to the record, and the pass that
  follows it opens the turn before the loop can record it.

In `workflow_tool.rs`, a turn that looks something up, starts a workflow and
awaits it under a deadline:

- **A start receipt does not rest a turn whose tool call is unknown.** The
  reply says `AwaitingTools`. The deadline then resolves the wait under the
  open turn and moves nothing. After "never executed" one call is sent and
  the run waits; the look-up's return records the turn.
- **A tool call sent again rests its turn on the child it still awaits**,
  and the child's report takes the run on.
- **A turn an earlier build rested on its children over an unknown call is
  opened again**, by a settle pass, and stays open through the child's
  report. An answer that says what the look-up returned rests it.
- **The same when the child's report is the first thing this build does to
  the record.**
- **The same when the answer is, and the child reports while the call is
  out.** The report leaves the turn waiting on the look-up.
- **A turn an earlier build moved on with a call out is put back on its
  wait** (call 5): `RecordingTurn` and `Running` with the look-up out
  becomes `AwaitingTools` and `WaitingForEffect`, and no turn is recorded.

In `goal_evaluation.rs`:

- **A turn is held for an evaluation whose outcome is unknown** (call 1),
  through the tool's result, and after "never executed" for the evaluation
  sent again.

Two unit tests in `run.rs`. One holds the rest condition over every status a
call of the turn can have, beside an unknown memory promotion, and holds the
settle pass's own rest to the same condition. The other holds whom the
repair asks: a live run that is not winding down, at `RecordingTurn` or
`AwaitingChildren`, with a call whose outcome moves the run out or unknown.

**The records of an earlier build.** Two of the three are this build's own
record with the values the earlier build wrote differently: the phase, and
the phase in the stored reply to the result that rested the turn. Each was
compared with the record the unfixed source writes at that point, and is
identical to it byte for byte. The third, the turn moved on with a call
out, exists only where a process stopped between two steps. It is built
from what the earlier report's transition writes and was not compared.

**On `main` at `7be7380`** thirteen of the fourteen fail, and the fourteenth
is the one that holds what was already right. Twelve fail at the assertion
they are named for. The thirteenth, the turn moved on with a call out,
fails in its setup: the unfixed source records the turn before the record
it forges can be built.

## 8. Breaks

Eighteen breaks of the change were run one at a time, each against the unit
tests and the five test files nearest it, and a survivor again against the
whole crate. All eighteen are caught.

The two layers are held apart. With the arms' condition put back to what is
out, the repair opens the turn again in the same command's settle pass, so
the run ends up where it should, one write later. What holds the arms is
therefore what a later pass cannot mend: the reply to the command that
lands the result, the goal evaluation's turn, which the repair does not
ask, and the unit test.

- **The rest condition.** Asking only what is out, which is the defect: the
  two replies, the evaluation's test and the unit test. Counting an effect
  outside the turn: the unit test. Counting only what moves the run: the
  evaluation's test.
- **`rest_turn_if_settled`.** Asking only what is out, and asking nothing:
  the replies and the evaluation's test, and for the second an existing
  test of a fan-out member's gate.
- **The settle pass's own rest** asking only what is out: the unit test
  alone. A parked run is not asked, so no whole run reaches it.
- **The repair's step** removed: every test of an earlier build's record.
  Not being what lets the pass act: the four tests whose record owes no
  other step. Measured as a turn: the turn-duration count.
- **Whom the repair asks.** A run that is winding down, and an ended run:
  the unit test alone. Only `RecordingTurn`, and only `AwaitingChildren`:
  the tests of the other phase. Every effect of the turn: thirty-three
  existing tests, which is how often a goal evaluation or a workflow cancel
  is out beside a rested turn (call 4). Only a call that is unknown, and
  only one that is out: the tests of the other.
- **What the repair writes.** Leaving the status as it finds it: the turn
  moved on with a call out. Rewriting the status of a parked run: the single
  write. That break survived its first run, because the guard of issue #186
  parks the run again in the next step of the same pass; the assertion that
  the repair is one write was added for it.

**The repair's place among the repairs is not held.** Moved to be the first
step of the pass, and to be the last before the loop's own, the whole crate
passes each time. What is held is that it runs before the loop's step.

**Not held by a test of its own:** seven of the nine arms have no world that
puts an unknown call beside them. They share one function with the two that
do (call 2).

The whole crate passes: 1,871 tests.
