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

Whether the turn had rested depended on which of the two outcomes arrived
first. Had the second call's result landed before the first ended unknown,
the turn stayed open at `awaiting-tools`, and the answer was waited for.

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
   of a rested turn. Whether a turn has rested would still depend on the
   order of two outcomes, and every other reader of the phase would still
   see a rested turn with a call unanswered.

I chose the first. The settle pass's own rest of a turn already asks it
(`AgentRun::needs_turn_rest` leaves a turn with an ambiguous effect to its
reconciliation decision, and says why). The arms did not.

## 3. The rule

A turn rests when the last of its own effects has come back. An effect whose
outcome is unknown has not come back: the turn rests when its answer says
what it returned, or, where the answer sends it again, when that call
returns. Whether a turn has rested does not depend on the order in which the
outcomes of two of its effects arrive.

The rule is about what rests a turn, and not about everything a run shows.
A run parked before its turn commits its calls stands elsewhere than one
parked after, as it did: the park holds the loop where it finds it.

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
  `AwaitingChildren`, and which holds a call of that turn whose outcome
  moves the run and is out or unknown, has its phase put back to
  `AwaitingTools` before the loop's step. That is a record an earlier build
  left. A parked run stays parked. A run that is not parked takes the
  dispatch wait of a turn with a call out.

Nothing durable is added: no field, status, error code or decision kind.

## 5. My calls

1. **Every kind inside the turn holds it while unknown, a goal evaluation
   and a workflow cancel included.** What holds a turn while it is out holds
   it while it is unknown. That is the settle pass's existing rule, and it
   is what makes the order of two outcomes not matter. It goes beyond the
   issue for those two kinds, in the same way for each: a turn whose last
   tool result lands while one is unknown used to rest, and after "never
   executed" went on with that call out. It now waits for the one sent
   again, as it waits for one that is out when the tool result lands.

   **The cost.** While a turn is held for an evaluation or a cancel sent
   again, the run reads `Running`: neither is a call the run's status waits
   on (issues #131 and #130). Where the evaluation is a human review, the
   one sent again waits behind a fresh approval, so the turn is held for a
   person while the run reads `Running`. That state is documented under
   issue #166 ("a held turn whose reconciliation park is
   answered is still left `Running`, with a goal evaluation's gate open or
   not"). This change reaches it in one more order of arrival: where it used
   to rest and go on.

   Yours to reverse. The narrower rule is "a call whose result is recorded
   with the turn", and it is narrower by exactly those two kinds.
2. **The nine copies of the rest decision are one function.** The defect
   was the same line nine times. Five of the arms now have a test that puts
   an unknown call beside them. The other four are a handoff's, and a
   handoff is its turn's only call: it is refused beside any effect that is
   out or unknown, and it fences evaluations. For those four the new
   condition equals the old in every state a run can reach.
3. **A record an earlier build left is repaired, not held at the record
   step.** The settle pass puts its phase back, and from there the record is
   one this build writes. Such a record lasts until a person answers, so
   they exist.
4. **The repair asks only the kinds whose own arm rests the turn**: the
   model call, a tool call, an A2A send, a workflow start. A goal evaluation
   or a workflow cancel is committed at any rest, so one out or unknown
   beside a rested turn is a state this build writes, and is left.
5. **The repair writes the turn's dispatch wait on a run that is not
   parked.** One record needs it: an earlier build's child report wrote
   `Running` and the turn's phase over a call that had been sent again, and
   the process stopped before its next step.
6. **The repair asks only a call of the turn the run is on, so what is not
   repaired is left exactly as it was.** Where an earlier build had already
   recorded a turn without a result, the run is on a later turn with that
   call out. The later turn is not held for it: the run goes on as it did
   under the build that left the record, and the earlier turn's record in
   the session memory is not rewritten. My first version asked a call of any
   turn, and the review showed what that did (section 9).
7. **The phase a parked run shows changes.** A run parked on a call of its
   turn with nothing else out shows `awaiting-tools` where it showed
   `recording-turn` or `awaiting-children`. An earlier build writes the same
   record whenever the unknown outcome arrives last.
8. **The specification's paragraph on the turn's rest is rewritten** (9.4).
   It said `AwaitingTools` holds exactly while an effect of the turn is
   outstanding. It now says a turn there stays there while one is
   outstanding or ambiguous, and that an effect committed beside a turn
   already at rest does not open it again.

## 6. Mixed builds and rollback

An earlier build writes the record this build now writes whenever the
unknown outcome is the last to arrive. Where that call is all the turn
waits for, an earlier build that takes such a record back takes the answer
as this build does. Where another call of the turn is still out or unknown,
it rests the turn at the next result or answer, which is the defect.

This build repairs an earlier build's rested record at its next settle pass.
An earlier build that takes such a record back still records it too early.

This build records an `awaiting-children` wait in the decision events when
a turn it opened again comes to rest on its children. The earlier build
recorded one when it first rested that turn, so the events of such a turn
show the wait twice, as they do for any turn that enters it twice.

## 7. Tests

Twenty whole-run tests and two unit tests.

In `crates/rakka-agent/tests/turn_unknown_call.rs`, a new file. One turn makes
two tool calls, a charge and a look-up, and the session memory is kept so the
recorded turn can be read:

- **The last result does not rest a turn whose other call is unknown.** The
  reply to the look-up's result says `WaitingForReconciliation` at
  `AwaitingTools`, which holds the transition and not a later pass. Nothing
  is recorded, and a further pass writes and sends nothing.
- **The order of the two outcomes does not decide where the turn stands.**
  The look-up's result first and the charge unknown after it, or the other
  way round: the same status, phase and held results. The first order is
  the one that was already right.
- **A call answered "never executed" is waited for before the turn is
  recorded.** After the answer the run is `WaitingForEffect` at
  `AwaitingTools` on turn 1, one call more is sent, no model call of turn 2
  exists, and nothing is recorded. When the charge returns, the session
  memory holds turn 1 with both results and turn 2's model call is out.
- **A call answered as executed rests the turn with its result.** This held
  before the change and holds after it.
- **A turn with two unknown calls rests when the second comes back.** The
  answer to the first does not rest the turn.
- **A turn an earlier build rested over an unknown call is opened again.**
  The settle pass puts the phase back in one write that carries the time of
  its pass, sends nothing, and measures no turn. From there the answer is
  waited for, and the turn is measured once, when it is recorded.
- **Such a turn waits for a call its first command sends again.** The answer
  is the first thing this build does to the record, and the pass that
  follows it opens the turn before the loop can record it.
- **A turn an earlier build recorded without a result is left as it was**
  (call 6). The run is on turn 2 with turn 1's charge out. Turn 2 makes no
  call: it is recorded as soon as its model output lands, its result is
  proposed and accepted, and the run waits only to end. The session memory
  holds turn 1 with the look-up alone, before and after the charge returns.

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

In the same file, a turn that also delegates, and one with a workflow cancel
out beside it. The review asked for these (section 9):

- **A send receipt, a member's failed send, and a member's failed start do
  not rest a turn whose tool call is unknown.** Three tests, each on the
  reply to the outcome that is the last of the turn's calls to come back.
- **A tool result does not rest a turn whose send, or whose start, is
  unknown.** One test over the two kinds.
- **A turn is held for a workflow cancel whose outcome is unknown** (call
  1), through the tool's result. After "never executed" the run reads
  `Running` at `AwaitingTools` with one call sent, and the pass that follows
  the cancel's return records the turn.

In `goal_evaluation.rs`:

- **A turn is held for an evaluation whose outcome is unknown** (call 1),
  through the tool's result, and after "never executed" for the evaluation
  sent again.

Two unit tests in `run.rs`. One holds the rest condition over every status a
call of the turn can have, beside an unknown memory promotion, and holds the
settle pass's own rest to the same condition. The other holds whom the
repair asks: a live run that is not winding down, at `RecordingTurn` or
`AwaitingChildren`, with a call of that turn whose outcome moves the run out
or unknown. A call of an earlier turn is not asked.

**The records of an earlier build.** Four are forged, each from this build's
own record:

- The turn rested over an unknown call, and the turn rested on its children
  over one. Each carries the values the earlier build wrote differently:
  the phase, and the phase in the stored reply to the result that rested
  the turn. Each is identical, byte for byte, to the record the unfixed
  source writes at that point.
- The turn recorded without a result. Compared with the record the unfixed
  source writes, it differs in three clock values.
- The turn moved on with a call out, which exists only where a process
  stopped between two steps. I did not compare it. The review did, with the
  loop's next step held back: it differs in five clock values.

**On `main` at `7be7380`** nineteen of the twenty fail, and the twentieth is
the one that holds what was already right. Seventeen fail at the assertion
they are named for. Two fail in their setup, because the unfixed source does
not pass through the record they forge: the turn moved on with a call out,
and the turn recorded without a result. The second of those is no test of
the defect. The unfixed source ends that run where this build does, and
what holds the test is the break that widens the repair to a call of any
turn.

## 8. Breaks

Thirty-five breaks of the change were run one at a time, each against the
unit tests and the three test files above, and a survivor again against the
whole crate. Thirty are caught. Five are not, and each is named below.

The two layers are held apart. With the arms' condition put back to what is
out, the repair opens the turn again in the same command's settle pass, so
the run ends up where it should, one write later. What holds the arms is
therefore what a later pass cannot mend: the reply to the command that
lands the outcome, the turns held for an evaluation or a cancel, which the
repair does not ask, and the unit test.

- **The rest condition.** Asking only what is out, which is the defect: ten
  tests. Counting an effect outside the turn: the unit test. Counting only
  what moves the run: the evaluation's and the cancel's tests.
- **Each kind excused alone while unknown.** A tool call, an A2A send, a
  workflow start, a workflow cancel, a goal evaluation: each is caught by a
  whole-run test of its own.
- **`rest_turn_if_settled`.** Asking only what is out, and asking nothing:
  the replies and the two held turns, and for the second an existing test
  of a fan-out member's gate.
- **Each arm alone put back to what is out.** The tool result, the send
  receipt, the start receipt, the member's failed send and the member's
  failed start: each is caught by its own test. **Not caught: the four arms
  of a handoff**, which pass the files run here; the review ran the whole
  crate against each, and it passes. A handoff is its turn's only call
  (call 2), so no run reaches those arms with another call of the turn out
  or unknown.
- **The settle pass's own rest** asking only what is out: the unit test
  alone. A parked run is not asked, so no whole run reaches it.
- **The repair's step** removed: every test of a record the repair opens.
  Not being what lets the pass act: the four tests whose record owes no
  other step. Measured as a turn: the turn-duration count.
- **Whom the repair asks.** A run that is winding down, and an ended run:
  the unit test alone. Only `RecordingTurn`, and only `AwaitingChildren`:
  the tests of the other phase. A call of any turn: the turn recorded
  without a result, and the unit test. Only a call that is unknown, and
  only one that is out: the tests of the other. Every effect of the turn:
  six tests in the files run for it. The review ran that break over the
  whole crate, before the repair was held to one turn, and counted forty:
  that is how often a goal evaluation or a workflow cancel is out beside a
  rested turn (call 4).
- **What the repair writes.** Leaving the status as it finds it: the turn
  moved on with a call out. Rewriting the status of a parked run: the single
  write. A write that carries no time: the record's stamp. **Not caught:
  writing the status of what the run rests on in place of the turn's
  dispatch wait.** The two agree unless a goal evaluation's gate is open
  beside the call, and no test builds that. The turn's dispatch wait is
  what a turn that commits with a call out is given, so the repair writes
  that.

**The repair's place among the repairs is not held.** Moved to be the first
step of the pass, and to be the last before the loop's own, the whole crate
passes each time. What is held is that it runs before the loop's step.

The whole crate passes: 1,877 tests.

## 9. Review

A fresh reviewer read the first commit (`2bcae273`), with about twenty-five
probes of its own, a mutation harness, and 6,000 random schedules of one
turn's outcomes. It could not build a run on which this build records a turn
while a call it made is out or unknown, and found no wedge and no settle
pass that spins or keeps writing. Each neighbouring repair's tests hold that
repair as they did before. The second commit answers what it found.

- **The repair reached the record I had called "not repaired".** A run an
  earlier build had left on turn 2 with turn 1's charge out, whose turn 2
  made no call, was held at `AwaitingTools` for that charge, and the
  charge's late result was recorded as a tool result of turn 2. The earlier
  build proposes at once and never records it. The repair now asks only a
  call of the turn the run is on (call 6), and a test holds the run to what
  the earlier build does with it.
- **No test held that an unknown workflow cancel holds a turn**, though the
  docs said it, nor an unknown workflow start, and an unknown A2A send was
  held by the unit test alone. Each now has a whole-run test.
- **Three of the seven arms I said had no test world have one.** The review
  built them from fixtures already in the file. They are tests now, and
  call 2 says which four remain and why.
- **The rollback sentence was wrong.** An earlier build that takes back a
  record with the turn open rests it at the next result or answer when
  another call of the turn is out or unknown. Section 6 and the
  compatibility entry say so.
- **The status after "never executed" was misstated**: a gated call waits
  behind a fresh gate, and an evaluation or a cancel leaves the run
  `Running`. The changelog's sentence on the repair read as if it opened a
  turn rested over an evaluation or a cancel.
- **Call 1's cost was not stated**, and it named the evaluation and not the
  cancel. It is stated now.
- **The specification's new sentence was too broad.** Where a run stands
  can depend on the order of a model's result and an unknown evaluation;
  whether a turn has rested does not. The paragraph also said `AwaitingTools`
  holds "exactly while", against call 4.
- **Four comments and one sentence of the guide were no longer true.**
  Corrected, with two older compatibility entries that spoke of what rests
  a turn.
- **Two breaks of the repair's write survived.** The record's stamp is held
  now. The other is the one section 8 names as not caught.
- Smaller: a count that was the files' and not the crate's, a status the
  unit test's table left out, an unlabelled assertion.

The review did not get to a real dispatcher fleet or a PostgreSQL store,
and read the handoff worlds without probing them.
