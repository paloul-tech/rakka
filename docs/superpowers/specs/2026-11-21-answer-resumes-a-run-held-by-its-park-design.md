# The answer that sets a parked run going closes its `run-resume` segment (issue #193)

2026-11-21. Short design, with my calls listed. The owner reviews at the pull
request. This is the second half of issue #191, and it is stacked on the fix
for the first half (PR #192). Section 11 says what the review of the first
build changed.

## 1. What was wrong

A command closes a `run-resume` segment only when it finds the run on a wait
of its phase: `AwaitingModel`, `AwaitingTools` or `AwaitingChildren`. A run
parked on an unknown outcome can stand at a phase that is no wait. The
answer that takes the park away and sets that run going finds it there, so
no timer starts and no segment closes.

Measured on the branch of PR #192 at `595a5048`. The last row is the same on
`main` at `49feccf`.

| Step | The run after it | What the command closed |
|---|---|---|
| A goal evaluation's outcome lands unknown while the model call is out | `WaitingForReconciliation` at `AwaitingModel` | |
| The model's result lands | `WaitingForReconciliation` at `EvaluatingModelOutput` | No `run-resume` (on `main`: one, which was #191) |
| The park is answered "never executed", with the task out of reach | `Running` at `DecidingContinuation`, on its proposal | `checkpoint-resolve`, and no `run-resume` |

The turn in which the run was parked, answered, and went on to its proposal
shows no resume. Segments are observability: no state and no decision reads
them.

## 2. What a fix has to respect

The first build of #191 asked what holds the run before every command as
well as after it, and its review measured that: 52 commands of the crate's
tests gained a segment and 48 lost one. Three things went wrong.

- **A hold that a reply ends.** A top-up park, and a wind-down on a
  proposal, are ended by the reply to an exchange the run itself sent.
  Whichever pass carries that exchange takes the reply, and every committing
  command and every replay runs one. A `Duplicate` replay closed a segment.
- **A run parked while it stands on its proposal.** The answer takes the
  park away and leaves the run where it does nothing until its task
  decides. That doubled the count for such a run.
- **A settle pass that mends a stale wait** does so on whatever command runs
  it.

## 3. Approaches

1. **Start the timer on a reconciliation park that alone holds the run, and
   close it only for the command that answered a park.**
2. **Start it on any park at a phase that is no wait.** The run parked on
   its proposal is one, and so is the parked run that is winding down, whose
   answer ends it and sets nothing going.
3. **Ask what holds the run on both sides**, which is what section 2
   measured.

I chose the first.

## 4. The rule

The `run-resume` timer starts when a command finds the run on a wait of its
phase, as before, **or held by a reconciliation park alone**
(`AgentRun::held_by_its_park_alone`): the run is `WaitingForReconciliation`,
it is not winding down, and with the park taken away the loop would take a
step. That is a run at `PreparingContext`, `EvaluatingModelOutput` or
`RecordingTurn`, or at `DecidingContinuation` with no proposal pending, that
asks for no top-up.

The segment closes as before, when the command's pass leaves the run ended
or with nothing holding it (`AgentRun::has_resumed`). **From a park it
closes only for the command that answered a park**: the command named a
reconciliation checkpoint and resolved it, and the record did not show the
run winding down.

**`has_resumed` gains a fifth hold: a wind-down the command began.** A
cancellation, a failed effect or a compensation winds the run down without
its having gone anywhere. An end decided for the run within the pass is
still no hold: a run whose result is accepted while it holds a call went on
to that end.

The standing rule is untouched: a command that leaves the run on its next
wait closes none. An answer that takes a parked run from `RecordingTurn` to
its next model call is one.

**Measured over the crate's tests**, the tests of this change included. I
logged every command that starts a timer, with its verdict under the base's
rule and under this one.

| | Commands |
|---|---|
| Start a timer | 6,539 |
| From a wait of the phase | 6,509 |
| From a park that alone holds the run | 30 |
| From a wait of the phase, with a different verdict than under the base | 0 |

Of the 30 from a park:

| The command | Commands | Closes `run-resume` |
|---|---|---|
| An answer that leaves the run on its next model call or its tools | 16 | No |
| An answer that takes the run on to its proposal | 3 | Yes |
| An answer that takes the run on to its end | 1 | Yes |
| An answer that takes the run to an end decided for it, with a call still out | 1 | Yes |
| An answer that schedules a compensation | 1 | No |
| An answer given again against a run cancelled behind the cache | 1 | No |
| A command that resolved nothing, on a stale park | 3 | No |
| A gate's grant, on a stale park | 1 | No |
| A cancellation | 3 | No |

The five commands that close one are in tests of this change, one of which
#191 wrote and this change amends. No earlier test world reaches the case.

## 5. The change

- `AgentRun::held_by_its_park_alone`.
- `AgentRun::loop_would_step`, which is the half of `can_advance` that is
  not the status, taken out so that both can ask it. `can_advance` answers
  as it did.
- `AgentRun::has_resumed`: the fifth hold.
- `AgentRunEntityStore::apply_unboxed`: where the timer starts, whether the
  checkpoint the command names is a park's, and what the segment asks from
  a park where it closes.

The resume an answer closes carries the resolution's two links, to the span
that parked the run and to the request. That code was already there for an
answer on a wait of the phase.

Nothing durable is added: no field, status, error code, decision kind or
segment class.

## 6. My calls

1. **Only a reconciliation park.** An approval or an authorization wait at a
   phase that is no wait reaches no test in the crate, so there is no world
   to hold a rule for it.
2. **"Alone" leaves out three parked runs**, each for the reason section 2
   gives or #193 does: one that stands on its proposal, one that also asks
   for a top-up, and one that is winding down.
3. **The command must have answered a park**: named a reconciliation
   checkpoint, and resolved it. A gate granted on a parked run resolves a
   checkpoint too. My first build asked only for a resolved checkpoint, of
   any kind (section 11).
4. **The wind-down is asked twice.** The predicate leaves out a run that is
   winding down as the cache shows it. Where the command is refused on the
   cache and answered again, the record read again may show a wind-down the
   cache did not, and an answer on that run closes none either.
5. **A wind-down the command began is a hold for every command**, not only
   from a park. It is in `has_resumed`, which now says what its comment
   said. From a wait of the phase it changes no verdict in the crate's
   tests: a wind-down's own reasons leave the phase on its wait.
6. **The standing rule is kept.** Sixteen of the twenty-three answers leave
   the run on its next wait and close none, as a tool result that leads to
   the next model call closes none.
7. **`can_advance` is split in two.** A pure extraction, so that the new
   predicate asks the same phases the crank does and cannot drift from it.

## 7. Mixed builds and rollback

Nothing is stored. An earlier build closes the segments it closed.

## 8. Tests

Eleven whole-run tests in `run_end_waits.rs` and one unit test of each
predicate. Two of the eleven were written for #191, and one of those is
amended here. Each whole-run test reads the segments the run closed from an
in-memory sink.

**The four that fail on the base** (PR #192 at `595a5048`):

- **A model result on a parked run closes no resume segment**, which #191
  wrote. Its second half held the cost of #191's first call: the answer
  closed none. It now holds that the answer closes the turn's one resume.
- **The resume an answer closes links the park and the request.** A traced
  run. The segment's two links name the span the park exported and the
  span of the answer's request.
- **The park answered through the effect command closes the resume with its
  links.** `ResolveIndeterminateEffect`, the other command that answers a
  park.
- **The answer that takes a parked run to its end closes a resume
  segment.** With the task in reach, the run proposes and is accepted
  within the answer's pass. Answered "abandon", it then ends. Answered
  "never executed", the evaluation is sent again and the accepted run waits
  for it: an end decided and not yet reached, which is no hold.

**The seven that hold what closes none**, and pass on the base. The last
three fail on this change's first build:

- **A park answered on a run that stands on its proposal**, which #191
  wrote.
- **The answer that ends a parked run that is winding down.**
- **The answer to a park on a run that asks for a top-up.** The answer's
  pass carries the top-up request, the task refuses, and the run ends: by
  the refusal, and not by the answer.
- **A stale park taken away.** A record parked with nothing left to
  reconcile, as a build before issue #131 could leave one, at its evaluated
  model output. Three commands touch it first, each in its own world: a
  replay, a second evaluation commanded, and the checkpoint timers. The
  last two commit. Each takes the park away, the run goes on to its
  proposal, and none closes a segment.
- **A gate granted on a stale park.** The same record beside a claim append
  that waits behind an approval. The grant resolves a checkpoint and
  answers no park.
- **A compensation answered on a park that alone holds the run.** The
  answer begins the run's wind-down.
- **An answer given again against a run cancelled behind the cache.** A
  resident facade and a facade that reads the record, each closing none.

The stale park is built from this build's parked record by settling the
evaluation and dropping its checkpoint. The reviewer read the source of the
build that leaves such a record and found it the same; neither of us ran
that build.

`workflow_tool.rs` already holds, from #191, that an answer which takes a
parked run to its next model call closes none.

One unit test holds `held_by_its_park_alone` over every phase the loop
steps from, a wait of the phase, each of the fourteen other statuses, a
top-up request, a wind-down and a pending proposal, and that `can_advance`
is false of the parked record and true of the same record under `Running`
at those four phases. The unit test of `has_resumed` holds the fifth hold
and the end decided within the pass.

## 9. Breaks

Seventeen breaks were run one at a time, each against the unit tests and
five test files, and a survivor again against the whole crate. Fifteen are
caught, none of them only by the whole crate.

- **The timer not started from a park, which is the defect**: the four
  tests that fail on the base.
- **What "answered a park" asks.** A command that applied counted as one
  that resolved: the stale park's test and the gate's. The checkpoint
  resolved need not be a park's: the gate's test. The wind-down the record
  read again shows not asked: the test of the run cancelled behind the
  cache. Only the checkpoint command counted: the effect command's test.
- **Every resume needing an answered park, from a wait of the phase too**:
  eight tests.
- **What holds a run.** A wind-down the command began not a hold: the
  compensation's test and the unit test. An end decided within the pass a
  hold too: the accepted run's test and the unit test.
- **What "alone" leaves out.** A run that asks for a top-up counted, one
  that stands on its proposal counted, the loop not asked at all: each by a
  whole-run test and the unit test.
- **Caught by the unit test alone.** A run that is winding down counted:
  the second guard of call 4 closes none for that run, so only the
  predicate's own test tells. Only a park at the evaluated model output
  counted: from the other three phases an answer lands on the next model
  call and closes none, so no whole-run test can tell them apart. An
  approval or an authorization wait counted as a park, and any status
  counted as one (call 1).

**Not caught, two.**

- **The command need not have resolved the checkpoint it names.** With the
  kind asked, this differs only for a command that names a park's
  checkpoint, does not resolve it, and still leaves the run resumed. An
  escalation, a refusal and a duplicate each leave the run parked. I kept
  the condition: the segment's links come from the resolution.
- **A lost record closing from a park on the run's end alone.** No test
  drops the record from a park (section 10).

The whole crate passes: 1,900 tests.

## 10. Not done

- **An approval or an authorization wait at a phase that is no wait** (call
  1).
- **The outcome that ends a run found winding down at a phase that is no
  wait**, and a top-up's answer. #193 leaves both out, and says why.
- **An answer whose pass errors after its commit** closes the segment with
  the run mid-loop, where the clean answer would have left the run on its
  next wait and closed none. A command from a wait of the phase does the
  same, on the base and before it.
- **An answer whose settle pass loses a write and drops the record** closes
  none on that call and none on its replay, unless the run had ended at the
  commit. The validation matrix already records this as owed for a wait of
  the phase.
- **A park with no checkpoint**, as a build before issue #106 could leave
  one, answered through the effect command: the run goes on and no segment
  closes, because no checkpoint is resolved.
- **The timer is decided on the facade's first read**, as #191 left it.

## 11. What the review found

A fresh reviewer read the first build of this change with a log of every
command in the crate's tests, eleven probes and its own breaks. It took the
tables of section 4 again and found the same numbers, and confirmed the
nine breaks I had run. It found four commands that closed a segment from a
park without setting the run going, and three wrong implementations that
passed the whole crate.

- **An answer that schedules a compensation closed `run-resume`.** It left
  the run `Cancelling` at the same phase, waiting for the compensation.
  `has_resumed` excused every wind-down the command brought on, and its own
  comment justified that only for a decided end. Call 5 is the answer.
- **A gate's grant on a stale park closed `run-resume`, linked to the
  gate.** I had asked for a resolved checkpoint of any kind, and written
  that this differs from "the park's checkpoint" in no state a test can
  build. The forged record of my own test builds it. Call 3.
- **An answer given again against a run cancelled behind the cache closed
  one**, where the same answer through a facade that reads the record
  closed none. I had taken the second guard out as unreachable. Call 4.
- **My stale-park test held a replay, and not "a command that resolved
  nothing".** A build that asked whether the command applied passed every
  test. The test now runs three commands, two of which commit.
- **No test answered a park through the effect command**, and the replay
  half of the links test held nothing. One is added and the other removed.
- **Sentences that were too broad or stale**: three comments in the code
  and the tests, and the count of tests #191 wrote.

It also found two things I leave as they are, in section 10: the errored
pass and the lost record.
