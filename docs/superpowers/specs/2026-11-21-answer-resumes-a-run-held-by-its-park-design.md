# The answer that sets a parked run going closes its `run-resume` segment (issue #193)

2026-11-21. Short design, with my calls listed. The owner reviews at the pull
request. This is the second half of issue #191, and it is stacked on the fix
for the first half (PR #192).

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
   close it only for a command that resolved a checkpoint.**
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
or with nothing holding it (`AgentRun::has_resumed`). From a park it closes
only if the command also resolved a checkpoint.

The standing rule is untouched: a command that leaves the run on its next
wait closes none. An answer that takes a parked run from `RecordingTurn` to
its next model call is one.

**Measured over the crate's tests**, the tests of this change included. I
logged every command that starts a timer, with the verdict before this
change and after it.

| | Commands |
|---|---|
| Start a timer | 6,504 |
| From a wait of the phase | 6,483 |
| From a park that alone holds the run | 21 |
| From a wait of the phase, with a different verdict than before | 0 |

Of the 21 from a park:

| The command | Commands | Closes `run-resume` |
|---|---|---|
| An answer that leaves the run on its next model call or its tools | 16 | No |
| An answer that takes the run on to its proposal | 2 | Yes |
| An answer that takes the run on to its end | 1 | Yes |
| A replay whose settle pass mends a stale park | 1 | No |
| A cancellation | 1 | No |

The three commands that close one are in tests of this change, one of which
#191 wrote and this change amends. No earlier test world reaches the case.

## 5. The change

- `AgentRun::held_by_its_park_alone`.
- `AgentRun::loop_would_step`, which is the half of `can_advance` that is
  not the status, taken out so that both can ask it. `can_advance` answers
  as it did.
- `AgentRunEntityStore::apply_unboxed`: where the timer starts, and one line
  where the segment closes.

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
3. **The command must have resolved a checkpoint, of any kind.** Only the
   answer or the settle pass's mend takes a park away, so "a checkpoint" and
   "the park's checkpoint" differ in no state a test can build.
4. **The standing rule is kept.** Sixteen of the nineteen answers leave the
   run on its next wait and close none, as a tool result that leads to the
   next model call closes none.
5. **`can_advance` is split in two.** A pure extraction, so that the new
   predicate asks the same phases the crank does and cannot drift from it.
6. **No second guard on the wind-down where the segment closes.** A command
   refused on a stale cache is answered again against the record re-read,
   which may show a wind-down the cache did not. `has_resumed` already says
   no for that run unless it ended.

## 7. Mixed builds and rollback

Nothing is stored. An earlier build closes the segments it closed.

## 8. Tests

Seven whole-run tests in `run_end_waits.rs` and one unit test. One of the
seven is #191's, amended. Each whole-run test reads the segments the run
closed from an in-memory sink.

**The three that fail on the base** (PR #192 at `595a5048`), each at the
assertion it is named for:

- **A model result on a parked run closes no resume segment**, which #191
  wrote. Its second half held the cost of #191's first call: the answer
  closed none. It now holds that the answer closes the turn's one resume.
- **The resume an answer closes links the park and the request.** A traced
  run. The segment's two links name the span the park exported and the
  span of the answer's request. A replay of the answer closes none.
- **The answer that takes a parked run to its end closes a resume
  segment.** With the task in reach, the run proposes, is accepted and ends
  within the answer's pass.

**The four that hold what "alone" leaves out**, and pass on the base:

- **A park answered on a run that stands on its proposal closes no resume
  segment**, which #191 wrote.
- **The answer that ends a parked run that is winding down closes no resume
  segment.**
- **The answer to a park on a run that asks for a top-up closes no resume
  segment.** The answer's pass carries the top-up request, the task
  refuses, and the run ends: by the refusal, and not by the answer.
- **A stale park the settle pass mends closes no resume segment.** A record
  parked with nothing left to reconcile, as a build before issue #131 could
  leave one, at its evaluated model output. A replay runs the pass and the
  run goes on to its proposal. The record is built from this build's parked
  record by settling the evaluation and dropping its checkpoint. I did not
  check it against a record that build wrote.

`workflow_tool.rs` already holds, from #191, that an answer which takes a
parked run to its next model call closes none.

The unit test in `run.rs` holds `held_by_its_park_alone` over every phase
the loop steps from, a wait of the phase, every other status, a top-up
request, a wind-down and a pending proposal, and that `can_advance` answers
as it did for the same records.

## 9. Breaks

Nine breaks were run one at a time, each against the unit tests and five
test files, with a survivor to be run again against the whole crate. All
nine are caught, and none needed the whole crate.

- **The timer not started from a park, which is the defect**: two of the
  tests that fail on the base. The third was written after the breaks were
  run, and fails on the base for the same reason.
- **The command need not have resolved a checkpoint**: the stale park's
  test.
- **Every resume needing a resolved checkpoint, from a wait of the phase
  too**: seven tests, two of them older than #191.
- **A run that is winding down counted, one that asks for a top-up counted,
  one that stands on its proposal counted**: each by its own whole-run test
  and by the unit test.
- **The loop not asked at all**: two whole-run tests and the unit test.
- **An approval or an authorization wait counted as a park, and any status
  counted as one**: the unit test alone (call 1).

The whole crate passes: 1,896 tests.

## 10. Not done

- **An approval or an authorization wait at a phase that is no wait** (call
  1).
- **The outcome that ends a run found winding down at a phase that is no
  wait**, and a top-up's answer. #193 leaves both out, and says why.
- **The timer is decided on the facade's first read**, as #191 left it.
