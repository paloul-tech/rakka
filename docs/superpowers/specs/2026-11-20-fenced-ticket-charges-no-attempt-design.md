# A call the dispatcher cancels before it starts is charged no attempt (issue #182)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request. Stacked on the fixes for issues #179 and #181.

## 1. What was wrong

When a run is winding down, the dispatcher's fence cancels a ticket whose next
attempt has not started, and tells the run `Cancelled`. It told the run with
the number of the attempt its claim was about to make: the row's count of
attempts, plus one. The run keeps the largest attempt number it is told
(`record_attempt`) and, when the call resolves, charges that many attempts and
gives back the rest of the reservation.

So a call that was sent, never claimed and then cancelled was recorded with
one attempt made and charged one effect attempt, though its executor was never
invoked. The run's consumption goes to its task with its settlement, and an
epoch's to its goal.

The comment where the run settles attempts already says what was meant: "A
generation that never dispatched (one the dispatch layer cancelled) reports
zero attempts and so consumes none".

## 2. Approaches

1. **The dispatcher reports the attempts that were made.** The fence passes
   the row's own count.
2. **The run does not charge a `Cancelled` outcome's attempt.**

I chose the first. The run cannot tell from `Cancelled` alone whether the
attempt it is told of started: the dispatcher also cancels a call after an
attempt that did start and was lost (a read-only call on a run that is
winding down, or one a reconciler found not executed), and that attempt is
charged on purpose. The dispatcher knows which case it is in. The fix is one
number at the place that knows.

## 3. The change

- **The fence** (`dispatch_claimed`, the branch for a ticket that provably
  never started) delivers `Cancelled` with the row's count of attempts, not
  the claim's next attempt.
- **The replay of that fence** (`owed_by_finished_row`, for a row a lost
  worker left `Cancelled` under a fence) reports the same count, so a run
  told by the replay records what a run told by the fence does.

The two cancellations that follow an attempt that started keep the claim's
attempt number. Nothing in the run changes.

## 4. My calls

1. **Fixed in the dispatcher** (section 2). Yours to reverse.
2. **The comment was right and the number was wrong.** The issue left open
   which. The run's budget rule, that every attempt which reached a durable
   `Started` is charged and no other, is stated in the same comment, and a
   ticket the fence cancels never reached `Started`.
3. **The replay takes the fence's count.** A row left `Cancelled` does not
   say whether its last attempt started. Most such rows are the plain
   fence's. The cost: a ticket cancelled after an attempt that started and
   was lost, whose second worker was then lost too before it told the run,
   is told with one attempt fewer than started.
4. **Records are not repaired.** A run that was already told keeps the
   attempt it was charged.

## 5. Mixed builds and rollback

Nothing durable is added. A dispatcher of an earlier build reports the larger
number, and a run of either build charges what it is told.

## 6. Tests

`a_call_the_fence_cancels_before_it_starts_is_charged_no_attempt` in
`crates/rakka-agent/tests/effect_dispatch.rs`, with the real dispatcher: a
tool call sent and never claimed, the run cancelled, one dispatcher pass. The
call ends `Cancelled` with no attempt made, the run's spent effect attempts do
not move, the reservation is given back, and the tool is never invoked. It
fails on the commit below this one, with one attempt made and one more spent.

## 7. Not done

- Records already charged (call 4).
- The double fault of call 3.
