# A claim checks its lease right after the dispatch authority answers (issue #207)

2026-11-23. Short design. The owner chose to cover grants ("Take #207,
covering grants too") and approved this design. The owner reviews at the
pull request.

## 1. What is wrong

Issue #196 checks the claim's lease before a definitive refusal made before
`Started` is delivered or parked (`settle_refused_before_started` in
`crates/rakka-agent/src/dispatch.rs`). Two other outcomes of an authority
answer that comes after the lease lapsed are not checked.

1. **A late grant makes an attempt beside another worker.** Meanwhile a
   second worker has claimed the ticket and called the tool. The first then
   writes `Started` and calls it too, even a non-idempotent tool. Measured by
   the #196 review's probe p8: two tool calls, and the first worker's pass
   ends `claim-fenced`. `docs/rakka-agents.md` states this as a limitation.
2. **A late grant reaches `credential-resolver-missing`.** That refusal
   comes after `Started`, so #196 does not check it. Where the workers differ
   in whether a resolver is wired, the run is failed while another worker's
   call is out, and that call's success is lost.

A late transient refusal also ends the pass in a `claim-fenced` error, with no
report: the deferral's fleet write is fenced.

## 2. The owner's ruling (2026-11-23)

**Cover grants.** The accepted cost: a granting authority that always answers
slower than the lease never makes an attempt, and the run waits, spending
nothing. The lease has to cover the authority's answer for every dispatch.

## 3. The change

`authorize_attempt` checks that the claim's lease is still current right
after the authority answers. It closes the `tool-authorize` segment as it does
today, then asks `is_current_claim_for(claim, 0)`. If the lease has lapsed,
it answers "out of lease" instead of the decision.

Both callers end the claim as one out of lease does (`ClaimConclusion::OutOfLease`,
counted in `AgentDispatchPass::lapsed`), writing nothing more:

| Caller | Before | After, on a lapsed lease |
|---|---|---|
| `attempt_invocation` (the attempt's own question) | a grant goes on to `Started` and the call; a definitive refusal ends out of lease (#196); a transient one ends the pass `claim-fenced` | every answer ends out of lease, before anything is written |
| `defer_before_reconciling` (#195's question before the reconciler) | a grant or a definitive refusal goes on to the reconciler | ends out of lease; the reconciler is not asked |

## 4. Calls made in the design (each the owner's to reverse)

1. **"Still current", not "covers the call".** The same reason the owner
   ruled for #195: a cover check would never make an attempt whose answer and
   call together outlast the lease.
2. **The check is inside `authorize_attempt`,** so both callers get it.
3. **An error from the authority keeps its handling.** The check applies to
   answers: an error still fails the attempt's pass, and still lets #195's
   question go on to the reconciler.
4. **`settle_refused_before_started` keeps its check.** The policy-conflict
   refusal is decided before any authority is asked and still needs it. For
   the authority's refusals it becomes a second read with no write or call
   between the two.
5. **What stays open:** a worker that stalls between this check and its
   `Started` write can still start an attempt beside another worker. Only
   that write lies between them. The docs' limitation is narrowed to say so.

## 5. What a deployment sees

- A grant that comes after the claim's lease lapsed makes no attempt. The
  claim ends out of lease, is counted in `AgentDispatchPass::lapsed`, and the
  ticket is left to the claim that follows. A non-idempotent tool is no
  longer called twice through this window.
- A transient refusal that comes after the lease ends the claim out of lease
  in the same way. `pump_run` returns a report where it returned a
  `claim-fenced` error.
- A late answer to the question #195 asks before the reconciler ends the
  claim before the reconciler is asked.
- Nothing durable is added. No status, error code or decision kind is added.

## 6. Mixed builds and rollback

Each claim is decided by the worker that holds it. An earlier-build worker
behaves as before. Rolled back, nothing is left to undo.

## 7. Tests

In `crates/rakka-agent/tests/dispatch_refused_before_started.rs`, beside
#196's lease tests, whose helpers they share:

- A late grant while another worker's call of a non-idempotent tool is out:
  one tool call, the first worker ends out of lease, and the run completes on
  the second worker's call (probe p8, turned into a test).
- A late grant with no other worker: no attempt, and the next claim makes it.
- A late transient refusal: the pass returns a report with one lapsed claim,
  not an error.
- A late answer to #195's question: the reconciler is asked nothing.
- A late grant on the retry of a counted lost attempt (idempotent): the
  loss stays counted, no retry is made, and the next claim makes it once.
- #196's resolver-race test turns around. The late grant ends worker one
  out of lease before `Started`, worker two makes the retry, and the run is
  not failed.
- Guard, unchanged: a refusal inside a renewed lease is still delivered.

Each new test fails on `main` except the guard. The breaks to run: the check
removed; the check made for a grant only; the `defer_before_reconciling` arm
ignoring "out of lease".

## 8. Documents

- `docs/rakka-agents.md`: the #196 claim rule widened to every answer; the
  stall limitation narrowed.
- A `docs/rakka-compatibility.md` entry.
- `CHANGELOG.md`.
