# A dispatch refused before it starts is charged no attempt (issue #196)

2026-11-22. Short design. The owner ruled on the one open question before
the build, and on one the first review raised (§7). A second review (§8)
changed tests and sentences, not the code. The owner reviews at the pull
request.

## 1. What was wrong

The dispatcher refuses a dispatch before it writes `Started` in four places,
all in `attempt_invocation` (`crates/rakka-agent/src/dispatch.rs`):

| Refusal | Route |
|---|---|
| `model-policy-conflict`: a model call's policy is weaker than its adapter's declaration | `settle_undispatchable` |
| the dispatch authority's definitive refusal (`tool-revoked`, `credential-revoked`, and others) | `settle_refusal` |
| a grant that fails `validate_for` (`grant-expired`, and others) | `settle_refusal` |
| `guardrail-revision-mismatch` | `settle_refusal` |

Each handed on `attempt`, the attempt about to be made (the row's count plus
one), and `refuse_dispatch` told the run that number. The run records the
largest attempt it is told and charges that many effect attempts when the
effect resolves (`settle_effect`). So a call refused on its first claim was
recorded and charged one attempt, though nothing was invoked; the budget's
rule is that an attempt counts once it reaches a durable `Started`
(`budget.rs`). A refusal on a later claim was one over, and so was the park
`settle_undispatchable` makes for an idempotent call refused on its retry
(`possibly_executed`), which is charged at the park.

One refusal on the same route comes after `Started`:
`credential-resolver-missing`. That attempt started, and its number is right.

## 2. The owner's ruling (2026-11-22)

A refused dispatch reports the attempts that started, as the wind-down fence
has since issue #182. `EffectFailed.attempts` is public and goes from
`Some(1)` to `Some(0)` for a call refused on its first claim; rakka.host's
run evidence will show 0.

## 3. The change

`settle_undispatchable` and `refuse_dispatch` take `started`, the attempts
that started, in place of `attempt`, and each caller passes what it knows:

| Caller | Passes |
|---|---|
| `model-policy-conflict` | `attempt - 1`, through `settle_refused_before_started` (§7) |
| `settle_refusal` (all three of its callers are before `Started`) | `attempt - 1` through `settle_refused_before_started` when definitive; `attempt` to `defer_dispatch` when transient, unchanged |
| `credential-resolver-missing` | `attempt`, unchanged |

Before `Started`, `attempt - 1` is the row's count on every path that reaches
an attempt: a fresh claim, a lost attempt's recovery after its burn, the
retry after a counted failure (issue #195), and a later attempt under the same
claim after a burn.

Nothing in the run branches on a refusal's number. The run's one rule on a
reported attempt, a handoff send's `attempts == 1` "provably undelivered"
test, decides only what an `Exhausted` outcome does; a refusal is delivered
as `Failed`. The other readers are the budget charge, `EffectFailed.attempts`,
and a park's attempt attribute and `ambiguous-attempt` link. A park follows
a refusal only on an idempotent retry, where at least one attempt started, so
no link is derived for attempt 0.

## 4. Mixed builds and rollback

Nothing durable is added. The dispatcher that refuses decides; an earlier one
reports as before. A run already told keeps what it was charged.

## 5. Tests

In `crates/rakka-agent/tests/dispatch_refused_before_started.rs`, first
built, each reading the run's record of the call, its charge and its
terminal reason:

- A model call refused for a policy conflict is charged no attempt.
- A call whose grant fails its check is charged no attempt.
- A call its authority refuses is charged no attempt.
- A call refused for its guardrail revision is charged no attempt.
- A call refused on its retry is charged the attempt it made (one, where it
  was two): a row that counts one, so the test tells the row's count from
  zero.
- An idempotent call refused on its retry parks charged the attempt it made.
- A guard: a call refused after its attempt started is charged that attempt.

The first six fail on `main` by exactly one attempt; the guard passes there.

Five breaks of the fix were run one at a time against the new file,
`effect_dispatch.rs` and `tool_authority.rs`. All five are caught:

| Break | Caught by |
|---|---|
| the policy-conflict site reports the refused attempt | its test |
| `settle_refusal` reports the refused attempt | the grant, authority, guardrail, retry and idempotent-park tests |
| the `credential-resolver-missing` site reports one fewer | the guard |
| the park reports one more | the idempotent-park test |
| `refuse_dispatch` reports one more | every refusal test and the guard |

The whole `rakka-agent` suite passes with the fix (1,962 tests): no existing
test pinned the old number.

The fixture's doc comment for `with_credential_resolver` said the missing
resolver is refused "before any grant check"; it is refused after the grant
check and the `Started` write, which is what the guard holds. Corrected.

The review's answer (§7) adds ten more to that file, and turns one test in
`dispatch_failed_attempt_recovery.rs` around:

- Four on the other paths to an attempt: a refusal after a lost attempt, a
  policy conflict after a lost attempt, a reconcileable call refused on its
  retry after "not executed", and a later attempt refused under the same
  claim. The grant test now asserts its code as well.
- Two on the idempotent park: it links attempt one's exported span, and an
  `AbandonAndFail` of it reports one attempt of three.
- One on the order: the worker dies after the missing resolver's `Started`
  and the row is `Dispatching`.
- Three on the lease (§7).
- `a_row_another_worker_cancelled_after_a_refusal_it_delivered_is_not_parked`
  (issue #195) built its finished row from a refusal before `Started`
  delivered on a lapsed lease, which can no longer happen. (A
  `credential-resolver-missing` refusal still can, and the second review's
  answer holds that path, §8.) It is now
  `a_refusal_on_a_lapsed_lease_leaves_the_retry_to_the_worker_that_holds_the_ticket`:
  worker one writes nothing, and worker two, under its current lease, makes
  the retry and the run completes. The success variant of the same race is
  unchanged, and its shared setup returns the race's outcome to both.

With the second review's four (§8), the new file has 21. On `main`'s
`dispatch.rs` 17 fail and 4 pass: the two after `Started`, the
missing-resolver race (that site is unchanged) and the renewed lease (a
guard: `main` has no check to get wrong). On `496ea0db` five fail: the three
on the lease, the idempotent retry refused after the lease lapsed and the
policy conflict met after it; so does the turned-around #195 test.

## 6. Not done

- After `credential-resolver-missing` on an idempotent retry, the park links
  the refused attempt, which invoked nothing and exported no span, not the
  one before it that may have executed (as on `main`; review probe G).
- `credential-resolver-missing` is not asked whether the lease is current
  (§7, §8).
- A lost idempotent attempt, then a late refusal of the retry, then the
  run's cancellation, ends the call `Cancelled` with no reconciliation,
  though the lost attempt may have executed (second review, probe p7). The
  claim counts the lost attempt before it asks the authority, so its row is
  `Failed, 1` when it ends out of lease; the wind-down fence then cancels an
  idempotent `Failed` row as never started. A worker that crashes right
  after that count reaches the same end today, and no placement of the
  lease check prevents it: the root is the fence's reading of a failed
  idempotent row. A follow-up issue is drafted in
  `tmp/2026-11-22-issue-196-review-probes/`.
- A grant that comes after the lease lapsed still leads to an attempt
  beside another worker's (second review, probe p8). That is the older
  limitation in `docs/rakka-agents.md` ("a worker that stalls past its lease
  before its durable `Started` can still start one"); the owner's ruling is
  about refusals.
- A transient refusal on a lapsed lease still ends the pass `claim-fenced`
  with no report, as on `main`; no test holds it.
- A worker that stalls between the lease check and the delivery can still
  deliver a refusal beside another worker's attempt.
- A refusing authority that always answers slower than the lease never has
  its refusal delivered (§7, accepted).

## 7. The review, and the owner's second ruling

A fresh reviewer read the two commits and ran probes, kept in
`tmp/2026-11-22-issue-196-review-probes/`. It confirmed the six-fail,
one-pass split against `main` and found:

| # | Finding | Answer |
|---|---|---|
| F1 | A refusal whose authority answered after the lease lapsed is delivered beside another worker's attempt. Probe H: that worker made and failed the retry meanwhile, and the run was charged one of the two attempts that started (`main`: two, by accident). | The owner's ruling below. |
| F2 | The new rustdoc said a park's `ambiguous-attempt` link is to the attempt that may have executed. After `credential-resolver-missing` the link is to the refused attempt, which invoked nothing (as on `main`). | Sentence corrected; the link itself is listed under "Not done". |
| F3 | The policy-conflict site's `attempt - 1` could be `0` and every test still passed; the lost-attempt, counted-failure and same-claim paths had no test. | Four tests ported from the probes. |
| F4 | No test read a park's link, or what an abandonment of that park reports. | Two tests. |
| F5 | The grant test did not assert its code. | It asserts `grant-expired`. |
| F6 | `RecordEffectResult.attempt` said "the dispatch attempt that produced the result". | Rewritten: the attempts the result accounts for, the largest recorded, charged at the first resolution or park. |
| F7 | The product doc listed "the dispatch authority's refusal"; a transient one reports nothing. | "definitive refusal". |
| F8 | The fixture comment that the missing resolver is refused after `Started` was held by nothing. | A test kills the worker after `Started` and finds the row `Dispatching`. |

Probing F1 further showed the worse form, older than this change (probe K):
the other worker's call of a non-idempotent tool was out when the stale
refusal came. The run failed `tool-revoked`, the row was cancelled under that
call, and the call's success was lost. `main` does the same and reports
`Some(1)`; this branch reported `Some(0)`. The fleet does fence the stale
worker, but only at the ticket cancel, after the delivery.

**The owner's ruling (2026-11-22): "Lease still current".** A definitive
refusal before `Started` is delivered, or parked, only while the claim's
lease is still current (`is_current_claim_for(claim, 0)`), as issue #195
checks before the first attempt after the reconciler's answer. Otherwise the
claim ends `OutOfLease`, writes nothing more and is counted in
`AgentDispatchPass::lapsed`, and the ticket is left to the claim that
follows. The cost, accepted: a refusing authority that always answers slower
than the lease never has its refusal delivered, and the run waits, spending
nothing. The docs say the lease has to cover the authority's answer. A worker
that stalls between the check and the delivery can still deliver.

The check is in a new `settle_refused_before_started`, which both routes
before `Started` take (the policy-conflict site and `settle_refusal`'s
definitive branch) and which also passes `attempt - 1`.

**My call, yours to reverse:** `credential-resolver-missing` is not checked.
A claim that ended out of lease there would leave its row `Dispatching`,
which the next claim recovers as a lost attempt: a non-idempotent call would
park as possibly executed though nothing ran. The window it leaves open is
not one write, as this paragraph first said (§8): it runs from the
authority's grant, which can itself come after the lease, to the refusal.

Three tests hold the ruling, each failing on `496ea0db` (worker one's pass
ends `claim-fenced` after it has delivered):

- A refusal whose lease lapsed while another worker called the tool is not
  delivered: that worker's call succeeds and the run completes on it.
- A refusal whose lease lapsed after another worker failed the attempt is
  not delivered: the row keeps both failures and the run is not failed.
- A late refusal leaves the call to the claim that follows, when no other
  worker took the ticket.

Eight breaks of the answer were run one at a time against the new file,
`dispatch_failed_attempt_recovery.rs`, `effect_dispatch.rs` and
`tool_authority.rs` (the after-`Started` one by hand). Seven are caught:

| Break | Caught by |
|---|---|
| no lease check | the three lease tests and the turned-around #195 test |
| `settle_refused_before_started` reports the refused attempt | every refusal before `Started` |
| the policy-conflict site subtracts twice | the policy conflict after a lost attempt |
| the authority's refusals skip the check | the three lease tests and the #195 test |
| the park reports one more | the three park tests |
| `refuse_dispatch` reports one more | every refusal test and the guard |
| the `credential-resolver-missing` site reports one fewer | the guard |
| **the policy-conflict site skips the check** | **survived** this round |

The survivor was declared here as unreachable without hooking the stores.
The second review showed the testkit already has that hook, and a test now
kills it (§8).

## 8. The second review

A second fresh reviewer read `045bac0f` and ran probes (kept with the first
review's). It confirmed that every refusal before `Started` goes through
`settle_refused_before_started` and nothing else does, and the split against
`main` and `496ea0db`. Its findings:

| # | Finding | Answer |
|---|---|---|
| F-G | A lost idempotent attempt, a late refusal and a cancellation end the call with no reconciliation (probe p7). | "Not done" (§6), with a drafted follow-up: the root is older than this change. |
| F-A | §7 said the missing resolver's unchecked window is one write; it runs from a late grant (probes p1, p1b: through that site a call another worker had out is lost when the workers differ in whether a resolver is wired). | §7 corrected; the call stays the author's, for the owner. |
| F-C | No test held "or parked": checking only non-idempotent refusals passed every test. | A test: an idempotent retry refused after the lease lapsed parks nothing. |
| F-D | The declared survivor can be reached with `CrashingStateStore::before_next_load`. | A test: a policy conflict met after the lease lapsed is not delivered. |
| F-E | The removed #195 variant's path is still reachable through `credential-resolver-missing`, and no test held it; §5 said it "can no longer happen". | A test, which also holds the author's call: checking that site fails it. §5 corrected. |
| F-J | Nothing held that the check asks the renewed lease, not the expiry the claim was handed. | A test with two calls in one batch. |
| F-B | "Writes nothing": a claim that counted a lost attempt has written the row. | "Writes nothing more (a lost attempt it counted first stays counted)" in the product and compatibility docs and the rustdoc. |
| F-K | "The claim that follows asks the authority again": not under a wind-down, and not for a policy conflict. | "The ticket is left to the claim that follows", everywhere. |
| F-F | The `lapsed` rustdoc named only the authority's refusal. | It names the four refusals. |
| F-I | "Which the row counted when the claim read it": on a lost attempt's path the claim read `Dispatching, 0`. | "Which the row counts by then". |
| F-H | "A transient refusal is not asked" and the author's call are held by no test. | The resolver test holds the call; the transient case is under "Not done". |
| nits | "Five" listed four; the CHANGELOG omitted "or parked"; a test's name said the late refusal is delivered. | Corrected; the test is `a_late_refusal_leaves_the_call_to_the_claim_that_follows`. |

Each of the four new tests was checked against the break that showed its
gap, run one at a time: parks skipping the check, the policy site skipping
it, the missing-resolver site checked, and the check asking the claim's
handed expiry. Each is caught by its test alone.

The reviewer also offered an option for the owner: a lease check right after
the authority answers, grants included. It would close late grants (probe
p8) and the missing-resolver window along with refusals, and leave no
`Dispatching` row behind; it would also stop a granting authority slower
than the lease from ever making an attempt. Not built.
