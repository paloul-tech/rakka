# A dispatch refused before it starts is charged no attempt (issue #196)

2026-11-22. Short design. The owner ruled on the one open question before
the build, and on one the review raised (§7). The owner reviews at the pull
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

- Five on the other paths to an attempt: a refusal after a lost attempt, a
  policy conflict after a lost attempt, a reconcileable call refused on its
  retry after "not executed", and a later attempt refused under the same
  claim; and the grant test now asserts its code.
- Two on the idempotent park: it links attempt one's exported span, and an
  `AbandonAndFail` of it reports one attempt of three.
- One on the order: the worker dies after the missing resolver's `Started`
  and the row is `Dispatching`.
- Three on the lease (§7).
- `a_row_another_worker_cancelled_after_a_refusal_it_delivered_is_not_parked`
  (issue #195) built its finished row from a refusal delivered on a lapsed
  lease, which can no longer happen. It is now
  `a_refusal_on_a_lapsed_lease_leaves_the_retry_to_the_worker_that_holds_the_ticket`:
  worker one writes nothing, and worker two, under its current lease, makes
  the retry and the run completes. The success variant of the same race is
  unchanged, and its shared setup returns the race's outcome to both.

On `main`'s `dispatch.rs`, 12 of the 17 in the new file fail; the two after
`Started` pass, and the three on the lease fail too. On `496ea0db` the three
on the lease and the turned-around #195 test fail.

## 6. Not done

- After `credential-resolver-missing` on an idempotent retry, the park links
  the refused attempt, which invoked nothing and exported no span, not the
  one before it that may have executed (as on `main`; review probe G).
- `credential-resolver-missing` is not asked whether the lease is current
  (§7).
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
claim ends `OutOfLease`, writes nothing and is counted in
`AgentDispatchPass::lapsed`, and the claim that follows asks the authority
again. The cost, accepted: a refusing authority that always answers slower
than the lease never has its refusal delivered, and the run waits, spending
nothing. The docs say the lease has to cover the authority's answer. A worker
that stalls between the check and the delivery can still deliver.

The check is in a new `settle_refused_before_started`, which both routes
before `Started` take (the policy-conflict site and `settle_refusal`'s
definitive branch) and which also passes `attempt - 1`.

**My call, yours to reverse:** `credential-resolver-missing` is not checked.
It comes after `Started`, one write after the grant check, so its window is
that write; and a claim that ended out of lease there would leave its row
`Dispatching`, which the next claim recovers as a lost attempt: a
non-idempotent call would park as possibly executed though nothing ran.

Three tests hold the ruling, each failing on `496ea0db` (worker one's pass
ends `claim-fenced` after it has delivered):

- A refusal whose lease lapsed while another worker called the tool is not
  delivered: that worker's call succeeds and the run completes on it.
- A refusal whose lease lapsed after another worker failed the attempt is
  not delivered: the row keeps both failures and the run is not failed.
- A refusal whose lease lapsed is delivered by the claim that follows, when
  no other worker took the ticket.

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
| **the policy-conflict site skips the check** | **survives** |

The survivor is declared. The policy check makes no call of its own and
follows the claim's reads at once; the one slow step that can come before it
on a claim, a reconciler's answer, is already followed by issue #195's check
that the lease is current. So no test reaches that site on a lapsed lease
without hooking the stores, and the check there is kept so that every
refusal before `Started` follows one rule.
