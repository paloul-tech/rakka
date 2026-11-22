# A dispatch refused before it starts is charged no attempt (issue #196)

2026-11-22. Short design. The owner ruled on the one open question before
the build. The owner reviews at the pull request.

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
| `model-policy-conflict` | `attempt - 1` |
| `settle_refusal` (all three of its callers are before `Started`) | `attempt - 1` to `settle_undispatchable`; `attempt` to `defer_dispatch`, unchanged |
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

In `crates/rakka-agent/tests/dispatch_refused_before_started.rs`, each reading
the run's record of the call, its charge and its terminal reason:

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

## 6. Not done

- A refused park's `ambiguous-attempt` link now points at the attempt that
  may have executed (by reading: `indeterminate_transition_links` uses the
  recorded attempt); no test reads the link.
