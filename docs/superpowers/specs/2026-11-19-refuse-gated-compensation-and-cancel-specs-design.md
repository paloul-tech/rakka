# A gated compensation or workflow-cancel spec is refused (issue #128)

Design and record, 2026-11-19. Branch `refuse-gated-wind-down-specs`, off
`main` at `321f218`.

## 1. What was wrong

A deployment could give `AgentEffectPolicies` a compensation spec or a
workflow-cancel spec that requires a checkpoint or an authorization. The
run recorded the effect under that spec, and opened no gate in the
transition that recorded it. A gated effect with no grant is not sent, and
a wind-down fences neither kind.

The issue was filed from reading. What each case did, and how I know:

| Case | What happened | Evidence |
|---|---|---|
| A compensation, either gate | `Pending` for good; the run `Cancelling` for good; the original effect already `Compensated` | ran on `main` |
| A workflow cancel recorded by a wind-down | `Pending`, no gate, never sent; the run `Cancelling` | ran, with the refusal removed. That the run waits on the cancel, and not only on its child, is by reading |
| A workflow cancel chasing a fan-in straggler, and the turn then proposes | `Pending` for good; the run went on and ended `Completed` with it so; the straggler not told to stop | ran, with the refusal removed |
| The same, and the turn then commits tool calls | the turn opened an approval for the cancel with its own; the run waited `WaitingForApproval` | ran, with the refusal removed |

In the last case an approval released the cancel, a denial failed it and
wound the run down, and a run cancel closed the gate and left the cancel
`Pending`. All three ran.

The issue said nothing opens the gate, on the ground that both effects are
recorded once a run is winding down. That is true of a compensation. It is
not true of a workflow cancel: a run that goes on records one for a fan-in
straggler, in the turn it is then on, and the gate block of that turn
selects by turn number.

## 2. The ruling

The owner's, on 2026-11-19: refuse the gated spec. It was made on the
issue as filed, before the last two rows above were known. The refusal
takes away the one arrangement in which a gate opened, and the pull
request says so.

The other course was to open the gate and say what it means. It has no
meaning that holds. The original effect is marked `Compensated` in the
transition that records the compensation, so a denial would leave it
compensated with no compensation run. A `Cancel` closes every waiting
approval and authorization, and would leave the effect waiting again. A
gated workflow cancel only delays stopping a child its parent has already
decided to stop. The one arrangement that did open a gate bears this out:
a denial wound the whole run down, and a cancel left the effect stuck.

## 3. The change

`with_compensation_spec` and `with_workflow_cancel_spec` refuse a spec
whose `checkpoint_required` or `authorization_required` is set, with
`effect-policy-invalid`, the code every other unenforceable spec is refused
under. One private function holds the rule and its reason.

## 4. My calls

1. **Refused at the two setters, and nowhere else.** `AgentEffectPolicies`
   keeps its specs private and is not deserialized, and the run takes both
   specs from it with no overlay. The setters are the only way either spec
   enters. I added no check where the run records the effect: it could only
   fire for a configuration that can no longer be built.
2. **The existing code, `effect-policy-invalid`,** and no new one. The
   message names the kind and the reason.
3. **A record that already holds such an effect is not repaired.** No such
   record is known: the host sets neither spec. A repair would have to
   choose between sending a compensation its deployment meant to gate and
   cancelling one the run decided on. A `Cancel` leaves the effect as it
   was.
4. **No other kind's spec is refused.** A gated model spec has the same
   defect (section 7), and it is not this issue's.

## 5. Tests

- `effect::tests::a_gated_compensation_or_workflow_cancel_spec_is_refused`:
  each gate and both together are refused for both kinds, under the stable
  code, with the kind named; a spec with no gate is accepted and stored; and
  a tool, a memory promotion, a goal evaluation, an A2A send, a workflow
  start and a claim append still take a gated spec. It fails on `main`.
- `a_cancel_leaves_a_gated_compensation_as_it_was` built its state by
  configuring a gated compensation, which is now refused. It rewrites the
  run record instead: the compensation `Pending`, gated, and never handed
  to the sink. The ticket this build had already written for it is left in
  the sink; a cancel does not read it. The test still fails when the term
  it was written to hold is removed from the cancel's rule.

## 6. The review

A fresh reviewer read the commit. The refusal held: it found no other way
to record either effect gated, and nothing in the workspace that sets
either spec gated. What it corrected was the reasons:

- **"Both are recorded by a run that is winding down" was false** of a
  workflow cancel, and an existing test asserts the state that shows it.
  I then ran the straggler cases above, which the first write-up did not
  have.
- **The rewritten record was not one an earlier build wrote.** It was
  `Pending` with a `dispatched_at`. The helper clears it now, and its doc
  says what is and is not rewritten.
- **"Every other kind's gate is opened by something" left out the model
  call.** Section 7.

## 7. Found, not done

- **A gated model spec is accepted, and its call is never sent.**
  `with_model_spec` takes a spec that requires a checkpoint or an
  authorization. The model call is recorded `Pending` with no gate, and the
  run rests `WaitingForEffect` on it, for good. Ran, both gates. The host's
  model spec is not gated. Drafted in `tmp/`, not filed.
- #123: a compensation or a workflow cancel outstanding when a run ends.
  The two were filed to be decided together; this half needed no ruling
  from that one.
