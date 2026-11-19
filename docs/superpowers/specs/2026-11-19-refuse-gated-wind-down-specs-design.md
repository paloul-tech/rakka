# A gated compensation or workflow-cancel spec is refused (issue #128)

Design and record, 2026-11-19. Branch `refuse-gated-wind-down-specs`, off
`main` at `321f218`.

## 1. What was wrong

A deployment could give `AgentEffectPolicies` a compensation spec or a
workflow-cancel spec that requires a checkpoint or an authorization. The
run then recorded the effect under that spec and opened no gate, because
both are recorded once the run is already winding down, after the
transition in which a turn opens the gates of its calls. A gated effect
with no grant is not sent. The wind-down fence exempts both kinds, so the
effect stayed `Pending` and the run stayed `Cancelling`, with nothing that
could move it.

The issue was filed from reading. It is reproduced now, for both gates:

```
with_compensation_spec -> Ok
compensate -> Ok
run Cancelling; effects [ModelCall Succeeded, ToolCall Compensated, CompensationCall Pending]; waiting []
```

## 2. The ruling

The owner's, on 2026-11-19: refuse the gated spec.

The other course was to open the gate and say what it means. It has no
meaning that holds. The original effect is marked `Compensated` in the
transition that records the compensation, so a denial would leave it
compensated with no compensation run. A `Cancel` closes every waiting
approval and authorization, and would leave the effect waiting again. A
gated workflow cancel only delays stopping a child its parent has already
decided to stop. After #133 the run's end would close such a gate as well.

## 3. The change

`with_compensation_spec` and `with_workflow_cancel_spec` refuse a spec
whose `checkpoint_required` or `authorization_required` is set, with
`effect-policy-invalid`, the code every other unenforceable spec is refused
under. One private function holds the rule and its reason.

## 4. My calls

1. **Refused at the two setters, and nowhere else.** `AgentEffectPolicies`
   keeps its specs private and is not deserialized, so the setters are the
   only way either spec enters it. I added no check where the run records
   the effect: it could only fire for a configuration that can no longer be
   built.
2. **The existing code, `effect-policy-invalid`,** and no new one. The
   message names the kind and the reason.
3. **A record that already holds such an effect is not repaired.** No such
   record is known: the host sets neither spec. A repair would have to
   choose between sending a compensation its deployment meant to gate and
   cancelling one the run decided on. A `Cancel` leaves it as it was, which
   an existing test holds.
4. **Every other kind keeps its gate.** A tool, a memory promotion, a goal
   evaluation, an A2A send, a workflow start and a claim append each have a
   transition that opens theirs.

## 5. Tests

- `effect::tests::a_gated_compensation_or_workflow_cancel_spec_is_refused`:
  each gate and both together are refused for both kinds, under the stable
  code, with the kind named; a spec with no gate is accepted and stored; and
  every other kind still takes a gated spec. It fails on `main`.
- `a_cancel_leaves_a_gated_compensation_as_it_was` built its state by
  configuring a gated compensation, which is now refused. It rewrites the
  stored record as an earlier build left it. It still fails when the term
  it was written to hold is removed from the cancel's rule.

## 6. Not done

- #123: a compensation or a workflow cancel outstanding when a run ends.
  The two were filed to be decided together; this half needed no ruling
  from that one.
