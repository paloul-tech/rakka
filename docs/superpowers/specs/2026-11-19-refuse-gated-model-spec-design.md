# A gated model spec is refused (issue #176)

Design and record, 2026-11-19. Branch `refuse-gated-model-spec`, stacked on
`refuse-gated-wind-down-specs` (pull request #175, issue #128), which is
off `main` at `321f218`.

## 1. What was wrong

`AgentEffectPolicies::with_model_spec` accepted a spec that requires a
checkpoint or an authorization. The run recorded a turn's model call
under it and opened no gate. A gated effect with no grant is not sent, so
the call stayed `Pending` and the run rested `WaitingForEffect` on it, with
no checkpoint for anyone to answer.

Ran on `main` at `321f218`, for both gates:

```
with_model_spec -> Ok
run WaitingForEffect; effects [(ModelCall, Pending)]; waiting 0     (three passes, unchanged)
```

Nothing else opens the gate. A turn opens the gates of its calls when it
commits them, and that is after its model call has answered. No command
opens a gate for a model call. Unlike the workflow cancel of #128, I found
no arrangement in which this gate opens.

It was found reviewing #128's fix, whose record had said every other
kind's gate is opened by something.

## 2. Approaches

- **Refuse the gated spec where it is set.** Built. It is what #128's fix
  does for a compensation and a workflow cancel, by the owner's ruling
  there, and the owner asked for this fix after I recommended the same
  course here.
- **Open a gate when the model call is recorded.** Not built. It needs
  answers nobody has asked for: what a denial, an expiry and a cancel do to
  the turn, and whether the gate is asked once for the run or every turn.
  The refusal does not rule it out.

## 3. The change

`with_model_spec` refuses a spec whose `checkpoint_required` or
`authorization_required` is set, with `effect-policy-invalid`, through the
function #128's fix added for the other two setters. That function's doc
now covers all three.

## 4. My calls

1. **Refused, not gated** (section 2). The issue leaves the choice open.
2. **At the setter, and nowhere else.** `with_model_spec` is the only
   assignment of the model spec; the default is `AgentEffectSpec::read_only()`
   and `AgentEffectSpec::for_model_policy` sets no gate. The run takes the
   spec with no overlay.
3. **The existing code, `effect-policy-invalid`.**
4. **A run that already rests on such a call is not repaired.** Its effect
   carries the gate it was recorded with. A `Cancel` cancels the call and
   ends the run `Cancelled`, which ran. No such run is known: the host's
   model spec has no gate.
5. **Stacked on #175,** because it calls the function that change adds.
   Off `main` it would add the same function a second time.

## 5. Tests

`effect::tests::a_gated_model_spec_is_refused`: each gate and both together
are refused under the stable code, with the kind named; a spec built by
`for_model_policy` is accepted and stored; and the default model spec has
no gate. It fails without the change.

Nothing in the workspace set a gated model spec: two test fixtures and one
example call the setter, each with a spec that has no gate.

## 6. Not done

- A gate on a model call that works (section 2).
