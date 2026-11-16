# A plain task's handoff is narrowed by the catalog

Issue #142. A follow-up the owner asked for when accepting PR #141 (issue #69).

Branch `plain-task-handoff-narrowing`, off `main` at `e175ba1`.

## 1. Intent

A handoff passes a task to another agent, and the catalog states what communal access that agent gets with it: which knowledge spaces, and which read capabilities. For a task that carries a delegation envelope, that statement is the target's grant. A plain task (no goal, no delegation, no delegation ceilings on its definition) carries no envelope, so its handoff narrows nothing, and the target reads and appends under its own agent's whole grant whatever the catalog said.

After this change the catalog's statement is the target's grant for a plain task too, for reads and appends together. The rule is then the same at every door: a handoff or a delegation passes on what the catalog states and no more.

## 2. Goals, non-goals, anti-goals

**Goals**

- A handoff of a plain task gives the target exactly the knowledge spaces and read capabilities the catalog states for it.
- Nothing else about the target's run changes: no skill, tool, workflow or environment narrowing, no ceiling, no depth, no deadline.
- No run changes its grant mid-flight.
- A compatibility entry and a roll note, because some targets lose communal access.

**Non-goals**

- Chained handoffs. A second handoff's grant is the catalog's statement for the second target; it is not intersected with the first handoff's. That is how an enveloped root behaves today and it is not changed.
- Intersecting a root's statement with the handing agent's own definition. A root passes on what the catalog states (`delegated_grant`), as today.
- A wiring switch that keeps the earlier behaviour.
- #140.

**Anti-goals**

- A handoff that widens: a target's grant outside its own agent's definition and setup.
- Closing reads without appends, or appends without reads.
- A second copy of the grant rule. `delegation::delegated_grant` stays the only one.
- A new field on the assignment, the run or the handoff record.

## 3. Approaches

**How it lands**

| Approach | Trade-off |
|---|---|
| **Flip it outright** (chosen by the owner, 2026-11-16) | One rule at every door. A target whose catalog entry states no spaces loses all communal access on a plain task's handoff, and a deployment must state them before it upgrades. |
| Flip it, with a wiring switch that keeps the old behaviour for a release | A softer landing. One more setting, and the gap stays open wherever the switch is left on. |
| Narrow only when the catalog states something | No deployment loses access by saying nothing. "Empty" would then mean "nothing" at every other door and "everything" at this one, and a catalog could not delegate nothing on a plain task. |

**Where the grant is applied** *(one sensible way)*

The handoff step in `delegation_envelope_for` already applies the shared rule to an envelope. It skips a task that has none. Giving such a task an empty root envelope at that step makes the existing code apply, unchanged. The alternative, carrying the handoff's grant to the run beside the envelope, needs a new assignment field (a wire change) and a second reading of it at each of the three doors that read the envelope's grant today.

## 4. Design

In `delegation_envelope_for` (`crates/rakka-agent/src/task.rs`): when the task holds a handoff that is not `Refused`, and the base envelope is absent, the step starts from `AgentRunDelegationEnvelope::default()`. The rest of the step is as it is. With no lineage and no statement of its own, the shared rule answers the catalog's statement for both sets, so the envelope carries `knowledge_spaces: Some(handoff.knowledge_spaces)` and `read_capabilities: Some(handoff.read_capabilities)`.

**Why the default envelope narrows nothing else.** Every reader of the envelope, and what it does with a default one:

| Reader | Field | With the default |
|---|---|---|
| Delegation door, handoff door (`run.rs`) | `allowed_skills` | Empty: no narrowing |
| Tool planning (`run.rs`) | `allowed_tools` | Empty: no narrowing |
| Workflow door (`run.rs`) | `allowed_workflows` | Empty: no narrowing |
| Delegation door | `budget`, `depth` | No ceilings; depth 0, which is what an absent envelope reads as |
| Delegation door | `lineage`, `ancestors` | Empty: a root, no cycle history |
| Delegation and workflow doors | `deadline` | None |
| Fan-in | `fan_in` | None: the wiring's default |
| Dispatch authority (`tools.rs`) | `environments` | Empty: no narrowing |
| Claim-append door (`run.rs`), dispatch authority (`tools.rs`) | `knowledge_spaces` | The catalog's statement: **the change** |
| Communal read grant (`communal.rs`) | `knowledge_spaces`, `read_capabilities` | The catalog's statement: **the change** |
| Delegation door | `knowledge_spaces`, `read_capabilities` | **The change, passed on:** what the target delegates further is the next catalog statement intersected with this grant |

**What a deployment sees**

- A target whose catalog entry states spaces X, and whose agent is granted X, Y and Z: it reads and appends in X.
- A target whose catalog entry states no spaces: no communal access. This is the common case for a catalog written before anyone thought about handoffs and claims.
- An append outside the grant is refused, with the codes that exist: `run-claim-space-not-delegated` at the run's command door, `claim-space-not-delegated` at dispatch.
- A read outside the grant is not an error: the space is not searched, and a claim that needs a capability the run was not passed is not admitted.
- A delegation the target makes passes on at most what the target was passed.
- The target's run record holds an envelope where it held none.

**When it takes effect**

- The envelope is computed when the task entity makes an assignment, and a run keeps the envelope it accepted. So a run already accepted is unchanged for its life.
- A handoff recorded on the earlier build and assigned on the new one is narrowed by what its record holds. A record written before issue #69 holds no read capabilities, so that target reads only claims that require none.
- During a roll, whether a handoff is narrowed depends on which build hosts the task entity when it assigns.
- A refused handoff restores the source, and the source's later assignments carry no envelope, as now.

## 5. Compatibility

- **Behaviour change, and it can remove access.** On a plain task's handoff, the target's communal reads and appends are bounded by the catalog's statement where they were bounded by its own agent's grant.
- **Roll note.** Before upgrading, state on every catalog target a plain task can be handed to the knowledge spaces and read capabilities it needs (`with_knowledge_space`, `with_read_capability`). The earlier build ignores them for a plain task, so stating them first is safe.
- **Nothing durable changes shape.** The assignment already carries an optional envelope, and both sets exist on it. An earlier build reads the new envelope and enforces it. A rollback is clean.
- No new error code, field, schema version or metric.

## 6. Proofs

1. A plain task's handoff gives the target's run an envelope that is the default envelope plus the catalog's two statements. This is the pinning test, inverted and renamed.
2. That envelope narrows the target's read grant to the catalog's statement.
3. An append by the target inside the catalog's spaces is accepted; one outside is refused `run-claim-space-not-delegated`.
4. A catalog target that states nothing gives the target no communal space and no read capability.
5. A refused handoff leaves the source's next assignment with no envelope.
6. A plain task that was never handed off still carries no envelope.
7. The tests of an enveloped task's handoff pass unchanged (`a_root_handoff_grants_the_targets_stated_read_capabilities`, `a_delegated_tasks_handoff_intersects_the_targets_statement_with_its_grant`).
8. The target of a plain task's handoff can still call a tool and delegate to any catalog skill with no ceiling applied: the envelope narrows nothing else.

## 7. Documents

- `docs/rakka-agent-security-validation-matrix.md`: the "Owed" item goes; the enforced row for handoff names the new proof.
- `docs/rakka-compatibility.md`: the handoff exception in the issue #69 entry is corrected, and a new entry carries the behaviour change and the roll note.
- `docs/rakka-agents.md`: the handoff wording loses its exception for a plain task.
- The comment in `delegation_envelope_for`, and `CHANGELOG.md` (Changed, Migration notes).

## 8. Left to the builder

- Test names, and whether proof 3 and proof 8 extend `handoff_record.rs` or an existing end-to-end file.
- How proof 8 is shown with the fixtures that exist.
