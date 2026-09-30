# Coordination Capability Acceptance (M5)

One durable coordination story, end to end: the coordination-capability
milestone of `docs/plans/rakka-agent/spec.md` (section 22), demonstrated with
deterministic model adapters over real `ClusterSharding` — all five agent
entity types — the in-process `rakka-a2a` service core, and the production
effect dispatcher. Everything is in-memory; no external services.

The walk: one `AgentTaskId` is posted to a team board, atomically claimed by
the member whose definition authorizes it, handed off to a specialist, gated
by a human-owned approval upstream and by the checkpoint that parks the
consequential refund, reviewed in a moderated conversation, and finally
closed — surviving an owner death inside the handoff and inside the
conversation, and replayable from a cursor afterwards.

Two beats are the milestone's envelope bullet, and they are refusals: a board
member and a conversation participant, each admitted by the roster and each
refused because its *definition* never granted the coordination capability the
operation spends. Board membership and a participant roster are trusted
application wiring, never authority sources of their own.

Delegation and workflow tools are deliberately not walk beats: the M4
milestone proves them in `examples/multi-agent-goal-acceptance`, and this walk
covers the coordination capabilities that milestone did not.

Run it:

```sh
cargo run -p rakka-example-coordination-capability-acceptance
```

`tests/acceptance.rs` runs the same flow and asserts this transcript
verbatim, plus the typed facts behind every line.

## Expected stdout

```text
ok  1/16 five sharded entity types over real ClusterSharding: four agents, one team, and two board tasks posted deliberately unassigned
ok  2/16 two members claimed concurrently through rakka-a2a: one owner admitted at generation 1, the loser's stale-epoch command failed closed
ok  3/16 TEAM ENVELOPE: a member whose definition never granted Team was refused team-coordination-unauthorized and its board entry reopened
ok  4/16 the board waited with nothing resident: 0 resident entities, and the claim activated across the passivation
ok  5/16 the owner's model turn transferred the SAME AgentTaskId: the handoff record and its A2aSendCall committed in one compare-and-set, fencing the source
ok  6/16 HandedOff recorded strictly after the target's durable acceptance: one task id, one new generation, and no session or private memory travelled
ok  7/16 HANDOFF POD LOSS: the task store died mid-transfer; recovery re-derived the HandoffResult and converged on one transfer
ok  8/16 a human-owned approval was declared upstream of the ticket: the dependency edge registered with the upstream in the declaring transition, and the dependent's decision graph read unsatisfied
ok  9/16 an authenticated human result completed the upstream through rakka-a2a and unblocked the dependent; a replayed submission echoed the original
ok 10/16 CHECKPOINT BOUNDARY: the consequential effect still parked on a bound AgentCheckpoint — the human result resolved no checkpoint and invoked nothing
ok 11/16 the moderated conversation ran its bounded rounds in order; the dense turn ledger absorbed a replayed turn without recording a second
ok 12/16 MODERATION ENVELOPE: a rostered speaker whose definition never granted Moderation was refused conversation-moderation-unauthorized
ok 13/16 CONVERSATION POD LOSS: the conversation store died mid-round; participant, round, turn owner, transcript, and budgets recovered without duplicating a turn
ok 14/16 the conversation's terminal reached the governing task, and the terminal task closed its board entry with the claim epoch bumped
ok 15/16 the coordination log replayed from a cursor across task, team, and conversation with no gap or repeat; a truncated window answered WindowExpired with a floor that resumed
ok 16/16 no content crossed onto a coordination surface: planted sentinels appear in no board, no replay page, and no metric
```
