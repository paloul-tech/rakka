//! Coordinator limits: depth, fan-out, descendants, concurrency, budget, and
//! cycle ceilings fail closed and are recoverable after coordinator loss
//! ([specification 8.4 and 9.7](../../docs/plans/rakka-agent/spec.md),
//! scenario 34).
//!
//! Every ceiling is enforced at the delegation door from the run's envelope,
//! its own escrowed allocation, and the durable delegation cells — parent-
//! local, single-entity, before the committing compare-and-set — and every
//! refusal is a failed tool result the model corrects course from. Every
//! count re-derives from durable state, and the fixture rebuilds each entity
//! from its store on every command, so each assertion below already spans a
//! coordinator loss.

mod common;

use std::sync::{Arc, Mutex};

use common::{
    delegation_config_with_fan_in, delegation_tool_id, goal_spec_draft, goal_spec_with_fan_out,
    goal_task_creation_command, run_scope, skill_id, task_definition, task_scope, Fixture, SKILL,
    SKILL_2, TENANT,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::SessionMemoryStore;
use rakka_agent::{
    AgentA2aSendExecutor, AgentA2aSendFinding, AgentDelegationRecord, AgentDispatchFuture,
    AgentGoalDelegationBudget, AgentModelTurn, AgentRunEffect, AgentRunScope, AgentRunStatus,
    AgentTaskContent, AgentTaskId, AgentToolCallId, AgentToolCallRequest, TenantId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::json;

/// A send executor that creates every child it is asked to, named per skill
/// and slot, and counts what actually crossed the boundary.
struct CountingExecutor {
    seen: Mutex<Vec<AgentDelegationRecord>>,
}

impl CountingExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
        })
    }

    fn sent(&self) -> usize {
        self.seen
            .lock()
            .expect("the record log should not be poisoned")
            .len()
    }
}

impl AgentA2aSendExecutor for CountingExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        self.seen
            .lock()
            .expect("the record log should not be poisoned")
            .push(delegation.clone());
        let child = AgentTaskId::new(format!(
            "child-{}-{}",
            delegation.requested_skill.as_str(),
            delegation.slot
        ))
        .expect("task id should be valid");
        Box::pin(async move {
            Ok(AgentA2aSendFinding::Sent {
                child_task: child,
                child_run: None,
                peer_status: "submitted".to_string(),
            })
        })
    }
}

fn delegate_call(id: &str, skill: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(id).expect("call id should be valid"),
        delegation_tool_id(),
        json!({ "skill": skill, "input": { "text": "hello" } }),
    )
    .expect("the tool call is bounded")
}

fn two_delegation_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Fanning out twice.")
        .with_tool_call(delegate_call("delegate-1", SKILL))
        .with_tool_call(delegate_call("delegate-2", SKILL_2))
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

struct CeilingWorld {
    fixture: Fixture,
    executor: Arc<CountingExecutor>,
    session: Arc<rakka_agent::InMemorySessionMemoryStore>,
}

/// A goal-rooted world under explicit delegation ceilings, scripted with a
/// two-delegation turn and a proposing turn.
async fn ceiling_world(budget: AgentGoalDelegationBudget) -> CeilingWorld {
    let executor = CountingExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(two_delegation_turn())
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(delegation_config_with_fan_in());
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_fan_out(None, Some(budget)), true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");
    CeilingWorld {
        fixture,
        executor,
        session,
    }
}

/// Every refusal code the recorded session shows the model, in order.
async fn session_refusal_codes(
    session: &Arc<rakka_agent::InMemorySessionMemoryStore>,
) -> Vec<String> {
    let page = session
        .read(&run_scope(), rakka_agent::SessionMemoryCursor::start())
        .await
        .expect("the session should read");
    page.entries
        .iter()
        .filter(|entry| entry.role == rakka_agent::MemoryEntryRole::ToolResult)
        .filter_map(|entry| {
            entry
                .content
                .inline_value()
                .and_then(|value| value.get("error"))
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
        .collect()
}

async fn committed_cells(fixture: &Fixture) -> usize {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    state.loop_state().expect("the loop ran").delegation_count()
}

/// Depth: a ceiling of zero refuses the first hop — the child would sit at
/// depth one — and the refusal is stable across the restart every fixture
/// command already is.
#[tokio::test]
async fn the_depth_ceiling_fails_closed() {
    let world = ceiling_world(AgentGoalDelegationBudget {
        max_depth: Some(0),
        ..Default::default()
    })
    .await;
    assert_eq!(committed_cells(&world.fixture).await, 0);
    assert_eq!(world.executor.sent(), 0, "nothing crossed the boundary");
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes.len(), 2);
    assert!(codes.iter().all(|code| code == "delegation-depth-exceeded"));
}

/// Fan-out: the second direct child of a run bounded to one refuses, in the
/// same turn, and exactly one send ever reached the executor.
#[tokio::test]
async fn the_fan_out_ceiling_fails_closed_mid_turn() {
    let world = ceiling_world(AgentGoalDelegationBudget {
        max_fan_out: Some(1),
        ..Default::default()
    })
    .await;
    assert_eq!(committed_cells(&world.fixture).await, 1);
    assert_eq!(world.executor.sent(), 1);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes, vec!["delegation-fan-out-exceeded".to_string()]);
}

/// Concurrency: the second concurrently unsettled child refuses under a
/// ceiling of one, and the count re-derives from the durable cells.
#[tokio::test]
async fn the_concurrency_ceiling_fails_closed_mid_turn() {
    let world = ceiling_world(AgentGoalDelegationBudget {
        max_concurrent: Some(1),
        ..Default::default()
    })
    .await;
    assert_eq!(committed_cells(&world.fixture).await, 1);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes, vec!["delegation-concurrency-exceeded".to_string()]);
}

/// Descendants: a conserved allocation of one covers exactly one child — the
/// first delegation is granted a zero sub-quota, carried to the wire as the
/// child's own ceiling, and the second refuses. On terminal, the spend folds
/// into the run's consumption, which is what a replacement generation would
/// be escrowed against.
#[tokio::test]
async fn the_descendants_escrow_fails_closed_and_settles_at_terminal() {
    let world = ceiling_world(AgentGoalDelegationBudget {
        max_descendants: Some(1),
        ..Default::default()
    })
    .await;
    assert_eq!(committed_cells(&world.fixture).await, 1);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes, vec!["delegation-descendants-exhausted".to_string()]);

    // The one sent record carried the escrowed sub-quota: nothing for the
    // child's own subtree, as validated provenance on the wire.
    let seen = world
        .executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].granted_descendants, Some(0));
    assert_eq!(
        seen[0]
            .budget
            .expect("the wire budget carries the narrowed grant")
            .max_descendants,
        Some(0)
    );

    // The terminal fold: one child plus its zero sub-quota, on the very
    // consumption the owed settlement carries up.
    let mut run = world.fixture.run();
    run.recover(world.fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert!(state.status().expect("the run exists").is_terminal());
    assert_eq!(
        state
            .loop_state()
            .expect("loop state")
            .budget()
            .consumption()
            .descendants,
        1
    );
}

/// Budget: a goal that disables delegation outright — a zero descendants
/// allocation — still assigns and completes; the empty dimension refuses
/// delegation, never work.
#[tokio::test]
async fn a_zero_descendants_allocation_never_refuses_the_assignment() {
    let world = ceiling_world(AgentGoalDelegationBudget {
        max_descendants: Some(0),
        ..Default::default()
    })
    .await;
    // Both delegations refused; the run still completed its own work.
    assert_eq!(committed_cells(&world.fixture).await, 0);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes.len(), 2);
    assert!(codes
        .iter()
        .all(|code| code == "delegation-descendants-exhausted"));
    let mut run = world.fixture.run();
    run.recover(world.fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed),
        "an empty descendants grant disables delegation, not the run"
    );
}

/// A world whose root task is itself a delegated child: its run's envelope
/// carries the validated provenance chain, which is what the deep-chain
/// ceilings and cycle rejection enforce against.
async fn delegated_child_world(
    provenance: rakka_agent::AgentTaskDelegationProvenance,
) -> CeilingWorld {
    let executor = CountingExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Sub-delegating.")
                        .with_tool_call(delegate_call("delegate-1", SKILL)),
                )
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(delegation_config_with_fan_in());
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Create {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::TaskCreation,
                [TENANT, "ticket-1", "1"],
            )
            .expect("the operation id derives"),
            creation: Box::new(rakka_agent::AgentTaskCreation {
                definition: task_definition(),
                input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: Some(common::agent_id()),
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: Some(provenance.parent_task.clone()),
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: Some(Box::new(provenance)),
                telemetry: Default::default(),
            }),
        })
        .await
        .expect("the delegated child task should create");
    fixture.pump().await.expect("the loop should converge");
    CeilingWorld {
        fixture,
        executor,
        session,
    }
}

fn upstream_parent_run() -> AgentRunScope {
    AgentRunScope::new(
        TenantId::new(TENANT),
        rakka_agent::AgentId::new("root-coordinator").expect("agent id should be valid"),
        rakka_agent::AgentRunId::new("root-task-gen-1").expect("run id should be valid"),
    )
    .expect("the scope is valid")
}

fn chain_provenance(
    ancestors: Vec<rakka_agent::AgentId>,
    budget: Option<AgentGoalDelegationBudget>,
) -> rakka_agent::AgentTaskDelegationProvenance {
    let parent_run = upstream_parent_run();
    let lineage: Vec<_> = (0..ancestors.len().max(1) - 1)
        .map(|slot| {
            rakka_agent::delegation_id_for(&parent_run, 1, slot).expect("the delegation id derives")
        })
        .collect();
    let depth = lineage.len() as u32 + 1;
    rakka_agent::AgentTaskDelegationProvenance {
        delegation: rakka_agent::delegation_id_for(&parent_run, 2, 0)
            .expect("the delegation id derives"),
        parent_task: AgentTaskId::new("root-task").expect("task id should be valid"),
        parent_run,
        lineage,
        ancestors: if ancestors.len() <= 1 {
            Vec::new()
        } else {
            ancestors[..ancestors.len() - 1].to_vec()
        },
        depth,
        requested_skill: skill_id(),
        capability_scopes: Default::default(),
        credential_bindings: Vec::new(),
        result_schema: None,
        budget,
        deadline: None,
    }
}

/// Depth on a real chain: a child created at depth one under a max-depth-one
/// grant refuses its own delegation — the grandchild would sit at depth two.
#[tokio::test]
async fn a_deep_chain_refuses_past_the_granted_depth() {
    let provenance = chain_provenance(
        vec![rakka_agent::AgentId::new("root-coordinator").expect("agent id")],
        Some(AgentGoalDelegationBudget {
            max_depth: Some(1),
            ..Default::default()
        }),
    );
    let world = delegated_child_world(provenance).await;
    assert_eq!(committed_cells(&world.fixture).await, 0);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes, vec!["delegation-depth-exceeded".to_string()]);
}

/// Cycle, direct: the catalog resolves the requested skill back to an agent
/// already in the validated ancestor chain — the delegating parent itself —
/// and the delegation refuses at agent-identity granularity.
#[tokio::test]
async fn a_cycle_through_the_ancestor_chain_is_refused() {
    // The fixture catalog resolves SKILL to the "translator" specialist;
    // a chain that already passed through the translator refuses to visit
    // it again.
    let provenance = chain_provenance(
        vec![
            rakka_agent::AgentId::new("translator").expect("agent id"),
            rakka_agent::AgentId::new("root-coordinator").expect("agent id"),
        ],
        None,
    );
    let world = delegated_child_world(provenance).await;
    assert_eq!(committed_cells(&world.fixture).await, 0);
    assert_eq!(world.executor.sent(), 0);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes, vec!["delegation-cycle-detected".to_string()]);
}

/// Cycle, self: a run may never delegate to its own agent, ancestors or not.
#[tokio::test]
async fn self_delegation_is_a_cycle() {
    let executor = CountingExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    // A catalog that resolves the skill to the fixture agent itself.
    let config = rakka_agent::AgentRunDelegationConfig::new(
        delegation_tool_id(),
        Arc::new(
            rakka_agent::StaticAgentDelegationCatalog::new().with_target(
                skill_id(),
                rakka_agent::AgentDelegationTarget::new(
                    common::agent_id(),
                    rakka_agent::AgentTaskDefinitionId::new("self-definition")
                        .expect("definition id should be valid"),
                ),
            ),
        ),
        std::collections::BTreeSet::from([
            rakka_agent::AgentCoordinationCapabilityKind::Delegation,
        ]),
    )
    .expect("the delegation configuration declares the capability");
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Delegating to myself.")
                        .with_tool_call(delegate_call("delegate-1", SKILL)),
                )
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(config);
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_fan_out(None, None), true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");

    assert_eq!(committed_cells(&fixture).await, 0);
    assert_eq!(executor.sent(), 0);
    let codes = session_refusal_codes(&session).await;
    assert_eq!(codes, vec!["delegation-cycle-detected".to_string()]);
}

/// A chain that carries lineage without its ancestor agents cannot be
/// cycle-checked, so it may not be extended: the run finishes its own work
/// but refuses to sub-delegate, deny-when-unknown.
#[tokio::test]
async fn an_unaccounted_ancestry_refuses_sub_delegation() {
    let parent_run = upstream_parent_run();
    let lineage: Vec<_> = (0..2)
        .map(|slot| {
            rakka_agent::delegation_id_for(&parent_run, 1, slot).expect("the delegation id derives")
        })
        .collect();
    let provenance = rakka_agent::AgentTaskDelegationProvenance {
        delegation: rakka_agent::delegation_id_for(&parent_run, 2, 0)
            .expect("the delegation id derives"),
        parent_task: AgentTaskId::new("root-task").expect("task id should be valid"),
        parent_run,
        depth: lineage.len() as u32 + 1,
        lineage,
        // The pre-slice shape: a chain recorded before ancestry existed.
        ancestors: Vec::new(),
        requested_skill: skill_id(),
        capability_scopes: Default::default(),
        credential_bindings: Vec::new(),
        result_schema: None,
        budget: None,
        deadline: None,
    };
    let world = delegated_child_world(provenance).await;
    assert_eq!(committed_cells(&world.fixture).await, 0);
    let codes = session_refusal_codes(&world.session).await;
    assert_eq!(codes, vec!["delegation-ancestry-unknown".to_string()]);
    let mut run = world.fixture.run();
    run.recover(world.fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed),
        "the run's own work survives; only sub-delegation is closed"
    );
}

/// Forged ancestry refuses at the creation door: an ancestor chain that
/// disagrees with the lineage never becomes durable provenance the ceilings
/// would then trust.
#[tokio::test]
async fn an_incoherent_ancestry_refuses_the_child_creation() {
    let parent_run = upstream_parent_run();
    let lineage: Vec<_> = (0..2)
        .map(|slot| {
            rakka_agent::delegation_id_for(&parent_run, 1, slot).expect("the delegation id derives")
        })
        .collect();
    let provenance = rakka_agent::AgentTaskDelegationProvenance {
        delegation: rakka_agent::delegation_id_for(&parent_run, 2, 0)
            .expect("the delegation id derives"),
        parent_task: AgentTaskId::new("root-task").expect("task id should be valid"),
        parent_run,
        depth: lineage.len() as u32 + 1,
        lineage,
        // Two lineage entries, one claimed agent: a gap to hide an ancestor
        // in.
        ancestors: vec![rakka_agent::AgentId::new("root-coordinator").expect("agent id")],
        requested_skill: skill_id(),
        capability_scopes: Default::default(),
        credential_bindings: Vec::new(),
        result_schema: None,
        budget: None,
        deadline: None,
    };
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_delegation(delegation_config_with_fan_in());
    fixture.instantiate_agent().await;
    let error = fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Create {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::TaskCreation,
                [TENANT, "ticket-1", "1"],
            )
            .expect("the operation id derives"),
            creation: Box::new(rakka_agent::AgentTaskCreation {
                definition: task_definition(),
                input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: Some(common::agent_id()),
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: Some(Box::new(provenance)),
                telemetry: Default::default(),
            }),
        })
        .await
        .expect_err("the forged ancestry refuses the creation");
    assert_eq!(error.code(), "task-delegation-provenance-invalid");
    let _ = task_scope();
}

/// The definition's own delegation ceilings cap what any provenance can
/// enable: a "root child" minted with an inflated budget still delegates
/// only what the definition permits.
#[tokio::test]
async fn the_definition_ceiling_caps_a_forged_root_grant() {
    let executor = CountingExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(two_delegation_turn())
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(delegation_config_with_fan_in());
    fixture.instantiate_agent().await;
    // The peer grants everything; the definition allows one direct child.
    let provenance = chain_provenance(
        vec![rakka_agent::AgentId::new("root-coordinator").expect("agent id")],
        Some(AgentGoalDelegationBudget {
            max_fan_out: Some(64),
            ..Default::default()
        }),
    );
    fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Create {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::TaskCreation,
                [TENANT, "ticket-1", "1"],
            )
            .expect("the operation id derives"),
            creation: Box::new(rakka_agent::AgentTaskCreation {
                definition: task_definition().with_delegation(AgentGoalDelegationBudget {
                    max_fan_out: Some(1),
                    ..Default::default()
                }),
                input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: Some(common::agent_id()),
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: Some(Box::new(provenance)),
                telemetry: Default::default(),
            }),
        })
        .await
        .expect("the child task should create");
    fixture.pump().await.expect("the loop should converge");

    assert_eq!(committed_cells(&fixture).await, 1);
    assert_eq!(executor.sent(), 1);
    let codes = session_refusal_codes(&session).await;
    assert_eq!(codes, vec!["delegation-fan-out-exceeded".to_string()]);
}

/// The definition's ceilings reach a task with neither a goal record nor
/// delegation provenance: a plain agent-owned task's runs — an epoch task's
/// equally — carry the definition-only envelope, so
/// `AgentTaskDefinition::delegation` enforces at the door instead of
/// enforcing nothing for the very tasks that have no other authority source.
#[tokio::test]
async fn a_plain_tasks_definition_ceilings_enforce_at_the_door() {
    let executor = CountingExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(two_delegation_turn())
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(delegation_config_with_fan_in());
    fixture.instantiate_agent().await;
    fixture
        .create_task_with(
            task_definition().with_delegation(AgentGoalDelegationBudget {
                max_fan_out: Some(1),
                ..Default::default()
            }),
        )
        .await;
    fixture.pump().await.expect("the loop should converge");

    assert_eq!(committed_cells(&fixture).await, 1);
    assert_eq!(executor.sent(), 1);
    let codes = session_refusal_codes(&session).await;
    assert_eq!(codes, vec!["delegation-fan-out-exceeded".to_string()]);
}

/// A creation carrying both a goal spec and delegation provenance refuses:
/// the goal record would win the run's envelope and re-root the chain —
/// empty ancestors, depth zero — voiding the parent's ceilings and the
/// cycle set at every later door.
#[tokio::test]
async fn a_delegated_creation_carrying_a_goal_spec_is_refused() {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(proposing_turn()),
    ))
    .with_delegation(delegation_config_with_fan_in());
    fixture.instantiate_agent().await;

    let mut command = goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec_with_fan_out(None, None), true),
    );
    let rakka_agent::AgentTaskEntityCommand::Create { creation, .. } = &mut command else {
        panic!("the goal creation command is a create");
    };
    creation.delegation = Some(Box::new(chain_provenance(
        vec![rakka_agent::AgentId::new("root-coordinator").expect("agent id")],
        None,
    )));

    let error = fixture
        .apply_task_command(command)
        .await
        .expect_err("a delegated child cannot institute its own goal");
    assert_eq!(error.code(), "task-delegation-provenance-invalid");
}
