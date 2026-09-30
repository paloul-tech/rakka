//! Property tests for the derived continuous-goal wake identity.
//!
//! Specification: section 6.9. The slice 3.1 exit criterion is that wake-id
//! deduplication is a *construction* property: the same logical occurrence
//! reached from any trigger path — a timer scan, a duplicate scan after
//! scanner restart, a redelivered event, an A2A command — derives one
//! [`AgentWakeId`], while any change to the tenant, goal, schedule revision,
//! or occurrence derives a different one. The golden vectors at the bottom pin
//! the encoding itself: the derivation is a persisted compatibility surface,
//! and a drift that every property here tolerates would still break records
//! already written.

use proptest::prelude::*;
use rakka_agent::{
    wake_admission_operation_id, wake_id_for_occurrence, AgentGoalId, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentWakeBinding, AgentWakeCallbackId,
    AgentWakeEventId, AgentWakeId, AgentWakeOccurrence, AgentWakeTriggerKind, ScheduleRevision,
    TenantId, AGENT_WAKE_ID_PREFIX,
};
use rakka_agent_workflow::{AgentTimestampMillis, AgentTriggerSource};

/// Characters an application-supplied identifier may reasonably contain,
/// including the `-` that could forge a readable-composite boundary — the
/// length-prefixed digest encoding must be indifferent to all of them.
const ALPHABET: &[char] = &[
    'a', 'b', 'c', 'd', 'e', 'z', 'A', 'Z', '0', '1', '9', '-', '_', '.', ':', '=', '@', 'é', '本',
];

fn segment() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(ALPHABET.to_vec()), 1..48)
        .prop_map(|chars| chars.into_iter().collect())
}

fn tenant() -> impl Strategy<Value = TenantId> {
    segment().prop_map(TenantId::new)
}

fn goal() -> impl Strategy<Value = AgentGoalId> {
    segment().prop_map(|value| AgentGoalId::new(value).expect("alphabet segments are valid ids"))
}

fn schedule_revision() -> impl Strategy<Value = ScheduleRevision> {
    (1_u64..u64::MAX).prop_map(ScheduleRevision::new)
}

fn occurrence() -> impl Strategy<Value = AgentWakeOccurrence> {
    prop_oneof![
        any::<u64>().prop_map(|millis| AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(millis),
        }),
        segment().prop_map(|value| AgentWakeOccurrence::ExternalEvent {
            event: AgentWakeEventId::new(value).expect("alphabet segments are valid ids"),
        }),
        (segment(), segment()).prop_map(|(first, second)| AgentWakeOccurrence::Command {
            operation: AgentOperationId::new(AgentOperationKind::Command, [first, second])
                .expect("alphabet segments are valid operation segments"),
        }),
        segment().prop_map(|value| AgentWakeOccurrence::Callback {
            callback: AgentWakeCallbackId::new(value).expect("alphabet segments are valid ids"),
        }),
    ]
}

fn trigger_kind() -> impl Strategy<Value = AgentWakeTriggerKind> {
    proptest::sample::select(AgentWakeTriggerKind::ALL.to_vec())
}

/// One delivery of an occurrence: everything about a wake that is metadata,
/// not identity — the trigger class, the accepted time, and which source
/// metadata, if any, the delivery carried.
fn trigger_path() -> impl Strategy<Value = (AgentWakeTriggerKind, u64, u8)> {
    (trigger_kind(), any::<u64>(), 0_u8..3)
}

fn binding_for(
    tenant: &TenantId,
    goal: &AgentGoalId,
    revision: ScheduleRevision,
    occurrence: &AgentWakeOccurrence,
    path: &(AgentWakeTriggerKind, u64, u8),
) -> AgentWakeBinding {
    let (kind, accepted_at, source) = path;
    let mut binding = AgentWakeBinding::new(
        tenant.clone(),
        goal.clone(),
        revision,
        occurrence.clone(),
        *kind,
        AgentTimestampMillis::new(*accepted_at),
        AgentRevisionNumber::INITIAL,
    )
    .expect("a binding over valid components is accepted");
    binding = match source {
        0 => binding,
        1 => binding
            .with_source(AgentTriggerSource::schedule())
            .expect("a bare schedule source is accepted"),
        _ => binding
            .with_source(AgentTriggerSource::webhook())
            .expect("a bare webhook source is accepted"),
    };
    binding
}

proptest! {
    /// The exit criterion of slice 3.1: however differently the same
    /// occurrence is delivered — trigger class, accepted time, source labels,
    /// duplicate construction — every path derives the same wake id and the
    /// same admission operation id.
    #[test]
    fn the_same_occurrence_from_any_trigger_path_yields_one_identity(
        tenant in tenant(),
        goal in goal(),
        revision in schedule_revision(),
        occurrence in occurrence(),
        first_path in trigger_path(),
        second_path in trigger_path(),
    ) {
        let first = binding_for(&tenant, &goal, revision, &occurrence, &first_path);
        let second = binding_for(&tenant, &goal, revision, &occurrence, &second_path);
        prop_assert_eq!(first.wake_id(), second.wake_id());
        prop_assert_eq!(
            first.admission_operation_id().expect("operation id derives"),
            second.admission_operation_id().expect("operation id derives")
        );

        let direct = wake_id_for_occurrence(&tenant, &goal, revision, &occurrence)
            .expect("derivation over valid components succeeds");
        prop_assert_eq!(first.wake_id(), &direct);
    }

    /// Distinct logical occurrences derive distinct identities: the
    /// length-prefixed encoding is injective, so no arrangement of goal text,
    /// revision digits, and occurrence text can alias another tuple.
    #[test]
    fn distinct_tuples_derive_distinct_identities(
        tenant in tenant(),
        first_goal in goal(),
        second_goal in goal(),
        first_revision in schedule_revision(),
        second_revision in schedule_revision(),
        first_occurrence in occurrence(),
        second_occurrence in occurrence(),
    ) {
        prop_assume!(
            (first_goal.as_str(), first_revision, &first_occurrence)
                != (second_goal.as_str(), second_revision, &second_occurrence)
        );
        let first = wake_id_for_occurrence(&tenant, &first_goal, first_revision, &first_occurrence)
            .expect("derivation succeeds");
        let second =
            wake_id_for_occurrence(&tenant, &second_goal, second_revision, &second_occurrence)
                .expect("derivation succeeds");
        prop_assert_ne!(first, second);
    }

    /// Changing exactly one component changes the identity — the schedule
    /// revision case is the fencing precondition of slice 3.2: a wake
    /// constructed under an obsolete revision can never collide with the
    /// adopted one.
    #[test]
    fn every_component_is_load_bearing(
        tenant in tenant(),
        other_tenant in tenant(),
        goal in goal(),
        revision in schedule_revision(),
        occurrence in occurrence(),
    ) {
        let baseline = wake_id_for_occurrence(&tenant, &goal, revision, &occurrence)
            .expect("derivation succeeds");

        let fenced = wake_id_for_occurrence(&tenant, &goal, revision.next(), &occurrence)
            .expect("derivation succeeds");
        prop_assert_ne!(&baseline, &fenced);

        if other_tenant != tenant {
            let cross_tenant = wake_id_for_occurrence(&other_tenant, &goal, revision, &occurrence)
                .expect("derivation succeeds");
            prop_assert_ne!(&baseline, &cross_tenant);
        }
    }

    /// The derived value is always a valid, fixed-length identity segment:
    /// no goal or event identity, however long or exotic, can push a wake id
    /// past the identity bounds the epoch derivations of slice 3.3 build on.
    #[test]
    fn the_derived_identity_is_always_valid_and_fixed_length(
        tenant in tenant(),
        goal in goal(),
        revision in schedule_revision(),
        occurrence in occurrence(),
    ) {
        let wake = wake_id_for_occurrence(&tenant, &goal, revision, &occurrence)
            .expect("derivation succeeds");
        prop_assert!(wake.as_str().starts_with(AGENT_WAKE_ID_PREFIX));
        prop_assert_eq!(wake.as_str().len(), AGENT_WAKE_ID_PREFIX.len() + 64);
        prop_assert!(AgentWakeId::new(wake.as_str()).is_ok());
    }
}

/// The same identity value under a different occurrence kind is a different
/// occurrence: the kind label is a digest segment of its own.
#[test]
fn occurrence_kinds_with_equal_identity_values_stay_distinct() {
    let tenant = TenantId::new("acme");
    let goal = AgentGoalId::new("nightly-reconciliation").expect("goal id is valid");
    let event = AgentWakeOccurrence::ExternalEvent {
        event: AgentWakeEventId::new("1753500000000").expect("event id is valid"),
    };
    let scheduled = AgentWakeOccurrence::Scheduled {
        due_at: AgentTimestampMillis::new(1_753_500_000_000),
    };
    let event_id = wake_id_for_occurrence(&tenant, &goal, ScheduleRevision::INITIAL, &event)
        .expect("derivation succeeds");
    let scheduled_id =
        wake_id_for_occurrence(&tenant, &goal, ScheduleRevision::INITIAL, &scheduled)
            .expect("derivation succeeds");
    assert_ne!(event_id, scheduled_id);
}

/// Golden vectors pinning the persisted encoding. A change here is a schema
/// migration, not a refactor: wakes already durable were derived under this
/// exact construction.
#[test]
fn the_derivation_matches_its_pinned_golden_vectors() {
    let tenant = TenantId::new("acme");
    let goal = AgentGoalId::new("nightly-reconciliation").expect("goal id is valid");

    let scheduled = wake_id_for_occurrence(
        &tenant,
        &goal,
        ScheduleRevision::new(1),
        &AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(1_753_500_000_000),
        },
    )
    .expect("derivation succeeds");
    assert_eq!(
        scheduled.as_str(),
        "wake-73e57f72c96f774e5dd6f15cc0d3fb10f758ab6b1c59ebd7b0389e074cc8f392"
    );

    let event = wake_id_for_occurrence(
        &tenant,
        &goal,
        ScheduleRevision::new(2),
        &AgentWakeOccurrence::ExternalEvent {
            event: AgentWakeEventId::new("ledger-sync-42").expect("event id is valid"),
        },
    )
    .expect("derivation succeeds");
    assert_eq!(
        event.as_str(),
        "wake-eedf7353822d4668edeb935996821d35ea7ee0f68d0b3672983d751ad188b9d5"
    );

    let command = wake_id_for_occurrence(
        &tenant,
        &goal,
        ScheduleRevision::new(1),
        &AgentWakeOccurrence::Command {
            operation: AgentOperationId::new(AgentOperationKind::Command, ["acme", "wake-now-7"])
                .expect("operation id is valid"),
        },
    )
    .expect("derivation succeeds");
    assert_eq!(
        command.as_str(),
        "wake-254adf4942e382776cce9053bad1b0229022bfc9957a1b421def8c52e1bde08e"
    );

    let admission = wake_admission_operation_id(&tenant, &goal, &scheduled)
        .expect("admission operation id derives");
    assert_eq!(
        admission.as_str(),
        format!("wake-admission/acme/nightly-reconciliation/{scheduled}")
    );
}
