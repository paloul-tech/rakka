//! Retention for a knowledge space: a window per trust status, discharged by
//! a bounded, deployment-invoked sweep.
//!
//! [Specification 13.1](../../../docs/plans/rakka-agent/spec.md) requires every
//! memory API to provide retention, tombstone, and deletion semantics. The two
//! per-claim operations are on [`KnowledgeGraphStore`]; this module is the
//! third: it ages claims out of one `(TenantId, KnowledgeSpaceId)` space.
//!
//! # One sweep, every backend
//!
//! The sweep is written once, here, over the store's existing `query` and
//! `transitions` reads plus `delete`. A backend implements no retention logic
//! of its own, so the policy has one implementation and one proof
//! ([specification 13.6](../../../docs/plans/rakka-agent/spec.md): a backend
//! stays small).
//!
//! # The clock
//!
//! A claim does not record when it entered its current trust status, so the
//! sweep reads it:
//!
//! | The claim is | It entered that state at |
//! | --- | --- |
//! | a tombstoned stub | the tombstone's `tombstoned_at` |
//! | `Proposed` | `created_at` |
//! | in any other status | `occurred_at` of its last transition |
//!
//! A claim is due when that instant plus the window is at or before `now`. A
//! stub is judged only by the tombstone window, never by its trust status. A
//! status with no window — and a stub when there is no tombstone window — is
//! retained. **There is no built-in window:** nothing names a default for
//! shared knowledge, and deleting it by a default nobody chose would be a
//! failure of this design, not a convenience.
//!
//! # A claim that moves under the sweep is left alone
//!
//! Every delete the sweep issues carries the transition count and tombstone
//! state it read. If the claim's trust moved, or an operator tombstoned it,
//! between the read and the delete, the store answers `claim-delete-stale`;
//! the sweep counts the claim as moved and goes on. The refusal spends
//! nothing, so a later pass deletes the claim under the same derived operation
//! id once it is due in its new state.
//!
//! # No resident sweeper, no registry
//!
//! The sweep is called by the application on its own schedule, like the
//! private tier's `AgentMemoryRetentionSweep`. Rakka keeps no registry of
//! knowledge spaces: the application names the spaces it sweeps. The policy is
//! deployment configuration passed to each call, never stored per claim, so it
//! can tighten or loosen at any time.
//!
//! # The legal hold governs the sweep only
//!
//! A held space survives every sweep: the pass reads nothing and reports
//! [`KnowledgeSpaceRetentionReport::held`]. An explicit `tombstone` or `delete`
//! takes no policy and is not blocked by a hold, as in the private tier.
//!
//! # What erasure does not reach
//!
//! Deleting or tombstoning a claim removes it from the graph. It does not
//! reach the copies of its content that live elsewhere, and a deployment
//! should not discover that after an erasure request:
//!
//! - **The run record.** The `ClaimAppend` effect request on the run that
//!   asked for the append keeps the subject, predicate, object and evidence
//!   references. Rakka does not purge run records.
//! - **Session memory.** A tool result the claim was mapped from stays in the
//!   run's session memory until that run's retention is discharged.
//! - **Evidence artifacts.** The store holds references only. The artifacts
//!   are the application's.
//! - **Transition reasons, after a tombstone.** A tombstone keeps the whole
//!   transition history, including each transition's free-text reason and its
//!   evidence. A reason that quotes the statement survives a tombstone and is
//!   removed only by a delete.
//! - **Context snapshots,** once a snapshot embeds communal claims. A snapshot
//!   keeps its copy until the run's snapshot purge, by the rule
//!   `rakka_agent::memory_retention` already sets for private memory, and
//!   Rakka keeps no reverse index from a claim to the snapshots or run records
//!   that copied it.
//!
//! Erasing a subject's content therefore means deleting the claims **and**
//! discharging the retention of the runs that produced them.
//!
//! # Cost
//!
//! Each pass walks the space in claim-id order through `query`, and makes one
//! bounded `transitions` read per candidate that has left `Proposed` and whose
//! status has a window. On a backend whose `query` filters after decoding
//! rows, a pass is linear in the space. That is an accepted cost; a "status
//! since" stamp on the claim is the remedy if it ever matters.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rakka_agent_workflow::AgentTimestampMillis;
use serde::{Deserialize, Serialize};

use crate::claim::{Claim, ClaimOperationId, ClaimTrustStatus};
use crate::error::{ClaimError, ClaimResult};
use crate::scope::KnowledgeSpaceScope;
use crate::store::{
    ClaimCursor, ClaimFilter, ClaimTransitionCursor, KnowledgeGraphStore, CLAIM_PAGE_MAX_ENTRIES,
};
use crate::tombstone::{ClaimDeleteExpectation, ClaimDeleteRequest};

/// The most claims one sweep call examines unless told otherwise.
pub const KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH: usize = 128;

/// The discriminator every sweep delete derives its operation id from, so a
/// re-driven pass converges on one delete per claim.
const RETENTION_DELETE_DISCRIMINATOR: &str = "retention";

/// How long claims rest in a knowledge space before the sweep removes them.
///
/// Deployment configuration, passed to each sweep call and never stored per
/// claim. The default — [`KnowledgeSpaceRetentionPolicy::retain_all`] — ages
/// nothing out: a trust status ages only when a window is set for it, and
/// [`ClaimTrustStatus`] is non-exhaustive, so a status added later is retained
/// until a deployment sets a window for it too.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeSpaceRetentionPolicy {
    #[serde(default)]
    windows: BTreeMap<ClaimTrustStatus, u64>,
    #[serde(default)]
    tombstone_window_millis: Option<u64>,
    #[serde(default)]
    legal_hold: bool,
}

impl KnowledgeSpaceRetentionPolicy {
    /// Nothing ages out.
    #[must_use]
    pub const fn retain_all() -> Self {
        Self {
            windows: BTreeMap::new(),
            tombstone_window_millis: None,
            legal_hold: false,
        }
    }

    /// Live claims resting in `status` age out `millis` after they entered
    /// it. Setting a status again replaces its window.
    #[must_use]
    pub fn with_window(mut self, status: ClaimTrustStatus, millis: u64) -> Self {
        self.windows.insert(status, millis);
        self
    }

    /// Tombstoned stubs, of any trust status, age out `millis` after they
    /// were tombstoned. Without it a stub lives until someone deletes it.
    #[must_use]
    pub const fn with_tombstone_window(mut self, millis: u64) -> Self {
        self.tombstone_window_millis = Some(millis);
        self
    }

    /// A held space survives every sweep. The hold does not block an explicit
    /// `tombstone` or `delete`.
    #[must_use]
    pub const fn with_legal_hold(mut self, hold: bool) -> Self {
        self.legal_hold = hold;
        self
    }

    /// The window for live claims resting in `status`, when one is set.
    #[must_use]
    pub fn window(&self, status: ClaimTrustStatus) -> Option<u64> {
        self.windows.get(&status).copied()
    }

    /// The window for tombstoned stubs, when one is set.
    #[must_use]
    pub const fn tombstone_window(&self) -> Option<u64> {
        self.tombstone_window_millis
    }

    /// Whether the space is under a legal hold.
    #[must_use]
    pub const fn legal_hold(&self) -> bool {
        self.legal_hold
    }
}

/// What one sweep call did.
///
/// Every examined claim lands in exactly one of `deleted`, `not_yet_due`,
/// `retained` and `moved`, so the four sum to `examined`.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct KnowledgeSpaceRetentionReport {
    /// Claims the call read and judged.
    pub examined: usize,
    /// Claims that were due and were deleted.
    pub deleted: usize,
    /// Claims with a window that has not run out.
    pub not_yet_due: usize,
    /// Claims no window applies to: their trust status has none, or they are
    /// stubs and the policy sets no tombstone window.
    pub retained: usize,
    /// Claims that changed between the sweep's read and its delete — trust
    /// moved, tombstoned, or already gone. They are left alone; a later pass
    /// judges them in the state they are then in.
    pub moved: usize,
    /// The space is under a legal hold: nothing was read or deleted.
    pub held: bool,
    /// Where the next call resumes, when claims remain past this call's batch.
    /// Only a position: starting again from the beginning is always safe.
    pub next: Option<ClaimCursor>,
}

/// A caller-supplied step run on each due claim immediately before its
/// delete. Test support: it is how a conformance clause moves a claim under
/// the sweep.
pub(crate) type BeforeRetentionDelete<'h> =
    dyn Fn(&Claim) -> Pin<Box<dyn Future<Output = ()> + Send + 'h>> + Send + Sync + 'h;

/// Whether a claim that entered its state at `entered_at` has outlived
/// `window` by `now`.
///
/// A due instant past the end of the clock has not arrived at any `now`: the
/// addition is checked, never saturated, because a saturated sum would call a
/// claim due before its window ran out.
fn is_due(entered_at: AgentTimestampMillis, window: u64, now: AgentTimestampMillis) -> bool {
    entered_at
        .as_millis()
        .checked_add(window)
        .is_some_and(|due_at| due_at <= now.as_millis())
}

/// Discharges one bounded retention pass over one knowledge space.
///
/// Examines at most `max_batch` claims (a bound of zero behaves as one),
/// starting after `cursor`'s position — only the position is read; the
/// cursor's own page limit is ignored — and deletes each one that is due
/// under `policy` at `now`. When claims remain past the batch,
/// [`KnowledgeSpaceRetentionReport::next`] says where to resume; the
/// application calls again until it is `None`.
///
/// Under a legal hold the pass reads nothing, deletes nothing, and reports
/// `held`.
///
/// # Errors
///
/// A store failure is returned as an error. Claims already deleted in that
/// call stay deleted, and the next call carries on: every delete is idempotent
/// on an operation id derived from the scope and the claim.
pub async fn discharge_knowledge_space_retention(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    policy: &KnowledgeSpaceRetentionPolicy,
    now: AgentTimestampMillis,
    cursor: Option<ClaimCursor>,
    max_batch: usize,
) -> ClaimResult<KnowledgeSpaceRetentionReport> {
    discharge(store, scope, policy, now, cursor, max_batch, None).await
}

/// [`discharge_knowledge_space_retention`], with a step run on each due claim
/// immediately before its delete.
pub(crate) async fn discharge_knowledge_space_retention_with_hook(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    policy: &KnowledgeSpaceRetentionPolicy,
    now: AgentTimestampMillis,
    cursor: Option<ClaimCursor>,
    max_batch: usize,
    before_delete: &BeforeRetentionDelete<'_>,
) -> ClaimResult<KnowledgeSpaceRetentionReport> {
    discharge(
        store,
        scope,
        policy,
        now,
        cursor,
        max_batch,
        Some(before_delete),
    )
    .await
}

async fn discharge(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    policy: &KnowledgeSpaceRetentionPolicy,
    now: AgentTimestampMillis,
    cursor: Option<ClaimCursor>,
    max_batch: usize,
    before_delete: Option<&BeforeRetentionDelete<'_>>,
) -> ClaimResult<KnowledgeSpaceRetentionReport> {
    let mut report = KnowledgeSpaceRetentionReport::default();
    if policy.legal_hold() {
        report.held = true;
        return Ok(report);
    }
    let max_batch = max_batch.max(1);
    // Stubs are swept too: the tombstone window is theirs.
    let filter = ClaimFilter::matching_all().including_tombstoned();
    let mut position = cursor.and_then(|cursor| cursor.position().cloned());

    while report.examined < max_batch {
        let remaining = max_batch - report.examined;
        let page_cursor = match &position {
            Some(after) => ClaimCursor::after(after.clone()),
            None => ClaimCursor::start(),
        }
        .with_limit(remaining.min(CLAIM_PAGE_MAX_ENTRIES));
        let page = store.query(scope, &filter, page_cursor).await?;
        let more = page.next.is_some();
        // A backend may serve a page shorter than asked (a declared bound), but
        // never longer; the `take` holds the batch bound against one that does.
        for claim in page.claims.into_iter().take(remaining) {
            report.examined += 1;
            position = Some(claim.claim_id.clone());
            examine(
                store,
                scope,
                policy,
                now,
                &claim,
                before_delete,
                &mut report,
            )
            .await?;
        }
        if !more {
            // The space is exhausted: there is nowhere to resume.
            return Ok(report);
        }
    }
    // The batch is spent and the last page said more claims follow. The cursor
    // rests on the last claim examined — which this call may have deleted; a
    // keyset position needs no row behind it.
    report.next = position.map(ClaimCursor::after);
    Ok(report)
}

/// Judges one claim against the policy and deletes it when it is due.
async fn examine(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    policy: &KnowledgeSpaceRetentionPolicy,
    now: AgentTimestampMillis,
    claim: &Claim,
    before_delete: Option<&BeforeRetentionDelete<'_>>,
    report: &mut KnowledgeSpaceRetentionReport,
) -> ClaimResult<()> {
    // The window and the instant the claim entered the state it applies to. A
    // stub is judged by the tombstone window alone, whatever its trust.
    let (window, entered_at) = if let Some(tombstone) = claim.tombstone() {
        (policy.tombstone_window(), Some(tombstone.tombstoned_at))
    } else if claim.trust() == ClaimTrustStatus::Proposed {
        (policy.window(claim.trust()), Some(claim.created_at))
    } else {
        (policy.window(claim.trust()), None)
    };
    let Some(window) = window else {
        report.retained += 1;
        return Ok(());
    };
    let entered_at = match entered_at {
        Some(entered_at) => entered_at,
        None => {
            // The last transition is the one whose ordinal equals the claim's
            // transition count. One bounded read: the entry after the one
            // before it, and nothing after that.
            let count = claim.transition_count();
            let page = store
                .transitions(
                    scope,
                    &claim.claim_id,
                    ClaimTransitionCursor::after_ordinal(count.saturating_sub(1)).with_limit(1),
                )
                .await?;
            match (page.transitions.first(), page.next) {
                (Some(last), None) if last.ordinal == count => last.occurred_at,
                // No such transition, a different ordinal, or a later one
                // behind it: the claim moved since the query read it.
                _ => {
                    report.moved += 1;
                    return Ok(());
                }
            }
        }
    };
    if !is_due(entered_at, window, now) {
        report.not_yet_due += 1;
        return Ok(());
    }

    if let Some(before_delete) = before_delete {
        before_delete(claim).await;
    }
    let request = ClaimDeleteRequest {
        claim_id: claim.claim_id.clone(),
        operation_id: ClaimOperationId::derive_delete(
            scope,
            &claim.claim_id,
            RETENTION_DELETE_DISCRIMINATOR,
        )?,
        // The state the sweep judged, both halves: a tombstone does not change
        // the transition count, so the count alone would let the sweep delete
        // the stub an operator made of a claim it read as live.
        expected: Some(ClaimDeleteExpectation {
            transition_count: claim.transition_count(),
            tombstoned: claim.is_tombstoned(),
        }),
    };
    match store.delete(scope, &request).await {
        Ok(()) => report.deleted += 1,
        // Moved, tombstoned, or deleted by someone else since the read. The
        // refusal spent nothing; a later pass judges the claim as it then is.
        Err(ClaimError::DeleteStale { .. } | ClaimError::NotFound { .. }) => report.moved += 1,
        Err(error) => return Err(error),
    }
    Ok(())
}

/// A bounded, deployment-invoked retention pass over one knowledge space.
///
/// Holds only an `Arc<dyn KnowledgeGraphStore>`, so it runs unchanged over any
/// backend. There is no resident sweeper: the application calls
/// [`KnowledgeSpaceRetentionSweep::discharge`] on its own schedule, once per
/// space it names, and again while the report carries a cursor.
#[derive(Clone)]
pub struct KnowledgeSpaceRetentionSweep {
    store: Arc<dyn KnowledgeGraphStore>,
    max_batch: usize,
}

impl fmt::Debug for KnowledgeSpaceRetentionSweep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KnowledgeSpaceRetentionSweep")
            .field("backend", &self.store.backend_name())
            .field("max_batch", &self.max_batch)
            .finish()
    }
}

impl KnowledgeSpaceRetentionSweep {
    /// A sweep over the given store, at
    /// [`KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH`].
    #[must_use]
    pub fn new(store: Arc<dyn KnowledgeGraphStore>) -> Self {
        Self {
            store,
            max_batch: KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
        }
    }

    /// Bounds how many claims one call examines; zero behaves as one.
    #[must_use]
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch.max(1);
        self
    }

    /// The most claims one call examines.
    #[must_use]
    pub const fn max_batch(&self) -> usize {
        self.max_batch
    }

    /// Discharges one bounded pass; see
    /// [`discharge_knowledge_space_retention`].
    ///
    /// # Errors
    ///
    /// Returns the store's failure. Claims already deleted in the call stay
    /// deleted.
    pub async fn discharge(
        &self,
        scope: &KnowledgeSpaceScope,
        policy: &KnowledgeSpaceRetentionPolicy,
        now: AgentTimestampMillis,
        cursor: Option<ClaimCursor>,
    ) -> ClaimResult<KnowledgeSpaceRetentionReport> {
        discharge_knowledge_space_retention(
            self.store.as_ref(),
            scope,
            policy,
            now,
            cursor,
            self.max_batch,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_policy_retains_everything() {
        let policy = KnowledgeSpaceRetentionPolicy::default();
        assert_eq!(policy, KnowledgeSpaceRetentionPolicy::retain_all());
        for status in ClaimTrustStatus::ALL {
            assert_eq!(
                policy.window(status),
                None,
                "no built-in window deletes {} claims",
                status.as_label()
            );
        }
        assert_eq!(policy.tombstone_window(), None);
        assert!(!policy.legal_hold());
    }

    #[test]
    fn a_window_is_set_per_status() {
        let policy = KnowledgeSpaceRetentionPolicy::retain_all()
            .with_window(ClaimTrustStatus::Proposed, 5)
            .with_tombstone_window(7)
            .with_legal_hold(true);
        assert_eq!(policy.window(ClaimTrustStatus::Proposed), Some(5));
        assert_eq!(policy.window(ClaimTrustStatus::Verified), None);
        assert_eq!(policy.window(ClaimTrustStatus::Disputed), None);
        assert_eq!(policy.window(ClaimTrustStatus::Retracted), None);
        assert_eq!(policy.tombstone_window(), Some(7));
        assert!(policy.legal_hold());

        // Setting a window again replaces it; a hold can be lifted.
        let policy = policy
            .with_window(ClaimTrustStatus::Proposed, 9)
            .with_legal_hold(false);
        assert_eq!(policy.window(ClaimTrustStatus::Proposed), Some(9));
        assert!(!policy.legal_hold());
    }

    #[test]
    fn a_policy_round_trips_through_serde() {
        let policy = KnowledgeSpaceRetentionPolicy::retain_all()
            .with_window(ClaimTrustStatus::Proposed, 5)
            .with_window(ClaimTrustStatus::Retracted, u64::MAX)
            .with_tombstone_window(7)
            .with_legal_hold(true);
        let json = serde_json::to_string(&policy).expect("the policy serializes");
        let decoded: KnowledgeSpaceRetentionPolicy =
            serde_json::from_str(&json).expect("the policy deserializes");
        assert_eq!(decoded, policy);

        // Configuration written with nothing set is the retain-all policy.
        let empty: KnowledgeSpaceRetentionPolicy =
            serde_json::from_str("{}").expect("an empty policy deserializes");
        assert_eq!(empty, KnowledgeSpaceRetentionPolicy::retain_all());
    }

    #[test]
    fn a_due_instant_past_the_end_of_the_clock_never_arrives() {
        // 1 + u64::MAX overflows: the claim is not due at any representable
        // `now`, rather than due at a saturated one.
        assert!(!is_due(
            AgentTimestampMillis::new(1),
            u64::MAX,
            AgentTimestampMillis::new(u64::MAX)
        ));
        assert!(is_due(
            AgentTimestampMillis::new(0),
            u64::MAX,
            AgentTimestampMillis::new(u64::MAX)
        ));
        // Inclusive at the boundary, and never before the entry instant.
        assert!(is_due(
            AgentTimestampMillis::new(10),
            100,
            AgentTimestampMillis::new(110)
        ));
        assert!(!is_due(
            AgentTimestampMillis::new(10),
            100,
            AgentTimestampMillis::new(109)
        ));
        assert!(!is_due(
            AgentTimestampMillis::new(10),
            0,
            AgentTimestampMillis::new(9)
        ));
        assert!(is_due(
            AgentTimestampMillis::new(10),
            0,
            AgentTimestampMillis::new(10)
        ));
    }

    #[test]
    fn the_sweeps_batch_bound_is_floored_at_one() {
        let store: Arc<dyn KnowledgeGraphStore> =
            Arc::new(crate::store::InMemoryKnowledgeGraphStore::new());
        let sweep = KnowledgeSpaceRetentionSweep::new(store);
        assert_eq!(
            sweep.max_batch(),
            KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH
        );
        assert_eq!(KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH, 128);
        assert_eq!(sweep.clone().with_max_batch(0).max_batch(), 1);
        assert_eq!(sweep.with_max_batch(7).max_batch(), 7);
    }
}
