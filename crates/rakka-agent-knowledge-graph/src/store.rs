//! The portable knowledge-graph store SPI and its in-memory reference
//! implementation.
//!
//! The SPI is database-agnostic by construction
//! ([specification 13.6](../../../docs/plans/rakka-agent/spec.md)): no vendor
//! client, connection, query language, or vendor identifier appears in any
//! public type, and changing a backend must not change claim identity,
//! provenance, trust, idempotency, or authorization semantics — the
//! [`crate::conformance`] harness is the enforcement mechanism, run unchanged
//! against every implementation.
//!
//! The five core operations — append, get, query, traverse, transition — are
//! mandatory: a backend that cannot serve them is not a conformant backend,
//! so they are not "capabilities". Only genuinely optional features are
//! reported through [`KnowledgeGraphCapabilities`]; `SemanticSearch` is
//! declared now because the specification names it, but no search method
//! ships in this slice — the refusal contract
//! (`claim-capability-unsupported`) exists from day one, and the method
//! belongs to the communal-retrieval slice.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use rakka_agent::{
    AgentDelegationId, AgentGoalId, AgentId, AgentRunId, AgentTaskId, MemoryClassification,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde::{Deserialize, Serialize};

use crate::claim::{Claim, ClaimId, ClaimNodeId, ClaimPredicate, ClaimTrustStatus};
use crate::error::{ClaimError, ClaimFuture};
use crate::promotion::{validate_promotion, ClaimPromotionPolicy};
use crate::scope::KnowledgeSpaceScope;
use crate::transition::{
    ClaimTransitionOutcome, ClaimTrustTransition, ClaimTrustTransitionRequest,
};

/// Default number of claims one query page returns.
pub const CLAIM_PAGE_DEFAULT_LIMIT: usize = 32;

/// Largest number of claims one query page may return.
pub const CLAIM_PAGE_MAX_ENTRIES: usize = 64;

/// Largest number of transitions one audit page may return.
pub const CLAIM_TRANSITION_PAGE_MAX_ENTRIES: usize = 32;

/// Largest traversal depth any request may ask for.
pub const CLAIM_TRAVERSAL_MAX_DEPTH: u32 = 4;

/// Largest number of nodes one traversal report may carry.
pub const CLAIM_TRAVERSAL_MAX_NODES: usize = 256;

/// Largest number of edge claims one traversal report may carry.
pub const CLAIM_TRAVERSAL_MAX_EDGES: usize = 1024;

/// A genuinely optional store feature a backend may support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum KnowledgeGraphCapability {
    /// Semantic (embedding-based) claim search.
    SemanticSearch,
}

impl KnowledgeGraphCapability {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::SemanticSearch => "semantic-search",
        }
    }
}

/// What one backend supports, beyond the mandatory core operations
/// ([specification 13.6](../../../docs/plans/rakka-agent/spec.md): an
/// implementation reports optional capabilities rather than forcing the core
/// API to assume every backend supports the same query features).
///
/// Declared limits let a backend advertise *tighter* bounds than the crate
/// caps; the effective limit of any request is the smaller of the request,
/// the backend's declaration, and the crate cap — always clamped, never
/// unbounded, never an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphCapabilities {
    optional: BTreeSet<KnowledgeGraphCapability>,
    max_traversal_depth: u32,
    max_page_entries: usize,
}

impl KnowledgeGraphCapabilities {
    /// The mandatory core only: no optional capability, crate-cap limits.
    #[must_use]
    pub const fn core() -> Self {
        Self {
            optional: BTreeSet::new(),
            max_traversal_depth: CLAIM_TRAVERSAL_MAX_DEPTH,
            max_page_entries: CLAIM_PAGE_MAX_ENTRIES,
        }
    }

    /// Declares one optional capability as supported.
    #[must_use]
    pub fn with_capability(mut self, capability: KnowledgeGraphCapability) -> Self {
        self.optional.insert(capability);
        self
    }

    /// Declares a tighter traversal-depth bound (clamped to the crate cap,
    /// floored at one).
    #[must_use]
    pub fn with_max_traversal_depth(mut self, depth: u32) -> Self {
        self.max_traversal_depth = depth.clamp(1, CLAIM_TRAVERSAL_MAX_DEPTH);
        self
    }

    /// Declares a tighter page bound (clamped to the crate cap, floored at
    /// one).
    #[must_use]
    pub fn with_max_page_entries(mut self, entries: usize) -> Self {
        self.max_page_entries = entries.clamp(1, CLAIM_PAGE_MAX_ENTRIES);
        self
    }

    /// Whether the backend supports an optional capability.
    #[must_use]
    pub fn supports(&self, capability: KnowledgeGraphCapability) -> bool {
        self.optional.contains(&capability)
    }

    /// The backend's declared traversal-depth bound.
    #[must_use]
    pub const fn max_traversal_depth(&self) -> u32 {
        self.max_traversal_depth
    }

    /// The backend's declared page bound.
    #[must_use]
    pub const fn max_page_entries(&self) -> usize {
        self.max_page_entries
    }
}

impl Default for KnowledgeGraphCapabilities {
    fn default() -> Self {
        Self::core()
    }
}

/// A bounded, portable claim filter: trust, classification, statement, and
/// provenance dimensions ([specification 13.6](../../../docs/plans/rakka-agent/spec.md):
/// provenance/trust filtering is a core SPI operation).
///
/// The default filter matches everything. The provenance `effect` reference is
/// deliberately not a query axis: an effect id is an audit pointer read off a
/// claim, not a dimension a caller enumerates claims by.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClaimFilter {
    trust: Option<BTreeSet<ClaimTrustStatus>>,
    classifications: Option<BTreeSet<MemoryClassification>>,
    subject: Option<ClaimNodeId>,
    predicate: Option<ClaimPredicate>,
    object_node: Option<ClaimNodeId>,
    agent: Option<AgentId>,
    goal: Option<AgentGoalId>,
    task: Option<AgentTaskId>,
    run: Option<AgentRunId>,
    delegation: Option<AgentDelegationId>,
    created_after: Option<AgentTimestampMillis>,
    created_before: Option<AgentTimestampMillis>,
    min_confidence_bps: Option<u16>,
}

impl ClaimFilter {
    /// A filter that matches every claim.
    #[must_use]
    pub fn matching_all() -> Self {
        Self::default()
    }

    /// Restricts to claims in any of the given trust states.
    #[must_use]
    pub fn with_trust(mut self, trust: BTreeSet<ClaimTrustStatus>) -> Self {
        self.trust = Some(trust);
        self
    }

    /// Restricts to claims in any of the given classifications.
    #[must_use]
    pub fn with_classifications(mut self, classifications: BTreeSet<MemoryClassification>) -> Self {
        self.classifications = Some(classifications);
        self
    }

    /// Restricts to claims about the given subject node.
    #[must_use]
    pub fn with_subject(mut self, subject: ClaimNodeId) -> Self {
        self.subject = Some(subject);
        self
    }

    /// Restricts to claims carrying the given predicate.
    #[must_use]
    pub fn with_predicate(mut self, predicate: ClaimPredicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Restricts to edge claims pointing at the given object node.
    #[must_use]
    pub fn with_object_node(mut self, object: ClaimNodeId) -> Self {
        self.object_node = Some(object);
        self
    }

    /// Restricts to claims asserted by the given agent.
    #[must_use]
    pub fn with_agent(mut self, agent: AgentId) -> Self {
        self.agent = Some(agent);
        self
    }

    /// Restricts to claims asserted in service of the given goal.
    #[must_use]
    pub fn with_goal(mut self, goal: AgentGoalId) -> Self {
        self.goal = Some(goal);
        self
    }

    /// Restricts to claims asserted in service of the given task.
    #[must_use]
    pub fn with_task(mut self, task: AgentTaskId) -> Self {
        self.task = Some(task);
        self
    }

    /// Restricts to claims asserted by the given run.
    #[must_use]
    pub fn with_run(mut self, run: AgentRunId) -> Self {
        self.run = Some(run);
        self
    }

    /// Restricts to claims asserted under the given delegation.
    #[must_use]
    pub fn with_delegation(mut self, delegation: AgentDelegationId) -> Self {
        self.delegation = Some(delegation);
        self
    }

    /// Restricts to claims created at or after the instant (inclusive).
    #[must_use]
    pub fn with_created_after(mut self, at: AgentTimestampMillis) -> Self {
        self.created_after = Some(at);
        self
    }

    /// Restricts to claims created before the instant (exclusive).
    #[must_use]
    pub fn with_created_before(mut self, at: AgentTimestampMillis) -> Self {
        self.created_before = Some(at);
        self
    }

    /// Restricts to claims asserting at least the given confidence.
    #[must_use]
    pub fn with_min_confidence_bps(mut self, confidence_bps: u16) -> Self {
        self.min_confidence_bps = Some(confidence_bps);
        self
    }

    /// Whether the filter admits a claim.
    ///
    /// The one shared predicate: every backend answers `query` with exactly
    /// this admission rule, which is what lets the conformance suite compare
    /// implementations without reading their internals.
    #[must_use]
    pub fn admits(&self, claim: &Claim) -> bool {
        if let Some(trust) = &self.trust {
            if !trust.contains(&claim.trust()) {
                return false;
            }
        }
        if let Some(classifications) = &self.classifications {
            if !classifications.contains(&claim.classification) {
                return false;
            }
        }
        if let Some(subject) = &self.subject {
            if &claim.subject != subject {
                return false;
            }
        }
        if let Some(predicate) = &self.predicate {
            if &claim.predicate != predicate {
                return false;
            }
        }
        if let Some(object) = &self.object_node {
            if claim.object.node() != Some(object) {
                return false;
            }
        }
        if let Some(agent) = &self.agent {
            if &claim.provenance.agent != agent {
                return false;
            }
        }
        if let Some(goal) = &self.goal {
            if claim.provenance.goal.as_ref() != Some(goal) {
                return false;
            }
        }
        if let Some(task) = &self.task {
            if claim.provenance.task.as_ref() != Some(task) {
                return false;
            }
        }
        if let Some(run) = &self.run {
            if claim.provenance.run.as_ref() != Some(run) {
                return false;
            }
        }
        if let Some(delegation) = &self.delegation {
            if claim.provenance.delegation.as_ref() != Some(delegation) {
                return false;
            }
        }
        if let Some(after) = self.created_after {
            if claim.created_at.as_millis() < after.as_millis() {
                return false;
            }
        }
        if let Some(before) = self.created_before {
            if claim.created_at.as_millis() >= before.as_millis() {
                return false;
            }
        }
        if let Some(minimum) = self.min_confidence_bps {
            if claim.confidence_bps < minimum {
                return false;
            }
        }
        true
    }
}

/// Bounded query position: where to resume, and how many claims to return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimCursor {
    after: Option<ClaimId>,
    limit: usize,
}

impl ClaimCursor {
    /// The first page at the default limit.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: CLAIM_PAGE_DEFAULT_LIMIT,
        }
    }

    /// Resumes after the given claim id.
    #[must_use]
    pub const fn after(claim_id: ClaimId) -> Self {
        Self {
            after: Some(claim_id),
            limit: CLAIM_PAGE_DEFAULT_LIMIT,
        }
    }

    /// Sets the page limit, clamped to `1..=`[`CLAIM_PAGE_MAX_ENTRIES`].
    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit.clamp(1, CLAIM_PAGE_MAX_ENTRIES);
        self
    }

    /// The claim id the page resumes after, when any.
    #[must_use]
    pub const fn position(&self) -> Option<&ClaimId> {
        self.after.as_ref()
    }

    /// The clamped page limit.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for ClaimCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of claims, ascending by claim id.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimPage {
    /// The admitted claims, ascending by claim id.
    pub claims: Vec<Claim>,
    /// The cursor of the next page, when more claims remain.
    pub next: Option<ClaimCursor>,
}

/// Bounded audit-listing position over one claim's transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTransitionCursor {
    after_ordinal: Option<u32>,
    limit: usize,
}

impl ClaimTransitionCursor {
    /// The first page at the maximum transition-page limit.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after_ordinal: None,
            limit: CLAIM_TRANSITION_PAGE_MAX_ENTRIES,
        }
    }

    /// Resumes after the given ordinal.
    #[must_use]
    pub const fn after_ordinal(ordinal: u32) -> Self {
        Self {
            after_ordinal: Some(ordinal),
            limit: CLAIM_TRANSITION_PAGE_MAX_ENTRIES,
        }
    }

    /// Sets the page limit, clamped to
    /// `1..=`[`CLAIM_TRANSITION_PAGE_MAX_ENTRIES`].
    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit.clamp(1, CLAIM_TRANSITION_PAGE_MAX_ENTRIES);
        self
    }

    /// The ordinal the page resumes after, when any.
    #[must_use]
    pub const fn position(&self) -> Option<u32> {
        self.after_ordinal
    }

    /// The clamped page limit.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for ClaimTransitionCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of transitions, ascending by ordinal.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTransitionPage {
    /// The transitions, ascending by ordinal.
    pub transitions: Vec<ClaimTrustTransition>,
    /// The cursor of the next page, when more transitions remain.
    pub next: Option<ClaimTransitionCursor>,
}

/// Which way edges are followed from a frontier node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimTraversalDirection {
    /// Follow edges whose subject is the frontier node.
    Outbound,
    /// Follow edges whose object is the frontier node.
    Inbound,
    /// Follow edges in both directions.
    Both,
}

/// One bounded relationship traversal
/// ([specification 13.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every dimension is clamped at construction: depth to
/// [`CLAIM_TRAVERSAL_MAX_DEPTH`], the node budget to
/// [`CLAIM_TRAVERSAL_MAX_NODES`], the edge budget to
/// [`CLAIM_TRAVERSAL_MAX_EDGES`]. Only [`ClaimObject::Node`] claims are
/// edges; by default every trust state except `Retracted` extends traversal —
/// a retracted edge is withdrawn, and following it would resurrect what the
/// retraction withdrew.
///
/// [`ClaimObject::Node`]: crate::claim::ClaimObject::Node
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTraversal {
    start: ClaimNodeId,
    direction: ClaimTraversalDirection,
    predicates: Option<BTreeSet<ClaimPredicate>>,
    trust: BTreeSet<ClaimTrustStatus>,
    depth: u32,
    node_budget: usize,
    edge_budget: usize,
}

impl ClaimTraversal {
    /// An outbound depth-1 traversal from the given node, at the full node
    /// and edge budgets.
    #[must_use]
    pub fn from_node(start: ClaimNodeId) -> Self {
        Self {
            start,
            direction: ClaimTraversalDirection::Outbound,
            predicates: None,
            trust: BTreeSet::from([
                ClaimTrustStatus::Proposed,
                ClaimTrustStatus::Verified,
                ClaimTrustStatus::Disputed,
            ]),
            depth: 1,
            node_budget: CLAIM_TRAVERSAL_MAX_NODES,
            edge_budget: CLAIM_TRAVERSAL_MAX_EDGES,
        }
    }

    /// Sets the direction edges are followed.
    #[must_use]
    pub const fn with_direction(mut self, direction: ClaimTraversalDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Restricts followed edges to the given predicates.
    #[must_use]
    pub fn with_predicates(mut self, predicates: BTreeSet<ClaimPredicate>) -> Self {
        self.predicates = Some(predicates);
        self
    }

    /// Sets the trust states an edge must hold to be followed.
    #[must_use]
    pub fn with_trust(mut self, trust: BTreeSet<ClaimTrustStatus>) -> Self {
        self.trust = trust;
        self
    }

    /// Sets the depth, clamped to `1..=`[`CLAIM_TRAVERSAL_MAX_DEPTH`].
    #[must_use]
    pub fn with_depth(mut self, depth: u32) -> Self {
        self.depth = depth.clamp(1, CLAIM_TRAVERSAL_MAX_DEPTH);
        self
    }

    /// Sets the node budget, clamped to `1..=`[`CLAIM_TRAVERSAL_MAX_NODES`].
    #[must_use]
    pub fn with_node_budget(mut self, budget: usize) -> Self {
        self.node_budget = budget.clamp(1, CLAIM_TRAVERSAL_MAX_NODES);
        self
    }

    /// Sets the edge budget, clamped to `1..=`[`CLAIM_TRAVERSAL_MAX_EDGES`].
    #[must_use]
    pub fn with_edge_budget(mut self, budget: usize) -> Self {
        self.edge_budget = budget.clamp(1, CLAIM_TRAVERSAL_MAX_EDGES);
        self
    }

    /// The node the traversal starts from.
    #[must_use]
    pub const fn start(&self) -> &ClaimNodeId {
        &self.start
    }

    /// The direction edges are followed.
    #[must_use]
    pub const fn direction(&self) -> ClaimTraversalDirection {
        self.direction
    }

    /// The predicate restriction, when any.
    #[must_use]
    pub const fn predicates(&self) -> Option<&BTreeSet<ClaimPredicate>> {
        self.predicates.as_ref()
    }

    /// The trust states an edge must hold.
    #[must_use]
    pub const fn trust(&self) -> &BTreeSet<ClaimTrustStatus> {
        &self.trust
    }

    /// The clamped depth.
    #[must_use]
    pub const fn depth(&self) -> u32 {
        self.depth
    }

    /// The clamped node budget.
    #[must_use]
    pub const fn node_budget(&self) -> usize {
        self.node_budget
    }

    /// The clamped edge budget.
    #[must_use]
    pub const fn edge_budget(&self) -> usize {
        self.edge_budget
    }
}

/// The deterministic result of one bounded traversal.
///
/// Nodes appear in breadth-first level order — the start node first, then
/// each level ascending by node id — and a node appears at all only when at
/// least one in-scope edge touches it, so an unknown start node and a
/// foreign-scope start node produce the identical empty report (scenario 18).
/// Edges appear in the order they were followed, ascending by claim id within
/// a level. Any budget or depth cut sets `truncated` — bounded and explicit,
/// never silent.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTraversalReport {
    /// The nodes reached, in breadth-first level order.
    pub nodes: Vec<ClaimNodeId>,
    /// The edge claims followed, in traversal order.
    pub edges: Vec<Claim>,
    /// Whether a depth, node, or edge budget cut the traversal short.
    pub truncated: bool,
}

/// The portable communal knowledge-graph store, scoped
/// `(TenantId, KnowledgeSpaceId)`
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md),
/// [13.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Object-safe; callers hold `Arc<dyn KnowledgeGraphStore>`.
///
/// # Scope isolation
///
/// Every read under a wrong scope answers exactly as if the addressed data
/// never existed — `None`, an empty page, an empty report — and every write
/// fails with the same `claim-not-found` an absent claim produces, so an
/// unauthorized caller learns nothing, not even existence (scenario 18).
/// Answering only for the addressed scope is the implementation's obligation:
/// a returned claim carries no tenant or space, so no layer above can re-check
/// it — the same contract clause the private-memory retriever documents.
///
/// # Idempotency
///
/// Appends and transitions are deduplicated by their operation ids. A replay
/// returns the *original* logical result — the originally stored claim, the
/// originally applied transition outcome — even when later operations have
/// moved the record since (scenario 16), and a decided promotion gate is not
/// re-evaluated on replay. An operation id reused across kinds fails
/// `claim-operation-conflict`.
pub trait KnowledgeGraphStore: Send + Sync + 'static {
    /// Stable name of the backing implementation, for diagnostics and error
    /// detail — never for dispatch decisions.
    fn backend_name(&self) -> &'static str;

    /// The backend's optional-capability report and declared limits.
    fn capabilities(&self) -> KnowledgeGraphCapabilities;

    /// Appends one born-`Proposed` claim, idempotently on its operation id.
    ///
    /// A replay returns the originally stored claim. A different claim under
    /// an already-claimed id fails `claim-already-exists`; a claim carrying
    /// any trust but `Proposed` or any transition history fails
    /// `claim-append-not-proposed` (open decision 3); a claim whose id is not
    /// the one its own operation id derives in this scope fails
    /// `claim-append-id-not-derived`.
    ///
    /// That last check is what keeps claim identity *derived* rather than
    /// merely conventional. [`Claim::new`] cannot construct a mismatched
    /// claim, but [`Claim::restore`] must accept any persisted id to load one
    /// at all, so the door re-derives: without it a caller could squat the id
    /// another writer's operation will derive and deny that append forever.
    ///
    /// [`Claim::restore`]: crate::claim::Claim::restore
    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim>;

    /// Reads one claim; a wrong scope answers `None`, exactly like absent.
    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>>;

    /// Bounded filtered lookup, ascending by claim id; a wrong scope answers
    /// the empty page, byte-identical to querying an empty space.
    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage>;

    /// Bounded breadth-first traversal over edge claims; a wrong scope or an
    /// unknown start node answers the identical empty report.
    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport>;

    /// Applies one append-only trust transition, idempotently on the
    /// request's operation id.
    ///
    /// The source status is derived from the claim's current durable state;
    /// the legal table and the bounded history are enforced through
    /// [`Claim::apply_transition`], and a `Verified` target consults `policy`
    /// through [`validate_promotion`] — fail closed, before anything is
    /// written. `now` exists solely for grant-expiry evaluation.
    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome>;

    /// Ordinal-ordered audit listing of one claim's transitions; a wrong
    /// scope or an unknown claim answers the empty page, byte-identical to a
    /// claim with no transitions.
    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage>;
}

/// What one spent operation id resolved to.
#[derive(Debug, Clone)]
enum ClaimLedgerEntry {
    /// The claim an append stored, exactly as stored.
    AppliedAppend(Box<Claim>),
    /// The outcome a transition produced, exactly as produced.
    AppliedTransition(Box<ClaimTransitionOutcome>),
}

#[derive(Debug, Default)]
struct Inner {
    /// `scope key -> claim id -> claim`, current trust denormalized on.
    claims: BTreeMap<String, BTreeMap<String, Claim>>,
    /// `scope key -> claim id -> ordinal-ordered transitions`.
    transitions: BTreeMap<String, BTreeMap<String, Vec<ClaimTrustTransition>>>,
    /// `scope key -> operation id -> original result` — the replay ledger.
    operations: BTreeMap<String, BTreeMap<String, ClaimLedgerEntry>>,
}

/// The in-memory reference implementation of [`KnowledgeGraphStore`].
///
/// The reference for semantics, not durability
/// ([specification 19](../../../docs/plans/rakka-agent/spec.md): production
/// durability claims are never based on the in-memory implementation). Every
/// scope's data lives under its injective key, so cross-scope reads are
/// structurally empty rather than filtered.
#[derive(Debug, Clone, Default)]
pub struct InMemoryKnowledgeGraphStore {
    inner: Arc<Mutex<Inner>>,
}

impl InMemoryKnowledgeGraphStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of claims held in one scope, for tests and diagnostics.
    #[must_use]
    pub fn len(&self, scope: &KnowledgeSpaceScope) -> usize {
        self.inner
            .lock()
            .expect("the in-memory graph lock is never poisoned")
            .claims
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one scope holds no claims.
    #[must_use]
    pub fn is_empty(&self, scope: &KnowledgeSpaceScope) -> bool {
        self.len(scope) == 0
    }
}

impl KnowledgeGraphStore for InMemoryKnowledgeGraphStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core()
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        Box::pin(async move {
            claim.validate()?;
            if claim.trust() != ClaimTrustStatus::Proposed || claim.transition_count() != 0 {
                return Err(ClaimError::AppendNotProposed {
                    claim_id: claim.claim_id.clone(),
                });
            }
            // Re-derive the identity: a restored record can carry any id, and
            // an id that is not this operation's is a squat on someone else's.
            let derived = ClaimId::derive_appended(scope, &claim.operation_id)?;
            if claim.claim_id != derived {
                return Err(ClaimError::AppendIdNotDerived {
                    claim_id: claim.claim_id.clone(),
                    derived,
                });
            }
            let key = scope.key();
            let mut inner = self
                .inner
                .lock()
                .expect("the in-memory graph lock is never poisoned");
            if let Some(entry) = inner
                .operations
                .get(&key)
                .and_then(|operations| operations.get(claim.operation_id.as_str()))
            {
                return match entry {
                    ClaimLedgerEntry::AppliedAppend(original) => Ok(original.as_ref().clone()),
                    ClaimLedgerEntry::AppliedTransition(_) => Err(ClaimError::OperationConflict {
                        operation_id: claim.operation_id.clone(),
                    }),
                };
            }
            if inner
                .claims
                .get(&key)
                .is_some_and(|claims| claims.contains_key(claim.claim_id.as_str()))
            {
                return Err(ClaimError::AlreadyExists {
                    claim_id: claim.claim_id.clone(),
                });
            }
            inner
                .claims
                .entry(key.clone())
                .or_default()
                .insert(claim.claim_id.as_str().to_string(), claim.clone());
            inner.operations.entry(key).or_default().insert(
                claim.operation_id.as_str().to_string(),
                ClaimLedgerEntry::AppliedAppend(Box::new(claim.clone())),
            );
            Ok(claim.clone())
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        Box::pin(async move {
            Ok(self
                .inner
                .lock()
                .expect("the in-memory graph lock is never poisoned")
                .claims
                .get(&scope.key())
                .and_then(|claims| claims.get(claim_id.as_str()))
                .cloned())
        })
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .expect("the in-memory graph lock is never poisoned");
            let Some(claims) = inner.claims.get(&scope.key()) else {
                return Ok(ClaimPage {
                    claims: Vec::new(),
                    next: None,
                });
            };
            let after = cursor.position().map(|id| id.as_str().to_string());
            let mut page = Vec::new();
            let mut next = None;
            for (id, claim) in claims {
                if let Some(after) = &after {
                    if id.as_str() <= after.as_str() {
                        continue;
                    }
                }
                if !filter.admits(claim) {
                    continue;
                }
                if page.len() == cursor.limit() {
                    next = Some(ClaimCursor::after(claim.claim_id.clone()));
                    break;
                }
                page.push(claim.clone());
            }
            // The next cursor resumes after the last returned claim, at the
            // same limit.
            let next = next.map(|_| {
                let last: &Claim = page.last().expect("a full page holds its limit");
                ClaimCursor::after(last.claim_id.clone()).with_limit(cursor.limit())
            });
            Ok(ClaimPage { claims: page, next })
        })
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .expect("the in-memory graph lock is never poisoned");
            let Some(claims) = inner.claims.get(&scope.key()) else {
                return Ok(ClaimTraversalReport {
                    nodes: Vec::new(),
                    edges: Vec::new(),
                    truncated: false,
                });
            };

            // Edge admission: Node objects only, trust and predicate filtered.
            let follows = |claim: &Claim| -> bool {
                claim.object.node().is_some()
                    && traversal.trust().contains(&claim.trust())
                    && traversal
                        .predicates()
                        .is_none_or(|predicates| predicates.contains(&claim.predicate))
            };
            // The neighbors an edge yields for a frontier node, per direction.
            let neighbor = |claim: &Claim, node: &ClaimNodeId| -> Option<ClaimNodeId> {
                let object = claim.object.node()?;
                match traversal.direction() {
                    ClaimTraversalDirection::Outbound if &claim.subject == node => {
                        Some(object.clone())
                    }
                    ClaimTraversalDirection::Inbound if object == node => {
                        Some(claim.subject.clone())
                    }
                    ClaimTraversalDirection::Both if &claim.subject == node => Some(object.clone()),
                    ClaimTraversalDirection::Both if object == node => Some(claim.subject.clone()),
                    _ => None,
                }
            };

            let mut report = ClaimTraversalReport {
                nodes: Vec::new(),
                edges: Vec::new(),
                truncated: false,
            };
            let mut visited: BTreeSet<ClaimNodeId> = BTreeSet::new();
            let mut spent_edges: BTreeSet<String> = BTreeSet::new();
            let mut frontier: VecDeque<ClaimNodeId> = VecDeque::new();

            // The start node enters the report only when at least one
            // in-scope edge touches it, so an unknown and a foreign-scope
            // start are indistinguishable (scenario 18).
            let start_touched = claims
                .values()
                .any(|claim| follows(claim) && neighbor(claim, traversal.start()).is_some());
            if !start_touched {
                return Ok(report);
            }
            report.nodes.push(traversal.start().clone());
            visited.insert(traversal.start().clone());
            frontier.push_back(traversal.start().clone());

            for _ in 0..traversal.depth() {
                if frontier.is_empty() {
                    break;
                }
                // Claims iterate in ascending claim-id order, which is what
                // makes the level's edge order and the next level's node
                // order deterministic.
                let mut next_level: BTreeSet<ClaimNodeId> = BTreeSet::new();
                for node in std::mem::take(&mut frontier) {
                    for claim in claims.values() {
                        if !follows(claim) || spent_edges.contains(claim.claim_id.as_str()) {
                            continue;
                        }
                        let Some(reached) = neighbor(claim, &node) else {
                            continue;
                        };
                        if report.edges.len() == traversal.edge_budget() {
                            report.truncated = true;
                            return Ok(report);
                        }
                        spent_edges.insert(claim.claim_id.as_str().to_string());
                        report.edges.push(claim.clone());
                        if !visited.contains(&reached) {
                            next_level.insert(reached);
                        }
                    }
                }
                for reached in next_level {
                    if report.nodes.len() == traversal.node_budget() {
                        report.truncated = true;
                        return Ok(report);
                    }
                    report.nodes.push(reached.clone());
                    visited.insert(reached.clone());
                    frontier.push_back(reached);
                }
            }
            // Depth exhausted with reachable work left is a cut, not an end.
            if !frontier.is_empty() {
                let more = frontier.iter().any(|node| {
                    claims.values().any(|claim| {
                        follows(claim)
                            && !spent_edges.contains(claim.claim_id.as_str())
                            && neighbor(claim, node).is_some()
                    })
                });
                if more {
                    report.truncated = true;
                }
            }
            Ok(report)
        })
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        Box::pin(async move {
            let key = scope.key();
            let mut inner = self
                .inner
                .lock()
                .expect("the in-memory graph lock is never poisoned");
            if let Some(entry) = inner
                .operations
                .get(&key)
                .and_then(|operations| operations.get(request.operation_id.as_str()))
            {
                return match entry {
                    // A replay answers the original outcome without
                    // re-running legality or the gate: a decided promotion is
                    // not re-litigated, even by a grant that has since
                    // expired.
                    ClaimLedgerEntry::AppliedTransition(original) => Ok(original.as_ref().clone()),
                    ClaimLedgerEntry::AppliedAppend(_) => Err(ClaimError::OperationConflict {
                        operation_id: request.operation_id.clone(),
                    }),
                };
            }
            let Some(claim) = inner
                .claims
                .get(&key)
                .and_then(|claims| claims.get(request.claim_id.as_str()))
                .cloned()
            else {
                // Absent and out-of-scope are the same refusal (scenario 18).
                return Err(ClaimError::NotFound {
                    claim_id: request.claim_id.clone(),
                });
            };

            let updated = claim.apply_transition(request.to)?;
            let gate = if request.to == ClaimTrustStatus::Verified {
                validate_promotion(scope, &claim, policy, request.promotion.as_deref(), now)?
            } else {
                None
            };

            let mut transition = ClaimTrustTransition::new(
                claim.claim_id.clone(),
                request.operation_id.clone(),
                updated.transition_count(),
                claim.trust(),
                request.to,
                request.actor.clone(),
                request.occurred_at,
            )?;
            if let Some(provenance) = &request.provenance {
                transition = transition.with_provenance(provenance.clone())?;
            }
            if let Some(reason) = &request.reason {
                transition = transition.with_reason(reason.clone())?;
            }
            if !request.evidence.is_empty() {
                transition = transition.with_evidence(request.evidence.clone())?;
            }
            if let Some(policy_ref) = &request.policy {
                transition = transition.with_policy(policy_ref.clone());
            }
            if let Some(receipt) = gate {
                transition = transition.with_gate(receipt);
            }

            let outcome = ClaimTransitionOutcome {
                claim: updated.clone(),
                transition: transition.clone(),
            };
            inner
                .claims
                .entry(key.clone())
                .or_default()
                .insert(updated.claim_id.as_str().to_string(), updated);
            inner
                .transitions
                .entry(key.clone())
                .or_default()
                .entry(transition.claim_id.as_str().to_string())
                .or_default()
                .push(transition);
            inner.operations.entry(key).or_default().insert(
                request.operation_id.as_str().to_string(),
                ClaimLedgerEntry::AppliedTransition(Box::new(outcome.clone())),
            );
            Ok(outcome)
        })
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .expect("the in-memory graph lock is never poisoned");
            let history = inner
                .transitions
                .get(&scope.key())
                .and_then(|transitions| transitions.get(claim_id.as_str()));
            let Some(history) = history else {
                return Ok(ClaimTransitionPage {
                    transitions: Vec::new(),
                    next: None,
                });
            };
            let after = cursor.position().unwrap_or(0);
            let mut page = Vec::new();
            let mut more = false;
            for transition in history {
                if transition.ordinal <= after {
                    continue;
                }
                if page.len() == cursor.limit() {
                    more = true;
                    break;
                }
                page.push(transition.clone());
            }
            let next = if more {
                let last: &ClaimTrustTransition = page.last().expect("a full page holds its limit");
                Some(ClaimTransitionCursor::after_ordinal(last.ordinal).with_limit(cursor.limit()))
            } else {
                None
            };
            Ok(ClaimTransitionPage {
                transitions: page,
                next,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent::{AgentId, KnowledgeSpaceId, MemoryClassification, TenantId};
    use rakka_agent_workflow::PrincipalRef;

    use crate::claim::{ClaimObject, ClaimOperationId, ClaimProvenance};

    use super::*;

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            TenantId::new("acme"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    fn actor() -> PrincipalRef {
        PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "reviewer".to_string(),
            display_name: None,
        }
    }

    fn edge(scope: &KnowledgeSpaceScope, discriminator: &str, from: &str, to: &str) -> Claim {
        let operation_id = ClaimOperationId::derive_append(scope, discriminator)
            .expect("the operation id derives");
        Claim::new(
            scope,
            operation_id,
            ClaimNodeId::new(from).expect("the node id is valid"),
            ClaimPredicate::new("links").expect("the predicate is valid"),
            ClaimObject::Node(ClaimNodeId::new(to).expect("the node id is valid")),
            ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
            5_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid")
    }

    #[test]
    fn cursors_and_traversals_clamp_their_limits() {
        assert_eq!(ClaimCursor::start().with_limit(0).limit(), 1);
        assert_eq!(
            ClaimCursor::start()
                .with_limit(CLAIM_PAGE_MAX_ENTRIES + 100)
                .limit(),
            CLAIM_PAGE_MAX_ENTRIES
        );
        assert_eq!(ClaimTransitionCursor::start().with_limit(0).limit(), 1);

        let traversal =
            ClaimTraversal::from_node(ClaimNodeId::new("n").expect("the node id is valid"))
                .with_depth(0)
                .with_node_budget(0)
                .with_edge_budget(usize::MAX);
        assert_eq!(traversal.depth(), 1);
        assert_eq!(traversal.node_budget(), 1);
        assert_eq!(traversal.edge_budget(), CLAIM_TRAVERSAL_MAX_EDGES);

        let capabilities = KnowledgeGraphCapabilities::core()
            .with_max_traversal_depth(100)
            .with_max_page_entries(0);
        assert_eq!(
            capabilities.max_traversal_depth(),
            CLAIM_TRAVERSAL_MAX_DEPTH
        );
        assert_eq!(capabilities.max_page_entries(), 1);
        assert!(!capabilities.supports(KnowledgeGraphCapability::SemanticSearch));
    }

    #[tokio::test]
    async fn traversal_is_deterministic_and_direction_aware() {
        let store = InMemoryKnowledgeGraphStore::new();
        let scope = scope();
        // a -> b, a -> c, b -> d; plus d -> a to close a cycle.
        for (discriminator, from, to) in [
            ("e1", "a", "b"),
            ("e2", "a", "c"),
            ("e3", "b", "d"),
            ("e4", "d", "a"),
        ] {
            store
                .append(&scope, &edge(&scope, discriminator, from, to))
                .await
                .expect("the edge appends");
        }

        let traversal =
            ClaimTraversal::from_node(ClaimNodeId::new("a").expect("the node id is valid"))
                .with_depth(3);
        let first = store.traverse(&scope, &traversal).await.expect("traverses");
        let second = store.traverse(&scope, &traversal).await.expect("traverses");
        assert_eq!(first, second);
        // Level order: a, then {b, c} ascending, then d. The cycle edge is
        // followed once and revisits no node.
        let names: Vec<&str> = first.nodes.iter().map(ClaimNodeId::as_str).collect();
        assert_eq!(names, ["a", "b", "c", "d"]);
        assert_eq!(first.edges.len(), 4);
        assert!(!first.truncated);

        let inbound =
            ClaimTraversal::from_node(ClaimNodeId::new("d").expect("the node id is valid"))
                .with_direction(ClaimTraversalDirection::Inbound)
                .with_depth(2);
        let report = store.traverse(&scope, &inbound).await.expect("traverses");
        let names: Vec<&str> = report.nodes.iter().map(ClaimNodeId::as_str).collect();
        assert_eq!(names, ["d", "b", "a"]);

        // A depth cut is explicit.
        let shallow = store
            .traverse(
                &scope,
                &ClaimTraversal::from_node(ClaimNodeId::new("a").expect("the node id is valid")),
            )
            .await
            .expect("traverses");
        assert!(shallow.truncated);
        // An edge-budget cut is explicit.
        let starved = store
            .traverse(
                &scope,
                &ClaimTraversal::from_node(ClaimNodeId::new("a").expect("the node id is valid"))
                    .with_depth(3)
                    .with_edge_budget(1),
            )
            .await
            .expect("traverses");
        assert!(starved.truncated);
        assert_eq!(starved.edges.len(), 1);
    }

    #[tokio::test]
    async fn a_transition_stamps_ordinals_and_lists_in_order() {
        let store = InMemoryKnowledgeGraphStore::new();
        let scope = scope();
        let claim = edge(&scope, "e1", "a", "b");
        store
            .append(&scope, &claim)
            .await
            .expect("the claim appends");

        for (index, to) in [ClaimTrustStatus::Disputed, ClaimTrustStatus::Retracted]
            .into_iter()
            .enumerate()
        {
            let operation_id =
                ClaimOperationId::derive_transition(&scope, &claim.claim_id, format!("t-{index}"))
                    .expect("the operation id derives");
            let outcome = store
                .transition(
                    &scope,
                    &ClaimTrustTransitionRequest::new(
                        claim.claim_id.clone(),
                        operation_id,
                        to,
                        actor(),
                        AgentTimestampMillis::new(10 + index as u64),
                    ),
                    &ClaimPromotionPolicy::default(),
                    AgentTimestampMillis::new(10),
                )
                .await
                .expect("the transition applies");
            assert_eq!(outcome.transition.ordinal, index as u32 + 1);
        }

        let page = store
            .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
            .await
            .expect("the history lists");
        assert_eq!(page.transitions.len(), 2);
        assert_eq!(page.transitions[0].ordinal, 1);
        assert_eq!(page.transitions[1].ordinal, 2);
        assert!(page.next.is_none());
    }
}
