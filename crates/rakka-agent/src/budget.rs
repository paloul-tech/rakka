//! The hierarchical escrow budget ledger.
//!
//! Owns the escrow model: a parent debits an allocation inside the transition
//! that creates a child and carries it on the creation command, so a child can
//! never oversubscribe its parent. Dispatch-time reservation touches only the
//! run's own ledger and is a single-entity transition. Settlement and return
//! travel back up as deduplicated exchanges, and exhaustion parks the scope
//! with a structured top-up request rather than failing it silently.
//!
//! Both `Started` and `Indeterminate` attempts consume budget: work whose
//! outcome is unknown has still been paid for.
//!
//! Specification: section 9.7, with the continuous budget windows of 8.2. The
//! goal-scope windows are filled by slice 3.3.
//!
//! # The hierarchy, and what is escrowed
//!
//! ```text
//! definition ceiling          enforced at admission ([`crate::admission`])
//!   └─ task allocation        AgentEscrowLedger on the task entity
//!        └─ run allocation    debited in the assignment transition,
//!           │                 carried on the assignment command
//!           └─ turn/effect reservation   AgentRunBudget, single-entity
//! ```
//!
//! The dimensions split in two, and the split is the whole design:
//!
//! - **Conserved** dimensions ([`AgentBudgetDimension::CONSERVED`]) are
//!   quantities that are spent: iterations, model calls, tool calls, effects,
//!   effect attempts, tokens, and cost. Only these can be escrowed, because
//!   only these can be debited from a parent, held by a child, and returned
//!   unused. They live in [`AgentBudgetAllocation`].
//! - **Non-conserved** limits — a wall-clock deadline and a concurrent-effect
//!   ceiling — are not quantities at all. Two children running under one
//!   deadline do not each consume half of it, and a concurrency ceiling is a
//!   level rather than a total. Escrowing them would be a category error, so
//!   they are inherited and narrowed rather than debited, and live in
//!   [`AgentBudgetLimits`].
//!
//! [`AgentBudgetGrant`] is the pair: exactly what a creation command carries.
//!
//! # Why settlement and return are two commands, and ordered
//!
//! [Specification 9.7](../../../docs/plans/rakka-agent/spec.md) names a
//! settlement command and a return command, and the order between them is a
//! correctness property rather than a style choice. A parent's headroom is
//!
//! ```text
//! available = allocation - own consumption - Σ outstanding child allocations
//! ```
//!
//! Settlement adds the child's consumption to the parent's own; return drops
//! the child's allocation from the outstanding sum. Applying settlement first
//! transiently *under*-reports headroom — the consumed amount is counted both
//! inside the child's still-outstanding escrow and in the parent's
//! consumption — which is conservative and therefore safe. Applying return
//! first would release the child's whole allocation, including the part it
//! actually spent, and a settlement lost after that point would leave the
//! parent believing it has headroom that was already burned. So
//! [`AgentEscrowLedger::return_child`] refuses a return for a child that has
//! not settled, and the initiating child must send its return only once its
//! settlement is acknowledged. Each command is separately deduplicated on the
//! escrow record rather than on the exchange journal's bounded ring, so
//! replaying either credits the parent exactly once even after the ring has
//! aged out ([specification 18](../../../docs/plans/rakka-agent/spec.md)
//! scenario 61).
//!
//! # The run emits from its own terminal transition, never from a delivery
//!
//! A terminal run commits the settlement it owes into its own exchange journal
//! in the same compare-and-set that made it terminal
//! ([`crate::run`]'s `owed_ledger_exchange`), and the courier drains it — a
//! command's settle pass, a recovery sweep, a sweep — exactly as a durable
//! outbox drains. It is *not* driven from inside an `accept` of an incoming
//! exchange: the task that escrowed the run may be mid-delivery of that run's
//! own assignment, and driving a settlement back into it there would re-enter a
//! transition whose reply has not settled, recursing without bound. The run's
//! [`AgentRunSettlementStatus`](crate::AgentRunSettlementStatus) sequences the
//! settlement and the return across passivation, so the two ordered commands
//! survive a loss between them.
//!
//! # Exhaustion parks and asks; it does not fail on the spot
//!
//! A run that exhausts a ceiling does not fail immediately: it parks with the
//! structured exhaustion recorded in its loop state
//! ([`AgentPendingTopUp`](crate::AgentPendingTopUp)) and asks its parent for
//! more through a deduplicated `BudgetAllocation` exchange, committed to the
//! run's journal by the transition that hit the ceiling and drained by the same
//! courier that drains a settlement. The parent's grant is an ordinary
//! parent-local allocation decision under its own ceilings, deduplicated on the
//! child's escrow record by the request sequence. A grant that adds room in the
//! exhausted dimension resumes the run where it parked — the failing charge was
//! all-or-nothing ([`AgentRunBudget::reserve_model_turn`],
//! [`AgentRunBudget::reserve_tool_turn`]), so re-attempting it double-counts
//! nothing. A grant of nothing is the parent's honest answer when it has
//! nothing left, and the run stops with the *original* exhaustion rather than
//! parking forever. Because each grant strictly reduces the parent's headroom,
//! the asking always terminates.
//!
//! # Over-consumption is recorded, not rejected
//!
//! Tokens and cost are only knowable from the turn a provider actually billed,
//! so a final turn can bill past the allocation that admitted it. That
//! overspend is a fact, and the ledger records it: a child may settle more than
//! it held, [`AgentEscrowLedger::available`] saturates at zero, and the parent
//! simply has nothing left to grant. The enforcement lives where it can work —
//! the *next* charge refuses ([`AgentRunBudget::charge_model_call`]) — because
//! a ledger that rejected the settlement would be denying a bill that has
//! already been paid.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentTimestampMillis, StateSchemaVersion};
use serde::{Deserialize, Serialize};

use crate::definition::AgentBudgetCeilings;
use crate::identity::{validated_id, AgentIdentityError, AgentRunId};
use crate::model::AgentModelUsage;
use crate::schema::{
    AgentRecordKind, AgentSchemaError, VersionedAgentRecord,
    CURRENT_AGENT_ESCROW_LEDGER_SCHEMA_VERSION,
};

/// How many children one escrow ledger may hold outstanding at once.
///
/// An outstanding child is durable state, so the set has to be bounded like
/// every other component of an entity record. Opening one past the bound fails
/// the creating transition closed rather than dropping an escrow the parent
/// would then never reclaim. Entries are dropped as children return, so the
/// bound limits *live* children, not children over a scope's lifetime.
pub const AGENT_ESCROW_CHILD_CAPACITY: usize = 64;

/// Result type for escrow ledger operations.
pub type AgentEscrowResult<T> = Result<T, AgentEscrowError>;

/// Stable refusal code of [`AgentEscrowError::UnknownChild`]: no escrow exists
/// for the named child.
///
/// It is the one refusal that proves the ledger already answered. A child the
/// parent no longer holds is a child whose escrow was settled and returned, so
/// a settlement or return refused with this code is a completed step the
/// initiator may advance on. Every *other* refusal of a ledger exchange — an
/// `unsupported-exchange` from an owner that predates the ledger, a payload it
/// could not decode — is not the ledger answering, and the initiator must
/// leave the exchange outstanding rather than read it as done
/// ([`crate::run`]'s `check_settle`).
pub const AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN: &str = "escrow-child-unknown";

/// One dimension of the budget ledger
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The label is a bounded, low-cardinality value, so it is safe as a metric
/// label and as a stable reason code — which is exactly what a structured
/// exhaustion reason has to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentBudgetDimension {
    /// Autonomous loop iterations.
    LoopIterations,
    /// Model calls.
    ModelCalls,
    /// Tool calls.
    ToolCalls,
    /// Durable external effects committed.
    Effects,
    /// External dispatch attempts. An attempt that reached durable `Started`
    /// counts even if its outcome became `Indeterminate`
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    EffectAttempts,
    /// Model tokens.
    Tokens,
    /// Provider cost, in micro-units of currency.
    Cost,
    /// Elapsed wall-clock time against the run's deadline.
    WallClock,
    /// Concurrently dispatched effects.
    ConcurrentEffects,
}

impl AgentBudgetDimension {
    /// The dimensions that are conserved quantities, and therefore escrowable.
    ///
    /// See the module documentation: a dimension is in this set exactly when
    /// debiting it from a parent, holding it in a child, and returning the
    /// unused remainder is meaningful arithmetic.
    pub const CONSERVED: [Self; 7] = [
        Self::LoopIterations,
        Self::ModelCalls,
        Self::ToolCalls,
        Self::Effects,
        Self::EffectAttempts,
        Self::Tokens,
        Self::Cost,
    ];

    /// Whether this dimension is a conserved, escrowable quantity.
    #[must_use]
    pub const fn is_conserved(self) -> bool {
        !matches!(self, Self::WallClock | Self::ConcurrentEffects)
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::LoopIterations => "loop-iterations",
            Self::ModelCalls => "model-calls",
            Self::ToolCalls => "tool-calls",
            Self::Effects => "effects",
            Self::EffectAttempts => "effect-attempts",
            Self::Tokens => "tokens",
            Self::Cost => "cost",
            Self::WallClock => "wall-clock",
            Self::ConcurrentEffects => "concurrent-effects",
        }
    }
}

impl Display for AgentBudgetDimension {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A hard ceiling reached, with everything the policy acting on it needs
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It carries the dimension, the limit, and the consumed value — never a bare
/// message — so a run can park with a reason an operator or a top-up request can
/// act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetExhaustion {
    /// The dimension that ran out.
    pub dimension: AgentBudgetDimension,
    /// The limit that was in force.
    pub limit: u64,
    /// What had been consumed when the charge was refused.
    pub consumed: u64,
}

impl AgentBudgetExhaustion {
    /// Creates an exhaustion record.
    #[must_use]
    pub const fn new(dimension: AgentBudgetDimension, limit: u64, consumed: u64) -> Self {
        Self {
            dimension,
            limit,
            consumed,
        }
    }

    /// Stable machine-readable reason code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.dimension.as_label()
    }
}

impl Display for AgentBudgetExhaustion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the {} budget is exhausted: {} consumed of {}",
            self.dimension, self.consumed, self.limit
        )
    }
}

/// A conserved grant: what a parent debited and a child holds
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// `None` means unbounded. An unbounded parent may grant an unbounded child;
/// a bounded parent may not, which is what
/// [`AgentBudgetAllocation::narrowed_to`] enforces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetAllocation {
    /// Autonomous loop iterations granted.
    pub loop_iterations: Option<u64>,
    /// Model calls granted.
    pub model_calls: Option<u64>,
    /// Tool calls granted.
    pub tool_calls: Option<u64>,
    /// Durable effects granted.
    pub effects: Option<u64>,
    /// External dispatch attempts granted.
    pub effect_attempts: Option<u64>,
    /// Model tokens granted.
    pub tokens: Option<u64>,
    /// Provider cost granted, in micro-units of currency.
    pub cost_micros: Option<u64>,
}

impl AgentBudgetAllocation {
    /// A grant of everything.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            loop_iterations: None,
            model_calls: None,
            tool_calls: None,
            effects: None,
            effect_attempts: None,
            tokens: None,
            cost_micros: None,
        }
    }

    /// A grant of nothing.
    #[must_use]
    pub const fn nothing() -> Self {
        Self {
            loop_iterations: Some(0),
            model_calls: Some(0),
            tool_calls: Some(0),
            effects: Some(0),
            effect_attempts: Some(0),
            tokens: Some(0),
            cost_micros: Some(0),
        }
    }

    /// The conserved half of a definition's ceilings.
    ///
    /// A ceiling and an allocation are different things — a ceiling bounds, an
    /// allocation is held — but a scope with no parent to debit is allocated
    /// exactly its ceiling, which is what makes the definition ceiling the top
    /// of the hierarchy.
    #[must_use]
    pub fn from_ceilings(ceilings: &AgentBudgetCeilings) -> Self {
        Self {
            loop_iterations: ceilings.max_loop_iterations.map(u64::from),
            model_calls: ceilings.max_model_calls.map(u64::from),
            tool_calls: ceilings.max_tool_calls.map(u64::from),
            effects: ceilings.max_effects.map(u64::from),
            effect_attempts: ceilings.max_effect_attempts.map(u64::from),
            tokens: ceilings.max_tokens,
            cost_micros: ceilings.max_cost_micros,
        }
    }

    /// What this grant holds in one dimension, or `None` when it is unbounded.
    ///
    /// A dimension that is not conserved is never part of an allocation, and
    /// reads as unbounded here; [`AgentBudgetLimits`] is where it lives.
    #[must_use]
    pub const fn get(&self, dimension: AgentBudgetDimension) -> Option<u64> {
        match dimension {
            AgentBudgetDimension::LoopIterations => self.loop_iterations,
            AgentBudgetDimension::ModelCalls => self.model_calls,
            AgentBudgetDimension::ToolCalls => self.tool_calls,
            AgentBudgetDimension::Effects => self.effects,
            AgentBudgetDimension::EffectAttempts => self.effect_attempts,
            AgentBudgetDimension::Tokens => self.tokens,
            AgentBudgetDimension::Cost => self.cost_micros,
            AgentBudgetDimension::WallClock | AgentBudgetDimension::ConcurrentEffects => None,
        }
    }

    /// Sets what this grant holds in one conserved dimension.
    ///
    /// A dimension that is not conserved is ignored: it cannot be granted.
    pub const fn set(&mut self, dimension: AgentBudgetDimension, amount: Option<u64>) {
        match dimension {
            AgentBudgetDimension::LoopIterations => self.loop_iterations = amount,
            AgentBudgetDimension::ModelCalls => self.model_calls = amount,
            AgentBudgetDimension::ToolCalls => self.tool_calls = amount,
            AgentBudgetDimension::Effects => self.effects = amount,
            AgentBudgetDimension::EffectAttempts => self.effect_attempts = amount,
            AgentBudgetDimension::Tokens => self.tokens = amount,
            AgentBudgetDimension::Cost => self.cost_micros = amount,
            AgentBudgetDimension::WallClock | AgentBudgetDimension::ConcurrentEffects => {}
        }
    }

    /// This grant narrowed to `ceiling`: the smaller of the two per dimension.
    ///
    /// Unbounded is the identity, so an unbounded ceiling narrows nothing and a
    /// bounded ceiling always binds — a child can never widen its parent.
    #[must_use]
    pub fn narrowed_to(&self, ceiling: &Self) -> Self {
        let mut narrowed = *self;
        for dimension in AgentBudgetDimension::CONSERVED {
            let amount = match (self.get(dimension), ceiling.get(dimension)) {
                (None, bound) => bound,
                (held, None) => held,
                (Some(held), Some(bound)) => Some(held.min(bound)),
            };
            narrowed.set(dimension, amount);
        }
        narrowed
    }

    /// Whether this grant bounds nothing at all.
    #[must_use]
    pub fn is_unbounded(&self) -> bool {
        AgentBudgetDimension::CONSERVED
            .iter()
            .all(|dimension| self.get(*dimension).is_none())
    }

    /// The first dimension in which this grant holds nothing while `request`
    /// asked for something.
    ///
    /// This is what separates a partial grant, which a child can work with,
    /// from an empty one, which leaves it unable to take a single turn.
    #[must_use]
    pub fn first_empty_for(&self, request: &Self) -> Option<AgentBudgetDimension> {
        AgentBudgetDimension::CONSERVED
            .into_iter()
            .find(|dimension| {
                let wanted = request.get(*dimension).unwrap_or(u64::MAX);
                wanted > 0 && self.get(*dimension) == Some(0)
            })
    }

    /// This grant with `other` added, dimension by dimension.
    ///
    /// Unbounded absorbs: topping up an unbounded dimension leaves it
    /// unbounded.
    #[must_use]
    pub fn saturating_add(&self, other: &Self) -> Self {
        let mut sum = *self;
        for dimension in AgentBudgetDimension::CONSERVED {
            let amount = match (self.get(dimension), other.get(dimension)) {
                (Some(held), Some(added)) => Some(held.saturating_add(added)),
                _ => None,
            };
            sum.set(dimension, amount);
        }
        sum
    }
}

/// The limits a scope inherits rather than holds
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// See the module documentation: these are not conserved quantities, so they
/// are narrowed down the hierarchy rather than debited from it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetLimits {
    /// Maximum wall-clock duration, in milliseconds.
    pub max_wall_clock_millis: Option<u64>,
    /// Maximum concurrently dispatched effects.
    pub max_concurrent_effects: Option<u32>,
}

impl AgentBudgetLimits {
    /// Limits that bound nothing.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            max_wall_clock_millis: None,
            max_concurrent_effects: None,
        }
    }

    /// The non-conserved half of a definition's ceilings.
    #[must_use]
    pub const fn from_ceilings(ceilings: &AgentBudgetCeilings) -> Self {
        Self {
            max_wall_clock_millis: ceilings.max_wall_clock_millis,
            max_concurrent_effects: ceilings.max_concurrent_effects,
        }
    }
}

/// What a creation command carries: a conserved grant plus the limits the child
/// inherits ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetGrant {
    /// The escrowed allocation the parent debited.
    pub allocation: AgentBudgetAllocation,
    /// The non-conserved limits the child runs under.
    pub limits: AgentBudgetLimits,
}

impl AgentBudgetGrant {
    /// Creates a grant.
    #[must_use]
    pub const fn new(allocation: AgentBudgetAllocation, limits: AgentBudgetLimits) -> Self {
        Self { allocation, limits }
    }

    /// A grant of everything, bounded by nothing.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self::new(
            AgentBudgetAllocation::unbounded(),
            AgentBudgetLimits::unbounded(),
        )
    }

    /// The grant a scope with no parent to debit holds: exactly its ceilings.
    #[must_use]
    pub fn from_ceilings(ceilings: &AgentBudgetCeilings) -> Self {
        Self::new(
            AgentBudgetAllocation::from_ceilings(ceilings),
            AgentBudgetLimits::from_ceilings(ceilings),
        )
    }
}

/// What a scope actually spent, per conserved dimension
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is what a settlement command carries upward: consumption is a fact, so
/// unlike an allocation it is never unbounded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetConsumption {
    /// Autonomous loop iterations consumed.
    pub loop_iterations: u64,
    /// Model calls consumed.
    pub model_calls: u64,
    /// Tool calls consumed.
    pub tool_calls: u64,
    /// Durable effects committed.
    pub effects: u64,
    /// External dispatch attempts made.
    pub effect_attempts: u64,
    /// Model tokens consumed.
    pub tokens: u64,
    /// Provider cost consumed, in micro-units of currency.
    pub cost_micros: u64,
}

impl AgentBudgetConsumption {
    /// Consumption of nothing.
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            loop_iterations: 0,
            model_calls: 0,
            tool_calls: 0,
            effects: 0,
            effect_attempts: 0,
            tokens: 0,
            cost_micros: 0,
        }
    }

    /// What was consumed in one dimension.
    ///
    /// A dimension that is not conserved is not consumption and reads as zero.
    #[must_use]
    pub const fn get(&self, dimension: AgentBudgetDimension) -> u64 {
        match dimension {
            AgentBudgetDimension::LoopIterations => self.loop_iterations,
            AgentBudgetDimension::ModelCalls => self.model_calls,
            AgentBudgetDimension::ToolCalls => self.tool_calls,
            AgentBudgetDimension::Effects => self.effects,
            AgentBudgetDimension::EffectAttempts => self.effect_attempts,
            AgentBudgetDimension::Tokens => self.tokens,
            AgentBudgetDimension::Cost => self.cost_micros,
            AgentBudgetDimension::WallClock | AgentBudgetDimension::ConcurrentEffects => 0,
        }
    }

    /// Adds `amount` to one conserved dimension.
    pub const fn add(&mut self, dimension: AgentBudgetDimension, amount: u64) {
        match dimension {
            AgentBudgetDimension::LoopIterations => {
                self.loop_iterations = self.loop_iterations.saturating_add(amount);
            }
            AgentBudgetDimension::ModelCalls => {
                self.model_calls = self.model_calls.saturating_add(amount);
            }
            AgentBudgetDimension::ToolCalls => {
                self.tool_calls = self.tool_calls.saturating_add(amount);
            }
            AgentBudgetDimension::Effects => self.effects = self.effects.saturating_add(amount),
            AgentBudgetDimension::EffectAttempts => {
                self.effect_attempts = self.effect_attempts.saturating_add(amount);
            }
            AgentBudgetDimension::Tokens => self.tokens = self.tokens.saturating_add(amount),
            AgentBudgetDimension::Cost => {
                self.cost_micros = self.cost_micros.saturating_add(amount)
            }
            AgentBudgetDimension::WallClock | AgentBudgetDimension::ConcurrentEffects => {}
        }
    }

    /// This consumption plus `other`, dimension by dimension.
    #[must_use]
    pub fn saturating_add(&self, other: &Self) -> Self {
        let mut sum = *self;
        for dimension in AgentBudgetDimension::CONSERVED {
            sum.add(dimension, other.get(dimension));
        }
        sum
    }

    /// Whether nothing at all was consumed.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == Self::zero()
    }
}

validated_id! {
    /// A child scope one escrow ledger holds an outstanding allocation for.
    ///
    /// The key is the child's own durable identity, so the ledger's
    /// idempotence is the child's identity rather than a separate token: an
    /// allocation replayed for a child that already holds one returns the
    /// original grant instead of debiting the parent a second time
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    pub AgentEscrowChildId, "escrow child id"
}

impl AgentEscrowChildId {
    /// The escrow key of the run serving one assignment generation.
    ///
    /// A run id already carries its generation
    /// ([`crate::task::run_id_for_assignment`]), so two generations of one task
    /// are two children and can never share an escrow.
    pub fn for_run(run: &AgentRunId) -> Result<Self, AgentIdentityError> {
        Self::new(run.as_str())
    }
}

/// A parent's record of one outstanding child allocation.
///
/// It is what makes the ordering rule of the module documentation enforceable
/// after the exchange journal's deduplication window has aged out: settlement
/// and return are fenced by what this record already holds, not by a bounded
/// ring of operation ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentChildEscrow {
    allocated: AgentBudgetAllocation,
    granted_through: u64,
    last_top_up: Option<AgentBudgetAllocation>,
    settled: Option<AgentBudgetConsumption>,
}

impl AgentChildEscrow {
    /// The total this child currently holds, top-ups included.
    #[must_use]
    pub const fn allocated(&self) -> &AgentBudgetAllocation {
        &self.allocated
    }

    /// The highest top-up sequence this escrow has granted.
    #[must_use]
    pub const fn granted_through(&self) -> u64 {
        self.granted_through
    }

    /// What the child settled, once it has.
    #[must_use]
    pub const fn settled(&self) -> Option<&AgentBudgetConsumption> {
        self.settled.as_ref()
    }
}

/// The escrow ledger one scope owns
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is a component of the owning entity's own durable state, so every
/// allocation, settlement, and return commits in the same compare-and-set as
/// the domain transition that decided it. That placement is what makes
/// oversubscription structurally impossible: the ledger has exactly one writer,
/// and a grant is decided against the same record it is written to. There is no
/// distributed transaction anywhere in the hierarchy — which is precisely what
/// [specification 9.7](../../../docs/plans/rakka-agent/spec.md) requires escrow
/// to replace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEscrowLedger {
    schema_version: StateSchemaVersion,
    allocation: AgentBudgetAllocation,
    limits: AgentBudgetLimits,
    consumed: AgentBudgetConsumption,
    outstanding: BTreeMap<AgentEscrowChildId, AgentChildEscrow>,
}

impl AgentEscrowLedger {
    /// Opens a ledger holding `grant`.
    #[must_use]
    pub fn new(grant: AgentBudgetGrant) -> Self {
        Self {
            schema_version: CURRENT_AGENT_ESCROW_LEDGER_SCHEMA_VERSION,
            allocation: grant.allocation,
            limits: grant.limits,
            consumed: AgentBudgetConsumption::zero(),
            outstanding: BTreeMap::new(),
        }
    }

    /// The total this scope holds.
    #[must_use]
    pub const fn allocation(&self) -> &AgentBudgetAllocation {
        &self.allocation
    }

    /// The non-conserved limits this scope passes down.
    #[must_use]
    pub const fn limits(&self) -> &AgentBudgetLimits {
        &self.limits
    }

    /// What this scope and its settled children have consumed.
    #[must_use]
    pub const fn consumed(&self) -> &AgentBudgetConsumption {
        &self.consumed
    }

    /// The children whose allocations are still outstanding.
    pub fn outstanding(&self) -> impl Iterator<Item = (&AgentEscrowChildId, &AgentChildEscrow)> {
        self.outstanding.iter()
    }

    /// One child's escrow.
    #[must_use]
    pub fn child(&self, child: &AgentEscrowChildId) -> Option<&AgentChildEscrow> {
        self.outstanding.get(child)
    }

    /// The headroom this scope can still grant in one dimension, or `None` when
    /// it is unbounded.
    ///
    /// It saturates at zero: see the module documentation on over-consumption.
    #[must_use]
    pub fn available(&self, dimension: AgentBudgetDimension) -> Option<u64> {
        let allocation = self.allocation.get(dimension)?;
        let outstanding: u64 = self
            .outstanding
            .values()
            .map(|escrow| escrow.allocated.get(dimension).unwrap_or(0))
            .fold(0, u64::saturating_add);
        Some(
            allocation
                .saturating_sub(self.consumed.get(dimension))
                .saturating_sub(outstanding),
        )
    }

    /// Everything this scope can still grant.
    #[must_use]
    pub fn available_allocation(&self) -> AgentBudgetAllocation {
        let mut available = AgentBudgetAllocation::unbounded();
        for dimension in AgentBudgetDimension::CONSERVED {
            available.set(dimension, self.available(dimension));
        }
        available
    }

    /// Debits a child's first allocation and records the escrow.
    ///
    /// The grant is `request` narrowed to what this scope can still afford, so
    /// a child can never widen its parent. Replaying it for a child that
    /// already holds an escrow returns the original grant and debits nothing:
    /// the child's identity is the deduplication key, which is what makes an
    /// allocation command safe to re-drive
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    pub fn open_child(
        &mut self,
        child: AgentEscrowChildId,
        request: &AgentBudgetAllocation,
    ) -> AgentEscrowResult<AgentBudgetAllocation> {
        if let Some(escrow) = self.outstanding.get(&child) {
            return Ok(escrow.allocated);
        }
        if self.outstanding.len() >= AGENT_ESCROW_CHILD_CAPACITY {
            return Err(AgentEscrowError::ChildCapacity {
                capacity: AGENT_ESCROW_CHILD_CAPACITY,
            });
        }
        let allocated = request.narrowed_to(&self.available_allocation());
        self.outstanding.insert(
            child,
            AgentChildEscrow {
                allocated,
                granted_through: 0,
                last_top_up: None,
                settled: None,
            },
        );
        Ok(allocated)
    }

    /// Grants a child more of what this scope still holds
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): the grant
    /// is an ordinary parent-local allocation decision under the same
    /// ceilings).
    ///
    /// A zero grant is a legitimate answer, and the honest one when the parent
    /// has nothing left: the child then stops with its exhaustion on record
    /// rather than being told to keep waiting.
    ///
    /// `sequence` fences the replay window the journal's bounded ring cannot: a
    /// re-driven request for the sequence this escrow last granted returns that
    /// same grant without debiting again, and one for an older sequence is
    /// refused rather than granted a second time.
    pub fn top_up_child(
        &mut self,
        child: &AgentEscrowChildId,
        sequence: u64,
        request: &AgentBudgetAllocation,
    ) -> AgentEscrowResult<AgentBudgetAllocation> {
        let available = self.available_allocation();
        let escrow =
            self.outstanding
                .get_mut(child)
                .ok_or_else(|| AgentEscrowError::UnknownChild {
                    child: child.clone(),
                })?;

        if sequence <= escrow.granted_through {
            return match (sequence == escrow.granted_through, escrow.last_top_up) {
                (true, Some(granted)) => Ok(granted),
                _ => Err(AgentEscrowError::StaleTopUp {
                    child: child.clone(),
                    sequence,
                    granted_through: escrow.granted_through,
                }),
            };
        }
        if escrow.settled.is_some() {
            return Err(AgentEscrowError::AlreadySettled {
                child: child.clone(),
            });
        }

        let granted = request.narrowed_to(&available);
        escrow.allocated = escrow.allocated.saturating_add(&granted);
        escrow.granted_through = sequence;
        escrow.last_top_up = Some(granted);
        Ok(granted)
    }

    /// Records a child's terminal consumption against this scope's own.
    ///
    /// Replaying it returns the original settlement and credits nothing
    /// ([specification 18](../../../docs/plans/rakka-agent/spec.md)
    /// scenario 61), fenced by the escrow record rather than by a bounded ring.
    pub fn settle_child(
        &mut self,
        child: &AgentEscrowChildId,
        consumed: &AgentBudgetConsumption,
    ) -> AgentEscrowResult<AgentBudgetConsumption> {
        let escrow =
            self.outstanding
                .get_mut(child)
                .ok_or_else(|| AgentEscrowError::UnknownChild {
                    child: child.clone(),
                })?;
        if let Some(settled) = escrow.settled {
            return Ok(settled);
        }
        escrow.settled = Some(*consumed);
        self.consumed = self.consumed.saturating_add(consumed);
        Ok(*consumed)
    }

    /// Releases a settled child's unused allocation and closes its escrow.
    ///
    /// It refuses a child that has not settled: releasing an allocation before
    /// knowing what of it was spent would hand back headroom that was already
    /// burned (see the module documentation). Replaying it returns the same
    /// released amount and credits nothing — the escrow is gone, so a replay
    /// that outlives both the journal's window and this record is refused as an
    /// unknown child, which fails closed rather than crediting twice.
    pub fn return_child(
        &mut self,
        child: &AgentEscrowChildId,
    ) -> AgentEscrowResult<AgentBudgetAllocation> {
        let escrow = self
            .outstanding
            .get(child)
            .ok_or_else(|| AgentEscrowError::UnknownChild {
                child: child.clone(),
            })?;
        let Some(settled) = escrow.settled else {
            return Err(AgentEscrowError::NotSettled {
                child: child.clone(),
            });
        };

        let mut released = AgentBudgetAllocation::unbounded();
        for dimension in AgentBudgetDimension::CONSERVED {
            let amount = escrow
                .allocated
                .get(dimension)
                .map(|held| held.saturating_sub(settled.get(dimension)));
            released.set(dimension, amount);
        }
        self.outstanding.remove(child);
        Ok(released)
    }
}

impl VersionedAgentRecord for AgentEscrowLedger {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::EscrowLedger;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Why an escrow ledger refused an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentEscrowError {
    /// The ledger already holds as many outstanding children as it may.
    ChildCapacity {
        /// The bound in force.
        capacity: usize,
    },
    /// No escrow exists for the named child.
    UnknownChild {
        /// The child the operation named.
        child: AgentEscrowChildId,
    },
    /// The child has not settled, so its allocation cannot be released.
    NotSettled {
        /// The child the operation named.
        child: AgentEscrowChildId,
    },
    /// The child has already settled and cannot be granted more.
    AlreadySettled {
        /// The child the operation named.
        child: AgentEscrowChildId,
    },
    /// A top-up request older than the one this escrow last granted.
    StaleTopUp {
        /// The child the request named.
        child: AgentEscrowChildId,
        /// The sequence the request carried.
        sequence: u64,
        /// The sequence the escrow has already granted through.
        granted_through: u64,
    },
    /// The ledger record is not interpretable under the current schema policy.
    Schema(AgentSchemaError),
}

impl AgentEscrowError {
    /// Stable machine-readable reason code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ChildCapacity { .. } => "escrow-child-capacity",
            Self::UnknownChild { .. } => AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN,
            Self::NotSettled { .. } => "escrow-child-not-settled",
            Self::AlreadySettled { .. } => "escrow-child-already-settled",
            Self::StaleTopUp { .. } => "escrow-top-up-stale",
            Self::Schema(error) => error.code(),
        }
    }
}

impl Display for AgentEscrowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChildCapacity { capacity } => {
                write!(f, "the escrow ledger already holds {capacity} children")
            }
            Self::UnknownChild { child } => write!(f, "no escrow exists for the child {child}"),
            Self::NotSettled { child } => write!(
                f,
                "the child {child} has not settled its consumption, so its allocation cannot be released"
            ),
            Self::AlreadySettled { child } => {
                write!(f, "the child {child} has already settled and cannot be granted more")
            }
            Self::StaleTopUp {
                child,
                sequence,
                granted_through,
            } => write!(
                f,
                "the child {child} requested top-up {sequence} but its escrow has granted through {granted_through}"
            ),
            Self::Schema(error) => Display::fmt(error, f),
        }
    }
}

impl Error for AgentEscrowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentSchemaError> for AgentEscrowError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

/// The run's own durable budget ledger: the bottom of the escrow hierarchy.
///
/// It holds the allocation its parent debited and carried on the assignment,
/// and every charge against it is a single-entity transition on the run — never
/// a synchronous read of a parent scope
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). That is what
/// makes dispatch-time reservation a local decision: the parent's headroom was
/// settled when the run was created, so no dispatch has to ask about it.
///
/// Every charge is a *reservation before the work*, not an accounting of it
/// afterwards. An effect is charged when it is persisted, not when its result
/// comes back, because an effect that reaches durable dispatch has been paid
/// for whether or not its outcome is ever known. Tokens and cost are the
/// exception, and necessarily so: they are only knowable from the turn the
/// provider actually billed, so they are charged on the way in from the result,
/// and the *next* charge is what refuses to proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunBudget {
    allocation: AgentBudgetAllocation,
    limits: AgentBudgetLimits,
    consumed: AgentBudgetConsumption,
    reserved_attempts: u64,
    top_ups: u64,
    deadline: Option<AgentTimestampMillis>,
}

impl AgentRunBudget {
    /// Credits a run's ledger with the grant its assignment carried.
    ///
    /// `started_at` anchors the wall-clock dimension, so a run's deadline is
    /// fixed at the moment it durably accepted its assignment — not at the
    /// moment a pod happened to activate it, which would let a restart extend a
    /// deadline ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn allocate(grant: AgentBudgetGrant, started_at: AgentTimestampMillis) -> Self {
        let deadline = grant
            .limits
            .max_wall_clock_millis
            .map(|millis| AgentTimestampMillis::new(started_at.as_millis().saturating_add(millis)));
        Self {
            allocation: grant.allocation,
            limits: grant.limits,
            consumed: AgentBudgetConsumption::zero(),
            reserved_attempts: 0,
            top_ups: 0,
            deadline,
        }
    }

    /// The escrowed allocation this run holds.
    #[must_use]
    pub const fn allocation(&self) -> &AgentBudgetAllocation {
        &self.allocation
    }

    /// The non-conserved limits this run runs under.
    #[must_use]
    pub const fn limits(&self) -> &AgentBudgetLimits {
        &self.limits
    }

    /// What this run has consumed.
    ///
    /// This is what its settlement carries upward when it reaches a terminal
    /// outcome.
    #[must_use]
    pub const fn consumption(&self) -> &AgentBudgetConsumption {
        &self.consumed
    }

    /// Attempts reserved by effects this run has committed and not resolved.
    #[must_use]
    pub const fn reserved_attempts(&self) -> u64 {
        self.reserved_attempts
    }

    /// How many top-ups this run has been granted.
    ///
    /// It is the sequence a top-up request carries, so the parent can fence a
    /// replay that has aged out of the exchange journal's window.
    #[must_use]
    pub const fn top_ups(&self) -> u64 {
        self.top_ups
    }

    /// Loop iterations consumed.
    #[must_use]
    pub const fn loop_iterations(&self) -> u64 {
        self.consumed.loop_iterations
    }

    /// Model calls consumed.
    #[must_use]
    pub const fn model_calls(&self) -> u64 {
        self.consumed.model_calls
    }

    /// Tool calls consumed.
    #[must_use]
    pub const fn tool_calls(&self) -> u64 {
        self.consumed.tool_calls
    }

    /// Tokens consumed.
    #[must_use]
    pub const fn tokens(&self) -> u64 {
        self.consumed.tokens
    }

    /// Provider cost consumed, in micro-units of currency.
    #[must_use]
    pub const fn cost_micros(&self) -> u64 {
        self.consumed.cost_micros
    }

    /// The absolute deadline, when the run has one.
    #[must_use]
    pub const fn deadline(&self) -> Option<AgentTimestampMillis> {
        self.deadline
    }

    /// Charges one loop iteration, or reports the ceiling it would cross.
    pub fn charge_iteration(
        &mut self,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_deadline(now)?;
        self.charge(AgentBudgetDimension::LoopIterations, 1)
    }

    /// Charges one model call, or reports the ceiling it would cross.
    pub fn charge_model_call(
        &mut self,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_deadline(now)?;
        self.check(AgentBudgetDimension::Tokens)?;
        self.check(AgentBudgetDimension::Cost)?;
        self.charge(AgentBudgetDimension::ModelCalls, 1)
    }

    /// Charges one tool call, or reports the ceiling it would cross.
    pub fn charge_tool_call(
        &mut self,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_deadline(now)?;
        self.charge(AgentBudgetDimension::ToolCalls, 1)
    }

    /// Charges everything one model turn commits — a loop iteration, a model
    /// call, and one model effect — and reserves that effect's attempt bound, as
    /// a single all-or-nothing operation
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): before
    /// dispatch, atomically reserve the applicable budget or deny the
    /// operation).
    ///
    /// Every ceiling is checked before anything is charged, so a turn that
    /// cannot afford one dimension never leaves another charged. That atomicity
    /// is what makes exhaustion *parkable*: a run that parks to ask its parent
    /// for more budget re-attempts this exact charge on resume without
    /// double-counting a half that already went through. The dimensions are
    /// checked in a stable priority — deadline, concurrency, iteration, tokens,
    /// cost, model call, effect, attempts — so the reported exhaustion is
    /// deterministic.
    pub fn reserve_model_turn(
        &mut self,
        max_attempts: u32,
        outstanding: usize,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        let attempts = u64::from(max_attempts);
        self.check_deadline(now)?;
        self.check_concurrency(outstanding, 1)?;
        self.check_amount(AgentBudgetDimension::LoopIterations, 1)?;
        self.check(AgentBudgetDimension::Tokens)?;
        self.check(AgentBudgetDimension::Cost)?;
        self.check_amount(AgentBudgetDimension::ModelCalls, 1)?;
        self.check_amount(AgentBudgetDimension::Effects, 1)?;
        self.check_amount(AgentBudgetDimension::EffectAttempts, attempts)?;
        // Every ceiling cleared, so no charge can now fail.
        self.consumed.add(AgentBudgetDimension::LoopIterations, 1);
        self.consumed.add(AgentBudgetDimension::ModelCalls, 1);
        self.consumed.add(AgentBudgetDimension::Effects, 1);
        self.reserved_attempts = self.reserved_attempts.saturating_add(attempts);
        Ok(())
    }

    /// Charges everything one tool fan-out commits — one tool call and one
    /// effect per tool — and reserves the fan-out's total attempt bound, as a
    /// single all-or-nothing operation.
    ///
    /// A turn's tool calls are committed together or not at all
    /// ([specification 11] fan-out atomicity), so the reservation is too: a run
    /// that cannot afford the whole fan-out parks with none of it charged and
    /// re-attempts this exact fan-out on resume. `total_attempts` is the sum of
    /// the fan-out's per-tool attempt bounds, and `outstanding` is how many
    /// effects are already in flight, which is a level checked against the
    /// concurrency ceiling rather than debited.
    pub fn reserve_tool_turn(
        &mut self,
        count: u64,
        total_attempts: u64,
        outstanding: usize,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_deadline(now)?;
        self.check_concurrency(outstanding, count)?;
        self.check_amount(AgentBudgetDimension::ToolCalls, count)?;
        self.check_amount(AgentBudgetDimension::Effects, count)?;
        self.check_amount(AgentBudgetDimension::EffectAttempts, total_attempts)?;
        self.consumed.add(AgentBudgetDimension::ToolCalls, count);
        self.consumed.add(AgentBudgetDimension::Effects, count);
        self.reserved_attempts = self.reserved_attempts.saturating_add(total_attempts);
        Ok(())
    }

    /// Whether `count` more effects fit under the concurrency ceiling given how
    /// many are already `outstanding`.
    ///
    /// Concurrency is a *level*, not a total: it bounds how many effects run at
    /// once, so it is checked against the in-flight count rather than debited
    /// from a running sum ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    fn check_concurrency(
        &self,
        outstanding: usize,
        count: u64,
    ) -> Result<(), AgentBudgetExhaustion> {
        let Some(maximum) = self.limits.max_concurrent_effects else {
            return Ok(());
        };
        let outstanding = u64::try_from(outstanding).unwrap_or(u64::MAX);
        if outstanding.saturating_add(count) > u64::from(maximum) {
            return Err(AgentBudgetExhaustion::new(
                AgentBudgetDimension::ConcurrentEffects,
                u64::from(maximum),
                outstanding,
            ));
        }
        Ok(())
    }

    /// Reserves the attempt bound of one redispatched generation, or reports
    /// the ceiling it would cross.
    ///
    /// This is the redispatch half of the reservation discipline: a
    /// reconciliation that proves an ambiguous generation never executed
    /// authorizes a *new* generation with a fresh attempt budget
    /// ([specification 11.3](../../../docs/plans/rakka-agent/spec.md)), and
    /// that budget must be spoken for before the generation becomes
    /// dispatchable, exactly as the original turn's reservation was. Only the
    /// attempts are reserved — the durable effect was charged once at commit,
    /// and a new generation of it is not a new effect.
    pub fn reserve_attempts(&mut self, max_attempts: u32) -> Result<(), AgentBudgetExhaustion> {
        let attempts = u64::from(max_attempts);
        self.check_amount(AgentBudgetDimension::EffectAttempts, attempts)?;
        self.reserved_attempts = self.reserved_attempts.saturating_add(attempts);
        Ok(())
    }

    /// Settles one effect's reservation from its durable result
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): settle
    /// usage from the durable accepted result).
    ///
    /// `attempts_made` counts every attempt that reached durable `Started`,
    /// including one whose outcome became `Indeterminate`: ambiguity does not
    /// make an attempt free. The rest of the reservation is released back into
    /// the run's own allocation, so an effect that succeeded first try does not
    /// permanently cost the run the retries it never made.
    pub fn settle_effect(&mut self, reserved_attempts: u32, attempts_made: u32) {
        let reserved = u64::from(reserved_attempts);
        let made = u64::from(attempts_made).min(reserved);
        self.consumed
            .add(AgentBudgetDimension::EffectAttempts, made);
        self.reserved_attempts = self.reserved_attempts.saturating_sub(reserved);
    }

    /// Records what a completed model turn billed.
    ///
    /// Tokens and cost are only knowable from the turn itself, so recording them
    /// never refuses: the work is already done and already billed. The next
    /// charge is what refuses, which is why [`Self::charge_model_call`] checks
    /// both before it admits another call.
    pub fn record_usage(&mut self, usage: AgentModelUsage) {
        self.consumed
            .add(AgentBudgetDimension::Tokens, usage.total_tokens());
        self.consumed
            .add(AgentBudgetDimension::Cost, usage.cost_micros);
    }

    /// Credits a top-up the parent scope granted
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The run's allocation grows; nothing it already consumed is forgotten, so
    /// a top-up resumes the run exactly where its exhaustion parked it.
    pub fn credit(&mut self, granted: &AgentBudgetAllocation, sequence: u64) {
        self.allocation = self.allocation.saturating_add(granted);
        self.top_ups = self.top_ups.max(sequence);
    }

    /// The allocation this run holds and will not use.
    ///
    /// This is what its return carries upward once its settlement is
    /// acknowledged: everything it was granted, less everything it spent.
    #[must_use]
    pub fn unused(&self) -> AgentBudgetAllocation {
        let mut unused = AgentBudgetAllocation::unbounded();
        for dimension in AgentBudgetDimension::CONSERVED {
            let amount = self
                .allocation
                .get(dimension)
                .map(|held| held.saturating_sub(self.consumed.get(dimension)));
            unused.set(dimension, amount);
        }
        unused
    }

    /// The ceiling the ledger has already crossed, if it has crossed one.
    ///
    /// This is the recovery check: a run whose token or cost ceiling was crossed
    /// by the turn it just recorded is exhausted *now*, before it prepares
    /// another one.
    #[must_use]
    pub fn exhaustion(&self, now: AgentTimestampMillis) -> Option<AgentBudgetExhaustion> {
        self.check_deadline(now)
            .and_then(|()| self.check(AgentBudgetDimension::Tokens))
            .and_then(|()| self.check(AgentBudgetDimension::Cost))
            .err()
    }

    fn check(&self, dimension: AgentBudgetDimension) -> Result<(), AgentBudgetExhaustion> {
        self.check_amount(dimension, 1)
    }

    fn check_amount(
        &self,
        dimension: AgentBudgetDimension,
        amount: u64,
    ) -> Result<(), AgentBudgetExhaustion> {
        let Some(limit) = self.allocation.get(dimension) else {
            return Ok(());
        };
        let held = match dimension {
            // Attempts are reserved ahead of the work, so what is spoken for —
            // not only what is spent — is what the next reservation is checked
            // against. Otherwise two effects could each reserve the last
            // attempt.
            AgentBudgetDimension::EffectAttempts => self
                .consumed
                .get(dimension)
                .saturating_add(self.reserved_attempts),
            _ => self.consumed.get(dimension),
        };
        if held.saturating_add(amount) > limit {
            return Err(AgentBudgetExhaustion::new(dimension, limit, held));
        }
        Ok(())
    }

    fn charge(
        &mut self,
        dimension: AgentBudgetDimension,
        amount: u64,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_amount(dimension, amount)?;
        self.consumed.add(dimension, amount);
        Ok(())
    }

    fn check_deadline(&self, now: AgentTimestampMillis) -> Result<(), AgentBudgetExhaustion> {
        let Some(deadline) = self.deadline else {
            return Ok(());
        };
        if now.as_millis() >= deadline.as_millis() {
            // The record reports the ceiling in force and the elapsed time,
            // like every other dimension — never the absolute deadline, which
            // no consumer can compare against a configured ceiling. The
            // deadline is `started_at + limit`, so elapsed is recovered from
            // it without storing the start separately.
            let limit = self.limits.max_wall_clock_millis.unwrap_or(0);
            let elapsed = now
                .as_millis()
                .saturating_sub(deadline.as_millis())
                .saturating_add(limit);
            return Err(AgentBudgetExhaustion::new(
                AgentBudgetDimension::WallClock,
                limit,
                elapsed,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child(id: &str) -> AgentEscrowChildId {
        AgentEscrowChildId::new(id).expect("a valid escrow child id")
    }

    fn tokens(amount: u64) -> AgentBudgetAllocation {
        AgentBudgetAllocation {
            tokens: Some(amount),
            ..AgentBudgetAllocation::unbounded()
        }
    }

    fn consumed_tokens(amount: u64) -> AgentBudgetConsumption {
        AgentBudgetConsumption {
            tokens: amount,
            ..AgentBudgetConsumption::zero()
        }
    }

    fn ledger(amount: u64) -> AgentEscrowLedger {
        AgentEscrowLedger::new(AgentBudgetGrant::new(
            tokens(amount),
            AgentBudgetLimits::unbounded(),
        ))
    }

    #[test]
    fn wall_clock_exhaustion_reports_the_ceiling_and_the_elapsed_time() {
        // Like every other dimension, the structured record carries the limit
        // in force and what was consumed against it — never the absolute
        // epoch deadline, which no consumer can compare to a configured
        // ceiling.
        let grant = AgentBudgetGrant::new(
            AgentBudgetAllocation::unbounded(),
            AgentBudgetLimits {
                max_wall_clock_millis: Some(60_000),
                max_concurrent_effects: None,
            },
        );
        let started_at = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, started_at);

        budget
            .charge_iteration(AgentTimestampMillis::new(1_752_451_200_001))
            .expect("the deadline has not passed");

        let exhaustion = budget
            .charge_iteration(AgentTimestampMillis::new(1_752_451_260_001))
            .expect_err("the deadline has passed");
        assert_eq!(exhaustion.dimension, AgentBudgetDimension::WallClock);
        assert_eq!(exhaustion.limit, 60_000);
        assert_eq!(exhaustion.consumed, 60_001);
    }

    #[test]
    fn a_child_can_never_be_granted_more_than_its_parent_can_afford() {
        let mut ledger = ledger(100);

        let first = ledger
            .open_child(child("run-a"), &tokens(80))
            .expect("the parent can afford it");
        assert_eq!(first.tokens, Some(80));

        // The request is not the grant: the parent narrows it to what it still
        // holds rather than widening itself to serve it.
        let second = ledger
            .open_child(child("run-b"), &tokens(80))
            .expect("a partial grant is still a grant");
        assert_eq!(second.tokens, Some(20));
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(0));
    }

    #[test]
    fn an_unbounded_request_is_bounded_by_a_bounded_parent() {
        let mut ledger = ledger(100);
        let granted = ledger
            .open_child(child("run-a"), &AgentBudgetAllocation::unbounded())
            .expect("the parent grants what it holds");
        assert_eq!(granted.tokens, Some(100));
    }

    #[test]
    fn replaying_an_allocation_never_debits_the_parent_twice() {
        // Scenario 61's allocation half, at the ledger: the child's identity is
        // the deduplication key, so a re-driven allocation returns the original
        // grant rather than debiting again.
        let mut ledger = ledger(100);
        let first = ledger
            .open_child(child("run-a"), &tokens(60))
            .expect("the first grant");
        let replay = ledger
            .open_child(child("run-a"), &tokens(60))
            .expect("the replayed grant");

        assert_eq!(first, replay);
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(40));
    }

    #[test]
    fn replaying_a_settlement_or_a_return_never_credits_the_parent_twice() {
        let mut ledger = ledger(100);
        ledger
            .open_child(child("run-a"), &tokens(60))
            .expect("the grant");

        for _ in 0..3 {
            let settled = ledger
                .settle_child(&child("run-a"), &consumed_tokens(25))
                .expect("the settlement");
            assert_eq!(settled.tokens, 25);
        }
        // Settlement counts consumption without releasing escrow: 100 - 25
        // consumed - 60 still outstanding.
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(15));

        let released = ledger.return_child(&child("run-a")).expect("the return");
        assert_eq!(released.tokens, Some(35));
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(75));

        // The escrow is closed. A replay that outlived both the journal window
        // and the record fails closed rather than crediting a second time.
        let replay = ledger
            .return_child(&child("run-a"))
            .expect_err("the escrow is closed");
        assert_eq!(replay.code(), "escrow-child-unknown");
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(75));
    }

    #[test]
    fn a_return_before_a_settlement_is_refused() {
        // The ordering rule of the module documentation: releasing an
        // allocation before knowing what of it was spent would hand back
        // headroom that was already burned.
        let mut ledger = ledger(100);
        ledger
            .open_child(child("run-a"), &tokens(60))
            .expect("the grant");

        let refusal = ledger
            .return_child(&child("run-a"))
            .expect_err("nothing is known about what the child spent");
        assert_eq!(refusal.code(), "escrow-child-not-settled");
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(40));
    }

    #[test]
    fn a_replayed_top_up_returns_its_original_grant_and_an_older_one_is_refused() {
        let mut ledger = ledger(100);
        ledger
            .open_child(child("run-a"), &tokens(40))
            .expect("the grant");

        let first = ledger
            .top_up_child(&child("run-a"), 1, &tokens(30))
            .expect("the top-up");
        assert_eq!(first.tokens, Some(30));

        let replay = ledger
            .top_up_child(&child("run-a"), 1, &tokens(30))
            .expect("the replayed top-up");
        assert_eq!(replay, first);
        assert_eq!(
            ledger
                .child(&child("run-a"))
                .expect("the escrow")
                .allocated()
                .tokens,
            Some(70)
        );

        let stale = ledger
            .top_up_child(&child("run-a"), 0, &tokens(30))
            .expect_err("an older sequence is refused");
        assert_eq!(stale.code(), "escrow-top-up-stale");
    }

    #[test]
    fn a_parent_with_nothing_left_grants_nothing_rather_than_widening_itself() {
        let mut ledger = ledger(40);
        ledger
            .open_child(child("run-a"), &tokens(40))
            .expect("the grant");

        let granted = ledger
            .top_up_child(&child("run-a"), 1, &tokens(30))
            .expect("a zero grant is an answer");
        assert_eq!(granted.tokens, Some(0));
    }

    #[test]
    fn over_consumption_is_recorded_and_saturates_the_parents_headroom() {
        // A final turn can bill past the allocation that admitted it. The
        // overspend is a fact, so the ledger records it rather than denying a
        // bill that has already been paid.
        let mut ledger = ledger(100);
        ledger
            .open_child(child("run-a"), &tokens(60))
            .expect("the grant");
        ledger
            .settle_child(&child("run-a"), &consumed_tokens(75))
            .expect("the settlement");
        ledger.return_child(&child("run-a")).expect("the return");

        assert_eq!(ledger.consumed().tokens, 75);
        assert_eq!(ledger.available(AgentBudgetDimension::Tokens), Some(25));
    }

    #[test]
    fn an_attempt_reservation_is_released_by_what_the_attempts_did_not_use() {
        let grant = AgentBudgetGrant::new(
            AgentBudgetAllocation {
                effect_attempts: Some(4),
                ..AgentBudgetAllocation::unbounded()
            },
            AgentBudgetLimits::unbounded(),
        );
        let now = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, now);

        budget
            .reserve_tool_turn(1, 3, 0, now)
            .expect("three of four attempts");
        // The reservation is spoken for: a second effect wanting three attempts
        // cannot have the one attempt that is left.
        let exhaustion = budget
            .reserve_tool_turn(1, 3, 0, now)
            .expect_err("only one attempt is unreserved");
        assert_eq!(exhaustion.dimension, AgentBudgetDimension::EffectAttempts);

        budget.settle_effect(3, 1);
        assert_eq!(budget.consumption().effect_attempts, 1);
        assert_eq!(budget.reserved_attempts(), 0);
        // Two of the three reserved attempts went unused and are the run's
        // again.
        budget
            .reserve_tool_turn(1, 3, 0, now)
            .expect("the released attempts are spendable");
    }

    #[test]
    fn a_redispatch_reservation_is_checked_against_what_is_spoken_for() {
        // The redispatch half of the reservation discipline: a reconciliation
        // that authorizes a new generation reserves its attempt bound against
        // what is already consumed *and* reserved, exactly as a turn's
        // reservation is.
        let grant = AgentBudgetGrant::new(
            AgentBudgetAllocation {
                effect_attempts: Some(3),
                ..AgentBudgetAllocation::unbounded()
            },
            AgentBudgetLimits::unbounded(),
        );
        let now = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, now);

        budget
            .reserve_tool_turn(1, 2, 0, now)
            .expect("two of three attempts");
        budget.reserve_attempts(1).expect("the third is free");
        let exhaustion = budget
            .reserve_attempts(1)
            .expect_err("everything is spoken for");
        assert_eq!(exhaustion.dimension, AgentBudgetDimension::EffectAttempts);
        assert_eq!(budget.reserved_attempts(), 3);
    }

    #[test]
    fn an_indeterminate_attempt_still_consumes_its_attempt_budget() {
        // Scenario 52's second clause: ambiguity does not make an attempt free.
        let grant = AgentBudgetGrant::new(
            AgentBudgetAllocation {
                effect_attempts: Some(1),
                ..AgentBudgetAllocation::unbounded()
            },
            AgentBudgetLimits::unbounded(),
        );
        let now = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, now);

        budget
            .reserve_tool_turn(1, 1, 0, now)
            .expect("the only attempt");
        // The attempt reached durable `Started` and its outcome is unknown. It
        // is settled exactly as a known one would be.
        budget.settle_effect(1, 1);

        assert_eq!(budget.consumption().effect_attempts, 1);
        let exhaustion = budget
            .reserve_tool_turn(1, 1, 0, now)
            .expect_err("the attempt budget is spent");
        assert_eq!(exhaustion.dimension, AgentBudgetDimension::EffectAttempts);
    }

    #[test]
    fn a_concurrency_limit_is_checked_as_a_level_and_never_debited() {
        let grant = AgentBudgetGrant::new(
            AgentBudgetAllocation::unbounded(),
            AgentBudgetLimits {
                max_wall_clock_millis: None,
                max_concurrent_effects: Some(2),
            },
        );
        let now = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, now);

        budget
            .reserve_tool_turn(1, 1, 1, now)
            .expect("one in flight");
        let exhaustion = budget
            .reserve_tool_turn(1, 1, 2, now)
            .expect_err("two are already in flight");
        assert_eq!(
            exhaustion.dimension,
            AgentBudgetDimension::ConcurrentEffects
        );
        // The level cleared, and nothing about it was spent.
        budget
            .reserve_tool_turn(1, 1, 0, now)
            .expect("the level is a level, not a total");
    }

    #[test]
    fn a_top_up_resumes_a_run_where_its_exhaustion_parked_it() {
        let grant = AgentBudgetGrant::new(
            AgentBudgetAllocation {
                model_calls: Some(1),
                ..AgentBudgetAllocation::unbounded()
            },
            AgentBudgetLimits::unbounded(),
        );
        let now = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, now);

        budget.charge_model_call(now).expect("the only call");
        let exhaustion = budget
            .charge_model_call(now)
            .expect_err("the model-call budget is spent");
        assert_eq!(exhaustion.dimension, AgentBudgetDimension::ModelCalls);

        budget.credit(
            &AgentBudgetAllocation {
                model_calls: Some(2),
                ..AgentBudgetAllocation::nothing()
            },
            1,
        );
        assert_eq!(budget.top_ups(), 1);
        // Nothing already consumed is forgotten: the top-up resumes the run
        // where its exhaustion parked it rather than starting its ledger over.
        assert_eq!(budget.consumption().model_calls, 1);
        budget.charge_model_call(now).expect("the topped-up call");
    }

    #[test]
    fn a_runs_return_carries_what_it_held_and_did_not_spend() {
        let grant = AgentBudgetGrant::new(tokens(100), AgentBudgetLimits::unbounded());
        let now = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(grant, now);

        budget.record_usage(AgentModelUsage {
            input_tokens: 30,
            output_tokens: 10,
            cost_micros: 0,
        });

        assert_eq!(budget.consumption().tokens, 40);
        assert_eq!(budget.unused().tokens, Some(60));
    }
}
