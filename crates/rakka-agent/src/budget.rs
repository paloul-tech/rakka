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
//! escrow *hierarchy* — parent-local allocation, settlement, and return, each a
//! deduplicated exchange — is filled by slice 1.9, and goal-scope windows by
//! slice 3.3.
//!
//! # The run's own ledger, landed by slice 1.5
//!
//! [`AgentRunBudget`] is the bottom of that hierarchy: the remaining loop,
//! model-call, tool-call, token, cost, and deadline budgets that
//! [specification 9.4](../../../docs/plans/rakka-agent/spec.md) requires the
//! durable loop state to carry. It is deliberately the run's *own* record, and
//! every charge against it is a single-entity transition on the run — never a
//! synchronous read of a parent scope
//! ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
//!
//! Exhaustion is a *structured* stop, not a failure code: an
//! [`AgentBudgetExhaustion`] names the scope, the dimension, the limit, and what
//! was consumed, so the policy that acts on it — park, escalate, request a
//! top-up, fail — is deciding on facts rather than on a string. Slice 1.9 adds
//! the top-up exchange; until it does, an exhausted run stops with the reason on
//! record, which is what makes the later exchange a pure addition.

use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::AgentTimestampMillis;
use serde::{Deserialize, Serialize};

use crate::definition::AgentBudgetCeilings;
use crate::model::AgentModelUsage;

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
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::LoopIterations => "loop-iterations",
            Self::ModelCalls => "model-calls",
            Self::ToolCalls => "tool-calls",
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

/// The run's own durable budget ledger.
///
/// It is allocated once, from the ceilings the task definition carried on the
/// assignment, and charged by the run's own bounded transitions. A dimension the
/// definition left unbounded stays unbounded here: an absent ceiling is not a
/// ceiling of zero.
///
/// Every charge is a *reservation before the work*, not an accounting of it
/// afterwards. A model call is charged when its effect is persisted, not when
/// its result comes back, because an effect that reaches durable dispatch has
/// been paid for whether or not its outcome is ever known
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). Tokens and
/// cost are the exception, and necessarily so: they are only knowable from the
/// turn the provider actually billed, so they are charged on the way in from the
/// result, and the *next* charge is what refuses to proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunBudget {
    ceilings: AgentBudgetCeilings,
    loop_iterations: u32,
    model_calls: u32,
    tool_calls: u32,
    tokens: u64,
    cost_micros: u64,
    deadline: Option<AgentTimestampMillis>,
}

impl AgentRunBudget {
    /// Allocates a run's ledger from the ceilings it was assigned under.
    ///
    /// `started_at` anchors the wall-clock dimension, so a run's deadline is
    /// fixed at the moment it durably accepted its assignment — not at the
    /// moment a pod happened to activate it, which would let a restart extend a
    /// deadline ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn allocate(ceilings: AgentBudgetCeilings, started_at: AgentTimestampMillis) -> Self {
        let deadline = ceilings
            .max_wall_clock_millis
            .map(|millis| AgentTimestampMillis::new(started_at.as_millis().saturating_add(millis)));
        Self {
            ceilings,
            loop_iterations: 0,
            model_calls: 0,
            tool_calls: 0,
            tokens: 0,
            cost_micros: 0,
            deadline,
        }
    }

    /// The ceilings this ledger was allocated under.
    #[must_use]
    pub const fn ceilings(&self) -> &AgentBudgetCeilings {
        &self.ceilings
    }

    /// Loop iterations consumed.
    #[must_use]
    pub const fn loop_iterations(&self) -> u32 {
        self.loop_iterations
    }

    /// Model calls consumed.
    #[must_use]
    pub const fn model_calls(&self) -> u32 {
        self.model_calls
    }

    /// Tool calls consumed.
    #[must_use]
    pub const fn tool_calls(&self) -> u32 {
        self.tool_calls
    }

    /// Tokens consumed.
    #[must_use]
    pub const fn tokens(&self) -> u64 {
        self.tokens
    }

    /// Provider cost consumed, in micro-units of currency.
    #[must_use]
    pub const fn cost_micros(&self) -> u64 {
        self.cost_micros
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
        charge_u32(
            &mut self.loop_iterations,
            self.ceilings.max_loop_iterations,
            AgentBudgetDimension::LoopIterations,
        )
    }

    /// Charges one model call, or reports the ceiling it would cross.
    pub fn charge_model_call(
        &mut self,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_deadline(now)?;
        self.check_tokens()?;
        self.check_cost()?;
        charge_u32(
            &mut self.model_calls,
            self.ceilings.max_model_calls,
            AgentBudgetDimension::ModelCalls,
        )
    }

    /// Charges one tool call, or reports the ceiling it would cross.
    pub fn charge_tool_call(
        &mut self,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetExhaustion> {
        self.check_deadline(now)?;
        charge_u32(
            &mut self.tool_calls,
            self.ceilings.max_tool_calls,
            AgentBudgetDimension::ToolCalls,
        )
    }

    /// Records what a completed model turn billed.
    ///
    /// Tokens and cost are only knowable from the turn itself, so recording them
    /// never refuses: the work is already done and already billed. The next
    /// charge is what refuses, which is why [`Self::charge_model_call`] checks
    /// both before it admits another call.
    pub fn record_usage(&mut self, usage: AgentModelUsage) {
        self.tokens = self.tokens.saturating_add(usage.total_tokens());
        self.cost_micros = self.cost_micros.saturating_add(usage.cost_micros);
    }

    /// The ceiling the ledger has already crossed, if it has crossed one.
    ///
    /// This is the recovery check: a run whose token or cost ceiling was crossed
    /// by the turn it just recorded is exhausted *now*, before it prepares
    /// another one.
    #[must_use]
    pub fn exhaustion(&self, now: AgentTimestampMillis) -> Option<AgentBudgetExhaustion> {
        self.check_deadline(now)
            .and_then(|()| self.check_tokens())
            .and_then(|()| self.check_cost())
            .err()
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
            let limit = self.ceilings.max_wall_clock_millis.unwrap_or(0);
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

    fn check_tokens(&self) -> Result<(), AgentBudgetExhaustion> {
        let Some(maximum) = self.ceilings.max_tokens else {
            return Ok(());
        };
        if self.tokens >= maximum {
            return Err(AgentBudgetExhaustion::new(
                AgentBudgetDimension::Tokens,
                maximum,
                self.tokens,
            ));
        }
        Ok(())
    }

    fn check_cost(&self) -> Result<(), AgentBudgetExhaustion> {
        let Some(maximum) = self.ceilings.max_cost_micros else {
            return Ok(());
        };
        if self.cost_micros >= maximum {
            return Err(AgentBudgetExhaustion::new(
                AgentBudgetDimension::Cost,
                maximum,
                self.cost_micros,
            ));
        }
        Ok(())
    }
}

fn charge_u32(
    consumed: &mut u32,
    ceiling: Option<u32>,
    dimension: AgentBudgetDimension,
) -> Result<(), AgentBudgetExhaustion> {
    if let Some(maximum) = ceiling {
        if *consumed >= maximum {
            return Err(AgentBudgetExhaustion::new(
                dimension,
                u64::from(maximum),
                u64::from(*consumed),
            ));
        }
    }
    *consumed = consumed.saturating_add(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_clock_exhaustion_reports_the_ceiling_and_the_elapsed_time() {
        // Like every other dimension, the structured record carries the limit
        // in force and what was consumed against it — never the absolute
        // epoch deadline, which no consumer can compare to a configured
        // ceiling.
        let ceilings = AgentBudgetCeilings {
            max_wall_clock_millis: Some(60_000),
            ..AgentBudgetCeilings::unbounded()
        };
        let started_at = AgentTimestampMillis::new(1_752_451_200_000);
        let mut budget = AgentRunBudget::allocate(ceilings, started_at);

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
}
