//! What an exhausted outbox row says its run is owed (issue #134).
//!
//! A dispatch worker spends a ticket's budget by writing the outbox row
//! `Exhausted`, and tells the run afterwards. A worker lost between the two
//! leaves a row that is finished and a run that was never told. The row's
//! last failure line is the line of the write that exhausted it, and each
//! site that exhausts a row writes a line of a known shape, so the line says
//! which outcome the lost worker was about to deliver.
//!
//! The lines live here, beside the function that reads them back, so the
//! sites that write a line and the reader cannot drift apart.

/// The code of a generation whose budget was spent on attempts whose worker
/// was lost after `Started`.
pub(crate) const WORKER_LOST_CODE: &str = "dispatcher-lost-after-started";

/// The failure line a burned attempt's outbox row keeps when its worker was
/// lost after `Started`.
pub(crate) const WORKER_LOST_LINE: &str = "dispatcher lost after started";

/// The detail of the `Exhausted` outcome of a budget spent recovering
/// ambiguous attempts.
pub(crate) const AMBIGUOUS_BUDGET_SPENT_MESSAGE: &str =
    "the retry budget was spent recovering ambiguous attempts";

/// The detail carried by an `Indeterminate` outcome parked for an ambiguity
/// the pipeline could not resolve.
pub(crate) const INDETERMINATE_OUTCOME_MESSAGE: &str =
    "the attempt's outcome could not be established; an explicit reconciliation decision is owed";

/// The code of a ticket whose outbox row was finished and whose run was
/// told from the row: an `Exhausted` row whose line names no code, and a
/// `Dispatched` or `Cancelled` row whose result reached nobody.
pub(crate) const TICKET_ROW_SETTLED: &str = "ticket-row-settled";

/// A reconciliation finding that establishes nothing about its intent: the
/// line its burned attempt's row keeps, and the codes its two parks carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnknownFinding {
    /// The failure line the burned attempt's outbox row keeps.
    pub(crate) line: &'static str,
    /// The code of the park of a run that is winding down.
    pub(crate) winding_down_code: &'static str,
    /// The code of the park of a spent budget.
    pub(crate) exhausted_code: &'static str,
}

/// The reconciler answered that it does not know.
pub(crate) const RECONCILIATION_UNKNOWN: UnknownFinding = UnknownFinding {
    line: "reconciliation outcome unknown",
    winding_down_code: "reconciliation-unknown",
    exhausted_code: "reconciliation-exhausted",
};

/// The reconciler answered with an outcome that does not belong to the
/// effect.
pub(crate) const OUTCOME_MISMATCH: UnknownFinding = UnknownFinding {
    line: "effect-outcome-mismatch: the reconciled outcome does not fit the effect",
    winding_down_code: "effect-outcome-mismatch",
    exhausted_code: "effect-outcome-mismatch",
};

/// What the worker that exhausted a row was about to tell its run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwedByExhaustedRow {
    /// The budget was spent on failures: the generation is exhausted under
    /// this code.
    Exhausted {
        /// The stable code of the failure that spent the budget.
        code: String,
        /// The failure's detail.
        detail: String,
    },
    /// The budget was spent and nothing was established: the generation
    /// parks for a reconciliation decision under this code.
    Indeterminate {
        /// The stable code of the park.
        code: &'static str,
    },
}

/// Reads an exhausted row's last failure line back into the outcome its
/// worker was about to deliver.
///
/// | The line | Written by | The outcome |
/// |---|---|---|
/// | a finding's line | a reconciliation that established nothing | `Indeterminate` under the finding's code |
/// | the worker-lost line | a budget spent on attempts lost after `Started` | `Exhausted` under the worker-lost code |
/// | `code: detail` | a failed attempt, or an unknown outcome found not executed | `Exhausted` under `code` |
///
/// The finding lines are matched first: one of them has the `code: detail`
/// shape itself. A line is split at its first `": "`, and only when what
/// precedes it is a code: not empty, and without whitespace.
///
/// A line of no known shape, or no line, says the budget is spent and not
/// how. A reconcileable effect then parks, since for that class only its
/// protocol establishes an outcome and the budget to ask it is gone. Any
/// other effect is exhausted under [`TICKET_ROW_SETTLED`], with the line as
/// its detail.
pub(crate) fn owed_by_exhausted_row(line: Option<&str>, reconcileable: bool) -> OwedByExhaustedRow {
    let line = line.unwrap_or_default();
    for finding in [RECONCILIATION_UNKNOWN, OUTCOME_MISMATCH] {
        if line == finding.line {
            return OwedByExhaustedRow::Indeterminate {
                code: finding.exhausted_code,
            };
        }
    }
    if line == WORKER_LOST_LINE {
        return OwedByExhaustedRow::Exhausted {
            code: WORKER_LOST_CODE.to_string(),
            detail: AMBIGUOUS_BUDGET_SPENT_MESSAGE.to_string(),
        };
    }
    if let Some((code, detail)) = line.split_once(": ") {
        if !code.is_empty() && !code.contains(char::is_whitespace) {
            return OwedByExhaustedRow::Exhausted {
                code: code.to_string(),
                detail: detail.to_string(),
            };
        }
    }
    if reconcileable {
        OwedByExhaustedRow::Indeterminate {
            code: RECONCILIATION_UNKNOWN.exhausted_code,
        }
    } else {
        OwedByExhaustedRow::Exhausted {
            code: TICKET_ROW_SETTLED.to_string(),
            detail: line.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exhausted(code: &str, detail: &str) -> OwedByExhaustedRow {
        OwedByExhaustedRow::Exhausted {
            code: code.to_string(),
            detail: detail.to_string(),
        }
    }

    #[test]
    fn a_failed_attempts_line_gives_back_its_code_and_detail() {
        for reconcileable in [false, true] {
            assert_eq!(
                owed_by_exhausted_row(
                    Some("tool-refused: the tool refused the call"),
                    reconcileable
                ),
                exhausted("tool-refused", "the tool refused the call")
            );
        }
    }

    #[test]
    fn a_line_is_split_at_its_first_separator() {
        for reconcileable in [false, true] {
            assert_eq!(
                owed_by_exhausted_row(Some("a: b: c"), reconcileable),
                exhausted("a", "b: c")
            );
        }
    }

    #[test]
    fn the_worker_lost_line_gives_back_its_code() {
        for reconcileable in [false, true] {
            assert_eq!(
                owed_by_exhausted_row(Some(WORKER_LOST_LINE), reconcileable),
                exhausted(WORKER_LOST_CODE, AMBIGUOUS_BUDGET_SPENT_MESSAGE)
            );
        }
    }

    #[test]
    fn a_finding_that_established_nothing_parks_under_its_code() {
        for reconcileable in [false, true] {
            assert_eq!(
                owed_by_exhausted_row(Some(RECONCILIATION_UNKNOWN.line), reconcileable),
                OwedByExhaustedRow::Indeterminate {
                    code: "reconciliation-exhausted"
                }
            );
        }
    }

    #[test]
    fn the_mismatch_line_is_a_finding_and_not_a_failed_attempt() {
        for reconcileable in [false, true] {
            assert_eq!(
                owed_by_exhausted_row(Some(OUTCOME_MISMATCH.line), reconcileable),
                OwedByExhaustedRow::Indeterminate {
                    code: "effect-outcome-mismatch"
                }
            );
        }
    }

    /// Lines no exhausting site writes.
    const UNKNOWN_SHAPES: [Option<&str>; 5] = [
        Some("no colon here"),
        Some("two words: detail"),
        Some(": detail"),
        Some(""),
        None,
    ];

    #[test]
    fn a_line_of_no_known_shape_parks_a_reconcileable_effect() {
        for line in UNKNOWN_SHAPES {
            assert_eq!(
                owed_by_exhausted_row(line, true),
                OwedByExhaustedRow::Indeterminate {
                    code: "reconciliation-exhausted"
                },
                "{line:?}"
            );
        }
    }

    #[test]
    fn a_line_of_no_known_shape_exhausts_any_other_effect_under_the_rows_code() {
        for line in UNKNOWN_SHAPES {
            assert_eq!(
                owed_by_exhausted_row(line, false),
                exhausted("ticket-row-settled", line.unwrap_or_default()),
                "{line:?}"
            );
        }
    }
}
