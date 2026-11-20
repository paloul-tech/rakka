//! Tombstone and delete for the PostgreSQL backend.
//!
//! Each is one data-modifying-CTE statement, like every other write in this
//! crate, inside the same bounded compare-and-set loop `transition` uses. The
//! statement is fenced on the claim row's `transition_count` column exactly as
//! the loop read it, so the claim change and the ledger rewrite are an
//! all-or-nothing outcome of the state the caller saw.
//!
//! # Finding the ledger rows to erase
//!
//! The ledger has no `claim_id` column, and none is added: the operations to
//! erase are found from the claim itself. The append's operation id is on the
//! claim, each transition's is on its transition record, and a tombstone's is
//! on the stub — at most 34. They are read before the statement and passed to
//! it; the fence is what makes that safe. A transition that lands between the
//! read and the write moves the count, the statement matches nothing, and the
//! loop reads again.
//!
//! # A history that is not the claim's
//!
//! The transition records are read together with the claim row's fence, in
//! one statement. What that one snapshot holds is judged against the claim
//! the loop read a moment before:
//!
//! - the fence is no longer the one read, or the claim row is gone: the claim
//!   moved, a race, and the loop reads again;
//! - the fence is the one read, and for each ordinal from one to the record's
//!   count there is one row whose record is that claim's transition of that
//!   ordinal: the history is whole, and its rows name its operations;
//! - the fence is the one read and the rows are anything else: the history is
//!   damaged. Nothing raced, and reading again answers the same.
//!
//! A damaged claim is refused `claim-history-damaged` and nothing is written.
//! A store that opted in
//! ([`with_damaged_history_erasure`](PostgresKnowledgeGraphStore::with_damaged_history_erasure))
//! erases it. It takes no operation's name from a row of a damaged history: a
//! row that is not the claim's own names another claim's operation. The
//! claim's transition results are found in the ledger instead, by reading the
//! scope's transition rows there and comparing each one's own claim id.
//!
//! # An operation id belongs to one claim
//!
//! A replayed tombstone answers its own claim's stub, and a replayed delete
//! answers success for the claim it removed. The same id sent for a
//! *different* claim is neither: it answers `claim-operation-conflict`. A
//! deleted claim can never exist again, so a claim still present under a
//! delete's id is not the one that delete removed. The ledger inserts carry no
//! `ON CONFLICT` clause for the same reason: two claims erased at once under
//! one id must not both apply and share one ledger row. The loser's statement
//! aborts whole, and the next read answers the conflict.

use rakka_agent_knowledge_graph::claim::{Claim, ClaimId, ClaimOperationId};
use rakka_agent_knowledge_graph::error::{ClaimError, ClaimResult};
use rakka_agent_knowledge_graph::scope::KnowledgeSpaceScope;
use rakka_agent_knowledge_graph::tombstone::{ClaimDeleteRequest, ClaimTombstoneRequest};
use tokio_postgres::types::ToSql;

use super::{
    decode_claim, decode_transition, encode, is_unique_violation, map_error, LedgerHit,
    PostgresKnowledgeGraphStore, WriteStart, BACKEND_NAME, CLAIM_TOMBSTONE_MARKER, KIND_DELETE,
    KIND_ERASED, KIND_TOMBSTONE, KIND_TRANSITION, TOMBSTONE_FENCE_TRANSITION_COUNT,
    TRANSITION_CAS_MAX_ATTEMPTS,
};

/// One claim's fence column and every transition the store holds for it,
/// read by **one statement** and therefore under one snapshot.
///
/// One row for each stored transition, in ordinal order; one row with no
/// transition for a claim that has none; no row for a claim that is gone.
/// At most `CLAIM_MAX_TRUST_TRANSITIONS` rows for a healthy claim; the bound
/// is the domain's, enforced when each was written.
///
/// The fence is here to tell a claim that moved from one that is damaged. A
/// claim and its history are written together, so one snapshot never holds a
/// healthy claim beside a history that is not its own. Read in a statement of
/// its own, a history can be one transition ahead of the claim the caller
/// read a moment before; read with the fence, that shows as a fence that is
/// no longer the caller's.
const CLAIM_HISTORY_SQL: &str = "
SELECT claim.transition_count AS fence, tr.ordinal, tr.record
FROM rakka_agent_knowledge_claim AS claim
LEFT JOIN rakka_agent_knowledge_claim_transition AS tr
       ON tr.tenant = claim.tenant AND tr.space = claim.space
      AND tr.claim_id = claim.claim_id
WHERE claim.tenant = $1 AND claim.space = $2 AND claim.claim_id = $3
ORDER BY tr.ordinal
";

/// Every transition outcome in one tenant and space whose stored result
/// mentions one claim id.
///
/// The ledger has no `claim_id` column, so this is a read of every transition
/// row of the scope's ledger, where every other read an erasure makes is
/// keyed. It runs only for a claim whose history is damaged, and only in a
/// store that opted in to erasing one.
///
/// A match on the id is a nomination. The id also appears in the stored
/// result of another claim whose statement points at this one, so each
/// nominated result is parsed and its own claim id compared.
const CLAIM_LEDGER_OUTCOMES_SQL: &str = "
SELECT operation_id, result FROM rakka_agent_knowledge_claim_op
WHERE tenant = $1 AND space = $2 AND kind = $3
  AND position($4::BYTEA IN result) > 0
";

/// What one snapshot holds of a claim's history, against the claim the
/// caller read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryState {
    /// The claim is as the caller read it, and the store holds exactly the
    /// transitions its record counts.
    Whole,
    /// The claim row is gone, or its fence is no longer the one the caller
    /// read: a transition, a tombstone or a delete landed since. A race, and
    /// the caller reads again.
    Moved,
    /// The claim is as the caller read it, and the store does not hold the
    /// transitions its record counts. Nothing raced: reading again answers
    /// the same.
    Damaged,
}

/// One stored transition row, as far as it can be read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredTransition {
    /// The row's ordinal column.
    ordinal: i32,
    /// What the row's record says it is. `None` for a record that cannot be
    /// read.
    record: Option<RecordedTransition>,
}

/// What a stored transition record says of itself.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedTransition {
    /// The claim the record says it belongs to.
    claim_id: String,
    /// The ordinal the record says it has.
    ordinal: u32,
    /// The operation that wrote it.
    operation_id: String,
}

/// Judges one history read.
///
/// `read_fence` is the fence the caller read with the claim, `stored_fence`
/// the one this read found (`None` when the claim row is gone), `claim_id`
/// and `count` the claim and the transitions its record counts, and `stored`
/// the transition rows in ascending ordinal order.
///
/// A whole history is one row for each ordinal from one to `count`, whose
/// record is that claim's transition of that ordinal. The number of rows
/// alone is not enough: a claim that lost one transition and gained another
/// holds the right number, and the lost one's operation could not be named.
/// Nor are the ordinals alone: a row under the claim's key whose record is
/// another claim's transition names that claim's operation.
fn history_state(
    read_fence: i32,
    stored_fence: Option<i32>,
    claim_id: &str,
    count: u32,
    stored: &[StoredTransition],
) -> HistoryState {
    if stored_fence != Some(read_fence) {
        return HistoryState::Moved;
    }
    let whole = u32::try_from(stored.len()).ok() == Some(count)
        && stored.iter().zip(1_u32..).all(|(row, expected)| {
            u32::try_from(row.ordinal).ok() == Some(expected)
                && row
                    .record
                    .as_ref()
                    .is_some_and(|record| record.claim_id == claim_id && record.ordinal == expected)
        });
    if whole {
        HistoryState::Whole
    } else {
        HistoryState::Damaged
    }
}

/// Reads what one stored transition record says of itself.
///
/// A record this binary must not judge, one from a schema newer or older
/// than it reads, is refused as it always was. Any other record that does
/// not restore is unreadable, which is damage: `None`.
fn recorded_transition(record: &[u8]) -> ClaimResult<Option<RecordedTransition>> {
    match decode_transition(record) {
        Ok(transition) => Ok(Some(RecordedTransition {
            claim_id: transition.claim_id.as_str().to_string(),
            ordinal: transition.ordinal,
            operation_id: transition.operation_id.as_str().to_string(),
        })),
        Err(
            error
            @ (ClaimError::SchemaVersionAhead { .. } | ClaimError::SchemaVersionTooOld { .. }),
        ) => Err(error),
        Err(_) => Ok(None),
    }
}

/// What [`PostgresKnowledgeGraphStore::erasable_operations`] found.
pub(crate) enum ErasableHistory {
    /// The operation ids whose ledger payloads the erasure must clear.
    Whole(Vec<String>),
    /// The claim moved since the caller read it; the caller reads again.
    Moved,
    /// The claim's stored history is damaged. The operation ids are the ones
    /// the claim itself names: its append, and its tombstone when it is a
    /// stub. No stored row is asked for a name.
    Damaged(Vec<String>),
}

/// One tombstone: the claim row becomes the stub, the ledger rows of the
/// claim's earlier operations are erased, and the tombstone's own ledger row
/// is written — all one statement, all fenced on the transition count the
/// caller read.
///
/// The stub row's `transition_count` is set to the fence value (`$6`), which
/// is what a binary from before tombstones can never match.
///
/// The row's search words are cleared here too: they are derived from the
/// statement being withdrawn. The claim table's trigger would clear them if
/// this statement did not, and it is what covers a binary from before the
/// search index; this statement says so itself rather than lean on it.
const TOMBSTONE_SQL: &str = "
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), upd AS (
    UPDATE rakka_agent_knowledge_claim
    SET subject = $5, predicate = $5, object_node = NULL, tombstoned = TRUE,
        transition_count = $6, record = $7, search_tokens = NULL
    WHERE tenant = $1 AND space = $2 AND claim_id = $4
      AND transition_count = $8
      AND NOT EXISTS (SELECT 1 FROM existing_op)
    RETURNING claim_id
), erased AS (
    UPDATE rakka_agent_knowledge_claim_op
    SET kind = $10, result = ''::BYTEA
    WHERE tenant = $1 AND space = $2 AND operation_id = ANY($9)
      AND EXISTS (SELECT 1 FROM upd)
), ins_op AS (
    INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result)
    SELECT $1, $2, $3, $11, $7 FROM upd
)
SELECT EXISTS (SELECT 1 FROM upd) AS applied
";

/// One delete: the claim row and every transition row of the claim are
/// removed, the ledger rows of the claim's earlier operations are erased, and
/// the delete's own content-free ledger row is written — all one statement.
///
/// The fence is the row's `transition_count` column as the caller read it:
/// the count for a live row, the fence value for a stub. That one comparison
/// also catches a tombstone that lands between the read and the write, since
/// a tombstone replaces the count with the fence value. Every later step
/// reads `del`, so nothing is removed or erased unless the claim row was.
///
/// The ledger rows are *erased*, never deleted: a row that was simply removed
/// would let a replayed append insert the claim again.
const DELETE_SQL: &str = "
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), del AS (
    DELETE FROM rakka_agent_knowledge_claim
    WHERE tenant = $1 AND space = $2 AND claim_id = $4
      AND transition_count = $5
      AND NOT EXISTS (SELECT 1 FROM existing_op)
    RETURNING claim_id
), del_tr AS (
    DELETE FROM rakka_agent_knowledge_claim_transition
    WHERE tenant = $1 AND space = $2 AND claim_id = $4
      AND EXISTS (SELECT 1 FROM del)
), erased AS (
    UPDATE rakka_agent_knowledge_claim_op
    SET kind = $7, result = ''::BYTEA
    WHERE tenant = $1 AND space = $2 AND operation_id = ANY($6)
      AND EXISTS (SELECT 1 FROM del)
), ins_op AS (
    INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result)
    SELECT $1, $2, $3, $8, ''::BYTEA FROM del
)
SELECT EXISTS (SELECT 1 FROM del) AS applied
";

impl PostgresKnowledgeGraphStore {
    /// The operation ids whose ledger payloads erasing `claim` must clear: its
    /// append, every transition in its history, and its tombstone when it is a
    /// stub.
    ///
    /// `fence` is the fence the caller read with `claim`. The history is read
    /// with the claim row's fence as it then stands ([`CLAIM_HISTORY_SQL`]),
    /// so the answer tells a claim that moved since the caller's read, which
    /// the caller reads again, from one whose stored history is damaged,
    /// which no second read will mend.
    pub(crate) async fn erasable_operations(
        &self,
        scope: &KnowledgeSpaceScope,
        claim: &Claim,
        fence: i32,
    ) -> ClaimResult<ErasableHistory> {
        let rows = self
            .client
            .query(
                CLAIM_HISTORY_SQL,
                &[
                    &scope.tenant().as_str(),
                    &scope.space().as_str(),
                    &claim.claim_id.as_str(),
                ],
            )
            .await
            .map_err(map_error)?;
        let stored_fence = rows.first().map(|row| row.get::<_, i32>("fence"));
        let mut stored = Vec::with_capacity(rows.len());
        for row in &rows {
            let ordinal: Option<i32> = row.get("ordinal");
            let record: Option<Vec<u8>> = row.get("record");
            if let Some((ordinal, record)) = ordinal.zip(record) {
                stored.push(StoredTransition {
                    ordinal,
                    record: recorded_transition(&record)?,
                });
            }
        }
        let mut operations = vec![claim.operation_id.as_str().to_string()];
        if let Some(tombstone) = claim.tombstone() {
            operations.push(tombstone.operation_id.as_str().to_string());
        }
        match history_state(
            fence,
            stored_fence,
            claim.claim_id.as_str(),
            claim.transition_count(),
            &stored,
        ) {
            HistoryState::Moved => Ok(ErasableHistory::Moved),
            HistoryState::Damaged => Ok(ErasableHistory::Damaged(operations)),
            HistoryState::Whole => {
                operations.extend(
                    stored
                        .into_iter()
                        .filter_map(|row| row.record)
                        .map(|record| record.operation_id),
                );
                Ok(ErasableHistory::Whole(operations))
            }
        }
    }

    /// The operation ids to clear when erasing a claim whose stored history
    /// is damaged, or the refusal of a store that did not opt in.
    ///
    /// `named` holds the operations the claim itself names. Its transitions'
    /// stored results, each of which holds the whole claim as it stood after
    /// that transition, are found in the ledger itself
    /// ([`CLAIM_LEDGER_OUTCOMES_SQL`]) and added, whether or not a row is
    /// stored for them.
    ///
    /// The statement that follows is fenced like any other erasure, so a
    /// transition that lands after this read is not missed: it moves the
    /// fence, the statement matches nothing, and the caller reads again.
    async fn damaged_history_operations(
        &self,
        scope: &KnowledgeSpaceScope,
        claim_id: &ClaimId,
        mut named: Vec<String>,
    ) -> ClaimResult<Vec<String>> {
        if !self.damaged_history_erasure {
            return Err(ClaimError::HistoryDamaged {
                claim_id: claim_id.clone(),
            });
        }
        let rows = self
            .client
            .query(
                CLAIM_LEDGER_OUTCOMES_SQL,
                &[
                    &scope.tenant().as_str(),
                    &scope.space().as_str(),
                    &KIND_TRANSITION,
                    &claim_id.as_str().as_bytes(),
                ],
            )
            .await
            .map_err(map_error)?;
        for row in rows {
            let operation_id: String = row.get("operation_id");
            let result: Vec<u8> = row.get("result");
            if outcome_claim_id(&operation_id, &result)? == claim_id.as_str() {
                named.push(operation_id);
            }
        }
        Ok(named)
    }

    /// The tombstone operation; see `KnowledgeGraphStore::tombstone`.
    pub(crate) async fn tombstone_claim(
        &self,
        scope: &KnowledgeSpaceScope,
        request: &ClaimTombstoneRequest,
    ) -> ClaimResult<Claim> {
        for _ in 0..TRANSITION_CAS_MAX_ATTEMPTS {
            // The ledger first, on every attempt, in the same snapshot as the
            // claim: a replay answers the stub as first written, never
            // `claim-tombstoned` against the stub this very operation made.
            let (claim, fence) = match self
                .write_start(scope, &request.operation_id, &request.claim_id)
                .await?
            {
                WriteStart::Spent(LedgerHit::Tombstone(result), _) => {
                    let stub = decode_claim(&result)?;
                    // The id was spent tombstoning this claim, or another one.
                    return if stub.claim_id == request.claim_id {
                        Ok(stub)
                    } else {
                        Err(conflict(&request.operation_id))
                    };
                }
                WriteStart::Spent(LedgerHit::Erased, _) => {
                    return Err(erased(&request.operation_id));
                }
                WriteStart::Spent(
                    LedgerHit::Append(_) | LedgerHit::Transition(_) | LedgerHit::Deleted,
                    _,
                ) => return Err(conflict(&request.operation_id)),
                // Absent and out-of-scope are the same refusal (scenario 18).
                WriteStart::Absent => return Err(not_found(&request.claim_id)),
                WriteStart::Present(claim, fence) => (*claim, fence),
            };
            // Refuses a claim that is already a stub (`claim-tombstoned`).
            let stub = claim.apply_tombstone(request.to_tombstone())?;
            let operations = match self.erasable_operations(scope, &claim, fence).await? {
                ErasableHistory::Whole(operations) => operations,
                // The claim moved between the two reads: the fresh read
                // decides.
                ErasableHistory::Moved => continue,
                ErasableHistory::Damaged(named) => {
                    self.damaged_history_operations(scope, &request.claim_id, named)
                        .await?
                }
            };

            let stub_record = encode(&stub.to_record())?;
            let params: [&(dyn ToSql + Sync); 11] = [
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &request.operation_id.as_str(),
                &request.claim_id.as_str(),
                &CLAIM_TOMBSTONE_MARKER,
                &TOMBSTONE_FENCE_TRANSITION_COUNT,
                &stub_record,
                &fence,
                &operations,
                &KIND_ERASED,
                &KIND_TOMBSTONE,
            ];
            match self.client.query_one(TOMBSTONE_SQL, &params).await {
                Ok(row) if row.get::<_, bool>("applied") => return Ok(stub),
                // Nothing applied: the fence moved, or the operation id was
                // spent after the read. Loop — the fresh read decides.
                Ok(_) => {}
                // The stub went in and the ledger row was already there: the
                // id was spent on another claim while this statement waited.
                // The statement aborted whole; the fresh read answers.
                Err(error) if is_unique_violation(&error) => {}
                Err(error) => return Err(map_error(error)),
            }
        }
        Err(lost_races("tombstone"))
    }

    /// The delete operation; see `KnowledgeGraphStore::delete`.
    pub(crate) async fn delete_claim(
        &self,
        scope: &KnowledgeSpaceScope,
        request: &ClaimDeleteRequest,
    ) -> ClaimResult<()> {
        for _ in 0..TRANSITION_CAS_MAX_ATTEMPTS {
            // The ledger first, on every attempt, in the same snapshot as the
            // claim: a replayed delete answers success, never
            // `claim-not-found` for the claim this very operation removed.
            let (claim, fence) = match self
                .write_start(scope, &request.operation_id, &request.claim_id)
                .await?
            {
                // A deleted claim can never exist again, so a claim still here
                // is not the one this id removed: the id was reused for another
                // claim, and answering success would report an erasure that did
                // not happen.
                WriteStart::Spent(LedgerHit::Deleted, true) => {
                    return Err(conflict(&request.operation_id));
                }
                WriteStart::Spent(LedgerHit::Deleted, false) => return Ok(()),
                WriteStart::Spent(LedgerHit::Erased, _) => {
                    return Err(erased(&request.operation_id));
                }
                WriteStart::Spent(
                    LedgerHit::Append(_) | LedgerHit::Transition(_) | LedgerHit::Tombstone(_),
                    _,
                ) => return Err(conflict(&request.operation_id)),
                // Absent and out-of-scope are the same refusal (scenario 18).
                WriteStart::Absent => return Err(not_found(&request.claim_id)),
                WriteStart::Present(claim, fence) => (*claim, fence),
            };
            // The caller's expectation, against the authoritative record and
            // before anything is written: a stale delete spends nothing. Re-run
            // on every attempt, so a lost race is judged against the state
            // that won it.
            if let Some(expected) = request.expected {
                if expected.transition_count != claim.transition_count()
                    || expected.tombstoned != claim.is_tombstoned()
                {
                    return Err(ClaimError::DeleteStale {
                        claim_id: request.claim_id.clone(),
                    });
                }
            }
            let operations = match self.erasable_operations(scope, &claim, fence).await? {
                ErasableHistory::Whole(operations) => operations,
                // The claim moved between the two reads: the fresh read
                // decides.
                ErasableHistory::Moved => continue,
                ErasableHistory::Damaged(named) => {
                    self.damaged_history_operations(scope, &request.claim_id, named)
                        .await?
                }
            };

            let params: [&(dyn ToSql + Sync); 8] = [
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &request.operation_id.as_str(),
                &request.claim_id.as_str(),
                &fence,
                &operations,
                &KIND_ERASED,
                &KIND_DELETE,
            ];
            match self.client.query_one(DELETE_SQL, &params).await {
                Ok(row) if row.get::<_, bool>("applied") => return Ok(()),
                // Nothing applied: the fence moved, or the operation id was
                // spent after the read. Loop — the fresh read decides.
                Ok(_) => {}
                // The claim row went and the ledger row was already there: the
                // id was spent on another claim while this statement waited.
                // The statement aborted whole; the fresh read answers.
                Err(error) if is_unique_violation(&error) => {}
                Err(error) => return Err(map_error(error)),
            }
        }
        Err(lost_races("delete"))
    }
}

fn erased(operation_id: &ClaimOperationId) -> ClaimError {
    ClaimError::OperationErased {
        operation_id: operation_id.clone(),
    }
}

fn conflict(operation_id: &ClaimOperationId) -> ClaimError {
    ClaimError::OperationConflict {
        operation_id: operation_id.clone(),
    }
}

fn not_found(claim_id: &ClaimId) -> ClaimError {
    ClaimError::NotFound {
        claim_id: claim_id.clone(),
    }
}

/// The id of the claim a stored transition outcome belongs to.
///
/// Only the id is read, and nothing of the outcome is validated: the result
/// may be another claim's, which this erasure has no business refusing over.
/// A result that cannot be read is refused by the name of its operation,
/// which is the ledger row an operator has to look at.
fn outcome_claim_id(operation_id: &str, result: &[u8]) -> ClaimResult<String> {
    let unreadable = |detail: String| ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: format!(
            "the stored transition outcome of operation {operation_id} could not be parsed: \
             {detail}"
        ),
    };
    let outcome: serde_json::Value =
        serde_json::from_slice(result).map_err(|error| unreadable(error.to_string()))?;
    outcome
        .get("claim")
        .and_then(|claim| claim.get("claim_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| unreadable("it names no claim".to_string()))
}

fn lost_races(operation: &str) -> ClaimError {
    ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: format!(
            "the {operation} lost {TRANSITION_CAS_MAX_ATTEMPTS} compare-and-set races; retry"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        history_state, outcome_claim_id, recorded_transition, HistoryState, RecordedTransition,
        StoredTransition,
    };
    use crate::TOMBSTONE_FENCE_TRANSITION_COUNT as STUB;

    const CLAIM: &str = "claim-a";

    /// The claim's own transition of `ordinal`, stored under that ordinal.
    fn own(ordinal: u32) -> StoredTransition {
        stored(ordinal, CLAIM, ordinal)
    }

    /// A row under `ordinal` whose record says it is `claim_id`'s transition
    /// `recorded`.
    fn stored(ordinal: u32, claim_id: &str, recorded: u32) -> StoredTransition {
        StoredTransition {
            ordinal: i32::try_from(ordinal).expect("a small ordinal"),
            record: Some(RecordedTransition {
                claim_id: claim_id.to_string(),
                ordinal: recorded,
                operation_id: format!("op-{claim_id}-{recorded}"),
            }),
        }
    }

    fn judged(
        read: i32,
        found: Option<i32>,
        count: u32,
        rows: &[StoredTransition],
    ) -> HistoryState {
        history_state(read, found, CLAIM, count, rows)
    }

    #[test]
    fn a_history_of_exactly_the_counted_transitions_is_whole() {
        assert_eq!(judged(0, Some(0), 0, &[]), HistoryState::Whole);
        assert_eq!(
            judged(2, Some(2), 2, &[own(1), own(2)]),
            HistoryState::Whole
        );
        // A stub's fence is not its count: its record still counts its
        // transitions.
        assert_eq!(
            judged(STUB, Some(STUB), 3, &[own(1), own(2), own(3)]),
            HistoryState::Whole
        );
    }

    /// A transition that lands between the caller's read and this one raises
    /// the fence with the history, in one statement. The history is one ahead
    /// of the claim the caller read, and the fence says why: a race, to be
    /// read again, and never damage.
    #[test]
    fn a_fence_that_is_no_longer_the_callers_is_a_claim_that_moved() {
        assert_eq!(
            judged(2, Some(3), 2, &[own(1), own(2), own(3)]),
            HistoryState::Moved
        );
        // Tombstoned since: the fence is the stub's.
        assert_eq!(
            judged(2, Some(STUB), 2, &[own(1), own(2)]),
            HistoryState::Moved
        );
        // Deleted since: no claim row.
        assert_eq!(judged(2, None, 2, &[]), HistoryState::Moved);
        // Moved, whatever the history then holds.
        assert_eq!(judged(2, Some(3), 2, &[own(1)]), HistoryState::Moved);
    }

    #[test]
    fn the_callers_fence_beside_another_history_is_damage() {
        // A transition lost, the last and an earlier one.
        assert_eq!(judged(2, Some(2), 2, &[own(1)]), HistoryState::Damaged);
        assert_eq!(judged(2, Some(2), 2, &[own(2)]), HistoryState::Damaged);
        assert_eq!(judged(1, Some(1), 1, &[]), HistoryState::Damaged);
        // One too many.
        assert_eq!(
            judged(2, Some(2), 2, &[own(1), own(2), own(3)]),
            HistoryState::Damaged
        );
        assert_eq!(judged(0, Some(0), 0, &[own(1)]), HistoryState::Damaged);
        // The right number, and not the ones the record counts.
        assert_eq!(
            judged(2, Some(2), 2, &[own(2), own(3)]),
            HistoryState::Damaged
        );
        // A stub's history is held to its record's count too.
        assert_eq!(
            judged(STUB, Some(STUB), 2, &[own(1)]),
            HistoryState::Damaged
        );
    }

    /// The right ordinals are not enough either. A row whose record is
    /// another claim's transition, or this claim's transition of another
    /// ordinal, or cannot be read, is not the transition the record counts,
    /// and would name an operation that is not it. Nor is the right record
    /// enough, under an ordinal that is not its own.
    #[test]
    fn a_row_that_is_not_the_claims_own_transition_of_its_ordinal_is_damage() {
        assert_eq!(
            judged(2, Some(2), 2, &[own(1), stored(2, "claim-b", 2)]),
            HistoryState::Damaged
        );
        assert_eq!(
            judged(2, Some(2), 2, &[own(1), stored(2, CLAIM, 1)]),
            HistoryState::Damaged
        );
        let unreadable = StoredTransition {
            ordinal: 2,
            record: None,
        };
        assert_eq!(
            judged(2, Some(2), 2, &[own(1), unreadable]),
            HistoryState::Damaged
        );
        // The claim's own second transition, stored under another ordinal:
        // the record is right and the row is not where a read by ordinal
        // finds it.
        assert_eq!(
            judged(2, Some(2), 2, &[own(1), stored(5, CLAIM, 2)]),
            HistoryState::Damaged
        );
    }

    #[test]
    fn a_record_that_cannot_be_read_is_none_and_not_an_error() {
        for unreadable in [&b"not json"[..], br#"{"claim_id":"claim-a"}"#] {
            assert_eq!(
                recorded_transition(unreadable).expect("an unreadable record is damage"),
                None
            );
        }
    }

    #[test]
    fn a_stored_outcome_answers_the_id_of_its_own_claim() {
        // The id of another claim appears in the statement, and is not the
        // answer.
        let outcome = br#"{"claim":{"schema_version":1,"claim_id":"claim-a",
            "object":{"node":"claim-b"},"a_later_field":true},"transition":{"ordinal":1}}"#;
        assert_eq!(
            outcome_claim_id("op-1", outcome).expect("the outcome names its claim"),
            "claim-a"
        );
    }

    #[test]
    fn a_stored_outcome_that_names_no_claim_is_refused_by_its_operation() {
        for unreadable in [
            &b"not json"[..],
            br#"{"transition":{}}"#,
            br#"{"claim":{}}"#,
        ] {
            let error = outcome_claim_id("op-unreadable", unreadable)
                .expect_err("the outcome is unreadable");
            assert_eq!(error.code(), "claim-backend-failed", "{error}");
            assert!(error.to_string().contains("op-unreadable"), "{error}");
        }
    }
}
