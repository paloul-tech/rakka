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
    KIND_ERASED, KIND_TOMBSTONE, TOMBSTONE_FENCE_TRANSITION_COUNT, TRANSITION_CAS_MAX_ATTEMPTS,
};

/// Every transition record of one claim. At most `CLAIM_MAX_TRUST_TRANSITIONS`
/// rows; the bound is the domain's, enforced when each was written.
const CLAIM_TRANSITION_RECORDS_SQL: &str = "
SELECT record FROM rakka_agent_knowledge_claim_transition
WHERE tenant = $1 AND space = $2 AND claim_id = $3
ORDER BY ordinal
";

/// One tombstone: the claim row becomes the stub, the ledger rows of the
/// claim's earlier operations are erased, and the tombstone's own ledger row
/// is written — all one statement, all fenced on the transition count the
/// caller read.
///
/// The stub row's `transition_count` is set to the fence value (`$6`), which
/// is what a binary from before tombstones can never match.
const TOMBSTONE_SQL: &str = "
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), upd AS (
    UPDATE rakka_agent_knowledge_claim
    SET subject = $5, predicate = $5, object_node = NULL, tombstoned = TRUE,
        transition_count = $6, record = $7
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
    /// Answers `None` when the history read does not hold exactly the claim's
    /// transition count — a transition landed between the two reads, and the
    /// caller reads again.
    pub(crate) async fn erasable_operations(
        &self,
        scope: &KnowledgeSpaceScope,
        claim: &Claim,
    ) -> ClaimResult<Option<Vec<String>>> {
        let rows = self
            .client
            .query(
                CLAIM_TRANSITION_RECORDS_SQL,
                &[
                    &scope.tenant().as_str(),
                    &scope.space().as_str(),
                    &claim.claim_id.as_str(),
                ],
            )
            .await
            .map_err(map_error)?;
        if u32::try_from(rows.len()).ok() != Some(claim.transition_count()) {
            return Ok(None);
        }
        let mut operations = vec![claim.operation_id.as_str().to_string()];
        if let Some(tombstone) = claim.tombstone() {
            operations.push(tombstone.operation_id.as_str().to_string());
        }
        for row in rows {
            let record: Vec<u8> = row.get("record");
            operations.push(
                decode_transition(&record)?
                    .operation_id
                    .as_str()
                    .to_string(),
            );
        }
        Ok(Some(operations))
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
            let Some(operations) = self.erasable_operations(scope, &claim).await? else {
                continue;
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
            let Some(operations) = self.erasable_operations(scope, &claim).await? else {
                continue;
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

fn lost_races(operation: &str) -> ClaimError {
    ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: format!(
            "the {operation} lost {TRANSITION_CAS_MAX_ATTEMPTS} compare-and-set races; retry"
        ),
    }
}
