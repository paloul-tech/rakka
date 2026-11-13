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

use rakka_agent_knowledge_graph::claim::{Claim, ClaimId, ClaimOperationId};
use rakka_agent_knowledge_graph::error::{ClaimError, ClaimResult};
use rakka_agent_knowledge_graph::scope::KnowledgeSpaceScope;
use rakka_agent_knowledge_graph::tombstone::ClaimTombstoneRequest;
use tokio_postgres::types::ToSql;

use super::{
    decode_claim, decode_transition, encode, map_error, LedgerHit, PostgresKnowledgeGraphStore,
    BACKEND_NAME, CLAIM_TOMBSTONE_MARKER, KIND_ERASED, KIND_TOMBSTONE,
    TOMBSTONE_FENCE_TRANSITION_COUNT, TRANSITION_CAS_MAX_ATTEMPTS,
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
    ON CONFLICT (tenant, space, operation_id) DO NOTHING
)
SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
       (SELECT kind   FROM existing_op)   AS replay_kind,
       (SELECT result FROM existing_op)   AS replay_result,
       EXISTS (SELECT 1 FROM upd)         AS applied
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
            // Ledger first, on every attempt: a replay answers the stub as
            // first written.
            match self
                .ledger_hit(scope, request.operation_id.as_str())
                .await?
            {
                Some(LedgerHit::Tombstone(result)) => return decode_claim(&result),
                Some(LedgerHit::Erased) => return Err(erased(&request.operation_id)),
                Some(LedgerHit::Append(_) | LedgerHit::Transition(_) | LedgerHit::Deleted) => {
                    return Err(conflict(&request.operation_id));
                }
                None => {}
            }
            let Some((claim, fence)) = self.claim_head(scope, &request.claim_id).await? else {
                // Absent and out-of-scope are the same refusal (scenario 18).
                return Err(not_found(&request.claim_id));
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
            let row = self
                .client
                .query_one(TOMBSTONE_SQL, &params)
                .await
                .map_err(map_error)?;
            if row.get::<_, bool>("replayed") {
                let kind: Option<String> = row.get("replay_kind");
                let result: Option<Vec<u8>> = row.get("replay_result");
                return match (kind.as_deref(), result) {
                    (Some(KIND_TOMBSTONE), Some(result)) => decode_claim(&result),
                    (Some(KIND_ERASED), _) => Err(erased(&request.operation_id)),
                    (Some(_), _) => Err(conflict(&request.operation_id)),
                    _ => Err(ClaimError::Backend {
                        backend: BACKEND_NAME.to_string(),
                        message: "the ledger reported a replay without its stored result"
                            .to_string(),
                    }),
                };
            }
            if row.get::<_, bool>("applied") {
                return Ok(stub);
            }
            // The fence moved: a concurrent transition, tombstone or delete
            // won the race. Loop — the fresh read decides against that state.
        }
        Err(lost_races("tombstone"))
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
