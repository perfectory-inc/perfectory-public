//! Building-register-unit application ledger persistence.

use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationRollbackCommand,
};
use foundation_normalization_domain::{
    validate_building_register_unit_proposal, NormalizationError, NormalizationTargetKind,
};
use serde_json::Value as JsonValue;
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::postgres_error::map_sqlx;
use crate::row_mapping::{ApplicationForRollback, ProposalForApply};

pub const APPLY_COMMAND_TYPE: &str = "building_register_unit.normalization.apply.v1";
pub const ROLLBACK_COMMAND_TYPE: &str = "building_register_unit.normalization.rollback.v1";

const ACTIVE_OVERRIDES_SQL: &str = include_str!("building_register_unit/active_overrides.sql");

pub struct ActiveOverrideRow {
    pub application_id: Uuid,
    pub snapshot: JsonValue,
}

#[derive(Default)]
pub struct OverrideChainState {
    active_override: Option<ActiveOverrideRow>,
    lineage_predecessor_proposal_id: Option<Uuid>,
}

pub fn validate_proposal(proposal: &ProposalForApply) -> Result<(), NormalizationError> {
    validate_building_register_unit_proposal(
        &proposal.target_schema_version,
        &proposal.proposal_schema_version,
        &proposal.target_identity,
        &proposal.proposed_record,
    )
}

pub async fn serialize_target_identity(
    tx: &mut Transaction<'_, Postgres>,
    target_identity: &JsonValue,
) -> Result<(), NormalizationError> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
            hashtextextended(
                'foundation.normalization.building_register_unit:' || ($1::jsonb)::text,
                0
            )
         )",
    )
    .bind(target_identity)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

pub async fn load_active_override(
    tx: &mut Transaction<'_, Postgres>,
    target_identity: &JsonValue,
    excluded_application_id: Option<Uuid>,
) -> Result<Option<JsonValue>, NormalizationError> {
    let state =
        load_override_chain_state_for_target(tx, target_identity, excluded_application_id).await?;
    Ok(state.active_override.map(|row| row.snapshot))
}

pub async fn load_override_chain_state(
    tx: &mut Transaction<'_, Postgres>,
    target_identity: &JsonValue,
) -> Result<OverrideChainState, NormalizationError> {
    load_override_chain_state_for_target(tx, target_identity, None).await
}

pub async fn list_active_overrides(
    pool: &PgPool,
) -> Result<Vec<ActiveOverrideRow>, NormalizationError> {
    let rows = sqlx::query(ACTIVE_OVERRIDES_SQL)
        .bind(APPLY_COMMAND_TYPE)
        .bind(Option::<JsonValue>::None)
        .bind(Option::<Uuid>::None)
        .bind(ROLLBACK_COMMAND_TYPE)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx)?;
    Ok(map_override_chain_rows(rows)?
        .into_iter()
        .filter_map(|state| state.active_override)
        .collect())
}

async fn load_override_chain_state_for_target(
    tx: &mut Transaction<'_, Postgres>,
    target_identity: &JsonValue,
    excluded_application_id: Option<Uuid>,
) -> Result<OverrideChainState, NormalizationError> {
    let rows = sqlx::query(ACTIVE_OVERRIDES_SQL)
        .bind(APPLY_COMMAND_TYPE)
        .bind(target_identity)
        .bind(excluded_application_id)
        .bind(ROLLBACK_COMMAND_TYPE)
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    let mut states = map_override_chain_rows(rows)?;
    match states.len() {
        0 => Ok(OverrideChainState::default()),
        1 => states.pop().ok_or_else(|| {
            NormalizationError::Persistence(
                "building-register-unit override chain state is missing".to_owned(),
            )
        }),
        _ => Err(NormalizationError::Persistence(
            "building-register-unit override chain returned duplicate target states".to_owned(),
        )),
    }
}

fn map_override_chain_rows(
    rows: Vec<PgRow>,
) -> Result<Vec<OverrideChainState>, NormalizationError> {
    let mut states = Vec::with_capacity(rows.len());
    for row in rows {
        let chain_valid: bool = row.try_get("chain_valid").map_err(map_sqlx)?;
        if !chain_valid {
            return Err(NormalizationError::Persistence(
                "building-register-unit override chain is invalid".to_owned(),
            ));
        }
        let application_id: Option<Uuid> = row.try_get("application_id").map_err(map_sqlx)?;
        let snapshot: Option<JsonValue> = row.try_get("after_snapshot").map_err(map_sqlx)?;
        let active_override = match (application_id, snapshot) {
            (Some(application_id), Some(snapshot)) => Some(ActiveOverrideRow {
                application_id,
                snapshot,
            }),
            (None, None) => None,
            _ => {
                return Err(NormalizationError::Persistence(
                    "building-register-unit active override row is incomplete".to_owned(),
                ));
            }
        };
        let lineage_predecessor_proposal_id: Option<Uuid> =
            row.try_get("lineage_tail_proposal_id").map_err(map_sqlx)?;
        if lineage_predecessor_proposal_id.is_none() {
            return Err(NormalizationError::Persistence(
                "building-register-unit override chain tail is missing".to_owned(),
            ));
        }
        states.push(OverrideChainState {
            active_override,
            lineage_predecessor_proposal_id,
        });
    }
    Ok(states)
}

pub async fn insert_application(
    tx: &mut Transaction<'_, Postgres>,
    command: &NormalizationApplicationCommand,
    proposal: &ProposalForApply,
    chain_state: OverrideChainState,
) -> Result<(), NormalizationError> {
    let before_snapshot = application_before_snapshot(chain_state);
    let after_snapshot = serde_json::json!({
        "proposal_id": command.proposal_id,
        "target_identity": proposal.target_identity.clone(),
        "proposed_record": proposal.proposed_record.clone(),
    });
    sqlx::query(
        "INSERT INTO catalog.normalization_application
         (id, proposal_id, command_type, target_kind, target_id, expected_version,
          before_snapshot, after_snapshot, applied_by_principal_id, outbox_event_id)
         VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, NULL)",
    )
    .bind(command.id)
    .bind(command.proposal_id)
    .bind(APPLY_COMMAND_TYPE)
    .bind(NormalizationTargetKind::BuildingRegisterUnit.wire_name())
    .bind(command.expected_version)
    .bind(&before_snapshot)
    .bind(&after_snapshot)
    .bind(command.applied_by_principal_id.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

pub fn validate_rollback_command(
    command: &NormalizationRollbackCommand,
) -> Result<(), NormalizationError> {
    if command.reason.trim().is_empty() {
        return Err(NormalizationError::InvalidInput(
            "reason must not be empty".to_owned(),
        ));
    }
    if command.rolled_back_by_principal_id.as_uuid() == Uuid::nil() {
        return Err(NormalizationError::InvalidInput(
            "rolled_back_by_principal_id must not be nil".to_owned(),
        ));
    }
    Ok(())
}

pub fn target_identity_for_rollback(
    application: &ApplicationForRollback,
) -> Result<JsonValue, NormalizationError> {
    application
        .after_snapshot
        .get("target_identity")
        .filter(|identity| identity.is_object())
        .cloned()
        .ok_or_else(|| {
            NormalizationError::Persistence(
                "building-register-unit application target_identity is missing".to_owned(),
            )
        })
}

pub async fn insert_rollback_application(
    tx: &mut Transaction<'_, Postgres>,
    command: &NormalizationRollbackCommand,
    application: &ApplicationForRollback,
    active_before: Option<JsonValue>,
    active_after: Option<JsonValue>,
) -> Result<(), NormalizationError> {
    let before_snapshot = active_override_snapshot(active_before);
    let after_snapshot = active_override_snapshot(active_after);
    sqlx::query(
        "INSERT INTO catalog.normalization_application
         (id, proposal_id, command_type, target_kind, target_id, expected_version,
          before_snapshot, after_snapshot, applied_by_principal_id, rollback_of, outbox_event_id)
         VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, $9, NULL)",
    )
    .bind(command.id)
    .bind(application.proposal_id)
    .bind(ROLLBACK_COMMAND_TYPE)
    .bind(NormalizationTargetKind::BuildingRegisterUnit.wire_name())
    .bind(command.expected_current_version)
    .bind(&before_snapshot)
    .bind(&after_snapshot)
    .bind(command.rolled_back_by_principal_id.as_uuid())
    .bind(command.application_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

fn active_override_snapshot(active_override: Option<JsonValue>) -> JsonValue {
    serde_json::json!({
        "active_override": active_override.unwrap_or(JsonValue::Null),
    })
}

fn application_before_snapshot(chain_state: OverrideChainState) -> JsonValue {
    serde_json::json!({
        "active_override": chain_state
            .active_override
            .map_or(JsonValue::Null, |row| row.snapshot),
        "lineage_predecessor_proposal_id": chain_state.lineage_predecessor_proposal_id,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use uuid::Uuid;

    use super::{
        active_override_snapshot, application_before_snapshot, ActiveOverrideRow,
        OverrideChainState,
    };

    #[test]
    fn active_override_snapshot_preserves_nullable_envelope() {
        assert_eq!(
            active_override_snapshot(None),
            json!({"active_override": null})
        );
        assert_eq!(
            active_override_snapshot(Some(json!({"proposal_id":"proposal-a"}))),
            json!({"active_override":{"proposal_id":"proposal-a"}})
        );
    }

    #[test]
    fn application_snapshot_separates_active_state_from_lineage() {
        let active_proposal = Uuid::now_v7();
        let lineage_proposal = Uuid::now_v7();
        assert_eq!(
            application_before_snapshot(OverrideChainState {
                active_override: Some(ActiveOverrideRow {
                    application_id: Uuid::now_v7(),
                    snapshot: json!({"proposal_id": active_proposal}),
                }),
                lineage_predecessor_proposal_id: Some(lineage_proposal),
            }),
            json!({
                "active_override": {"proposal_id": active_proposal},
                "lineage_predecessor_proposal_id": lineage_proposal,
            })
        );
    }
}
