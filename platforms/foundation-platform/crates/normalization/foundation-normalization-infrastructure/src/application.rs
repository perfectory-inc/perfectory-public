//! Proposal application and rollback transaction orchestration.

use catalog_application::industrial_complex_patch::{
    parse_industrial_complex_proposed_record, parse_industrial_complex_restore_input,
    parse_industrial_complex_target_identity,
};
use catalog_infrastructure::{
    IndustrialComplexMutationReceipt, PgIndustrialComplexTransactionParticipant,
};
use foundation_normalization_application::{
    ApplyNormalizationProposal, NormalizationApplicationCommand, NormalizationApplicationRecord,
    NormalizationRollbackCommand, NormalizationRollbackRecord,
};
use foundation_normalization_domain::{NormalizationError, NormalizationTargetKind};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::building_register_unit;
use crate::industrial_complex;
use crate::postgres_error::{map_catalog, map_sqlx};
use crate::proposal;
use crate::row_mapping::{parse_target_kind, ApplicationForRollback};

pub async fn apply(
    pool: &PgPool,
    command: NormalizationApplicationCommand,
) -> Result<NormalizationApplicationRecord, NormalizationError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let persisted = proposal::lock_for_apply(&mut tx, command.proposal_id).await?;
    ApplyNormalizationProposal::validate_status(persisted.status)?;

    let target_id = match persisted.target_kind {
        NormalizationTargetKind::BuildingRegisterUnit => {
            building_register_unit::validate_proposal(&persisted)?;
            building_register_unit::serialize_target_identity(&mut tx, &persisted.target_identity)
                .await?;
            let chain_state = building_register_unit::load_override_chain_state(
                &mut tx,
                &persisted.target_identity,
            )
            .await?;
            building_register_unit::insert_application(&mut tx, &command, &persisted, chain_state)
                .await?;
            None
        }
        NormalizationTargetKind::IndustrialComplex => {
            let target_id = parse_industrial_complex_target_identity(&persisted.target_identity)
                .map_err(map_catalog)?;
            let patch = parse_industrial_complex_proposed_record(&persisted.proposed_record)
                .map_err(map_catalog)?;
            let receipt = PgIndustrialComplexTransactionParticipant::new()
                .apply(&mut tx, target_id, command.expected_version, patch)
                .await
                .map_err(map_catalog)?;
            insert_industrial_complex_application(&mut tx, &command, &receipt).await?;
            Some(target_id.as_uuid())
        }
        other @ NormalizationTargetKind::BuildingRegisterFloor => {
            return Err(NormalizationError::InvalidInput(format!(
                "unsupported normalization target_kind: {}",
                other.wire_name()
            )));
        }
    };

    proposal::mark_applied(&mut tx, command.proposal_id).await?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(NormalizationApplicationRecord {
        id: command.id,
        proposal_id: command.proposal_id,
        target_kind: persisted.target_kind,
        target_id,
    })
}

pub async fn rollback(
    pool: &PgPool,
    command: NormalizationRollbackCommand,
) -> Result<NormalizationRollbackRecord, NormalizationError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let persisted = lock_application_for_rollback(&mut tx, command.application_id).await?;

    let target_id = match persisted.target_kind {
        NormalizationTargetKind::BuildingRegisterUnit => {
            building_register_unit::validate_rollback_command(&command)?;
            let target_identity = building_register_unit::target_identity_for_rollback(&persisted)?;
            building_register_unit::serialize_target_identity(&mut tx, &target_identity).await?;
            ensure_no_existing_rollback(&mut tx, command.application_id).await?;
            let active_before =
                building_register_unit::load_active_override(&mut tx, &target_identity, None)
                    .await?;
            let active_after = building_register_unit::load_active_override(
                &mut tx,
                &target_identity,
                Some(command.application_id),
            )
            .await?;
            building_register_unit::insert_rollback_application(
                &mut tx,
                &command,
                &persisted,
                active_before,
                active_after,
            )
            .await?;
            None
        }
        NormalizationTargetKind::IndustrialComplex => {
            ensure_no_existing_rollback(&mut tx, command.application_id).await?;
            let target_id = persisted.target_id.ok_or_else(|| {
                NormalizationError::InvalidInput(
                    "normalization application target_id is required for rollback".to_owned(),
                )
            })?;
            let expected_current_snapshot =
                industrial_complex::load_rollback_head(&mut tx, target_id, command.application_id)
                    .await?;
            let restore = parse_industrial_complex_restore_input(
                &persisted.before_snapshot,
                &persisted.after_snapshot,
                &expected_current_snapshot,
                target_id,
            )
            .map_err(map_catalog)?;
            let receipt = PgIndustrialComplexTransactionParticipant::new()
                .restore(&mut tx, command.expected_current_version, restore)
                .await
                .map_err(map_catalog)?;
            insert_industrial_complex_rollback(&mut tx, &command, &persisted, &receipt).await?;
            Some(target_id.as_uuid())
        }
        other @ NormalizationTargetKind::BuildingRegisterFloor => {
            return Err(NormalizationError::InvalidInput(format!(
                "unsupported normalization target_kind: {}",
                other.wire_name()
            )));
        }
    };

    proposal::mark_rolled_back(&mut tx, persisted.proposal_id).await?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(NormalizationRollbackRecord {
        id: command.id,
        rollback_of: command.application_id,
        target_kind: persisted.target_kind,
        target_id,
    })
}

async fn lock_application_for_rollback(
    tx: &mut Transaction<'_, Postgres>,
    application_id: Uuid,
) -> Result<ApplicationForRollback, NormalizationError> {
    let row = sqlx::query(
        "SELECT proposal_id, target_kind, target_id, before_snapshot, after_snapshot, rollback_of
         FROM catalog.normalization_application
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(application_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or(NormalizationError::ApplicationNotFound)?;

    let rollback_of: Option<Uuid> = row.try_get("rollback_of").map_err(map_sqlx)?;
    if rollback_of.is_some() {
        return Err(NormalizationError::InvalidState(
            "normalization rollback rows cannot be rolled back".to_owned(),
        ));
    }

    let target_kind: String = row.try_get("target_kind").map_err(map_sqlx)?;
    Ok(ApplicationForRollback {
        proposal_id: row.try_get("proposal_id").map_err(map_sqlx)?,
        target_kind: parse_target_kind(target_kind.as_str())?,
        target_id: row
            .try_get::<Option<Uuid>, _>("target_id")
            .map_err(map_sqlx)?
            .map(foundation_shared_kernel::ids::ComplexId::new),
        before_snapshot: row.try_get("before_snapshot").map_err(map_sqlx)?,
        after_snapshot: row.try_get("after_snapshot").map_err(map_sqlx)?,
    })
}

async fn ensure_no_existing_rollback(
    tx: &mut Transaction<'_, Postgres>,
    application_id: Uuid,
) -> Result<(), NormalizationError> {
    let existing = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM catalog.normalization_application
             WHERE rollback_of = $1
         )",
    )
    .bind(application_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    if existing {
        return Err(NormalizationError::InvalidState(
            "normalization application is already rolled back".to_owned(),
        ));
    }
    Ok(())
}

async fn insert_industrial_complex_application(
    tx: &mut Transaction<'_, Postgres>,
    command: &NormalizationApplicationCommand,
    receipt: &IndustrialComplexMutationReceipt,
) -> Result<(), NormalizationError> {
    sqlx::query(
        "INSERT INTO catalog.normalization_application
         (id, proposal_id, command_type, target_kind, target_id, expected_version,
          before_snapshot, after_snapshot, applied_by_principal_id, outbox_event_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(command.id)
    .bind(command.proposal_id)
    .bind(industrial_complex::APPLY_COMMAND_TYPE)
    .bind(NormalizationTargetKind::IndustrialComplex.wire_name())
    .bind(receipt.target_id.as_uuid())
    .bind(command.expected_version)
    .bind(receipt.before_snapshot.as_json())
    .bind(receipt.after_snapshot.as_json())
    .bind(command.applied_by_principal_id.as_uuid())
    .bind(receipt.outbox_event_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn insert_industrial_complex_rollback(
    tx: &mut Transaction<'_, Postgres>,
    command: &NormalizationRollbackCommand,
    application: &ApplicationForRollback,
    receipt: &IndustrialComplexMutationReceipt,
) -> Result<(), NormalizationError> {
    sqlx::query(
        "INSERT INTO catalog.normalization_application
         (id, proposal_id, command_type, target_kind, target_id, expected_version,
          before_snapshot, after_snapshot, applied_by_principal_id, rollback_of, outbox_event_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(command.id)
    .bind(application.proposal_id)
    .bind(industrial_complex::ROLLBACK_COMMAND_TYPE)
    .bind(NormalizationTargetKind::IndustrialComplex.wire_name())
    .bind(receipt.target_id.as_uuid())
    .bind(command.expected_current_version)
    .bind(receipt.before_snapshot.as_json())
    .bind(receipt.after_snapshot.as_json())
    .bind(command.rolled_back_by_principal_id.as_uuid())
    .bind(command.application_id)
    .bind(receipt.outbox_event_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}
