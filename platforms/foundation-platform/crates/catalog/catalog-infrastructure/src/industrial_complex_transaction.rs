//! Caller-owned `PostgreSQL` transaction participant for canonical industrial-complex changes.

use catalog_application::industrial_complex_patch::{
    IndustrialComplexPatch, RestoreIndustrialComplexInput,
};
use catalog_domain::{CatalogError, ComplexMutation, IndustrialComplex};
use foundation_shared_kernel::events::catalog_v1::CatalogEvent;
use foundation_shared_kernel::ids::ComplexId;
use serde_json::Value as JsonValue;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::row_map::{map_sqlx, row_to_complex, u64_to_i64};
use crate::unit_of_work::insert_outbox_event;

/// JSON snapshot of canonical industrial-complex fields stored around a mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexSnapshot(JsonValue);

impl IndustrialComplexSnapshot {
    /// Borrows the stable JSON representation used by Catalog mutation ledgers.
    #[must_use]
    pub const fn as_json(&self) -> &JsonValue {
        &self.0
    }
}

/// Result of a canonical industrial-complex mutation inside a caller-owned transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexMutationReceipt {
    /// Canonical aggregate changed by the participant.
    pub target_id: ComplexId,
    /// Canonical state locked immediately before the mutation.
    pub before_snapshot: IndustrialComplexSnapshot,
    /// Canonical state returned by the update.
    pub after_snapshot: IndustrialComplexSnapshot,
    /// Catalog outbox row inserted in the same borrowed transaction.
    pub outbox_event_id: Uuid,
}

/// `PostgreSQL` participant for canonical industrial-complex mutation and outbox work.
///
/// The participant has no pool and can only operate through a transaction borrowed from its
/// caller. Transaction begin, commit, and rollback remain the caller's responsibility.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgIndustrialComplexTransactionParticipant;

impl PgIndustrialComplexTransactionParticipant {
    /// Creates a stateless transaction participant.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Applies a validated Catalog patch in the supplied transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when the target is missing, archived, stale, invalid, or when
    /// canonical/outbox persistence fails.
    pub async fn apply(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        target_id: ComplexId,
        expected_version: i64,
        patch: IndustrialComplexPatch,
    ) -> Result<IndustrialComplexMutationReceipt, CatalogError> {
        mutate(tx, target_id, expected_version, patch).await
    }

    /// Restores canonical fields parsed from a prior Catalog snapshot in the supplied transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when the target is missing, archived, stale, invalid, or when
    /// canonical/outbox persistence fails.
    pub async fn restore(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        expected_current_version: i64,
        input: RestoreIndustrialComplexInput,
    ) -> Result<IndustrialComplexMutationReceipt, CatalogError> {
        let target_id = input.target_id();
        let before = lock_industrial_complex(tx, target_id).await?;
        ensure_expected_version(&before, expected_current_version)?;
        input.validate_current(&before)?;
        mutate_locked(tx, before, input.into_patch()).await
    }
}

async fn mutate(
    tx: &mut Transaction<'_, Postgres>,
    target_id: ComplexId,
    expected_version: i64,
    patch: IndustrialComplexPatch,
) -> Result<IndustrialComplexMutationReceipt, CatalogError> {
    let before = lock_industrial_complex(tx, target_id).await?;
    ensure_expected_version(&before, expected_version)?;
    mutate_locked(tx, before, patch).await
}

async fn mutate_locked(
    tx: &mut Transaction<'_, Postgres>,
    before: IndustrialComplex,
    patch: IndustrialComplexPatch,
) -> Result<IndustrialComplexMutationReceipt, CatalogError> {
    let target_id = before.id;
    let mutation = patch.into_effective_mutation(&before)?;
    let after = update_industrial_complex(tx, target_id, &mutation).await?;
    let event = CatalogEvent::IndustrialComplexUpdated(after.updated_event(&mutation));
    let outbox_event_id = insert_outbox_event(tx, &event).await?;

    Ok(IndustrialComplexMutationReceipt {
        target_id,
        before_snapshot: industrial_complex_snapshot(&before),
        after_snapshot: industrial_complex_snapshot(&after),
        outbox_event_id,
    })
}

async fn lock_industrial_complex(
    tx: &mut Transaction<'_, Postgres>,
    complex_id: ComplexId,
) -> Result<IndustrialComplex, CatalogError> {
    let row = sqlx::query(
        "SELECT id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                created_at, updated_at, archived_at, version
         FROM catalog.industrial_complex
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(complex_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| CatalogError::ComplexNotFound(complex_id.to_string()))?;

    let complex = row_to_complex(&row)?;
    if complex.archived_at.is_some() {
        return Err(CatalogError::ComplexAlreadyArchived(complex_id.to_string()));
    }
    Ok(complex)
}

const fn ensure_expected_version(
    before: &IndustrialComplex,
    expected_version: i64,
) -> Result<(), CatalogError> {
    if before.version != expected_version {
        return Err(CatalogError::ComplexVersionConflict {
            expected: expected_version,
            current: before.version,
        });
    }
    Ok(())
}

async fn update_industrial_complex(
    tx: &mut Transaction<'_, Postgres>,
    complex_id: ComplexId,
    mutation: &ComplexMutation,
) -> Result<IndustrialComplex, CatalogError> {
    let area_i64 = mutation.area_m2.map(u64_to_i64).transpose()?;
    let row = sqlx::query(
        "UPDATE catalog.industrial_complex
         SET name = COALESCE($2, name),
             area_m2 = COALESCE($3, area_m2),
             updated_at = now(),
             version = version + 1
         WHERE id = $1 AND archived_at IS NULL
         RETURNING id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                   created_at, updated_at, archived_at, version",
    )
    .bind(complex_id.as_uuid())
    .bind(mutation.name.as_deref())
    .bind(area_i64)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    row_to_complex(&row)
}

fn industrial_complex_snapshot(complex: &IndustrialComplex) -> IndustrialComplexSnapshot {
    IndustrialComplexSnapshot(serde_json::json!({
        "id": complex.id.as_uuid(),
        "official_complex_code": complex.official_complex_code,
        "name": complex.name,
        "kind": complex.kind.wire_name(),
        "primary_bjdong_code": complex.primary_bjdong_code,
        "area_m2": complex.area_m2,
        "version": complex.version,
    }))
}
