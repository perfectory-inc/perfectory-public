//! `PgCatalogUnitOfWork` — Catalog mutation + Outbox 이벤트를 한 sqlx 트랜잭션에 묶는다.
//!
//! ADR 0032 기둥 2 의 At-least-once invariant 의 인프라 측 책임자.
//! 각 메서드:
//! 1. `pool.begin()` → `Transaction<'_, Postgres>` 획득
//! 2. 비즈니스 row INSERT/UPDATE — 같은 `&mut *tx` 사용
//! 3. `outbox_event` row INSERT — 같은 tx
//! 4. `tx.commit()` — 어느 단계든 `?` 로 early return 시 Drop 으로 자동 rollback
//!
//! 이로써 partial failure 가 불가능: complex 만 INSERT 되고 outbox 가 비거나, 반대 경우 없음.

use async_trait::async_trait;
use catalog_application::ports::{
    CatalogUnitOfWork, MarkTileLayerDynamicCommand, RuntimeManifestPublicationCapability,
    UpsertIndustrialComplexCommand, VectorTileArtifactPromotionCommand, VectorTileFileAssetCommand,
    VectorTileManifestPromotionCommand, VectorTileManifestRollbackCommand,
    VectorTileSourceRecordCommand,
};
use catalog_domain::{
    CatalogError, ComplexMutation, IndustrialComplex, Parcel, ParcelKind, RuntimeTileLayer,
    ServingGeneration, VectorTileArtifact, VectorTileManifest, VectorTileRuntimeManifest,
};
use chrono::Utc;
use foundation_shared_kernel::events::catalog_v1::{
    vector_tile_runtime_manifest_object_key, CatalogEvent, VectorTileManifestPromotedV1,
    VectorTileManifestRolledBackV1, VectorTileRuntimeManifestPublishedV2,
    VectorTileRuntimeUnitSelectionV2,
};
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, ParcelId, StaffId, VectorTileManifestId, VectorTileRuntimeManifestId,
};
use serde_json::Value as JsonValue;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::row_map::{
    is_unique_violation_code, map_sqlx, row_to_complex, row_to_parcel, row_to_vector_tile_artifact,
    row_to_vector_tile_manifest, u64_to_i64,
};
use crate::sqlx_repository::load_active_vector_tile_runtime_manifest;

/// `PostgreSQL` implementation of Catalog mutation unit-of-work ports.
pub struct PgCatalogUnitOfWork {
    pool: PgPool,
    runtime_manifest_publication: RuntimeManifestPublicationCapability,
}

impl PgCatalogUnitOfWork {
    /// Creates a unit-of-work backed by the given `PostgreSQL` pool.
    ///
    /// Publication of the public v2 runtime-manifest event is off. Three of the four production
    /// call sites have nothing to do with v2, so widening the constructor would make them all
    /// answer a question only one of them is asked; a capability that defaulted to on would also
    /// publish from any environment that forgot to say no.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self {
            pool,
            runtime_manifest_publication: RuntimeManifestPublicationCapability::disabled(),
        }
    }

    /// Records whether this deployment may emit the public v2 runtime-manifest event.
    ///
    /// The capability gates the event, not the activation: a deployment with publication off still
    /// records the switch in the internal ledger, which is what keeps v1 behaviour byte-identical
    /// while v2 rolls out.
    #[must_use]
    pub const fn with_runtime_manifest_publication(
        mut self,
        capability: RuntimeManifestPublicationCapability,
    ) -> Self {
        self.runtime_manifest_publication = capability;
        self
    }
}

#[async_trait]
impl CatalogUnitOfWork for PgCatalogUnitOfWork {
    async fn create_complex(&self, complex: &IndustrialComplex) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let area_i64 = u64_to_i64(complex.area_m2)?;
        let insert_res = sqlx::query(
            "INSERT INTO catalog.industrial_complex
             (id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
              created_at, updated_at, version)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(complex.id.as_uuid())
        .bind(&complex.official_complex_code)
        .bind(&complex.name)
        .bind(complex.kind.wire_name())
        .bind(&complex.primary_bjdong_code)
        .bind(area_i64)
        .bind(complex.created_at)
        .bind(complex.updated_at)
        .bind(complex.version)
        .execute(&mut *tx)
        .await;

        match insert_res {
            Ok(_) => {}
            Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => {
                return Err(map_industrial_complex_unique_violation(
                    db.constraint(),
                    complex.official_complex_code.as_str(),
                ));
            }
            Err(e) => return Err(map_sqlx(e)),
        }

        let event = CatalogEvent::IndustrialComplexCreatedV2(complex.created_event());
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }

    async fn upsert_complexes_by_official_code(
        &self,
        commands: &[UpsertIndustrialComplexCommand],
    ) -> Result<Vec<IndustrialComplex>, CatalogError> {
        upsert_industrial_complexes_by_official_code(&self.pool, commands).await
    }

    async fn update_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        mutate: ComplexMutation,
    ) -> Result<IndustrialComplex, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let area_i64 = mutate.area_m2.map(u64_to_i64).transpose()?;

        let row = sqlx::query(
            "UPDATE catalog.industrial_complex
             SET name      = COALESCE($3, name),
                 area_m2   = COALESCE($4, area_m2),
                 updated_at = now(),
                 version   = version + 1
             WHERE id = $1 AND version = $2 AND archived_at IS NULL
             RETURNING id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                       created_at, updated_at, archived_at, version",
        )
        .bind(id.as_uuid())
        .bind(expected_version)
        .bind(mutate.name.as_deref())
        .bind(area_i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let updated = if let Some(r) = row {
            row_to_complex(&r)?
        } else {
            // 진단 조회: row 가 존재하는데 version 만 다른가, 아예 없는가?
            let current: Option<(i64, Option<chrono::DateTime<Utc>>)> = sqlx::query_as(
                "SELECT version, archived_at FROM catalog.industrial_complex WHERE id = $1",
            )
            .bind(id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx)?;
            return Err(current.map_or_else(
                || CatalogError::ComplexNotFound(id.to_string()),
                |(current_version, archived_at)| {
                    if archived_at.is_some() {
                        CatalogError::ComplexAlreadyArchived(id.to_string())
                    } else {
                        CatalogError::ComplexVersionConflict {
                            expected: expected_version,
                            current: current_version,
                        }
                    }
                },
            ));
        };

        let event = CatalogEvent::IndustrialComplexUpdated(updated.updated_event(&mutate));
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(updated)
    }

    async fn archive_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        operator_staff_id: StaffId,
        reason: Option<String>,
        request_id: Option<String>,
    ) -> Result<IndustrialComplex, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let row = sqlx::query(
            "UPDATE catalog.industrial_complex
             SET archived_at = now(),
                 archived_by_staff_id = $3,
                 archive_reason = $4,
                 updated_at = now(),
                 version = version + 1
             WHERE id = $1 AND version = $2 AND archived_at IS NULL
             RETURNING id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                       created_at, updated_at, archived_at, version",
        )
        .bind(id.as_uuid())
        .bind(expected_version)
        .bind(operator_staff_id.as_uuid())
        .bind(reason.as_deref())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let archived = if let Some(r) = row {
            row_to_complex(&r)?
        } else {
            let current: Option<(i64, Option<chrono::DateTime<Utc>>)> = sqlx::query_as(
                "SELECT version, archived_at FROM catalog.industrial_complex WHERE id = $1",
            )
            .bind(id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx)?;
            return Err(current.map_or_else(
                || CatalogError::ComplexNotFound(id.to_string()),
                |(current_version, archived_at)| {
                    if archived_at.is_some() {
                        CatalogError::ComplexAlreadyArchived(id.to_string())
                    } else {
                        CatalogError::ComplexVersionConflict {
                            expected: expected_version,
                            current: current_version,
                        }
                    }
                },
            ));
        };

        let event = CatalogEvent::IndustrialComplexArchived(archived.archived_event(
            operator_staff_id,
            reason,
            request_id,
        ));
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(archived)
    }

    async fn update_parcel_kind(
        &self,
        id: ParcelId,
        expected_version: i64,
        new_kind: ParcelKind,
    ) -> Result<Parcel, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        // 변경 전 row 를 `FOR UPDATE` 로 잠가 event payload 의 previous_kind 가 race-free.
        let before_row = sqlx::query(
            "SELECT id, complex_id, pnu, kind, area_m2, created_at, updated_at, version
             FROM catalog.parcel
             WHERE id = $1
             FOR UPDATE",
        )
        .bind(id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let before = if let Some(r) = before_row {
            row_to_parcel(&r)?
        } else {
            return Err(CatalogError::ParcelNotFound(id.to_string()));
        };

        if before.version != expected_version {
            return Err(CatalogError::ComplexVersionConflict {
                expected: expected_version,
                current: before.version,
            });
        }

        let updated_row = sqlx::query(
            "UPDATE catalog.parcel
             SET kind = $3,
                 updated_at = now(),
                 version = version + 1
             WHERE id = $1 AND version = $2
             RETURNING id, complex_id, pnu, kind, area_m2, created_at, updated_at, version",
        )
        .bind(id.as_uuid())
        .bind(expected_version)
        .bind(new_kind.wire_name())
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let updated = row_to_parcel(&updated_row)?;

        let event = CatalogEvent::ParcelKindChanged(before.kind_changed_event(new_kind));
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(updated)
    }

    async fn rollback_vector_tile_manifest(
        &self,
        command: VectorTileManifestRollbackCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        let to_version = command.to_version.trim();
        if to_version.is_empty() {
            return Err(CatalogError::InvalidVectorTileManifestRollback(
                "to_version must not be empty".to_owned(),
            ));
        }

        let expected_current_version = command.expected_current_version.trim();
        if expected_current_version.is_empty() {
            return Err(CatalogError::InvalidVectorTileManifestRollback(
                "expected_current_version must not be empty".to_owned(),
            ));
        }

        let reason = command.reason.trim();
        if reason.is_empty() {
            return Err(CatalogError::InvalidVectorTileManifestRollback(
                "reason must not be empty".to_owned(),
            ));
        }

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let active_row = sqlx::query(
            "SELECT id, current_version
             FROM catalog.vector_tile_manifest
             WHERE is_active = true
             ORDER BY published_at DESC
             LIMIT 1
             FOR UPDATE",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| CatalogError::VectorTileManifestNotFound("active".to_owned()))?;
        let active_id: Uuid = active_row.try_get("id").map_err(map_sqlx)?;
        let active_version: String = active_row.try_get("current_version").map_err(map_sqlx)?;

        if active_version != expected_current_version {
            return Err(CatalogError::VectorTileManifestVersionConflict {
                expected: expected_current_version.to_owned(),
                current: active_version,
            });
        }

        if active_version == to_version {
            return Err(CatalogError::InvalidVectorTileManifestRollback(format!(
                "{to_version} is already active"
            )));
        }

        let target_row = sqlx::query(
            "SELECT id
             FROM catalog.vector_tile_manifest
             WHERE current_version = $1
             FOR UPDATE",
        )
        .bind(to_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| CatalogError::VectorTileManifestNotFound(to_version.to_owned()))?;
        let target_id: Uuid = target_row.try_get("id").map_err(map_sqlx)?;

        sqlx::query(
            "UPDATE catalog.vector_tile_manifest
             SET is_active = false,
                 updated_at = now(),
                 version = version + 1
             WHERE id = $1",
        )
        .bind(active_id)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            "UPDATE catalog.vector_tile_manifest
             SET is_active = true,
                 previous_version = $2,
                 published_at = now(),
                 updated_at = now(),
                 version = version + 1
             WHERE id = $1",
        )
        .bind(target_id)
        .bind(&active_version)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let manifest = load_vector_tile_manifest_by_id_tx(&mut tx, target_id).await?;
        let event = CatalogEvent::VectorTileManifestRolledBack(VectorTileManifestRolledBackV1 {
            schema_version: 1,
            manifest_id: VectorTileManifestId::new(target_id),
            previous_manifest_id: VectorTileManifestId::new(active_id),
            current_version: manifest.current_version.clone(),
            previous_version: manifest.previous_version.clone(),
            expected_current_version: expected_current_version.to_owned(),
            operator_staff_id: command.operator_staff_id,
            request_id: command.request_id,
            rollback_reason: reason.to_owned(),
            rolled_back_at: Utc::now(),
        });
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(manifest)
    }

    async fn promote_vector_tile_manifest(
        &self,
        command: VectorTileManifestPromotionCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        validate_promotion_command(&command)?;

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let active = lock_active_vector_tile_manifest_tx(&mut tx).await?;
        ensure_promote_can_replace_active(&active, &command)?;
        let manifest_id = Uuid::now_v7();
        let source_record_id = Uuid::now_v7();
        let manifest_file_asset_id = Uuid::now_v7();

        insert_promoted_vector_tile_manifest_tx(
            &mut tx,
            manifest_id,
            source_record_id,
            manifest_file_asset_id,
            &active.current_version,
            &command,
        )
        .await?;

        switch_active_vector_tile_manifest_tx(
            &mut tx,
            active.id,
            manifest_id,
            &active.current_version,
        )
        .await?;
        let manifest = load_vector_tile_manifest_by_id_tx(&mut tx, manifest_id).await?;
        let event = promoted_vector_tile_manifest_event(&manifest, active.id, &command);
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(manifest)
    }

    async fn promote_vector_tile_runtime_manifest(
        &self,
        expected_manifest_id: Option<Uuid>,
        next_manifest_id: Uuid,
    ) -> Result<u64, CatalogError> {
        let generation: i64 =
            sqlx::query_scalar("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
                .bind(expected_manifest_id)
                .bind(next_manifest_id)
                .fetch_one(&self.pool)
                .await
                .map_err(map_runtime_manifest_gate_error)?;
        u64::try_from(generation).map_err(|error| {
            CatalogError::InvalidVectorTileRuntimeManifest(format!(
                "promoted runtime manifest generation is invalid: {error}"
            ))
        })
    }

    async fn mark_tile_layer_dynamic(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        // The fixed order is pointer -> publication unit -> release, and it is fixed *here* rather
        // than left to whichever statement happens to touch a row first. Every code path that
        // changes a serving source takes these three in this sequence.
        let current_manifest_id = lock_runtime_manifest_pointer_tx(&mut tx).await?;
        let units = lock_publication_units_tx(&mut tx).await?;
        let carried = current_manifest_selections_tx(&mut tx, current_manifest_id).await?;

        let target = find_publication_unit(&units, &command.unit_key)?;
        let activated_release_id = Uuid::now_v7();
        let selections =
            plan_dynamic_activation(&units, target, &carried, &command, activated_release_id)?;
        lock_release_rows_tx(
            &mut tx,
            &carried_release_ids(&selections, activated_release_id),
        )
        .await?;

        insert_dynamic_release_tx(&mut tx, activated_release_id, target.id, &command).await?;
        insert_release_layers_tx(&mut tx, activated_release_id, &command.layers).await?;

        let manifest_id = Uuid::now_v7();
        insert_runtime_manifest_tx(&mut tx, manifest_id, &command).await?;
        insert_manifest_units_tx(&mut tx, manifest_id, &selections).await?;

        // The gate owns the compare-and-swap, the completeness check, and every unit pointer
        // update; it also re-takes the pointer lock this transaction already holds, which is a
        // no-op rather than an upgrade precisely because we took the stronger mode first.
        sqlx::query_scalar::<_, i64>("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
            .bind(current_manifest_id)
            .bind(manifest_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_runtime_manifest_gate_error)?;

        // Read back inside the transaction. A post-commit read through the pool could observe a
        // later promotion and return a manifest this caller never published.
        let manifest = load_active_vector_tile_runtime_manifest(&mut tx)
            .await?
            .ok_or_else(|| {
                CatalogError::InvalidVectorTileRuntimeManifest(
                    "the promotion gate left no active runtime manifest".to_owned(),
                )
            })?;

        if self.runtime_manifest_publication.is_enabled() {
            insert_outbox_event(&mut tx, &runtime_manifest_published_event(&manifest)).await?;
        }

        tx.commit().await.map_err(map_sqlx)?;
        Ok(manifest)
    }
}

/// One publication unit as the activation transaction observed it under `FOR UPDATE`.
struct LockedPublicationUnit {
    id: Uuid,
    unit_key: String,
    active_release_id: Option<Uuid>,
    serving_generation: i64,
}

/// A selection the currently active manifest already made for one publication unit.
struct CarriedSelection {
    release_id: Uuid,
    data_revision: Uuid,
    canonical_iceberg_snapshot_id: String,
}

/// One unit's row in the manifest being assembled.
struct ManifestUnitSelection {
    publication_unit_id: Uuid,
    release_id: Uuid,
    serving_generation: i64,
    data_revision: Uuid,
    canonical_iceberg_snapshot_id: String,
}

/// Maps the promotion gate's own refusals to the typed runtime-manifest error.
///
/// `40001` is the compare-and-swap conflict and the non-monotonic generation; `23514` is every
/// state-machine rule the gate repeats — completeness, first-publication-is-dynamic, static uses the
/// selected revision, no serving-generation gap, release-addressed static identity. All of them say
/// the manifest offered for promotion is not a valid publication, which is what this variant means.
/// Mapping them to an infrastructure error reported a caller's invalid manifest as a server fault.
fn map_runtime_manifest_gate_error(error: sqlx::Error) -> CatalogError {
    match error {
        sqlx::Error::Database(database)
            if matches!(database.code().as_deref(), Some("40001" | "23514")) =>
        {
            CatalogError::InvalidVectorTileRuntimeManifest(database.message().to_owned())
        }
        other => map_sqlx(other),
    }
}

/// Takes the first lock of the fixed order and reads the pointer it protects.
///
/// `SHARE ROW EXCLUSIVE` is the mode `catalog.promote_vector_tile_runtime_manifest` takes on this
/// relation. Taking it up front means the gate's own `LOCK TABLE`, later in the same transaction, is
/// a repeat acquisition instead of an upgrade — two transactions each holding the weaker `ROW SHARE`
/// and both trying to upgrade would deadlock. The mode conflicts with itself, so activations
/// serialize, and it does not conflict with `ACCESS SHARE`, so Martin's view keeps reading the
/// current pointer while one is in flight.
async fn lock_runtime_manifest_pointer_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<Uuid>, CatalogError> {
    sqlx::query(
        "LOCK TABLE catalog.vector_tile_runtime_manifest_pointer IN SHARE ROW EXCLUSIVE MODE",
    )
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    sqlx::query_scalar(
        "SELECT manifest_id
         FROM catalog.vector_tile_runtime_manifest_pointer
         WHERE singleton = true
         FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)
}

/// Takes the second lock of the fixed order: every publication unit, in one statement.
///
/// Every unit rather than only the one being switched. The gate refuses a manifest that does not
/// select all of them and rewrites all of their pointers, so this transaction reads and changes
/// every unit whether or not the caller named it. `ORDER BY unit_key` fixes the acquisition order in
/// the statement instead of leaving it to the planner.
async fn lock_publication_units_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<LockedPublicationUnit>, CatalogError> {
    let rows = sqlx::query(
        "SELECT id, unit_key, active_release_id, serving_generation
         FROM catalog.vector_tile_publication_unit
         ORDER BY unit_key
         FOR UPDATE",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    rows.iter()
        .map(|row| {
            Ok(LockedPublicationUnit {
                id: row.try_get("id").map_err(map_sqlx)?,
                unit_key: row.try_get("unit_key").map_err(map_sqlx)?,
                active_release_id: row.try_get("active_release_id").map_err(map_sqlx)?,
                serving_generation: row.try_get("serving_generation").map_err(map_sqlx)?,
            })
        })
        .collect()
}

/// Takes the last lock of the fixed order over the releases this manifest will name.
///
/// `FOR SHARE`, not `FOR UPDATE`: a release row is immutable once written, so what this transaction
/// needs is for the rows to still be there when the gate resolves the manifest's foreign keys, not
/// the right to change them.
async fn lock_release_rows_tx(
    tx: &mut Transaction<'_, Postgres>,
    release_ids: &[Uuid],
) -> Result<(), CatalogError> {
    if release_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "SELECT id
         FROM catalog.vector_tile_release
         WHERE id = ANY($1)
         ORDER BY id
         FOR SHARE",
    )
    .bind(release_ids)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Reads the selections the active manifest already made, keyed by publication unit.
///
/// A one-unit edit still has to publish a complete manifest, so the other units' selections are
/// carried forward from the pointer rather than recomputed — recomputing them from the unit rows
/// would let a half-committed neighbour leak into a manifest this transaction claims is complete.
async fn current_manifest_selections_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Option<Uuid>,
) -> Result<BTreeMap<Uuid, CarriedSelection>, CatalogError> {
    let Some(manifest_id) = manifest_id else {
        return Ok(BTreeMap::new());
    };
    let rows = sqlx::query(
        "SELECT publication_unit_id, release_id, data_revision, canonical_iceberg_snapshot_id
         FROM catalog.vector_tile_runtime_manifest_unit
         WHERE manifest_id = $1
         ORDER BY publication_unit_id",
    )
    .bind(manifest_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    let mut selections = BTreeMap::new();
    for row in &rows {
        let publication_unit_id: Uuid = row.try_get("publication_unit_id").map_err(map_sqlx)?;
        selections.insert(
            publication_unit_id,
            CarriedSelection {
                release_id: row.try_get("release_id").map_err(map_sqlx)?,
                data_revision: row.try_get("data_revision").map_err(map_sqlx)?,
                canonical_iceberg_snapshot_id: row
                    .try_get("canonical_iceberg_snapshot_id")
                    .map_err(map_sqlx)?,
            },
        );
    }
    Ok(selections)
}

fn find_publication_unit<'units>(
    units: &'units [LockedPublicationUnit],
    unit_key: &str,
) -> Result<&'units LockedPublicationUnit, CatalogError> {
    units
        .iter()
        .find(|unit| unit.unit_key == unit_key)
        .ok_or_else(|| {
            // Units are seeded, not created by an activation. Creating one here would let a typo in
            // `unit_key` publish a second unit nobody configured — and, because the gate demands a
            // complete manifest, break every later publication until someone noticed.
            CatalogError::InvalidVectorTileRuntimeManifest(format!(
                "publication unit {unit_key} does not exist"
            ))
        })
}

/// Builds the complete manifest selection: the activated unit plus every other unit carried over.
///
/// Two rules the promotion gate enforces are answered here so the failure names the unit rather
/// than arriving as a check-constraint violation: a manifest must select every publication unit, and
/// each selected unit's serving generation must be the value that unit's transition implies —
/// `1` for a first publication, one past the current value when the release changes, and the current
/// value when the manifest re-selects the release the unit already serves. A carried unit's source
/// selection did not change, so its generation does not move; see
/// `20260730000003_serving_generation_tracks_one_unit_source_selection.sql`.
fn plan_dynamic_activation(
    units: &[LockedPublicationUnit],
    target: &LockedPublicationUnit,
    carried: &BTreeMap<Uuid, CarriedSelection>,
    command: &MarkTileLayerDynamicCommand,
    activated_release_id: Uuid,
) -> Result<Vec<ManifestUnitSelection>, CatalogError> {
    units
        .iter()
        .map(|unit| {
            if unit.id == target.id {
                return Ok(ManifestUnitSelection {
                    publication_unit_id: unit.id,
                    release_id: activated_release_id,
                    serving_generation: next_serving_generation(unit, command)?,
                    data_revision: command.data_revision.as_uuid(),
                    canonical_iceberg_snapshot_id: command
                        .canonical_iceberg_snapshot_id
                        .as_str()
                        .to_owned(),
                });
            }
            let selection = carried.get(&unit.id).ok_or_else(|| {
                CatalogError::InvalidVectorTileRuntimeManifest(format!(
                    "publication unit {} has never published, so no complete manifest can be \
                     assembled; activate it before switching {}",
                    unit.unit_key, command.unit_key
                ))
            })?;
            Ok(ManifestUnitSelection {
                publication_unit_id: unit.id,
                release_id: selection.release_id,
                serving_generation: unit.serving_generation,
                data_revision: selection.data_revision,
                canonical_iceberg_snapshot_id: selection.canonical_iceberg_snapshot_id.clone(),
            })
        })
        .collect()
}

/// Resolves the activated unit's next serving generation, refusing a stale observation.
///
/// The stored `serving_generation` is not on its own evidence of what the caller could have seen:
/// the column defaults to 1, so a unit that has never published carries 1 while having no active
/// release at all. Only `active_release_id` distinguishes the two states, which is why a first
/// publication is exactly "both expectations absent" and anything else is compared against both.
fn next_serving_generation(
    unit: &LockedPublicationUnit,
    command: &MarkTileLayerDynamicCommand,
) -> Result<i64, CatalogError> {
    let expected_release = command
        .expected_active_release_id
        .map(|release_id| release_id.as_uuid());
    let expected_generation = command
        .expected_serving_generation
        .map(ServingGeneration::value);

    match unit.active_release_id {
        None => {
            if expected_release.is_some() || expected_generation.is_some() {
                return Err(serving_state_conflict(
                    unit,
                    expected_release,
                    expected_generation,
                ));
            }
            Ok(1)
        }
        Some(active_release_id) => {
            let observed = observed_serving_generation(unit)?;
            if expected_release != Some(active_release_id) || expected_generation != Some(observed)
            {
                return Err(serving_state_conflict(
                    unit,
                    expected_release,
                    expected_generation,
                ));
            }
            advance_serving_generation(unit)
        }
    }
}

/// The next generation for a unit whose selected release changes.
///
/// Only for the unit being switched. A carried unit keeps its generation, because the value tracks
/// one unit's source selection and a carry-forward changes nothing about it.
fn advance_serving_generation(unit: &LockedPublicationUnit) -> Result<i64, CatalogError> {
    unit.serving_generation.checked_add(1).ok_or_else(|| {
        CatalogError::InvalidVectorTileRuntimeManifest(format!(
            "publication unit {} has exhausted its serving generation",
            unit.unit_key
        ))
    })
}

fn observed_serving_generation(unit: &LockedPublicationUnit) -> Result<u64, CatalogError> {
    u64::try_from(unit.serving_generation).map_err(|error| {
        CatalogError::Infrastructure(format!(
            "publication unit {} has a negative serving generation: {error}",
            unit.unit_key
        ))
    })
}

fn serving_state_conflict(
    unit: &LockedPublicationUnit,
    expected_release: Option<Uuid>,
    expected_generation: Option<u64>,
) -> CatalogError {
    // The stored generation is only reportable alongside an active release: without one it is the
    // column default, not something the caller could have observed.
    let current = unit.active_release_id.map_or_else(
        || describe_serving_state(None, None),
        |active_release_id| {
            describe_serving_state(
                Some(active_release_id),
                u64::try_from(unit.serving_generation).ok(),
            )
        },
    );
    CatalogError::VectorTileServingStateConflict {
        unit_key: unit.unit_key.clone(),
        expected: describe_serving_state(expected_release, expected_generation),
        current,
    }
}

/// Renders an observed-state pair the way the command carries it: both present, or both absent.
fn describe_serving_state(release_id: Option<Uuid>, generation: Option<u64>) -> String {
    match (release_id, generation) {
        (None, None) => "unpublished".to_owned(),
        (release_id, generation) => format!(
            "release={} generation={}",
            release_id.map_or_else(|| "none".to_owned(), |id| id.to_string()),
            generation.map_or_else(|| "none".to_owned(), |value| value.to_string()),
        ),
    }
}

/// Every release the assembled manifest names, minus the one this transaction is about to insert.
///
/// The new release cannot be locked before it exists, and does not need to be: no other transaction
/// can reach a row this one has not committed.
fn carried_release_ids(
    selections: &[ManifestUnitSelection],
    activated_release_id: Uuid,
) -> Vec<Uuid> {
    selections
        .iter()
        .map(|selection| selection.release_id)
        .filter(|release_id| *release_id != activated_release_id)
        .collect()
}

async fn insert_dynamic_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    release_id: Uuid,
    publication_unit_id: Uuid,
    command: &MarkTileLayerDynamicCommand,
) -> Result<(), CatalogError> {
    let source_file_asset_ids = command
        .lineage
        .source_file_asset_ids
        .iter()
        .map(FileAssetId::as_uuid)
        .collect::<Vec<_>>();
    let insert = sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_record_id, source_file_asset_ids, source_kind, martin_source_id,
          tiles_url_template, postgis_projection_revision)
         VALUES ($1, $2, $3, $4, $5, $6, 'dynamic_postgis', $7, $8, $9)",
    )
    .bind(release_id)
    .bind(publication_unit_id)
    .bind(command.data_revision.as_uuid())
    .bind(command.canonical_iceberg_snapshot_id.as_str())
    .bind(command.lineage.source_record_id.as_uuid())
    .bind(&source_file_asset_ids)
    .bind(&command.martin_source_id)
    .bind(command.tiles_url_template.as_str())
    .bind(command.postgis_projection_revision.as_uuid())
    .execute(&mut **tx)
    .await;

    match insert {
        Ok(_) => Ok(()),
        // `vector_tile_release_unit_revision_snapshot_kind_key`. This is what a replayed activation
        // hits: the unit already has a dynamic release for exactly this revision and snapshot, so
        // the retry is refused instead of publishing a second one.
        Err(sqlx::Error::Database(database))
            if is_unique_violation_code(database.code().as_deref()) =>
        {
            Err(CatalogError::InvalidVectorTileRuntimeManifest(format!(
                "publication unit {} already has a dynamic release for data revision {} at snapshot {}",
                command.unit_key,
                command.data_revision,
                command.canonical_iceberg_snapshot_id.as_str()
            )))
        }
        Err(error) => Err(map_sqlx(error)),
    }
}

async fn insert_release_layers_tx(
    tx: &mut Transaction<'_, Postgres>,
    release_id: Uuid,
    layers: &BTreeMap<String, RuntimeTileLayer>,
) -> Result<(), CatalogError> {
    for (layer_id, layer) in layers {
        let filter_properties = serde_json::to_value(&layer.feature_filter_properties)
            .map_err(|error| CatalogError::Infrastructure(format!("serde encode: {error}")))?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release_layer
             (release_id, layer_id, source_layer, feature_id_property,
              tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
              feature_filter_properties)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(release_id)
        .bind(layer_id)
        .bind(&layer.source_layer)
        .bind(layer.feature_id_property.as_str())
        .bind(i16::from(layer.tile_min_zoom))
        .bind(i16::from(layer.tile_max_zoom))
        .bind(i16::from(layer.render_min_zoom))
        .bind(i16::from(layer.render_max_zoom))
        .bind(&filter_properties)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

/// Writes the immutable manifest row and preallocates the `file_asset` identity of its projection.
///
/// The object key is derived, not supplied: `vector_tile_runtime_manifest_object_key` is the one
/// definition of the create-only layout, so the row and the object that will later be written under
/// it cannot disagree. Size zero and no checksum are the honest description of an identity reserved
/// before the bytes exist.
///
/// The generation is derived under the pointer lock rather than read earlier and passed in — the
/// gate refuses a generation that does not increase, and only a value taken while the pointer is
/// held is still true when the gate compares it.
async fn insert_runtime_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    command: &MarkTileLayerDynamicCommand,
) -> Result<(), CatalogError> {
    let manifest_file_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
         VALUES ($1, $2, 'application/json', 0, $3, 'public', 1)",
    )
    .bind(manifest_file_asset_id)
    .bind(vector_tile_runtime_manifest_object_key(
        VectorTileRuntimeManifestId::new(manifest_id),
    ))
    .bind(command.lineage.source_record_id.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    sqlx::query(
        "INSERT INTO catalog.vector_tile_runtime_manifest
         (id, manifest_generation, manifest_file_asset_id)
         SELECT $1, coalesce(max(manifest_generation), 0) + 1, $2
         FROM catalog.vector_tile_runtime_manifest",
    )
    .bind(manifest_id)
    .bind(manifest_file_asset_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn insert_manifest_units_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    selections: &[ManifestUnitSelection],
) -> Result<(), CatalogError> {
    for selection in selections {
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
             (manifest_id, publication_unit_id, release_id, serving_generation,
              data_revision, canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(manifest_id)
        .bind(selection.publication_unit_id)
        .bind(selection.release_id)
        .bind(selection.serving_generation)
        .bind(selection.data_revision)
        .bind(&selection.canonical_iceberg_snapshot_id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

/// Builds the additive v2 event from the manifest the gate actually published.
///
/// From the manifest, not from the command: the event then reports what serving generation and
/// which units the pointer really names, including the neighbours this activation carried forward.
fn runtime_manifest_published_event(manifest: &VectorTileRuntimeManifest) -> CatalogEvent {
    CatalogEvent::VectorTileRuntimeManifestPublished(VectorTileRuntimeManifestPublishedV2 {
        schema_version: 2,
        manifest_id: manifest.current_version,
        manifest_generation: manifest.manifest_generation.value(),
        publication_units: manifest
            .publication_units
            .iter()
            .map(|(unit_key, unit)| {
                (
                    unit_key.clone(),
                    VectorTileRuntimeUnitSelectionV2 {
                        active_release_id: unit.active_release_id,
                        data_revision: unit.data_revision,
                        serving_generation: unit.serving_generation.value(),
                        canonical_iceberg_snapshot_id: unit
                            .canonical_iceberg_snapshot_id
                            .as_str()
                            .to_owned(),
                    },
                )
            })
            .collect(),
        published_at: manifest.published_at,
    })
}

async fn upsert_industrial_complexes_by_official_code(
    pool: &PgPool,
    commands: &[UpsertIndustrialComplexCommand],
) -> Result<Vec<IndustrialComplex>, CatalogError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }

    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let mut complexes = Vec::with_capacity(commands.len());

    for command in commands {
        let existing_row = sqlx::query(
            "SELECT id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                    created_at, updated_at, archived_at, version
             FROM catalog.industrial_complex
             WHERE official_complex_code = $1
               AND archived_at IS NULL
             FOR UPDATE",
        )
        .bind(&command.official_complex_code)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let complex = if let Some(row) = existing_row {
            upsert_existing_industrial_complex(&mut tx, command, &row).await?
        } else {
            insert_industrial_complex_from_upsert(&mut tx, command).await?
        };
        complexes.push(complex);
    }

    tx.commit().await.map_err(map_sqlx)?;
    Ok(complexes)
}

async fn upsert_existing_industrial_complex(
    tx: &mut Transaction<'_, Postgres>,
    command: &UpsertIndustrialComplexCommand,
    row: &sqlx::postgres::PgRow,
) -> Result<IndustrialComplex, CatalogError> {
    let existing = row_to_complex(row)?;
    let changed_fields = changed_industrial_complex_fields(&existing, command);
    if changed_fields.is_empty() {
        return Ok(existing);
    }

    let updated = update_industrial_complex_from_upsert(tx, &existing, command).await?;
    let event =
        CatalogEvent::IndustrialComplexUpdated(updated.updated_fields_event(changed_fields));
    insert_outbox_event(tx, &event).await?;
    Ok(updated)
}

async fn update_industrial_complex_from_upsert(
    tx: &mut Transaction<'_, Postgres>,
    existing: &IndustrialComplex,
    command: &UpsertIndustrialComplexCommand,
) -> Result<IndustrialComplex, CatalogError> {
    let area_i64 = u64_to_i64(command.area_m2)?;
    let updated_row = sqlx::query(
        "UPDATE catalog.industrial_complex
         SET name = $2,
             kind = $3,
             primary_bjdong_code = $4,
             area_m2 = $5,
             updated_at = now(),
             version = version + 1
         WHERE id = $1
         RETURNING id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                   created_at, updated_at, archived_at, version",
    )
    .bind(existing.id.as_uuid())
    .bind(&command.name)
    .bind(command.kind.wire_name())
    .bind(&command.primary_bjdong_code)
    .bind(area_i64)
    .fetch_one(&mut **tx)
    .await;

    match updated_row {
        Ok(row) => row_to_complex(&row),
        Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => {
            Err(map_industrial_complex_unique_violation(
                db.constraint(),
                command.official_complex_code.as_str(),
            ))
        }
        Err(error) => Err(map_sqlx(error)),
    }
}

async fn insert_industrial_complex_from_upsert(
    tx: &mut Transaction<'_, Postgres>,
    command: &UpsertIndustrialComplexCommand,
) -> Result<IndustrialComplex, CatalogError> {
    let now = Utc::now();
    let area_i64 = u64_to_i64(command.area_m2)?;
    let inserted_row = sqlx::query(
        "INSERT INTO catalog.industrial_complex
         (id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
          created_at, updated_at, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)
         RETURNING id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                   created_at, updated_at, archived_at, version",
    )
    .bind(Uuid::now_v7())
    .bind(&command.official_complex_code)
    .bind(&command.name)
    .bind(command.kind.wire_name())
    .bind(&command.primary_bjdong_code)
    .bind(area_i64)
    .bind(now)
    .bind(now)
    .fetch_one(&mut **tx)
    .await;

    let inserted = match inserted_row {
        Ok(row) => row_to_complex(&row)?,
        Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => {
            return Err(map_industrial_complex_unique_violation(
                db.constraint(),
                command.official_complex_code.as_str(),
            ));
        }
        Err(error) => return Err(map_sqlx(error)),
    };
    let event = CatalogEvent::IndustrialComplexCreatedV2(inserted.created_event());
    insert_outbox_event(tx, &event).await?;
    Ok(inserted)
}

fn changed_industrial_complex_fields(
    existing: &IndustrialComplex,
    command: &UpsertIndustrialComplexCommand,
) -> Vec<String> {
    let mut fields = Vec::with_capacity(4);
    if existing.name != command.name {
        fields.push("name".to_owned());
    }
    if existing.kind != command.kind {
        fields.push("kind".to_owned());
    }
    if existing.primary_bjdong_code != command.primary_bjdong_code {
        fields.push("primary_bjdong_code".to_owned());
    }
    if existing.area_m2 != command.area_m2 {
        fields.push("area_m2".to_owned());
    }
    fields
}

fn map_industrial_complex_unique_violation(
    constraint: Option<&str>,
    official_complex_code: &str,
) -> CatalogError {
    match constraint {
        Some(
            "industrial_complex_official_complex_code_idx"
            | "industrial_complex_official_complex_code_key"
            | "industrial_complex_active_official_code_idx",
        ) => CatalogError::ComplexOfficialCodeConflict(official_complex_code.to_owned()),
        Some(other) => CatalogError::Infrastructure(format!(
            "unexpected industrial_complex unique constraint violation: {other}"
        )),
        None => CatalogError::Infrastructure(
            "unexpected industrial_complex unique constraint violation without constraint name"
                .to_owned(),
        ),
    }
}

struct ActiveVectorTileManifest {
    id: Uuid,
    current_version: String,
}

async fn lock_active_vector_tile_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<ActiveVectorTileManifest, CatalogError> {
    let row = sqlx::query(
        "SELECT id, current_version
         FROM catalog.vector_tile_manifest
         WHERE is_active = true
         ORDER BY published_at DESC
         LIMIT 1
         FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| CatalogError::VectorTileManifestNotFound("active".to_owned()))?;

    Ok(ActiveVectorTileManifest {
        id: row.try_get("id").map_err(map_sqlx)?,
        current_version: row.try_get("current_version").map_err(map_sqlx)?,
    })
}

fn ensure_promote_can_replace_active(
    active: &ActiveVectorTileManifest,
    command: &VectorTileManifestPromotionCommand,
) -> Result<(), CatalogError> {
    if active.current_version != command.expected_current_version {
        return Err(CatalogError::VectorTileManifestVersionConflict {
            expected: command.expected_current_version.clone(),
            current: active.current_version.clone(),
        });
    }

    if active.current_version == command.current_version {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(format!(
            "{} is already active",
            command.current_version
        )));
    }

    Ok(())
}

async fn insert_promoted_vector_tile_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    source_record_id: Uuid,
    manifest_file_asset_id: Uuid,
    previous_version: &str,
    command: &VectorTileManifestPromotionCommand,
) -> Result<(), CatalogError> {
    insert_vector_tile_source_record_tx(tx, source_record_id, &command.source_record).await?;
    insert_vector_tile_file_asset_tx(
        tx,
        manifest_file_asset_id,
        source_record_id,
        &command.manifest_file_asset,
    )
    .await?;
    insert_vector_tile_manifest_row_tx(
        tx,
        manifest_id,
        source_record_id,
        manifest_file_asset_id,
        previous_version,
        command,
    )
    .await?;
    insert_vector_tile_artifacts_tx(tx, manifest_id, source_record_id, &command.artifacts).await
}

async fn insert_vector_tile_manifest_row_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    source_record_id: Uuid,
    manifest_file_asset_id: Uuid,
    previous_version: &str,
    command: &VectorTileManifestPromotionCommand,
) -> Result<(), CatalogError> {
    let manifest_insert = sqlx::query(
        "INSERT INTO catalog.vector_tile_manifest
         (id, current_version, previous_version, tiles_url_template,
          source_snapshot_id, manifest_file_asset_id, source_record_id, is_active, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, false, 1)",
    )
    .bind(manifest_id)
    .bind(&command.current_version)
    .bind(previous_version)
    .bind(&command.tiles_url_template)
    .bind(&command.source_snapshot_id)
    .bind(manifest_file_asset_id)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await;

    match manifest_insert {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => Err(
            CatalogError::VectorTileManifestAlreadyExists(command.current_version.clone()),
        ),
        Err(error) => Err(map_sqlx(error)),
    }
}

async fn insert_vector_tile_artifacts_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    source_record_id: Uuid,
    artifacts: &BTreeMap<String, VectorTileArtifactPromotionCommand>,
) -> Result<(), CatalogError> {
    for (layer, artifact) in artifacts {
        insert_vector_tile_artifact_tx(tx, manifest_id, source_record_id, layer, artifact).await?;
    }
    Ok(())
}

async fn switch_active_vector_tile_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    active_id: Uuid,
    manifest_id: Uuid,
    previous_version: &str,
) -> Result<(), CatalogError> {
    sqlx::query(
        "UPDATE catalog.vector_tile_manifest
         SET is_active = false,
             updated_at = now(),
             version = version + 1
         WHERE id = $1",
    )
    .bind(active_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    sqlx::query(
        "UPDATE catalog.vector_tile_manifest
         SET is_active = true,
             previous_version = $2,
             published_at = now(),
             updated_at = now(),
             version = version + 1
         WHERE id = $1",
    )
    .bind(manifest_id)
    .bind(previous_version)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    Ok(())
}

fn promoted_vector_tile_manifest_event(
    manifest: &VectorTileManifest,
    previous_manifest_id: Uuid,
    command: &VectorTileManifestPromotionCommand,
) -> CatalogEvent {
    CatalogEvent::VectorTileManifestPromoted(VectorTileManifestPromotedV1 {
        schema_version: 1,
        manifest_id: manifest.id,
        previous_manifest_id: VectorTileManifestId::new(previous_manifest_id),
        current_version: manifest.current_version.clone(),
        previous_version: manifest.previous_version.clone(),
        expected_current_version: command.expected_current_version.clone(),
        operator_staff_id: command.operator_staff_id,
        request_id: command.request_id.clone(),
        promoted_at: Utc::now(),
    })
}

fn validate_promotion_command(
    command: &VectorTileManifestPromotionCommand,
) -> Result<(), CatalogError> {
    if command.current_version.trim().is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "current_version must not be empty".to_owned(),
        ));
    }
    if command.expected_current_version.trim().is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "expected_current_version must not be empty".to_owned(),
        ));
    }
    if command.tiles_url_template.trim().is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "tiles_url_template must not be empty".to_owned(),
        ));
    }
    if command.source_snapshot_id.trim().is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "source_snapshot_id must not be empty".to_owned(),
        ));
    }
    if command.artifacts.is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "artifacts must not be empty".to_owned(),
        ));
    }
    Ok(())
}

async fn insert_vector_tile_source_record_tx(
    tx: &mut Transaction<'_, Postgres>,
    source_record_id: Uuid,
    source_record: &VectorTileSourceRecordCommand,
) -> Result<(), CatalogError> {
    sqlx::query(
        "INSERT INTO catalog.source_record
         (id, source, source_url, external_id, checksum_sha256, raw_object_key)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(source_record_id)
    .bind(source_record.source.trim())
    .bind(source_record.source_url.as_deref())
    .bind(source_record.external_id.as_deref())
    .bind(source_record.checksum_sha256.as_deref())
    .bind(source_record.raw_object_key.as_deref())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn insert_vector_tile_file_asset_tx(
    tx: &mut Transaction<'_, Postgres>,
    file_asset_id: Uuid,
    source_record_id: Uuid,
    file_asset: &VectorTileFileAssetCommand,
) -> Result<(), CatalogError> {
    let size_bytes = u64_to_i64(file_asset.size_bytes)?;
    let insert = sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, checksum_sha256, title,
          source_record_id, visibility, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)",
    )
    .bind(file_asset_id)
    .bind(file_asset.object_key.trim())
    .bind(file_asset.mime_type.trim())
    .bind(size_bytes)
    .bind(file_asset.checksum_sha256.as_deref())
    .bind(file_asset.title.as_deref())
    .bind(source_record_id)
    .bind(file_asset.visibility.trim())
    .execute(&mut **tx)
    .await;

    match insert {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => Err(
            CatalogError::FileAssetObjectKeyConflict(file_asset.object_key.clone()),
        ),
        Err(error) => Err(map_sqlx(error)),
    }
}

async fn insert_vector_tile_artifact_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    source_record_id: Uuid,
    layer: &str,
    artifact: &VectorTileArtifactPromotionCommand,
) -> Result<(), CatalogError> {
    let artifact_id = Uuid::now_v7();
    let tilejson_file_asset_id = Uuid::now_v7();
    insert_vector_tile_file_asset_tx(
        tx,
        tilejson_file_asset_id,
        source_record_id,
        &artifact.tilejson_file_asset,
    )
    .await?;

    sqlx::query(
        "INSERT INTO catalog.vector_tile_artifact
         (id, manifest_id, layer, source_layer, tile_min_zoom, tile_max_zoom,
          render_min_zoom, render_max_zoom, tilejson_file_asset_id, object_key_prefix,
          flat_tile_count, flat_tile_total_bytes, source_record_id, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 1)",
    )
    .bind(artifact_id)
    .bind(manifest_id)
    .bind(layer.trim())
    .bind(artifact.source_layer.trim())
    .bind(i16::from(artifact.tile_min_zoom))
    .bind(i16::from(artifact.tile_max_zoom))
    .bind(i16::from(artifact.render_min_zoom))
    .bind(i16::from(artifact.render_max_zoom))
    .bind(tilejson_file_asset_id)
    .bind(artifact.object_key_prefix.trim())
    .bind(u64_to_i64(artifact.flat_tile_count)?)
    .bind(u64_to_i64(artifact.flat_tile_total_bytes)?)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    for source_file_asset in &artifact.source_file_assets {
        let source_file_asset_id = Uuid::now_v7();
        insert_vector_tile_file_asset_tx(
            tx,
            source_file_asset_id,
            source_record_id,
            source_file_asset,
        )
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_artifact_source_file_asset
             (artifact_id, file_asset_id)
             VALUES ($1, $2)",
        )
        .bind(artifact_id)
        .bind(source_file_asset_id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }

    Ok(())
}

async fn load_vector_tile_manifest_by_id_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
) -> Result<VectorTileManifest, CatalogError> {
    let row = sqlx::query(
        "SELECT id, current_version, previous_version, tiles_url_template,
                source_snapshot_id, manifest_file_asset_id, source_record_id, published_at,
                created_at, updated_at, version
         FROM catalog.vector_tile_manifest
         WHERE id = $1",
    )
    .bind(manifest_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| CatalogError::VectorTileManifestNotFound(manifest_id.to_string()))?;

    let manifest_file_asset_id = FileAssetId::new(
        row.try_get::<Uuid, _>("manifest_file_asset_id")
            .map_err(map_sqlx)?,
    );
    let artifact_rows = sqlx::query(
        "SELECT vta.id, vta.manifest_id, vta.layer, vta.source_layer,
                vta.tile_min_zoom, vta.tile_max_zoom, vta.render_min_zoom,
                vta.render_max_zoom, vta.tilejson_file_asset_id,
                fa.object_key AS tilejson_object_key, vta.object_key_prefix,
                vta.flat_tile_count, vta.flat_tile_total_bytes,
                vta.source_record_id, vta.created_at, vta.updated_at, vta.version
         FROM catalog.vector_tile_artifact vta
         JOIN catalog.file_asset fa ON fa.id = vta.tilejson_file_asset_id
         WHERE vta.manifest_id = $1
         ORDER BY vta.layer",
    )
    .bind(manifest_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    let mut artifacts = Vec::<VectorTileArtifact>::with_capacity(artifact_rows.len());
    for artifact_row in &artifact_rows {
        let artifact_id: Uuid = artifact_row.try_get("id").map_err(map_sqlx)?;
        let source_file_asset_rows = sqlx::query(
            "SELECT file_asset_id
             FROM catalog.vector_tile_artifact_source_file_asset
             WHERE artifact_id = $1
             ORDER BY file_asset_id",
        )
        .bind(artifact_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        let source_file_asset_ids = source_file_asset_rows
            .iter()
            .map(|source_row| {
                source_row
                    .try_get::<Uuid, _>("file_asset_id")
                    .map(FileAssetId::new)
                    .map_err(map_sqlx)
            })
            .collect::<Result<Vec<_>, _>>()?;
        artifacts.push(row_to_vector_tile_artifact(
            artifact_row,
            manifest_file_asset_id,
            source_file_asset_ids,
        )?);
    }

    row_to_vector_tile_manifest(&row, artifacts)
}

/// 같은 sqlx tx 에 outbox row INSERT.
pub(crate) async fn insert_outbox_event(
    tx: &mut Transaction<'_, Postgres>,
    event: &CatalogEvent,
) -> Result<Uuid, CatalogError> {
    let event_id = Uuid::now_v7();
    let envelope = serde_json::to_value(event)
        .map_err(|e| CatalogError::Infrastructure(format!("serde encode: {e}")))?;
    let type_tag = extract_type_tag(&envelope)?;

    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at)
         VALUES ($1, $2, $3, now())",
    )
    .bind(event_id)
    .bind(type_tag)
    .bind(&envelope)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    Ok(event_id)
}

fn extract_type_tag(envelope: &JsonValue) -> Result<String, CatalogError> {
    envelope
        .get("type")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            CatalogError::Infrastructure(
                "CatalogEvent serialization missing 'type' tag — serde derive misconfigured".into(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{extract_type_tag, map_industrial_complex_unique_violation, CatalogError};
    use chrono::Utc;
    use foundation_shared_kernel::events::catalog_v1::{CatalogEvent, IndustrialComplexCreatedV1};
    use foundation_shared_kernel::ids::ComplexId;
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn type_tag_matches_wire_format() -> Result<(), Box<dyn Error>> {
        let event = CatalogEvent::IndustrialComplexCreated(IndustrialComplexCreatedV1 {
            schema_version: 1,
            complex_id: ComplexId::new(Uuid::nil()),
            name: "테스트".into(),
            primary_bjdong_code: "1111111111".into(),
            created_at: Utc::now(),
        });
        let json = serde_json::to_value(&event)?;
        assert_eq!(
            extract_type_tag(&json)?,
            "catalog.industrial_complex.created.v1"
        );
        Ok(())
    }

    #[test]
    fn active_official_code_unique_violation_maps_to_conflict() {
        let error = map_industrial_complex_unique_violation(
            Some("industrial_complex_active_official_code_idx"),
            "IC-001",
        );

        assert!(matches!(
            error,
            CatalogError::ComplexOfficialCodeConflict(code) if code == "IC-001"
        ));
    }
}
