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
    CatalogUnitOfWork, MarkTileLayerDynamicCommand, PromoteTileLayerStaticCommand,
    PublishedRuntimeManifest, RecordVectorTileBuildResultCommand,
    RuntimeManifestPublicationCapability, StartLakehouseBakeCommand,
    StartStaticReleaseReaddressCommand, StartVectorTileBuildCommand,
    UpsertIndustrialComplexCommand, UpsertIndustrialComplexEffect, UpsertIndustrialComplexOutcome,
    VectorTileArtifactPromotionCommand, VectorTileFileAssetCommand,
    VectorTileManifestPromotionCommand, VectorTileManifestRollbackCommand,
    VectorTileSourceRecordCommand,
};
use catalog_domain::{
    static_file_asset_id_for_build, static_release_id_for_build, static_release_martin_source_id,
    static_release_pmtiles_object_key, validate_build_promotion, validate_build_result_report,
    validate_build_snapshot_binding, BuildEvidenceDigest, CanonicalIcebergSnapshotId, CatalogError,
    CatalogMutationKind, ComplexMutation, IndustrialComplex, IndustrialComplexDevelopmentStage,
    IndustrialComplexLotSalesStatus, IndustrialComplexStatus, RequestFingerprint, RuntimeTileLayer,
    ServingGeneration, VectorTileArtifact, VectorTileBuildKind, VectorTileBuildOutcome,
    VectorTileBuildPromotionInput, VectorTileBuildPromotionVerdict, VectorTileBuildStatus,
    VectorTileManifest, VectorTileRuntimeManifest, CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION,
};
use chrono::Utc;
use foundation_shared_kernel::events::catalog_v1::{
    vector_tile_runtime_manifest_object_key, CatalogEvent, VectorTileManifestPromotedV1,
    VectorTileManifestRolledBackV1, VectorTileRuntimeManifestPublishedV2,
    VectorTileRuntimeUnitSelectionV2,
};
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, StaffId, VectorTileBuildJobId, VectorTileManifestId,
    VectorTileReleaseId, VectorTileRuntimeManifestId,
};
use serde_json::Value as JsonValue;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::industrial_complex_transaction::{
    ensure_expected_version, industrial_complex_snapshot, lock_industrial_complex,
    update_industrial_complex,
};
use crate::row_map::{
    is_unique_violation_code, map_sqlx, row_to_complex, row_to_vector_tile_artifact,
    row_to_vector_tile_manifest, u64_to_i64, INDUSTRIAL_COMPLEX_COLUMNS,
};
use crate::sqlx_repository::load_vector_tile_runtime_manifest_by_id;

mod dynamic_release;
mod industrial_complex_upsert;
mod lakehouse_bake;
mod manifest_promotion;
mod mutation_claim;
mod readdress;
mod static_release;

use dynamic_release::{
    advance_serving_generation, carried_release_ids, current_manifest_selections_tx,
    find_publication_unit, insert_dynamic_release_tx, insert_manifest_units_tx,
    insert_release_layers_tx, insert_runtime_manifest_tx, lock_publication_units_tx,
    lock_release_rows_tx, lock_runtime_manifest_pointer_tx, map_runtime_manifest_gate_error,
    observed_serving_generation, plan_dynamic_activation, runtime_manifest_published_event,
    serving_state_conflict, CarriedSelection, LockedPublicationUnit, ManifestUnitSelection,
};
use industrial_complex_upsert::{
    map_industrial_complex_unique_violation, upsert_industrial_complexes_by_official_code,
};
use manifest_promotion::{
    ensure_promote_can_replace_active, insert_promoted_vector_tile_manifest_tx,
    load_vector_tile_manifest_by_id_tx, lock_active_vector_tile_manifest_tx,
    promoted_vector_tile_manifest_event, switch_active_vector_tile_manifest_tx,
    validate_promotion_command,
};
use mutation_claim::{
    claim_build_key_tx, claim_build_start_key_tx, claim_manifest_mutation_key_tx,
    claim_mutation_key_tx, ensure_recorded_artifact_matches, record_mutation_outbox_event_tx,
    validate_build_artifact_identity,
};
use static_release::{
    invalid_runtime, lock_validated_build_tx, mark_superseded_build_tx, publish_static_release_tx,
    set_lock_timeout_tx, MutationClaim, StaticPromotionPlan,
};

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
            // `$20::numeric` because the progress percentage travels as exact decimal text: the
            // column is `numeric(5,2)` and binding an `f64` would store a value the source never
            // stated. The cast is what lets Postgres parse the text it will store.
            "INSERT INTO catalog.industrial_complex
             (id, official_complex_code, name, kind, primary_bjdong_code,
              area_m2, status, sido_code, sigungu_code, address_text, management_agency_name,
              developer_name, designated_date, construction_start_date, completion_date,
              lot_sales_status, business_period_raw, business_period_start_month,
              business_period_end_month, development_progress_percent, designation_basis_law_raw,
              development_method_raw, development_purpose_raw, invited_industries_raw,
              created_at, updated_at, version, kind_raw, status_raw, lot_sales_status_raw,
              development_stage)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                     $17, $18, $19, $20::numeric, $21, $22, $23, $24, $25, $26, $27, $28, $29,
                     $30, $31)",
        )
        .bind(complex.id.as_uuid())
        .bind(&complex.official_complex_code)
        .bind(&complex.name)
        .bind(complex.kind.wire_name())
        .bind(complex.primary_bjdong_code.as_deref())
        .bind(area_i64)
        .bind(complex.status.map(IndustrialComplexStatus::wire_name))
        .bind(complex.sido_code.as_deref())
        .bind(complex.sigungu_code.as_deref())
        .bind(complex.address_text.as_deref())
        .bind(complex.management_agency_name.as_deref())
        .bind(complex.developer_name.as_deref())
        .bind(complex.designated_date)
        .bind(complex.construction_start_date)
        .bind(complex.completion_date)
        .bind(
            complex
                .lot_sales_status
                .map(IndustrialComplexLotSalesStatus::wire_name),
        )
        .bind(complex.business_period_raw.as_deref())
        .bind(complex.business_period_start_month.as_deref())
        .bind(complex.business_period_end_month.as_deref())
        .bind(complex.development_progress_percent.as_deref())
        .bind(complex.designation_basis_law_raw.as_deref())
        .bind(complex.development_method_raw.as_deref())
        .bind(complex.development_purpose_raw.as_deref())
        .bind(complex.invited_industries_raw.as_deref())
        .bind(complex.created_at)
        .bind(complex.updated_at)
        .bind(complex.version)
        .bind(complex.kind_raw.as_deref())
        .bind(complex.status_raw.as_deref())
        .bind(complex.lot_sales_status_raw.as_deref())
        .bind(
            complex
                .development_stage
                .map(IndustrialComplexDevelopmentStage::wire_name),
        )
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

        let event = CatalogEvent::IndustrialComplexCreatedV3(complex.created_event());
        insert_outbox_event(&mut tx, &event).await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }

    async fn upsert_complexes_by_official_code(
        &self,
        commands: &[UpsertIndustrialComplexCommand],
    ) -> Result<Vec<UpsertIndustrialComplexOutcome>, CatalogError> {
        upsert_industrial_complexes_by_official_code(&self.pool, commands).await
    }

    async fn update_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        mutate: ComplexMutation,
        applied_by: StaffId,
    ) -> Result<IndustrialComplex, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let before = lock_industrial_complex(&mut tx, id).await?;
        ensure_expected_version(&before, expected_version)?;
        let updated = update_industrial_complex(&mut tx, id, &mutate).await?;

        // The row keeps only the new values; the ledger keeps what they replaced (ADR-0023).
        sqlx::query(
            "INSERT INTO catalog.catalog_edit (
                 id, command_type, target_kind, target_id, expected_version,
                 before_snapshot, after_snapshot, applied_by_principal_id
             ) VALUES ($1, 'catalog.industrial_complex.update', 'industrial_complex',
                       $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::now_v7())
        .bind(id.as_uuid())
        .bind(expected_version)
        .bind(industrial_complex_snapshot(&before).as_json())
        .bind(industrial_complex_snapshot(&updated).as_json())
        .bind(applied_by.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

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

        let row = sqlx::query(&format!(
            "UPDATE catalog.industrial_complex
             SET archived_at = now(),
                 archived_by_staff_id = $3,
                 archive_reason = $4,
                 updated_at = now(),
                 version = version + 1
             WHERE id = $1 AND version = $2 AND archived_at IS NULL
             RETURNING {INDUSTRIAL_COMPLEX_COLUMNS}"
        ))
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
    ) -> Result<PublishedRuntimeManifest, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        set_lock_timeout_tx(&mut tx).await?;

        // The manifest identity is minted before anything is written, because the ledger claim below
        // records it and the claim has to come first. The deferred foreign key is what makes that
        // ordering legal — see 20260730000004.
        let manifest_id = Uuid::now_v7();

        // The fixed order is ledger -> pointer -> publication unit -> release, and it is fixed *here*
        // rather than left to whichever statement happens to touch a row first.
        //
        // The ledger leads for a reason that is not aesthetic: the pointer lock is table-level, so it
        // serializes activations of *every* unit. A replay that had to take it first would queue
        // behind an unrelated in-flight activation only to discover it has nothing to do, and every
        // pooled connection carries a 2500ms statement timeout. Claiming the key first means a replay
        // answers immediately. Deadlock is impossible in this order: a transaction holding the pointer
        // lock already owns its ledger row, so it can never be waited on for one.
        match claim_mutation_key_tx(&mut tx, &command, manifest_id).await? {
            MutationClaim::Replay(recorded_manifest_id) => {
                let manifest =
                    load_vector_tile_runtime_manifest_by_id(&mut tx, recorded_manifest_id)
                        .await?
                        .ok_or_else(|| {
                            CatalogError::Infrastructure(format!(
                        "ledger key {} records manifest {recorded_manifest_id}, which is absent",
                        command.idempotency_key
                    ))
                        })?;
                // Committing rather than rolling back: nothing was written, and the commit is what
                // releases the locks the claim took while it waited.
                tx.commit().await.map_err(map_sqlx)?;
                return Ok(PublishedRuntimeManifest::Replayed(manifest));
            }
            MutationClaim::Claimed => {}
        }

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

        // Read back by identity inside the transaction, so this reply and a later replay of it come
        // from one definition. Reading the pointer would work here and not there.
        let manifest = load_vector_tile_runtime_manifest_by_id(
            &mut tx,
            VectorTileRuntimeManifestId::new(manifest_id),
        )
        .await?
        .ok_or_else(|| {
            CatalogError::InvalidVectorTileRuntimeManifest(
                "the promotion gate left no runtime manifest under the promoted id".to_owned(),
            )
        })?;

        if self.runtime_manifest_publication.is_enabled() {
            let event_id =
                insert_outbox_event(&mut tx, &runtime_manifest_published_event(&manifest)).await?;
            record_mutation_outbox_event_tx(&mut tx, &command.idempotency_key, event_id).await?;
        }

        tx.commit().await.map_err(map_sqlx)?;
        Ok(PublishedRuntimeManifest::Published(manifest))
    }

    async fn start_vector_tile_build(
        &self,
        command: StartVectorTileBuildCommand,
    ) -> Result<VectorTileBuildJobId, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        set_lock_timeout_tx(&mut tx).await?;
        let build_job_id = VectorTileBuildJobId::new(Uuid::now_v7());

        if !claim_build_start_key_tx(&mut tx, &command).await? {
            let existing: Option<Uuid> = sqlx::query_scalar(
                "SELECT build.id
                 FROM catalog.vector_tile_build_job AS build
                 JOIN catalog.vector_tile_publication_unit AS unit
                   ON unit.id = build.publication_unit_id
                 WHERE unit.unit_key = $1 AND build.idempotency_key = $2",
            )
            .bind(&command.unit_key)
            .bind(&command.idempotency_key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx)?;
            let existing = existing.ok_or_else(|| {
                CatalogError::Infrastructure(format!(
                    "build-start ledger key {} has no vector_tile_build_job outcome",
                    command.idempotency_key
                ))
            })?;
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(VectorTileBuildJobId::new(existing));
        }

        let row = sqlx::query(
            "SELECT unit.id AS publication_unit_id, unit.active_release_id,
                    release.data_revision, release.canonical_iceberg_snapshot_id,
                    release.source_kind
             FROM catalog.vector_tile_publication_unit AS unit
             JOIN catalog.vector_tile_release AS release
               ON release.id = $2 AND release.publication_unit_id = unit.id
             WHERE unit.unit_key = $1
             FOR UPDATE OF unit FOR SHARE OF release",
        )
        .bind(&command.unit_key)
        .bind(command.input_release_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| {
            invalid_runtime(format!(
                "publication unit {} has no input release {}",
                command.unit_key, command.input_release_id
            ))
        })?;

        let publication_unit_id: Uuid = row.try_get("publication_unit_id").map_err(map_sqlx)?;
        let active_release_id: Option<Uuid> = row.try_get("active_release_id").map_err(map_sqlx)?;
        if active_release_id != Some(command.input_release_id.as_uuid()) {
            return Err(invalid_runtime(format!(
                "static build input release {} is not active for publication unit {}",
                command.input_release_id, command.unit_key
            )));
        }
        let source_kind: String = row.try_get("source_kind").map_err(map_sqlx)?;
        if source_kind != "dynamic_postgis" {
            return Err(invalid_runtime(format!(
                "static build input release must be dynamic_postgis, got {source_kind}"
            )));
        }
        let data_revision: Uuid = row.try_get("data_revision").map_err(map_sqlx)?;
        if data_revision != command.input_data_revision.as_uuid() {
            return Err(invalid_runtime(format!(
                "static build input data revision does not match release {}",
                command.input_release_id
            )));
        }
        let release_snapshot = CanonicalIcebergSnapshotId::new(
            row.try_get::<String, _>("canonical_iceberg_snapshot_id")
                .map_err(map_sqlx)?,
        )
        .map_err(invalid_runtime)?;
        validate_build_snapshot_binding(&command.frozen_source_snapshot_id, &release_snapshot)
            .map_err(invalid_runtime)?;

        sqlx::query(
            "INSERT INTO catalog.vector_tile_build_job
             (id, publication_unit_id, input_release_id, input_data_revision,
              frozen_source_snapshot_id, status, idempotency_key)
             VALUES ($1, $2, $3, $4, $5, 'running', $6)",
        )
        .bind(build_job_id.as_uuid())
        .bind(publication_unit_id)
        .bind(command.input_release_id.as_uuid())
        .bind(command.input_data_revision.as_uuid())
        .bind(command.frozen_source_snapshot_id.as_str())
        .bind(&command.idempotency_key)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(build_job_id)
    }

    async fn start_static_release_readdress(
        &self,
        command: StartStaticReleaseReaddressCommand,
    ) -> Result<VectorTileBuildJobId, CatalogError> {
        readdress::start(&self.pool, command).await
    }

    async fn start_lakehouse_bake(
        &self,
        command: StartLakehouseBakeCommand,
    ) -> Result<VectorTileBuildJobId, CatalogError> {
        lakehouse_bake::start(&self.pool, command).await
    }

    async fn record_vector_tile_build_result(
        &self,
        command: RecordVectorTileBuildResultCommand,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let row = sqlx::query(
            "SELECT build.status, build.frozen_source_snapshot_id, unit.unit_key,
                    build.result_release_id, build.result_pmtiles_file_asset_id,
                    build.result_pmtiles_object_key, build.result_tiles_url_template,
                    build.result_pmtiles_sha256, build.result_pmtiles_bytes,
                    build.result_evidence_sha256, build.result_recorded_by_staff_id,
                    build.failure_reason
             FROM catalog.vector_tile_build_job AS build
             JOIN catalog.vector_tile_publication_unit AS unit
               ON unit.id = build.publication_unit_id
             WHERE build.id = $1
             FOR UPDATE OF build",
        )
        .bind(command.build_job_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| {
            invalid_runtime(format!(
                "vector tile build job {} does not exist",
                command.build_job_id
            ))
        })?;

        let status = VectorTileBuildStatus::try_from(
            row.try_get::<String, _>("status")
                .map_err(map_sqlx)?
                .as_str(),
        )
        .map_err(invalid_runtime)?;
        validate_build_result_report(status, &command.outcome).map_err(invalid_runtime)?;

        match &command.outcome {
            VectorTileBuildOutcome::Validated { evidence, artifact } => {
                let unit_key: String = row.try_get("unit_key").map_err(map_sqlx)?;
                validate_build_artifact_identity(&unit_key, command.build_job_id, artifact)?;
                if status == VectorTileBuildStatus::Validated {
                    ensure_recorded_artifact_matches(
                        &row,
                        evidence,
                        artifact,
                        command.operator_staff_id,
                    )?;
                    tx.commit().await.map_err(map_sqlx)?;
                    return Ok(());
                }
                let size_bytes = u64_to_i64(artifact.size_bytes)?;
                sqlx::query(
                    "UPDATE catalog.vector_tile_build_job
                     SET status = 'validated',
                         result_snapshot_id = frozen_source_snapshot_id,
                         result_evidence_sha256 = $2,
                         result_release_id = $3,
                         result_pmtiles_file_asset_id = $4,
                         result_pmtiles_object_key = $5,
                         result_tiles_url_template = $6,
                         result_pmtiles_sha256 = $7,
                         result_pmtiles_bytes = $8,
                         result_recorded_by_staff_id = $9,
                         failure_reason = NULL,
                         updated_at = now()
                     WHERE id = $1",
                )
                .bind(command.build_job_id.as_uuid())
                .bind(evidence.as_str())
                .bind(artifact.release_id.as_uuid())
                .bind(artifact.file_asset_id.as_uuid())
                .bind(&artifact.object_key)
                .bind(artifact.tiles_url_template.as_str())
                .bind(artifact.checksum.as_str())
                .bind(size_bytes)
                .bind(command.operator_staff_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
            }
            VectorTileBuildOutcome::Failed(reason) => {
                sqlx::query(
                    "UPDATE catalog.vector_tile_build_job
                     SET status = 'failed', failure_reason = $2,
                         result_recorded_by_staff_id = $3, updated_at = now()
                     WHERE id = $1",
                )
                .bind(command.build_job_id.as_uuid())
                .bind(reason)
                .bind(command.operator_staff_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
            }
        }

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }

    async fn promote_tile_layer_static(
        &self,
        command: PromoteTileLayerStaticCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        set_lock_timeout_tx(&mut tx).await?;
        let manifest_id = Uuid::now_v7();
        match claim_manifest_mutation_key_tx(
            &mut tx,
            &command.idempotency_key,
            CatalogMutationKind::PromoteTileLayerStatic,
            &command.request_fingerprint(),
            manifest_id,
            command.operator_staff_id,
        )
        .await?
        {
            MutationClaim::Replay(recorded_manifest_id) => {
                let manifest =
                    load_vector_tile_runtime_manifest_by_id(&mut tx, recorded_manifest_id)
                        .await?
                        .ok_or_else(|| {
                            CatalogError::Infrastructure(format!(
                                "static promotion ledger key {} records absent manifest {}",
                                command.idempotency_key, recorded_manifest_id
                            ))
                        })?;
                tx.commit().await.map_err(map_sqlx)?;
                return Ok(manifest);
            }
            MutationClaim::Claimed => {}
        }

        let current_manifest_id = lock_runtime_manifest_pointer_tx(&mut tx).await?;
        let units = lock_publication_units_tx(&mut tx).await?;
        let carried = current_manifest_selections_tx(&mut tx, current_manifest_id).await?;
        let target = find_publication_unit(&units, &command.unit_key)?;
        let build = lock_validated_build_tx(&mut tx, command.build_job_id, target.id).await?;

        if matches!(
            build.kind,
            VectorTileBuildKind::Readdress | VectorTileBuildKind::LakehouseBake
        ) && build.input_serving_generation
            != Some(u64_to_i64(command.expected_serving_generation.value())?)
        {
            return Err(invalid_runtime(
                "readdress and lakehouse bake promotions must use the serving generation captured at start",
            ));
        }

        if mark_superseded_build_tx(&mut tx, target, &build, &command).await? {
            tx.commit().await.map_err(map_sqlx)?;
            return Err(invalid_runtime(format!(
                "build {} was superseded because its input release is no longer active",
                command.build_job_id
            )));
        }

        let manifest = publish_static_release_tx(
            &mut tx,
            StaticPromotionPlan {
                current_manifest_id,
                manifest_id,
                units: &units,
                carried: &carried,
                target,
                build: &build,
                command: &command,
            },
        )
        .await?;
        if self.runtime_manifest_publication.is_enabled() {
            let event_id =
                insert_outbox_event(&mut tx, &runtime_manifest_published_event(&manifest)).await?;
            record_mutation_outbox_event_tx(&mut tx, &command.idempotency_key, event_id).await?;
        }
        tx.commit().await.map_err(map_sqlx)?;
        Ok(manifest)
    }
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

    /// Re-registering an archived complex's code derives its id again and hits the key.
    #[test]
    fn primary_key_violation_maps_to_conflict() {
        let error =
            map_industrial_complex_unique_violation(Some("industrial_complex_pkey"), "IC-001");

        assert!(matches!(
            error,
            CatalogError::ComplexOfficialCodeConflict(code) if code == "IC-001"
        ));
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
