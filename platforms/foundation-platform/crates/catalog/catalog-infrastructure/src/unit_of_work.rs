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
    RuntimeManifestPublicationCapability, StartVectorTileBuildCommand,
    UpsertIndustrialComplexCommand, UpsertIndustrialComplexEffect, UpsertIndustrialComplexOutcome,
    VectorTileArtifactPromotionCommand, VectorTileFileAssetCommand,
    VectorTileManifestPromotionCommand, VectorTileManifestRollbackCommand,
    VectorTileSourceRecordCommand,
};
use catalog_domain::{
    static_file_asset_id_for_build, static_release_id_for_build, static_release_martin_source_id,
    static_release_pmtiles_object_key, validate_build_promotion, validate_build_result_report,
    validate_build_snapshot_binding, BuildEvidenceDigest, CanonicalIcebergSnapshotId, CatalogError,
    CatalogMutationKind, ComplexMutation, IndustrialComplex, IndustrialComplexLotSalesStatus,
    IndustrialComplexStatus, Parcel, ParcelKind, ParcelKindEdit, RequestFingerprint,
    RuntimeTileLayer, ServingGeneration, VectorTileArtifact, VectorTileBuildOutcome,
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
    ComplexId, FileAssetId, ParcelId, StaffId, VectorTileBuildJobId, VectorTileManifestId,
    VectorTileReleaseId, VectorTileRuntimeManifestId,
};
use serde_json::Value as JsonValue;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::row_map::{
    is_unique_violation_code, map_sqlx, row_to_complex, row_to_parcel, row_to_vector_tile_artifact,
    row_to_vector_tile_manifest, u64_to_i64, INDUSTRIAL_COMPLEX_COLUMNS,
};
use crate::sqlx_repository::load_vector_tile_runtime_manifest_by_id;

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
            // `$21::numeric` because the progress percentage travels as exact decimal text: the
            // column is `numeric(5,2)` and binding an `f64` would store a value the source never
            // stated. The cast is what lets Postgres parse the text it will store.
            "INSERT INTO catalog.industrial_complex
             (id, lakehouse_complex_id, official_complex_code, name, kind, primary_bjdong_code,
              area_m2, status, sido_code, sigungu_code, address_text, management_agency_name,
              developer_name, designated_date, construction_start_date, completion_date,
              lot_sales_status, business_period_raw, business_period_start_month,
              business_period_end_month, development_progress_percent, designation_basis_law_raw,
              development_method_raw, development_purpose_raw, invited_industries_raw,
              created_at, updated_at, version)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                     $18, $19, $20, $21::numeric, $22, $23, $24, $25, $26, $27, $28)",
        )
        .bind(complex.id.as_uuid())
        .bind(complex.lakehouse_complex_id.map(|id| id.as_uuid()))
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
    ) -> Result<IndustrialComplex, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        let area_i64 = mutate.area_m2.map(u64_to_i64).transpose()?;

        let row = sqlx::query(&format!(
            "UPDATE catalog.industrial_complex
             SET name      = COALESCE($3, name),
                 area_m2   = COALESCE($4, area_m2),
                 updated_at = now(),
                 version   = version + 1
             WHERE id = $1 AND version = $2 AND archived_at IS NULL
             RETURNING {INDUSTRIAL_COMPLEX_COLUMNS}"
        ))
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

    async fn update_parcel_kind(
        &self,
        id: ParcelId,
        expected_version: i64,
        new_kind: ParcelKind,
        applied_by: StaffId,
    ) -> Result<Parcel, CatalogError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;

        // 변경 전 row 를 `FOR UPDATE` 로 잠가 event payload 의 previous_kind 가 race-free.
        let before_row = sqlx::query(
            "SELECT id, pnu, kind, area_m2, created_at, updated_at, version
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
             RETURNING id, pnu, kind, area_m2, created_at, updated_at, version",
        )
        .bind(id.as_uuid())
        .bind(expected_version)
        .bind(new_kind.wire_name())
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let updated = row_to_parcel(&updated_row)?;

        // The edit ledger row, in the same transaction as the row it describes. ADR-0006 rebuilds
        // the serving projection from a snapshot plus an audited edit ledger, so an edit recorded
        // only on the parcel is one a rebuild silently drops (ADR-0023).
        //
        // `catalog.catalog_edit` and not the Normalization context's own ledger: that one belongs
        // to a package `package_boundary.rs` keeps out of Catalog, and Catalog does not depend on
        // it. ADR-0023 §Decision 2 names it; this comment does not, because that guard reads source
        // text and a comment spelling the table would be a crossing in its eyes.
        //
        // The snapshots carry the field that changed and the identity of what it changed on — not
        // the whole row, because the ledger records the edit and the Iceberg snapshot records the
        // rest.
        sqlx::query(
            "INSERT INTO catalog.catalog_edit
             (id, command_type, target_kind, target_id, expected_version,
              before_snapshot, after_snapshot, applied_by_principal_id)
             VALUES ($1, 'parcel.kind.update.v1', 'parcel', $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::new_v4())
        .bind(id.as_uuid())
        .bind(expected_version)
        .bind(serde_json::json!({
            "parcel_id": id.as_uuid(),
            // `null` when nobody had decided one yet. The ledger records what was there, and
            // what was there is nothing (root ADR-0070).
            "kind": before.kind.map(ParcelKind::wire_name),
            "version": before.version,
        }))
        .bind(serde_json::json!({
            "parcel_id": id.as_uuid(),
            "kind": updated.kind.map(ParcelKind::wire_name),
            "version": updated.version,
        }))
        .bind(applied_by.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        // A parcel that had no kind is being given one, not having one replaced. Reporting
        // the first assignment as a change would need a `previous_kind` that never existed
        // (root ADR-0070).
        let event = match before.kind_edit_event(new_kind) {
            ParcelKindEdit::Assigned(assigned) => CatalogEvent::ParcelKindAssigned(assigned),
            ParcelKindEdit::Changed(changed) => CatalogEvent::ParcelKindChanged(changed),
        };
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

/// Whether this transaction owns the idempotency key or is answering a prior use of it.
enum MutationClaim {
    /// The key is this transaction's; the mutation proceeds.
    Claimed,
    /// A prior transaction recorded the same request; answer with the manifest it published.
    Replay(VectorTileRuntimeManifestId),
}

struct LockedValidatedBuild {
    unit_key: String,
    status: VectorTileBuildStatus,
    input_release_id: Uuid,
    input_data_revision: Uuid,
    frozen_source_snapshot_id: String,
    input_source_record_id: Uuid,
    input_source_file_asset_ids: Vec<Uuid>,
    release_id: Uuid,
    file_asset_id: Uuid,
    object_key: String,
    tiles_url_template: String,
    pmtiles_sha256: String,
    pmtiles_bytes: i64,
    validation_evidence_sha256: String,
}

struct StaticPromotionPlan<'a> {
    current_manifest_id: Option<Uuid>,
    manifest_id: Uuid,
    units: &'a [LockedPublicationUnit],
    carried: &'a BTreeMap<Uuid, CarriedSelection>,
    target: &'a LockedPublicationUnit,
    build: &'a LockedValidatedBuild,
    command: &'a PromoteTileLayerStaticCommand,
}

async fn mark_superseded_build_tx(
    tx: &mut Transaction<'_, Postgres>,
    target: &LockedPublicationUnit,
    build: &LockedValidatedBuild,
    command: &PromoteTileLayerStaticCommand,
) -> Result<bool, CatalogError> {
    let verdict = validate_build_promotion(VectorTileBuildPromotionInput {
        status: build.status,
        input_release_id: VectorTileReleaseId::new(build.input_release_id),
        active_release_id: VectorTileReleaseId::new(target.active_release_id.ok_or_else(|| {
            invalid_runtime(format!(
                "publication unit {} has no active release",
                command.unit_key
            ))
        })?),
    })
    .map_err(invalid_runtime)?;
    if verdict == VectorTileBuildPromotionVerdict::Promotable {
        return Ok(false);
    }

    sqlx::query(
        "UPDATE catalog.vector_tile_build_job
         SET status = 'superseded', updated_at = now() WHERE id = $1",
    )
    .bind(command.build_job_id.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    // A superseded attempt has no manifest answer. Failed mutations do not retain an idempotency
    // row; the terminal build status is the durable outcome instead.
    sqlx::query("DELETE FROM catalog.catalog_mutation_idempotency WHERE idempotency_key = $1")
        .bind(&command.idempotency_key)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    Ok(true)
}

async fn publish_static_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    plan: StaticPromotionPlan<'_>,
) -> Result<VectorTileRuntimeManifest, CatalogError> {
    validate_static_serving_observation(plan.target, plan.command)?;
    let selections = plan_static_activation(plan.units, plan.target, plan.carried, plan.build)?;
    let mut releases_to_lock = carried_release_ids(&selections, plan.build.release_id);
    releases_to_lock.push(plan.build.input_release_id);
    releases_to_lock.sort_unstable();
    releases_to_lock.dedup();
    lock_release_rows_tx(tx, &releases_to_lock).await?;

    insert_static_release_tx(tx, plan.target.id, plan.build).await?;
    copy_release_layers_tx(tx, plan.build.input_release_id, plan.build.release_id).await?;
    insert_static_runtime_manifest_tx(tx, plan.manifest_id, plan.build.input_source_record_id)
        .await?;
    insert_manifest_units_tx(tx, plan.manifest_id, &selections).await?;

    sqlx::query_scalar::<_, i64>("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
        .bind(plan.current_manifest_id)
        .bind(plan.manifest_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(map_runtime_manifest_gate_error)?;

    // The gate has already changed active_release_id, so the old dynamic release can now be stored
    // as a distinct same-revision fallback without violating the column CHECK.
    sqlx::query(
        "UPDATE catalog.vector_tile_publication_unit
         SET fallback_release_id = $2, fallback_data_revision = $3, updated_at = now()
         WHERE id = $1",
    )
    .bind(plan.target.id)
    .bind(plan.build.input_release_id)
    .bind(plan.build.input_data_revision)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    sqlx::query(
        "UPDATE catalog.vector_tile_build_job
         SET status = 'promoted', updated_at = now() WHERE id = $1",
    )
    .bind(plan.command.build_job_id.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    load_vector_tile_runtime_manifest_by_id(tx, VectorTileRuntimeManifestId::new(plan.manifest_id))
        .await?
        .ok_or_else(|| {
            invalid_runtime("the static promotion gate left no runtime manifest under its id")
        })
}

async fn lock_validated_build_tx(
    tx: &mut Transaction<'_, Postgres>,
    build_job_id: VectorTileBuildJobId,
    publication_unit_id: Uuid,
) -> Result<LockedValidatedBuild, CatalogError> {
    let row = sqlx::query(
        "SELECT unit.unit_key, build.status, build.input_release_id, build.input_data_revision,
                build.frozen_source_snapshot_id, build.result_release_id,
                build.result_pmtiles_file_asset_id, build.result_pmtiles_object_key,
                build.result_tiles_url_template, build.result_pmtiles_sha256,
                build.result_pmtiles_bytes, build.result_evidence_sha256,
                input.source_record_id, input.source_file_asset_ids, input.source_kind
         FROM catalog.vector_tile_build_job AS build
         JOIN catalog.vector_tile_publication_unit AS unit
           ON unit.id = build.publication_unit_id
         JOIN catalog.vector_tile_release AS input ON input.id = build.input_release_id
         WHERE build.id = $1 AND build.publication_unit_id = $2
         FOR UPDATE OF build FOR SHARE OF input",
    )
    .bind(build_job_id.as_uuid())
    .bind(publication_unit_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| {
        invalid_runtime(format!(
            "validated build {build_job_id} does not belong to the requested publication unit"
        ))
    })?;
    let status_raw: String = row.try_get("status").map_err(map_sqlx)?;
    let status = VectorTileBuildStatus::try_from(status_raw.as_str()).map_err(invalid_runtime)?;
    let input_source_kind: String = row.try_get("source_kind").map_err(map_sqlx)?;
    if input_source_kind != "dynamic_postgis" {
        return Err(invalid_runtime(format!(
            "static promotion build input must be dynamic_postgis, got {input_source_kind}"
        )));
    }
    let required_uuid = |column: &str| -> Result<Uuid, CatalogError> {
        row.try_get::<Option<Uuid>, _>(column)
            .map_err(map_sqlx)?
            .ok_or_else(|| invalid_runtime(format!("validated build is missing {column}")))
    };
    let required_text = |column: &str| -> Result<String, CatalogError> {
        row.try_get::<Option<String>, _>(column)
            .map_err(map_sqlx)?
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_runtime(format!("validated build is missing {column}")))
    };
    Ok(LockedValidatedBuild {
        unit_key: row.try_get("unit_key").map_err(map_sqlx)?,
        status,
        input_release_id: row.try_get("input_release_id").map_err(map_sqlx)?,
        input_data_revision: row.try_get("input_data_revision").map_err(map_sqlx)?,
        frozen_source_snapshot_id: row.try_get("frozen_source_snapshot_id").map_err(map_sqlx)?,
        input_source_record_id: row.try_get("source_record_id").map_err(map_sqlx)?,
        input_source_file_asset_ids: row.try_get("source_file_asset_ids").map_err(map_sqlx)?,
        release_id: required_uuid("result_release_id")?,
        file_asset_id: required_uuid("result_pmtiles_file_asset_id")?,
        object_key: required_text("result_pmtiles_object_key")?,
        tiles_url_template: required_text("result_tiles_url_template")?,
        pmtiles_sha256: required_text("result_pmtiles_sha256")?,
        pmtiles_bytes: row
            .try_get::<Option<i64>, _>("result_pmtiles_bytes")
            .map_err(map_sqlx)?
            .ok_or_else(|| invalid_runtime("validated build is missing result_pmtiles_bytes"))?,
        validation_evidence_sha256: required_text("result_evidence_sha256")?,
    })
}

fn validate_static_serving_observation(
    unit: &LockedPublicationUnit,
    command: &PromoteTileLayerStaticCommand,
) -> Result<(), CatalogError> {
    let current_release = unit.active_release_id;
    let current_generation = observed_serving_generation(unit)?;
    if current_release != Some(command.expected_active_release_id.as_uuid())
        || current_generation != command.expected_serving_generation.value()
    {
        return Err(serving_state_conflict(
            unit,
            Some(command.expected_active_release_id.as_uuid()),
            Some(command.expected_serving_generation.value()),
        ));
    }
    Ok(())
}

fn plan_static_activation(
    units: &[LockedPublicationUnit],
    target: &LockedPublicationUnit,
    carried: &BTreeMap<Uuid, CarriedSelection>,
    build: &LockedValidatedBuild,
) -> Result<Vec<ManifestUnitSelection>, CatalogError> {
    units
        .iter()
        .map(|unit| {
            if unit.id == target.id {
                return Ok(ManifestUnitSelection {
                    publication_unit_id: unit.id,
                    release_id: build.release_id,
                    serving_generation: advance_serving_generation(unit)?,
                    data_revision: build.input_data_revision,
                    canonical_iceberg_snapshot_id: build.frozen_source_snapshot_id.clone(),
                });
            }
            let selection = carried.get(&unit.id).ok_or_else(|| {
                invalid_runtime(format!(
                    "publication unit {} has no carried selection for static promotion",
                    unit.unit_key
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

async fn insert_static_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    publication_unit_id: Uuid,
    build: &LockedValidatedBuild,
) -> Result<(), CatalogError> {
    let source_id = static_release_martin_source_id(
        &build.unit_key,
        VectorTileReleaseId::new(build.release_id),
    );
    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, checksum_sha256,
          source_record_id, visibility, version)
         VALUES ($1, $2, 'application/vnd.pmtiles', $3, $4, $5, 'private', 1)",
    )
    .bind(build.file_asset_id)
    .bind(&build.object_key)
    .bind(build.pmtiles_bytes)
    .bind(&build.pmtiles_sha256)
    .bind(build.input_source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_record_id, source_file_asset_ids, source_kind, martin_source_id,
          tiles_url_template, pmtiles_object_key, pmtiles_file_asset_id,
          pmtiles_sha256, pmtiles_bytes, validated_at, validation_evidence_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, 'static_pmtiles', $7, $8, $9, $10, $11, $12,
                 now(), $13)",
    )
    .bind(build.release_id)
    .bind(publication_unit_id)
    .bind(build.input_data_revision)
    .bind(&build.frozen_source_snapshot_id)
    .bind(build.input_source_record_id)
    .bind(&build.input_source_file_asset_ids)
    .bind(source_id)
    .bind(&build.tiles_url_template)
    .bind(&build.object_key)
    .bind(build.file_asset_id)
    .bind(&build.pmtiles_sha256)
    .bind(build.pmtiles_bytes)
    .bind(&build.validation_evidence_sha256)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn copy_release_layers_tx(
    tx: &mut Transaction<'_, Postgres>,
    input_release_id: Uuid,
    release_id: Uuid,
) -> Result<(), CatalogError> {
    let inserted = sqlx::query(
        "INSERT INTO catalog.vector_tile_release_layer
         (release_id, layer_id, source_layer, feature_id_property,
          tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
          feature_filter_properties)
         SELECT $2, layer_id, source_layer, feature_id_property,
                tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
                feature_filter_properties
         FROM catalog.vector_tile_release_layer
         WHERE release_id = $1",
    )
    .bind(input_release_id)
    .bind(release_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .rows_affected();
    if inserted == 0 {
        return Err(invalid_runtime(
            "static promotion input release has no layers to copy",
        ));
    }
    Ok(())
}

async fn insert_static_runtime_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    source_record_id: Uuid,
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
    .bind(source_record_id)
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

/// Bounds every lock wait in this transaction so contention is answerable instead of opaque.
///
/// Without it a duplicate blocked on the ledger key or the pointer lock runs into the pool's
/// `statement_timeout` (2500ms, set per connection by the API), surfaces as `57014`, and is served as
/// a redacted 500 — the one case where the client's correct action, retrying the same key, is exactly
/// what it cannot infer. `SET LOCAL` so the setting dies with the transaction and cannot leak back
/// into the pooled connection.
async fn set_lock_timeout_tx(tx: &mut Transaction<'_, Postgres>) -> Result<(), CatalogError> {
    sqlx::query("SET LOCAL lock_timeout = '1200ms'")
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    Ok(())
}

fn invalid_runtime(message: impl Into<String>) -> CatalogError {
    CatalogError::InvalidVectorTileRuntimeManifest(message.into())
}

/// Claims a build-start idempotency key; `false` means the identical committed start is replayed.
async fn claim_build_start_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &StartVectorTileBuildCommand,
) -> Result<bool, CatalogError> {
    let fingerprint = command.request_fingerprint();
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO catalog.catalog_mutation_idempotency
         (idempotency_key, command_kind, request_fingerprint_sha256,
          request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
         VALUES ($1, $2, $3, $4, NULL, $5)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING idempotency_key",
    )
    .bind(&command.idempotency_key)
    .bind(CatalogMutationKind::StartVectorTileBuild.as_str())
    .bind(fingerprint.as_str())
    .bind(CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION)
    .bind(command.operator_staff_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| map_contention_error(&command.idempotency_key, error))?;
    if claimed.is_some() {
        return Ok(true);
    }

    let row = read_mutation_claim_tx(tx, &command.idempotency_key).await?;
    verify_mutation_claim(
        &row,
        &command.idempotency_key,
        CatalogMutationKind::StartVectorTileBuild,
        &fingerprint,
    )?;
    Ok(false)
}

async fn claim_manifest_mutation_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    idempotency_key: &str,
    kind: CatalogMutationKind,
    fingerprint: &RequestFingerprint,
    manifest_id: Uuid,
    operator_staff_id: StaffId,
) -> Result<MutationClaim, CatalogError> {
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO catalog.catalog_mutation_idempotency
         (idempotency_key, command_kind, request_fingerprint_sha256,
          request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING idempotency_key",
    )
    .bind(idempotency_key)
    .bind(kind.as_str())
    .bind(fingerprint.as_str())
    .bind(CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION)
    .bind(manifest_id)
    .bind(operator_staff_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| map_contention_error(idempotency_key, error))?;
    if claimed.is_some() {
        return Ok(MutationClaim::Claimed);
    }
    let row = read_mutation_claim_tx(tx, idempotency_key).await?;
    verify_mutation_claim(&row, idempotency_key, kind, fingerprint)?;
    row.try_get::<Option<Uuid>, _>("outcome_manifest_id")
        .map_err(map_sqlx)?
        .map(|id| MutationClaim::Replay(VectorTileRuntimeManifestId::new(id)))
        .ok_or_else(|| {
            CatalogError::Infrastructure(format!(
                "manifest-answering ledger key {idempotency_key} has no outcome manifest"
            ))
        })
}

async fn read_mutation_claim_tx(
    tx: &mut Transaction<'_, Postgres>,
    idempotency_key: &str,
) -> Result<sqlx::postgres::PgRow, CatalogError> {
    sqlx::query(
        "SELECT command_kind, request_fingerprint_sha256,
                request_fingerprint_schema_version, outcome_manifest_id
         FROM catalog.catalog_mutation_idempotency
         WHERE idempotency_key = $1",
    )
    .bind(idempotency_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| map_contention_error(idempotency_key, error))?
    .ok_or_else(|| CatalogError::MutationContended {
        idempotency_key: idempotency_key.to_owned(),
    })
}

fn verify_mutation_claim(
    row: &sqlx::postgres::PgRow,
    idempotency_key: &str,
    expected_kind: CatalogMutationKind,
    expected_fingerprint: &RequestFingerprint,
) -> Result<(), CatalogError> {
    let version: String = row
        .try_get("request_fingerprint_schema_version")
        .map_err(map_sqlx)?;
    if version.trim() != CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION {
        return Err(CatalogError::MutationFingerprintVersionChanged {
            idempotency_key: idempotency_key.to_owned(),
            recorded: version,
            current: CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION.to_owned(),
        });
    }
    let kind: String = row.try_get("command_kind").map_err(map_sqlx)?;
    let fingerprint: String = row
        .try_get("request_fingerprint_sha256")
        .map_err(map_sqlx)?;
    if kind != expected_kind.as_str() || fingerprint.trim() != expected_fingerprint.as_str() {
        return Err(CatalogError::MutationIdempotencyKeyReused {
            idempotency_key: idempotency_key.to_owned(),
            command_kind: kind,
        });
    }
    Ok(())
}

fn validate_build_artifact_identity(
    unit_key: &str,
    build_job_id: VectorTileBuildJobId,
    artifact: &catalog_domain::ValidatedPmtilesArtifact,
) -> Result<(), CatalogError> {
    let expected_release_id = static_release_id_for_build(build_job_id);
    if artifact.release_id != expected_release_id {
        return Err(invalid_runtime(format!(
            "validated PMTiles release id must equal build-derived {expected_release_id}"
        )));
    }
    let expected_file_asset_id = static_file_asset_id_for_build(build_job_id);
    if artifact.file_asset_id != expected_file_asset_id {
        return Err(invalid_runtime(format!(
            "validated PMTiles file asset id must equal build-derived {expected_file_asset_id}"
        )));
    }
    let expected_key = static_release_pmtiles_object_key(unit_key, artifact.release_id);
    if artifact.object_key != expected_key {
        return Err(invalid_runtime(format!(
            "validated PMTiles object key must equal {expected_key}"
        )));
    }
    let source_id = static_release_martin_source_id(unit_key, artifact.release_id);
    let expected_route = format!("/{source_id}/{{z}}/{{x}}/{{y}}");
    if !artifact
        .tiles_url_template
        .as_str()
        .ends_with(&expected_route)
    {
        return Err(invalid_runtime(format!(
            "validated PMTiles URL must address the release-derived Martin source {source_id}"
        )));
    }
    if artifact.size_bytes == 0 {
        return Err(invalid_runtime(
            "validated PMTiles size_bytes must be greater than zero",
        ));
    }
    Ok(())
}

fn ensure_recorded_artifact_matches(
    row: &sqlx::postgres::PgRow,
    evidence: &BuildEvidenceDigest,
    artifact: &catalog_domain::ValidatedPmtilesArtifact,
    operator: StaffId,
) -> Result<(), CatalogError> {
    let same = row
        .try_get::<Option<Uuid>, _>("result_release_id")
        .map_err(map_sqlx)?
        == Some(artifact.release_id.as_uuid())
        && row
            .try_get::<Option<Uuid>, _>("result_pmtiles_file_asset_id")
            .map_err(map_sqlx)?
            == Some(artifact.file_asset_id.as_uuid())
        && row
            .try_get::<Option<String>, _>("result_pmtiles_object_key")
            .map_err(map_sqlx)?
            .as_deref()
            == Some(artifact.object_key.as_str())
        && row
            .try_get::<Option<String>, _>("result_tiles_url_template")
            .map_err(map_sqlx)?
            .as_deref()
            == Some(artifact.tiles_url_template.as_str())
        && row
            .try_get::<Option<String>, _>("result_pmtiles_sha256")
            .map_err(map_sqlx)?
            .as_deref()
            .map(str::trim)
            == Some(artifact.checksum.as_str())
        && row
            .try_get::<Option<i64>, _>("result_pmtiles_bytes")
            .map_err(map_sqlx)?
            == i64::try_from(artifact.size_bytes).ok()
        && row
            .try_get::<Option<String>, _>("result_evidence_sha256")
            .map_err(map_sqlx)?
            .as_deref()
            .map(str::trim)
            == Some(evidence.as_str())
        && row
            .try_get::<Option<Uuid>, _>("result_recorded_by_staff_id")
            .map_err(map_sqlx)?
            == Some(operator.as_uuid());
    if !same {
        return Err(invalid_runtime(
            "validated build result is immutable and the repeated report differs",
        ));
    }
    Ok(())
}

/// Claims the idempotency key, or resolves what a prior use of it recorded.
///
/// `ON CONFLICT DO NOTHING RETURNING` rather than a plain insert: a bare unique violation raises
/// `23505`, and Postgres has no continuable error — the transaction would be aborted and the replay
/// would need a second one, losing the atomicity the ledger exists for. `DO NOTHING` waits for the
/// conflicting transaction, then returns zero rows and leaves this transaction usable. There is
/// deliberately no `SELECT` before `begin()`: two callers would both find nothing and race.
async fn claim_mutation_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &MarkTileLayerDynamicCommand,
    manifest_id: Uuid,
) -> Result<MutationClaim, CatalogError> {
    let fingerprint = command.request_fingerprint();
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO catalog.catalog_mutation_idempotency
         (idempotency_key, command_kind, request_fingerprint_sha256,
          request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING idempotency_key",
    )
    .bind(&command.idempotency_key)
    .bind(CatalogMutationKind::MarkTileLayerDynamic.as_str())
    .bind(fingerprint.as_str())
    .bind(CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION)
    .bind(manifest_id)
    .bind(command.operator_staff_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| map_contention_error(&command.idempotency_key, error))?;
    if claimed.is_some() {
        return Ok(MutationClaim::Claimed);
    }

    let recorded = sqlx::query(
        "SELECT command_kind, request_fingerprint_sha256, request_fingerprint_schema_version,
                outcome_manifest_id
         FROM catalog.catalog_mutation_idempotency
         WHERE idempotency_key = $1",
    )
    .bind(&command.idempotency_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| map_contention_error(&command.idempotency_key, error))?;
    // The conflicting transaction rolled back after this one waited on it, so the key is free again
    // but this statement already lost its chance to take it. Retryable, and named as such.
    let Some(recorded) = recorded else {
        return Err(CatalogError::MutationContended {
            idempotency_key: command.idempotency_key.clone(),
        });
    };

    // Read before the digest is compared, because it decides whether comparing is meaningful at all.
    // Two digests from different encodings say nothing about each other, and reporting that as a key
    // reuse would blame the caller for a deployment change.
    let recorded_version: String = recorded
        .try_get("request_fingerprint_schema_version")
        .map_err(map_sqlx)?;
    if recorded_version.trim() != CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION {
        return Err(CatalogError::MutationFingerprintVersionChanged {
            idempotency_key: command.idempotency_key.clone(),
            recorded: recorded_version,
            current: CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION.to_owned(),
        });
    }

    let recorded_fingerprint: String = recorded
        .try_get("request_fingerprint_sha256")
        .map_err(map_sqlx)?;
    let recorded_kind: String = recorded.try_get("command_kind").map_err(map_sqlx)?;
    if recorded_fingerprint.trim() != fingerprint.as_str() {
        return Err(CatalogError::MutationIdempotencyKeyReused {
            idempotency_key: command.idempotency_key.clone(),
            command_kind: recorded_kind,
        });
    }
    // The command kind is inside the fingerprint, so a matching digest already proves it. Reading it
    // back keeps the check honest if the encoding is ever versioned apart from the kind.
    if recorded_kind != CatalogMutationKind::MarkTileLayerDynamic.as_str() {
        return Err(CatalogError::MutationIdempotencyKeyReused {
            idempotency_key: command.idempotency_key.clone(),
            command_kind: recorded_kind,
        });
    }

    let recorded_manifest_id: Option<Uuid> =
        recorded.try_get("outcome_manifest_id").map_err(map_sqlx)?;
    recorded_manifest_id
        .map(|id| MutationClaim::Replay(VectorTileRuntimeManifestId::new(id)))
        .ok_or_else(|| {
            CatalogError::Infrastructure(format!(
                "ledger key {} recorded no manifest for a manifest-answering command",
                command.idempotency_key
            ))
        })
}

/// Ties the ledger row to the event this transaction emitted.
///
/// Makes "the mutation, its outbox event and its ledger row are one transaction" assertable rather
/// than merely intended, and records the consequence of the publication capability being off: the
/// column stays null, and a later replay does not retro-announce.
async fn record_mutation_outbox_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    idempotency_key: &str,
    event_id: Uuid,
) -> Result<(), CatalogError> {
    sqlx::query(
        "UPDATE catalog.catalog_mutation_idempotency
         SET outbox_event_id = $2
         WHERE idempotency_key = $1",
    )
    .bind(idempotency_key)
    .bind(event_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Maps a bounded lock wait to a retryable conflict instead of an opaque infrastructure failure.
///
/// `55P03` is `lock_timeout`; `57014` is the pool's `statement_timeout` winning the race to fire
/// first. Only statements that wait on the idempotency key use this mapping, so a genuinely slow
/// unrelated query is still reported as what it is.
fn map_contention_error(idempotency_key: &str, error: sqlx::Error) -> CatalogError {
    match error {
        sqlx::Error::Database(database)
            if matches!(database.code().as_deref(), Some("55P03" | "57014")) =>
        {
            CatalogError::MutationContended {
                idempotency_key: idempotency_key.to_owned(),
            }
        }
        other => map_sqlx(other),
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
) -> Result<Vec<UpsertIndustrialComplexOutcome>, CatalogError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }

    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let mut outcomes = Vec::with_capacity(commands.len());

    for command in commands {
        let existing_row = sqlx::query(&format!(
            "SELECT {INDUSTRIAL_COMPLEX_COLUMNS}
             FROM catalog.industrial_complex
             WHERE official_complex_code = $1
               AND archived_at IS NULL
             FOR UPDATE"
        ))
        .bind(&command.official_complex_code)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let outcome = if let Some(row) = existing_row {
            upsert_existing_industrial_complex(&mut tx, command, &row).await?
        } else {
            UpsertIndustrialComplexOutcome {
                complex: insert_industrial_complex_from_upsert(&mut tx, command).await?,
                effect: UpsertIndustrialComplexEffect::Inserted,
            }
        };
        outcomes.push(outcome);
    }

    tx.commit().await.map_err(map_sqlx)?;
    Ok(outcomes)
}

async fn upsert_existing_industrial_complex(
    tx: &mut Transaction<'_, Postgres>,
    command: &UpsertIndustrialComplexCommand,
    row: &sqlx::postgres::PgRow,
) -> Result<UpsertIndustrialComplexOutcome, CatalogError> {
    let existing = row_to_complex(row)?;
    let changed_fields = changed_industrial_complex_fields(&existing, command);
    if changed_fields.is_empty() {
        return Ok(UpsertIndustrialComplexOutcome {
            complex: existing,
            effect: UpsertIndustrialComplexEffect::Unchanged,
        });
    }

    let updated = update_industrial_complex_from_upsert(tx, &existing, command).await?;
    let event =
        CatalogEvent::IndustrialComplexUpdated(updated.updated_fields_event(changed_fields));
    insert_outbox_event(tx, &event).await?;
    Ok(UpsertIndustrialComplexOutcome {
        complex: updated,
        effect: UpsertIndustrialComplexEffect::Updated,
    })
}

async fn update_industrial_complex_from_upsert(
    tx: &mut Transaction<'_, Postgres>,
    existing: &IndustrialComplex,
    command: &UpsertIndustrialComplexCommand,
) -> Result<IndustrialComplex, CatalogError> {
    let area_i64 = u64_to_i64(command.area_m2)?;
    let updated_row = sqlx::query(&format!(
        "UPDATE catalog.industrial_complex
         SET name = $2,
             kind = $3,
             primary_bjdong_code = $4,
             area_m2 = $5,
             status = $6,
             sido_code = $7,
             sigungu_code = $8,
             address_text = $9,
             management_agency_name = $10,
             developer_name = $11,
             designated_date = $12,
             completion_date = $13,
             lakehouse_complex_id = $14,
             construction_start_date = $15,
             development_progress_percent = $16::numeric,
             lot_sales_status = $17,
             business_period_raw = $18,
             business_period_start_month = $19,
             business_period_end_month = $20,
             designation_basis_law_raw = $21,
             development_method_raw = $22,
             development_purpose_raw = $23,
             invited_industries_raw = $24,
             updated_at = now(),
             version = version + 1
         WHERE id = $1
         RETURNING {INDUSTRIAL_COMPLEX_COLUMNS}"
    ))
    .bind(existing.id.as_uuid())
    .bind(&command.name)
    .bind(command.kind.wire_name())
    .bind(command.primary_bjdong_code.as_deref())
    .bind(area_i64)
    .bind(command.status.map(IndustrialComplexStatus::wire_name))
    .bind(command.sido_code.as_deref())
    .bind(command.sigungu_code.as_deref())
    .bind(command.address_text.as_deref())
    .bind(command.management_agency_name.as_deref())
    .bind(command.developer_name.as_deref())
    .bind(command.designated_date)
    .bind(command.completion_date)
    .bind(command.lakehouse_complex_id.map(|id| id.as_uuid()))
    .bind(command.construction_start_date)
    .bind(command.development_progress_percent.as_deref())
    .bind(
        command
            .lot_sales_status
            .map(IndustrialComplexLotSalesStatus::wire_name),
    )
    .bind(command.business_period_raw.as_deref())
    .bind(command.business_period_start_month.as_deref())
    .bind(command.business_period_end_month.as_deref())
    .bind(command.designation_basis_law_raw.as_deref())
    .bind(command.development_method_raw.as_deref())
    .bind(command.development_purpose_raw.as_deref())
    .bind(command.invited_industries_raw.as_deref())
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
    let inserted_row = sqlx::query(&format!(
        "INSERT INTO catalog.industrial_complex
         (id, lakehouse_complex_id, official_complex_code, name, kind, primary_bjdong_code,
          area_m2, status, sido_code, sigungu_code, address_text, management_agency_name,
          developer_name, designated_date, construction_start_date, completion_date,
          lot_sales_status, business_period_raw, business_period_start_month,
          business_period_end_month, development_progress_percent, designation_basis_law_raw,
          development_method_raw, development_purpose_raw, invited_industries_raw,
          created_at, updated_at, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                 $19, $20, $21::numeric, $22, $23, $24, $25, $26, $27, 1)
         RETURNING {INDUSTRIAL_COMPLEX_COLUMNS}"
    ))
    .bind(Uuid::now_v7())
    .bind(command.lakehouse_complex_id.map(|id| id.as_uuid()))
    .bind(&command.official_complex_code)
    .bind(&command.name)
    .bind(command.kind.wire_name())
    .bind(command.primary_bjdong_code.as_deref())
    .bind(area_i64)
    .bind(command.status.map(IndustrialComplexStatus::wire_name))
    .bind(command.sido_code.as_deref())
    .bind(command.sigungu_code.as_deref())
    .bind(command.address_text.as_deref())
    .bind(command.management_agency_name.as_deref())
    .bind(command.developer_name.as_deref())
    .bind(command.designated_date)
    .bind(command.construction_start_date)
    .bind(command.completion_date)
    .bind(
        command
            .lot_sales_status
            .map(IndustrialComplexLotSalesStatus::wire_name),
    )
    .bind(command.business_period_raw.as_deref())
    .bind(command.business_period_start_month.as_deref())
    .bind(command.business_period_end_month.as_deref())
    .bind(command.development_progress_percent.as_deref())
    .bind(command.designation_basis_law_raw.as_deref())
    .bind(command.development_method_raw.as_deref())
    .bind(command.development_purpose_raw.as_deref())
    .bind(command.invited_industries_raw.as_deref())
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
    let event = CatalogEvent::IndustrialComplexCreatedV3(inserted.created_event());
    insert_outbox_event(tx, &event).await?;
    Ok(inserted)
}

fn changed_industrial_complex_fields(
    existing: &IndustrialComplex,
    command: &UpsertIndustrialComplexCommand,
) -> Vec<String> {
    let mut fields = Vec::new();
    if existing.lakehouse_complex_id != command.lakehouse_complex_id {
        fields.push("lakehouse_complex_id".to_owned());
    }
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
    if existing.status != command.status {
        fields.push("status".to_owned());
    }
    if existing.sido_code != command.sido_code {
        fields.push("sido_code".to_owned());
    }
    if existing.sigungu_code != command.sigungu_code {
        fields.push("sigungu_code".to_owned());
    }
    if existing.address_text != command.address_text {
        fields.push("address_text".to_owned());
    }
    if existing.management_agency_name != command.management_agency_name {
        fields.push("management_agency_name".to_owned());
    }
    if existing.developer_name != command.developer_name {
        fields.push("developer_name".to_owned());
    }
    if existing.designated_date != command.designated_date {
        fields.push("designated_date".to_owned());
    }
    if existing.completion_date != command.completion_date {
        fields.push("completion_date".to_owned());
    }
    if existing.construction_start_date != command.construction_start_date {
        fields.push("construction_start_date".to_owned());
    }
    // Compared as text, which is how the row carries it. `59.90` and `59.9` are the same number and
    // different text, and the comparison would call a re-load a change; the producer emits two
    // decimal places for every value, and Postgres renders `numeric(5,2)` the same way, so both
    // sides of this comparison are already normalized.
    if existing.development_progress_percent != command.development_progress_percent {
        fields.push("development_progress_percent".to_owned());
    }
    if existing.lot_sales_status != command.lot_sales_status {
        fields.push("lot_sales_status".to_owned());
    }
    if existing.business_period_raw != command.business_period_raw {
        fields.push("business_period_raw".to_owned());
    }
    if existing.business_period_start_month != command.business_period_start_month {
        fields.push("business_period_start_month".to_owned());
    }
    if existing.business_period_end_month != command.business_period_end_month {
        fields.push("business_period_end_month".to_owned());
    }
    if existing.designation_basis_law_raw != command.designation_basis_law_raw {
        fields.push("designation_basis_law_raw".to_owned());
    }
    if existing.development_method_raw != command.development_method_raw {
        fields.push("development_method_raw".to_owned());
    }
    if existing.development_purpose_raw != command.development_purpose_raw {
        fields.push("development_purpose_raw".to_owned());
    }
    if existing.invited_industries_raw != command.invited_industries_raw {
        fields.push("invited_industries_raw".to_owned());
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
