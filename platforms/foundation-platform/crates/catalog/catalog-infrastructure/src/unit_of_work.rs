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
    CatalogUnitOfWork, UpsertIndustrialComplexCommand, VectorTileArtifactPromotionCommand,
    VectorTileFileAssetCommand, VectorTileManifestPromotionCommand,
    VectorTileManifestRollbackCommand, VectorTileSourceRecordCommand,
};
use catalog_domain::{
    CatalogError, ComplexMutation, IndustrialComplex, Parcel, ParcelKind, VectorTileArtifact,
    VectorTileManifest,
};
use chrono::Utc;
use foundation_shared_kernel::events::catalog_v1::{
    CatalogEvent, VectorTileManifestPromotedV1, VectorTileManifestRolledBackV1,
};
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, ParcelId, StaffId, VectorTileManifestId,
};
use serde_json::Value as JsonValue;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::row_map::{
    is_unique_violation_code, map_sqlx, row_to_complex, row_to_parcel, row_to_vector_tile_artifact,
    row_to_vector_tile_manifest, u64_to_i64,
};

/// `PostgreSQL` implementation of Catalog mutation unit-of-work ports.
pub struct PgCatalogUnitOfWork {
    pool: PgPool,
}

impl PgCatalogUnitOfWork {
    /// Creates a unit-of-work backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
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
          manifest_file_asset_id, source_record_id, is_active, version)
         VALUES ($1, $2, $3, $4, $5, $6, false, 1)",
    )
    .bind(manifest_id)
    .bind(&command.current_version)
    .bind(previous_version)
    .bind(&command.tiles_url_template)
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
                manifest_file_asset_id, source_record_id, published_at,
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
