//! Vector tile manifest promotion: the active manifest switch and its artifact rows. Moved out of
//! `unit_of_work.rs` unchanged.

use super::*;

pub(super) struct ActiveVectorTileManifest {
    pub(super) id: Uuid,
    pub(super) current_version: String,
}

pub(super) async fn lock_active_vector_tile_manifest_tx(
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

pub(super) fn ensure_promote_can_replace_active(
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

pub(super) async fn insert_promoted_vector_tile_manifest_tx(
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

pub(super) async fn insert_vector_tile_manifest_row_tx(
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

pub(super) async fn insert_vector_tile_artifacts_tx(
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

pub(super) async fn switch_active_vector_tile_manifest_tx(
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

pub(super) fn promoted_vector_tile_manifest_event(
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

pub(super) fn validate_promotion_command(
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

pub(super) async fn insert_vector_tile_source_record_tx(
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

pub(super) async fn insert_vector_tile_file_asset_tx(
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

pub(super) async fn insert_vector_tile_artifact_tx(
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

pub(super) async fn load_vector_tile_manifest_by_id_tx(
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
