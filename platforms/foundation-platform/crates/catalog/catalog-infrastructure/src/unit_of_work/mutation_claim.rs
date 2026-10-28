//! Idempotency-key claims, recorded artifact identity checks and the mutation outbox event.
//! Moved out of `unit_of_work.rs` unchanged.

use super::*;

/// Claims a build-start idempotency key; `false` means the identical committed start is replayed.
pub(super) async fn claim_build_start_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &StartVectorTileBuildCommand,
) -> Result<bool, CatalogError> {
    let fingerprint = command.request_fingerprint();
    claim_build_key_tx(
        tx,
        &command.idempotency_key,
        CatalogMutationKind::StartVectorTileBuild,
        &fingerprint,
        command.operator_staff_id,
    )
    .await
}

pub(super) async fn claim_build_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    idempotency_key: &str,
    kind: CatalogMutationKind,
    fingerprint: &RequestFingerprint,
    operator_staff_id: StaffId,
) -> Result<bool, CatalogError> {
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO catalog.catalog_mutation_idempotency
         (idempotency_key, command_kind, request_fingerprint_sha256,
          request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
         VALUES ($1, $2, $3, $4, NULL, $5)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING idempotency_key",
    )
    .bind(idempotency_key)
    .bind(kind.as_str())
    .bind(fingerprint.as_str())
    .bind(CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION)
    .bind(operator_staff_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| map_contention_error(idempotency_key, error))?;
    if claimed.is_some() {
        return Ok(true);
    }

    let row = read_mutation_claim_tx(tx, idempotency_key).await?;
    verify_mutation_claim(&row, idempotency_key, kind, fingerprint)?;
    Ok(false)
}

pub(super) async fn claim_manifest_mutation_key_tx(
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

pub(super) async fn read_mutation_claim_tx(
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

pub(super) fn verify_mutation_claim(
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

pub(super) fn validate_build_artifact_identity(
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

pub(super) fn ensure_recorded_artifact_matches(
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
pub(super) async fn claim_mutation_key_tx(
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
pub(super) async fn record_mutation_outbox_event_tx(
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
pub(super) fn map_contention_error(idempotency_key: &str, error: sqlx::Error) -> CatalogError {
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
