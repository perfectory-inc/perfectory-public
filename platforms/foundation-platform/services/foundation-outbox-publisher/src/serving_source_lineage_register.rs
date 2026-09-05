//! Registers a serving-source lineage pair (and optional revision) from measured values.
//!
//! Three times in one week the source_record/file_asset pair a publish lane requires was
//! inserted by hand-typed SQL — for the parcel mirror and twice for the administrative
//! boundary lane. A hand-typed insert has no validation, no idempotency, and no evidence;
//! the third occurrence made it a named debt. This command is the paved road: every value
//! is a measured input (checksums, sizes, real object keys — lineage is never invented,
//! and publication refuses fabricated values three layers down), the insert is one
//! transaction, a repeat run with identical values reuses the existing rows, and the same
//! identity with different facts refuses loudly.

use std::path::PathBuf;

use anyhow::{bail, Context};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const ENV_PREFIX: &str = "FOUNDATION_PLATFORM_SERVING_SOURCE_LINEAGE";

/// Lanes that hang a candidate revision off the registered pair.
const REVISION_LANES: [&str; 1] = ["administrative-boundary"];

#[derive(Debug)]
struct Config {
    source: String,
    external_id: String,
    source_checksum_sha256: String,
    raw_object_key: String,
    file_object_key: String,
    file_mime_type: String,
    file_size_bytes: i64,
    file_checksum_sha256: String,
    revision_lane: Option<String>,
    canonical_iceberg_snapshot_id: Option<String>,
    source_snapshot_id: Option<String>,
    summary_path: Option<PathBuf>,
    database_url: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let confirmed = optional_bool_env(&format!("{ENV_PREFIX}_CONFIRM"))?.unwrap_or(false);
        if !confirmed {
            bail!("{ENV_PREFIX}_CONFIRM=true is required: this command writes catalog lineage");
        }
        let config = Self {
            source: required_env_value(&format!("{ENV_PREFIX}_SOURCE"))?,
            external_id: required_env_value(&format!("{ENV_PREFIX}_EXTERNAL_ID"))?,
            source_checksum_sha256: required_env_value(&format!(
                "{ENV_PREFIX}_SOURCE_CHECKSUM_SHA256"
            ))?,
            raw_object_key: required_env_value(&format!("{ENV_PREFIX}_RAW_OBJECT_KEY"))?,
            file_object_key: required_env_value(&format!("{ENV_PREFIX}_FILE_OBJECT_KEY"))?,
            file_mime_type: required_env_value(&format!("{ENV_PREFIX}_FILE_MIME_TYPE"))?,
            file_size_bytes: required_env_value(&format!("{ENV_PREFIX}_FILE_SIZE_BYTES"))?
                .parse::<i64>()
                .context("file size must be an integer byte count")?,
            file_checksum_sha256: required_env_value(&format!(
                "{ENV_PREFIX}_FILE_CHECKSUM_SHA256"
            ))?,
            revision_lane: optional_env_value(&format!("{ENV_PREFIX}_REVISION_LANE"))?,
            canonical_iceberg_snapshot_id: optional_env_value(&format!(
                "{ENV_PREFIX}_CANONICAL_ICEBERG_SNAPSHOT_ID"
            ))?,
            source_snapshot_id: optional_env_value(&format!("{ENV_PREFIX}_SOURCE_SNAPSHOT_ID"))?,
            summary_path: optional_env_value(&format!("{ENV_PREFIX}_SUMMARY_PATH"))?
                .map(PathBuf::from),
            database_url: required_env_value("DATABASE_URL")?,
        };
        validate(&config)?;
        Ok(config)
    }
}

fn validate(config: &Config) -> anyhow::Result<()> {
    for (label, value) in [
        ("source checksum", &config.source_checksum_sha256),
        ("file checksum", &config.file_checksum_sha256),
    ] {
        if !is_sha256_hex(value) {
            bail!("{label} must be 64 lowercase hex characters — a measured value, not a label");
        }
    }
    if config.file_size_bytes <= 0 {
        bail!("file size must be a positive measured byte count");
    }
    if let Some(lane) = &config.revision_lane {
        if !REVISION_LANES.contains(&lane.as_str()) {
            bail!(
                "unknown revision lane {lane:?}; this command hangs revisions for: {}",
                REVISION_LANES.join(", ")
            );
        }
        if config.canonical_iceberg_snapshot_id.is_none() || config.source_snapshot_id.is_none() {
            bail!(
                "a revision lane requires {ENV_PREFIX}_CANONICAL_ICEBERG_SNAPSHOT_ID and \
                 {ENV_PREFIX}_SOURCE_SNAPSHOT_ID"
            );
        }
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Runs the lineage registration.
///
/// # Errors
/// Returns an error when configuration is invalid, an existing row carries different facts
/// for the same identity, or the database refuses the transaction.
pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let mut conn = PgConnection::connect(&config.database_url)
        .await
        .context("failed to connect to the catalog database")?;
    let mut tx = conn
        .begin()
        .await
        .context("failed to begin the lineage transaction")?;

    let (source_record_id, source_record_reused) = upsert_source_record(&mut tx, &config).await?;
    let (file_asset_id, file_asset_reused) =
        upsert_file_asset(&mut tx, &config, source_record_id).await?;
    let revision = match config.revision_lane.as_deref() {
        Some("administrative-boundary") => {
            Some(upsert_admin_boundary_revision(&mut tx, &config, source_record_id).await?)
        }
        _ => None,
    };

    tx.commit()
        .await
        .context("failed to commit the lineage transaction")?;

    let summary = serde_json::json!({
        "schema_version": "foundation-platform.serving_source_lineage_registration.v1",
        "source_record_id": source_record_id,
        "source_record_reused": source_record_reused,
        "file_asset_id": file_asset_id,
        "file_asset_reused": file_asset_reused,
        "revision": revision.map(|(id, reused)| serde_json::json!({
            "id": id,
            "reused": reused,
            "lane": config.revision_lane,
        })),
        "source": config.source,
        "external_id": config.external_id,
    });
    if let Some(path) = &config.summary_path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create summary directory {}", parent.display())
            })?;
        }
        std::fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&summary)?),
        )
        .with_context(|| format!("failed to write summary {}", path.display()))?;
    }
    tracing::info!(
        source_record_id = %source_record_id,
        source_record_reused,
        file_asset_id = %file_asset_id,
        file_asset_reused,
        revision = ?revision,
        "serving-source-lineage-register-ok"
    );
    Ok(())
}

/// Inserts or reuses the source record for `(source, external_id)`.
///
/// Reuse requires every recorded fact to match; the same identity with a different checksum
/// or object key is two claims about one thing and refuses.
async fn upsert_source_record(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &Config,
) -> anyhow::Result<(Uuid, bool)> {
    let existing = sqlx::query(
        "SELECT id, checksum_sha256, raw_object_key FROM catalog.source_record
         WHERE source = $1 AND external_id = $2",
    )
    .bind(&config.source)
    .bind(&config.external_id)
    .fetch_optional(&mut **tx)
    .await
    .context("failed to look up the source record")?;
    if let Some(row) = existing {
        let checksum: String = row.try_get("checksum_sha256")?;
        let raw_key: Option<String> = row.try_get("raw_object_key")?;
        if checksum != config.source_checksum_sha256
            || raw_key.as_deref() != Some(config.raw_object_key.as_str())
        {
            bail!(
                "source record for {}/{} already exists with different facts; refusing to \
                 register a second claim",
                config.source,
                config.external_id
            );
        }
        return Ok((row.try_get("id")?, true));
    }
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256, raw_object_key)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(&config.source)
    .bind(&config.external_id)
    .bind(&config.source_checksum_sha256)
    .bind(&config.raw_object_key)
    .execute(&mut **tx)
    .await
    .context("failed to insert the source record")?;
    Ok((id, false))
}

async fn upsert_file_asset(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &Config,
    source_record_id: Uuid,
) -> anyhow::Result<(Uuid, bool)> {
    let existing = sqlx::query(
        "SELECT id, checksum_sha256, size_bytes FROM catalog.file_asset
         WHERE object_key = $1 AND source_record_id = $2",
    )
    .bind(&config.file_object_key)
    .bind(source_record_id)
    .fetch_optional(&mut **tx)
    .await
    .context("failed to look up the file asset")?;
    if let Some(row) = existing {
        let checksum: Option<String> = row.try_get("checksum_sha256")?;
        let size: i64 = row.try_get("size_bytes")?;
        if checksum.as_deref() != Some(config.file_checksum_sha256.as_str())
            || size != config.file_size_bytes
        {
            bail!(
                "file asset for {} already exists with different facts; refusing to register \
                 a second claim",
                config.file_object_key
            );
        }
        return Ok((row.try_get("id")?, true));
    }
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.file_asset
             (id, object_key, mime_type, size_bytes, checksum_sha256, source_record_id, visibility)
         VALUES ($1, $2, $3, $4, $5, $6, 'internal')",
    )
    .bind(id)
    .bind(&config.file_object_key)
    .bind(&config.file_mime_type)
    .bind(config.file_size_bytes)
    .bind(&config.file_checksum_sha256)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .context("failed to insert the file asset")?;
    Ok((id, false))
}

async fn upsert_admin_boundary_revision(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &Config,
    source_record_id: Uuid,
) -> anyhow::Result<(Uuid, bool)> {
    let canonical = config
        .canonical_iceberg_snapshot_id
        .as_deref()
        .context("revision lane requires a canonical Iceberg snapshot id")?;
    let source_snapshot = config
        .source_snapshot_id
        .as_deref()
        .context("revision lane requires a source snapshot id")?;
    let existing = sqlx::query(
        "SELECT id, status FROM catalog.administrative_boundary_revision
         WHERE source_record_id = $1
           AND canonical_iceberg_snapshot_id = $2
           AND source_snapshot_id = $3",
    )
    .bind(source_record_id)
    .bind(canonical)
    .bind(source_snapshot)
    .fetch_optional(&mut **tx)
    .await
    .context("failed to look up the boundary revision")?;
    if let Some(row) = existing {
        return Ok((row.try_get("id")?, true));
    }
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.administrative_boundary_revision
             (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status)
         VALUES ($1, $2, $3, $4, 'candidate')",
    )
    .bind(id)
    .bind(canonical)
    .bind(source_snapshot)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .context("failed to insert the boundary revision")?;
    Ok((id, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            source: "vworldkr__boundary_emd".to_owned(),
            external_id: "LSMD_ADM_SECT_UMD_202606_sido17".to_owned(),
            source_checksum_sha256: "a".repeat(64),
            raw_object_key: "bronze/source=vworldkr__boundary_emd/".to_owned(),
            file_object_key: "target/source/official-administrative-boundary-snapshot.jsonl"
                .to_owned(),
            file_mime_type: "application/jsonl".to_owned(),
            file_size_bytes: 139_561_876,
            file_checksum_sha256: "b".repeat(64),
            revision_lane: None,
            canonical_iceberg_snapshot_id: None,
            source_snapshot_id: None,
            summary_path: None,
            database_url: "postgres://unused".to_owned(),
        }
    }

    #[test]
    fn measured_values_pass_validation() {
        assert!(validate(&config()).is_ok());
    }

    #[test]
    fn a_checksum_that_is_not_a_measurement_is_refused() {
        let mut invalid = config();
        invalid.source_checksum_sha256 = "measured-later".to_owned();
        let error = validate(&invalid).expect_err("a non-hex checksum must refuse");
        assert!(error.to_string().contains("measured"), "{error}");
    }

    #[test]
    fn a_revision_lane_demands_its_snapshots() {
        let mut missing = config();
        missing.revision_lane = Some("administrative-boundary".to_owned());
        let error = validate(&missing).expect_err("a lane without snapshots must refuse");
        assert!(error.to_string().contains("SNAPSHOT"), "{error}");

        let mut unknown = config();
        unknown.revision_lane = Some("parcel".to_owned());
        let error = validate(&unknown).expect_err("an unknown lane must refuse");
        assert!(
            error.to_string().contains("unknown revision lane"),
            "{error}"
        );
    }

    #[test]
    fn a_zero_byte_file_claim_is_refused() {
        let mut invalid = config();
        invalid.file_size_bytes = 0;
        assert!(validate(&invalid).is_err());
    }
}
