//! Promotes one validated administrative PostGIS projection as a complete runtime-manifest unit.
//! The CAS function remains the only visibility switch; this command only prepares its immutable
//! release and complete next manifest.

use std::env;

use anyhow::{bail, Context};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CONFIRM";
const DATA_REVISION_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID";
const SOURCE_FILE_ASSET_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID";
const EXPECTED_MANIFEST_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID";
const RELEASE_ID_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID";
const MANIFEST_ID_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID";
const TILES_URL_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let pool = PgPool::connect(&config.database_url)
        .await
        .context("failed to connect to DATABASE_URL for administrative runtime promotion")?;
    let mut transaction = pool.begin().await?;
    let (current_manifest, current_generation) = current_manifest(&mut transaction).await?;
    if current_manifest != config.expected_manifest_id {
        bail!(
            "runtime manifest compare-and-swap precondition failed: expected {:?}, current {:?}",
            config.expected_manifest_id,
            current_manifest
        );
    }
    verify_inputs(&mut transaction, &config).await?;
    let publication_unit_id = ensure_admin_unit(&mut transaction).await?;
    insert_release(&mut transaction, &config, publication_unit_id).await?;
    let next_generation = current_generation.map_or(1, |value| value + 1);
    sqlx::query(
        "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation, published_at)
         VALUES ($1, $2, now())",
    )
    .bind(config.manifest_id)
    .bind(next_generation)
    .execute(&mut *transaction)
    .await
    .context("failed to create administrative runtime manifest")?;

    let units = sqlx::query(
        "SELECT id, active_release_id, active_data_revision, serving_generation
           FROM catalog.vector_tile_publication_unit ORDER BY unit_key",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for unit in units {
        let unit_id: Uuid = unit.try_get("id")?;
        let (release_id, data_revision, serving_generation) = if unit_id == publication_unit_id {
            let active_release = unit.try_get::<Option<Uuid>, _>("active_release_id")?;
            let current_serving_generation = unit.try_get::<i64, _>("serving_generation")?;
            (
                config.release_id,
                config.data_revision,
                if active_release.is_some() {
                    current_serving_generation + 1
                } else {
                    1
                },
            )
        } else {
            (
                unit.try_get::<Option<Uuid>, _>("active_release_id")?
                    .context("every publication unit must have an active release")?,
                unit.try_get::<Option<Uuid>, _>("active_data_revision")?
                    .context("every publication unit must have an active data revision")?,
                // Unchanged, not `+ 1`. This unit re-selects the release it already serves, and
                // `20260730000003_serving_generation_tracks_one_unit_source_selection.sql` requires a
                // re-selected release to hold its generation — the value tracks one unit's source
                // selection, and carrying it forward changes nothing about it. The `+ 1` here was
                // correct under the previous rule and was left behind when that rule was narrowed.
                unit.try_get::<i64, _>("serving_generation")?,
            )
        };
        let canonical_snapshot = sqlx::query_scalar::<_, String>(
            "SELECT canonical_iceberg_snapshot_id FROM catalog.vector_tile_release WHERE id = $1",
        )
        .bind(release_id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
                (manifest_id, publication_unit_id, release_id, serving_generation,
                 data_revision, canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(config.manifest_id)
        .bind(unit_id)
        .bind(release_id)
        .bind(serving_generation)
        .bind(data_revision)
        .bind(canonical_snapshot)
        .execute(&mut *transaction)
        .await?;
    }

    let promoted_generation =
        sqlx::query_scalar::<_, i64>("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
            .bind(current_manifest)
            .bind(config.manifest_id)
            .fetch_one(&mut *transaction)
            .await
            .context("administrative runtime manifest CAS failed")?;
    transaction.commit().await?;
    println!(
        "administrative-boundary-runtime-promote-ok manifest_generation={} manifest_id={} release_id={}",
        promoted_generation, config.manifest_id, config.release_id
    );
    Ok(())
}

struct Config {
    database_url: String,
    data_revision: Uuid,
    canonical_snapshot_id: String,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    expected_manifest_id: Option<Uuid>,
    release_id: Uuid,
    manifest_id: Uuid,
    tiles_url_template: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if !matches!(
            env::var(CONFIRM_ENV)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes"
        ) {
            bail!("{CONFIRM_ENV}=1 is required before runtime promotion");
        }
        let parse_uuid = |name: &str| -> anyhow::Result<Uuid> {
            Uuid::parse_str(&required_env_value(name)?)
                .with_context(|| format!("{name} must be a UUID"))
        };
        let canonical_snapshot_id = required_env_value(CANONICAL_SNAPSHOT_ENV)?;
        if canonical_snapshot_id.is_empty()
            || canonical_snapshot_id == "0"
            || !canonical_snapshot_id
                .bytes()
                .all(|byte| byte.is_ascii_digit())
        {
            bail!("{CANONICAL_SNAPSHOT_ENV} must be a positive decimal snapshot id");
        }
        let tiles_url_template = required_env_value(TILES_URL_ENV)?;
        if !tiles_url_template.ends_with("/admin/{z}/{x}/{y}")
            || !tiles_url_template.starts_with("http")
        {
            bail!("{TILES_URL_ENV} must end with /admin/{{z}}/{{x}}/{{y}}");
        }
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            data_revision: parse_uuid(DATA_REVISION_ENV)?,
            canonical_snapshot_id,
            source_record_id: parse_uuid(SOURCE_RECORD_ENV)?,
            source_file_asset_id: parse_uuid(SOURCE_FILE_ASSET_ENV)?,
            expected_manifest_id: optional_env_value(EXPECTED_MANIFEST_ENV)?
                .map(|value| {
                    Uuid::parse_str(&value)
                        .with_context(|| format!("{EXPECTED_MANIFEST_ENV} must be a UUID"))
                })
                .transpose()?,
            release_id: parse_uuid(RELEASE_ID_ENV)?,
            manifest_id: parse_uuid(MANIFEST_ID_ENV)?,
            tiles_url_template,
        })
    }
}

async fn current_manifest(
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<(Option<Uuid>, Option<i64>)> {
    let row = sqlx::query(
        "SELECT pointer.manifest_id, manifest.manifest_generation
           FROM catalog.vector_tile_runtime_manifest_pointer AS pointer
           JOIN catalog.vector_tile_runtime_manifest AS manifest
             ON manifest.id = pointer.manifest_id
          WHERE pointer.singleton = true",
    )
    .fetch_optional(&mut **transaction)
    .await?;
    match row {
        Some(row) => Ok((
            Some(row.try_get("manifest_id")?),
            Some(row.try_get("manifest_generation")?),
        )),
        None => Ok((None, None)),
    }
}

async fn verify_inputs(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT canonical_iceberg_snapshot_id, source_record_id, status
           FROM catalog.administrative_boundary_revision WHERE id = $1",
    )
    .bind(config.data_revision)
    .fetch_one(&mut **transaction)
    .await
    .context("administrative boundary revision does not exist")?;
    if row.try_get::<String, _>("canonical_iceberg_snapshot_id")? != config.canonical_snapshot_id
        || row.try_get::<Uuid, _>("source_record_id")? != config.source_record_id
        || !matches!(
            row.try_get::<String, _>("status")?.as_str(),
            "validated" | "published"
        )
    {
        bail!("administrative revision provenance/status is not promotable");
    }
    let source_count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM catalog.source_record WHERE id = $1")
            .bind(config.source_record_id)
            .fetch_one(&mut **transaction)
            .await?;
    let file_count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM catalog.file_asset WHERE id = $1")
            .bind(config.source_file_asset_id)
            .fetch_one(&mut **transaction)
            .await?;
    if source_count != 1 || file_count != 1 {
        bail!("runtime promotion lineage source_record/file_asset is missing");
    }
    Ok(())
}

async fn ensure_admin_unit(transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
         VALUES (gen_random_uuid(), 'admin') ON CONFLICT (unit_key) DO NOTHING",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = 'admin'",
    )
    .fetch_one(&mut **transaction)
    .await?)
}

async fn insert_release(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
    publication_unit_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
             source_record_id, source_file_asset_ids, source_kind, martin_source_id,
             tiles_url_template, postgis_projection_revision)
         VALUES ($1, $2, $3, $4, $5, $6, 'dynamic_postgis', 'admin', $7, $3)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(config.release_id)
    .bind(publication_unit_id)
    .bind(config.data_revision)
    .bind(&config.canonical_snapshot_id)
    .bind(config.source_record_id)
    .bind(vec![config.source_file_asset_id])
    .bind(&config.tiles_url_template)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release_layer
            (release_id, layer_id, source_layer, feature_id_property,
             tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
             feature_filter_properties)
         VALUES ($1, 'admin', 'admin', 'administrative_unit_id', 5, 16, 5, 16,
                 $2::jsonb)
         ON CONFLICT (release_id, layer_id) DO NOTHING",
    )
    .bind(config.release_id)
    .bind(json!({
        "canonical_code": "canonical_code",
        "scope_kind": "scope_kind"
    }))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
