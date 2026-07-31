//! Materializes a validated official-boundary source snapshot into the append-only PostGIS
//! serving projection. The source snapshot and registry remain the evidence SSOT; this command
//! only creates a projection bound to one existing Catalog revision.

use std::{env, fs, io::BufRead, io::BufReader, path::PathBuf};

use anyhow::{bail, Context};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::public_data_control_support::required_env_value;

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CONFIRM";
const SOURCE_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH";
const REGISTRY_EVIDENCE_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_REGISTRY_EVIDENCE_PATH";
const DATA_REVISION_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID";
const SOURCE_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY";
/// The `catalog.vector_tile_publication_unit.unit_key` this command materialises.
///
/// `catalog.promote_vector_tile_runtime_manifest` refuses a release whose projection load names a
/// different unit than the manifest selects, so this string and the one the promote command creates
/// the unit with are the same fact. Declared once so they cannot drift into a rejection that reads
/// like a data problem.
pub(crate) const ADMINISTRATIVE_UNIT_KEY: &str = "admin";
const FORBIDDEN_SOURCE_PROVIDERS: &[&str] = &[
    "VWorld",
    "data.go.kr",
    "provider-parcel",
    "vworld_parcel_boundaries_silver_handoff_jsonl",
];

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let rows = read_rows(&config.source_path, &config.source_snapshot_id)?;
    ensure_registry_ready(&config.registry_evidence_path)?;
    let pool = PgPool::connect(&config.database_url)
        .await
        .context("failed to connect to DATABASE_URL for administrative boundary PostGIS publish")?;
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut *transaction)
        .await?;
    verify_revision(&mut transaction, &config).await?;

    // The publication unit is ensured here as well as in the promote command. It used to exist only
    // there, so a database where publish had run but promote had not held loads naming a unit that
    // did not exist — a state no foreign key could describe until this command could create it too.
    let publication_unit_id = ensure_administrative_unit(&mut transaction).await?;

    // The revision this publishes, registered against the unit it revises. Same id as the
    // administrative boundary revision it derives from, deliberately: the operator names one
    // `DATA_REVISION` and it must not become two unrelated identifiers in two ledgers. The
    // administrative ledger keeps the boundary *fact*; this one records that a publication unit has
    // a revision built from that fact's canonical snapshot.
    sqlx::query(
        "INSERT INTO catalog.publication_revision
            (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
             derived_from_administrative_revision)
         VALUES ($1, $2, $3, $4, $1)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(config.data_revision)
    .bind(publication_unit_id)
    .bind(&config.canonical_snapshot_id)
    .bind(config.source_record_id)
    .execute(&mut *transaction)
    .await
    .context("failed to register the administrative publication revision")?;

    // The load is opened before a single geometry lands, so the rows this run writes carry the
    // identity of the run that wrote them. Keying the projection on the load rather than on the
    // revision is what makes a re-publish of one revision a second, separately serviceable fact
    // instead of an in-place overwrite that no row records.
    let projection_load_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, status)
         VALUES ($1, $2, $3, $4, 'running')",
    )
    .bind(projection_load_id)
    .bind(publication_unit_id)
    .bind(config.data_revision)
    .bind(&config.canonical_snapshot_id)
    .execute(&mut *transaction)
    .await
    .context("failed to open the administrative PostGIS projection load")?;

    let mut geometry_count = 0i64;
    for row in rows {
        let unit_id = ensure_unit(&mut transaction, &row).await?;
        if !row.parent_scope_kind.trim().is_empty() {
            let parent_id = ensure_parent_unit(&mut transaction, &row).await?;
            publish_parent(&mut transaction, &config, &row, unit_id, parent_id).await?;
        }
        if let Some(geometry) = row.geometry.as_ref() {
            publish_geometry(
                &mut transaction,
                &config,
                &row,
                unit_id,
                geometry,
                projection_load_id,
            )
            .await?;
            geometry_count += 1;
        }
        if !row.source_name.trim().is_empty() {
            publish_identifier(&mut transaction, &config, &row, unit_id).await?;
        }
    }

    // Counted by load, not by revision. Under the old revision-scoped count a re-publish that wrote
    // nothing still read a previous run's rows and reported them as its own.
    let publication_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM serving_postgis.administrative_unit_boundary_publication
          WHERE projection_load_id = $1",
    )
    .bind(projection_load_id)
    .fetch_one(&mut *transaction)
    .await?;
    if publication_count == 0 {
        bail!("administrative boundary publish produced no geometries");
    }
    // Every offered geometry that did not become its own row in this load: a second source row for a
    // unit this load already carries is skipped by the conflict clause below. It is derived from what
    // is in the table rather than assumed, so it cannot claim a rejection that did not happen. A row
    // whose geometry failed `st_isvalid` never reaches here — `publish_geometry` aborts the whole
    // transaction, and a failed load leaves no ledger row at all.
    let rejected_count = geometry_count - publication_count;
    sqlx::query(
        "UPDATE catalog.administrative_boundary_revision
            SET status = CASE WHEN status = 'candidate' THEN 'validated' ELSE status END,
                validated_at = COALESCE(validated_at, now())
          WHERE id = $1",
    )
    .bind(config.data_revision)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET status = 'succeeded',
                loaded_row_count = $2,
                rejected_row_count = $3,
                finished_at = now()
          WHERE id = $1 AND status = 'running'",
    )
    .bind(projection_load_id)
    .bind(publication_count)
    .bind(rejected_count)
    .execute(&mut *transaction)
    .await
    .context("failed to close the administrative PostGIS projection load")?;
    transaction.commit().await?;
    println!(
        "administrative-boundary-postgis-publish-ok geometries={} publication_rows={} rejected_rows={} data_revision={} projection_load_id={}",
        geometry_count, publication_count, rejected_count, config.data_revision, projection_load_id
    );
    Ok(())
}

struct Config {
    database_url: String,
    source_path: PathBuf,
    registry_evidence_path: PathBuf,
    data_revision: Uuid,
    canonical_snapshot_id: String,
    source_snapshot_id: String,
    source_record_id: Uuid,
    source_object_key: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if !bool_env(CONFIRM_ENV)? {
            bail!("{CONFIRM_ENV}=1 is required before writing administrative boundary PostGIS");
        }
        let data_revision = Uuid::parse_str(&required_env_value(DATA_REVISION_ENV)?)
            .context("data revision must be a UUID")?;
        let source_record_id = Uuid::parse_str(&required_env_value(SOURCE_RECORD_ENV)?)
            .context("source record id must be a UUID")?;
        let canonical_snapshot_id = required_env_value(CANONICAL_SNAPSHOT_ENV)?;
        if !is_positive_digits(&canonical_snapshot_id) {
            bail!("{CANONICAL_SNAPSHOT_ENV} must be a positive decimal snapshot id");
        }
        let source_snapshot_id = required_env_value(SOURCE_SNAPSHOT_ENV)?;
        if !source_snapshot_id.starts_with("iceberg:") {
            bail!("{SOURCE_SNAPSHOT_ENV} must use the iceberg: source snapshot contract");
        }
        let source_object_key = required_env_value(SOURCE_OBJECT_KEY_ENV)?;
        if source_object_key.starts_with('/') || source_object_key.contains("..") {
            bail!("{SOURCE_OBJECT_KEY_ENV} must be a relative immutable object key");
        }
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            source_path: PathBuf::from(env::var(SOURCE_PATH_ENV).unwrap_or_else(|_| {
                "target/source/official-administrative-boundary-snapshot.jsonl".to_owned()
            })),
            registry_evidence_path: PathBuf::from(
                env::var(REGISTRY_EVIDENCE_PATH_ENV).unwrap_or_else(|_| {
                    "target/audit/administrative-spatial-scope-registry-evidence.json".to_owned()
                }),
            ),
            data_revision,
            canonical_snapshot_id,
            source_snapshot_id,
            source_record_id,
            source_object_key,
        })
    }
}

#[derive(Deserialize)]
struct SourceRow {
    scope_kind: String,
    canonical_code: String,
    valid_from_utc: String,
    status: String,
    geometry_srid: i64,
    source_provider: String,
    source_snapshot_id: String,
    source_name: String,
    parent_scope_kind: String,
    parent_canonical_code: String,
    geometry: Option<JsonValue>,
    geometry_sha256: Option<String>,
}

fn read_rows(path: &PathBuf, expected_snapshot: &str) -> anyhow::Result<Vec<SourceRow>> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("failed to read source line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: SourceRow = serde_json::from_str(&line)
            .with_context(|| format!("invalid source JSONL at line {}", index + 1))?;
        if row.source_snapshot_id != expected_snapshot {
            bail!("source_snapshot_id mismatch at line {}", index + 1);
        }
        if row.status != "active" || row.geometry_srid != 4326 {
            bail!("source row {} is not an active EPSG:4326 row", index + 1);
        }
        if row.source_provider.trim().is_empty()
            || FORBIDDEN_SOURCE_PROVIDERS.contains(&row.source_provider.as_str())
        {
            bail!(
                "source row {} is not from an allowed official boundary provider",
                index + 1
            );
        }
        if !matches!(row.scope_kind.as_str(), "sido" | "sigungu" | "legal_dong") {
            bail!(
                "unsupported administrative scope kind at line {}",
                index + 1
            );
        }
        if let Some(geometry) = row.geometry.as_ref() {
            if !matches!(json_type(geometry), "Polygon" | "MultiPolygon") {
                bail!("administrative boundary geometry must be Polygon or MultiPolygon");
            }
            let expected = row
                .geometry_sha256
                .as_deref()
                .context("geometry_sha256 is required when geometry is present")?;
            let actual = json_sha256(geometry)?;
            if expected != actual {
                bail!("geometry_sha256 mismatch at line {}", index + 1);
            }
        }
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("administrative boundary source snapshot contains no rows");
    }
    Ok(rows)
}

fn ensure_registry_ready(path: &PathBuf) -> anyhow::Result<()> {
    let value: JsonValue = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )?;
    if value.get("status").and_then(JsonValue::as_str) != Some("ready") {
        bail!("administrative spatial scope registry evidence is not ready");
    }
    Ok(())
}

async fn verify_revision(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT revision.canonical_iceberg_snapshot_id, revision.source_snapshot_id,
                revision.source_record_id, source.raw_object_key, revision.status
           FROM catalog.administrative_boundary_revision AS revision
           JOIN catalog.source_record AS source ON source.id = revision.source_record_id
          WHERE revision.id = $1",
    )
    .bind(config.data_revision)
    .fetch_optional(&mut **transaction)
    .await?
    .context("administrative boundary data revision does not exist")?;
    let snapshot: String = row.try_get("canonical_iceberg_snapshot_id")?;
    let source_snapshot: String = row.try_get("source_snapshot_id")?;
    let source_record: Uuid = row.try_get("source_record_id")?;
    let raw_object_key: Option<String> = row.try_get("raw_object_key")?;
    let status: String = row.try_get("status")?;
    if snapshot != config.canonical_snapshot_id
        || source_snapshot != config.source_snapshot_id
        || source_record != config.source_record_id
        || raw_object_key.as_deref() != Some(config.source_object_key.as_str())
        || status == "superseded"
    {
        bail!("administrative boundary revision provenance or status does not match publish input");
    }
    let source_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM catalog.source_record WHERE id = $1)",
    )
    .bind(config.source_record_id)
    .fetch_one(&mut **transaction)
    .await?;
    if !source_exists {
        bail!("administrative boundary source_record does not exist");
    }
    Ok(())
}

/// Provisions the `admin` vector-tile publication unit if this deployment has not published one yet.
///
/// Shared with `administrative_boundary_runtime_promote`, which is where it used to live alone. A
/// projection load now names its unit by foreign key, so the unit has to exist by the time the load
/// is opened — and the load is opened by *this* command, one operator step before the promote.
pub(crate) async fn ensure_administrative_unit(
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
         VALUES (gen_random_uuid(), $1) ON CONFLICT (unit_key) DO NOTHING",
    )
    .bind(ADMINISTRATIVE_UNIT_KEY)
    .execute(&mut **transaction)
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = $1",
    )
    .bind(ADMINISTRATIVE_UNIT_KEY)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn ensure_parent_unit(
    transaction: &mut Transaction<'_, Postgres>,
    row: &SourceRow,
) -> anyhow::Result<Uuid> {
    let parent_stable_key = format!(
        "scope:{}:{}",
        scope_kind_slug(&row.parent_scope_kind),
        row.parent_canonical_code
    );
    sqlx::query(
        "INSERT INTO catalog.administrative_unit (id, unit_kind, stable_key)
         VALUES (gen_random_uuid(), $1, $2) ON CONFLICT (stable_key) DO NOTHING",
    )
    .bind(&row.parent_scope_kind)
    .bind(&parent_stable_key)
    .execute(&mut **transaction)
    .await?;
    let (id, kind): (Uuid, String) = sqlx::query_as(
        "SELECT id, unit_kind FROM catalog.administrative_unit WHERE stable_key = $1",
    )
    .bind(parent_stable_key)
    .fetch_one(&mut **transaction)
    .await?;
    if kind != row.parent_scope_kind {
        bail!("administrative parent stable key has a different unit kind");
    }
    Ok(id)
}

async fn publish_parent(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
    row: &SourceRow,
    child_id: Uuid,
    parent_id: Uuid,
) -> anyhow::Result<()> {
    if child_id == parent_id {
        bail!("administrative unit cannot be its own parent");
    }
    let effective_date = DateTime::parse_from_rfc3339(&row.valid_from_utc)
        .context("source valid_from_utc must be RFC3339")?
        .date_naive();
    let already_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
             SELECT 1 FROM catalog.administrative_unit_parent
              WHERE child_unit_id = $1 AND parent_unit_id = $2
                AND data_revision = $3 AND lower(effective_period) = $4
         )",
    )
    .bind(child_id)
    .bind(parent_id)
    .bind(config.data_revision)
    .bind(effective_date)
    .fetch_one(&mut **transaction)
    .await?;
    if already_exists {
        return Ok(());
    }
    sqlx::query(
        "UPDATE catalog.administrative_unit_parent
            SET effective_period = daterange(lower(effective_period), $2, '[)')
          WHERE child_unit_id = $1 AND upper_inf(effective_period)
            AND lower(effective_period) < $2",
    )
    .bind(child_id)
    .bind(effective_date)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.administrative_unit_parent
            (id, child_unit_id, parent_unit_id, effective_period, data_revision,
             source_snapshot_id, source_record_id)
         VALUES (gen_random_uuid(), $1, $2, daterange($3, NULL, '[)'), $4, $5, $6)",
    )
    .bind(child_id)
    .bind(parent_id)
    .bind(effective_date)
    .bind(config.data_revision)
    .bind(&config.source_snapshot_id)
    .bind(config.source_record_id)
    .execute(&mut **transaction)
    .await
    .context("failed to append administrative parent fact")?;
    Ok(())
}

async fn ensure_unit(
    transaction: &mut Transaction<'_, Postgres>,
    row: &SourceRow,
) -> anyhow::Result<Uuid> {
    let stable_key = format!(
        "scope:{}:{}",
        scope_kind_slug(&row.scope_kind),
        row.canonical_code
    );
    sqlx::query(
        "INSERT INTO catalog.administrative_unit (id, unit_kind, stable_key)
         VALUES (gen_random_uuid(), $1, $2) ON CONFLICT (stable_key) DO NOTHING",
    )
    .bind(&row.scope_kind)
    .bind(&stable_key)
    .execute(&mut **transaction)
    .await?;
    let (id, kind): (Uuid, String) = sqlx::query_as(
        "SELECT id, unit_kind FROM catalog.administrative_unit WHERE stable_key = $1",
    )
    .bind(stable_key)
    .fetch_one(&mut **transaction)
    .await?;
    if kind != row.scope_kind {
        bail!("administrative unit stable key has a different unit kind");
    }
    Ok(id)
}

async fn publish_identifier(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
    row: &SourceRow,
    unit_id: Uuid,
) -> anyhow::Result<()> {
    let effective_date = DateTime::parse_from_rfc3339(&row.valid_from_utc)
        .context("source valid_from_utc must be RFC3339")?
        .date_naive();
    sqlx::query(
        "UPDATE catalog.administrative_unit_identifier
            SET effective_period = daterange(lower(effective_period), $2, '[)')
          WHERE administrative_unit_id = $1 AND authority = 'official'
            AND upper_inf(effective_period) AND lower(effective_period) < $2",
    )
    .bind(unit_id)
    .bind(effective_date)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.administrative_unit_identifier
            (id, administrative_unit_id, authority, code, display_name, effective_period,
             data_revision, source_snapshot_id, source_record_id)
         VALUES (gen_random_uuid(), $1, 'official', $2, $3, daterange($4, NULL, '[)'),
                 $5, $6, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(unit_id)
    .bind(&row.canonical_code)
    .bind(&row.source_name)
    .bind(effective_date)
    .bind(config.data_revision)
    .bind(&config.source_snapshot_id)
    .bind(config.source_record_id)
    .execute(&mut **transaction)
    .await
    .context("failed to append administrative identifier fact")?;
    Ok(())
}

async fn publish_geometry(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
    row: &SourceRow,
    unit_id: Uuid,
    geometry: &JsonValue,
    projection_load_id: Uuid,
) -> anyhow::Result<()> {
    let geometry_json = serde_json::to_string(geometry)?;
    sqlx::query(
        "WITH input AS (
            SELECT public.st_multi(public.st_force2d(public.st_setsrid(
                public.st_geomfromgeojson($1), 4326))) AS geom
         )
         INSERT INTO serving_postgis.administrative_unit_boundary_publication
            (administrative_unit_id, data_revision, canonical_iceberg_snapshot_id,
             source_snapshot_id, source_record_id, source_object_key, scope_kind,
             canonical_code, display_name, geometry_checksum_sha256, geom, properties,
             projection_load_id)
         SELECT $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''),
                encode(public.digest(public.st_asewkb(input.geom), 'sha256'), 'hex'),
                input.geom,
                jsonb_build_object('scope_kind', $8, 'canonical_code', $9,
                                   'display_name', NULLIF($10, '')),
                $11
           FROM input
          WHERE public.st_isvalid(input.geom)
         ON CONFLICT (projection_load_id, administrative_unit_id) DO NOTHING",
    )
    .bind(geometry_json)
    .bind(unit_id)
    .bind(config.data_revision)
    .bind(&config.canonical_snapshot_id)
    .bind(&config.source_snapshot_id)
    .bind(config.source_record_id)
    .bind(&config.source_object_key)
    .bind(&row.scope_kind)
    .bind(&row.canonical_code)
    .bind(&row.source_name)
    .bind(projection_load_id)
    .execute(&mut **transaction)
    .await
    .context("failed to append administrative boundary geometry")?;
    // Keyed by the load this run opened. Against `data_revision` this check read whatever an earlier
    // run of the same revision had left behind, so an insert that the `st_isvalid` filter dropped
    // still passed and the command printed `-ok` having written nothing.
    let valid = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
             SELECT 1 FROM serving_postgis.administrative_unit_boundary_publication
              WHERE projection_load_id = $1 AND administrative_unit_id = $2
               AND public.st_isvalid(geom) AND public.st_srid(geom) = 4326
         )",
    )
    .bind(projection_load_id)
    .bind(unit_id)
    .fetch_one(&mut **transaction)
    .await?;
    if !valid {
        bail!(
            "invalid or missing administrative boundary geometry for {}",
            row.canonical_code
        );
    }
    Ok(())
}

fn json_type(value: &JsonValue) -> &str {
    value.get("type").and_then(JsonValue::as_str).unwrap_or("")
}

fn json_sha256(value: &JsonValue) -> anyhow::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

fn scope_kind_slug(kind: &str) -> &str {
    match kind {
        "legal_dong" => "legal-dong",
        value => value,
    }
}

fn is_positive_digits(value: &str) -> bool {
    !value.is_empty() && value != "0" && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn bool_env(name: &str) -> anyhow::Result<bool> {
    match env::var(name)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" => Ok(true),
        _ => bail!("{name}=1 is required"),
    }
}
