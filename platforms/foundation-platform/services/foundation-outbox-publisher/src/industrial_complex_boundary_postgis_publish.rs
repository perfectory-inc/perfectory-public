//! Materialises `silver.industrial_complex_boundaries` into the append-only PostGIS serving
//! projection for the `complex` publication unit.
//!
//! The lakehouse stays canonical. This command only projects one Iceberg snapshot of the Silver
//! boundary table into `serving_postgis.industrial_complex_boundary_publication`, keyed on the
//! projection load it opens, and the runtime manifest decides whether anything ever reads it.
//!
//! **The reprojection happens in PostGIS, not here** (root ADR-0042). Silver carries EPSG:5186 in
//! metres because neither this workspace nor the Spark image has a projection library; serving is
//! EPSG:4326. `ST_Transform(ST_SetSRID(ST_GeomFromWKB(...), 5186), 4326)` is the same expression
//! `parcel_marker_anchor_artifact_export` already uses for the same reason. A hand-written inverse
//! Transverse Mercator would be wrong in ways nothing downstream can detect.
//!
//! The input is a JSONL file, as with `publish-administrative-boundary-postgis`. It is produced by
//! `infra/lakehouse/spark/jobs/industrial_complex_boundaries_silver_to_postgis_handoff.py`, which is
//! the only reader that can open this table: `complex_id` is filled by the Silver *writer* from its
//! join against `silver.industrial_complexes`, so the Bronze-to-Silver handoff export leaves it
//! null, and `crate::lakehouse_snapshot_scan` decodes only string/long/date/timestamp/decimal
//! columns — `geometry_wkb` is `binary` and the bbox columns are `double`.
//!
//! Nothing here invents a row. The 98 complexes with no polygon in the source have no Silver
//! boundary and therefore no row in this table.

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    fs,
    io::{BufRead as _, BufReader},
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use lakehouse_application::{BOUNDARY_KIND_OFFICIAL, INDUSTRIAL_COMPLEX_BOUNDARY_SRID};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_CONFIRM";
const SOURCE_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID";
const SOURCE_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY";

const DEFAULT_SOURCE_PATH: &str = "target/source/industrial-complex-boundary-postgis-source.jsonl";

/// The `catalog.vector_tile_publication_unit.unit_key` this command materialises.
///
/// `catalog_domain::vector_tile_feature_filter_properties` already names `complex` as a
/// foundation-owned layer and `official_complex_code` as its one public property, and
/// `20260805000001` lists `complex` among the keys the addressable spelling had to keep admitting.
/// The key is therefore read off what the repository already publishes rather than chosen here.
pub(crate) const COMPLEX_UNIT_KEY: &str = "complex";

/// Runs `publish-industrial-complex-boundary-postgis`.
///
/// # Errors
/// Returns an error when the confirmation or provenance environment is incomplete, when the source
/// file disagrees with the provenance it is published under, when a geometry cannot be reprojected
/// into a valid EPSG:4326 multipolygon, or when the load would commit no rows.
pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let rows = read_rows(&config)?;
    let pool = PgPool::connect(&config.database_url).await.context(
        "failed to connect to DATABASE_URL for industrial complex boundary PostGIS publish",
    )?;

    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut *transaction)
        .await?;

    verify_source_record(&mut transaction, &config).await?;
    let publication_unit_id = ensure_complex_unit(&mut transaction).await?;
    let data_revision =
        register_or_reuse_revision(&mut transaction, publication_unit_id, &config).await?;

    // Opened before a single geometry lands, so the rows this run writes carry the identity of the
    // run that wrote them. Keying the projection on the load rather than on the revision is what
    // makes a re-publish a second, separately serviceable fact instead of an in-place overwrite.
    let projection_load_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, status)
         VALUES ($1, $2, $3, $4, 'running')",
    )
    .bind(projection_load_id)
    .bind(publication_unit_id)
    .bind(data_revision)
    .bind(&config.canonical_snapshot_id)
    .execute(&mut *transaction)
    .await
    .context("failed to open the industrial complex PostGIS projection load")?;

    for row in &rows {
        publish_boundary(&mut transaction, &config, row, projection_load_id).await?;
    }

    // Counted from the table, by load. A count derived from the input would report what this run
    // intended rather than what it committed.
    let publication_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_publication
          WHERE projection_load_id = $1",
    )
    .bind(projection_load_id)
    .fetch_one(&mut *transaction)
    .await?;
    if publication_count == 0 {
        bail!("industrial complex boundary publish produced no geometries");
    }
    // Every insert either lands or aborts the transaction: the table's own CHECK requires
    // `st_isvalid` and EPSG:4326, and the primary key refuses a repeat within one load that
    // `read_rows` did not already reject. So a run that reaches here rejected nothing, and this
    // states that from the two numbers rather than asserting it.
    let rejected_count = i64::try_from(rows.len())
        .context("source row count does not fit in the ledger's counter")?
        - publication_count;
    ensure!(
        rejected_count == 0,
        "the projection committed {publication_count} of {} source boundaries",
        rows.len()
    );

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
    .context("failed to close the industrial complex PostGIS projection load")?;
    transaction.commit().await?;

    println!(
        "industrial-complex-boundary-postgis-publish-ok source_boundaries={} publication_rows={} \
         rejected_rows={} canonical_iceberg_snapshot_id={} data_revision={} projection_load_id={}",
        rows.len(),
        publication_count,
        rejected_count,
        config.canonical_snapshot_id,
        data_revision,
        projection_load_id
    );
    Ok(())
}

struct Config {
    database_url: String,
    source_path: PathBuf,
    canonical_snapshot_id: String,
    source_snapshot_id: String,
    source_record_id: Uuid,
    source_object_key: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if !bool_env(CONFIRM_ENV)? {
            bail!("{CONFIRM_ENV}=1 is required before writing industrial complex boundary PostGIS");
        }
        let canonical_snapshot_id = required_env_value(CANONICAL_SNAPSHOT_ENV)?;
        ensure!(
            is_positive_digits(&canonical_snapshot_id),
            "{CANONICAL_SNAPSHOT_ENV} must be a positive decimal Iceberg snapshot id"
        );
        let source_snapshot_id = required_env_value(SOURCE_SNAPSHOT_ENV)?;
        let source_object_key = required_env_value(SOURCE_OBJECT_KEY_ENV)?;
        ensure!(
            !source_object_key.starts_with('/') && !source_object_key.contains(".."),
            "{SOURCE_OBJECT_KEY_ENV} must be a relative immutable object key"
        );
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            source_path: PathBuf::from(
                optional_env_value(SOURCE_PATH_ENV)?
                    .unwrap_or_else(|| DEFAULT_SOURCE_PATH.to_owned()),
            ),
            canonical_snapshot_id,
            source_snapshot_id,
            source_record_id: Uuid::parse_str(&required_env_value(SOURCE_RECORD_ENV)?)
                .with_context(|| format!("{SOURCE_RECORD_ENV} must be a UUID"))?,
            source_object_key,
        })
    }
}

/// One Silver boundary as the Spark export states it.
#[derive(Debug, Deserialize)]
struct SourceRow {
    complex_id: String,
    official_complex_code: String,
    boundary_kind: String,
    /// Lowercase hex of the standard little-endian WKB Silver stores, in [`INDUSTRIAL_COMPLEX_BOUNDARY_SRID`].
    geometry_wkb_hex: String,
    geometry_srid: i32,
    /// Exact decimal text, because `area_sqm_calculated` is `decimal(18,2)` on both sides and a
    /// double would round it on the way through.
    area_sqm_calculated: String,
    geometry_checksum_sha256: String,
    /// Silver's lineage id for the boundary, which the boundary export sets to the Bronze object key.
    source_record_id: String,
    source_snapshot_id: String,
}

/// Reads and validates the export, refusing anything it cannot publish as stated.
fn read_rows(config: &Config) -> anyhow::Result<Vec<SourceRow>> {
    let path: &Path = config.source_path.as_ref();
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut rows = Vec::new();
    let mut seen_complex_ids = BTreeSet::new();
    let mut seen_codes = BTreeSet::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("failed to read source line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: SourceRow = serde_json::from_str(&line)
            .with_context(|| format!("invalid source JSONL at line {}", index + 1))?;
        validate_row(&row, config, index + 1)?;
        ensure!(
            seen_complex_ids.insert(row.complex_id.clone()),
            "complex_id {} appears twice; one load states one boundary per complex",
            row.complex_id
        );
        ensure!(
            seen_codes.insert(row.official_complex_code.clone()),
            "official_complex_code {} appears twice; one load states one boundary per complex",
            row.official_complex_code
        );
        rows.push(row);
    }
    ensure!(
        !rows.is_empty(),
        "the industrial complex boundary export at {} contains no rows",
        path.display()
    );
    Ok(rows)
}

fn validate_row(row: &SourceRow, config: &Config, line: usize) -> anyhow::Result<()> {
    ensure!(
        row.source_snapshot_id == config.source_snapshot_id,
        "source_snapshot_id mismatch at line {line}: the export was built from {} and is being \
         published as {}",
        row.source_snapshot_id,
        config.source_snapshot_id
    );
    // The export carries Silver's own lineage id, which the boundary Silver export sets to the
    // Bronze object key. Comparing it to the key the operator named is what stops a publish from
    // attributing one snapshot's polygons to another snapshot's `catalog.source_record`.
    ensure!(
        row.source_record_id == config.source_object_key,
        "source_record_id mismatch at line {line}: the export cites {} and {SOURCE_OBJECT_KEY_ENV} \
         names {}",
        row.source_record_id,
        config.source_object_key
    );
    ensure!(
        row.geometry_srid == INDUSTRIAL_COMPLEX_BOUNDARY_SRID,
        "line {line} declares EPSG:{} rather than the EPSG:{INDUSTRIAL_COMPLEX_BOUNDARY_SRID} \
         Silver stores; reprojecting it as if it were would misplace the complex",
        row.geometry_srid
    );
    ensure!(
        row.boundary_kind == BOUNDARY_KIND_OFFICIAL,
        "line {line} is a {} boundary; only {BOUNDARY_KIND_OFFICIAL} boundaries have a producer",
        row.boundary_kind
    );
    ensure!(
        is_uuid_v5(&row.complex_id),
        "line {line} carries complex_id {}, which is not the UUIDv5 the lakehouse derives; a \
         locally minted id joins to nothing in the lakehouse",
        row.complex_id
    );
    ensure!(
        !row.official_complex_code.trim().is_empty(),
        "line {line} has no official_complex_code"
    );
    // The checksum is verified against the bytes rather than carried through. A copied checksum is
    // a claim; a recomputed one is the reason the column can be trusted as evidence the projection
    // was built from the geometry Silver stored.
    let wkb = decode_hex(&row.geometry_wkb_hex)
        .with_context(|| format!("line {line} has a geometry_wkb_hex that is not lowercase hex"))?;
    ensure!(!wkb.is_empty(), "line {line} carries an empty geometry");
    let actual = sha256_hex(&wkb);
    ensure!(
        actual == row.geometry_checksum_sha256,
        "geometry_checksum_sha256 mismatch at line {line}: the export states {} and its own bytes \
         hash to {actual}",
        row.geometry_checksum_sha256
    );
    ensure!(
        is_positive_decimal(&row.area_sqm_calculated),
        "line {line} carries area_sqm_calculated {}, which is not a positive decimal; Silver \
         refuses a boundary that encloses no area, so this row did not come from it",
        row.area_sqm_calculated
    );
    Ok(())
}

/// Confirms the `catalog.source_record` the operator named exists and addresses the same object.
async fn verify_source_record(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
) -> anyhow::Result<()> {
    let raw_object_key: Option<String> =
        sqlx::query_scalar("SELECT raw_object_key FROM catalog.source_record WHERE id = $1")
            .bind(config.source_record_id)
            .fetch_optional(&mut **transaction)
            .await?
            .context("the industrial complex boundary source_record does not exist")?;
    ensure!(
        raw_object_key.as_deref() == Some(config.source_object_key.as_str()),
        "catalog.source_record {} addresses {:?} rather than {}",
        config.source_record_id,
        raw_object_key,
        config.source_object_key
    );
    Ok(())
}

/// Provisions the `complex` publication unit if this deployment has not published one yet.
///
/// Minted here rather than seeded by the migration. `catalog.promote_vector_tile_runtime_manifest`
/// requires every manifest to select every publication unit, so a unit that exists before it has a
/// release would make the next promotion of every other unit fail. `ensure_administrative_unit`
/// mints `admin` in the same place for the same reason: the load names its unit by foreign key, so
/// the unit has to exist by the time the load is opened.
async fn ensure_complex_unit(transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
         VALUES (gen_random_uuid(), $1) ON CONFLICT (unit_key) DO NOTHING",
    )
    .bind(COMPLEX_UNIT_KEY)
    .execute(&mut **transaction)
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = $1",
    )
    .bind(COMPLEX_UNIT_KEY)
    .fetch_one(&mut **transaction)
    .await?)
}

/// Returns the `complex` revision of this canonical snapshot, minting it on first publish.
///
/// The operator names the Iceberg snapshot, not a revision uuid.
/// `publication_revision_unit_snapshot_key` already states that one unit has one revision per
/// canonical snapshot, so the id is derivable from facts the operator can check and a re-publish of
/// the same snapshot reuses it instead of inventing a second name for one version of the data.
/// `derived_from_administrative_revision` stays NULL: an industrial-complex boundary asserts nothing
/// about an administrative boundary, which is the distinction `20260731000002` split the ledgers for.
async fn register_or_reuse_revision(
    transaction: &mut Transaction<'_, Postgres>,
    publication_unit_id: Uuid,
    config: &Config,
) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO catalog.publication_revision
            (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (publication_unit_id, canonical_iceberg_snapshot_id) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(publication_unit_id)
    .bind(&config.canonical_snapshot_id)
    .bind(config.source_record_id)
    .execute(&mut **transaction)
    .await
    .context("failed to register the industrial complex publication revision")?;

    let stored = sqlx::query(
        "SELECT id, source_record_id, derived_from_administrative_revision
           FROM catalog.publication_revision
          WHERE publication_unit_id = $1 AND canonical_iceberg_snapshot_id = $2
          FOR SHARE",
    )
    .bind(publication_unit_id)
    .bind(&config.canonical_snapshot_id)
    .fetch_one(&mut **transaction)
    .await?;
    let stored_id: Uuid = stored.try_get("id")?;
    let stored_source_record: Uuid = stored.try_get("source_record_id")?;
    let stored_lineage: Option<Uuid> = stored.try_get("derived_from_administrative_revision")?;
    ensure!(
        stored_lineage.is_none(),
        "publication revision {stored_id} carries administrative lineage; an industrial complex \
         boundary revision has none"
    );
    ensure!(
        stored_source_record == config.source_record_id,
        "publication revision {stored_id} was registered against source_record {stored_source_record}, \
         not the {} this publish names",
        config.source_record_id
    );
    Ok(stored_id)
}

/// Appends one boundary, reprojected by PostGIS from the source CRS to EPSG:4326.
///
/// `ST_Multi` promotes the single polygons in the source; the shapefile mixes single-ring records
/// with multi-ring ones and the column admits only `MultiPolygon`. `ST_Force2D` drops any Z the WKB
/// carries, because the serving geometry is planar and a 3D geometry would fail the column's type.
///
/// Nothing filters an invalid geometry out. `complex_boundary_publication_geometry_check` requires
/// `st_isvalid`, so a geometry the reprojection breaks aborts the whole transaction and the load
/// leaves no ledger row — which is the honest outcome, rather than a `succeeded` load quietly
/// missing a complex.
async fn publish_boundary(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
    row: &SourceRow,
    projection_load_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO serving_postgis.industrial_complex_boundary_publication
            (complex_id, official_complex_code, projection_load_id, boundary_kind,
             source_snapshot_id, source_record_id, source_object_key, area_sqm_calculated,
             source_geometry_checksum_sha256, geom)
         SELECT $1::uuid, $2, $3, $4, $5, $6, $7, $8::numeric, $9,
                public.st_multi(public.st_force2d(public.st_transform(
                    public.st_setsrid(public.st_geomfromwkb(decode($10, 'hex')), $11::integer),
                    4326)))",
    )
    .bind(&row.complex_id)
    .bind(&row.official_complex_code)
    .bind(projection_load_id)
    .bind(&row.boundary_kind)
    .bind(&row.source_snapshot_id)
    .bind(config.source_record_id)
    .bind(&config.source_object_key)
    .bind(&row.area_sqm_calculated)
    .bind(&row.geometry_checksum_sha256)
    .bind(&row.geometry_wkb_hex)
    .bind(INDUSTRIAL_COMPLEX_BOUNDARY_SRID)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to append the industrial complex boundary for {}",
            row.official_complex_code
        )
    })?;
    Ok(())
}

fn is_positive_digits(value: &str) -> bool {
    !value.is_empty() && value != "0" && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// Accepts the exact decimal text `decimal(18,2)` round-trips through JSON, when it is above zero.
fn is_positive_decimal(value: &str) -> bool {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, "0"));
    let digits_only = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    digits_only(whole)
        && digits_only(fraction)
        && (whole.bytes().any(|b| b != b'0') || fraction.bytes().any(|b| b != b'0'))
}

/// Whether the text is a lowercase canonical UUID whose version nibble is 5.
///
/// The same shape `catalog.industrial_complex_lakehouse_complex_id_is_uuid_v5` refuses in SQL, and
/// stated here so the publish fails on the file rather than 1,343 inserts later on a CHECK.
fn is_uuid_v5(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        14 => *byte == b'5',
        19 => matches!(byte, b'8' | b'9' | b'a' | b'b'),
        _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(byte),
    })
}

fn decode_hex(value: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(value.len().is_multiple_of(2), "hex text has an odd length");
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)?;
            ensure!(
                text.bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "hex text is not lowercase"
            );
            Ok(u8::from_str_radix(text, 16)?)
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}

fn bool_env(name: &str) -> anyhow::Result<bool> {
    match std::env::var(name)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" => Ok(true),
        _ => bail!("{name}=1 is required"),
    }
}

#[cfg(test)]
mod tests;
