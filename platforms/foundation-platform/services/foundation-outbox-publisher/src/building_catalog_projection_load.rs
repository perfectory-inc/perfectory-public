//! Loads the title-register buildings into `catalog.building` (ADR-0073 step 7).
//!
//! The manifest pipeline of the unit load, one table over: the export writes per-sigungu gzip
//! JSONL objects plus a manifest, this command starts from the manifest, stages each object
//! through `COPY`, and merges with `ON CONFLICT (register_pk) DO UPDATE`. Identity is derived
//! (`building_id_for_register_pk`), attachment is PNU arithmetic, and a building whose parcel is
//! not in the catalog is skipped and counted — never invented.
//!
//! Facts the register did not state arrive as JSON nulls and load as SQL nulls (migration
//! 20260903000003): 18.7% of the national snapshot has no approval year, and a fabricated year
//! would be a claim nobody made. `below_ground_floors` is the one exception — the schema keeps
//! its `NOT NULL DEFAULT 0`, so an unstated basement count loads as the schema's own zero.

use anyhow::{bail, Context};
use catalog_domain::{building_id_for_register_pk, parcel_id_for_pnu};
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::handoff_manifest_support::{
    validate_manifest, verdict, HandoffContract, Manifest, PassTotals,
};
use crate::handoff_object_support::{gunzip_text, object_retry_delay};
use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_PROJECTION_LOAD_CONFIRM";
const CONTRACT_PATH_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_TITLE_CATALOG_HANDOFF_CONTRACT_PATH";
const DEFAULT_CONTRACT_PATH: &str = "infra/lakehouse/contracts/building-title-catalog-handoff.json";
const MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.building_title_catalog_handoff_manifest.v1";
const LABEL_PREFIX: &str = "building-projection-load";
const OBJECT_ATTEMPTS: usize = 3;
const OBJECT_RETRY_BASE_DELAY_SECONDS: u64 = 5;

/// One handoff row, deserialised whole so a shape change fails loudly here.
#[derive(Debug, Deserialize)]
struct HandoffBuildingRow {
    register_pk: String,
    pnu: String,
    purpose_code: Option<String>,
    structure_code: Option<String>,
    floor_area_m2: Option<f64>,
    stories: Option<i16>,
    below_ground_floors: Option<i16>,
    built_year: Option<i32>,
}

struct Config {
    database_url: String,
    contract_path: String,
    confirmed: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            contract_path: optional_env_value(CONTRACT_PATH_ENV)?
                .unwrap_or_else(|| DEFAULT_CONTRACT_PATH.to_owned()),
            confirmed: optional_bool_env(CONFIRM_ENV)?.unwrap_or(false),
        })
    }
}

/// Parses one handoff object into rows, refusing an empty or malformed one.
fn buildings_in_object(
    object_bytes: &[u8],
    object_key: &str,
) -> anyhow::Result<Vec<HandoffBuildingRow>> {
    let text = gunzip_text(object_bytes, object_key)?;
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffBuildingRow = serde_json::from_str(line).with_context(|| {
            format!(
                "handoff object {object_key} line {} is not a row",
                index + 1
            )
        })?;
        Pnu::parse(row.pnu.clone()).with_context(|| {
            format!(
                "handoff object {object_key} line {} carries a PNU this catalog cannot hold",
                index + 1
            )
        })?;
        if row.register_pk.trim().is_empty() {
            bail!(
                "handoff object {object_key} line {} has no register_pk, and the natural key is \
                 what the merge conflicts on",
                index + 1
            );
        }
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("handoff object {object_key} carried no rows, which is not a state this dataset has");
    }
    Ok(rows)
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS building_projection_stage (
             id uuid NOT NULL,
             parcel_id uuid NOT NULL,
             register_pk text NOT NULL,
             purpose_code text,
             structure_code text,
             floor_area_m2 double precision,
             stories smallint,
             below_ground_floors smallint,
             built_year integer
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create the building projection stage")?;
    Ok(())
}

fn push_optional_text(buffer: &mut String, value: Option<&str>) {
    match value {
        // Code columns carry provider codes (digits and ASCII); the general escaping lives in
        // the shared module and free-text columns are not part of this table's handoff.
        Some(text) => buffer.push_str(&crate::handoff_object_support::copy_text_escape(text)),
        None => buffer.push_str("\\N"),
    }
}

fn push_optional_display<T: std::fmt::Display>(buffer: &mut String, value: Option<T>) {
    match value {
        Some(inner) => {
            use std::fmt::Write as _;
            let _ = write!(buffer, "{inner}");
        }
        None => buffer.push_str("\\N"),
    }
}

/// Streams one object's rows into the stage and merges the attachable ones.
async fn load_object(
    conn: &mut PgConnection,
    rows: &[HandoffBuildingRow],
) -> anyhow::Result<(u64, u64, u64)> {
    conn.execute("TRUNCATE TABLE building_projection_stage")
        .await
        .context("failed to truncate the building projection stage")?;

    let mut copy = conn
        .copy_in_raw(
            "COPY building_projection_stage \
             (id, parcel_id, register_pk, purpose_code, structure_code, floor_area_m2, \
              stories, below_ground_floors, built_year) \
             FROM STDIN WITH (FORMAT text)",
        )
        .await
        .context("failed to start COPY into the building projection stage")?;
    let mut buffer = String::with_capacity(1024 * 1024);
    for row in rows {
        let pnu = Pnu::parse(row.pnu.clone())
            .context("a staged PNU stopped being a PNU between reading and writing")?;
        buffer.push_str(&building_id_for_register_pk(row.register_pk.as_str()).to_string());
        buffer.push('\t');
        buffer.push_str(&parcel_id_for_pnu(&pnu).as_uuid().to_string());
        buffer.push('\t');
        buffer.push_str(&crate::handoff_object_support::copy_text_escape(
            row.register_pk.as_str(),
        ));
        buffer.push('\t');
        push_optional_text(&mut buffer, row.purpose_code.as_deref());
        buffer.push('\t');
        push_optional_text(&mut buffer, row.structure_code.as_deref());
        buffer.push('\t');
        push_optional_display(&mut buffer, row.floor_area_m2);
        buffer.push('\t');
        push_optional_display(&mut buffer, row.stories);
        buffer.push('\t');
        push_optional_display(&mut buffer, row.below_ground_floors);
        buffer.push('\t');
        push_optional_display(&mut buffer, row.built_year);
        buffer.push('\n');
        if buffer.len() >= 8 * 1024 * 1024 {
            copy.send(buffer.as_bytes())
                .await
                .context("COPY send failed")?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        copy.send(buffer.as_bytes())
            .await
            .context("COPY send failed")?;
    }
    let staged = copy.finish().await.context("COPY finish failed")?;
    if staged != rows.len() as u64 {
        bail!(
            "COPY reported {staged} rows but the object carried {}",
            rows.len()
        );
    }

    let attached = sqlx::query(
        "INSERT INTO catalog.building \
             (id, parcel_id, register_pk, purpose_code, structure_code, floor_area_m2, \
              stories, below_ground_floors, built_year) \
         SELECT s.id, s.parcel_id, s.register_pk, s.purpose_code, s.structure_code, \
                s.floor_area_m2, s.stories, COALESCE(s.below_ground_floors, 0), s.built_year \
         FROM building_projection_stage s \
         WHERE EXISTS (SELECT 1 FROM catalog.parcel p WHERE p.id = s.parcel_id) \
         ON CONFLICT (register_pk) DO UPDATE SET \
             parcel_id = EXCLUDED.parcel_id, \
             purpose_code = EXCLUDED.purpose_code, \
             structure_code = EXCLUDED.structure_code, \
             floor_area_m2 = EXCLUDED.floor_area_m2, \
             stories = EXCLUDED.stories, \
             below_ground_floors = EXCLUDED.below_ground_floors, \
             built_year = EXCLUDED.built_year, \
             updated_at = now()",
    )
    .execute(&mut *conn)
    .await
    .context("failed to merge the stage into catalog.building")?
    .rows_affected();

    let orphaned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM building_projection_stage s \
         WHERE NOT EXISTS (SELECT 1 FROM catalog.parcel p WHERE p.id = s.parcel_id)",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to count orphaned buildings in the stage")?;

    #[allow(clippy::cast_sign_loss)]
    Ok((staged, attached, orphaned as u64))
}

async fn load_one(
    storage: &R2ObjectStorage,
    conn: &mut PgConnection,
    key: &str,
) -> anyhow::Result<(u64, u64, u64)> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read handoff object {key}"))?;
    let rows = buildings_in_object(&bytes, key)?;
    load_object(conn, &rows).await
}

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let contract = HandoffContract::load(&config.contract_path)?;

    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for the building projection load")?;
    let manifest_bytes = storage
        .get_object_bytes_range_retried(contract.manifest_object.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read the handoff manifest {}; without it this command cannot know \
                 what the export wrote",
                contract.manifest_object
            )
        })?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("failed to parse the manifest {}", contract.manifest_object))?;
    validate_manifest(&manifest, &contract, MANIFEST_SCHEMA_VERSION)?;

    if !config.confirmed {
        println!(
            "{LABEL_PREFIX}-plan objects={} manifest_rows={} (set {CONFIRM_ENV}=true to load)",
            manifest.objects.len(),
            manifest.exported_row_count,
        );
        return Ok(());
    }

    let mut conn = PgConnection::connect(config.database_url.as_str())
        .await
        .context("failed to connect to DATABASE_URL for the building projection load")?;
    prepare_stage(&mut conn).await?;

    let mut staged_total = 0_u64;
    let mut attached_total = 0_u64;
    let mut orphaned_total = 0_u64;
    let mut unread: Vec<String> = Vec::new();
    for (index, object) in manifest.objects.iter().enumerate() {
        let mut outcome = None;
        for attempt in 1..=OBJECT_ATTEMPTS {
            match load_one(&storage, &mut conn, object.key.as_str()).await {
                Ok(counts) => {
                    outcome = Some(counts);
                    break;
                }
                Err(error) if attempt < OBJECT_ATTEMPTS => {
                    println!(
                        "{LABEL_PREFIX}-retry key={} attempt={attempt}/{OBJECT_ATTEMPTS} error={error:#}",
                        object.key
                    );
                    tokio::time::sleep(object_retry_delay(
                        OBJECT_RETRY_BASE_DELAY_SECONDS,
                        attempt,
                    ))
                    .await;
                }
                Err(error) => {
                    println!(
                        "{LABEL_PREFIX}-unread key={} attempts={OBJECT_ATTEMPTS} error={error:#}",
                        object.key
                    );
                    unread.push(object.key.clone());
                }
            }
        }
        if let Some((staged, attached, orphaned)) = outcome {
            if staged != object.rows {
                bail!(
                    "object {} carried {staged} rows but the manifest promised {}; the export \
                     and its manifest disagree",
                    object.key,
                    object.rows
                );
            }
            staged_total += staged;
            attached_total += attached;
            orphaned_total += orphaned;
            println!(
                "{LABEL_PREFIX}-object {}/{} key={} staged={staged} attached={attached} orphaned={orphaned}",
                index + 1,
                manifest.objects.len(),
                object.key
            );
        }
    }

    conn.execute("ANALYZE catalog.building")
        .await
        .context("failed to analyze catalog.building after loading")?;

    let table_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.building")
        .fetch_one(&mut conn)
        .await
        .context("failed to count catalog.building after loading")?;
    verdict(
        LABEL_PREFIX,
        &unread,
        &PassTotals {
            object_count: manifest.objects.len(),
            staged: staged_total,
            attached: attached_total,
            orphaned: orphaned_total,
            manifest_rows: manifest.exported_row_count,
            table_rows,
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gzipped(body: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(body.as_bytes()).expect("fixture write");
        encoder.finish().expect("fixture finish")
    }

    fn row_json(register_pk: &str, pnu: &str, built_year: &str) -> String {
        format!(
            "{{\"register_pk\":\"{register_pk}\",\"pnu\":\"{pnu}\",\"purpose_code\":\"03000\",\
             \"structure_code\":\"11\",\"floor_area_m2\":163.4,\"stories\":2,\
             \"below_ground_floors\":0,\"built_year\":{built_year}}}"
        )
    }

    #[test]
    fn a_compressed_object_yields_its_buildings() {
        let body = format!(
            "{}\n{}\n",
            row_json("PK-1", "9999900000100000001", "1971"),
            row_json("PK-2", "9999900000100000002", "null")
        );

        let rows = buildings_in_object(&gzipped(&body), "k").expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].built_year, Some(1971));
        // The register stated no year, and the row says so rather than carrying one.
        assert_eq!(rows[1].built_year, None);
    }

    #[test]
    fn a_row_without_a_register_pk_stops_the_object() {
        let body = row_json("  ", "9999900000100000001", "1971");

        let error = buildings_in_object(&gzipped(&body), "k")
            .expect_err("the natural key is what the merge conflicts on");

        assert!(format!("{error:#}").contains("register_pk"));
    }

    #[test]
    fn plain_text_where_gzip_was_promised_is_an_error() {
        let error = buildings_in_object(b"{}\n", "k").expect_err("plain bytes must be refused");

        assert!(format!("{error:#}").contains("decompress"));
    }

    #[test]
    fn an_empty_object_is_an_error_not_a_success() {
        let error =
            buildings_in_object(&gzipped(""), "k").expect_err("an empty object must not be a pass");

        assert!(format!("{error:#}").contains("no rows"));
    }

    #[test]
    fn an_absent_fact_becomes_a_copy_null_not_a_value() {
        let mut buffer = String::new();
        push_optional_display::<i32>(&mut buffer, None);
        buffer.push('\t');
        push_optional_display(&mut buffer, Some(1971));
        buffer.push('\t');
        push_optional_text(&mut buffer, None);

        assert_eq!(buffer, "\\N\t1971\t\\N");
    }
}
