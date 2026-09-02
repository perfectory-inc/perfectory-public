//! Loads the exclusive-part units the lakehouse holds into `catalog.building_unit` (ADR-0072).
//!
//! `catalog.building_unit` was a vessel with no producer while
//! `silver.building_register_units` held 19,765,555 rows. Measured 2026-09-03, 98.84% of those
//! rows carry a PNU that `catalog.parcel` holds — so units attach to parcels by PNU, and the
//! rows that cannot attach are counted, never invented (ADR-0072 §3).
//!
//! **The manifest is the input.** The Spark export decides how many handoff objects exist, so
//! unlike the parcel load there is no pre-measured object list: the export writes a manifest
//! last, and this command starts from it. A command that listed the prefix instead would read an
//! empty listing as no work to do.
//!
//! **Identity is derived, not generated.** `building_unit_id_for_register_pk` hashes the
//! register PK, and the merge upserts on `register_pk`, so a re-run updates rows in place and
//! mints nothing (the parcel precedent, one table over).
//!
//! **Orphans are counted per object, at merge time.** A staged row whose derived `parcel_id` is
//! not in `catalog.parcel` is skipped by the merge's join and counted by a second query. The
//! count is reported even when zero: a metric that appears only on failure cannot be told apart
//! from one nobody collected.

use anyhow::{bail, Context};
use catalog_domain::building_unit_id_for_register_pk;
use catalog_domain::parcel_id_for_pnu;
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::handoff_object_support::{copy_text_escape, gunzip_text, object_retry_delay};
use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_UNIT_PROJECTION_LOAD_CONFIRM";
const CONTRACT_PATH_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_UNIT_HANDOFF_CONTRACT_PATH";
const DEFAULT_CONTRACT_PATH: &str = "infra/lakehouse/contracts/building-unit-handoff.json";
const MANIFEST_SCHEMA_VERSION: &str = "foundation-platform.building_unit_handoff_manifest.v1";
const OBJECT_ATTEMPTS: usize = 3;
const OBJECT_RETRY_BASE_DELAY_SECONDS: u64 = 5;

/// The handoff contract: every name the Spark writer and this reader must agree on, once.
#[derive(Debug, Deserialize)]
struct HandoffContract {
    schema_version: u32,
    manifest_object: String,
    columns: Vec<String>,
}

/// What the export wrote, read back as this load's work list.
#[derive(Debug, Deserialize)]
struct Manifest {
    schema_version: String,
    columns: Vec<String>,
    objects: Vec<ManifestObject>,
    exported_row_count: u64,
    null_pnu_row_count: u64,
    invalid_pnu_row_count: u64,
}

#[derive(Debug, Deserialize)]
struct ManifestObject {
    key: String,
    rows: u64,
}

/// One handoff row, deserialised whole so a shape change fails loudly here.
#[derive(Debug, Deserialize)]
struct HandoffUnitRow {
    register_pk: String,
    pnu: String,
    building_name: String,
    dong_name: String,
    ho_name: String,
    floor_label: String,
    exclusive_area_m2: Option<f64>,
    usage_name: String,
    structure_name: String,
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

/// Refuses a manifest this command cannot vouch for, before any object is read.
///
/// The column list is compared against the contract because the writer and the reader both read
/// it: a column added on one side is refused here until both moved — the same property the
/// contract file promises in prose, held as code.
fn validate_manifest(manifest: &Manifest, contract: &HandoffContract) -> anyhow::Result<()> {
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        bail!(
            "manifest schema_version {:?} is not the {MANIFEST_SCHEMA_VERSION:?} this command reads",
            manifest.schema_version
        );
    }
    if manifest.columns != contract.columns {
        bail!(
            "manifest columns {:?} do not match the contract's {:?}; the export and this loader \
             are reading different contracts",
            manifest.columns,
            contract.columns
        );
    }
    if manifest.objects.is_empty() {
        bail!("the manifest names no objects, which is not a state this dataset has");
    }
    let listed: u64 = manifest.objects.iter().map(|object| object.rows).sum();
    if listed != manifest.exported_row_count {
        bail!(
            "the manifest's objects sum to {listed} rows but it claims {} were exported; \
             the export did not finish writing what it counted",
            manifest.exported_row_count
        );
    }
    Ok(())
}

/// Parses one handoff object into rows, refusing an empty or malformed one.
fn units_in_object(object_bytes: &[u8], object_key: &str) -> anyhow::Result<Vec<HandoffUnitRow>> {
    let text = gunzip_text(object_bytes, object_key)?;
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffUnitRow = serde_json::from_str(line).with_context(|| {
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
        "CREATE TEMPORARY TABLE IF NOT EXISTS building_unit_projection_stage (
             id uuid NOT NULL,
             parcel_id uuid NOT NULL,
             register_pk text NOT NULL,
             building_name text NOT NULL,
             dong_name text NOT NULL,
             ho_name text NOT NULL,
             floor_label text NOT NULL,
             exclusive_area_m2 double precision,
             usage_name text NOT NULL,
             structure_name text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create the building unit projection stage")?;
    Ok(())
}

/// Streams one object's rows into the stage and merges the attachable ones.
///
/// Returns `(staged, attached, orphaned)`. Orphans — rows whose parcel is not in the catalog —
/// are left in the stage's truncation, not written anywhere: inventing a parcel row for them
/// would claim a cadastral fact no source stated.
async fn load_object(
    conn: &mut PgConnection,
    rows: &[HandoffUnitRow],
) -> anyhow::Result<(u64, u64, u64)> {
    conn.execute("TRUNCATE TABLE building_unit_projection_stage")
        .await
        .context("failed to truncate the building unit projection stage")?;

    let mut copy = conn
        .copy_in_raw(
            "COPY building_unit_projection_stage \
             (id, parcel_id, register_pk, building_name, dong_name, ho_name, floor_label, \
              exclusive_area_m2, usage_name, structure_name) \
             FROM STDIN WITH (FORMAT text)",
        )
        .await
        .context("failed to start COPY into the building unit projection stage")?;
    let mut buffer = String::with_capacity(1024 * 1024);
    for row in rows {
        let pnu = Pnu::parse(row.pnu.clone())
            .context("a staged PNU stopped being a PNU between reading and writing")?;
        buffer.push_str(&building_unit_id_for_register_pk(row.register_pk.as_str()).to_string());
        buffer.push('\t');
        buffer.push_str(&parcel_id_for_pnu(&pnu).as_uuid().to_string());
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.register_pk.as_str()));
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.building_name.as_str()));
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.dong_name.as_str()));
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.ho_name.as_str()));
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.floor_label.as_str()));
        buffer.push('\t');
        match row.exclusive_area_m2 {
            Some(area) => buffer.push_str(&format!("{area}")),
            None => buffer.push_str("\\N"),
        }
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.usage_name.as_str()));
        buffer.push('\t');
        buffer.push_str(&copy_text_escape(row.structure_name.as_str()));
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
        "INSERT INTO catalog.building_unit \
             (id, parcel_id, register_pk, building_name, dong_name, ho_name, floor_label, \
              exclusive_area_m2, usage_name, structure_name) \
         SELECT s.id, s.parcel_id, s.register_pk, s.building_name, s.dong_name, s.ho_name, \
                s.floor_label, s.exclusive_area_m2, s.usage_name, s.structure_name \
         FROM building_unit_projection_stage s \
         WHERE EXISTS (SELECT 1 FROM catalog.parcel p WHERE p.id = s.parcel_id) \
         ON CONFLICT (register_pk) DO UPDATE SET \
             parcel_id = EXCLUDED.parcel_id, \
             building_name = EXCLUDED.building_name, \
             dong_name = EXCLUDED.dong_name, \
             ho_name = EXCLUDED.ho_name, \
             floor_label = EXCLUDED.floor_label, \
             exclusive_area_m2 = EXCLUDED.exclusive_area_m2, \
             usage_name = EXCLUDED.usage_name, \
             structure_name = EXCLUDED.structure_name, \
             updated_at = now()",
    )
    .execute(&mut *conn)
    .await
    .context("failed to merge the stage into catalog.building_unit")?
    .rows_affected();

    let orphaned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM building_unit_projection_stage s \
         WHERE NOT EXISTS (SELECT 1 FROM catalog.parcel p WHERE p.id = s.parcel_id)",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to count orphaned units in the stage")?;

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
    let rows = units_in_object(&bytes, key)?;
    load_object(conn, &rows).await
}

/// Decides what a finished pass amounts to, and refuses to call a partial one complete.
#[allow(clippy::too_many_arguments)]
fn verdict(
    object_count: usize,
    unread: &[String],
    staged: u64,
    attached: u64,
    orphaned: u64,
    manifest_rows: u64,
    table_rows: i64,
) -> anyhow::Result<()> {
    let label = if unread.is_empty() {
        "building-unit-projection-load-ok"
    } else {
        "building-unit-projection-load-incomplete"
    };
    println!(
        "{label} objects={object_count} unread={} staged={staged} attached={attached} \
         orphaned={orphaned} manifest_rows={manifest_rows} table_rows={table_rows}",
        unread.len()
    );
    for key in unread {
        println!("building-unit-projection-load-incomplete-key {key}");
    }
    if !unread.is_empty() {
        bail!(
            "{} of {object_count} objects could not be read; the table holds {table_rows} rows \
             and is not the whole dataset. Re-running loads only what is missing.",
            unread.len()
        );
    }
    if staged != manifest_rows {
        bail!(
            "the pass staged {staged} rows but the manifest promised {manifest_rows}; an object \
             changed between export and load"
        );
    }
    if attached + orphaned != staged {
        bail!(
            "attached {attached} + orphaned {orphaned} does not account for the {staged} staged \
             rows; the merge lost track of some"
        );
    }
    Ok(())
}

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let contract: HandoffContract = serde_json::from_str(
        &std::fs::read_to_string(&config.contract_path).with_context(|| {
            format!(
                "failed to read the handoff contract {}",
                config.contract_path
            )
        })?,
    )
    .with_context(|| {
        format!(
            "failed to parse the handoff contract {}",
            config.contract_path
        )
    })?;
    if contract.schema_version != 1 {
        bail!(
            "handoff contract schema_version {} is not the 1 this command reads",
            contract.schema_version
        );
    }

    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for the building unit projection load")?;
    let manifest_bytes = storage
        .get_object_bytes_range_retried(contract.manifest_object.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read the handoff manifest {}; without it this command cannot know \
                 what the export wrote, and listing the prefix instead would read an empty \
                 listing as no work",
                contract.manifest_object
            )
        })?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("failed to parse the manifest {}", contract.manifest_object))?;
    validate_manifest(&manifest, &contract)?;

    if !config.confirmed {
        println!(
            "building-unit-projection-load-plan objects={} manifest_rows={} null_pnu={} \
             invalid_pnu={} (set {CONFIRM_ENV}=true to load)",
            manifest.objects.len(),
            manifest.exported_row_count,
            manifest.null_pnu_row_count,
            manifest.invalid_pnu_row_count,
        );
        return Ok(());
    }

    let mut conn = PgConnection::connect(config.database_url.as_str())
        .await
        .context("failed to connect to DATABASE_URL for the building unit projection load")?;
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
                        "building-unit-projection-load-retry key={} attempt={attempt}/{OBJECT_ATTEMPTS} error={error:#}",
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
                        "building-unit-projection-load-unread key={} attempts={OBJECT_ATTEMPTS} error={error:#}",
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
                "building-unit-projection-load-object {}/{} key={} staged={staged} attached={attached} orphaned={orphaned}",
                index + 1,
                manifest.objects.len(),
                object.key
            );
        }
    }

    conn.execute("ANALYZE catalog.building_unit")
        .await
        .context("failed to analyze catalog.building_unit after loading")?;

    let table_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.building_unit")
        .fetch_one(&mut conn)
        .await
        .context("failed to count catalog.building_unit after loading")?;
    verdict(
        manifest.objects.len(),
        &unread,
        staged_total,
        attached_total,
        orphaned_total,
        manifest.exported_row_count,
        table_rows,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn contract() -> HandoffContract {
        HandoffContract {
            schema_version: 1,
            manifest_object: "silver-handoff/building_register_units/manifest.json".to_owned(),
            columns: vec!["register_pk".to_owned(), "pnu".to_owned()],
        }
    }

    fn manifest() -> Manifest {
        Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION.to_owned(),
            columns: vec!["register_pk".to_owned(), "pnu".to_owned()],
            objects: vec![
                ManifestObject {
                    key: "silver-handoff/building_register_units/99999.jsonl.gz".to_owned(),
                    rows: 3,
                },
                ManifestObject {
                    key: "silver-handoff/building_register_units/99998.jsonl.gz".to_owned(),
                    rows: 2,
                },
            ],
            exported_row_count: 5,
            null_pnu_row_count: 1,
            invalid_pnu_row_count: 0,
        }
    }

    fn gzipped(body: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(body.as_bytes()).expect("fixture write");
        encoder.finish().expect("fixture finish")
    }

    fn row_json(register_pk: &str, pnu: &str) -> String {
        format!(
            "{{\"register_pk\":\"{register_pk}\",\"pnu\":\"{pnu}\",\"building_name\":\"본관\",\
             \"dong_name\":\"101동\",\"ho_name\":\"101호\",\"floor_label\":\"1층\",\
             \"exclusive_area_m2\":84.5,\"usage_name\":\"공장\",\"structure_name\":\"철골\"}}"
        )
    }

    #[test]
    fn a_manifest_that_adds_up_passes() {
        validate_manifest(&manifest(), &contract()).expect("a consistent manifest");
    }

    #[test]
    fn a_manifest_whose_objects_do_not_sum_to_its_total_is_refused() {
        // The manifest is written last, so a sum that disagrees with the total means the export
        // died between writing objects and counting them — the load must not guess which is true.
        let mut broken = manifest();
        broken.exported_row_count = 99;

        let error = validate_manifest(&broken, &contract())
            .expect_err("a manifest that does not add up must be refused");

        assert!(format!("{error:#}").contains("did not finish"));
    }

    #[test]
    fn a_manifest_with_different_columns_is_refused() {
        // The writer and the reader both read the contract's column list; a manifest carrying a
        // different one means they read different contracts, and loading it would put values in
        // the wrong columns silently.
        let mut drifted = manifest();
        drifted.columns = vec!["register_pk".to_owned()];

        let error =
            validate_manifest(&drifted, &contract()).expect_err("a column drift must be refused");

        assert!(format!("{error:#}").contains("different contracts"));
    }

    #[test]
    fn a_manifest_from_a_newer_export_is_refused() {
        let mut newer = manifest();
        newer.schema_version = "foundation-platform.building_unit_handoff_manifest.v2".to_owned();

        let error = validate_manifest(&newer, &contract())
            .expect_err("a newer manifest must not be guessed at");

        assert!(format!("{error:#}").contains("schema_version"));
    }

    #[test]
    fn an_empty_manifest_is_refused() {
        let mut empty = manifest();
        empty.objects.clear();
        empty.exported_row_count = 0;

        let error = validate_manifest(&empty, &contract())
            .expect_err("no objects is not a state this dataset has");

        assert!(format!("{error:#}").contains("no objects"));
    }

    #[test]
    fn a_compressed_object_yields_its_units() {
        let body = format!(
            "{}\n{}\n",
            row_json("PK-1", "9999900000100000001"),
            row_json("PK-2", "9999900000100000002")
        );

        let rows = units_in_object(&gzipped(&body), "k").expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ho_name, "101호");
    }

    #[test]
    fn a_row_without_a_register_pk_stops_the_object() {
        let body = row_json("  ", "9999900000100000001");

        let error = units_in_object(&gzipped(&body), "k")
            .expect_err("the natural key is what the merge conflicts on");

        assert!(format!("{error:#}").contains("register_pk"));
    }

    #[test]
    fn plain_text_where_gzip_was_promised_is_an_error() {
        let error = units_in_object(b"{}\n", "k").expect_err("plain bytes must be refused");

        assert!(format!("{error:#}").contains("decompress"));
    }

    #[test]
    fn an_empty_object_is_an_error_not_a_success() {
        let error =
            units_in_object(&gzipped(""), "k").expect_err("an empty object must not be a pass");

        assert!(format!("{error:#}").contains("no rows"));
    }

    #[test]
    fn a_pass_with_nothing_unread_and_matching_counts_is_complete() {
        verdict(2, &[], 5, 4, 1, 5, 4).expect("a complete pass");
    }

    #[test]
    fn one_unread_object_makes_the_pass_fail() {
        let unread = vec!["silver-handoff/building_register_units/99999.jsonl.gz".to_owned()];

        let error = verdict(2, &unread, 3, 3, 0, 5, 3)
            .expect_err("a pass that could not read an object is not a finished pass");

        assert!(format!("{error:#}").contains("1 of 2"));
    }

    #[test]
    fn a_pass_that_staged_fewer_rows_than_the_manifest_promised_fails() {
        // Every object read cleanly and still the totals disagree: an object changed between
        // export and load. Calling that complete would report a shorter table as the dataset.
        let error = verdict(2, &[], 4, 4, 0, 5, 4)
            .expect_err("a shortfall against the manifest is not a finished pass");

        assert!(format!("{error:#}").contains("manifest promised"));
    }

    #[test]
    fn rows_the_merge_lost_track_of_fail_the_pass() {
        let error = verdict(2, &[], 5, 3, 1, 5, 3)
            .expect_err("attached plus orphaned must account for every staged row");

        assert!(format!("{error:#}").contains("lost track"));
    }
}
