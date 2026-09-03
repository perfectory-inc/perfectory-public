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
//!
//! **The link is filled here, not by a second pass (ADR-0075).** The handoff carries the
//! register's own building key, `building_id` is derived by the arithmetic that issued the
//! building rows' ids, and the FK's existence demand is an explicit check — a unit whose
//! building is absent loads with a NULL link and is counted as `link_dropped`, because NULL is
//! an answer (ADR-0074 §2) and inventing a building row is not.

use anyhow::{bail, Context};
use catalog_domain::building_id_for_register_pk;
use catalog_domain::building_unit_id_for_register_pk;
use catalog_domain::parcel_id_for_pnu;
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::handoff_manifest_support::{
    validate_manifest, verdict, HandoffContract, Manifest, PassTotals,
};
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

/// The unit manifest's own counters, alongside the shared shape.
#[derive(Debug, Deserialize)]
struct UnitManifestExtras {
    null_pnu_row_count: u64,
    invalid_pnu_row_count: u64,
}

/// One handoff row, deserialised whole so a shape change fails loudly here.
#[derive(Debug, Deserialize)]
struct HandoffUnitRow {
    register_pk: String,
    pnu: String,
    building_register_pk: Option<String>,
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
             building_id uuid,
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
/// Returns `(staged, attached, orphaned, link_dropped)`. Orphans — rows whose parcel is not in
/// the catalog — are left in the stage's truncation, not written anywhere: inventing a parcel
/// row for them would claim a cadastral fact no source stated. `link_dropped` counts merged
/// rows whose building key named a building the catalog does not hold — they load with a NULL
/// link (ADR-0075 §3).
async fn load_object(
    conn: &mut PgConnection,
    rows: &[HandoffUnitRow],
) -> anyhow::Result<(u64, u64, u64, u64)> {
    conn.execute("TRUNCATE TABLE building_unit_projection_stage")
        .await
        .context("failed to truncate the building unit projection stage")?;

    let mut copy = conn
        .copy_in_raw(
            "COPY building_unit_projection_stage \
             (id, parcel_id, building_id, register_pk, building_name, dong_name, ho_name, \
              floor_label, exclusive_area_m2, usage_name, structure_name) \
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
        match row.building_register_pk.as_deref().map(str::trim) {
            Some(key) if !key.is_empty() => {
                buffer.push_str(&building_id_for_register_pk(key).to_string());
            }
            _ => buffer.push_str("\\N"),
        }
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
             (id, parcel_id, building_id, register_pk, building_name, dong_name, ho_name, \
              floor_label, exclusive_area_m2, usage_name, structure_name) \
         SELECT s.id, s.parcel_id, \
                CASE WHEN EXISTS (SELECT 1 FROM catalog.building b WHERE b.id = s.building_id) \
                     THEN s.building_id ELSE NULL END, \
                s.register_pk, s.building_name, s.dong_name, s.ho_name, \
                s.floor_label, s.exclusive_area_m2, s.usage_name, s.structure_name \
         FROM building_unit_projection_stage s \
         WHERE EXISTS (SELECT 1 FROM catalog.parcel p WHERE p.id = s.parcel_id) \
         ON CONFLICT (register_pk) DO UPDATE SET \
             parcel_id = EXCLUDED.parcel_id, \
             building_id = EXCLUDED.building_id, \
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

    let link_dropped: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM building_unit_projection_stage s \
         WHERE EXISTS (SELECT 1 FROM catalog.parcel p WHERE p.id = s.parcel_id) \
           AND s.building_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM catalog.building b WHERE b.id = s.building_id)",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to count merged units whose building the catalog does not hold")?;

    #[allow(clippy::cast_sign_loss)]
    Ok((staged, attached, orphaned as u64, link_dropped as u64))
}

async fn load_one(
    storage: &R2ObjectStorage,
    conn: &mut PgConnection,
    key: &str,
) -> anyhow::Result<(u64, u64, u64, u64)> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read handoff object {key}"))?;
    let rows = units_in_object(&bytes, key)?;
    load_object(conn, &rows).await
}

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let contract = HandoffContract::load(&config.contract_path)?;

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
    let extras: UnitManifestExtras = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("failed to parse the manifest {}", contract.manifest_object))?;
    validate_manifest(&manifest, &contract, MANIFEST_SCHEMA_VERSION)?;

    if !config.confirmed {
        println!(
            "building-unit-projection-load-plan objects={} manifest_rows={} null_pnu={} \
             invalid_pnu={} (set {CONFIRM_ENV}=true to load)",
            manifest.objects.len(),
            manifest.exported_row_count,
            extras.null_pnu_row_count,
            extras.invalid_pnu_row_count,
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
    let mut link_dropped_total = 0_u64;
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
        if let Some((staged, attached, orphaned, link_dropped)) = outcome {
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
            link_dropped_total += link_dropped;
            println!(
                "building-unit-projection-load-object {}/{} key={} staged={staged} \
                 attached={attached} orphaned={orphaned} link_dropped={link_dropped}",
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
    println!("building-unit-projection-load-detail link_dropped={link_dropped_total}");
    verdict(
        "building-unit-projection-load",
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

    fn row_json(register_pk: &str, pnu: &str, building_register_pk: &str) -> String {
        format!(
            "{{\"register_pk\":\"{register_pk}\",\"pnu\":\"{pnu}\",             \"building_register_pk\":{building_register_pk},\"building_name\":\"본관\",             \"dong_name\":\"101동\",\"ho_name\":\"101호\",\"floor_label\":\"1층\",             \"exclusive_area_m2\":84.5,\"usage_name\":\"공장\",\"structure_name\":\"철골\"}}"
        )
    }

    #[test]
    fn a_compressed_object_yields_its_units() {
        let body = format!(
            "{}\n{}\n",
            row_json("PK-1", "9999900000100000001", "\"BLDG-1\""),
            row_json("PK-2", "9999900000100000002", "null")
        );

        let rows = units_in_object(&gzipped(&body), "k").expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ho_name, "101호");
        assert_eq!(rows[0].building_register_pk.as_deref(), Some("BLDG-1"));
        // The register stated no link, and the row says so rather than carrying one (ADR-0075).
        assert_eq!(rows[1].building_register_pk, None);
    }

    #[test]
    fn a_row_without_a_register_pk_stops_the_object() {
        let body = row_json("  ", "9999900000100000001", "null");

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
}
