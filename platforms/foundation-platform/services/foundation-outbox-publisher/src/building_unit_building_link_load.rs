//! Backfills `catalog.building_unit.building_id` from the register's own link (ADR-0074 step 5).
//!
//! The third manifest pipeline, one join over: the export writes per-sigungu gzip JSONL pairs
//! plus a manifest, this command starts from the manifest, stages each object through `COPY`,
//! and fills with `UPDATE ... FROM`. The building id is derived, not looked up
//! (`building_id_for_register_pk` — the same arithmetic that issued the building rows' ids), and
//! the FK's existence demand is an explicit `EXISTS` so a pair whose building is a parcel orphan
//! is counted, never invented.
//!
//! NULL is an answer (ADR-0074 §2): a unit this pass does not reach keeps its NULL, and the
//! verdict's equation `updated + unit_missing = staged` refuses a pass that
//! lost track of a pair.

use anyhow::{bail, Context};
use catalog_domain::building_id_for_register_pk;
use foundation_outbox::R2ObjectStorage;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::building_link_evidence::{ApprovedBuildingLinks, BuildingLinkEvidence};
use crate::handoff_manifest_support::{
    validate_manifest, verdict, HandoffContract, Manifest, PassTotals,
};
use crate::handoff_object_support::{gunzip_text, object_retry_delay};
use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_UNIT_LINK_LOAD_CONFIRM";
const CONTRACT_PATH_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_UNIT_LINK_HANDOFF_CONTRACT_PATH";
const DEFAULT_CONTRACT_PATH: &str =
    "infra/lakehouse/contracts/building-unit-building-link-handoff.json";
const MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.building_unit_building_link_handoff_manifest.v2";
const LABEL_PREFIX: &str = "building-unit-link-load";
const OBJECT_ATTEMPTS: usize = 3;
const OBJECT_RETRY_BASE_DELAY_SECONDS: u64 = 5;

/// One handoff pair, deserialised whole so a shape change fails loudly here.
#[derive(Debug, Deserialize)]
struct HandoffLinkRow {
    register_pk: String,
    #[serde(deserialize_with = "crate::building_link_evidence::required_nullable_key")]
    building_register_pk: Option<String>,
    building_link_evidence: BuildingLinkEvidence,
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

/// Parses one handoff object into pairs, refusing an empty or malformed one.
fn pairs_in_object(
    object_bytes: &[u8],
    object_key: &str,
    approvals: &ApprovedBuildingLinks,
) -> anyhow::Result<Vec<HandoffLinkRow>> {
    let text = gunzip_text(object_bytes, object_key)?;
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffLinkRow = serde_json::from_str(line).with_context(|| {
            format!(
                "handoff object {object_key} line {} is not a pair",
                index + 1
            )
        })?;
        if row.register_pk.is_empty() || row.register_pk.trim() != row.register_pk {
            bail!(
                "handoff object {object_key} line {} has no register_pk, and that key is what \
                 the backfill joins on",
                index + 1
            );
        }
        approvals.validate(
            &row.building_link_evidence,
            &row.register_pk,
            row.building_register_pk.as_deref(),
        )?;
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("handoff object {object_key} carried no rows, which is not a state this dataset has");
    }
    Ok(rows)
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS building_unit_link_stage (
             register_pk text NOT NULL,
             building_id uuid
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create the building-unit link stage")?;
    Ok(())
}

/// Streams one object's pairs into the stage and fills the reachable units.
///
/// Returns `(staged, updated, unit_missing, building_missing)` — every staged pair lands in
/// either updated or unit-missing. Missing parents are a subset of updated rows; their old links are cleared.
async fn load_object(
    conn: &mut PgConnection,
    rows: &[HandoffLinkRow],
) -> anyhow::Result<(u64, u64, u64, u64)> {
    conn.execute("TRUNCATE TABLE building_unit_link_stage")
        .await
        .context("failed to truncate the building-unit link stage")?;

    let mut copy = conn
        .copy_in_raw(
            "COPY building_unit_link_stage (register_pk, building_id) \
             FROM STDIN WITH (FORMAT text)",
        )
        .await
        .context("failed to start COPY into the building-unit link stage")?;
    let mut buffer = String::with_capacity(1024 * 1024);
    for row in rows {
        buffer.push_str(&crate::handoff_object_support::copy_text_escape(
            row.register_pk.as_str(),
        ));
        buffer.push('\t');
        match row.building_register_pk.as_deref() {
            Some(pk) => buffer.push_str(&building_id_for_register_pk(pk).to_string()),
            None => buffer.push_str("\\N"),
        }
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

    let updated = sqlx::query(
        "UPDATE catalog.building_unit u \
         SET building_id = CASE \
             WHEN EXISTS (SELECT 1 FROM catalog.building b WHERE b.id = s.building_id) \
             THEN s.building_id ELSE NULL END, updated_at = now() \
         FROM building_unit_link_stage s \
         WHERE u.register_pk = s.register_pk",
    )
    .execute(&mut *conn)
    .await
    .context("failed to fill building_id from the stage")?
    .rows_affected();

    let unit_missing: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM building_unit_link_stage s \
         WHERE NOT EXISTS (SELECT 1 FROM catalog.building_unit u WHERE u.register_pk = s.register_pk)",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to count pairs whose unit is not in the catalog")?;

    let building_missing: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM building_unit_link_stage s \
         WHERE EXISTS (SELECT 1 FROM catalog.building_unit u WHERE u.register_pk = s.register_pk) \
           AND s.building_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM catalog.building b WHERE b.id = s.building_id)",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to count pairs whose building is not in the catalog")?;

    #[allow(clippy::cast_sign_loss)]
    Ok((
        staged,
        updated,
        unit_missing as u64,
        building_missing as u64,
    ))
}

async fn load_one(
    storage: &R2ObjectStorage,
    conn: &mut PgConnection,
    key: &str,
    approvals: &ApprovedBuildingLinks,
) -> anyhow::Result<(u64, u64, u64, u64)> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read handoff object {key}"))?;
    let rows = pairs_in_object(&bytes, key, approvals)?;
    load_object(conn, &rows).await
}

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let contract = HandoffContract::load(&config.contract_path)?;

    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for the building-unit link load")?;
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

    let approvals = ApprovedBuildingLinks::load(&config.database_url).await?;
    let mut conn = PgConnection::connect(config.database_url.as_str())
        .await
        .context("failed to connect to DATABASE_URL for the building-unit link load")?;
    prepare_stage(&mut conn).await?;

    let mut staged_total = 0_u64;
    let mut updated_total = 0_u64;
    let mut unit_missing_total = 0_u64;
    let mut building_missing_total = 0_u64;
    let mut unread: Vec<String> = Vec::new();
    for (index, object) in manifest.objects.iter().enumerate() {
        let mut outcome = None;
        for attempt in 1..=OBJECT_ATTEMPTS {
            match load_one(&storage, &mut conn, object.key.as_str(), &approvals).await {
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
        if let Some((staged, updated, unit_missing, building_missing)) = outcome {
            if staged != object.rows {
                bail!(
                    "object {} carried {staged} rows but the manifest promised {}; the export \
                     and its manifest disagree",
                    object.key,
                    object.rows
                );
            }
            staged_total += staged;
            updated_total += updated;
            unit_missing_total += unit_missing;
            building_missing_total += building_missing;
            println!(
                "{LABEL_PREFIX}-object {}/{} key={} staged={staged} updated={updated} \
                 unit_missing={unit_missing} building_missing={building_missing}",
                index + 1,
                manifest.objects.len(),
                object.key
            );
        }
    }

    conn.execute("ANALYZE catalog.building_unit")
        .await
        .context("failed to analyze catalog.building_unit after the backfill")?;

    let table_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.building_unit WHERE building_id IS NOT NULL",
    )
    .fetch_one(&mut conn)
    .await
    .context("failed to count linked units after the backfill")?;
    println!(
        "{LABEL_PREFIX}-detail unit_missing={unit_missing_total} \
         building_missing={building_missing_total}"
    );
    verdict(
        LABEL_PREFIX,
        &unread,
        &PassTotals {
            object_count: manifest.objects.len(),
            staged: staged_total,
            attached: updated_total,
            orphaned: unit_missing_total,
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

    fn pair_json(register_pk: &str, building_register_pk: &str) -> String {
        serde_json::json!({
            "register_pk": register_pk,
            "building_register_pk": building_register_pk,
            "building_link_evidence": crate::building_link_evidence::source_fixture(),
        })
        .to_string()
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL; cargo xtask integration foundation postgres"]
    async fn postgres_link_load_withdraws_missing_parents_and_reassigns_without_losing_units(
    ) -> foundation_disposable_database::TestResult {
        foundation_disposable_database::run_in_disposable_database("unit_parent_links", |pool| async move {
            let mut conn = pool.acquire().await?;
            // The production SQL uses qualified catalog names: an independent disposable
            // database, not search_path, isolates these deliberately small physical fixtures.
            conn.execute("CREATE SCHEMA catalog").await?;
            conn.execute("CREATE TABLE catalog.building (id uuid PRIMARY KEY)").await?;
            conn.execute("CREATE TABLE catalog.building_unit (
                register_pk text PRIMARY KEY,
                building_id uuid REFERENCES catalog.building(id),
                preserved_value text NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )").await?;
            let old_parent = building_id_for_register_pk("OLD-PARENT");
            let new_parent = building_id_for_register_pk("NEW-PARENT");
            sqlx::query("INSERT INTO catalog.building (id) VALUES ($1), ($2)")
                .bind(old_parent).bind(new_parent).execute(&mut *conn).await?;
            for key in ["WITHDRAW", "MISSING-PARENT", "REASSIGN", "UNTOUCHED"] {
                sqlx::query("INSERT INTO catalog.building_unit (register_pk, building_id, preserved_value)
                    VALUES ($1, $2, $1)")
                    .bind(key).bind(old_parent).execute(&mut *conn).await?;
            }
            let withdrawal = serde_json::json!({
                "register_pk": "WITHDRAW", "building_register_pk": null,
                "building_link_evidence": {"unit_row_id":"approved-withdrawal-row",
                    "building_link_method":"unresolved",
                    "normalization_application_id":"11111111-1111-4111-8111-111111111111"}
            });
            let approvals = ApprovedBuildingLinks::fixture(
                "11111111-1111-4111-8111-111111111111", "approved-withdrawal-row", "WITHDRAW", None,
            );
            let body = format!("{withdrawal}\n{}\n{}\n{}\n",
                pair_json("MISSING-PARENT", "ABSENT-PARENT"),
                pair_json("REASSIGN", "NEW-PARENT"),
                pair_json("ABSENT-UNIT", "NEW-PARENT"));
            let rows = pairs_in_object(&gzipped(&body), "synthetic-links.jsonl.gz", &approvals)?;
            prepare_stage(&mut conn).await?;
            for _ in 0..2 {
                assert_eq!(load_object(&mut conn, &rows).await?, (4, 3, 1, 1));
                let actual: Vec<(String, Option<uuid::Uuid>, String)> = sqlx::query_as(
                    "SELECT register_pk, building_id, preserved_value FROM catalog.building_unit ORDER BY register_pk",
                ).fetch_all(&mut *conn).await?;
                assert_eq!(actual, vec![
                    ("MISSING-PARENT".to_owned(), None, "MISSING-PARENT".to_owned()),
                    ("REASSIGN".to_owned(), Some(new_parent), "REASSIGN".to_owned()),
                    ("UNTOUCHED".to_owned(), Some(old_parent), "UNTOUCHED".to_owned()),
                    ("WITHDRAW".to_owned(), None, "WITHDRAW".to_owned()),
                ]);
            }
            Ok(())
        }).await
    }

    #[test]
    fn a_compressed_object_yields_its_pairs() {
        let body = format!(
            "{}\n{}\n",
            pair_json("UNIT-1", "BLDG-1"),
            pair_json("UNIT-2", "BLDG-1")
        );

        let rows = pairs_in_object(&gzipped(&body), "k", &ApprovedBuildingLinks::default())
            .expect("pairs");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].building_register_pk.as_deref(), Some("BLDG-1"));
    }

    #[test]
    fn a_pair_without_its_unit_key_stops_the_object() {
        let body = pair_json("  ", "BLDG-1");

        let error = pairs_in_object(&gzipped(&body), "k", &ApprovedBuildingLinks::default())
            .expect_err("the unit key is what the backfill joins on");

        assert!(format!("{error:#}").contains("register_pk"));
    }

    #[test]
    fn a_pair_without_its_building_key_stops_the_object() {
        let body = pair_json("UNIT-1", "");

        let error = pairs_in_object(&gzipped(&body), "k", &ApprovedBuildingLinks::default())
            .expect_err("the export promised to leave unlinked units behind");

        assert!(format!("{error:#}").contains("building_register_pk"));
    }

    #[test]
    fn plain_text_where_gzip_was_promised_is_an_error() {
        let error = pairs_in_object(b"{}\n", "k", &ApprovedBuildingLinks::default())
            .expect_err("plain bytes must be refused");

        assert!(format!("{error:#}").contains("decompress"));
    }

    #[test]
    fn an_empty_object_is_an_error_not_a_success() {
        let error = pairs_in_object(&gzipped(""), "k", &ApprovedBuildingLinks::default())
            .expect_err("an empty object must not be a pass");

        assert!(format!("{error:#}").contains("no rows"));
    }

    #[test]
    fn recovery_distinguishes_explicit_unlink_from_omission_and_stale_approval(
    ) -> anyhow::Result<()> {
        let source: serde_json::Value = serde_json::from_str(&pair_json("UNIT-1", "BLDG-1"))?;
        let empty = ApprovedBuildingLinks::default();
        for key in ["building_register_pk", "building_link_evidence"] {
            let mut bad = source.clone();
            bad.as_object_mut().expect("row").remove(key);
            assert!(pairs_in_object(&gzipped(&bad.to_string()), "missing", &empty).is_err());
        }
        let mut unlinked = source.clone();
        unlinked["building_register_pk"] = serde_json::Value::Null;
        unlinked["building_link_evidence"] =
            serde_json::json!({"building_link_method": "unresolved"});
        let rows = pairs_in_object(&gzipped(&unlinked.to_string()), "explicit-null", &empty)?;
        assert!(rows[0].building_register_pk.is_none());

        let application = "11111111-1111-4111-8111-111111111111";
        let active =
            ApprovedBuildingLinks::fixture(application, "source-row:UNIT-1", "UNIT-1", None);
        assert!(pairs_in_object(&gzipped(&source.to_string()), "stale-source", &active).is_err());
        assert!(
            pairs_in_object(&gzipped(&unlinked.to_string()), "missing-approval", &active).is_err()
        );
        unlinked["building_link_evidence"] = serde_json::json!({
            "unit_row_id": "source-row:UNIT-1", "building_link_method": "staff_unlinked",
            "normalization_application_id": application
        });
        pairs_in_object(&gzipped(&unlinked.to_string()), "approved-null", &active)?;
        assert!(pairs_in_object(&gzipped(&unlinked.to_string()), "inactive", &empty).is_err());
        unlinked["register_pk"] = serde_json::json!("UNIT-OTHER");
        assert!(pairs_in_object(&gzipped(&unlinked.to_string()), "wrong-unit", &active).is_err());
        Ok(())
    }
}
