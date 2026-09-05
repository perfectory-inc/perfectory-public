//! Loads per-parcel zoning verdicts from the land-use plan handoffs into `catalog.parcel_zoning`
//! (root ADR-0083 §4–5).
//!
//! The verdict is a code-tree walk, not a lookup table: each plan row's zone code climbs the
//! LMIS `parent_ucode` edges until it reaches one of the seven anchor families, or `UQA001`
//! (도시 미세분) when it arrives there without passing the four urban anchors. A code that
//! reaches no anchor is not a 용도지역 and is excluded — counted per code in the summary,
//! never silently (ADR-0083 §4). 접함(3) rows never qualify: being adjacent to a zone is not
//! the parcel's own use.
//!
//! **Staged, then merged.** Same shape as `parcel_catalog_projection_load`: rows land in a
//! session-temporary stage through `COPY`, one handoff object at a time, and the merge
//! deduplicates (pnu, zone) pairs by keeping the strongest designation (포함 over 저촉).
//! `ON CONFLICT DO NOTHING` makes a repeat run a no-op.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};

use anyhow::{bail, Context};
use foundation_outbox::R2ObjectStorage;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_ZONING_PROJECTION_LOAD_CONFIRM";
const PLAN_CONTRACT_ENV: &str = "LAND_USE_PLAN_SOURCE_CONTRACT";
const ZONE_CODE_CONTRACT_ENV: &str = "LAND_USE_ZONE_CODE_SOURCE_CONTRACT";
const DEFAULT_PLAN_CONTRACT: &str =
    "infra/lakehouse/contracts/vworld-land-use-plan-source-objects.json";
const DEFAULT_ZONE_CODE_CONTRACT: &str =
    "infra/lakehouse/contracts/vworld-land-use-zone-code-source-objects.json";
const SOURCE_CONTRACT_SCHEMA_VERSION: u32 = 1;

/// The ADR-0083 §4 anchor set: the seven family roots a zone code may resolve to directly.
const ZONING_ANCHORS: [&str; 7] = [
    "UQA100", "UQA200", "UQA300", "UQA400", "UQB001", "UQC001", "UQD001",
];
/// 도시지역 root: reached without passing the four urban anchors, the verdict is
/// "urban, subdivision unstated".
const URBAN_ROOT: &str = "UQA001";
/// The deepest observed chain is three edges; sixteen refuses a cycle without refusing a tree.
const MAX_WALK_DEPTH: usize = 16;

/// One plan handoff row, of which this load reads five fields.
#[derive(Debug, Deserialize)]
struct PlanHandoffRow {
    pnu: String,
    inclusion_code: String,
    zone_code: String,
    zone_name: Option<String>,
    source_snapshot_id: String,
}

/// One zone-code handoff row: a node and its parent edge in the LMIS tree.
#[derive(Debug, Deserialize)]
struct ZoneCodeHandoffRow {
    ucode: String,
    parent_ucode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PlanSourceContract {
    schema_version: u32,
    selected_vintage: String,
    handoff_prefix: String,
    handoff_suffix: String,
    objects: Vec<PlanSourceObject>,
}

#[derive(Debug, Deserialize)]
struct PlanSourceObject {
    object_key: String,
    region_code: String,
    vintage: String,
}

#[derive(Debug, Deserialize)]
struct ZoneCodeSourceContract {
    schema_version: u32,
    handoff_prefix: String,
    handoff_suffix: String,
    objects: Vec<ZoneCodeSourceObject>,
}

#[derive(Debug, Deserialize)]
struct ZoneCodeSourceObject {
    object_key: String,
}

/// Runs the projection load.
///
/// # Errors
/// Returns an error when configuration, either contract, a handoff read, or the merge fails.
pub async fn run() -> anyhow::Result<()> {
    let confirmed = optional_bool_env(CONFIRM_ENV)?.unwrap_or(false);
    if !confirmed {
        bail!("{CONFIRM_ENV}=true is required: this command writes catalog.parcel_zoning");
    }
    let database_url = required_env_value("DATABASE_URL")?;
    let plan_contract: PlanSourceContract = read_contract(
        &optional_env_value(PLAN_CONTRACT_ENV)?.unwrap_or_else(|| DEFAULT_PLAN_CONTRACT.to_owned()),
    )?;
    let zone_code_contract: ZoneCodeSourceContract = read_contract(
        &optional_env_value(ZONE_CODE_CONTRACT_ENV)?
            .unwrap_or_else(|| DEFAULT_ZONE_CODE_CONTRACT.to_owned()),
    )?;
    if plan_contract.schema_version != SOURCE_CONTRACT_SCHEMA_VERSION
        || zone_code_contract.schema_version != SOURCE_CONTRACT_SCHEMA_VERSION
    {
        bail!("a source object contract is not the schema_version this command reads");
    }

    let plan_keys = plan_handoff_keys(&plan_contract)?;
    let zone_code_key = zone_code_handoff_key(&zone_code_contract)?;

    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for the zoning projection load")?;
    let parents = read_zone_code_tree(&storage, &zone_code_key).await?;
    tracing::info!(
        zone_codes = parents.len(),
        plan_objects = plan_keys.len(),
        "zoning projection inputs resolved"
    );

    let mut conn = PgConnection::connect(&database_url)
        .await
        .context("failed to connect to the catalog database")?;
    prepare_stage(&mut conn).await?;

    let mut totals = LoadTotals::default();
    for key in &plan_keys {
        let report = load_one(&storage, &mut conn, &parents, key).await?;
        tracing::info!(
            object = %key,
            rows_read = report.rows_read,
            rows_staged = report.rows_staged,
            adjacency_skipped = report.adjacency_skipped,
            unresolved_skipped = report.unresolved_skipped,
            inserted = report.inserted,
            "zoning projection object merged"
        );
        totals.add(&report);
    }

    let table_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.parcel_zoning")
        .fetch_one(&mut conn)
        .await
        .context("failed to count catalog.parcel_zoning")?;
    let mut unresolved: Vec<(String, u64)> = totals.unresolved_by_code.into_iter().collect();
    unresolved.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    tracing::info!(
        objects = plan_keys.len(),
        rows_read = totals.rows_read,
        rows_staged = totals.rows_staged,
        adjacency_skipped = totals.adjacency_skipped,
        unresolved_skipped = totals.unresolved_skipped,
        inserted = totals.inserted,
        table_rows,
        top_unresolved = ?unresolved.iter().take(10).collect::<Vec<_>>(),
        "parcel-zoning-projection-load-ok"
    );
    Ok(())
}

fn read_contract<T: serde::de::DeserializeOwned>(path: &str) -> anyhow::Result<T> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read source contract {path}"))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse source contract {path}"))
}

/// The selected vintage's handoff keys, refused unless it covers exactly the 17 provinces.
fn plan_handoff_keys(contract: &PlanSourceContract) -> anyhow::Result<Vec<String>> {
    let picked: Vec<&PlanSourceObject> = contract
        .objects
        .iter()
        .filter(|object| object.vintage == contract.selected_vintage)
        .collect();
    let mut regions: Vec<&str> = picked
        .iter()
        .map(|object| object.region_code.as_str())
        .collect();
    regions.sort_unstable();
    regions.dedup();
    if picked.len() != 17 || regions.len() != 17 {
        bail!(
            "selected vintage {} covers {} objects over {} provinces, not the 17 a national \
             projection needs",
            contract.selected_vintage,
            picked.len(),
            regions.len()
        );
    }
    let mut keys: Vec<String> = picked
        .iter()
        .map(|object| {
            handoff_key(
                &object.object_key,
                &contract.handoff_prefix,
                &contract.handoff_suffix,
            )
        })
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

fn zone_code_handoff_key(contract: &ZoneCodeSourceContract) -> anyhow::Result<String> {
    match contract.objects.as_slice() {
        [object] => Ok(handoff_key(
            &object.object_key,
            &contract.handoff_prefix,
            &contract.handoff_suffix,
        )),
        other => bail!(
            "the zone-code contract names {} loadable objects, not the exactly one this command reads",
            other.len()
        ),
    }
}

fn handoff_key(object_key: &str, prefix: &str, suffix: &str) -> String {
    let file = object_key.rsplit('/').next().unwrap_or(object_key);
    let base = file.strip_suffix(".zip").unwrap_or(file);
    format!("{prefix}/{base}{suffix}")
}

/// Reads the LMIS tree edges from the zone-code handoff.
async fn read_zone_code_tree(
    storage: &R2ObjectStorage,
    key: &str,
) -> anyhow::Result<BTreeMap<String, Option<String>>> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read zone-code handoff {key}"))?;
    let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
    let mut parents = BTreeMap::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read {key} line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: ZoneCodeHandoffRow = serde_json::from_str(&line)
            .with_context(|| format!("zone-code handoff {key} line {} is not a row", index + 1))?;
        parents.insert(row.ucode, row.parent_ucode);
    }
    if parents.is_empty() {
        bail!("zone-code handoff {key} carried no rows");
    }
    Ok(parents)
}

/// Walks the parent edges to an anchor (ADR-0083 §4).
///
/// `None` means the code is not a 용도지역 — a 지구/구역 from another law, or an orphan whose
/// chain never reaches the tree roots. The caller counts it; nothing drops silently.
fn anchor_for(parents: &BTreeMap<String, Option<String>>, zone_code: &str) -> Option<String> {
    let mut current = zone_code.to_owned();
    for _ in 0..MAX_WALK_DEPTH {
        if ZONING_ANCHORS.contains(&current.as_str()) {
            return Some(current);
        }
        if current == URBAN_ROOT {
            return Some(URBAN_ROOT.to_owned());
        }
        let next = parents.get(&current)?.clone()?;
        let trimmed = next.trim();
        if trimmed.is_empty() || trimmed == "000000" {
            return None;
        }
        current = trimmed.to_owned();
    }
    None
}

#[derive(Debug, Default)]
struct LoadTotals {
    rows_read: u64,
    rows_staged: u64,
    adjacency_skipped: u64,
    unresolved_skipped: u64,
    inserted: u64,
    unresolved_by_code: BTreeMap<String, u64>,
}

impl LoadTotals {
    fn add(&mut self, report: &ObjectReport) {
        self.rows_read += report.rows_read;
        self.rows_staged += report.rows_staged;
        self.adjacency_skipped += report.adjacency_skipped;
        self.unresolved_skipped += report.unresolved_skipped;
        self.inserted += report.inserted;
        for (code, count) in &report.unresolved_by_code {
            *self.unresolved_by_code.entry(code.clone()).or_insert(0) += count;
        }
    }
}

#[derive(Debug, Default)]
struct ObjectReport {
    rows_read: u64,
    rows_staged: u64,
    adjacency_skipped: u64,
    unresolved_skipped: u64,
    inserted: u64,
    unresolved_by_code: BTreeMap<String, u64>,
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_zoning_projection_stage (
             pnu text NOT NULL,
             zone_code text NOT NULL,
             zone_name text,
             anchor_code text NOT NULL,
             inclusion_code text NOT NULL,
             source_snapshot_id text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create the parcel zoning projection stage")?;
    Ok(())
}

async fn load_one(
    storage: &R2ObjectStorage,
    conn: &mut PgConnection,
    parents: &BTreeMap<String, Option<String>>,
    key: &str,
) -> anyhow::Result<ObjectReport> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read plan handoff object {key}"))?;

    conn.execute("TRUNCATE TABLE parcel_zoning_projection_stage")
        .await
        .context("failed to truncate the parcel zoning projection stage")?;
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_zoning_projection_stage \
             (pnu, zone_code, zone_name, anchor_code, inclusion_code, source_snapshot_id) \
             FROM STDIN WITH (FORMAT text)",
        )
        .await
        .context("failed to start COPY into the parcel zoning projection stage")?;

    let mut report = ObjectReport::default();
    let mut buffer = String::with_capacity(8 * 1024 * 1024);
    let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read {key} line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        report.rows_read += 1;
        let row: PlanHandoffRow = serde_json::from_str(&line)
            .with_context(|| format!("plan handoff {key} line {} is not a row", index + 1))?;
        match row.inclusion_code.as_str() {
            "1" | "2" => {}
            _ => {
                report.adjacency_skipped += 1;
                continue;
            }
        }
        let Some(anchor) = anchor_for(parents, &row.zone_code) else {
            report.unresolved_skipped += 1;
            *report
                .unresolved_by_code
                .entry(row.zone_code.clone())
                .or_insert(0) += 1;
            continue;
        };

        buffer.push_str(&row.pnu);
        buffer.push('\t');
        buffer.push_str(&row.zone_code);
        buffer.push('\t');
        match &row.zone_name {
            Some(name) if !name.trim().is_empty() => buffer.push_str(name.trim()),
            _ => buffer.push_str("\\N"),
        }
        buffer.push('\t');
        buffer.push_str(&anchor);
        buffer.push('\t');
        buffer.push_str(&row.inclusion_code);
        buffer.push('\t');
        buffer.push_str(&row.source_snapshot_id);
        buffer.push('\n');
        report.rows_staged += 1;
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
    if staged != report.rows_staged {
        bail!(
            "COPY reported {staged} rows but the stream staged {}",
            report.rows_staged
        );
    }
    if report.rows_read == 0 {
        bail!("plan handoff {key} carried no rows, which is not a state this dataset has");
    }

    // One (parcel, zone) verdict per pair: 포함(1) beats 저촉(2) because min() on the code
    // text picks it; the anchor is a function of the zone code so max() is a tie of equals.
    let inserted = sqlx::query(
        "INSERT INTO catalog.parcel_zoning \
             (pnu, zone_code, zone_name, anchor_code, inclusion_code, source_snapshot_id)
         SELECT pnu, zone_code, max(zone_name), max(anchor_code), min(inclusion_code),
                max(source_snapshot_id)
         FROM parcel_zoning_projection_stage
         GROUP BY pnu, zone_code
         ON CONFLICT (pnu, zone_code) DO NOTHING",
    )
    .execute(&mut *conn)
    .await
    .context("failed to merge the parcel zoning projection stage")?
    .rows_affected();
    report.inserted = inserted;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(edges: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
        edges
            .iter()
            .map(|(code, parent)| ((*code).to_owned(), parent.map(ToOwned::to_owned)))
            .collect()
    }

    #[test]
    fn a_zone_walks_to_its_family_anchor() {
        let parents = tree(&[
            ("UQA320", Some("UQA300")),
            ("UQA300", Some("UQA001")),
            ("UQA001", Some("000000")),
            ("UQB300", Some("UQB001")),
            ("UQB001", Some("000000")),
        ]);
        assert_eq!(anchor_for(&parents, "UQA320").as_deref(), Some("UQA300"));
        assert_eq!(anchor_for(&parents, "UQB300").as_deref(), Some("UQB001"));
        assert_eq!(anchor_for(&parents, "UQB001").as_deref(), Some("UQB001"));
    }

    #[test]
    fn urban_root_reached_without_an_urban_anchor_is_the_unsubdivided_verdict() {
        let parents = tree(&[("UQA50X", Some("UQA001")), ("UQA001", Some("000000"))]);
        assert_eq!(anchor_for(&parents, "UQA50X").as_deref(), Some("UQA001"));
        assert_eq!(anchor_for(&parents, "UQA001").as_deref(), Some("UQA001"));
    }

    #[test]
    fn a_district_from_another_law_reaches_no_anchor_and_is_refused() {
        // 농업진흥구역: real chain from the measured code table — parents that never reach
        // the anchor set.
        let parents = tree(&[("UEA110", Some("UEA100")), ("UEA100", Some("000000"))]);
        assert_eq!(anchor_for(&parents, "UEA110"), None);
        // Unknown code entirely.
        assert_eq!(anchor_for(&parents, "ZZZ999"), None);
    }

    #[test]
    fn a_cycle_refuses_instead_of_spinning() {
        let parents = tree(&[("A", Some("B")), ("B", Some("A"))]);
        assert_eq!(anchor_for(&parents, "A"), None);
    }

    #[test]
    fn the_vintage_selection_refuses_a_partial_country() {
        let contract = PlanSourceContract {
            schema_version: 1,
            selected_vintage: "20260609".to_owned(),
            handoff_prefix: "silver-handoff/vworldkr__land_use_plan".to_owned(),
            handoff_suffix: ".jsonl.gz".to_owned(),
            objects: vec![PlanSourceObject {
                object_key: "bronze/source=vworldkr__land_use_plan/x-1.zip".to_owned(),
                region_code: "36".to_owned(),
                vintage: "20260609".to_owned(),
            }],
        };
        let error = plan_handoff_keys(&contract).expect_err("one province must refuse");
        assert!(error.to_string().contains("17"), "{error}");
    }
}
