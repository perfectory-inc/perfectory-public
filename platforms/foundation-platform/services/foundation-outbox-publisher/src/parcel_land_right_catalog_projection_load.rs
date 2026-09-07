//! Named `AL_D006` handoff rows: COPY each province, then merge atomically.

use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader},
};

use anyhow::{bail, Context};
use chrono::NaiveDate;
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::Pnu;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const DEFAULT_CONTRACT: &str =
    "infra/lakehouse/contracts/vworld-land-right-registration-source-objects.json";
const OFFICIAL_SIDO_CODES: [&str; 17] = [
    "11", "26", "27", "28", "29", "30", "31", "36", "41", "43", "44", "46", "47", "48", "50", "51",
    "52",
];

#[derive(Debug, Deserialize)]
struct SourceContract {
    schema_version: u32,
    #[serde(default)]
    coordinator_fills_real_inventory: bool,
    dataset_series: String,
    csv_delimiter: String,
    load_granularity: String,
    granularity_counts: RegionCounts,
    selected_vintage: String,
    handoff_prefix: String,
    handoff_suffix: String,
    objects: Vec<SourceObject>,
}

#[derive(Debug, Deserialize)]
struct RegionCounts {
    sido: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct SourceObject {
    object_key: String,
    dataset_name: String,
    region_code: String,
    vintage: String,
}

#[derive(Clone, Debug, Deserialize)]
struct HandoffRow {
    pnu: String,
    right_serial_no: String,
    building_name: Option<String>,
    dong_name: Option<String>,
    floor_name: Option<String>,
    ho_name: Option<String>,
    room_name: Option<String>,
    right_ratio: Option<String>,
    closure_kind_code: Option<String>,
    closure_kind_name: Option<String>,
    source_record_id: String,
    source_snapshot_id: String,
}

/// Loads the newest complete national land-right snapshot in one transaction.
///
/// # Errors
/// Refuses incomplete inventory, invalid lineage or row values, and storage/database failures.
pub async fn run() -> anyhow::Result<()> {
    const CONFIRM: &str = "FOUNDATION_PLATFORM_PARCEL_LAND_RIGHT_PROJECTION_LOAD_CONFIRM";
    if !optional_bool_env(CONFIRM)?.unwrap_or(false) {
        bail!("{CONFIRM}=true is required: this command writes catalog.parcel_land_right");
    }
    let path = optional_env_value("LAND_RIGHT_SOURCE_CONTRACT")?
        .unwrap_or_else(|| DEFAULT_CONTRACT.to_owned());
    let contract: SourceContract = serde_json::from_str(
        &std::fs::read_to_string(&path).with_context(|| format!("failed to read {path}"))?,
    )?;
    let objects = selected_objects(&contract)?;
    let storage = R2ObjectStorage::from_env()?;
    let mut conn = PgConnection::connect(&required_env_value("DATABASE_URL")?).await?;
    let mut transaction = conn.begin().await?;
    prepare_stage(&mut transaction).await?;
    let mut input_rows = 0_u64;
    for object in objects {
        let key = handoff_key(&contract, object)?;
        let bytes = storage
            .get_object_bytes_range_retried(&key)
            .await
            .with_context(|| format!("failed to read land-right handoff {key}"))?;
        let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
        let rows = stage_rows(&mut transaction, reader, object).await?;
        input_rows += rows;
        tracing::info!(object = %key, rows, "land_right_registration_object_staged");
    }
    let staged: i64 = sqlx::query_scalar("SELECT count(*) FROM parcel_land_right_projection_stage")
        .fetch_one(&mut *transaction)
        .await?;
    if u64::try_from(staged)? != input_rows {
        bail!("land_right_registration_stage_count_mismatch: read={input_rows} staged={staged}");
    }
    let conflicting_groups = conflicting_groups(&mut transaction).await?;
    let inserted = merge_stage(&mut transaction).await?;
    let collapsed_or_existing = input_rows
        .checked_sub(inserted)
        .context("land_right_registration_merge_count_mismatch")?;
    transaction.commit().await?;
    tracing::info!(
        input_rows,
        staged,
        inserted,
        collapsed_or_existing,
        conflicting_groups,
        "parcel-land-right-projection-load-ok"
    );
    Ok(())
}

fn selected_objects(contract: &SourceContract) -> anyhow::Result<Vec<&SourceObject>> {
    if contract.coordinator_fills_real_inventory
        || contract.schema_version != 1
        || contract.objects.is_empty()
        || contract.dataset_series != "AL_D006"
        || contract.csv_delimiter != "|"
        || contract.load_granularity != "sido"
    {
        bail!("land_right_registration_inventory_invalid: expected measured schema_version 1 AL_D006 pipe-delimited sido CSV inventory");
    }
    let mut keys = BTreeSet::new();
    let mut region_vintages = BTreeSet::new();
    for object in &contract.objects {
        parse_vintage(&object.vintage)?;
        if !OFFICIAL_SIDO_CODES.contains(&object.region_code.as_str())
            || !object
                .object_key
                .starts_with("bronze/source=vworldkr__land_right_registration/")
            || !object.object_key.ends_with(".zip")
            || object.dataset_name
                != format!("AL_D006_{}_{}.csv", object.region_code, object.vintage)
            || !keys.insert(&object.object_key)
            || !region_vintages.insert((&object.region_code, &object.vintage))
        {
            bail!("land_right_registration_inventory_invalid: region, source, member, or duplicate object");
        }
    }
    let official: BTreeSet<&str> = OFFICIAL_SIDO_CODES.into_iter().collect();
    let regions: BTreeSet<&str> = contract
        .objects
        .iter()
        .map(|o| o.region_code.as_str())
        .collect();
    if contract.granularity_counts.sido != 17 || regions != official {
        bail!("land_right_registration_partial_country: inventory must name the official 17 provinces");
    }
    let vintages: BTreeSet<&str> = contract
        .objects
        .iter()
        .map(|o| o.vintage.as_str())
        .collect();
    let newest_complete = vintages.into_iter().rev().find(|vintage| {
        let selected: Vec<_> = contract
            .objects
            .iter()
            .filter(|o| o.vintage == *vintage)
            .collect();
        selected.len() == 17
            && selected
                .iter()
                .map(|o| o.region_code.as_str())
                .collect::<BTreeSet<_>>()
                == official
    });
    if newest_complete != Some(contract.selected_vintage.as_str()) {
        bail!("land_right_registration_partial_country: selected vintage is not the newest complete province covering");
    }
    let mut selected: Vec<_> = contract
        .objects
        .iter()
        .filter(|o| o.vintage == contract.selected_vintage)
        .collect();
    selected.sort_unstable_by(|a, b| a.region_code.cmp(&b.region_code));
    Ok(selected)
}

fn handoff_key(contract: &SourceContract, object: &SourceObject) -> anyhow::Result<String> {
    if contract.handoff_prefix.trim().is_empty() || contract.handoff_suffix.is_empty() {
        bail!("land_right_registration_handoff_contract: missing prefix or suffix");
    }
    let base = object
        .object_key
        .rsplit('/')
        .next()
        .and_then(|s| s.strip_suffix(".zip"))
        .context("land_right_registration_handoff_contract: invalid ZIP key")?;
    Ok(format!(
        "{}/{base}{}",
        contract.handoff_prefix, contract.handoff_suffix
    ))
}

fn parse_vintage(value: &str) -> anyhow::Result<NaiveDate> {
    if value.len() != 8 || !value.bytes().all(|c| c.is_ascii_digit()) {
        bail!("land_right_registration_invalid_vintage: expected YYYYMMDD");
    }
    NaiveDate::parse_from_str(value, "%Y%m%d").context("land_right_registration_invalid_vintage")
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    // Constraint-free stage (root ADR-0093): provider duplicates must reach the
    // deterministic collapse in merge_stage instead of aborting the COPY.
    conn.execute(
        "CREATE TEMPORARY TABLE parcel_land_right_projection_stage
        (LIKE catalog.parcel_land_right INCLUDING DEFAULTS)
        ON COMMIT DROP",
    )
    .await?;
    Ok(())
}

fn copy_text(value: Option<&str>) -> String {
    value.map_or_else(
        || "\\N".to_owned(),
        |v| {
            v.replace('\\', "\\\\")
                .replace('\t', "\\t")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
        },
    )
}

async fn stage_rows(
    conn: &mut PgConnection,
    reader: impl BufRead,
    object: &SourceObject,
) -> anyhow::Result<u64> {
    let mut copy = conn.copy_in_raw(
        "COPY parcel_land_right_projection_stage
        (pnu, right_serial_no, building_name, dong_name, floor_name, ho_name, room_name,
         right_ratio, closure_kind_code, closure_kind, source_snapshot_id) FROM STDIN WITH (FORMAT text)",
    ).await?;
    let rows = match copy_rows(&mut copy, reader, object).await {
        Ok(rows) => rows,
        Err(error) => {
            copy.abort(error.to_string())
                .await
                .context("failed to abort land-right COPY")?;
            return Err(error);
        }
    };
    let staged = copy.finish().await?;
    if staged == 0 || staged != rows {
        bail!("land_right_registration_stage_count_mismatch: read={rows} staged={staged}");
    }
    Ok(rows)
}

async fn copy_rows(
    copy: &mut sqlx::postgres::PgCopyIn<&mut PgConnection>,
    reader: impl BufRead,
    object: &SourceObject,
) -> anyhow::Result<u64> {
    let mut buffer = String::with_capacity(8 * 1024 * 1024);
    let mut rows = 0_u64;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffRow =
            serde_json::from_str(&line).context("land_right_registration_invalid_handoff")?;
        rows += 1;
        if row.source_record_id != object.object_key || row.source_snapshot_id.trim().is_empty() {
            bail!("land_right_registration_lineage_mismatch");
        }
        let pnu = Pnu::parse(&row.pnu).context("land_right_registration_invalid_pnu")?;
        if !pnu.as_str().starts_with(&object.region_code) {
            bail!("land_right_registration_region_mismatch");
        }
        if row.right_serial_no.is_empty()
            || !row.right_serial_no.bytes().all(|b| b.is_ascii_digit())
        {
            bail!("land_right_registration_invalid_serial");
        }
        // Unit designation columns are key members (root ADR-0093): blank means
        // "no designation", stored as '' so the six-column identity is total.
        let fields = [
            Some(row.pnu.as_str()),
            Some(row.right_serial_no.as_str()),
            row.building_name.as_deref(),
            Some(row.dong_name.as_deref().unwrap_or("")),
            Some(row.floor_name.as_deref().unwrap_or("")),
            Some(row.ho_name.as_deref().unwrap_or("")),
            Some(row.room_name.as_deref().unwrap_or("")),
            row.right_ratio.as_deref(),
            row.closure_kind_code.as_deref(),
            row.closure_kind_name.as_deref(),
            Some(row.source_snapshot_id.as_str()),
        ];
        buffer.push_str(
            &fields
                .into_iter()
                .map(copy_text)
                .collect::<Vec<_>>()
                .join("\t"),
        );
        buffer.push('\n');
        if buffer.len() >= 8 * 1024 * 1024 {
            copy.send(buffer.as_bytes()).await?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        copy.send(buffer.as_bytes()).await?;
    }
    Ok(rows)
}

/// Groups whose six-column unit identity repeats with different payload values:
/// provider ratio/closure variants of the kind ADR-0093 measured (~49k groups
/// nationwide). They are collapsed deterministically, never silently: this
/// number is in the log.
async fn conflicting_groups(conn: &mut PgConnection) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM (
            SELECT 1 FROM parcel_land_right_projection_stage
            GROUP BY pnu, right_serial_no, dong_name, floor_name, ho_name, room_name
            HAVING count(DISTINCT (building_name, right_ratio,
                                   closure_kind_code, closure_kind)) > 1) conflicts",
    )
    .fetch_one(conn)
    .await?)
}

async fn merge_stage(conn: &mut PgConnection) -> anyhow::Result<u64> {
    // Deterministic collapse (root ADR-0093): one row per six-column unit
    // identity, chosen by a total order over the remaining columns so a rerun
    // picks the same row.
    Ok(sqlx::query(
        "INSERT INTO catalog.parcel_land_right
        (pnu, right_serial_no, building_name, dong_name, floor_name, ho_name, room_name,
         right_ratio, closure_kind_code, closure_kind, source_snapshot_id)
        SELECT DISTINCT ON (pnu, right_serial_no, dong_name, floor_name, ho_name, room_name)
            pnu, right_serial_no, building_name, dong_name, floor_name, ho_name, room_name,
            right_ratio, closure_kind_code, closure_kind, source_snapshot_id
        FROM parcel_land_right_projection_stage
        ORDER BY pnu, right_serial_no, dong_name, floor_name, ho_name, room_name,
            building_name NULLS FIRST, right_ratio NULLS FIRST,
            closure_kind_code NULLS FIRST, closure_kind NULLS FIRST,
            source_snapshot_id
        ON CONFLICT (pnu, right_serial_no, dong_name, floor_name, ho_name, room_name)
            DO NOTHING",
    )
    .execute(conn)
    .await?
    .rows_affected())
}

#[cfg(test)]
#[path = "parcel_land_right_projection_tests.rs"]
mod tests;
