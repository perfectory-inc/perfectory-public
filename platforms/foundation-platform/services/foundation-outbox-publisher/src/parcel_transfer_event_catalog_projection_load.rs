//! Named `AL_D157` CSV transfer-event attributes: COPY each province, then merge atomically.

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
    "infra/lakehouse/contracts/vworld-land-transfer-history-source-objects.json";

#[derive(Debug, Deserialize)]
struct SourceContract {
    schema_version: u32,
    #[serde(default)]
    coordinator_fills_real_inventory: bool,
    dataset_series: String,
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
    transfer_history_seq: i64,
    reason_code: Option<String>,
    reason: Option<String>,
    moved_at: Option<String>,
    erased_at: Option<String>,
    land_category: Option<String>,
    area_m2: Option<f64>,
    closure_seq: Option<String>,
    source_record_id: String,
    source_snapshot_id: String,
}

/// Runs the national transfer-event projection inside one transaction.
///
/// # Errors
/// Refuses incomplete inventory, invalid lineage or row values, and storage/database failures.
pub async fn run() -> anyhow::Result<()> {
    const CONFIRM: &str = "FOUNDATION_PLATFORM_PARCEL_TRANSFER_EVENT_PROJECTION_LOAD_CONFIRM";
    if !optional_bool_env(CONFIRM)?.unwrap_or(false) {
        bail!("{CONFIRM}=true is required: this command writes catalog.parcel_transfer_event");
    }
    let path = optional_env_value("LAND_TRANSFER_SOURCE_CONTRACT")?
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
            .with_context(|| format!("failed to read transfer-event handoff {key}"))?;
        let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
        let rows = stage_rows(&mut transaction, reader, object).await?;
        input_rows += rows;
        tracing::info!(object = %key, rows, "land_transfer_history_object_staged");
    }
    let staged: i64 =
        sqlx::query_scalar("SELECT count(*) FROM parcel_transfer_event_projection_stage")
            .fetch_one(&mut *transaction)
            .await?;
    if u64::try_from(staged)? != input_rows {
        bail!("land_transfer_history_stage_count_mismatch: read={input_rows} staged={staged}");
    }
    let inserted = merge_stage(&mut transaction).await?;
    let existing = input_rows
        .checked_sub(inserted)
        .context("land_transfer_history_merge_count_mismatch")?;
    transaction.commit().await?;
    tracing::info!(
        input_rows,
        staged,
        inserted,
        existing,
        "parcel-transfer-event-projection-load-ok"
    );
    Ok(())
}

fn selected_objects(contract: &SourceContract) -> anyhow::Result<Vec<&SourceObject>> {
    if contract.coordinator_fills_real_inventory
        || contract.schema_version != 1
        || contract.objects.is_empty()
        || contract.dataset_series != "AL_D157"
        || contract.load_granularity != "sido"
    {
        bail!("land_transfer_history_inventory_invalid: expected measured schema_version 1 AL_D157 sido CSV inventory");
    }
    let mut keys = BTreeSet::new();
    let mut region_vintages = BTreeSet::new();
    for object in &contract.objects {
        parse_vintage(&object.vintage)?;
        if object.region_code.len() != 2
            || !object.region_code.bytes().all(|c| c.is_ascii_digit())
            || !object
                .object_key
                .starts_with("bronze/source=vworldkr__land_transfer_history/")
            || !object.object_key.ends_with(".zip")
            || object.dataset_name
                != format!("AL_D157_{}_{}.csv", object.region_code, object.vintage)
            || !keys.insert(&object.object_key)
            || !region_vintages.insert((&object.region_code, &object.vintage))
        {
            bail!("land_transfer_history_inventory_invalid: region, source, member, or duplicate object");
        }
    }
    let regions: BTreeSet<&str> = contract
        .objects
        .iter()
        .map(|o| o.region_code.as_str())
        .collect();
    if contract.granularity_counts.sido != 17 || regions.len() != 17 {
        bail!("land_transfer_history_partial_country: inventory must name all 17 provinces");
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
                == regions
    });
    if newest_complete != Some(contract.selected_vintage.as_str()) {
        bail!("land_transfer_history_partial_country: selected vintage is not the newest complete province covering");
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
        bail!("land_transfer_history_handoff_contract: missing prefix or suffix");
    }
    let base = object
        .object_key
        .rsplit('/')
        .next()
        .and_then(|s| s.strip_suffix(".zip"))
        .context("land_transfer_history_handoff_contract: invalid ZIP key")?;
    Ok(format!(
        "{}/{base}{}",
        contract.handoff_prefix, contract.handoff_suffix
    ))
}

fn parse_vintage(value: &str) -> anyhow::Result<NaiveDate> {
    if value.len() != 8 || !value.bytes().all(|c| c.is_ascii_digit()) {
        bail!("land_transfer_history_invalid_vintage: expected YYYYMMDD");
    }
    NaiveDate::parse_from_str(value, "%Y%m%d").context("land_transfer_history_invalid_vintage")
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    // Inherit the serving key instead of duplicating it. Keep the stage for the entire
    // vintage: even identical event duplicates must fail COPY before the merge.
    conn.execute(
        "CREATE TEMPORARY TABLE parcel_transfer_event_projection_stage
        (LIKE catalog.parcel_transfer_event INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)
        ON COMMIT DROP",
    ).await?;
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
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_transfer_event_projection_stage
        (pnu, transfer_history_seq, reason_code, reason, moved_at, erased_at,
         land_category, area_m2, closure_seq, source_snapshot_id) FROM STDIN WITH (FORMAT text)",
        )
        .await?;
    let rows = match copy_rows(&mut copy, reader, object).await {
        Ok(counts) => counts,
        Err(error) => {
            copy.abort(error.to_string())
                .await
                .context("failed to abort transfer-event COPY")?;
            return Err(error);
        }
    };
    let staged = copy.finish().await?;
    if staged == 0 || staged != rows {
        bail!("land_transfer_history_stage_count_mismatch: read={rows} staged={staged}");
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
            serde_json::from_str(&line).context("land_transfer_history_invalid_handoff")?;
        rows += 1;
        if row.source_record_id != object.object_key || row.source_snapshot_id.trim().is_empty() {
            bail!("land_transfer_history_lineage_mismatch");
        }
        let pnu = Pnu::parse(&row.pnu).context("land_transfer_history_invalid_pnu")?;
        if row.area_m2.is_some_and(|area| !area.is_finite()) {
            bail!("land_transfer_history_invalid_area");
        }
        if !pnu.as_str().starts_with(&object.region_code) {
            bail!("land_transfer_history_region_mismatch");
        }
        let seq = row.transfer_history_seq.to_string();
        let area = row.area_m2.map(|value| value.to_string());
        let fields = [
            Some(row.pnu.as_str()),
            Some(seq.as_str()),
            row.reason_code.as_deref(),
            row.reason.as_deref(),
            row.moved_at.as_deref(),
            row.erased_at.as_deref(),
            row.land_category.as_deref(),
            area.as_deref(),
            row.closure_seq.as_deref(),
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

async fn merge_stage(conn: &mut PgConnection) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "INSERT INTO catalog.parcel_transfer_event
        (pnu, transfer_history_seq, reason_code, reason, moved_at, erased_at,
         land_category, area_m2, closure_seq, source_snapshot_id)
        SELECT pnu, transfer_history_seq, reason_code, reason, moved_at, erased_at,
            land_category, area_m2, closure_seq, source_snapshot_id
        FROM parcel_transfer_event_projection_stage
        ON CONFLICT (pnu, transfer_history_seq) DO NOTHING",
    )
    .execute(conn)
    .await?
    .rows_affected())
}

#[cfg(test)]
#[path = "parcel_transfer_event_projection_tests.rs"]
mod tests;
