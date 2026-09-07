//! Named `AL_D003` CSV forest-ledger attributes: COPY each province, then merge atomically.

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

const DEFAULT_CONTRACT: &str = "infra/lakehouse/contracts/vworld-land-forest-source-objects.json";

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
    land_category: Option<String>,
    area_m2: Option<f64>,
    ownership_kind_code: Option<String>,
    co_owner_count: Option<i32>,
    source_record_id: String,
    source_snapshot_id: String,
}

/// Runs the national forest-ledger projection inside one transaction.
///
/// # Errors
/// Refuses incomplete inventory, invalid lineage or row values, and storage/database failures.
pub async fn run() -> anyhow::Result<()> {
    const CONFIRM: &str = "FOUNDATION_PLATFORM_PARCEL_FOREST_LEDGER_PROJECTION_LOAD_CONFIRM";
    if !optional_bool_env(CONFIRM)?.unwrap_or(false) {
        bail!("{CONFIRM}=true is required: this command writes catalog.parcel_forest_ledger");
    }
    let path = optional_env_value("LAND_FOREST_SOURCE_CONTRACT")?
        .unwrap_or_else(|| DEFAULT_CONTRACT.to_owned());
    let contract: SourceContract = serde_json::from_str(
        &std::fs::read_to_string(&path).with_context(|| format!("failed to read {path}"))?,
    )?;
    let objects = selected_objects(&contract)?;
    let storage = R2ObjectStorage::from_env()?;
    let mut conn = PgConnection::connect(&required_env_value("DATABASE_URL")?).await?;
    let mut transaction = conn.begin().await?;
    prepare_stage(&mut transaction).await?;
    let mut merged = 0_u64;
    for object in objects {
        let key = handoff_key(&contract, object)?;
        let bytes = storage
            .get_object_bytes_range_retried(&key)
            .await
            .with_context(|| format!("failed to read forest-ledger handoff {key}"))?;
        let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
        let (rows, skipped) = stage_rows(&mut transaction, reader, object).await?;
        let changed = merge_stage(&mut transaction).await?;
        merged += changed;
        tracing::info!(object = %key, rows, skipped, changed, "land_forest_object_merged");
    }
    transaction.commit().await?;
    tracing::info!(merged, "parcel-forest-ledger-projection-load-ok");
    Ok(())
}

fn selected_objects(contract: &SourceContract) -> anyhow::Result<Vec<&SourceObject>> {
    if contract.coordinator_fills_real_inventory
        || contract.schema_version != 1
        || contract.objects.is_empty()
        || contract.dataset_series != "AL_D003"
        || contract.load_granularity != "sido"
    {
        bail!("land_forest_inventory_invalid: expected measured schema_version 1 AL_D003 sido CSV inventory");
    }
    let mut keys = BTreeSet::new();
    let mut region_vintages = BTreeSet::new();
    for object in &contract.objects {
        parse_vintage(&object.vintage)?;
        if object.region_code.len() != 2
            || !object.region_code.bytes().all(|c| c.is_ascii_digit())
            || !object
                .object_key
                .starts_with("bronze/source=vworldkr__land_forest/")
            || !object.object_key.ends_with(".zip")
            || object.dataset_name
                != format!("AL_D003_{}_{}.csv", object.region_code, object.vintage)
            || !keys.insert(&object.object_key)
            || !region_vintages.insert((&object.region_code, &object.vintage))
        {
            bail!("land_forest_inventory_invalid: region, source, member, or duplicate object");
        }
    }
    let regions: BTreeSet<&str> = contract
        .objects
        .iter()
        .map(|o| o.region_code.as_str())
        .collect();
    if contract.granularity_counts.sido != 17 || regions.len() != 17 {
        bail!("land_forest_partial_country: inventory must name all 17 provinces");
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
        bail!("land_forest_partial_country: selected vintage is not the newest complete province covering");
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
        bail!("land_forest_handoff_contract: missing prefix or suffix");
    }
    let base = object
        .object_key
        .rsplit('/')
        .next()
        .and_then(|s| s.strip_suffix(".zip"))
        .context("land_forest_handoff_contract: invalid ZIP key")?;
    Ok(format!(
        "{}/{base}{}",
        contract.handoff_prefix, contract.handoff_suffix
    ))
}

fn parse_vintage(value: &str) -> anyhow::Result<NaiveDate> {
    if value.len() != 8 || !value.bytes().all(|c| c.is_ascii_digit()) {
        bail!("land_forest_invalid_vintage: expected YYYYMMDD");
    }
    NaiveDate::parse_from_str(value, "%Y%m%d").context("land_forest_invalid_vintage")
}

fn valid_row(row: &HandoffRow) -> bool {
    row.area_m2.is_some_and(|a| a.is_finite() && a > 0.0)
        && row.co_owner_count.is_none_or(|count| count >= 0)
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE parcel_forest_ledger_projection_stage (
        pnu character(19) NOT NULL, land_category text, area_m2 numeric NOT NULL,
        ownership_kind text, co_owner_count integer, source_snapshot_id text NOT NULL,
        source_vintage date NOT NULL
    ) ON COMMIT DROP",
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
) -> anyhow::Result<(u64, u64)> {
    conn.execute("TRUNCATE parcel_forest_ledger_projection_stage")
        .await?;
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_forest_ledger_projection_stage
        (pnu, land_category, area_m2, ownership_kind, co_owner_count, source_snapshot_id,
         source_vintage) FROM STDIN WITH (FORMAT text)",
        )
        .await?;
    let (rows, skipped) = match copy_rows(&mut copy, reader, object).await {
        Ok(counts) => counts,
        Err(error) => {
            copy.abort(error.to_string())
                .await
                .context("failed to abort forest-ledger COPY")?;
            return Err(error);
        }
    };
    let staged = copy.finish().await?;
    if staged == 0 || staged != rows - skipped {
        bail!("land_forest_stage_count_mismatch: read={rows} skipped={skipped} staged={staged}");
    }
    Ok((rows, skipped))
}

async fn copy_rows(
    copy: &mut sqlx::postgres::PgCopyIn<&mut PgConnection>,
    reader: impl BufRead,
    object: &SourceObject,
) -> anyhow::Result<(u64, u64)> {
    let mut buffer = String::with_capacity(8 * 1024 * 1024);
    let vintage = parse_vintage(&object.vintage)?.to_string();
    let (mut rows, mut skipped) = (0_u64, 0_u64);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffRow = serde_json::from_str(&line).context("land_forest_invalid_handoff")?;
        rows += 1;
        if row.source_record_id != object.object_key || row.source_snapshot_id.trim().is_empty() {
            bail!("land_forest_lineage_mismatch");
        }
        let Ok(pnu) = Pnu::parse(&row.pnu) else {
            skipped += 1;
            continue;
        };
        if !valid_row(&row) {
            skipped += 1;
            continue;
        }
        if !pnu.as_str().starts_with(&object.region_code) {
            bail!("land_forest_region_mismatch");
        }
        let area = row.area_m2.context("land_forest_invalid_area")?.to_string();
        let co_owner_count = row.co_owner_count.map(|count| count.to_string());
        let fields = [
            Some(row.pnu.as_str()),
            row.land_category.as_deref(),
            Some(area.as_str()),
            row.ownership_kind_code.as_deref(),
            co_owner_count.as_deref(),
            Some(row.source_snapshot_id.as_str()),
            Some(vintage.as_str()),
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
    Ok((rows, skipped))
}

async fn merge_stage(conn: &mut PgConnection) -> anyhow::Result<u64> {
    Ok(sqlx::query("INSERT INTO catalog.parcel_forest_ledger
        (pnu, land_category, area_m2, ownership_kind, co_owner_count, source_snapshot_id, source_vintage)
        SELECT DISTINCT ON (pnu) pnu, land_category, area_m2, ownership_kind, co_owner_count,
            source_snapshot_id, source_vintage
        FROM parcel_forest_ledger_projection_stage AS stage
        ORDER BY pnu, source_vintage DESC, md5(row(stage.*)::text)
        ON CONFLICT (pnu) DO UPDATE SET land_category = EXCLUDED.land_category,
            area_m2 = EXCLUDED.area_m2, ownership_kind = EXCLUDED.ownership_kind,
            co_owner_count = EXCLUDED.co_owner_count, source_snapshot_id = EXCLUDED.source_snapshot_id,
            source_vintage = EXCLUDED.source_vintage, loaded_at = now()
        WHERE EXCLUDED.source_vintage > catalog.parcel_forest_ledger.source_vintage")
        .execute(conn).await?.rows_affected())
}

#[cfg(test)]
#[path = "parcel_forest_ledger_projection_tests.rs"]
mod tests;
