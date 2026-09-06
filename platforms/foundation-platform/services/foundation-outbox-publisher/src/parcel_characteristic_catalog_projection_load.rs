//! Verified `AL_D194` attributes: COPY each object, check the mapping, then merge atomically.
//! Source price is verification evidence only; the serving projection has no price column.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader},
};

use anyhow::{bail, Context};
use chrono::NaiveDate;
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::Pnu;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};

use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, optional_usize_env, required_env_value,
};

const DEFAULT_CONTRACT: &str =
    "infra/lakehouse/contracts/vworld-land-characteristic-source-objects.json";
const LAND_CATEGORIES: &[&str] = &[
    "전",
    "답",
    "과수원",
    "목장용지",
    "임야",
    "광천지",
    "염전",
    "대",
    "공장용지",
    "학교용지",
    "주차장",
    "주유소용지",
    "창고용지",
    "도로",
    "철도용지",
    "제방",
    "하천",
    "구거",
    "유지",
    "양어장",
    "수도용지",
    "공원",
    "체육용지",
    "유원지",
    "종교용지",
    "사적지",
    "묘지",
    "잡종지",
];

#[derive(Debug, Deserialize)]
struct SourceContract {
    schema_version: u32,
    #[serde(default)]
    coordinator_fills_real_inventory: bool,
    granularity_counts: RegionCounts,
    selected_vintage: String,
    handoff_prefix: String,
    handoff_suffix: String,
    objects: Vec<SourceObject>,
}

#[derive(Debug, Deserialize)]
struct RegionCounts {
    sigungu: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct SourceObject {
    object_key: String,
    region_code: String,
    vintage: String,
}

#[derive(Clone, Debug, Deserialize)]
struct HandoffRow {
    pnu: String,
    land_category: Option<String>,
    area_m2: Option<f64>,
    land_use_situation: Option<String>,
    terrain_height: Option<String>,
    terrain_shape: Option<String>,
    road_contact: Option<String>,
    verification_price_per_m2: String,
    base_year: String,
    base_month: String,
    source_vintage: String,
    source_record_id: String,
    source_snapshot_id: String,
}

/// Runs the national characteristic projection inside one transaction.
///
/// # Errors
/// Refuses incomplete inventory, invalid lineage, a failed mapping gate, and IO/SQL errors.
pub async fn run() -> anyhow::Result<()> {
    const CONFIRM: &str = "FOUNDATION_PLATFORM_PARCEL_CHARACTERISTIC_PROJECTION_LOAD_CONFIRM";
    if !optional_bool_env(CONFIRM)?.unwrap_or(false) {
        bail!("{CONFIRM}=true is required: this command writes catalog.parcel_characteristic");
    }
    let path = optional_env_value("LAND_CHARACTERISTIC_SOURCE_CONTRACT")?
        .unwrap_or_else(|| DEFAULT_CONTRACT.to_owned());
    let contract: SourceContract = serde_json::from_str(
        &std::fs::read_to_string(&path).with_context(|| format!("failed to read {path}"))?,
    )?;
    let objects = selected_objects(&contract)?;
    let sample_size = optional_usize_env("FOUNDATION_PLATFORM_PARCEL_CHARACTERISTIC_SAMPLE_SIZE")?
        .unwrap_or(1000);
    if !(1..=100_000).contains(&sample_size) {
        bail!("land_characteristic_sample_size: expected 1..=100000");
    }
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
            .with_context(|| format!("failed to read characteristic handoff {key}"))?;
        let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
        let (rows, sample, skipped) =
            stage_rows(&mut transaction, reader, object, sample_size).await?;
        validate_mapping(&sample)?;
        let price_check = compare_prices(&mut transaction, &sample).await?;
        price_check.validate()?;
        if price_check.comparable == 0 {
            tracing::warn!(object = %key, absent = price_check.absent, different_vintage = price_check.different_vintage,
                "land_characteristic_price_check_skipped_no_overlapping_vintage");
        }
        let changed = merge_stage(&mut transaction).await?;
        merged += changed;
        tracing::info!(object = %key, rows, skipped, sample_rows = sample.len(),
            comparable = price_check.comparable, mismatched = price_check.mismatched, changed,
            "land_characteristic_object_verified");
    }
    transaction.commit().await?;
    tracing::info!(merged, "parcel-characteristic-projection-load-ok");
    Ok(())
}

fn selected_objects(contract: &SourceContract) -> anyhow::Result<Vec<&SourceObject>> {
    if contract.coordinator_fills_real_inventory {
        bail!(
            "land_characteristic_inventory_unmeasured: coordinator inventory replacement required"
        );
    }
    if contract.schema_version != 1 || contract.objects.is_empty() {
        bail!("land_characteristic_inventory_invalid: expected nonempty schema_version 1");
    }
    let mut keys = BTreeSet::new();
    for object in &contract.objects {
        parse_vintage(&object.vintage)?;
        if object.region_code.len() != 5
            || !object.region_code.bytes().all(|c| c.is_ascii_digit())
            || !object
                .object_key
                .starts_with("bronze/source=vworldkr__land_characteristic/")
            || !object.object_key.ends_with(".zip")
            || !keys.insert(&object.object_key)
        {
            bail!("land_characteristic_inventory_invalid: region, source, or duplicate object");
        }
    }
    // The measured inventory is the region SSOT. Do not copy its count into code.
    let regions: BTreeSet<&str> = contract
        .objects
        .iter()
        .map(|o| o.region_code.as_str())
        .collect();
    if regions.len() != contract.granularity_counts.sigungu || regions.is_empty() {
        bail!("land_characteristic_partial_country: inventory lost a measured region");
    }
    let vintages: BTreeSet<&str> = contract
        .objects
        .iter()
        .map(|o| o.vintage.as_str())
        .collect();
    let newest_complete = vintages.into_iter().rev().find(|vintage| {
        let objects: Vec<_> = contract
            .objects
            .iter()
            .filter(|o| o.vintage == *vintage)
            .collect();
        objects.len() == regions.len()
            && objects
                .iter()
                .map(|o| o.region_code.as_str())
                .collect::<BTreeSet<_>>()
                == regions
    });
    if newest_complete != Some(contract.selected_vintage.as_str()) {
        bail!("land_characteristic_partial_country: selected vintage is not the newest complete region covering");
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
        bail!("land_characteristic_handoff_contract: missing prefix or suffix");
    }
    let base = object
        .object_key
        .rsplit('/')
        .next()
        .and_then(|s| s.strip_suffix(".zip"))
        .context("land_characteristic_handoff_contract: invalid ZIP key")?;
    Ok(format!(
        "{}/{base}{}",
        contract.handoff_prefix, contract.handoff_suffix
    ))
}

fn parse_vintage(value: &str) -> anyhow::Result<NaiveDate> {
    if value.len() != 8 || !value.bytes().all(|c| c.is_ascii_digit()) {
        bail!("land_characteristic_invalid_vintage: expected YYYYMMDD");
    }
    NaiveDate::parse_from_str(value, "%Y%m%d").context("land_characteristic_invalid_vintage")
}

fn valid_area(row: &HandoffRow) -> bool {
    row.area_m2.is_some_and(|a| a.is_finite() && a > 0.0)
}

fn validate_mapping(sample: &[HandoffRow]) -> anyhow::Result<()> {
    if sample.is_empty() {
        bail!("land_characteristic_mapping_sample_empty");
    }
    for (name, valid, minimum) in [
        (
            "pnu",
            sample.iter().filter(|r| Pnu::parse(&r.pnu).is_ok()).count(),
            99,
        ),
        ("area", sample.iter().filter(|r| valid_area(r)).count(), 99),
        (
            "land_category",
            sample
                .iter()
                .filter(|r| {
                    r.land_category
                        .as_deref()
                        .is_some_and(|v| LAND_CATEGORIES.contains(&v))
                })
                .count(),
            95,
        ),
    ] {
        if valid * 100 < sample.len() * minimum {
            bail!(
                "land_characteristic_mapping_{name}_failed: {valid}/{} below {minimum}%",
                sample.len()
            );
        }
    }
    Ok(())
}

fn assessment(row: &HandoffRow) -> anyhow::Result<(i16, i16, i64)> {
    let year: i16 = row
        .base_year
        .trim()
        .parse()
        .context("land_characteristic_invalid_base_year")?;
    let month: i16 = row
        .base_month
        .trim()
        .parse()
        .context("land_characteristic_invalid_base_month")?;
    let price: i64 = row
        .verification_price_per_m2
        .trim()
        .parse()
        .context("land_characteristic_invalid_verification_price")?;
    if year <= 0 || !(1..=12).contains(&month) || price < 0 {
        bail!("land_characteristic_invalid_assessment: expected positive year, month 1..12, nonnegative price");
    }
    Ok((year, month, price))
}

#[derive(Default)]
struct PriceCheck {
    comparable: u64,
    mismatched: u64,
    absent: u64,
    different_vintage: u64,
}

impl PriceCheck {
    fn observe(&mut self, source: (i16, i16, i64), price: Option<(i16, i16, i64)>) {
        match price {
            None => self.absent += 1,
            Some((year, month, _)) if (year, month) != (source.0, source.1) => {
                self.different_vintage += 1
            }
            Some((_, _, value)) => {
                self.comparable += 1;
                self.mismatched += u64::from(value != source.2);
            }
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.mismatched * 100 > self.comparable {
            bail!(
                "land_characteristic_price_mapping_mismatch: {}/{} exceeds 1%",
                self.mismatched,
                self.comparable
            );
        }
        Ok(())
    }
}

async fn compare_prices(
    conn: &mut PgConnection,
    sample: &[HandoffRow],
) -> anyhow::Result<PriceCheck> {
    let mut check = PriceCheck::default();
    for row in sample {
        if Pnu::parse(&row.pnu).is_err() {
            continue;
        }
        let source = assessment(row)?;
        let price = sqlx::query_as::<_, (i16, i16, i64)>(
            "SELECT base_year, base_month, price_per_m2 FROM catalog.parcel_price WHERE pnu = $1::character(19)"
        ).bind(&row.pnu).fetch_optional(&mut *conn).await?;
        check.observe(source, price);
    }
    Ok(check)
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE parcel_characteristic_projection_stage (
        pnu character(19) NOT NULL, land_category text, area_m2 numeric NOT NULL,
        land_use_situation text, terrain_height text, terrain_shape text, road_contact text,
        source_snapshot_id text NOT NULL, source_vintage date NOT NULL
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
    sample_size: usize,
) -> anyhow::Result<(u64, Vec<HandoffRow>, u64)> {
    conn.execute("TRUNCATE parcel_characteristic_projection_stage")
        .await?;
    let mut copy = conn.copy_in_raw("COPY parcel_characteristic_projection_stage
        (pnu, land_category, area_m2, land_use_situation, terrain_height, terrain_shape, road_contact,
         source_snapshot_id, source_vintage) FROM STDIN WITH (FORMAT text)").await?;
    let mut buffer = String::with_capacity(8 * 1024 * 1024);
    let mut sample = BTreeMap::new();
    let mut rows = 0_u64;
    let mut skipped = 0_u64;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffRow =
            serde_json::from_str(&line).context("land_characteristic_invalid_handoff")?;
        rows += 1;
        if row.source_record_id != object.object_key
            || row.source_vintage != object.vintage
            || row.source_snapshot_id.trim().is_empty()
        {
            bail!("land_characteristic_lineage_mismatch");
        }
        // Keep the lowest stable hashes across the whole object, including invalid rows.
        // Taking only its prefix would miss a shifted/corrupt tail in sorted DBF data.
        let mut hasher = Sha256::new();
        hasher.update(row.pnu.as_bytes());
        hasher.update(rows.to_le_bytes());
        let score: [u8; 32] = hasher.finalize().into();
        if sample.len() < sample_size
            || sample
                .last_key_value()
                .is_some_and(|(last, _)| &(score, rows) < last)
        {
            sample.insert((score, rows), row.clone());
            if sample.len() > sample_size {
                sample.pop_last();
            }
        }
        let Ok(pnu) = Pnu::parse(&row.pnu) else {
            skipped += 1;
            continue;
        };
        if !valid_area(&row) {
            skipped += 1;
            continue;
        }
        if !pnu.as_str().starts_with(&object.region_code) {
            bail!("land_characteristic_region_mismatch");
        }
        assessment(&row)?;
        let area = row
            .area_m2
            .context("land_characteristic_invalid_area")?
            .to_string();
        let vintage = parse_vintage(&row.source_vintage)?.to_string();
        let fields = [
            Some(row.pnu.as_str()),
            row.land_category.as_deref(),
            Some(area.as_str()),
            row.land_use_situation.as_deref(),
            row.terrain_height.as_deref(),
            row.terrain_shape.as_deref(),
            row.road_contact.as_deref(),
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
    let staged = copy.finish().await?;
    if rows == 0 || staged != rows - skipped {
        bail!("land_characteristic_stage_count_mismatch: read={rows} skipped={skipped} staged={staged}");
    }
    Ok((rows, sample.into_values().collect(), skipped))
}

async fn merge_stage(conn: &mut PgConnection) -> anyhow::Result<u64> {
    Ok(sqlx::query("INSERT INTO catalog.parcel_characteristic
        (pnu, land_category, area_m2, land_use_situation, terrain_height, terrain_shape, road_contact,
         source_snapshot_id, source_vintage)
        SELECT DISTINCT ON (pnu) pnu, land_category, area_m2, land_use_situation, terrain_height,
            terrain_shape, road_contact, source_snapshot_id, source_vintage
        FROM parcel_characteristic_projection_stage AS stage
        ORDER BY pnu, source_vintage DESC, md5(row(stage.*)::text)
        ON CONFLICT (pnu) DO UPDATE SET
            land_category = EXCLUDED.land_category, area_m2 = EXCLUDED.area_m2,
            land_use_situation = EXCLUDED.land_use_situation, terrain_height = EXCLUDED.terrain_height,
            terrain_shape = EXCLUDED.terrain_shape, road_contact = EXCLUDED.road_contact,
            source_snapshot_id = EXCLUDED.source_snapshot_id, source_vintage = EXCLUDED.source_vintage,
            loaded_at = now()
        WHERE EXCLUDED.source_vintage > catalog.parcel_characteristic.source_vintage")
        .execute(conn).await?.rows_affected())
}

#[cfg(test)]
#[path = "parcel_characteristic_projection_tests.rs"]
mod tests;
