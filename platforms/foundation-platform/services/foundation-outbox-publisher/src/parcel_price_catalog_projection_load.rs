//! Loads per-parcel official land price assessments from the D151 handoffs into
//! `catalog.parcel_price` (root ADR-0085 §2).
//!
//! One parcel keeps one row: the newest (base_year, base_month) assessment within the
//! contract's selected vintage. History stays in Silver; this table answers the panel's one
//! question. A row whose numeric fields do not parse is counted per field and skipped, never
//! silently dropped and never a reason to abort a province.
//!
//! **Staged, then merged.** Same shape as `parcel_zoning_catalog_projection_load`: rows land
//! in a session-temporary stage through `COPY`, one handoff object at a time, and the merge
//! keeps the newest assessment per parcel. `ON CONFLICT DO NOTHING` makes a repeat run a
//! no-op.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};

use anyhow::{bail, Context};
use foundation_outbox::R2ObjectStorage;
use serde::Deserialize;
use sqlx::{Connection, Executor, PgConnection};

use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PRICE_PROJECTION_LOAD_CONFIRM";
const CONTRACT_ENV: &str = "LAND_INDIVIDUAL_PRICE_SOURCE_CONTRACT";
const DEFAULT_CONTRACT: &str =
    "infra/lakehouse/contracts/vworld-land-individual-price-source-objects.json";
const SOURCE_CONTRACT_SCHEMA_VERSION: u32 = 1;

/// One D151 handoff row, of which this load reads six fields.
#[derive(Debug, Deserialize)]
struct PriceHandoffRow {
    pnu: String,
    base_year: String,
    base_month: String,
    price_per_m2: String,
    announced_date: Option<String>,
    source_snapshot_id: String,
}

#[derive(Debug, Deserialize)]
struct PriceSourceContract {
    schema_version: u32,
    selected_vintage: String,
    handoff_prefix: String,
    handoff_suffix: String,
    objects: Vec<PriceSourceObject>,
}

#[derive(Debug, Deserialize)]
struct PriceSourceObject {
    object_key: String,
    region_code: String,
    vintage: String,
}

/// Runs the projection load.
///
/// # Errors
/// Returns an error when configuration, the contract, a handoff read, or the merge fails.
pub async fn run() -> anyhow::Result<()> {
    let confirmed = optional_bool_env(CONFIRM_ENV)?.unwrap_or(false);
    if !confirmed {
        bail!("{CONFIRM_ENV}=true is required: this command writes catalog.parcel_price");
    }
    let database_url = required_env_value("DATABASE_URL")?;
    let contract: PriceSourceContract = read_contract(
        &optional_env_value(CONTRACT_ENV)?.unwrap_or_else(|| DEFAULT_CONTRACT.to_owned()),
    )?;
    if contract.schema_version != SOURCE_CONTRACT_SCHEMA_VERSION {
        bail!("the source object contract is not the schema_version this command reads");
    }
    let keys = price_handoff_keys(&contract)?;

    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for the price projection load")?;
    let mut conn = PgConnection::connect(&database_url)
        .await
        .context("failed to connect to the catalog database")?;
    prepare_stage(&mut conn).await?;

    let mut totals = LoadTotals::default();
    for key in &keys {
        let report = load_one(&storage, &mut conn, key).await?;
        tracing::info!(
            object = %key,
            rows_read = report.rows_read,
            rows_staged = report.rows_staged,
            unparsable_skipped = report.unparsable_skipped,
            inserted = report.inserted,
            "price projection object merged"
        );
        totals.add(&report);
    }

    let table_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.parcel_price")
        .fetch_one(&mut conn)
        .await
        .context("failed to count catalog.parcel_price")?;
    let mut unparsable: Vec<(String, u64)> = totals.unparsable_by_field.into_iter().collect();
    unparsable.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    tracing::info!(
        objects = keys.len(),
        rows_read = totals.rows_read,
        rows_staged = totals.rows_staged,
        unparsable_skipped = totals.unparsable_skipped,
        inserted = totals.inserted,
        table_rows,
        unparsable_by_field = ?unparsable,
        "parcel-price-projection-load-ok"
    );
    Ok(())
}

fn read_contract<T: serde::de::DeserializeOwned>(path: &str) -> anyhow::Result<T> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read source contract {path}"))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse source contract {path}"))
}

/// The selected vintage's handoff keys, refused unless it covers exactly the 17 provinces
/// (ADR-0082's subset-refusal, the same rule the zoning projection applies).
fn price_handoff_keys(contract: &PriceSourceContract) -> anyhow::Result<Vec<String>> {
    let picked: Vec<&PriceSourceObject> = contract
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
            let file = object
                .object_key
                .rsplit('/')
                .next()
                .unwrap_or(&object.object_key);
            let base = file.strip_suffix(".zip").unwrap_or(file);
            format!(
                "{}/{base}{}",
                contract.handoff_prefix, contract.handoff_suffix
            )
        })
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

#[derive(Debug, Default)]
struct LoadTotals {
    rows_read: u64,
    rows_staged: u64,
    unparsable_skipped: u64,
    inserted: u64,
    unparsable_by_field: BTreeMap<String, u64>,
}

impl LoadTotals {
    fn add(&mut self, report: &ObjectReport) {
        self.rows_read += report.rows_read;
        self.rows_staged += report.rows_staged;
        self.unparsable_skipped += report.unparsable_skipped;
        self.inserted += report.inserted;
        for (field, count) in &report.unparsable_by_field {
            *self.unparsable_by_field.entry(field.clone()).or_insert(0) += count;
        }
    }
}

#[derive(Debug, Default)]
struct ObjectReport {
    rows_read: u64,
    rows_staged: u64,
    unparsable_skipped: u64,
    inserted: u64,
    unparsable_by_field: BTreeMap<String, u64>,
}

/// The numeric shape a staged row must have; the source carries these as strings and the
/// stage columns are typed, so the parse happens here where the failure can be counted.
fn parse_assessment(row: &PriceHandoffRow) -> Result<(i16, i16, i64), &'static str> {
    let year: i16 = row.base_year.trim().parse().map_err(|_| "base_year")?;
    let month: i16 = row.base_month.trim().parse().map_err(|_| "base_month")?;
    let price: i64 = row
        .price_per_m2
        .trim()
        .parse()
        .map_err(|_| "price_per_m2")?;
    if !(1..=12).contains(&month) {
        return Err("base_month");
    }
    if year <= 0 || price < 0 {
        return Err(if year <= 0 {
            "base_year"
        } else {
            "price_per_m2"
        });
    }
    Ok((year, month, price))
}

async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_price_projection_stage (
             pnu text NOT NULL,
             price_per_m2 bigint NOT NULL,
             base_year smallint NOT NULL,
             base_month smallint NOT NULL,
             announced_date text,
             source_snapshot_id text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create the parcel price projection stage")?;
    Ok(())
}

async fn load_one(
    storage: &R2ObjectStorage,
    conn: &mut PgConnection,
    key: &str,
) -> anyhow::Result<ObjectReport> {
    let bytes = storage
        .get_object_bytes_range_retried(key)
        .await
        .with_context(|| format!("failed to read price handoff object {key}"))?;

    conn.execute("TRUNCATE TABLE parcel_price_projection_stage")
        .await
        .context("failed to truncate the parcel price projection stage")?;
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_price_projection_stage \
             (pnu, price_per_m2, base_year, base_month, announced_date, source_snapshot_id) \
             FROM STDIN WITH (FORMAT text)",
        )
        .await
        .context("failed to start COPY into the parcel price projection stage")?;

    let mut report = ObjectReport::default();
    let mut buffer = String::with_capacity(8 * 1024 * 1024);
    let reader = BufReader::new(flate2::read::GzDecoder::new(&bytes[..]));
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read {key} line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        report.rows_read += 1;
        let row: PriceHandoffRow = serde_json::from_str(&line)
            .with_context(|| format!("price handoff {key} line {} is not a row", index + 1))?;
        let (year, month, price) = match parse_assessment(&row) {
            Ok(parsed) => parsed,
            Err(field) => {
                report.unparsable_skipped += 1;
                *report
                    .unparsable_by_field
                    .entry(field.to_owned())
                    .or_insert(0) += 1;
                continue;
            }
        };

        buffer.push_str(&row.pnu);
        buffer.push('\t');
        buffer.push_str(&price.to_string());
        buffer.push('\t');
        buffer.push_str(&year.to_string());
        buffer.push('\t');
        buffer.push_str(&month.to_string());
        buffer.push('\t');
        match &row.announced_date {
            Some(date) if !date.trim().is_empty() => buffer.push_str(date.trim()),
            _ => buffer.push_str("\\N"),
        }
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
        bail!("price handoff {key} carried no rows, which is not a state this dataset has");
    }

    // One assessment per parcel: the newest (base_year, base_month) wins; on a full tie the
    // higher price is a deterministic pick among duplicates of the same assessment.
    let inserted = sqlx::query(
        "INSERT INTO catalog.parcel_price \
             (pnu, price_per_m2, base_year, base_month, announced_date, source_snapshot_id)
         SELECT DISTINCT ON (pnu)
                pnu, price_per_m2, base_year, base_month, announced_date, source_snapshot_id
         FROM parcel_price_projection_stage
         ORDER BY pnu, base_year DESC, base_month DESC, price_per_m2 DESC
         ON CONFLICT (pnu) DO NOTHING",
    )
    .execute(&mut *conn)
    .await
    .context("failed to merge the parcel price projection stage")?
    .rows_affected();
    report.inserted = inserted;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(year: &str, month: &str, price: &str) -> PriceHandoffRow {
        PriceHandoffRow {
            pnu: "9999938029104450003".to_owned(),
            base_year: year.to_owned(),
            base_month: month.to_owned(),
            price_per_m2: price.to_owned(),
            announced_date: None,
            source_snapshot_id: "vworldkr-land-individual-price:test".to_owned(),
        }
    }

    #[test]
    fn a_numeric_assessment_parses() {
        assert_eq!(
            parse_assessment(&row("2026", "1", "81700")),
            Ok((2026, 1, 81700))
        );
    }

    #[test]
    fn each_broken_field_is_named() {
        assert_eq!(
            parse_assessment(&row("년도", "1", "81700")),
            Err("base_year")
        );
        assert_eq!(
            parse_assessment(&row("2026", "13", "81700")),
            Err("base_month")
        );
        assert_eq!(
            parse_assessment(&row("2026", "1", "-5")),
            Err("price_per_m2")
        );
    }

    #[test]
    fn the_vintage_selection_refuses_a_partial_country() {
        let contract = PriceSourceContract {
            schema_version: 1,
            selected_vintage: "20260526".to_owned(),
            handoff_prefix: "silver-handoff/vworldkr__land_individual_price".to_owned(),
            handoff_suffix: ".jsonl.gz".to_owned(),
            objects: vec![PriceSourceObject {
                object_key: "bronze/source=vworldkr__land_individual_price/x-1.zip".to_owned(),
                region_code: "36".to_owned(),
                vintage: "20260526".to_owned(),
            }],
        };
        let error = price_handoff_keys(&contract).expect_err("one province must refuse");
        assert!(error.to_string().contains("17"), "{error}");
    }
}
