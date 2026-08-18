//! Loads `gold.complex_catalog` into the canonical `catalog.industrial_complex` table.
//!
//! This is the producer that table did not have. `GET /catalog/v1/complexes` reads
//! `catalog.industrial_complex` directly, so until something wrote the sourced complexes into it
//! the endpoint served whatever seed rows happened to exist — six of them, against 1,442 rows
//! sitting in Gold. Nothing was missing from the lakehouse; the last hop had no code (root
//! ADR-0040).
//!
//! What it writes and what it deliberately does not:
//!
//! - `official_complex_code` is the natural key. A code already in the table updates that row in
//!   place, keeping its `id`, because nine foreign keys point at that `id`. Nothing is deleted.
//! - Everything the profile source says about the complex — address, both lifecycle columns, the
//!   three dates, the managing and developing organizations, the two administrative codes, the
//!   site-formation progress, the business period, and the four free-text descriptions — is loaded
//!   from the same Gold row as the identity fields. Each reached Gold only once the projection
//!   stopped dropping it; before that this command recorded it in its summary as a column it could
//!   not load.
//! - `lakehouse_complex_id` carries the Gold row's own `complex_id`. That id is what Gold artifact
//!   object keys are derived from, and the canonical `id` is minted locally, so a row without it
//!   can name no object in R2.
//! - `primary_bjdong_code` is always `NULL`. Silver declares the column and zero of its rows fill
//!   it, so the Gold projection does not carry it at all; a canonical row must not claim an
//!   administrative code no source ever stated.
//! - A value the Gold row does not carry is written as `NULL`. Not `""`, not a zero, not the value
//!   from a neighbouring row: a complex whose source states no completion date has no completion
//!   date, and the canonical row says so.

use std::{env, sync::Arc};

use anyhow::{bail, ensure, Context};
use catalog_application::ports::{UpsertIndustrialComplexEffect, UpsertIndustrialComplexOutcome};
use catalog_application::{
    IndustrialComplexCatalogRow, UpsertIndustrialComplexCatalogRows,
    UpsertIndustrialComplexCatalogRowsInput,
};
use catalog_domain::{
    IndustrialComplexKind, IndustrialComplexLotSalesStatus, IndustrialComplexStatus,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use chrono::{NaiveDate, SecondsFormat, Utc};
use foundation_shared_kernel::ids::LakehouseComplexId;
use lakehouse_domain::GOLD_COMPLEX_CATALOG;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
use crate::public_data_control_support::required_env_value;

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_canonical_load_summary.v1";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_CONFIRM";
const EXPECTED_ROW_COUNT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_EXPECTED_ROW_COUNT";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_SUMMARY_PATH";

/// Runs the Gold-to-canonical industrial-complex load.
pub async fn run() -> anyhow::Result<()> {
    let config = CanonicalLoadConfig::from_env()?;

    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(GOLD_COMPLEX_CATALOG.table_name)
        .await
        .context("failed to resolve the Gold catalog snapshot")?
        .with_context(|| {
            format!(
                "{} has no current Iceberg snapshot to load",
                GOLD_COMPLEX_CATALOG.table_name
            )
        })?;

    let lakehouse = LakehouseObjectReader::from_env()?;
    let scanned = scan_snapshot_rows(&GOLD_COMPLEX_CATALOG, &lakehouse, &snapshot).await?;
    let scanned_row_count =
        u64::try_from(scanned.rows.len()).context("scanned row count overflow")?;
    if let Some(expected_row_count) = config.expected_row_count {
        ensure!(
            expected_row_count == scanned_row_count,
            "{} snapshot {} holds {scanned_row_count} rows but {expected_row_count} were expected",
            snapshot.table_name,
            snapshot.snapshot_id
        );
    }
    ensure!(
        scanned_row_count == scanned.manifest_record_count,
        "scanned {scanned_row_count} rows but the manifests declared {} rows",
        scanned.manifest_record_count
    );

    let plan = plan_catalog_rows(&scanned.rows)?;

    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to the database for the industrial-complex canonical load")?;
    let use_case =
        UpsertIndustrialComplexCatalogRows::new(Arc::new(PgCatalogUnitOfWork::new(pool)));
    let report = use_case
        .execute(UpsertIndustrialComplexCatalogRowsInput {
            rows: plan.rows.clone(),
        })
        .await
        .context("failed to write industrial-complex rows into the canonical Catalog table")?;

    let summary = CanonicalLoadSummary::build(
        &snapshot.table_name,
        &snapshot.snapshot_id.to_string(),
        &snapshot.metadata_location,
        &snapshot.manifest_list_location,
        scanned.data_file_count,
        scanned_row_count,
        &plan,
        &report.outcomes,
    )?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, &summary)?;
    }

    if summary.skipped_row_count > 0 {
        tracing::warn!(
            skipped_row_count = summary.skipped_row_count,
            "Gold rows were not loaded because no canonical area could be derived without inventing one"
        );
    }

    tracing::info!(
        gold_table = %summary.gold_table,
        gold_iceberg_snapshot_id = %summary.gold_iceberg_snapshot_id,
        scanned_row_count = summary.scanned_row_count,
        inserted_row_count = summary.inserted_row_count,
        updated_row_count = summary.updated_row_count,
        unchanged_row_count = summary.unchanged_row_count,
        skipped_row_count = summary.skipped_row_count,
        "industrial-complex canonical load succeeded"
    );
    Ok(())
}

struct CanonicalLoadConfig {
    database_url: String,
    expected_row_count: Option<u64>,
    summary_path: Option<PathBuf>,
}

impl CanonicalLoadConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env(CONFIRM_ENV)?.unwrap_or_default();
        ensure!(
            confirm.eq_ignore_ascii_case("true"),
            "{CONFIRM_ENV} must be true"
        );
        Ok(Self {
            database_url: required_env_value(DATABASE_URL_ENV)?,
            expected_row_count: optional_env(EXPECTED_ROW_COUNT_ENV)?
                .map(|value| parse_positive_u64(value.as_str(), EXPECTED_ROW_COUNT_ENV))
                .transpose()?,
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
        })
    }
}

/// One Gold row the load refused to write, and why.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SkippedRow {
    official_complex_code: String,
    reason: &'static str,
}

/// What the scan turned into: rows to write, and rows it would have had to invent a value for.
struct CanonicalLoadPlan {
    rows: Vec<IndustrialComplexCatalogRow>,
    skipped: Vec<SkippedRow>,
}

#[derive(Debug, Serialize)]
struct CanonicalLoadSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    load_run_id: Uuid,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    gold_metadata_location: String,
    gold_manifest_list_location: String,
    data_file_count: u64,
    scanned_row_count: u64,
    planned_row_count: u64,
    inserted_row_count: u64,
    updated_row_count: u64,
    unchanged_row_count: u64,
    skipped_row_count: u64,
    skipped_rows: Vec<SkippedRow>,
    /// Canonical columns this load leaves `NULL` because the Gold projection has no such column.
    ///
    /// Only `primary_bjdong_code`, and it is not a gap this command can close: zero Silver rows
    /// carry a legal-dong code, so the Gold projection declares no such column to read.
    columns_left_null: &'static [&'static str],
    /// Gold columns this load does not write, because `catalog.industrial_complex` has no column
    /// for them. Recorded so a reader of this summary sees what the API still cannot serve.
    gold_columns_not_loaded: Vec<&'static str>,
}

const COLUMNS_LEFT_NULL: &[&str] = &["primary_bjdong_code"];

/// Gold columns [`plan_catalog_rows`] reads into a canonical row.
///
/// The summary reports the *complement* of this against the Gold contract rather than a second
/// hand-written list. A column added to `gold.complex_catalog` and not read here then shows up as
/// unloaded on the next run; a hand-written list would have kept reporting the old answer, which is
/// how this command described `address_text` as unloadable for as long as it was true and no longer.
const GOLD_COLUMNS_LOADED: &[&str] = &[
    "complex_id",
    "official_complex_code",
    "name",
    "kind",
    "status",
    "sido_code",
    "sigungu_code",
    "address_text",
    "management_agency_name",
    "developer_name",
    "designated_date",
    "construction_start_date",
    "completion_date",
    "official_area_sqm",
    "development_progress_percent",
    "lot_sales_status",
    "business_period_raw",
    "business_period_start_month",
    "business_period_end_month",
    "designation_basis_law_raw",
    "development_method_raw",
    "development_purpose_raw",
    "invited_industries_raw",
];

/// Gold contract columns this load does not consume.
fn gold_columns_not_loaded() -> Vec<&'static str> {
    GOLD_COMPLEX_CATALOG
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !GOLD_COLUMNS_LOADED.contains(name))
        .collect()
}

impl CanonicalLoadSummary {
    #[allow(clippy::too_many_arguments)]
    fn build(
        gold_table: &str,
        gold_iceberg_snapshot_id: &str,
        gold_metadata_location: &str,
        gold_manifest_list_location: &str,
        data_file_count: u64,
        scanned_row_count: u64,
        plan: &CanonicalLoadPlan,
        outcomes: &[UpsertIndustrialComplexOutcome],
    ) -> anyhow::Result<Self> {
        Ok(Self {
            schema_version: SUMMARY_SCHEMA_VERSION,
            generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            load_run_id: Uuid::now_v7(),
            gold_table: gold_table.to_owned(),
            gold_iceberg_snapshot_id: gold_iceberg_snapshot_id.to_owned(),
            gold_metadata_location: gold_metadata_location.to_owned(),
            gold_manifest_list_location: gold_manifest_list_location.to_owned(),
            data_file_count,
            scanned_row_count,
            planned_row_count: u64::try_from(plan.rows.len())
                .context("planned row count overflow")?,
            inserted_row_count: count_effect(outcomes, UpsertIndustrialComplexEffect::Inserted)?,
            updated_row_count: count_effect(outcomes, UpsertIndustrialComplexEffect::Updated)?,
            unchanged_row_count: count_effect(outcomes, UpsertIndustrialComplexEffect::Unchanged)?,
            skipped_row_count: u64::try_from(plan.skipped.len())
                .context("skipped row count overflow")?,
            skipped_rows: plan.skipped.clone(),
            columns_left_null: COLUMNS_LEFT_NULL,
            gold_columns_not_loaded: gold_columns_not_loaded(),
        })
    }
}

fn count_effect(
    outcomes: &[UpsertIndustrialComplexOutcome],
    effect: UpsertIndustrialComplexEffect,
) -> anyhow::Result<u64> {
    u64::try_from(
        outcomes
            .iter()
            .filter(|outcome| outcome.effect == effect)
            .count(),
    )
    .context("upsert effect tally overflow")
}

/// Turns scanned Gold rows into canonical rows, refusing to guess a value the source lacks.
fn plan_catalog_rows(rows: &[JsonMap<String, JsonValue>]) -> anyhow::Result<CanonicalLoadPlan> {
    let mut planned = Vec::with_capacity(rows.len());
    let mut skipped = Vec::new();
    let mut seen_codes = BTreeSet::new();

    for row in rows {
        let official_complex_code = required_row_string(row, "official_complex_code")?;
        ensure!(
            seen_codes.insert(official_complex_code.clone()),
            "Gold snapshot carries more than one row for official_complex_code {official_complex_code}"
        );
        let lakehouse_complex_id = LakehouseComplexId::new(
            Uuid::parse_str(required_row_string(row, "complex_id")?.as_str()).with_context(
                || format!("Gold row {official_complex_code} carries an unreadable complex_id"),
            )?,
        );
        let name = required_row_string(row, "name")?;
        // A classification outside the contract is a broken contract, not a gap in the source, so
        // it fails the whole load instead of being hidden as one skipped row.
        let kind_wire = required_row_string(row, "kind")?;
        let kind = IndustrialComplexKind::from_wire(kind_wire.as_str()).map_err(|error| {
            anyhow::anyhow!("Gold row {official_complex_code} carries an unknown kind: {error}")
        })?;

        // A lifecycle value outside the contract is the same class of defect as an unknown kind:
        // the projection broke its own value domain, so it fails the load rather than being
        // written as `NULL`, which would spell "the source said nothing".
        let status = optional_row_string(row, "status")
            .map(|wire| IndustrialComplexStatus::from_wire(wire.as_str()))
            .transpose()
            .map_err(|error| {
                anyhow::anyhow!(
                    "Gold row {official_complex_code} carries an unknown status: {error}"
                )
            })?;

        // Same treatment for the lot-sales domain, and for the same reason: a value outside it did
        // not come from the source's three labels, so the projection broke its own value domain.
        let lot_sales_status = optional_row_string(row, "lot_sales_status")
            .map(|wire| IndustrialComplexLotSalesStatus::from_wire(wire.as_str()))
            .transpose()
            .map_err(|error| {
                anyhow::anyhow!(
                    "Gold row {official_complex_code} carries an unknown lot_sales_status: {error}"
                )
            })?;

        match canonical_area_m2(row.get("official_area_sqm")) {
            Ok(area_m2) => planned.push(IndustrialComplexCatalogRow {
                lakehouse_complex_id: Some(lakehouse_complex_id),
                official_complex_code,
                name,
                kind,
                // gold.complex_catalog has no legal-dong column; see the module comment.
                primary_bjdong_code: None,
                area_m2,
                status,
                sido_code: optional_row_string(row, "sido_code"),
                sigungu_code: optional_row_string(row, "sigungu_code"),
                address_text: optional_row_string(row, "address_text"),
                management_agency_name: optional_row_string(row, "management_agency_name"),
                developer_name: optional_row_string(row, "developer_name"),
                designated_date: optional_row_date(row, "designated_date")?,
                construction_start_date: optional_row_date(row, "construction_start_date")?,
                completion_date: optional_row_date(row, "completion_date")?,
                // `iceberg_scan` renders a Gold decimal as exact base-10 text, which is exactly the
                // shape the canonical `numeric(5,2)` column takes. No float appears on this path.
                development_progress_percent: optional_row_string(
                    row,
                    "development_progress_percent",
                ),
                lot_sales_status,
                business_period_raw: optional_row_string(row, "business_period_raw"),
                business_period_start_month: optional_row_string(
                    row,
                    "business_period_start_month",
                ),
                business_period_end_month: optional_row_string(row, "business_period_end_month"),
                designation_basis_law_raw: optional_row_string(row, "designation_basis_law_raw"),
                development_method_raw: optional_row_string(row, "development_method_raw"),
                development_purpose_raw: optional_row_string(row, "development_purpose_raw"),
                invited_industries_raw: optional_row_string(row, "invited_industries_raw"),
            }),
            Err(reason) => skipped.push(SkippedRow {
                official_complex_code,
                reason,
            }),
        }
    }

    Ok(CanonicalLoadPlan {
        rows: planned,
        skipped,
    })
}

/// Converts the Gold `decimal(18,2)` area into the canonical `bigint` column.
///
/// `iceberg_scan` renders a decimal as exact base-10 text, so this reads text rather than a float.
/// Rounding is not an option: `area_m2` would then carry a number no source ever stated.
fn canonical_area_m2(value: Option<&JsonValue>) -> Result<u64, &'static str> {
    let Some(raw) = value.and_then(JsonValue::as_str) else {
        return Err("official_area_sqm_missing");
    };
    let (integer_part, fraction_part) = raw.split_once('.').unwrap_or((raw, ""));
    if !fraction_part.is_empty() && fraction_part.bytes().any(|byte| byte != b'0') {
        return Err("official_area_sqm_not_whole");
    }
    if integer_part.starts_with('-') {
        return Err("official_area_sqm_negative");
    }
    integer_part
        .parse::<u64>()
        .map_err(|_| "official_area_sqm_unreadable")
}

fn required_row_string(row: &JsonMap<String, JsonValue>, name: &str) -> anyhow::Result<String> {
    row.get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("Gold catalog row is missing {name}"))
}

/// Reads an optional Gold string, treating a blank as absence.
///
/// The Gold quality gate rejects `""` in any string column, so a blank should never arrive. Folding
/// one to `None` anyway is what keeps a blank from becoming a canonical value if it ever does: the
/// canonical column would then claim the source stated an empty address.
fn optional_row_string(row: &JsonMap<String, JsonValue>, name: &str) -> Option<String> {
    row.get(name)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Reads an optional Gold `date`, which the snapshot scan renders as `YYYY-MM-DD`.
///
/// An unparseable date fails the load. It is not a missing value — the source stated something and
/// this command could not read it, and writing `NULL` would report that as "no date stated".
fn optional_row_date(
    row: &JsonMap<String, JsonValue>,
    name: &str,
) -> anyhow::Result<Option<NaiveDate>> {
    optional_row_string(row, name)
        .map(|raw| {
            NaiveDate::parse_from_str(raw.as_str(), "%Y-%m-%d")
                .with_context(|| format!("Gold catalog row carries an unreadable {name}: {raw}"))
        })
        .transpose()
}

fn write_summary(path: &Path, summary: &CanonicalLoadSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(summary)
        .context("failed to serialize the industrial-complex canonical load summary")?;
    std::fs::write(path, payload)
        .with_context(|| format!("failed to write the summary {}", path.display()))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_positive_u64(value: &str, name: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(parsed > 0, "{name} must be greater than zero");
    Ok(parsed)
}

#[cfg(test)]
mod tests;
