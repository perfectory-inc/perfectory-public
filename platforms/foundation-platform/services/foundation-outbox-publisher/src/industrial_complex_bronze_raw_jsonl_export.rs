//! Industrial-complex Bronze profile object to `bronze.industrial_complexes_raw_jsonl` producer.
//!
//! `infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py` has read this dataset since
//! it was written and nothing has ever produced it. This command is the writer: it decodes the
//! profile workbook out of its Bronze zip and emits one JSONL record per industrial complex.
//!
//! The profile source carries no administrative location, so the address resolution is an injected
//! input: without `..._ADDRESS_SOURCE_PATH` the command fails before it opens the Bronze object,
//! and a complex the resolution does not cover fails the whole run rather than being written with
//! a blank location. See root ADR-0033.

use std::{
    collections::BTreeMap,
    env, fs,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
};

mod address_source;
mod profile_workbook_decoder;

use anyhow::{bail, Context};
use chrono::Utc;
use lakehouse_application::{
    industrial_complex_bronze_raw_row_to_jsonl, industrial_complex_labels_measured_for,
    normalize_industrial_complex_bronze_raw_rows, IndustrialComplexBronzeRawRow,
    IndustrialComplexBronzeRawRowsInput, INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD,
};
use lakehouse_domain::BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL;
use zip::ZipArchive;

const DEFAULT_SOURCE_SLUG: &str = "vworldkr__sandan_profile";
const BRONZE_ROOT_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_BRONZE_ROOT";
const SOURCE_SLUG_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_SOURCE_SLUG";
const SOURCE_OBJECT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_SOURCE_OBJECT";
const SHEET_NAME_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_SHEET_NAME";
const OUTPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_OUTPUT_PATH";
const SUMMARY_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_SUMMARY_PATH";
const ADDRESS_SOURCE_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_ADDRESS_SOURCE_PATH";
const MAX_ROWS_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL_MAX_ROWS";

struct BronzeRawJsonlExportConfig {
    bronze_local_object_root: PathBuf,
    source_slug: String,
    /// Exact Bronze zip to decode; required once monthly snapshots accumulate under the prefix.
    source_object: Option<String>,
    /// Exact worksheet to decode; required once the workbook holds more than one sheet.
    sheet_name: Option<String>,
    address_source_path: PathBuf,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    max_rows: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BronzeRawJsonlExportReport {
    row_count: usize,
    address_resolution_count: usize,
    bronze_object_key: String,
    source_snapshot_id: String,
    /// `sta_ym` of the decoded table; the normalizer has already proven it is single-valued.
    snapshot_period: String,
    /// Whether the label tables were counted against [`Self::snapshot_period`].
    labels_measured_for_snapshot: bool,
    /// How many rows each resolution tier accounts for, keyed by tier wire name.
    ///
    /// One of the three tiers is a heuristic, so an export that could not say how many of its rows
    /// leaned on it would be reporting a location it cannot defend (root ADR-0034).
    resolution_tier_counts: BTreeMap<&'static str, u64>,
    /// How many rows each administrative granularity accounts for, keyed by granularity wire name.
    address_granularity_counts: BTreeMap<&'static str, u64>,
    /// Optional provider headers the worksheet did not carry, so their columns are null for every
    /// row of this export because the column was absent rather than because the cells were blank.
    absent_optional_headers: Vec<&'static str>,
    /// Entity-shaped tokens one unescaping pass left in the cells, and how many cells carry each.
    ///
    /// A reference outside the shared table, or one the provider escaped twice. Non-empty is not a
    /// failure and it is not a shrug either: this is where an operator reads what the export could
    /// not state the meaning of. See [`profile_workbook_decoder::DecodedProfileSheet`].
    residual_entity_references: BTreeMap<String, u64>,
}

/// Runs the industrial-complex Bronze profile to JSONL export.
///
/// # Errors
/// Returns an error when the address source is absent or incomplete, the Bronze object cannot be
/// decoded, or the output path already holds a previous export.
pub fn run() -> anyhow::Result<()> {
    let config = BronzeRawJsonlExportConfig::from_env()?;
    let report = export_bronze_raw_jsonl(&config)?;
    tracing::info!(
        contract = BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL,
        row_count = report.row_count,
        address_resolution_count = report.address_resolution_count,
        bronze_object_key = %report.bronze_object_key,
        source_snapshot_id = %report.source_snapshot_id,
        output_path = %config.output_path.display(),
        "industrial-complex Bronze JSONL export succeeded"
    );
    Ok(())
}

fn export_bronze_raw_jsonl(
    config: &BronzeRawJsonlExportConfig,
) -> anyhow::Result<BronzeRawJsonlExportReport> {
    // The injected address source is read first: a run that cannot prove where the complexes are
    // must not touch the Bronze object or the output path at all.
    let resolution = address_source::read_address_book(&config.address_source_path)?;
    let addresses = &resolution.book;
    let object_path = locate_source_object(config)?;
    let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, &object_path)?;
    let sheet = decode_profile_rows_from_bronze_zip(
        &object_path,
        config.sheet_name.as_deref(),
        config.max_rows,
    )
    .with_context(|| format!("failed to decode industrial-complex profile {bronze_object_key}"))?;
    let records = sheet.records;
    if !sheet.residual_entity_references.is_empty() {
        // Not a failure, for the same reason an absent optional header is not: the `202506`
        // snapshot really does carry twelve references the provider escaped twice, and refusing
        // them would cost all 1,442 rows to gain nothing. Unescaping a second time is the option
        // that is actually forbidden — it is what turns `&amp;lt;` into a tag the source never
        // wrote — so the export names what it left instead (root ADR-0050).
        tracing::warn!(
            residual_entity_references = ?sheet.residual_entity_references,
            "the industrial-complex profile worksheet carries entity references one unescaping \
             pass did not resolve; they reach the lakehouse exactly as one pass left them"
        );
    }
    if !sheet.absent_optional_headers.is_empty() {
        // Not a failure: an absent optional header costs one column, and failing would cost all
        // 1,442 rows. It is reported here and in the summary because a column of nulls looks
        // exactly like a provider that stopped filling the cells, and the two need different work.
        tracing::warn!(
            absent_optional_headers = ?sheet.absent_optional_headers,
            "the industrial-complex profile worksheet no longer carries every column this decoder \
             reads; those columns are null for every row of this export"
        );
    }

    let rows = normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
        records: &records,
        addresses,
        bronze_object_key: bronze_object_key.as_str(),
        source_slug: config.source_slug.as_str(),
        ingested_at_utc: Utc::now(),
    })
    .context("failed to build bronze.industrial_complexes_raw_jsonl rows")?;

    write_jsonl(&config.output_path, &rows)?;
    let source_snapshot_id = rows
        .first()
        .map(|row| row.source_snapshot_id.clone())
        .context("normalized zero rows")?;
    // Normalization already refused a table that mixes months, so any decoded row names the month.
    let snapshot_period = records
        .first()
        .map(|record| record.snapshot_period.trim().to_owned())
        .context("decoded zero rows")?;
    let labels_measured_for_snapshot = industrial_complex_labels_measured_for(&snapshot_period);
    if !labels_measured_for_snapshot {
        tracing::warn!(
            snapshot_period = %snapshot_period,
            measured_snapshot_period = INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD,
            "industrial-complex label tables were not counted for this snapshot month; \
             kind and status mappings for it are unconfirmed"
        );
    }

    let report = BronzeRawJsonlExportReport {
        row_count: rows.len(),
        address_resolution_count: addresses.len(),
        bronze_object_key,
        source_snapshot_id,
        snapshot_period,
        labels_measured_for_snapshot,
        resolution_tier_counts: resolution.tier_counts,
        address_granularity_counts: resolution.granularity_counts,
        absent_optional_headers: sheet.absent_optional_headers,
        residual_entity_references: sheet.residual_entity_references,
    };
    if let Some(summary_path) = &config.summary_path {
        write_summary(config, &rows, &report, summary_path)?;
    }
    Ok(report)
}

impl BronzeRawJsonlExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: PathBuf::from(required_env(BRONZE_ROOT_ENV)?),
            source_slug: optional_env(SOURCE_SLUG_ENV)?
                .unwrap_or_else(|| DEFAULT_SOURCE_SLUG.to_owned()),
            source_object: optional_env(SOURCE_OBJECT_ENV)?,
            sheet_name: optional_env(SHEET_NAME_ENV)?,
            address_source_path: PathBuf::from(required_env(ADDRESS_SOURCE_PATH_ENV).context(
                "an industrial complex without a sourced address is not representable, so this \
                 export refuses to run without an address resolution",
            )?),
            output_path: PathBuf::from(required_env(OUTPUT_PATH_ENV)?),
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
            max_rows: optional_usize_env(MAX_ROWS_ENV)?,
        })
    }
}

/// Chooses one zip from a prefix that may accumulate monthly snapshots.
/// A pin selects its exact object; without a pin, only a single zip is accepted.
fn locate_source_object(config: &BronzeRawJsonlExportConfig) -> anyhow::Result<PathBuf> {
    locate_single_bronze_object(
        &config.bronze_local_object_root,
        config.source_slug.as_str(),
        config.source_object.as_deref(),
        "zip",
    )
    .with_context(|| format!("set {SOURCE_OBJECT_ENV} to the exact object to export"))
}

/// Resolves exactly one Bronze object under `bronze/source={source_slug}/`, recursively.
///
/// Shared with the address-resolution builder, which has to reach the SAME profile object this
/// producer reads. Two locators would be free to disagree about which snapshot is current, and the
/// resolution would then describe a month the export never opened.
///
/// # Errors
/// Returns an error when the source prefix is absent, when a pin does not resolve to a file, when
/// no object with `extension` is found, or when more than one is and no pin was given.
pub(crate) fn locate_single_bronze_object(
    bronze_local_object_root: &Path,
    source_slug: &str,
    pinned_object: Option<&str>,
    extension: &str,
) -> anyhow::Result<PathBuf> {
    let source_root = bronze_local_object_root
        .join("bronze")
        .join(format!("source={source_slug}"));
    if !source_root.is_dir() {
        bail!(
            "Bronze source directory not found: {}",
            source_root.display()
        );
    }
    if let Some(object) = pinned_object {
        let pinned = source_root.join(object);
        if !pinned.is_file() {
            bail!("pinned Bronze object not found: {}", pinned.display());
        }
        return Ok(pinned);
    }
    let mut matches = Vec::new();
    collect_objects_with_extension(&source_root, extension, &mut matches)?;
    matches.sort();
    if matches.len() > 1 {
        bail!(
            "Bronze source {} holds {} .{extension} objects; the snapshot is ambiguous, so pin the \
             exact object",
            source_root.display(),
            matches.len()
        );
    }
    matches.into_iter().next().with_context(|| {
        format!(
            "no .{extension} Bronze object found in {}",
            source_root.display()
        )
    })
}

/// Collects every file with `extension` under `dir`, descending into the readable object-key
/// partition directories (`operation=…/scope=…`) the Bronze layout writes.
fn collect_objects_with_extension(
    dir: &Path,
    extension: &str,
    found: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let path = entry
            .with_context(|| format!("failed to read entry in {}", dir.display()))?
            .path();
        if path.is_dir() {
            collect_objects_with_extension(&path, extension, found)?;
        } else if path
            .extension()
            .is_some_and(|value| value.eq_ignore_ascii_case(extension))
        {
            found.push(path);
        }
    }
    Ok(())
}

/// Decodes the profile workbook out of a Bronze zip.
///
/// Shared with the address-resolution builder so the set of complexes that must be resolved is read
/// by the same decoder that later demands an address for each of them.
///
/// # Errors
/// Returns an error when the zip cannot be opened, does not hold exactly one workbook, or the
/// workbook cannot be decoded.
pub(crate) fn decode_profile_rows_from_bronze_zip(
    object_path: &Path,
    sheet_name: Option<&str>,
    max_rows: Option<usize>,
) -> anyhow::Result<profile_workbook_decoder::DecodedProfileSheet> {
    let workbook_bytes = read_single_workbook_entry(object_path)?;
    profile_workbook_decoder::decode_profile_rows(workbook_bytes, sheet_name, max_rows)
        .with_context(|| {
            format!(
                "failed to decode industrial-complex profile {}",
                object_path.display()
            )
        })
}

/// Reads the single workbook entry out of the Bronze zip.
fn read_single_workbook_entry(object_path: &Path) -> anyhow::Result<Vec<u8>> {
    let file = fs::File::open(object_path).with_context(|| {
        format!(
            "failed to open industrial-complex profile Bronze zip {}",
            object_path.display()
        )
    })?;
    let mut archive = ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read industrial-complex profile Bronze zip {}",
            object_path.display()
        )
    })?;

    let mut workbook_indexes = Vec::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("failed to inspect zip entry {index}"))?;
        if entry.is_dir() {
            continue;
        }
        if entry.name().to_ascii_lowercase().ends_with(".xlsx") {
            workbook_indexes.push(index);
        }
    }
    let [workbook_index] = workbook_indexes.as_slice() else {
        bail!(
            "industrial-complex profile Bronze zip must contain one .xlsx entry, found {}",
            workbook_indexes.len()
        );
    };

    let mut entry = archive
        .by_index(*workbook_index)
        .with_context(|| format!("failed to open zip entry {workbook_index}"))?;
    let mut bytes = Vec::new();
    entry
        .read_to_end(&mut bytes)
        .context("failed to read the profile workbook out of the Bronze zip")?;
    Ok(bytes)
}

/// Writes the JSONL export, refusing to replace an earlier one.
///
/// Bronze exports are evidence: a rerun that quietly truncates the previous file destroys the
/// record of what an earlier Silver load was built from.
fn write_jsonl(output_path: &Path, rows: &[IndustrialComplexBronzeRawRow]) -> anyhow::Result<()> {
    if output_path.exists() {
        bail!(
            "industrial-complex Bronze JSONL output already exists and is append-only evidence: {}",
            output_path.display()
        );
    }
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let mut payload = Vec::new();
    for row in rows {
        let line = industrial_complex_bronze_raw_row_to_jsonl(row)
            .context("failed to serialize a bronze.industrial_complexes_raw_jsonl row")?;
        payload
            .write_all(line.as_bytes())
            .context("failed to buffer a JSONL row")?;
        payload.write_all(b"\n").context("failed to buffer EOL")?;
    }
    fs::write(output_path, &payload)
        .with_context(|| format!("failed to write {}", output_path.display()))
}

fn write_summary(
    config: &BronzeRawJsonlExportConfig,
    rows: &[IndustrialComplexBronzeRawRow],
    report: &BronzeRawJsonlExportReport,
    summary_path: &Path,
) -> anyhow::Result<()> {
    let mut kind_counts = BTreeMap::<String, u64>::new();
    let mut status_counts = BTreeMap::<String, u64>::new();
    let mut lot_sales_status_counts = BTreeMap::<String, u64>::new();
    let mut sido_counts = BTreeMap::<String, u64>::new();
    let mut rows_with_a_legal_dong = 0_u64;
    let mut rows_with_an_administrative_code = 0_u64;
    let mut rows_without_an_administrative_code = Vec::<&str>::new();
    // A period the split does not recognize keeps its raw text and derives no months. Counting the
    // rows that end up that way, and naming the first few, is what keeps the outcome from reading
    // as "every period parsed" — an export that reported nothing here would look identical whether
    // one row failed to parse or every row did.
    let mut rows_with_a_business_period = 0_u64;
    let mut rows_with_unparsed_business_period = Vec::<&str>::new();
    for row in rows {
        *kind_counts.entry(row.complex_kind.clone()).or_insert(0) += 1;
        *status_counts.entry(row.status.clone()).or_insert(0) += 1;
        // Counted only over the rows that state one: a bucket named for a missing value is the
        // same invention the row itself refuses to make.
        if let Some(lot_sales_status) = &row.lot_sales_status {
            *lot_sales_status_counts
                .entry(lot_sales_status.clone())
                .or_insert(0) += 1;
        }
        if row.business_period_raw.is_some() {
            rows_with_a_business_period += 1;
            if row.business_period_start_month.is_none() {
                rows_with_unparsed_business_period.push(row.official_complex_code.as_str());
            }
        }
        // Counted over the rows that have one rather than under a `""` or `"unknown"` key: a
        // histogram bucket named for a missing value is the same invention the row refuses to make.
        match row.address.sido_code() {
            Some(sido_code) => {
                *sido_counts.entry(sido_code.to_owned()).or_insert(0) += 1;
                rows_with_an_administrative_code += 1;
            }
            None => rows_without_an_administrative_code.push(row.official_complex_code.as_str()),
        }
        if row.address.primary_bjdong_code().is_some() {
            rows_with_a_legal_dong += 1;
        }
    }
    let mut evidence_limitations = vec![
        "local_bronze_to_jsonl_export_only",
        "does_not_run_the_spark_bronze_to_silver_job",
        "does_not_write_iceberg_table",
        "address_resolution_is_only_as_good_as_its_injected_source",
        "does_not_approve_production_cutover",
    ];
    if !report.labels_measured_for_snapshot {
        // The kind/status tables are counted evidence for one month only. Saying so in the
        // artifact is the difference between a mapping that was verified and one that was assumed.
        evidence_limitations.push("label_tables_not_measured_for_this_snapshot_period");
    }
    if !rows_without_an_administrative_code.is_empty() {
        // Region is not a requirement of this pipeline (root ADR-0035), so these rows are exported
        // with `null` region columns rather than dropped or filled. A consumer that needs a region
        // has to read that off the artifact instead of assuming the columns are populated.
        evidence_limitations.push("some_rows_have_no_administrative_code_only_an_address_text");
    }
    if rows_with_a_legal_dong < rows.len() as u64 {
        // Most industrial complexes span several eup/myeon/dong and some span several provinces,
        // so no address source names one. Those rows carry `sido_code` and `sigungu_code` and a
        // null `primary_bjdong_code` — the ones counted above carry nothing at all — and a consumer
        // that needs dong-level identity has to know that from the artifact rather than discover it
        // downstream (root ADR-0034).
        evidence_limitations.push("some_rows_have_no_legal_dong_code_only_a_sigungu_code");
    }
    if !rows_with_unparsed_business_period.is_empty() {
        // The raw text is intact for these rows; only the two derived month columns are null. A
        // consumer that reads the months without reading this will think the period is absent.
        evidence_limitations.push("some_business_periods_state_no_months_and_derive_none");
    }
    if !report.absent_optional_headers.is_empty() {
        // The provider stopped publishing a column this decoder reads. Its rows are null because
        // the column was gone, which is a different fact from a blank cell.
        evidence_limitations.push("the_worksheet_did_not_carry_every_optional_column_read");
    }
    if !report.residual_entity_references.is_empty() {
        // One unescaping pass, deliberately. What it did not resolve is carried forward verbatim
        // and counted below rather than dropped or unescaped again (root ADR-0050).
        evidence_limitations.push("some_provider_escapes_did_not_resolve_in_one_pass");
    }
    if report
        .resolution_tier_counts
        .get("modal_notice_code")
        .is_some_and(|count| *count > 0)
    {
        // A heuristic tier: the modal district among a complex's own notices. It is right for the
        // complexes measured, and it is still a vote rather than an authority.
        evidence_limitations.push("some_addresses_resolved_by_the_modal_notice_heuristic");
    }
    let summary = serde_json::json!({
        "schema_version": "foundation-platform.industrial_complex_bronze_raw_jsonl_export.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "source": {
            "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
            "source_slug": config.source_slug,
            "bronze_object_key": report.bronze_object_key,
            "max_rows": config.max_rows,
            "address_source_path": config.address_source_path.display().to_string(),
            "address_resolution_count": report.address_resolution_count,
            "snapshot_period": report.snapshot_period,
            "absent_optional_headers": report.absent_optional_headers,
        },
        "provider_text": {
            "residual_entity_reference_counts": report.residual_entity_references,
        },
        "business_period": {
            "rows_with_a_business_period": rows_with_a_business_period,
            "rows_with_parsed_months":
                rows_with_a_business_period - rows_with_unparsed_business_period.len() as u64,
            "rows_without_parsed_months": rows_with_unparsed_business_period.len(),
            "official_complex_codes_without_parsed_months": rows_with_unparsed_business_period,
        },
        "address_resolution": {
            "tier_counts": report.resolution_tier_counts,
            "granularity_counts": report.address_granularity_counts,
            "rows_with_a_legal_dong_code": rows_with_a_legal_dong,
            "rows_without_a_legal_dong_code": rows.len() as u64 - rows_with_a_legal_dong,
            "rows_with_an_administrative_code": rows_with_an_administrative_code,
            "rows_without_an_administrative_code": rows_without_an_administrative_code.len(),
            "official_complex_codes_without_an_administrative_code":
                rows_without_an_administrative_code,
        },
        "label_tables": {
            "measured_snapshot_period": INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD,
            "measured_for_this_snapshot_period": report.labels_measured_for_snapshot,
        },
        "output": {
            "path": config.output_path.display().to_string(),
            "contract": BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL,
            "row_count": report.row_count,
            "source_snapshot_id": report.source_snapshot_id,
            "complex_kind_counts": kind_counts,
            "status_counts": status_counts,
            "lot_sales_status_counts": lot_sales_status_counts,
            "sido_code_counts": sido_counts,
        },
        "evidence_limitations": evidence_limitations
    });
    let payload = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize the industrial-complex Bronze JSONL export summary")?;
    if let Some(parent) = summary_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    fs::write(summary_path, &payload)
        .with_context(|| format!("failed to write {}", summary_path.display()))
}

fn bronze_object_key(root: &Path, object_path: &Path) -> anyhow::Result<String> {
    let relative = object_path.strip_prefix(root).with_context(|| {
        format!(
            "object {} is not under root {}",
            object_path.display(),
            root.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} is required"))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn optional_usize_env(name: &str) -> anyhow::Result<Option<usize>> {
    match optional_env(name)? {
        Some(value) => {
            Ok(Some(value.trim().parse::<usize>().with_context(|| {
                format!("{name} must be a non-negative integer")
            })?))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    use lakehouse_domain::bronze_industrial_complexes_raw_jsonl_columns;
    use uuid::Uuid;
    use zip::{write::SimpleFileOptions, ZipWriter};

    /// The reserved 20991231DS9999x synthetic range; a captured provider object id is private
    /// operational evidence and must not sit in a public fixture.
    const SYNTHETIC_OBJECT_NAME: &str = "20991231DS99990-1.zip";
    const SYNTHETIC_SECOND_OBJECT_NAME: &str = "20991231DS99990-2.zip";

    const ADDRESS_LINE_111010: &str = concat!(
        r#"{"official_complex_code":"111010","administrative_code":"1153000000","#,
        r#""administrative_code_granularity":"sigungu","#,
        r#""address_text":"서울특별시 구로구 구로동","#,
        r#""address_source_dataset":"industrylandorkr__industrial_complex_list","#,
        r#""address_source_record_id":"industrylandorkr:danji_cd=111010","#,
        r#""resolution_tier":"source_code_in_authority"}"#
    );

    fn temp_root(name: &str) -> PathBuf {
        env::temp_dir().join(format!("{name}-{}", Uuid::new_v4()))
    }

    fn write_profile_zip(path: &Path, rows: &[&[&str]]) -> anyhow::Result<()> {
        let workbook = profile_workbook_decoder::tests_support::build_profile_workbook(rows)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ZipWriter::new(cursor);
            writer.start_file("TB_IRSTT_BASS_HIST.xlsx", SimpleFileOptions::default())?;
            writer.write_all(&workbook)?;
            writer.finish()?;
        }
        fs::write(path, buffer)?;
        Ok(())
    }

    fn profile_header() -> &'static [&'static str] {
        &[
            "sta_ym",
            "krihs_irstt_code",
            "krihs_irstt_nm",
            "lrstt_ty",
            "make_sttus_nm",
            "manage_instt_nm",
            "bsms_opertn_entrps_nm",
            "appn_de",
            "compet_cnfm_de",
            "appn_ar",
            "strwrk_de",
            "make_procs_rt",
            "lttot_sttus_nm",
            "bsms_pd",
            "appn_basis_law",
            "devlop_mth",
            "make_purps_cn",
            "invite_upj",
        ]
    }

    fn profile_rows() -> Vec<&'static [&'static str]> {
        vec![
            profile_header(),
            &[
                "202506",
                "111010",
                "구로디지털단지",
                "국가",
                "조성완료",
                "한국산업단지공단",
                "",
                "19640415",
                "19730101",
                "1925368.7",
                "19650312",
                "100",
                "분양완료",
                "1964-04~1974-11",
                "산업입지 및 개발에 관한 법률",
                "공영개발",
                "수출산업 육성",
                "전자부품",
            ],
        ]
    }

    fn profile_column(header_name: &str) -> anyhow::Result<usize> {
        profile_header()
            .iter()
            .position(|name| *name == header_name)
            .with_context(|| format!("the fixture header carries {header_name}"))
    }

    fn config(root: &Path, address_source_path: PathBuf) -> BronzeRawJsonlExportConfig {
        BronzeRawJsonlExportConfig {
            bronze_local_object_root: root.to_path_buf(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            sheet_name: None,
            address_source_path,
            output_path: root.join("out").join("industrial_complexes_raw.jsonl"),
            summary_path: Some(root.join("out").join("summary.json")),
            max_rows: None,
        }
    }

    fn staged_root(name: &str) -> anyhow::Result<PathBuf> {
        let root = temp_root(name);
        write_profile_zip(
            &root
                .join("bronze")
                .join(format!("source={DEFAULT_SOURCE_SLUG}"))
                .join(SYNTHETIC_OBJECT_NAME),
            &profile_rows(),
        )?;
        Ok(root)
    }

    #[test]
    fn a_snapshot_month_the_label_tables_never_counted_is_named_in_the_summary(
    ) -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-industrial-complex-bronze-jsonl-unmeasured");
        let mut rows = profile_rows();
        // Same table, a month nobody has counted the kind/status labels for.
        let unmeasured = [
            "209912",
            "111010",
            "구로디지털단지",
            "국가",
            "조성완료",
            "한국산업단지공단",
            "",
            "19640415",
            "19730101",
            "1925368.7",
            "19650312",
            "100",
            "분양완료",
            "1964-04~1974-11",
            "산업입지 및 개발에 관한 법률",
            "공영개발",
            "수출산업 육성",
            "전자부품",
        ];
        rows[1] = &unmeasured;
        write_profile_zip(
            &root
                .join("bronze")
                .join(format!("source={DEFAULT_SOURCE_SLUG}"))
                .join(SYNTHETIC_OBJECT_NAME),
            &rows,
        )?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let config = config(&root, address_source_path);

        let report = export_bronze_raw_jsonl(&config)?;

        assert_eq!(report.snapshot_period, "209912");
        assert!(!report.labels_measured_for_snapshot);

        let summary_path = config
            .summary_path
            .as_ref()
            .context("summary path was configured")?;
        let summary =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(summary_path)?)?;
        assert_eq!(summary["source"]["snapshot_period"], "209912");
        assert_eq!(
            summary["label_tables"]["measured_snapshot_period"],
            INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD
        );
        assert_eq!(
            summary["label_tables"]["measured_for_this_snapshot_period"],
            false
        );
        let limitations = summary["evidence_limitations"]
            .as_array()
            .context("evidence_limitations must be an array")?;
        assert!(limitations
            .iter()
            .any(|value| value == "label_tables_not_measured_for_this_snapshot_period"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_one_jsonl_record_per_profile_row() -> anyhow::Result<()> {
        let root = staged_root("foundation-platform-industrial-complex-bronze-jsonl")?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let config = config(&root, address_source_path);

        let report = export_bronze_raw_jsonl(&config)?;

        assert_eq!(report.row_count, 1);
        assert_eq!(report.address_resolution_count, 1);
        assert_eq!(
            report.bronze_object_key,
            format!("bronze/source={DEFAULT_SOURCE_SLUG}/{SYNTHETIC_OBJECT_NAME}")
        );
        assert_eq!(
            report.source_snapshot_id,
            format!("{DEFAULT_SOURCE_SLUG}-202506")
        );

        let written = fs::read_to_string(&config.output_path)?;
        let value = serde_json::from_str::<serde_json::Value>(written.trim())?;
        let record = value
            .as_object()
            .context("exported line must be a JSON object")?;
        let mut expected_columns = bronze_industrial_complexes_raw_jsonl_columns();
        expected_columns.sort_unstable();
        assert_eq!(
            record.keys().map(String::as_str).collect::<Vec<_>>(),
            expected_columns
        );
        assert_eq!(record["official_complex_code"], "111010");
        assert_eq!(record["complex_kind"], "national");
        assert_eq!(record["status"], "operating");
        assert_eq!(record["sido_code"], "11");
        assert_eq!(record["sigungu_code"], "11530");
        // The resolution named a district, so the dong column is null rather than the district code
        // wearing a dong column's name (root ADR-0034).
        assert_eq!(record["primary_bjdong_code"], serde_json::Value::Null);
        assert_eq!(record["address_text"], "서울특별시 구로구 구로동");
        assert_eq!(record["completion_date"], "1973-01-01");
        assert_eq!(record["valid_from_utc"], "2025-06-01T00:00:00Z");
        assert_eq!(record["construction_start_date"], "1965-03-12");
        assert_eq!(record["development_progress_percent"], "100.00");
        assert_eq!(record["lot_sales_status"], "completed");
        assert_eq!(record["business_period_raw"], "1964-04~1974-11");
        assert_eq!(record["business_period_start_month"], "1964-04");
        assert_eq!(record["business_period_end_month"], "1974-11");
        assert_eq!(record["development_method_raw"], "공영개발");
        assert_eq!(record["invited_industries_raw"], "전자부품");

        let summary_path = config
            .summary_path
            .as_ref()
            .context("summary path was configured")?;
        let summary =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(summary_path)?)?;
        assert_eq!(
            summary["output"]["contract"],
            BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL
        );
        assert_eq!(summary["output"]["row_count"], 1);
        assert_eq!(summary["output"]["lot_sales_status_counts"]["completed"], 1);
        assert_eq!(summary["source"]["snapshot_period"], "202506");
        assert_eq!(
            summary["source"]["absent_optional_headers"],
            serde_json::json!([])
        );
        assert_eq!(summary["business_period"]["rows_with_a_business_period"], 1);
        assert_eq!(summary["business_period"]["rows_with_parsed_months"], 1);
        assert_eq!(summary["business_period"]["rows_without_parsed_months"], 0);
        assert_eq!(
            summary["label_tables"]["measured_for_this_snapshot_period"],
            true
        );
        assert_eq!(
            summary["address_resolution"]["tier_counts"]["source_code_in_authority"],
            1
        );
        assert_eq!(
            summary["address_resolution"]["granularity_counts"]["sigungu"],
            1
        );
        assert_eq!(
            summary["address_resolution"]["rows_without_a_legal_dong_code"],
            1
        );
        let limitations = summary["evidence_limitations"]
            .as_array()
            .context("evidence_limitations must be an array")?;
        assert!(limitations
            .iter()
            .any(|value| value == "some_rows_have_no_legal_dong_code_only_a_sigungu_code"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    /// The `2020-~2024-` row: its raw text reaches the export whole, its two derived months are
    /// null, and the summary says how many rows ended up that way and which. Reporting is the
    /// difference between "one period states no months" and an export that silently looks as if
    /// every period parsed.
    #[test]
    fn a_business_period_that_states_no_months_is_counted_in_the_summary() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-industrial-complex-bronze-jsonl-period");
        let mut rows = profile_rows();
        let no_months = [
            "202506",
            "111010",
            "구로디지털단지",
            "국가",
            "조성완료",
            "한국산업단지공단",
            "",
            "19640415",
            "19730101",
            "1925368.7",
            "19650312",
            "100",
            "분양완료",
            "2020-~2024-",
            "산업입지 및 개발에 관한 법률",
            "공영개발",
            "수출산업 육성",
            "전자부품",
        ];
        rows[1] = &no_months;
        write_profile_zip(
            &root
                .join("bronze")
                .join(format!("source={DEFAULT_SOURCE_SLUG}"))
                .join(SYNTHETIC_OBJECT_NAME),
            &rows,
        )?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let config = config(&root, address_source_path);

        export_bronze_raw_jsonl(&config)?;

        let written = fs::read_to_string(&config.output_path)?;
        let record = serde_json::from_str::<serde_json::Value>(written.trim())?;
        assert_eq!(record["business_period_raw"], "2020-~2024-");
        assert_eq!(
            record["business_period_start_month"],
            serde_json::Value::Null
        );
        assert_eq!(record["business_period_end_month"], serde_json::Value::Null);

        let summary_path = config
            .summary_path
            .as_ref()
            .context("summary path was configured")?;
        let summary =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(summary_path)?)?;
        assert_eq!(summary["business_period"]["rows_with_a_business_period"], 1);
        assert_eq!(summary["business_period"]["rows_with_parsed_months"], 0);
        assert_eq!(summary["business_period"]["rows_without_parsed_months"], 1);
        assert_eq!(
            summary["business_period"]["official_complex_codes_without_parsed_months"],
            serde_json::json!(["111010"])
        );
        let limitations = summary["evidence_limitations"]
            .as_array()
            .context("evidence_limitations must be an array")?;
        assert!(limitations
            .iter()
            .any(|value| value == "some_business_periods_state_no_months_and_derive_none"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    /// The escaped text reaches the JSONL unescaped, and what one pass could not resolve reaches
    /// the summary instead of the screen. This is the whole path the 580 escaped canonical rows
    /// travelled with nothing on it (root ADR-0050).
    #[test]
    fn provider_escapes_are_unescaped_once_and_the_remainder_reaches_the_summary(
    ) -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-industrial-complex-bronze-jsonl-escapes");
        let mut values = profile_rows()[1].to_vec();
        values[profile_column("make_purps_cn")?] = "R&amp;D 및 &amp;ldquo;신기술산업&amp;rdquo;";
        values[profile_column("invite_upj")?] = "전자부품&middot;컴퓨터&middot;영상";
        write_profile_zip(
            &root
                .join("bronze")
                .join(format!("source={DEFAULT_SOURCE_SLUG}"))
                .join(SYNTHETIC_OBJECT_NAME),
            &[profile_header(), &values],
        )?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let config = config(&root, address_source_path);

        let report = export_bronze_raw_jsonl(&config)?;

        let written = fs::read_to_string(&config.output_path)?;
        let record = serde_json::from_str::<serde_json::Value>(written.trim())?;
        assert_eq!(record["invited_industries_raw"], "전자부품·컴퓨터·영상");
        assert_eq!(
            record["development_purpose_raw"], "R&D 및 &ldquo;신기술산업&rdquo;",
            "the ampersand is unescaped once; a second pass would resolve the quotation marks the \
             provider escaped twice, and the same second pass turns `&amp;lt;` into a tag"
        );
        assert_eq!(
            report.residual_entity_references,
            [("&ldquo;".to_owned(), 1), ("&rdquo;".to_owned(), 1)]
                .into_iter()
                .collect()
        );

        let summary_path = config
            .summary_path
            .as_ref()
            .context("summary path was configured")?;
        let summary =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(summary_path)?)?;
        assert_eq!(
            summary["provider_text"]["residual_entity_reference_counts"],
            serde_json::json!({"&ldquo;": 1, "&rdquo;": 1})
        );
        let limitations = summary["evidence_limitations"]
            .as_array()
            .context("evidence_limitations must be an array")?;
        assert!(limitations
            .iter()
            .any(|value| value == "some_provider_escapes_did_not_resolve_in_one_pass"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    /// A provider that drops an optional column costs that column, not the export. The summary
    /// names the header so a reader can tell an absent column from blank cells.
    #[test]
    fn a_worksheet_missing_an_optional_column_still_exports_and_says_so() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-industrial-complex-bronze-jsonl-absent-header");
        let mut headers = profile_header().to_vec();
        let mut values = profile_rows()[1].to_vec();
        let dropped = headers
            .iter()
            .position(|name| *name == "invite_upj")
            .context("the fixture header carries invite_upj")?;
        headers.remove(dropped);
        values.remove(dropped);
        write_profile_zip(
            &root
                .join("bronze")
                .join(format!("source={DEFAULT_SOURCE_SLUG}"))
                .join(SYNTHETIC_OBJECT_NAME),
            &[&headers, &values],
        )?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let config = config(&root, address_source_path);

        let report = export_bronze_raw_jsonl(&config)?;

        assert_eq!(report.row_count, 1);
        assert_eq!(report.absent_optional_headers, vec!["invite_upj"]);

        let written = fs::read_to_string(&config.output_path)?;
        let record = serde_json::from_str::<serde_json::Value>(written.trim())?;
        assert_eq!(record["invited_industries_raw"], serde_json::Value::Null);
        // Everything else still arrives.
        assert_eq!(record["development_method_raw"], "공영개발");

        let summary_path = config
            .summary_path
            .as_ref()
            .context("summary path was configured")?;
        let summary =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(summary_path)?)?;
        assert_eq!(
            summary["source"]["absent_optional_headers"],
            serde_json::json!(["invite_upj"])
        );
        let limitations = summary["evidence_limitations"]
            .as_array()
            .context("evidence_limitations must be an array")?;
        assert!(limitations
            .iter()
            .any(|value| value == "the_worksheet_did_not_carry_every_optional_column_read"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn an_unresolved_complex_writes_no_output_at_all() -> anyhow::Result<()> {
        let root = staged_root("foundation-platform-industrial-complex-bronze-jsonl-unresolved")?;
        let address_source_path = root.join("addresses.jsonl");
        // A resolution for a different complex: the staged row stays unresolved.
        fs::write(
            &address_source_path,
            concat!(
                r#"{"official_complex_code":"999999","administrative_code":"4111710300","#,
                r#""administrative_code_granularity":"legal_dong","#,
                r#""address_text":"경기도 수원시","#,
                r#""address_source_dataset":"industrylandorkr__industrial_complex_list","#,
                r#""address_source_record_id":"industrylandorkr:danji_cd=999999","#,
                r#""resolution_tier":"source_code_in_authority"}"#,
                "\n"
            ),
        )?;
        let config = config(&root, address_source_path);

        let error = export_bronze_raw_jsonl(&config).expect_err("unresolved address must fail");

        assert!(
            format!("{error:#}").contains("no sourced address"),
            "{error:#}"
        );
        assert!(!config.output_path.exists());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn an_existing_export_is_never_overwritten() -> anyhow::Result<()> {
        let root = staged_root("foundation-platform-industrial-complex-bronze-jsonl-append-only")?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let config = config(&root, address_source_path);
        export_bronze_raw_jsonl(&config)?;
        let first = fs::read_to_string(&config.output_path)?;

        let error = export_bronze_raw_jsonl(&config).expect_err("a rerun must not truncate");

        assert!(format!("{error:#}").contains("append-only"), "{error:#}");
        assert_eq!(fs::read_to_string(&config.output_path)?, first);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn a_missing_address_source_file_fails_before_the_bronze_object_is_opened() -> anyhow::Result<()>
    {
        let root = temp_root("foundation-platform-industrial-complex-bronze-jsonl-no-source");
        // No Bronze object is staged: reaching it would already be the defect.
        let config = config(&root, root.join("absent-addresses.jsonl"));

        let error =
            export_bronze_raw_jsonl(&config).expect_err("an absent address source must fail");

        assert!(
            format!("{error:#}").contains("address resolution"),
            "{error:#}"
        );
        Ok(())
    }

    #[test]
    fn multiple_unpinned_zips_require_an_explicit_object_pin() -> anyhow::Result<()> {
        let root = staged_root("foundation-platform-industrial-complex-bronze-jsonl-multi-zip")?;
        write_profile_zip(
            &root
                .join("bronze")
                .join(format!("source={DEFAULT_SOURCE_SLUG}"))
                .join(SYNTHETIC_SECOND_OBJECT_NAME),
            &profile_rows(),
        )?;
        let address_source_path = root.join("addresses.jsonl");
        fs::write(&address_source_path, format!("{ADDRESS_LINE_111010}\n"))?;
        let mut config = config(&root, address_source_path);

        let error = export_bronze_raw_jsonl(&config).expect_err("ambiguous source must fail");
        assert!(
            format!("{error:#}").contains(SOURCE_OBJECT_ENV),
            "{error:#}"
        );

        config.source_object = Some(SYNTHETIC_SECOND_OBJECT_NAME.to_owned());
        let report = export_bronze_raw_jsonl(&config)?;
        assert_eq!(report.row_count, 1);

        fs::remove_dir_all(root)?;
        Ok(())
    }
}
