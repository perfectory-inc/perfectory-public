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
    industrial_complex_bronze_raw_row_to_jsonl, normalize_industrial_complex_bronze_raw_rows,
    IndustrialComplexBronzeRawRow, IndustrialComplexBronzeRawRowsInput,
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
    let addresses = address_source::read_address_book(&config.address_source_path)?;
    let object_path = locate_source_object(config)?;
    let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, &object_path)?;
    let workbook_bytes = read_single_workbook_entry(&object_path)?;
    let records = profile_workbook_decoder::decode_profile_rows(
        workbook_bytes,
        config.sheet_name.as_deref(),
        config.max_rows,
    )
    .with_context(|| format!("failed to decode industrial-complex profile {bronze_object_key}"))?;

    let rows = normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
        records: &records,
        addresses: &addresses,
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

    let report = BronzeRawJsonlExportReport {
        row_count: rows.len(),
        address_resolution_count: addresses.len(),
        bronze_object_key,
        source_snapshot_id,
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
    let source_root = config
        .bronze_local_object_root
        .join("bronze")
        .join(format!("source={}", config.source_slug));
    if !source_root.is_dir() {
        bail!(
            "industrial-complex profile Bronze source directory not found: {}",
            source_root.display()
        );
    }
    if let Some(object) = config.source_object.as_deref() {
        let pinned = source_root.join(object);
        if !pinned.is_file() {
            bail!(
                "pinned industrial-complex profile Bronze object not found: {}",
                pinned.display()
            );
        }
        return Ok(pinned);
    }
    let mut zips = Vec::new();
    for entry in fs::read_dir(&source_root)
        .with_context(|| format!("failed to read {}", source_root.display()))?
    {
        let path = entry
            .with_context(|| format!("failed to read entry in {}", source_root.display()))?
            .path();
        if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
        {
            zips.push(path);
        }
    }
    zips.sort();
    if zips.len() > 1 {
        bail!(
            concat!(
                "industrial-complex profile Bronze source {} holds {} zips; monthly snapshots are ",
                "ambiguous; set {} to the exact zip to export"
            ),
            source_root.display(),
            zips.len(),
            SOURCE_OBJECT_ENV
        );
    }
    zips.into_iter().next().with_context(|| {
        format!(
            "no industrial-complex profile Bronze zip found in {}",
            source_root.display()
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
    let mut sido_counts = BTreeMap::<String, u64>::new();
    for row in rows {
        *kind_counts.entry(row.complex_kind.clone()).or_insert(0) += 1;
        *status_counts.entry(row.status.clone()).or_insert(0) += 1;
        *sido_counts
            .entry(row.address.sido_code().to_owned())
            .or_insert(0) += 1;
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
        },
        "output": {
            "path": config.output_path.display().to_string(),
            "contract": BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL,
            "row_count": report.row_count,
            "source_snapshot_id": report.source_snapshot_id,
            "complex_kind_counts": kind_counts,
            "status_counts": status_counts,
            "sido_code_counts": sido_counts,
        },
        "evidence_limitations": [
            "local_bronze_to_jsonl_export_only",
            "does_not_run_the_spark_bronze_to_silver_job",
            "does_not_write_iceberg_table",
            "address_resolution_is_only_as_good_as_its_injected_source",
            "does_not_approve_production_cutover"
        ]
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

    const ADDRESS_LINE_111010: &str = concat!(
        r#"{"official_complex_code":"111010","primary_bjdong_code":"1153010200","#,
        r#""address_text":"서울특별시 구로구 구로동","#,
        r#""address_source_dataset":"ilis__industrial_complex_detail","#,
        r#""address_source_record_id":"ilis:111010"}"#
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

    fn profile_rows() -> Vec<&'static [&'static str]> {
        vec![
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
            ],
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
            ],
        ]
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
                .join("30138-6.zip"),
            &profile_rows(),
        )?;
        Ok(root)
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
            format!("bronze/source={DEFAULT_SOURCE_SLUG}/30138-6.zip")
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
        assert_eq!(record["address_text"], "서울특별시 구로구 구로동");
        assert_eq!(record["completion_date"], "1973-01-01");
        assert_eq!(record["valid_from_utc"], "2025-06-01T00:00:00Z");

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
                r#"{"official_complex_code":"999999","primary_bjdong_code":"4111710300","#,
                r#""address_text":"경기도 수원시","address_source_dataset":"ilis","#,
                r#""address_source_record_id":"ilis:999999"}"#,
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
                .join("30138-7.zip"),
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

        config.source_object = Some("30138-7.zip".to_owned());
        let report = export_bronze_raw_jsonl(&config)?;
        assert_eq!(report.row_count, 1);

        fs::remove_dir_all(root)?;
        Ok(())
    }
}
