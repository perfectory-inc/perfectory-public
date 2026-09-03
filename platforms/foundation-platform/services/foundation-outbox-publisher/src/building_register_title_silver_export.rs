//! 표제부 (building-register title) Bronze-to-Silver normalization export (root ADR-0073).
//!
//! Reads one national title snapshot zip out of the local Bronze mirror, normalizes every line
//! through `building_register_title_silver_plan`, and writes the Silver handoff JSONL that
//! `silver_scalar_handoff_to_lakehouse.py` loads into `silver.building_register_titles`.
//!
//! What the three sibling exports taught, kept:
//!
//! - **The snapshot is pinned or singular, never sorted-first.** Monthly zips accumulate, and a
//!   silently chosen first month would load an unverified snapshot into canonical
//!   (`locate_zip_object`, shared with the unit export rather than copied from it).
//! - **The summary counts what was left behind.** Reason counts are written even when a reason
//!   never fired — a metric that appears only on failure cannot be told apart from one nobody
//!   collected.
//!
//! What titles do not need: overrides (no staff normalization flow exists for title rows yet)
//! and the building-link index (titles *are* the buildings).

use std::{
    collections::BTreeMap,
    env,
    fs::{self, File},
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use lakehouse_application::building_register_title_silver_plan::{
    building_register_title_silver_row_to_jsonl, normalize_building_register_title_silver_rows,
    parse_building_register_title_source_row_from_hub_bulk_text_line,
    BuildingRegisterTitleSilverRowsInput,
};

use crate::building_register_unit_silver_export::{
    bronze_object_key, decode_zip_lines, locate_zip_object,
};

const DEFAULT_SOURCE_SLUG: &str = "hubgokr__building_register_main";
const ENV_PREFIX: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_TITLE_SILVER_HANDOFF";

struct TitleExportConfig {
    bronze_local_object_root: PathBuf,
    source_slug: String,
    /// Exact Bronze zip to export; required when monthly zips accumulate.
    source_object: Option<String>,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
    max_rows: Option<usize>,
}

impl TitleExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: PathBuf::from(required_env(&env_name("BRONZE_ROOT"))?),
            source_slug: optional_env(&env_name("SOURCE_SLUG"))?
                .unwrap_or_else(|| DEFAULT_SOURCE_SLUG.to_owned()),
            source_object: optional_env(&env_name("SOURCE_OBJECT"))?,
            output_path: PathBuf::from(required_env(&env_name("OUTPUT_PATH"))?),
            summary_path: optional_env(&env_name("SUMMARY_PATH"))?.map(PathBuf::from),
            source_snapshot_id: required_env(&env_name("SOURCE_SNAPSHOT_ID"))?,
            valid_from_utc: parse_valid_from_env()?,
            max_rows: optional_usize_env(&env_name("MAX_ROWS"))?,
        })
    }
}

fn env_name(suffix: &str) -> String {
    format!("{ENV_PREFIX}_{suffix}")
}

/// Runs the local 표제부 Bronze-to-Silver normalization export.
pub async fn run() -> anyhow::Result<()> {
    let config = TitleExportConfig::from_env()?;
    let report = export_handoff(&config)?;
    tracing::info!(
        row_count = report.row_count,
        output_path = %config.output_path.display(),
        "building-register title Silver normalization export succeeded"
    );
    Ok(())
}

struct TitleExportReport {
    row_count: usize,
}

fn export_handoff(config: &TitleExportConfig) -> anyhow::Result<TitleExportReport> {
    let object_path = locate_zip_object(
        &config.bronze_local_object_root,
        &config.source_slug,
        config.source_object.as_deref(),
        "title",
    )?;
    let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, &object_path)?;

    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let output = File::create(&config.output_path)
        .with_context(|| format!("failed to create {}", config.output_path.display()))?;
    let mut writer = BufWriter::new(output);

    let ingested_at_utc = Utc::now();
    let mut row_count = 0usize;
    let mut kind_counts = BTreeMap::<String, u64>::new();
    let mut reason_counts = BTreeMap::<String, u64>::new();
    let mut floor_area_present = 0u64;
    let mut approval_year_present = 0u64;
    let mut pnu_present = 0u64;

    decode_zip_lines(&object_path, config.max_rows, |line, line_number| {
        let record = parse_building_register_title_source_row_from_hub_bulk_text_line(
            line,
            &bronze_object_key,
            line_number,
        )
        .with_context(|| format!("failed to parse building-register title line {line_number}"))?;
        let rows =
            normalize_building_register_title_silver_rows(&BuildingRegisterTitleSilverRowsInput {
                records: std::slice::from_ref(&record),
                source_snapshot_id: config.source_snapshot_id.as_str(),
                bronze_object_key: &bronze_object_key,
                valid_from_utc: config.valid_from_utc,
                ingested_at_utc,
            })
            .context("failed to build building-register title Silver row")?;
        for row in &rows {
            let jsonl = building_register_title_silver_row_to_jsonl(row)
                .context("failed to serialize building-register title Silver row")?;
            writer
                .write_all(jsonl.as_bytes())
                .and_then(|()| writer.write_all(b"\n"))
                .context("failed to write building-register title Silver handoff")?;
            row_count += 1;
            *kind_counts
                .entry(row.main_or_annex_kind.clone())
                .or_insert(0) += 1;
            *reason_counts
                .entry(row.normalization_reason.clone())
                .or_insert(0) += 1;
            if row.floor_area_m2.is_some() {
                floor_area_present += 1;
            }
            if row.approval_year.is_some() {
                approval_year_present += 1;
            }
            if row.pnu.is_some() {
                pnu_present += 1;
            }
        }
        Ok(())
    })?;
    writer
        .flush()
        .context("failed to flush building-register title Silver handoff")?;
    if row_count == 0 {
        bail!("the title snapshot yielded no rows, which is not a state this dataset has");
    }

    if let Some(summary_path) = &config.summary_path {
        write_summary(
            config,
            &bronze_object_key,
            row_count,
            &kind_counts,
            &reason_counts,
            floor_area_present,
            approval_year_present,
            pnu_present,
            summary_path,
        )?;
    }

    Ok(TitleExportReport { row_count })
}

/// ADR-0073 asked for the required-column fill rates to be measured and recorded at
/// implementation time; this summary is that record, per run.
#[allow(clippy::too_many_arguments)]
fn write_summary(
    config: &TitleExportConfig,
    bronze_object_key: &str,
    row_count: usize,
    kind_counts: &BTreeMap<String, u64>,
    reason_counts: &BTreeMap<String, u64>,
    floor_area_present: u64,
    approval_year_present: u64,
    pnu_present: u64,
    summary_path: &Path,
) -> anyhow::Result<()> {
    // Reported whether or not they fired: `unknown: 0` and `unknown` absent are different claims.
    let mut kinds = kind_counts.clone();
    for kind in ["main", "annex", "unknown"] {
        kinds.entry(kind.to_owned()).or_insert(0);
    }
    let mut reasons = reason_counts.clone();
    for reason in ["accepted_title", "main_annex_unmarked"] {
        reasons.entry(reason.to_owned()).or_insert(0);
    }

    let summary = serde_json::json!({
        "schema_version": "foundation-platform.building_register_title_silver_export_summary.v1",
        "bronze_object_key": bronze_object_key,
        "source_snapshot_id": config.source_snapshot_id,
        "row_count": row_count,
        "main_or_annex_kind_counts": kinds,
        "normalization_reason_counts": reasons,
        "floor_area_present_count": floor_area_present,
        "approval_year_present_count": approval_year_present,
        "pnu_present_count": pnu_present,
    });
    let bytes = serde_json::to_vec(&summary).context("failed to serialize the title summary")?;
    if let Some(parent) = summary_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    fs::write(summary_path, bytes)
        .with_context(|| format!("failed to write {}", summary_path.display()))?;
    Ok(())
}

fn parse_valid_from_env() -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(&env_name("VALID_FROM_UTC"))?;
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("invalid valid_from_utc: {raw}"))?
        .to_utc())
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
    use zip::write::SimpleFileOptions;

    fn title_line() -> String {
        // The measured 77-field shape, reserved 99999* PNU band, coordinate-safe areas.
        let mut fields = vec![String::new(); 77];
        fields[0] = "1002121184".to_owned();
        fields[8] = "99999".to_owned();
        fields[9] = "00001".to_owned();
        fields[10] = "0".to_owned();
        fields[11] = "0001".to_owned();
        fields[12] = "0000".to_owned();
        fields[24] = "주건축물".to_owned();
        fields[26] = "81.7".to_owned();
        fields[28] = "163.4".to_owned();
        fields[34] = "03000".to_owned();
        fields[43] = "2".to_owned();
        fields[60] = "19710217".to_owned();
        fields.join("|")
    }

    fn write_fixture_zip(root: &Path, slug: &str, name: &str, lines: &[String]) -> PathBuf {
        let dir = root.join("bronze").join(format!("source={slug}"));
        fs::create_dir_all(&dir).unwrap_or_else(|error| panic!("fixture dir: {error}"));
        let path = dir.join(name);
        let file = File::create(&path).unwrap_or_else(|error| panic!("fixture zip: {error}"));
        let mut zip = zip::ZipWriter::new(file);
        zip.start_file("mart_djy_03.txt", SimpleFileOptions::default())
            .unwrap_or_else(|error| panic!("zip entry: {error}"));
        for line in lines {
            zip.write_all(line.as_bytes())
                .and_then(|()| zip.write_all(b"\n"))
                .unwrap_or_else(|error| panic!("zip write: {error}"));
        }
        zip.finish()
            .unwrap_or_else(|error| panic!("zip finish: {error}"));
        path
    }

    /// A fresh directory under the system temp root, removed by the guard on drop.
    struct TempRoot(PathBuf);
    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn temp_root(label: &str) -> TempRoot {
        let path = env::temp_dir().join(format!("{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap_or_else(|error| panic!("temp root: {error}"));
        TempRoot(path)
    }

    fn config(root: &Path, out: &Path, summary: &Path) -> TitleExportConfig {
        TitleExportConfig {
            bronze_local_object_root: root.to_path_buf(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            output_path: out.to_path_buf(),
            summary_path: Some(summary.to_path_buf()),
            source_snapshot_id: "hubgokr__building_register_main:2026-07".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap_or_else(|error| panic!("fixture time: {error}"))
                .to_utc(),
            max_rows: None,
        }
    }

    #[test]
    fn the_export_normalizes_a_snapshot_end_to_end() {
        let temp = temp_root("title-export-e2e");
        write_fixture_zip(
            &temp.0,
            DEFAULT_SOURCE_SLUG,
            "OPN0001.zip",
            &[title_line(), title_line()],
        );
        let out = &temp.0.join("titles.jsonl");
        let summary_path = &temp.0.join("summary.json");

        let report = export_handoff(&config(&temp.0, out.as_path(), summary_path.as_path()))
            .unwrap_or_else(|error| panic!("export: {error:#}"));

        assert_eq!(report.row_count, 2);
        let body =
            fs::read_to_string(out.as_path()).unwrap_or_else(|error| panic!("read out: {error}"));
        assert_eq!(body.lines().count(), 2);
        assert!(body.contains("\"main_or_annex_kind\":\"main\""));
        assert!(body.contains("\"floor_area_m2\":163.4"));
        let summary: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(summary_path.as_path())
                .unwrap_or_else(|error| panic!("read summary: {error}")),
        )
        .unwrap_or_else(|error| panic!("summary json: {error}"));
        assert_eq!(summary["row_count"], 2);
        assert_eq!(summary["main_or_annex_kind_counts"]["main"], 2);
        // Reported even at zero: absent and zero are different claims.
        assert_eq!(summary["main_or_annex_kind_counts"]["unknown"], 0);
        assert_eq!(summary["pnu_present_count"], 2);
    }

    #[test]
    fn two_unpinned_monthly_snapshots_are_refused() {
        // The sibling exports learned this the expensive way: sorted-first silently loads an
        // unverified month into canonical. The chooser is shared, and this proves the sharing.
        let temp = temp_root("title-export-two-months");
        write_fixture_zip(&temp.0, DEFAULT_SOURCE_SLUG, "OPN0001.zip", &[title_line()]);
        write_fixture_zip(&temp.0, DEFAULT_SOURCE_SLUG, "OPN0002.zip", &[title_line()]);
        let out = &temp.0.join("titles.jsonl");
        let summary_path = &temp.0.join("summary.json");

        let error = export_handoff(&config(&temp.0, out.as_path(), summary_path.as_path()))
            .map(|report| report.row_count)
            .expect_err("two unpinned snapshots must not silently pick one");

        assert!(format!("{error:#}").to_lowercase().contains("pin"));
    }

    #[test]
    fn an_empty_snapshot_is_an_error_not_a_success() {
        let temp = temp_root("title-export-empty");
        write_fixture_zip(&temp.0, DEFAULT_SOURCE_SLUG, "OPN0001.zip", &[]);
        let out = &temp.0.join("titles.jsonl");
        let summary_path = &temp.0.join("summary.json");

        let error = export_handoff(&config(&temp.0, out.as_path(), summary_path.as_path()))
            .map(|report| report.row_count)
            .expect_err("an empty snapshot must not export an empty Silver");

        assert!(format!("{error:#}").contains("no rows"));
    }
}
