//! `VWorld` cadastral Bronze-to-Silver handoff export command.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use collection_domain::dedupe_vworld_cadastral_features_by_pnu;
use lakehouse_application::{
    build_vworld_cadastral_silver_parcel_boundary_handoff,
    normalize_vworld_cadastral_silver_parcel_boundary_rows,
    VWorldCadastralSilverParcelBoundaryRowsInput,
};
use serde_json::Value as JsonValue;

/// Runs the local Bronze-to-Silver handoff export.
pub fn run() -> anyhow::Result<()> {
    let config = ExportConfig::from_env()?;
    let report = export_handoff(&config)?;
    tracing::info!(
        input_object_count = report.input_object_count,
        row_count = report.row_count,
        output_path = %config.output_path.display(),
        "VWorld cadastral Silver handoff export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportConfig {
    bronze_local_object_root: PathBuf,
    source_selector: SourceSelector,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    source_record_id: String,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceSelector {
    Exact(String),
    Prefix(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportReport {
    input_object_count: usize,
    row_count: usize,
}

impl ExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: required_path_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_BRONZE_ROOT",
            )?,
            source_selector: SourceSelector::from_env()?,
            output_path: required_path_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_PATH",
            )?,
            summary_path: optional_path_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SUMMARY_PATH",
            )?,
            source_record_id: required_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_RECORD_ID",
            )?,
            source_snapshot_id: required_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID",
            )?,
            valid_from_utc: parse_utc_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_VALID_FROM_UTC",
            )?,
        })
    }
}

impl SourceSelector {
    fn from_env() -> anyhow::Result<Self> {
        let source_slug =
            optional_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SLUG")?;
        let source_slug_prefix =
            optional_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SLUG_PREFIX")?;
        match (source_slug, source_slug_prefix) {
            (Some(slug), None) => Ok(Self::Exact(slug)),
            (None, Some(prefix)) => Ok(Self::Prefix(prefix)),
            (Some(_), Some(_)) => bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SLUG and FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SLUG_PREFIX cannot both be set"
            ),
            (None, None) => bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SLUG or FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SLUG_PREFIX is required"
            ),
        }
    }

    fn source_summary(&self) -> serde_json::Value {
        match self {
            Self::Exact(source_slug) => serde_json::json!({
                "source_slug": source_slug
            }),
            Self::Prefix(source_slug_prefix) => serde_json::json!({
                "source_slug_prefix": source_slug_prefix
            }),
        }
    }
}

fn export_handoff(config: &ExportConfig) -> anyhow::Result<ExportReport> {
    let object_paths =
        collect_bronze_object_paths(&config.bronze_local_object_root, &config.source_selector)?;
    if object_paths.is_empty() {
        bail!("no VWorld cadastral Bronze objects found for source selector");
    }

    let payloads = object_paths
        .iter()
        .map(|path| read_json_value(path))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let dedupe_report = dedupe_vworld_cadastral_features_by_pnu(&payloads)
        .context("failed to dedupe VWorld cadastral Bronze features")?;
    let rows = normalize_vworld_cadastral_silver_parcel_boundary_rows(
        &VWorldCadastralSilverParcelBoundaryRowsInput {
            records: &dedupe_report.records,
            source_record_id: config.source_record_id.as_str(),
            source_snapshot_id: config.source_snapshot_id.as_str(),
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: Utc::now(),
        },
    )
    .context("failed to normalize VWorld cadastral Silver parcel-boundary rows")?;
    let handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&rows)
        .context("failed to build VWorld cadastral Silver handoff")?;

    write_file(&config.output_path, handoff.jsonl.as_bytes())?;
    if let Some(summary_path) = &config.summary_path {
        let summary = serde_json::json!({
            "schema_version": "foundation-platform.vworld_cadastral_silver_handoff_export.v1",
            "generated_at_utc": Utc::now().to_rfc3339(),
            "status": "ready",
            "completion_claim_allowed": false,
            "production_cutover_allowed": false,
            "national_rollout_allowed": false,
            "source": {
                "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
                "selector": config.source_selector.source_summary(),
                "input_object_count": object_paths.len(),
                "source_record_id": config.source_record_id.as_str(),
                "source_snapshot_id": config.source_snapshot_id.as_str()
            },
            "output": {
                "path": config.output_path.display().to_string(),
                "contract": handoff.contract_table_name,
                "row_count": rows.len(),
                "source_snapshot_ids": handoff.source_snapshot_ids,
                "source_snapshot_truncated": handoff.source_snapshot_truncated
            },
            "quality_metrics": handoff.quality_metrics,
            "evidence_limitations": [
                "local_bronze_to_silver_handoff_only",
                "does_not_write_iceberg_table",
                "does_not_rebuild_postgis_anchor_or_pbf",
                "does_not_approve_production_cutover"
            ]
        });
        let payload = serde_json::to_vec_pretty(&summary)
            .context("failed to serialize VWorld cadastral Silver handoff export summary")?;
        write_file(summary_path, &payload)?;
    }

    Ok(ExportReport {
        input_object_count: object_paths.len(),
        row_count: rows.len(),
    })
}

fn collect_bronze_object_paths(
    root: &Path,
    selector: &SourceSelector,
) -> anyhow::Result<Vec<PathBuf>> {
    let bronze_root = root.join("bronze");
    if !bronze_root.is_dir() {
        bail!("Bronze root directory not found: {}", bronze_root.display());
    }
    let mut paths = Vec::new();
    for source_root in collect_source_roots(&bronze_root, selector)? {
        collect_json_files(&source_root, &mut paths)?;
    }
    paths.sort();
    Ok(paths)
}

fn collect_source_roots(
    bronze_root: &Path,
    selector: &SourceSelector,
) -> anyhow::Result<Vec<PathBuf>> {
    match selector {
        SourceSelector::Exact(source_slug) => {
            let source_root = bronze_root.join(format!("source={source_slug}"));
            if !source_root.is_dir() {
                bail!(
                    "Bronze source directory not found: {}",
                    source_root.display()
                );
            }
            Ok(vec![source_root])
        }
        SourceSelector::Prefix(source_slug_prefix) => {
            let expected_prefix = format!("source={source_slug_prefix}");
            let mut roots = Vec::new();
            for entry in fs::read_dir(bronze_root)
                .with_context(|| format!("failed to read Bronze root {}", bronze_root.display()))?
            {
                let entry = entry.with_context(|| {
                    format!("failed to read entry in {}", bronze_root.display())
                })?;
                let path = entry.path();
                if path.is_dir()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(&expected_prefix))
                {
                    roots.push(path);
                }
            }
            roots.sort();
            Ok(roots)
        }
    }
}

fn collect_json_files(dir: &Path, paths: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_json_files(&path, paths)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            paths.push(path);
        }
    }
    Ok(())
}

fn read_json_value(path: &Path) -> anyhow::Result<JsonValue> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read Bronze object {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("Bronze object is not valid JSON: {}", path.display()))
}

fn write_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("output path must have a parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(trimmed.to_owned())
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn required_path_env(name: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn optional_path_env(name: &str) -> anyhow::Result<Option<PathBuf>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(PathBuf::from(value.trim()))),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_utc_env(name: &str) -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(name)?;
    Ok(DateTime::parse_from_rfc3339(raw.trim())
        .with_context(|| format!("{name} must be an RFC3339 UTC timestamp"))?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::{export_handoff, ExportConfig, SourceSelector};
    use chrono::{DateTime, Utc};
    use std::{fs, path::PathBuf};
    use uuid::Uuid;

    #[test]
    fn exports_vworld_cadastral_bronze_objects_to_silver_handoff_jsonl() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-vworld-cadastral-silver-export-{}",
            Uuid::new_v4()
        ));
        let source_slug = "vworld-cadastral-national-99999-00101";
        let bronze_path = root
            .join("bronze")
            .join(format!("source={source_slug}"))
            .join("run_id=018f0000-0000-7000-8000-000000000001")
            .join("partition=operation=GetFeature")
            .join("part-000001.json");
        write_file(&bronze_path, sample_payload().as_bytes())?;
        let output_path = root.join("silver-handoff").join("parcel_boundaries.jsonl");
        let summary_path = root.join("audit").join("export-summary.json");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(source_slug.to_owned()),
            output_path: output_path.clone(),
            summary_path: Some(summary_path.clone()),
            source_record_id: "ledger-execution:synthetic-national-fixture".to_owned(),
            source_snapshot_id: "synthetic-national-fixture-vworld-cadastral-99999-00101"
                .to_owned(),
            valid_from_utc: parse_utc("2026-05-23T00:00:00Z")?,
        })?;

        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 1);
        let handoff = fs::read_to_string(&output_path)?;
        assert!(handoff.contains("\"pnu\":\"9999900801105800001\""));
        assert!(handoff.contains("\"geometry_wkb_encoding\":\"hex\""));
        assert!(handoff.contains("\"geometry_wkb_hex\":\"0106000000"));
        let summary = fs::read_to_string(&summary_path)?;
        assert!(summary.contains(
            "\"schema_version\": \"foundation-platform.vworld_cadastral_silver_handoff_export.v1\""
        ));
        assert!(summary.contains("\"row_count\": 1"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_prefix_matched_vworld_cadastral_sources_to_one_handoff_jsonl() -> anyhow::Result<()>
    {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-vworld-cadastral-silver-export-prefix-{}",
            Uuid::new_v4()
        ));
        let source_prefix = "vworld-cadastral-national-99999";
        for (source_slug, pnu) in [
            (
                "vworld-cadastral-national-99999-00101-r1-c1",
                "9999900701105020000",
            ),
            (
                "vworld-cadastral-national-99999-00101-r1-c2",
                "9999900701106340000",
            ),
        ] {
            let bronze_path = root
                .join("bronze")
                .join(format!("source={source_slug}"))
                .join("run_id=018f0000-0000-7000-8000-000000000001")
                .join("partition=operation=GetFeature")
                .join("part-000001.json");
            write_file(&bronze_path, sample_payload_for_pnu(pnu).as_bytes())?;
        }
        let output_path = root
            .join("silver-handoff")
            .join("parcel_boundaries-prefix.jsonl");
        let summary_path = root.join("audit").join("export-prefix-summary.json");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Prefix(source_prefix.to_owned()),
            output_path: output_path.clone(),
            summary_path: Some(summary_path.clone()),
            source_record_id: "ledger-execution:synthetic-sigungu-fixture-99999".to_owned(),
            source_snapshot_id: "synthetic-sigungu-fixture-99999-vworld-cadastral-tiled".to_owned(),
            valid_from_utc: parse_utc("2026-05-23T00:00:00Z")?,
        })?;

        assert_eq!(report.input_object_count, 2);
        assert_eq!(report.row_count, 2);
        let handoff = fs::read_to_string(&output_path)?;
        assert_eq!(handoff.lines().count(), 2);
        assert!(handoff.contains("\"pnu\":\"9999900701105020000\""));
        assert!(handoff.contains("\"pnu\":\"9999900701106340000\""));
        let summary = fs::read_to_string(&summary_path)?;
        assert!(summary.contains("\"source_slug_prefix\": \"vworld-cadastral-national-99999\""));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn write_file(path: &PathBuf, content: &[u8]) -> anyhow::Result<()> {
        let parent = path.parent().ok_or_else(|| anyhow::anyhow!("parent"))?;
        fs::create_dir_all(parent)?;
        fs::write(path, content)?;
        Ok(())
    }

    fn sample_payload() -> String {
        sample_payload_for_pnu("9999900801105800001")
    }

    fn sample_payload_for_pnu(pnu: &str) -> String {
        r#"{
          "response": {
            "result": {
              "featureCollection": {
                "features": [
                  {
                    "type": "Feature",
                    "properties": {
                      "pnu": "__PNU__",
                      "jibun": "580-1",
                      "bonbun": "0580",
                      "bubun": "0001"
                    },
                    "geometry": {
                      "type": "MultiPolygon",
                      "coordinates": [[[
                        [127.123470234300, 36.1234400],
                        [127.123470234310, 36.1234400],
                        [127.123470234310, 36.1234410],
                        [127.123470234300, 36.1234410],
                        [127.123470234300, 36.1234400]
                      ]]]
                    }
                  }
                ]
              }
            }
          }
        }"#
        .replace("__PNU__", pnu)
    }
}
