//! Full floor exports consume exactly two named ZIPs, never a source-directory walk.
use super::{optional_env, ExportConfig, OutputFormat, SourceSelector, DEFAULT_TITLE_SOURCE_SLUG};
use anyhow::{bail, ensure, Context};
use std::path::PathBuf;

/// An empty or `none` value disables the building-title witness.
pub(super) fn title_source_slug_from_env() -> anyhow::Result<Option<String>> {
    Ok(
        match optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_TITLE_SOURCE_SLUG",
        )? {
            Some(value) if value.trim().is_empty() || value == "none" => None,
            Some(value) => Some(value),
            None => Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
        },
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactInputs {
    pub floor: String,
    pub title: String,
}
impl ExactInputs {
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let floor = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_OBJECT",
        )?;
        let title = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_TITLE_SOURCE_OBJECT",
        )?;
        match (floor, title) {
            (None, None) => Ok(None),
            (Some(floor), Some(title)) => {
                let value = Self { floor, title };
                value.validate()?;
                Ok(Some(value))
            }
            _ => bail!("exact floor input requires both floor and title ZIPs"),
        }
    }
    fn validate(&self) -> anyhow::Result<()> {
        let date = crate::building_register_snapshot::object_date(&self.floor)?;
        crate::building_register_snapshot::validate_object_name(&self.title, date)
    }
    pub fn paths(&self, config: &ExportConfig) -> anyhow::Result<(PathBuf, PathBuf)> {
        let paths = self.destination_paths(config)?;
        for path in [&paths.0, &paths.1] {
            ensure!(
                path.is_file(),
                "exact structural ZIP is missing: {}",
                path.display()
            );
        }
        Ok(paths)
    }
    pub(super) fn destination_paths(
        &self,
        config: &ExportConfig,
    ) -> anyhow::Result<(PathBuf, PathBuf)> {
        self.validate()?;
        let SourceSelector::Exact(slug) = &config.source_selector else {
            bail!("exact floor input rejects prefix selection")
        };
        let title_slug = config
            .title_source_slug
            .as_deref()
            .context("exact floor input requires title witness")?;
        let path = |slug: &str, object: &str| -> anyhow::Result<PathBuf> {
            ensure!(
                !slug.contains('/') && !slug.contains('\\') && !slug.contains(".."),
                "invalid exact source slug"
            );
            let path = config
                .bronze_local_object_root
                .join("bronze")
                .join(format!("source={slug}"))
                .join(object);
            Ok(path)
        };
        Ok((path(slug, &self.floor)?, path(title_slug, &self.title)?))
    }
}

impl SourceSelector {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let source_slug =
            optional_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG")?;
        let source_slug_prefix = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG_PREFIX",
        )?;
        match (source_slug, source_slug_prefix) {
            (Some(slug), None) => Ok(Self::Exact(slug)),
            (None, Some(prefix)) => Ok(Self::Prefix(prefix)),
            (Some(_), Some(_)) => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG and FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG_PREFIX cannot both be set"
            ),
            (None, None) => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG or FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG_PREFIX is required"
            ),
        }
    }

    pub(super) fn source_summary(&self) -> serde_json::Value {
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

impl OutputFormat {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let Some(raw) = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT",
        )?
        else {
            return Ok(Self::Jsonl);
        };
        match raw.as_str() {
            "jsonl" => Ok(Self::Jsonl),
            "parquet" => Ok(Self::Parquet),
            _ => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT must be one of jsonl, parquet"
            ),
        }
    }

    pub(super) const fn wire_name(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Parquet => "parquet",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::export_handoff;
    use super::*;
    use chrono::Utc;
    use std::fs::File;
    #[test]
    fn full_floor_requires_same_export_date_and_safe_exact_files() {
        let good = ExactInputs {
            floor: "OPN20990920FLOOR.zip".into(),
            title: "OPN20990920TITLE.zip".into(),
        };
        assert!(good.validate().is_ok());
        let bad = ExactInputs {
            title: "OPN20990921TITLE.zip".into(),
            ..good
        };
        assert!(bad.validate().is_err());
    }

    #[tokio::test]
    async fn exact_floor_export_ignores_other_months_and_unselected_zip_files() -> anyhow::Result<()>
    {
        use std::io::Write;
        let root = tempfile::tempdir()?;
        let floor_slug = crate::building_register_source_role::SourceRole::Floor.slug();
        let title_slug = crate::building_register_source_role::SourceRole::Title.slug();
        let exact = ExactInputs {
            floor: "OPN20990920FLOOR.zip".into(),
            title: "OPN20990920TITLE.zip".into(),
        };
        for (slug, name) in [(floor_slug, &exact.floor), (title_slug, &exact.title)] {
            let directory = root.path().join("bronze").join(format!("source={slug}"));
            std::fs::create_dir_all(directory.join("unselected"))?;
            let file = File::create(directory.join(name))?;
            let mut zip = zip::ZipWriter::new(file);
            zip.start_file("empty.txt", zip::write::SimpleFileOptions::default())?;
            zip.write_all(b"\n")?;
            zip.finish()?;
            // Both would make a recursive exporter fail before it could finish.
            std::fs::write(directory.join("OPN20990820OLD.zip"), b"not a ZIP")?;
            std::fs::write(
                directory.join("unselected/OPN20990920OTHER.zip"),
                b"not a ZIP",
            )?;
        }
        let config = ExportConfig {
            bronze_local_object_root: root.path().into(),
            source_selector: SourceSelector::Exact(floor_slug.into()),
            committed_inputs: None,
            reuse_completed: false,
            exact_inputs: Some(exact.clone()),
            output_path: root.path().join("floor.jsonl"),
            proposal_input_path: None,
            summary_path: None,
            source_snapshot_id: "exact-floor-fixture".into(),
            valid_from_utc: Utc::now(),
            ingested_at_utc: Utc::now(),
            max_rows: None,
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: Some(title_slug.into()),
        };
        let report = export_handoff(&config).await?;
        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 0);
        std::fs::remove_file(exact.paths(&config)?.1)?;
        assert!(export_handoff(&config).await.is_err());
        Ok(())
    }
}
