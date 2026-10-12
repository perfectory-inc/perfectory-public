//! Inputs-only, authenticated receipt for the remote Iceberg preflight.
use std::{
    env::VarError,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context};
use serde::Serialize;

use super::{
    blocking::Cancellation, input_evidence, output_layout, prepare_inputs, write_file_create_new,
    ExportConfig,
};

pub(super) fn path_from_env() -> anyhow::Result<Option<PathBuf>> {
    parse_path(std::env::var(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH",
    ))
}

fn parse_path(value: Result<String, VarError>) -> anyhow::Result<Option<PathBuf>> {
    match value {
        Err(VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).context("invalid source inspection path"),
        Ok(path) => {
            ensure!(
                !path.trim().is_empty() && !path.contains('\0'),
                "source inspection path must not be blank or contain NUL"
            );
            Ok(Some(PathBuf::from(path)))
        }
    }
}

#[derive(Serialize)]
struct Receipt {
    schema_version: u8,
    source_snapshot_id: String,
    bronze_object_key: String,
    valid_from_utc: chrono::DateTime<chrono::Utc>,
    ingested_at_utc: chrono::DateTime<chrono::Utc>,
    inputs: super::committed_inputs::SemanticInputs,
    historical_binding: Option<super::committed_inputs::HistoricalBinding>,
}

struct CancelOnDrop(Cancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn run(config: &ExportConfig, path: &Path) -> anyhow::Result<()> {
    let cancellation = Cancellation::default();
    let _guard = CancelOnDrop(cancellation.clone());
    let payload = prepare(config, path, &cancellation).await?;
    cancellation.check()?;
    // Publication only occurs on the live caller after all awaited hashing has finished.
    write_file_create_new(path, &payload)
}

async fn prepare(
    config: &ExportConfig,
    path: &Path,
    cancellation: &Cancellation,
) -> anyhow::Result<Vec<u8>> {
    let committed = config
        .committed_inputs
        .as_ref()
        .context("source inspection requires authenticated committed Bronze inputs")?;
    ensure!(
        config.exact_inputs.is_some() && config.max_rows.is_none(),
        "source inspection requires complete exact FLOOR/title inputs"
    );
    let (objects, titles) = prepare_inputs(config)?;
    output_layout::validate_inspection(config, path, &objects, &titles)?;
    let before =
        input_evidence::capture_selected_cancellable(&objects, &titles, cancellation).await?;
    committed.verify(config, &before)?;
    let after =
        input_evidence::capture_selected_cancellable(&objects, &titles, cancellation).await?;
    input_evidence::unchanged(&before, &after)?;
    cancellation.check()?;
    let receipt = Receipt {
        schema_version: 2,
        source_snapshot_id: config.source_snapshot_id.clone(),
        bronze_object_key: committed.floor_object_key().to_owned(),
        valid_from_utc: config.valid_from_utc,
        ingested_at_utc: config.ingested_at_utc,
        inputs: committed.semantic(),
        historical_binding: committed.historical_binding()?,
    };
    ensure!(
        receipt.source_snapshot_id == committed.source_snapshot_id()?
            && receipt.valid_from_utc == committed.valid_from_utc()?,
        "source inspection configuration diverges from authenticated Bronze identity"
    );
    let payload = serde_json::to_vec(&receipt)?;
    ensure!(
        payload.len() <= 16 * 1024,
        "source inspection receipt exceeds size limit"
    );
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::super::{
        committed_inputs::{CommittedInput, CommittedInputs},
        source_inputs::ExactInputs,
        OutputFormat, SourceSelector,
    };
    use super::*;
    use crate::building_register_source_role::SourceRole;
    use chrono::{NaiveDate, Utc};
    use collection_domain::{BronzeObject, SnapshotBasis, SnapshotGranularity};
    use foundation_shared_kernel::{
        ids::{BronzeObjectId, IngestionRunId, SourceCatalogId},
        ObjectKey,
    };
    use sha2::{Digest, Sha256};
    use std::io::Write as _;

    #[test]
    fn supplied_inspection_path_never_falls_back_to_export() -> anyhow::Result<()> {
        assert_eq!(parse_path(Err(VarError::NotPresent))?, None);
        assert_eq!(
            parse_path(Ok("receipt.json".to_owned()))?,
            Some(PathBuf::from("receipt.json"))
        );
        for value in ["", "   ", "\t\n", "bad\0path"] {
            assert!(parse_path(Ok(value.to_owned())).is_err());
        }
        assert!(
            parse_path(Err(VarError::NotUnicode(std::ffi::OsString::from(
                "invalid"
            ))))
            .is_err()
        );
        Ok(())
    }

    fn row(role: SourceRole, name: &str, bytes: &[u8]) -> anyhow::Result<BronzeObject> {
        let date = NaiveDate::from_ymd_opt(2099, 10, 1).context("date")?;
        let time = date.and_time(chrono::NaiveTime::MIN).and_utc();
        Ok(BronzeObject {
            id: BronzeObjectId::new(uuid::Uuid::from_u128(1)),
            source_catalog_id: SourceCatalogId::new(uuid::Uuid::from_u128(2)),
            ingestion_run_id: IngestionRunId::new(uuid::Uuid::from_u128(3)),
            source_record_id: None,
            source_partition_key: None,
            source_identity_key: "fixture".into(),
            dedupe_key: "fixture".into(),
            request_params: serde_json::json!({}),
            object_key: ObjectKey::parse(&format!("bronze/source={}/{name}", role.slug()))?,
            checksum_sha256: format!("{:x}", Sha256::digest(bytes)),
            content_type: "application/zip".into(),
            size_bytes: bytes.len() as u64,
            logical_record_count: None,
            collected_at: time,
            snapshot_period: Some("2099-10".into()),
            snapshot_date: date,
            snapshot_granularity: SnapshotGranularity::Month,
            snapshot_basis: SnapshotBasis::ProviderFilePeriod,
            provider_file_id: name.strip_suffix(".zip").map(str::to_owned),
            provider_file_name: Some(name.into()),
            provider_updated_at: None,
            effective_date: None,
            created_at: time,
        })
    }

    #[tokio::test]
    async fn inspection_creates_only_receipt_and_refuses_reuse_or_corruption() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let names = ["OPN20991020FLOOR.zip", "OPN20991020TITLE.zip"];
        let mut rows = Vec::new();
        for (role, name) in [(SourceRole::Floor, names[0]), (SourceRole::Title, names[1])] {
            let path = root
                .path()
                .join(format!("bronze/source={}/{name}", role.slug()));
            std::fs::create_dir_all(path.parent().context("parent")?)?;
            let mut zip = zip::ZipWriter::new(std::fs::File::create(&path)?);
            zip.start_file("empty.txt", zip::write::SimpleFileOptions::default())?;
            zip.write_all(b"\n")?;
            zip.finish()?;
            rows.push(row(role, name, &std::fs::read(path)?)?);
        }
        let committed = CommittedInputs {
            history: super::super::history_witness::fixture()?,
            floor: CommittedInput::from_object(&rows[0], rows[0].object_key.as_str(), names[0])?,
            title: CommittedInput::from_object(&rows[1], rows[1].object_key.as_str(), names[1])?,
        };
        let config = ExportConfig {
            bronze_local_object_root: root.path().to_owned(),
            source_selector: SourceSelector::Exact(SourceRole::Floor.slug().into()),
            exact_inputs: Some(ExactInputs {
                floor: names[0].into(),
                title: names[1].into(),
            }),
            committed_inputs: Some(committed.clone()),
            reuse_completed: false,
            output_path: root.path().join("out/rows.jsonl"),
            proposal_input_path: None,
            summary_path: Some(root.path().join("out/ready.json")),
            source_snapshot_id: committed.source_snapshot_id()?,
            valid_from_utc: committed.valid_from_utc()?,
            ingested_at_utc: Utc::now(),
            max_rows: None,
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: Some(SourceRole::Title.slug().into()),
        };
        let receipt = root.path().join("out/inspection.json");
        run(&config, &receipt).await?;
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(&receipt)?)?;
        assert_eq!(value["source_snapshot_id"], config.source_snapshot_id);
        assert!(value["historical_binding"].is_null());
        assert!(!config.output_path.exists());
        assert!(!config.summary_path.as_ref().context("summary")?.exists());
        assert!(run(&config, &receipt).await.is_err());
        assert!(run(&config, &root.path().join("bronze/receipt.json"))
            .await
            .is_err());
        let floor = root.path().join(rows[0].object_key.as_str());
        std::fs::write(&floor, b"corrupt")?;
        assert!(run(&config, &root.path().join("out/second.json"))
            .await
            .is_err());
        assert!(!root.path().join("out/second.json").exists());
        let cancelled = Cancellation::default();
        cancelled.cancel();
        assert!(
            prepare(&config, &root.path().join("out/cancelled.json"), &cancelled)
                .await
                .is_err()
        );
        assert!(!root.path().join("out/cancelled.json").exists());
        Ok(())
    }
}
