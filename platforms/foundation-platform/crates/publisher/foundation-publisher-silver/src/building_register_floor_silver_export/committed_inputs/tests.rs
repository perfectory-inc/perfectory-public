use super::super::history_witness::fixture as history_fixture;
use super::*;
use foundation_shared_kernel::{
    ids::{BronzeObjectId, IngestionRunId, SourceCatalogId},
    ObjectKey,
};

fn object(role: SourceRole, name: &str, bytes: &[u8]) -> anyhow::Result<BronzeObject> {
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
        object_key: ObjectKey::parse(&format!("bronze/source={}/{name}", role.slug()))
            .context("key")?,
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

fn selection_candidates() -> anyhow::Result<Vec<collection_application::ports::BronzeMonthCandidate>>
{
    [
        (SourceRole::Title, "OPN20991020TITLE.zip", b"title"),
        (SourceRole::Floor, "OPN20991020FLOOR.zip", b"floor"),
    ]
    .into_iter()
    .map(|(role, name, bytes)| {
        Ok(collection_application::ports::BronzeMonthCandidate {
            source_slug: role.slug().to_owned(),
            object: object(role, name, bytes)?,
        })
    })
    .collect()
}

#[test]
fn automatic_selection_preserves_identity_without_inventing_ingestion_time() -> anyhow::Result<()> {
    use super::super::selection;
    let candidates = selection_candidates()?;
    let selected = selection::from_candidates(candidates.clone(), history_fixture()?)?;
    assert_eq!(selected.floor_source_object, "OPN20991020FLOOR.zip");
    assert_eq!(selected.title_source_object, "OPN20991020TITLE.zip");
    assert_eq!(selected.retained_ingested_at_utc, None);
    let mut reordered = candidates;
    reordered.reverse();
    for item in &mut reordered {
        item.object.collected_at = Utc::now();
        item.object.created_at = Utc::now();
    }
    assert_eq!(
        selected.source_snapshot_id,
        selection::from_candidates(reordered, history_fixture()?)?.source_snapshot_id
    );
    let value: serde_json::Value = serde_json::from_slice(&selection::payload(&selected)?)?;
    assert!(value["retained_ingested_at_utc"].is_null());
    assert_eq!(value["committed_inputs"]["floor"]["size_bytes"], 5);
    assert_eq!(
        value["committed_inputs"]
            .as_object()
            .context("inputs")?
            .len(),
        2
    );
    assert_eq!(
        selected.committed_inputs.history().sha256(),
        history_fixture()?.sha256()
    );
    let mut oversized = selected;
    oversized.floor_source_object = "a".repeat(16 * 1024);
    assert!(selection::payload(&oversized).is_err());
    Ok(())
}

#[test]
fn automatic_selection_refuses_ambiguous_missing_or_day_mismatched_pairs() -> anyhow::Result<()> {
    use super::super::selection;
    let candidates = selection_candidates()?;
    assert!(selection::from_candidates(Vec::new(), history_fixture()?).is_err());
    assert!(selection::from_candidates(vec![candidates[0].clone()], history_fixture()?).is_err());
    assert!(selection::from_candidates(
        vec![candidates[0].clone(), candidates[0].clone()],
        history_fixture()?
    )
    .is_err());
    let mut ambiguous = candidates.clone();
    ambiguous.push(candidates[0].clone());
    assert!(selection::from_candidates(ambiguous, history_fixture()?).is_err());
    let mut different_day = candidates.clone();
    different_day[0].object = object(SourceRole::Title, "OPN20991021TITLE.zip", b"title")?;
    assert!(selection::from_candidates(different_day, history_fixture()?).is_err());
    let mut bad_evidence = candidates;
    bad_evidence[0].object.checksum_sha256 = "bad".into();
    assert!(selection::from_candidates(bad_evidence, history_fixture()?).is_err());
    Ok(())
}

#[test]
fn automatic_selection_retains_only_authenticated_historical_time() -> anyhow::Result<()> {
    use super::super::selection;
    let witness = history_fixture()?.binding().clone();
    let mut candidates = selection_candidates()?;
    for (candidate, semantic) in candidates
        .iter_mut()
        .zip([&witness.inputs.title, &witness.inputs.floor])
    {
        let row = &mut candidate.object;
        let name = format!("{}.zip", semantic.provider_file_id);
        row.object_key =
            ObjectKey::parse(&format!("bronze/source={}/{name}", candidate.source_slug))?;
        row.provider_file_id = Some(semantic.provider_file_id.clone());
        row.snapshot_date = semantic.provider_month;
        row.snapshot_period = Some(semantic.provider_month.format("%Y-%m").to_string());
        row.checksum_sha256.clone_from(&semantic.checksum_sha256);
        row.size_bytes = semantic.size_bytes;
    }
    let selected = selection::from_candidates(candidates.clone(), history_fixture()?)?;
    assert_eq!(selected.source_snapshot_id, witness.source_snapshot_id);
    assert_eq!(
        selected.retained_ingested_at_utc,
        Some(witness.ingested_at_utc)
    );
    candidates[0].object.checksum_sha256 = "a".repeat(64);
    assert!(selection::from_candidates(candidates, history_fixture()?).is_err());
    Ok(())
}

fn staging_config(root: &std::path::Path) -> anyhow::Result<ExportConfig> {
    let floor_name = "OPN20991020FLOOR.zip";
    let title_name = "OPN20991020TITLE.zip";
    let floor = object(SourceRole::Floor, floor_name, b"floor")?;
    let title = object(SourceRole::Title, title_name, b"title")?;
    let committed = CommittedInputs {
        history: history_fixture()?,
        floor: CommittedInput::from_object(&floor, floor.object_key.as_str(), floor_name)?,
        title: CommittedInput::from_object(&title, title.object_key.as_str(), title_name)?,
    };
    Ok(ExportConfig {
        bronze_local_object_root: root.to_owned(),
        source_selector: SourceSelector::Exact(SourceRole::Floor.slug().into()),
        exact_inputs: Some(ExactInputs {
            floor: floor_name.into(),
            title: title_name.into(),
        }),
        source_snapshot_id: committed.source_snapshot_id()?,
        valid_from_utc: committed.valid_from_utc()?,
        ingested_at_utc: committed.valid_from_utc()?,
        committed_inputs: Some(committed),
        reuse_completed: false,
        output_path: root.join("out/rows.jsonl"),
        proposal_input_path: None,
        summary_path: Some(root.join("out/ready.json")),
        max_rows: None,
        chunk_rows: None,
        output_format: super::super::OutputFormat::Jsonl,
        title_source_slug: Some(SourceRole::Title.slug().into()),
    })
}

#[test]
fn staging_exact_pair_then_reuse_never_opens_transport() -> anyhow::Result<()> {
    use super::super::{blocking::Cancellation, staging};
    let root = tempfile::tempdir()?;
    let config = staging_config(root.path())?;
    let cancellation = Cancellation::default();
    let mut keys = Vec::new();
    let pending = staging::prepare_with(&config, &cancellation, |key| {
        keys.push(key.to_owned());
        Ok(std::io::Cursor::new(if key.contains("FLOOR") {
            b"floor"
        } else {
            b"title"
        }))
    })?;
    assert_eq!(pending.len(), 2);
    assert_eq!(keys.len(), 2);
    assert!(keys
        .iter()
        .any(|key| key.ends_with("/OPN20991020FLOOR.zip")));
    assert!(keys
        .iter()
        .any(|key| key.ends_with("/OPN20991020TITLE.zip")));
    staging::publish(pending)?;
    let reused = staging::prepare_with(
        &config,
        &cancellation,
        |_| -> anyhow::Result<std::io::Cursor<Vec<u8>>> {
            anyhow::bail!("reuse must not open transport")
        },
    )?;
    assert!(reused.is_empty());
    assert!(!root.path().join("out").exists());
    Ok(())
}

#[test]
fn staging_rejects_unauthenticated_selection_and_layout_before_mutation() -> anyhow::Result<()> {
    use super::super::{blocking::Cancellation, staging};
    for case in 0..6 {
        let root = tempfile::tempdir()?;
        let mut config = staging_config(root.path())?;
        match case {
            0 => config.committed_inputs = None,
            1 => config.source_selector = SourceSelector::Prefix("hubgokr".into()),
            2 => config.output_path = root.path().join("bronze/output"),
            3 => config.max_rows = Some(1),
            4 => config.summary_path = Some(config.output_path.clone()),
            5 => {
                let inputs = config.committed_inputs.as_mut().context("fixture")?;
                inputs.floor.snapshot_date =
                    NaiveDate::from_ymd_opt(2026, 7, 1).context("month")?;
                inputs.title.snapshot_date = inputs.floor.snapshot_date;
            }
            _ => unreachable!(),
        }
        let mut opened = false;
        let result = staging::prepare_with(&config, &Cancellation::default(), |_| {
            opened = true;
            Ok(std::io::Cursor::new(b"never"))
        });
        assert!(result.is_err(), "case {case}");
        assert!(!opened, "case {case}");
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn corrupt_title_blocks_missing_floor_before_transport_or_writes() -> anyhow::Result<()> {
    use super::super::{blocking::Cancellation, staging};
    let root = tempfile::tempdir()?;
    let config = staging_config(root.path())?;
    let expected = config
        .committed_inputs
        .as_ref()
        .context("fixture")?
        .expected_evidence(&config);
    let title = &expected[1].path;
    std::fs::create_dir_all(title.parent().context("parent")?)?;
    std::fs::write(title, b"wrong")?;
    let mut opened = false;
    assert!(
        staging::prepare_with(&config, &Cancellation::default(), |_| {
            opened = true;
            Ok(std::io::Cursor::new(b"floor"))
        })
        .is_err()
    );
    assert!(!opened);
    assert!(!expected[0].path.parent().context("parent")?.exists());
    assert_eq!(std::fs::read(title)?, b"wrong");
    Ok(())
}

#[test]
fn second_download_failure_discards_first_verified_temporary() -> anyhow::Result<()> {
    use super::super::{blocking::Cancellation, staging};
    let root = tempfile::tempdir()?;
    let config = staging_config(root.path())?;
    let mut opened = 0;
    let result = staging::prepare_with(&config, &Cancellation::default(), |_| {
        opened += 1;
        Ok(std::io::Cursor::new(if opened == 1 {
            b"floor".as_slice()
        } else {
            b"bad"
        }))
    });
    assert!(result.is_err());
    assert_eq!(opened, 2);
    for item in config
        .committed_inputs
        .as_ref()
        .context("fixture")?
        .expected_evidence(&config)
    {
        assert!(!item.path.exists());
        assert_eq!(
            std::fs::read_dir(item.path.parent().context("parent")?)?.count(),
            0
        );
    }
    Ok(())
}

#[test]
fn committed_input_rejects_wrong_provider_or_corrupt_ledger_evidence() -> anyhow::Result<()> {
    let name = "OPN20991020FLOOR.zip";
    let row = object(SourceRole::Floor, name, b"floor")?;
    let parse =
        |row: &BronzeObject| CommittedInput::from_object(row, row.object_key.as_str(), name);
    parse(&row)?;
    let mut bad = row.clone();
    bad.provider_file_id = Some("other".into());
    assert!(parse(&bad).is_err());
    let mut bad = row.clone();
    bad.checksum_sha256 = "A".repeat(64);
    assert!(parse(&bad).is_err());
    let mut bad = row.clone();
    bad.snapshot_period = Some("2099-08".into());
    assert!(parse(&bad).is_err());
    let mut bad = row.clone();
    bad.snapshot_basis = SnapshotBasis::CollectedAtFallback;
    assert!(parse(&bad).is_err());
    let mut bad = row.clone();
    bad.size_bytes = 0;
    assert!(parse(&bad).is_err());
    assert!(CommittedInput::from_object(&row, "wrong-key", name).is_err());
    Ok(())
}

#[test]
fn committed_source_identity_and_valid_time_are_owned_by_bronze() -> anyhow::Result<()> {
    let floor = object(SourceRole::Floor, "OPN20991020FLOOR.zip", b"floor")?;
    let title = object(SourceRole::Title, "OPN20991020TITLE.zip", b"title")?;
    let inputs = CommittedInputs {
        history: history_fixture()?,
        floor: CommittedInput::from_object(
            &floor,
            floor.object_key.as_str(),
            "OPN20991020FLOOR.zip",
        )?,
        title: CommittedInput::from_object(
            &title,
            title.object_key.as_str(),
            "OPN20991020TITLE.zip",
        )?,
    };
    assert_eq!(
        inputs.valid_from_utc()?.to_rfc3339(),
        "2099-10-01T00:00:00+00:00"
    );
    let mut changed = inputs.clone();
    changed.title.checksum_sha256 = "b".repeat(64);
    assert_ne!(inputs.source_snapshot_id()?, changed.source_snapshot_id()?);
    let mut recovered = inputs.clone();
    recovered.floor.bronze_object_id = uuid::Uuid::from_u128(100);
    recovered.title.source_catalog_id = uuid::Uuid::from_u128(200);
    recovered.floor.object_key = "bronze/source=alternate/recovered.zip".into();
    assert_eq!(
        inputs.source_snapshot_id()?,
        recovered.source_snapshot_id()?
    );
    let mut changed = inputs.clone();
    changed.title.provider_file_id = "other-provider-file".into();
    assert_ne!(inputs.source_snapshot_id()?, changed.source_snapshot_id()?);
    let mut changed = inputs.clone();
    changed.title.snapshot_date = NaiveDate::from_ymd_opt(2099, 11, 1).context("month")?;
    assert_ne!(inputs.source_snapshot_id()?, changed.source_snapshot_id()?);
    Ok(())
}

#[test]
fn historical_binding_uses_actual_persisted_lineage_and_refuses_other_september_pair(
) -> anyhow::Result<()> {
    let mut floor = CommittedInput::from_object(
        &object(SourceRole::Floor, "OPN20991020FLOOR.zip", b"floor")?,
        "bronze/source=hubgokr__building_register_floor_overview/OPN20991020FLOOR.zip",
        "OPN20991020FLOOR.zip",
    )?;
    let mut title = CommittedInput::from_object(
        &object(SourceRole::Title, "OPN20991020TITLE.zip", b"title")?,
        "bronze/source=hubgokr__building_register_main/OPN20991020TITLE.zip",
        "OPN20991020TITLE.zip",
    )?;
    let binding = history_fixture()?.binding().clone();
    floor.provider_file_id = binding.inputs.floor.provider_file_id.clone();
    floor.snapshot_date = binding.inputs.floor.provider_month;
    floor.size_bytes = binding.inputs.floor.size_bytes;
    floor.checksum_sha256 = binding.inputs.floor.checksum_sha256.clone();
    title.provider_file_id = binding.inputs.title.provider_file_id.clone();
    title.snapshot_date = binding.inputs.title.provider_month;
    title.size_bytes = binding.inputs.title.size_bytes;
    title.checksum_sha256 = binding.inputs.title.checksum_sha256.clone();
    let inputs = CommittedInputs {
        floor,
        title,
        history: history_fixture()?,
    };
    assert_eq!(inputs.source_snapshot_id()?, binding.source_snapshot_id);
    assert_eq!(inputs.valid_from_utc()?, binding.valid_from_utc);
    assert_eq!(
        binding.valid_from_utc.to_rfc3339(),
        "2099-09-20T00:00:00+00:00"
    );
    assert_eq!(
        binding.ingested_at_utc.to_rfc3339(),
        "2099-09-29T17:53:49.231+00:00"
    );
    let mut changed = inputs;
    changed.title.checksum_sha256 = "a".repeat(64);
    assert!(changed.source_snapshot_id().is_err());
    changed.floor.snapshot_date = NaiveDate::from_ymd_opt(2026, 7, 1).context("older month")?;
    changed.title.snapshot_date = changed.floor.snapshot_date;
    assert!(changed.source_snapshot_id().is_err());
    Ok(())
}

#[tokio::test]
async fn different_bytes_are_rejected_before_any_output_is_created() -> anyhow::Result<()> {
    use super::super::{export_handoff, OutputFormat};
    let root = tempfile::tempdir()?;
    let floor_name = "OPN20991020FLOOR.zip";
    let title_name = "OPN20991020TITLE.zip";
    let floor = object(SourceRole::Floor, floor_name, b"floor")?;
    let title = object(SourceRole::Title, title_name, b"title")?;
    for (row, bytes) in [(&floor, b"WRONG"), (&title, b"title")] {
        let path = root.path().join(row.object_key.as_str());
        std::fs::create_dir_all(path.parent().context("parent")?)?;
        std::fs::write(path, bytes)?;
    }
    let committed = CommittedInputs {
        history: history_fixture()?,
        floor: CommittedInput::from_object(&floor, floor.object_key.as_str(), floor_name)?,
        title: CommittedInput::from_object(&title, title.object_key.as_str(), title_name)?,
    };
    let config = ExportConfig {
        bronze_local_object_root: root.path().to_owned(),
        source_selector: SourceSelector::Exact(SourceRole::Floor.slug().into()),
        exact_inputs: Some(ExactInputs {
            floor: floor_name.into(),
            title: title_name.into(),
        }),
        committed_inputs: Some(committed.clone()),
        reuse_completed: false,
        output_path: root.path().join("out/rows.jsonl"),
        proposal_input_path: None,
        summary_path: Some(root.path().join("out/ready.json")),
        source_snapshot_id: committed.source_snapshot_id()?,
        valid_from_utc: committed.valid_from_utc()?,
        ingested_at_utc: committed.valid_from_utc()?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(SourceRole::Title.slug().into()),
    };
    let error = export_handoff(&config)
        .await
        .err()
        .context("uncommitted bytes must fail")?;
    assert!(format!("{error:#}").contains("differ from committed Bronze ledger"));
    assert!(!root.path().join("out").exists());
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // End-to-end authenticated reuse and refusal fixture.
async fn completed_handoff_reuses_exact_bytes_and_rejects_changed_artifacts() -> anyhow::Result<()>
{
    use super::super::{export_handoff, OutputFormat};
    use std::io::Write as _;

    let root = tempfile::tempdir()?;
    let floor_name = "OPN20991020FLOOR.zip";
    let title_name = "OPN20991020TITLE.zip";
    let mut rows = Vec::new();
    for (role, name) in [
        (SourceRole::Floor, floor_name),
        (SourceRole::Title, title_name),
    ] {
        let path = root
            .path()
            .join(format!("bronze/source={}/{name}", role.slug()));
        std::fs::create_dir_all(path.parent().context("Bronze parent")?)?;
        let mut zip = zip::ZipWriter::new(std::fs::File::create(&path)?);
        zip.start_file("empty.txt", zip::write::SimpleFileOptions::default())?;
        if matches!(role, SourceRole::Floor) {
            let line = super::super::interleaved_group_tests::floor_line(
                "building-a",
                "10",
                "basement",
                "1",
                "1F",
            );
            zip.write_all(line.as_bytes())?;
        } else {
            zip.write_all(b"\n")?;
        }
        zip.finish()?;
        rows.push(object(role, name, &std::fs::read(&path)?)?);
    }
    let committed = CommittedInputs {
        history: history_fixture()?,
        floor: CommittedInput::from_object(&rows[0], rows[0].object_key.as_str(), floor_name)?,
        title: CommittedInput::from_object(&rows[1], rows[1].object_key.as_str(), title_name)?,
    };
    let output = root.path().join("out/rows.jsonl");
    let summary = root.path().join("out/ready.json");
    let mut config = ExportConfig {
        bronze_local_object_root: root.path().to_owned(),
        source_selector: SourceSelector::Exact(SourceRole::Floor.slug().into()),
        exact_inputs: Some(ExactInputs {
            floor: floor_name.into(),
            title: title_name.into(),
        }),
        committed_inputs: Some(committed.clone()),
        reuse_completed: false,
        output_path: output.clone(),
        proposal_input_path: Some(root.path().join("out/proposals.jsonl")),
        summary_path: Some(summary.clone()),
        source_snapshot_id: committed.source_snapshot_id()?,
        valid_from_utc: committed.valid_from_utc()?,
        ingested_at_utc: committed.valid_from_utc()?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(SourceRole::Title.slug().into()),
    };
    let first = export_handoff(&config).await?;
    let ready_bytes = std::fs::read(&summary)?;
    let output_bytes = std::fs::read(&output)?;
    assert!(export_handoff(&config).await.is_err());
    config.reuse_completed = true;
    assert_eq!(export_handoff(&config).await?, first);
    let retained_time = config.ingested_at_utc;
    config.ingested_at_utc += chrono::Duration::days(1);
    assert!(export_handoff(&config).await.is_err());
    super::super::completed_handoff::restore_time(&mut config)?;
    assert_eq!(config.ingested_at_utc, retained_time);
    assert_eq!(export_handoff(&config).await?, first);
    let mut different_source = config.clone();
    different_source.source_snapshot_id.push_str("-different");
    assert!(super::super::completed_handoff::restore_time(&mut different_source).is_err());
    assert_eq!(std::fs::read(&summary)?, ready_bytes);
    assert_eq!(std::fs::read(&output)?, output_bytes);
    let mut altered: serde_json::Value = serde_json::from_slice(&ready_bytes)?;
    altered["output"]["row_count"] = serde_json::json!(first.row_count + 1);
    std::fs::write(&summary, serde_json::to_vec(&altered)?)?;
    assert!(export_handoff(&config).await.is_err());
    std::fs::write(&summary, &ready_bytes)?;
    let proposal = config.proposal_input_path.as_ref().context("proposal")?;
    std::fs::write(proposal, b"changed")?;
    assert!(export_handoff(&config).await.is_err());
    assert_eq!(std::fs::read(&summary)?, ready_bytes);
    assert_eq!(std::fs::read(&output)?, output_bytes);
    config.proposal_input_path = None;
    config.output_path = root.path().join("without-proposals/rows.jsonl");
    config.summary_path = Some(root.path().join("without-proposals/ready.json"));
    let without_proposals = export_handoff(&config).await?;
    assert!(without_proposals.normalization_proposal_count > 0);
    assert_eq!(export_handoff(&config).await?, without_proposals);
    Ok(())
}
