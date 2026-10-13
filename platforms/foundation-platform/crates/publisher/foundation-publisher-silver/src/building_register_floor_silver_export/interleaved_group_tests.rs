//! End-to-end ZIP fixture for a building whose floor rows are not contiguous.

use super::{
    export_handoff, hub_bulk_decoder::HubBuildingRegisterFloorBulkDecoder,
    source_inputs::ExactInputs, title_counts_loader::load_building_title_floor_counts,
    ExportConfig, OutputFormat, SourceSelector, DEFAULT_TITLE_SOURCE_SLUG,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use lakehouse_application::{
    build_building_register_floor_normalization_proposal_input,
    build_building_register_floor_silver_handoff,
    normalize_building_register_floor_silver_rows_with_title_counts,
    BuildingRegisterFloorSilverRowsInput,
};
use sha2::{Digest, Sha256};
use std::{fs, io::Write as _, path::Path};
use zip::{write::SimpleFileOptions, ZipWriter};

#[tokio::test]
async fn escaped_floor_row_that_expands_past_sink_budget_has_no_ready_summary() -> anyhow::Result<()>
{
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let key = format!("bronze/source={slug}/OPN20990920FLOOR.zip");
    let floor_path = root.join(&key);
    // Raw owned source data fits the common group budget; JSON escaping expands
    // this control text beyond one bounded Silver row.
    let label = "\u{0001}".repeat(1_400_000);
    write_zip(
        &floor_path,
        "one.txt",
        floor_line("building-a", "20", "지상", "1", &label).as_bytes(),
    )?;
    let output_path = root.join("silver/floors.jsonl");
    let summary_path = root.join("audit/summary.json");
    fs::create_dir_all(
        output_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("output parent"))?,
    )?;
    let sentinel = root.join("silver/foreign-sentinel");
    fs::write(&sentinel, b"keep")?;
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(slug.to_owned()),
        exact_inputs: None,
        committed_inputs: None,
        reuse_completed: false,
        output_path,
        proposal_input_path: None,
        summary_path: Some(summary_path.clone()),
        source_snapshot_id: "synthetic-escaped-floor-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: None,
    };
    let error = export_handoff(&config)
        .await
        .err()
        .context("escaped Silver row must exceed bounded sink")?;
    assert!(format!("{error:#}").contains("serialized row exceeds byte bound"));
    assert!(!summary_path.exists());
    assert_eq!(fs::read(&sentinel)?, b"keep");
    assert!(fs::read_dir(root.join("silver"))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .all(|entry| !entry
            .file_name()
            .to_string_lossy()
            .starts_with(".floor-scratch-")));
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep the complete input/output equivalence scenario together.
async fn exact_zip_interleaved_building_matches_full_input_handoff_and_context(
) -> anyhow::Result<()> {
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let floor_slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let floor_name = "OPN20990920FLOOR.zip";
    let title_name = "OPN20990920TITLE.zip";
    let object_key = format!("bronze/source={floor_slug}/{floor_name}");
    let floor_path = root.join(&object_key);
    let title_path = root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/{title_name}"
    ));

    // A is unresolved first; B interrupts it; A's accepted witness arrives later.
    // The blank physical line must survive as a gap in source_line_number.
    let floor_text = [
        floor_line("building-a", "10", "지하", "1", "1층"),
        String::new(),
        floor_line("building-b", "20", "지상", "1", "1층"),
        floor_line("building-a", "10", "지하", "1", "지하층"),
    ]
    .join("\r\n");
    write_zip(&floor_path, "mart_djy_04.txt", floor_text.as_bytes())?;

    // The first parseable A title has no count evidence. Its positive duplicate
    // must not replace that first observation.
    let title_text = [
        title_line("building-a", "", "0"),
        title_line("building-b", "1", "0"),
        title_line("building-a", "2", "2"),
    ]
    .join("\n");
    write_zip(&title_path, "mart_djy_03.txt", title_text.as_bytes())?;

    let output_path = root.join("silver/floors.jsonl");
    let proposal_path = root.join("proposals/floors.jsonl");
    let summary_path = root.join("audit/summary.json");
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(floor_slug.to_owned()),
        committed_inputs: None,
        reuse_completed: false,
        exact_inputs: Some(ExactInputs {
            floor: floor_name.to_owned(),
            title: title_name.to_owned(),
        }),
        output_path: output_path.clone(),
        proposal_input_path: Some(proposal_path.clone()),
        summary_path: Some(summary_path.clone()),
        source_snapshot_id: "synthetic-interleaved-floor-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
    };

    let report = export_handoff(&config).await?;

    // The oracle receives all rows from the same physical ZIP at once, so its
    // building-level resolver and context builder see both A rows together.
    let mut source_rows = Vec::new();
    HubBuildingRegisterFloorBulkDecoder::decode_zip_rows(&floor_path, &object_key, None, |row| {
        source_rows.push(row);
        Ok(())
    })?;
    let title_counts = load_building_title_floor_counts(&[title_path])?;
    let expected_rows = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: &source_rows,
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key: &object_key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &title_counts,
    )?;
    let expected_handoff = build_building_register_floor_silver_handoff(&expected_rows)?;
    let expected_proposals =
        build_building_register_floor_normalization_proposal_input(&expected_rows)?;

    assert_eq!(
        fs::read(&proposal_path)?,
        expected_proposals.jsonl.as_bytes(),
        "proposal context must include the later accepted A floor in physical input order"
    );
    let actual_handoff = fs::read(&output_path)?;
    assert_eq!(
        actual_handoff,
        expected_handoff.jsonl.as_bytes(),
        "Silver JSONL must preserve all full-input fields, row checksums, lineage, and order"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&actual_handoff)),
        format!("{:x}", Sha256::digest(expected_handoff.jsonl.as_bytes())),
    );
    assert_eq!(report.row_count, 3);
    assert_eq!(report.proposal_required_count, 1);
    assert_eq!(
        report.normalization_proposal_count,
        expected_proposals.proposal_count
    );
    assert_eq!(
        source_rows
            .iter()
            .map(|row| row.source_line_number)
            .collect::<Vec<_>>(),
        [Some(1), Some(3), Some(4)]
    );
    assert_eq!(title_counts["building-a"].above_ground, None);
    assert_eq!(title_counts["building-a"].below_ground, None);
    assert_eq!(expected_rows[0].normalization_status, "proposal_required");
    assert_eq!(expected_rows[2].normalization_status, "accepted");
    let context: serde_json::Value = serde_json::from_str(
        expected_proposals
            .jsonl
            .lines()
            .next()
            .ok_or_else(|| anyhow::anyhow!("expected A proposal context"))?,
    )?;
    assert_eq!(
        context["same_building_floor_sequence"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("expected A floor sequence"))?
            .iter()
            .map(|row| row["source_line_number"].as_u64())
            .collect::<Vec<_>>(),
        [Some(1), Some(4)]
    );
    let summary: serde_json::Value = serde_json::from_slice(&fs::read(summary_path)?)?;
    let evidence = summary["selected_input_evidence"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("selected input evidence is required"))?;
    assert_eq!(evidence.len(), 2);
    for item in evidence {
        assert_eq!(item["sha256"].as_str().map(str::len), Some(64));
        assert!(item["size_bytes"].as_u64().is_some_and(|size| size > 0));
    }
    let parquet_root = root.join("silver/floors-parquet");
    let mut parquet_config = config.clone();
    parquet_config.output_path = parquet_root.clone();
    parquet_config.proposal_input_path = None;
    parquet_config.summary_path = None;
    parquet_config.output_format = OutputFormat::Parquet;
    parquet_config.chunk_rows = Some(1);
    assert_eq!(
        export_handoff(&parquet_config).await?.row_count,
        expected_rows.len()
    );
    for (index, expected) in expected_rows.iter().enumerate() {
        let path = parquet_root.join(format!("part-{:06}.parquet", index + 1));
        let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(path)?,
        )?
        .build()?;
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("missing parquet row"))?;
        assert_eq!(batch.num_rows(), 1);
        for (column, value) in [
            ("floor_row_id", expected.floor_row_id.as_str()),
            ("row_checksum_sha256", expected.row_checksum_sha256.as_str()),
            (
                "normalization_status",
                expected.normalization_status.as_str(),
            ),
            ("source_snapshot_id", expected.source_snapshot_id.as_str()),
            ("bronze_object_key", expected.bronze_object_key.as_str()),
            ("source_record_id", expected.source_record_id.as_str()),
        ] {
            assert_eq!(
                parquet_string(&batch, column)?,
                value,
                "Parquet logical row parity for {column}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Full input/output equivalence fixture.
async fn two_separated_proposals_use_complete_building_context_in_target_input_order(
) -> anyhow::Result<()> {
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let floor_slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let floor_name = "OPN20990920FLOOR.zip";
    let title_name = "OPN20990920TITLE.zip";
    let key = format!("bronze/source={floor_slug}/{floor_name}");
    let floor_path = root.join(&key);
    let title_path = root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/{title_name}"
    ));
    let lines = [
        floor_line("building-a", "10", "지하", "1", "1층"),
        floor_line("building-b", "20", "지상", "1", "1층"),
        floor_line("building-a", "10", "지하", "2", "2층"),
    ]
    .join("\n");
    write_zip(&floor_path, "mart_djy_04.txt", lines.as_bytes())?;
    write_zip(
        &title_path,
        "mart_djy_03.txt",
        title_line("building-b", "1", "0").as_bytes(),
    )?;
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(floor_slug.to_owned()),
        committed_inputs: None,
        reuse_completed: false,
        exact_inputs: Some(ExactInputs {
            floor: floor_name.to_owned(),
            title: title_name.to_owned(),
        }),
        output_path: root.join("silver/floors.jsonl"),
        proposal_input_path: Some(root.join("proposals/floors.jsonl")),
        summary_path: None,
        source_snapshot_id: "synthetic-two-proposals-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
    };
    let mut source_rows = Vec::new();
    HubBuildingRegisterFloorBulkDecoder::decode_zip_rows(&floor_path, &key, None, |row| {
        source_rows.push(row);
        Ok(())
    })?;
    let title_counts = load_building_title_floor_counts(&[title_path])?;
    let expected = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: &source_rows,
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key: &key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &title_counts,
    )?;
    assert_eq!(
        expected
            .iter()
            .map(|row| row.normalization_status.as_str())
            .collect::<Vec<_>>(),
        ["proposal_required", "accepted", "proposal_required"]
    );
    let handoff = build_building_register_floor_silver_handoff(&expected)?;
    let proposals = build_building_register_floor_normalization_proposal_input(&expected)?;
    assert_eq!(proposals.proposal_count, 2);
    let report = export_handoff(&config).await?;
    assert_eq!(report.normalization_proposal_count, 2);
    assert_eq!(fs::read(&config.output_path)?, handoff.jsonl.as_bytes());
    assert_eq!(
        fs::read(
            config
                .proposal_input_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("proposal path"))?
        )?,
        proposals.jsonl.as_bytes()
    );
    let parsed = proposals
        .jsonl
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        parsed
            .iter()
            .map(|value| value["target"]["source_line_number"].as_u64())
            .collect::<Vec<_>>(),
        [Some(1), Some(3)]
    );
    for proposal in parsed {
        assert_eq!(
            proposal["same_building_floor_sequence"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("floor sequence"))?
                .len(),
            2
        );
    }
    Ok(())
}

#[tokio::test]
async fn contiguous_zip_keeps_full_input_jsonl_bytes_and_sha() -> anyhow::Result<()> {
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let floor_slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let floor_name = "OPN20990920FLOOR.zip";
    let title_name = "OPN20990920TITLE.zip";
    let key = format!("bronze/source={floor_slug}/{floor_name}");
    let floor_path = root.join(&key);
    let title_path = root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/{title_name}"
    ));
    write_zip(
        &floor_path,
        "mart_djy_04.txt",
        [
            floor_line("building-a", "10", "지하", "1", "지하층"),
            floor_line("building-a", "10", "지하", "2", "2층"),
            floor_line("building-b", "20", "지상", "1", "1층"),
        ]
        .join("\n")
        .as_bytes(),
    )?;
    write_zip(
        &title_path,
        "mart_djy_03.txt",
        title_line("building-b", "1", "0").as_bytes(),
    )?;
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(floor_slug.to_owned()),
        committed_inputs: None,
        reuse_completed: false,
        exact_inputs: Some(ExactInputs {
            floor: floor_name.to_owned(),
            title: title_name.to_owned(),
        }),
        output_path: root.join("silver/floors.jsonl"),
        proposal_input_path: Some(root.join("proposals/floors.jsonl")),
        summary_path: None,
        source_snapshot_id: "synthetic-contiguous-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
    };
    let mut source_rows = Vec::new();
    HubBuildingRegisterFloorBulkDecoder::decode_zip_rows(&floor_path, &key, None, |row| {
        source_rows.push(row);
        Ok(())
    })?;
    let counts = load_building_title_floor_counts(&[title_path])?;
    let expected = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: &source_rows,
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key: &key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &counts,
    )?;
    let expected_handoff = build_building_register_floor_silver_handoff(&expected)?;
    let expected_proposals = build_building_register_floor_normalization_proposal_input(&expected)?;
    export_handoff(&config).await?;
    let actual = fs::read(&config.output_path)?;
    assert_eq!(actual, expected_handoff.jsonl.as_bytes());
    assert_eq!(
        format!("{:x}", Sha256::digest(&actual)),
        format!("{:x}", Sha256::digest(expected_handoff.jsonl.as_bytes()))
    );
    // Captured from the fixed three-row synthetic CLI fixture at frozen v4.
    // Its canonical row/context serializers match baseline46; no baseline
    // binary was executed to obtain these literals.
    assert_eq!(actual.len(), 2741);
    assert_eq!(std::str::from_utf8(&actual)?.lines().count(), 3);
    assert_eq!(
        format!("{:x}", Sha256::digest(&actual)),
        "1afb1aa3f25f082d5cea81f7c77b71f34b2500cfe25e9e4979694ddb31545832"
    );
    let actual_proposal = fs::read(
        config
            .proposal_input_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("proposal path"))?,
    )?;
    assert_eq!(actual_proposal, expected_proposals.jsonl.as_bytes());
    assert_eq!(actual_proposal.len(), 3983);
    assert_eq!(std::str::from_utf8(&actual_proposal)?.lines().count(), 1);
    assert_eq!(
        format!("{:x}", Sha256::digest(&actual_proposal)),
        "37567da8b8752dd5b74f1b9291eee76a64dc574ecd3c7da7ab22e8e4f366077d"
    );
    Ok(())
}

#[tokio::test]
async fn later_building_witness_resolves_earlier_proposal_across_interleaving() -> anyhow::Result<()>
{
    let first = floor_line("building-a", "20", "지상", "1", "2층");
    let lines = [
        first.clone(),
        floor_line("building-b", "20", "지상", "1", "1층"),
        floor_line("building-a", "20", "지상", "2", "2층"),
    ];
    assert_floor_export_matches_full_input(
        &lines,
        &[
            title_line("building-a", "2", "0"),
            title_line("building-b", "1", "0"),
        ],
        &["accepted", "accepted", "accepted"],
        0,
        Some("resolved_by_building_witness_majority"),
    )
    .await
}

#[tokio::test]
async fn multiple_attics_and_positive_first_title_duplicate_keep_full_group_proposals(
) -> anyhow::Result<()> {
    let lines = [
        floor_line("building-a", "20", "지상", "1", "1층"),
        floor_line("building-b", "20", "지상", "1", "1층"),
        floor_line("building-a", "20", "지상", "", "다락"),
        floor_line("building-a", "20", "지상", "", "다락"),
    ];
    assert_floor_export_matches_full_input(
        &lines,
        &[
            title_line("building-a", "2", "0"),
            title_line("building-b", "1", "0"),
            title_line("building-a", "3", "0"),
        ],
        &[
            "accepted",
            "accepted",
            "proposal_required",
            "proposal_required",
        ],
        2,
        None,
    )
    .await
}

#[allow(clippy::too_many_lines)] // Full input/output equivalence fixture.
async fn assert_floor_export_matches_full_input(
    lines: &[String],
    title_lines: &[String],
    expected_statuses: &[&str],
    proposal_count: usize,
    first_reason: Option<&str>,
) -> anyhow::Result<()> {
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let floor_name = "OPN20990920FLOOR.zip";
    let title_name = "OPN20990920TITLE.zip";
    let key = format!("bronze/source={slug}/{floor_name}");
    let floor_path = root.join(&key);
    let title_path = root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/{title_name}"
    ));
    write_zip(&floor_path, "mart_djy_04.txt", lines.join("\n").as_bytes())?;
    write_zip(
        &title_path,
        "mart_djy_03.txt",
        title_lines.join("\n").as_bytes(),
    )?;
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(slug.to_owned()),
        committed_inputs: None,
        reuse_completed: false,
        exact_inputs: Some(ExactInputs {
            floor: floor_name.to_owned(),
            title: title_name.to_owned(),
        }),
        output_path: root.join("silver/floors.jsonl"),
        proposal_input_path: Some(root.join("proposals/floors.jsonl")),
        summary_path: Some(root.join("audit/summary.json")),
        source_snapshot_id: "synthetic-complete-building-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
    };
    let mut sources = Vec::new();
    HubBuildingRegisterFloorBulkDecoder::decode_zip_rows(&floor_path, &key, None, |row| {
        sources.push(row);
        Ok(())
    })?;
    let counts = load_building_title_floor_counts(&[title_path])?;
    let expected = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: &sources,
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key: &key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &counts,
    )?;
    assert_eq!(
        expected
            .iter()
            .map(|row| row.normalization_status.as_str())
            .collect::<Vec<_>>(),
        expected_statuses
    );
    if let Some(reason) = first_reason {
        let isolated = normalize_building_register_floor_silver_rows_with_title_counts(
            &BuildingRegisterFloorSilverRowsInput {
                records: &sources[..1],
                source_snapshot_id: &config.source_snapshot_id,
                bronze_object_key: &key,
                valid_from_utc: config.valid_from_utc,
                ingested_at_utc: config.ingested_at_utc,
            },
            &counts,
        )?;
        assert_eq!(isolated[0].normalization_status, "proposal_required");
        assert_eq!(expected[0].normalization_reason, reason);
    }
    assert_eq!(counts["building-a"].above_ground, Some(2));
    let expected_handoff = build_building_register_floor_silver_handoff(&expected)?;
    let expected_proposals = build_building_register_floor_normalization_proposal_input(&expected)?;
    assert_eq!(
        usize::try_from(expected_proposals.proposal_count)?,
        proposal_count
    );
    let report = export_handoff(&config).await?;
    assert_eq!(report.row_count, lines.len());
    assert_eq!(
        usize::try_from(report.proposal_required_count)?,
        proposal_count
    );
    assert_eq!(
        fs::read(&config.output_path)?,
        expected_handoff.jsonl.as_bytes()
    );
    assert_eq!(
        fs::read(
            config
                .proposal_input_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("proposal path"))?
        )?,
        expected_proposals.jsonl.as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_live_floor_export_joins_owned_scratch_cleanup_without_ready_marker(
) -> anyhow::Result<()> {
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let floor_path = root.join(format!("bronze/source={slug}/OPN20990920FLOOR.zip"));
    let mut input = String::new();
    for ordinal in 0..10_000 {
        input.push_str(&floor_line(
            &format!("building-{ordinal}"),
            "20",
            "지상",
            "1",
            "1층",
        ));
        input.push('\n');
    }
    write_zip(&floor_path, "mart_djy_04.txt", input.as_bytes())?;
    let output_path = root.join("silver/floors.jsonl");
    let summary_path = root.join("audit/summary.json");
    fs::create_dir_all(
        output_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("output parent"))?,
    )?;
    let sentinel = root.join("silver/foreign-sentinel");
    fs::write(&sentinel, b"keep")?;
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(slug.to_owned()),
        exact_inputs: None,
        committed_inputs: None,
        reuse_completed: false,
        output_path: output_path.clone(),
        proposal_input_path: None,
        summary_path: Some(summary_path.clone()),
        source_snapshot_id: "synthetic-cancel-floor-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: None,
    };
    let task = tokio::spawn(async move { export_handoff(&config).await });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let scratch_path = loop {
        let found = fs::read_dir(root.join("silver"))?
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".floor-scratch-")
            })
            .map(|entry| entry.path());
        if output_path.exists() {
            if let Some(path) = found {
                break path;
            }
        }
        assert!(
            !task.is_finished(),
            "export completed before cancellation observation"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "scratch/output ownership observation timed out"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    };
    task.abort();
    assert!(task.await.is_err(), "aborted exporter must stop");
    while scratch_path.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(
        !scratch_path.exists(),
        "cancelled exporter must finish owned SQLite scratch cleanup"
    );
    assert!(
        !summary_path.exists(),
        "cancelled exporter must not publish ready"
    );
    assert_eq!(fs::read(&sentinel)?, b"keep");
    Ok(())
}

fn parquet_string(batch: &arrow_array::RecordBatch, column: &str) -> anyhow::Result<String> {
    let index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == column)
        .ok_or_else(|| anyhow::anyhow!("missing Parquet column {column}"))?;
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| anyhow::anyhow!("Parquet column {column} is not a string"))?;
    Ok(values.value(0).to_owned())
}

#[tokio::test]
async fn completed_floor_artifact_is_immutable_and_fresh_failure_cleans_owned_scratch(
) -> anyhow::Result<()> {
    let owned_root = tempfile::tempdir()?;
    let root = owned_root.path().to_path_buf();
    let floor_slug = crate::building_register_source_role::SourceRole::Floor.slug();
    let floor_name = "OPN20990920FLOOR.zip";
    let title_name = "OPN20990920TITLE.zip";
    let floor_path = root.join(format!("bronze/source={floor_slug}/{floor_name}"));
    let title_path = root.join(format!(
        "bronze/source={DEFAULT_TITLE_SOURCE_SLUG}/{title_name}"
    ));
    write_zip(
        &floor_path,
        "mart_djy_04.txt",
        floor_line("building-a", "20", "지상", "1", "1층").as_bytes(),
    )?;
    write_zip(
        &title_path,
        "mart_djy_03.txt",
        title_line("building-a", "1", "0").as_bytes(),
    )?;
    let output_path = root.join("silver/floors.jsonl");
    let summary_path = root.join("audit/summary.json");
    fs::create_dir_all(
        summary_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("summary parent"))?,
    )?;
    fs::write(&summary_path, br#"{"status":"ready"}"#)?;
    fs::create_dir_all(
        output_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("output parent"))?,
    )?;
    fs::write(&output_path, b"completed-output")?;
    let sentinel = root.join("silver/foreign-sentinel");
    fs::create_dir_all(
        sentinel
            .parent()
            .ok_or_else(|| anyhow::anyhow!("sentinel parent"))?,
    )?;
    fs::write(&sentinel, b"keep")?;
    let config = ExportConfig {
        bronze_local_object_root: root.clone(),
        source_selector: SourceSelector::Exact(floor_slug.to_owned()),
        committed_inputs: None,
        reuse_completed: false,
        exact_inputs: Some(ExactInputs {
            floor: floor_name.to_owned(),
            title: title_name.to_owned(),
        }),
        output_path: output_path.clone(),
        proposal_input_path: None,
        summary_path: Some(summary_path.clone()),
        source_snapshot_id: "synthetic-interleaved-floor-20990920".to_owned(),
        valid_from_utc: utc("2099-09-20T00:00:00Z")?,
        ingested_at_utc: utc("2099-09-21T00:00:00Z")?,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
    };
    assert!(
        export_handoff(&config).await.is_err(),
        "completed artifact may not be overwritten"
    );
    assert_eq!(fs::read(&summary_path)?, br#"{"status":"ready"}"#);
    assert_eq!(fs::read(&output_path)?, b"completed-output");
    fs::remove_file(&summary_path)?;
    write_zip(&floor_path, "mart_djy_04.txt", b"short|invalid\n")?;
    assert!(
        export_handoff(&config).await.is_err(),
        "malformed floor row must fail"
    );
    assert!(
        !summary_path.exists(),
        "fresh failure must not publish ready summary"
    );
    assert_eq!(fs::read(&sentinel)?, b"keep");
    assert!(fs::read_dir(root.join("silver"))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .all(|entry| !entry
            .file_name()
            .to_string_lossy()
            .starts_with(".floor-scratch-")));
    Ok(())
}

pub(super) fn floor_line(pk: &str, code: &str, kind: &str, number: &str, label: &str) -> String {
    let mut fields = vec![String::new(); 22];
    fields[0] = pk.to_owned();
    fields[18] = code.to_owned();
    fields[19] = kind.to_owned();
    fields[20] = number.to_owned();
    fields[21] = label.to_owned();
    fields.join("|")
}

fn title_line(pk: &str, ground: &str, basement: &str) -> String {
    let mut fields = vec![String::new(); 45];
    fields[0] = pk.to_owned();
    fields[43] = ground.to_owned();
    fields[44] = basement.to_owned();
    fields.join("|")
}

fn utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn write_zip(path: &Path, entry_name: &str, content: &[u8]) -> anyhow::Result<()> {
    fs::create_dir_all(path.parent().ok_or_else(|| anyhow::anyhow!("ZIP parent"))?)?;
    let file = fs::File::create(path)?;
    let mut archive = ZipWriter::new(file);
    archive.start_file(entry_name, SimpleFileOptions::default())?;
    archive.write_all(content)?;
    archive.finish()?;
    Ok(())
}
