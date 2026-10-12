use std::{fs, fs::File, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Utc};
use lakehouse_application::BuildingRegisterFloorSilverRow;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

use super::{build_schema, rows_to_batch, ParquetSilverRowWriter};
use crate::lakehouse_engine_contract::serving_parquet_policy;

#[test]
fn rejects_over_budget_row_before_creating_parquet() -> Result<()> {
    let root = tempfile::tempdir()?;
    let output = root.path().join("oversized.parquet");
    let target = serving_parquet_policy()?.row_group_target_bytes()?;
    let row = floor_row(1, "x".repeat(target + 1))?;
    let mut writer = ParquetSilverRowWriter::new(output.clone(), None)?;

    let result = writer.write_rows(std::slice::from_ref(&row));

    assert!(
        result.is_err(),
        "a raw field larger than the byte budget must be rejected by write_rows"
    );
    assert!(
        !output.exists(),
        "reject an oversized row before creating its Parquet file"
    );
    Ok(())
}

#[test]
fn splits_large_rows_into_byte_bounded_groups_without_changing_values() -> Result<()> {
    let root = tempfile::tempdir()?;
    let output = root.path().join("large-rows.parquet");
    let target = serving_parquet_policy()?.row_group_target_bytes()?;
    // Forty distinct 512 KiB values exceed the byte target while staying far
    // below the old 8,192-row batch limit. Every SHA-256 block varies by row
    // and block index, so SNAPPY cannot collapse a repeated-character fixture.
    let rows = (1..=40)
        .map(|index| floor_row(index, varied_raw_value(index, target / 16)))
        .collect::<Result<Vec<_>>>()?;
    let mut writer = ParquetSilverRowWriter::new(output.clone(), None)?;
    writer.write_rows(&rows)?;
    writer.flush()?;

    let expected = rows_to_batch(&rows, Arc::new(build_schema()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&output)?)?;
    let row_group_count = builder.metadata().row_groups().len();
    assert_eq!(
        builder.metadata().file_metadata().num_rows(),
        i64::try_from(rows.len())?
    );
    let mut offset = 0;
    for batch in builder.with_batch_size(7).build()? {
        let batch = batch?;
        let end = offset + batch.num_rows();
        assert!(end <= expected.num_rows(), "Parquet added logical rows");
        assert!(
            batch == expected.slice(offset, batch.num_rows()),
            "Parquet changed fields or row order at input rows {offset}..{end}"
        );
        offset = end;
    }
    assert_eq!(offset, rows.len(), "Parquet omitted logical rows");
    assert!(
        row_group_count > 1,
        "byte-sized row groups must split this input; found {row_group_count} group"
    );
    Ok(())
}

#[test]
fn preserves_file_chunks_and_the_final_partial_chunk() -> Result<()> {
    let root = tempfile::tempdir()?;
    let output = root.path().join("chunks");
    let rows = (1..=5)
        .map(|index| floor_row(index, format!("raw floor {index}")))
        .collect::<Result<Vec<_>>>()?;
    let expected = rows_to_batch(&rows, Arc::new(build_schema()))?;
    let mut writer = ParquetSilverRowWriter::new(output.clone(), Some(2))?;
    writer.write_rows(&rows)?;
    writer.flush()?;
    let mut offset = 0;
    for (index, expected_rows) in [2, 2, 1].into_iter().enumerate() {
        let file = output.join(format!("part-{:06}.parquet", index + 1));
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(file)?)?;
        assert_eq!(builder.metadata().file_metadata().num_rows(), expected_rows);
        for group in builder.metadata().row_groups() {
            for column in group.columns() {
                assert_eq!(column.compression(), parquet::basic::Compression::SNAPPY);
            }
        }
        for batch in builder.build()? {
            let batch = batch?;
            assert!(batch == expected.slice(offset, batch.num_rows()));
            offset += batch.num_rows();
        }
    }
    assert_eq!(offset, rows.len());
    assert_eq!(fs::read_dir(output)?.count(), 3);
    Ok(())
}

#[test]
fn empty_input_creates_no_parquet_file() -> Result<()> {
    let root = tempfile::tempdir()?;
    for (name, chunk_rows) in [("single.parquet", None), ("chunks", Some(2))] {
        let output = root.path().join(name);
        let mut writer = ParquetSilverRowWriter::new(output.clone(), chunk_rows)?;
        writer.write_rows(&[])?;
        writer.flush()?;
        writer.flush()?;
        if chunk_rows.is_some() {
            assert_eq!(fs::read_dir(output)?.count(), 0);
        } else {
            assert!(!output.exists());
        }
    }
    Ok(())
}

#[test]
fn zero_chunk_size_does_not_remove_existing_output() -> Result<()> {
    let root = tempfile::tempdir()?;
    let output = root.path().join("chunks");
    fs::create_dir(&output)?;
    let sentinel = output.join("part-000001.parquet");
    fs::write(&sentinel, b"existing output")?;

    assert!(ParquetSilverRowWriter::new(output, Some(0)).is_err());
    assert_eq!(fs::read(sentinel)?, b"existing output");
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn parquet_close_failure_prevents_export_ready_summary() -> Result<()> {
    use super::super::{export_handoff, ExportConfig, OutputFormat, SourceSelector};
    use anyhow::Context;

    let root = tempfile::tempdir()?;
    let source_slug = "datagokr__building_register_floor_overview";
    let source = root
        .path()
        .join("bronze")
        .join(format!("source={source_slug}"));
    fs::create_dir_all(&source)?;
    fs::write(
        source.join("floor.json"),
        serde_json::to_vec(&serde_json::json!({
            "response": {"body": {"items": {"item": [{
                "mgmBldrgstPk": "11680-10300-1",
                "flrGbCd": "20",
                "flrGbCdNm": "지상",
                "flrNo": "1",
                "flrNoNm": "1층"
            }]}}}
        }))?,
    )?;
    let output = root.path().join("full.parquet");
    std::os::unix::fs::symlink("/dev/full", &output)?;
    let summary = root.path().join("ready.json");
    let timestamp = DateTime::parse_from_rfc3339("2026-06-20T00:00:00Z")?.with_timezone(&Utc);
    let result = export_handoff(&ExportConfig {
        bronze_local_object_root: root.path().to_path_buf(),
        source_selector: SourceSelector::Exact(source_slug.to_owned()),
        exact_inputs: None,
        committed_inputs: None,
        reuse_completed: false,
        output_path: output,
        proposal_input_path: None,
        summary_path: Some(summary.clone()),
        source_snapshot_id: "fixture-current-floor".to_owned(),
        valid_from_utc: timestamp,
        ingested_at_utc: timestamp,
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Parquet,
        title_source_slug: None,
    })
    .await;

    let error = format!(
        "{:#}",
        result.err().context("failed output must fail export")?
    );
    assert!(error.contains("failed to flush building-register floor Silver handoff"));
    assert!(error.contains("failed to close Parquet writer"));
    assert!(!summary.exists());
    Ok(())
}

fn varied_raw_value(row_index: u16, bytes: usize) -> String {
    let mut value = String::with_capacity(bytes);
    let mut block_index = 0_u64;
    while value.len() < bytes {
        let mut digest = Sha256::new();
        digest.update(row_index.to_le_bytes());
        digest.update(block_index.to_le_bytes());
        let block = format!("{:x}", digest.finalize());
        value.push_str(&block[..(bytes - value.len()).min(block.len())]);
        block_index += 1;
    }
    value
}

fn floor_row(index: u16, raw_label: String) -> Result<BuildingRegisterFloorSilverRow> {
    let timestamp = DateTime::parse_from_rfc3339("2026-06-20T00:00:00Z")?.with_timezone(&Utc);
    Ok(BuildingRegisterFloorSilverRow {
        floor_row_id: format!("fixture-floor-{index:04}"),
        mgm_bldrgst_pk: "fixture-building-0001".to_owned(),
        floor_type_code_raw: "20".to_owned(),
        floor_type_name_raw: "지상".to_owned(),
        floor_number_raw: index.to_string(),
        floor_label_raw: Some(raw_label),
        floor_kind: "above_ground".to_owned(),
        floor_number: Some(index),
        floor_index: Some(i16::try_from(index)?),
        floor_display_ko: Some(format!("{index}층")),
        normalization_status: "normalized".to_owned(),
        normalization_reason: "fixture".to_owned(),
        source_record_id: format!("fixture-source-{index:04}"),
        source_snapshot_id: "fixture-snapshot".to_owned(),
        bronze_object_key: "bronze/fixture-floor.jsonl".to_owned(),
        source_line_number: Some(u64::from(index)),
        valid_from_utc: timestamp,
        valid_to_utc: None,
        ingested_at_utc: timestamp,
        row_checksum_sha256: format!("{index:064x}"),
    })
}
