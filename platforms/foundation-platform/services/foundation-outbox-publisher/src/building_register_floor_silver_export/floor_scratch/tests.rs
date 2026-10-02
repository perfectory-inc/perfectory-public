use super::*;
use std::io::Write as _;
use zip::{write::SimpleFileOptions, ZipWriter};

#[test]
fn floor_stage_uses_duckdb() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let scratch = FloorScratch::new(
        &root.path().join("floor.jsonl"),
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
    )?;
    let version = scratch
        .connection()?
        .query_row("SELECT version()", [], |row| row.get::<_, String>(0));
    scratch.close()?;
    assert_eq!(version?, "v1.5.6");
    Ok(())
}

fn zip(path: &Path, text: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(path.parent().context("ZIP parent")?)?;
    let mut archive = ZipWriter::new(std::fs::File::create(path)?);
    archive.start_file("one.txt", SimpleFileOptions::default())?;
    archive.write_all(text.as_bytes())?;
    archive.finish()?;
    Ok(())
}

fn floor_line(pk: &str) -> String {
    let mut fields = vec![String::new(); 22];
    fields[0] = pk.to_owned();
    fields[18] = "20".to_owned();
    fields[19] = "지상".to_owned();
    fields[20] = "1".to_owned();
    fields[21] = "1층".to_owned();
    fields.join("|")
}

fn title_line(pk: &str, ground: &str, basement: &str) -> String {
    let mut fields = vec![String::new(); 45];
    fields[0] = pk.to_owned();
    fields[43] = ground.to_owned();
    fields[44] = basement.to_owned();
    fields.join("|")
}

fn config(root: &Path) -> anyhow::Result<ExportConfig> {
    let utc =
        chrono::DateTime::parse_from_rfc3339("2099-09-20T00:00:00Z")?.with_timezone(&chrono::Utc);
    Ok(ExportConfig {
        bronze_local_object_root: root.to_owned(),
        source_selector: super::super::SourceSelector::Exact(
            "hubgokr__building_register_floor_overview".to_owned(),
        ),
        exact_inputs: None,
        committed_inputs: None,
        reuse_completed: false,
        output_path: root.join("silver/floor.jsonl"),
        proposal_input_path: None,
        summary_path: None,
        source_snapshot_id: "fixture-floor-v2".to_owned(),
        valid_from_utc: utc,
        ingested_at_utc: utc,
        max_rows: None,
        chunk_rows: None,
        output_format: super::super::OutputFormat::Jsonl,
        title_source_slug: None,
    })
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the complete input/output equivalence scenario together.
fn interleaved_rows_preserve_exact_export_bytes() -> anyhow::Result<()> {
    use super::super::OutputFormat;
    use lakehouse_application::{
        build_building_register_floor_normalization_proposal_input,
        build_building_register_floor_silver_handoff,
    };

    const BUILDINGS: usize = 1280;
    const MAXIMUM_BYTES: u64 = 2 * 1024 * 1024;
    let root = tempfile::tempdir()?;
    let path = root.path().join("floor.zip");
    let title_path = root.path().join("title.zip");
    let key = "bronze/source=hubgokr__building_register_floor_overview/OPN20990920FLOOR.zip";
    let mut lines = Vec::new();
    let mut titles = Vec::new();
    let mut title_counts = HashMap::new();
    // Each pass interleaves every building. The unresolved basement label
    // precedes its accepted witness by two passes, with physical line gaps.
    for pass in 0..3 {
        for building in 0..BUILDINGS {
            let pk = format!("building-{building:04}");
            let mut fields = vec![String::new(); 22];
            fields[0] = pk.clone();
            match pass {
                0 => {
                    fields[18] = "10".to_owned();
                    fields[19] = "지하".to_owned();
                    fields[20] = "1".to_owned();
                    fields[21] = "1층".to_owned();
                    // First-observation semantics also remain active while
                    // title_counts, source_rows and resolved_rows coexist.
                    let title = title_line(&pk, "", "0");
                    let (pk, counts) =
                        parse_building_title_floor_counts_from_hub_bulk_text_line(&title)
                            .context("fixture title must parse")?;
                    title_counts.insert(pk, counts);
                    titles.push(title);
                }
                1 => {
                    let number = building % 7 + 1;
                    fields[18] = "20".to_owned();
                    fields[19] = "지상".to_owned();
                    fields[20] = number.to_string();
                    fields[21] = format!("{number}층");
                }
                _ => {
                    fields[18] = "10".to_owned();
                    fields[19] = "지하".to_owned();
                    fields[20] = "1".to_owned();
                    fields[21] = "지하층".to_owned();
                }
            }
            lines.push(fields.join("|"));
            if building % 17 == 0 {
                lines.push(String::new());
            }
        }
    }
    zip(&path, &lines.join("\r\n"))?;
    zip(&title_path, &titles.join("\n"))?;
    let source_rows = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| !line.is_empty())
        .map(|(index, line)| {
            parse_building_register_floor_source_row_from_hub_bulk_text_line(
                line,
                key,
                (index + 1) as u64,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut config = config(root.path())?;
    let expected_rows = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: &source_rows,
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key: key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &title_counts,
    )?;
    let expected_jsonl = build_building_register_floor_silver_handoff(&expected_rows)?;
    let expected_proposals =
        build_building_register_floor_normalization_proposal_input(&expected_rows)?;
    assert_eq!(expected_rows.len(), BUILDINGS * 3);
    assert_eq!(expected_proposals.proposal_count, BUILDINGS as u64);
    assert!(
        expected_jsonl.jsonl.len() + expected_proposals.jsonl.len() > CURSOR_BYTES,
        "fixture must commit output while the separate source stream is still active"
    );

    for (format, extension) in [
        (OutputFormat::Jsonl, "jsonl"),
        (OutputFormat::Parquet, "parquet"),
    ] {
        config.output_format = format;
        config.output_path = root.path().join(format!("silver/floor.{extension}"));
        let proposal_path = root.path().join(format!("proposals/{extension}.jsonl"));
        let mut output = SilverRowWriter::new(&config.output_path, None, format)?;
        let mut proposals = JsonlRowWriter::new(&proposal_path, None)?;
        let mut scratch = FloorScratch::new(&config.output_path, MAXIMUM_BYTES)?;
        scratch.load_titles(std::slice::from_ref(&title_path))?;
        let report = scratch
            .export_object(&config, &path, key, &mut output, Some(&mut proposals), None)
            .context("complete interleaved FLOOR export must preserve its output")?;
        output.flush()?;
        proposals.flush()?;
        assert_eq!(report.row_count, source_rows.len());
        assert_eq!(
            report.proposal_required_count,
            expected_proposals.proposal_count
        );
        assert_eq!(
            report.normalization_proposal_count,
            expected_proposals.proposal_count
        );
        assert_eq!(
            std::fs::read(&proposal_path)?,
            expected_proposals.jsonl.as_bytes()
        );

        let expected_path = root.path().join(format!("expected/floor.{extension}"));
        let mut expected_output = SilverRowWriter::new(&expected_path, None, format)?;
        for row in &expected_rows {
            expected_output.write_rows(std::slice::from_ref(row))?;
        }
        expected_output.flush()?;
        assert_eq!(
            std::fs::read(&config.output_path)?,
            std::fs::read(&expected_path)?,
            "{extension} must preserve every field, source ordinal and checksum"
        );
        if format == OutputFormat::Jsonl {
            assert_eq!(
                std::fs::read(&config.output_path)?,
                expected_jsonl.jsonl.as_bytes()
            );
        }
        scratch.close()?;
    }
    Ok(())
}

#[test]
fn title_stage_keeps_first_parseable_observation_in_decoder_order() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("title.zip");
    zip(
        &path,
        &[
            "short".to_owned(),
            title_line("", "7", "7"),
            title_line("building-a", "", "0"),
            title_line("building-b", " 3 ", " 1 "),
            title_line("building-a", "8", "2"),
            title_line("building-b", "", "0"),
            title_line("building-c", "70000", "0"),
            title_line("building-c", "2", "1"),
        ]
        .join("\n"),
    )?;
    let output = root.path().join("silver/floor.jsonl");
    let scratch = FloorScratch::new(&output, serving_scratch::DEFAULT_MAX_DATABASE_BYTES)?;
    scratch.load_titles(&[path])?;
    let a: (Option<i64>, Option<i64>) = scratch.connection()?.query_row(
        "SELECT above_ground, below_ground FROM title_counts WHERE pk = 'building-a'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let b: (Option<i64>, Option<i64>) = scratch.connection()?.query_row(
        "SELECT above_ground, below_ground FROM title_counts WHERE pk = 'building-b'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let c: (Option<i64>, Option<i64>) = scratch.connection()?.query_row(
        "SELECT above_ground, below_ground FROM title_counts WHERE pk = 'building-c'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(a, (None, None));
    assert_eq!(b, (Some(3), Some(1)));
    assert_eq!(c, (None, None));
    scratch.close()?;
    assert!(
        std::fs::read_dir(output.parent().context("output parent")?)?
            .next()
            .is_none()
    );
    Ok(())
}

#[test]
fn complete_group_budget_rejects_before_hydration_or_output() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("floor.zip");
    let line = floor_line("building-a");
    zip(&path, &format!("{line}\n{line}\n"))?;
    let key = "bronze/source=hubgokr__building_register_floor_overview/OPN20990920FLOOR.zip";
    let row = parse_building_register_floor_source_row_from_hub_bulk_text_line(&line, key, 1)?;
    let each = accounted_source_bytes(&row)?;
    let config = config(root.path())?;
    let mut scratch = FloorScratch::new_with_cursor_bytes(
        &config.output_path,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
        each + 1,
    )?;
    assert_eq!(scratch.stage_source_rows(&path, key, None)?, 2);
    let error = scratch
        .resolve_groups(&config, key)
        .err()
        .context("two complete rows exceed one-row budget")?;
    assert!(error
        .to_string()
        .contains("complete floor building group exceeds byte bound"));
    let count: (i64,) =
        (scratch
            .connection()?
            .query_row("SELECT COUNT(*) FROM resolved_rows", [], |row| row.get(0))?,);
    assert_eq!(count.0, 0, "budget rejection must precede any group output");
    scratch.close()?;
    assert!(
        std::fs::read_dir(config.output_path.parent().context("output parent")?)?
            .next()
            .is_none()
    );
    Ok(())
}

#[test]
fn small_resolved_building_groups_preserve_output() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("floor.zip");
    let first = floor_line("building-a");
    let second = floor_line("building-b");
    zip(&path, &format!("{first}\n{second}\n"))?;
    let key = "bronze/source=hubgokr__building_register_floor_overview/OPN20990920FLOOR.zip";
    let a = parse_building_register_floor_source_row_from_hub_bulk_text_line(&first, key, 1)?;
    let b = parse_building_register_floor_source_row_from_hub_bulk_text_line(&second, key, 2)?;
    assert!(
        accounted_source_bytes(&a)? + accounted_source_bytes(&b)? < CURSOR_BYTES,
        "both small buildings must fit one byte batch"
    );
    let config = config(root.path())?;
    let mut scratch = FloorScratch::new(
        &config.output_path,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
    )?;
    let mut output =
        SilverRowWriter::new(&config.output_path, None, super::super::OutputFormat::Jsonl)?;
    let report = scratch
        .export_object(&config, &path, key, &mut output, None, None)
        .context("two small building groups should commit together")?;
    assert_eq!(report.row_count, 2);
    output.flush()?;
    scratch.close()?;
    assert_eq!(
        std::fs::read_to_string(&config.output_path)?
            .lines()
            .count(),
        2
    );
    Ok(())
}

#[test]
fn owned_string_bytes_make_one_wide_row_cost_more_than_three_narrow_rows() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let key = "bronze/source=hubgokr__building_register_floor_overview/OPN20990920FLOOR.zip";
    let narrow = floor_line("building-a");
    let wide = narrow.replace("1층", &"x".repeat(8192));
    let narrow_row =
        parse_building_register_floor_source_row_from_hub_bulk_text_line(&narrow, key, 1)?;
    let wide_row = parse_building_register_floor_source_row_from_hub_bulk_text_line(&wide, key, 1)?;
    let narrow_bytes = accounted_source_bytes(&narrow_row)?;
    let wide_bytes = accounted_source_bytes(&wide_row)?;
    assert_eq!(wide_bytes - narrow_bytes, 8192 - "1층".len());
    let budget = (narrow_bytes * 3).max(4096);
    assert!(wide_bytes > budget);

    let narrow_path = root.path().join("narrow.zip");
    zip(&narrow_path, &format!("{narrow}\n{narrow}\n{narrow}\n"))?;
    let config = config(root.path())?;
    let mut scratch = FloorScratch::new_with_cursor_bytes(
        &config.output_path,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
        budget,
    )?;
    assert_eq!(scratch.stage_source_rows(&narrow_path, key, None)?, 3);
    scratch.resolve_groups(&config, key)?;
    scratch.close()?;

    let wide_path = root.path().join("wide.zip");
    zip(&wide_path, &wide)?;
    let scratch = FloorScratch::new_with_cursor_bytes(
        &config.output_path,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
        budget,
    )?;
    let error = scratch
        .stage_source_rows(&wide_path, key, None)
        .err()
        .context("wide owned label must exceed budget")?;
    assert!(error
        .to_string()
        .contains("floor source row exceeds byte bound"));
    let staged: (i64,) =
        (scratch
            .connection()?
            .query_row("SELECT COUNT(*) FROM source_rows", [], |row| row.get(0))?,);
    assert_eq!(staged.0, 0);
    scratch.close()?;
    Ok(())
}

#[test]
fn single_record_budget_rejects_before_stage_write() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("floor.zip");
    let line = floor_line("building-a");
    zip(&path, &line)?;
    let key = "bronze/source=hubgokr__building_register_floor_overview/OPN20990920FLOOR.zip";
    let row = parse_building_register_floor_source_row_from_hub_bulk_text_line(&line, key, 1)?;
    let limit = accounted_source_bytes(&row)? - 1;
    let output = root.path().join("silver/floor.jsonl");
    let scratch = FloorScratch::new_with_cursor_bytes(
        &output,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
        limit,
    )?;
    let error = scratch
        .stage_source_rows(&path, key, None)
        .err()
        .context("row exceeds tiny budget")?;
    assert!(error
        .to_string()
        .contains("floor source row exceeds byte bound"));
    let count: (i64,) =
        (scratch
            .connection()?
            .query_row("SELECT COUNT(*) FROM source_rows", [], |row| row.get(0))?,);
    assert_eq!(count.0, 0);
    scratch.close()?;
    Ok(())
}

#[test]
fn proposal_byte_budget_rejects_before_resolved_stage_write() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("floor.zip");
    let mut fields = vec![String::new(); 22];
    fields[0] = "building-a".to_owned();
    fields[18] = "10".to_owned();
    fields[19] = "지하".to_owned();
    fields[20] = "1".to_owned();
    fields[21] = "1층".to_owned();
    let line = fields.join("|");
    zip(&path, &line)?;
    let key = "bronze/source=hubgokr__building_register_floor_overview/OPN20990920FLOOR.zip";
    let source = parse_building_register_floor_source_row_from_hub_bulk_text_line(&line, key, 1)?;
    let config = config(root.path())?;
    let rows = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: std::slice::from_ref(&source),
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key: key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &HashMap::new(),
    )?;
    assert_eq!(rows[0].normalization_status, "proposal_required");
    let mut proposal = Vec::new();
    write_building_register_floor_entity_context_pack_line(&rows[0], &[&rows[0]], &mut proposal)?;
    let limit = proposal.len() - 1;
    assert!(accounted_source_bytes(&source)? <= limit);
    assert!(serde_json::to_vec(&rows[0])?.len() <= limit);
    let mut scratch = FloorScratch::new_with_cursor_bytes(
        &config.output_path,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
        limit,
    )?;
    assert_eq!(scratch.stage_source_rows(&path, key, None)?, 1);
    let error = scratch
        .resolve_groups(&config, key)
        .err()
        .context("proposal exceeds tiny sink budget")?;
    assert!(error.to_string().contains("byte bound"));
    let count: (i64,) =
        (scratch
            .connection()?
            .query_row("SELECT COUNT(*) FROM resolved_rows", [], |row| row.get(0))?,);
    assert_eq!(count.0, 0);
    scratch.close()?;
    Ok(())
}

#[test]
fn spill_exhaustion_keeps_foreign_sentinel_and_closes_own_scratch() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let output = root.path().join("silver/floor.jsonl");
    std::fs::create_dir_all(output.parent().context("output parent")?)?;
    let sentinel = output.parent().context("output parent")?.join("sentinel");
    std::fs::write(&sentinel, b"keep")?;
    // Hold data, memory and CPU constant; change only the spill cap.
    for maximum_bytes in [64 * 1024 * 1024, 64 * 1024] {
        let scratch = FloorScratch::new(&output, maximum_bytes)?;
        scratch
            .connection()?
            .execute_batch("SET memory_limit='16MiB'; SET threads=1")?;
        let result = (|| -> anyhow::Result<()> {
            scratch.connection()?.execute_batch(
                "CREATE TABLE pressure AS SELECT i, repeat(md5(i::VARCHAR),32) AS payload FROM range(32768) t(i)"
            )?;
            let mut statement = scratch
                .connection()?
                .prepare("SELECT i FROM pressure ORDER BY i DESC")?;
            let _ = statement.stream_arrow([])?;
            while statement.step()?.is_some() {}
            Ok(())
        })();
        if maximum_bytes == 64 * 1024 {
            let settings: (String, String, String) = scratch.connection()?.query_row("SELECT current_setting('max_temp_directory_size'), current_setting('memory_limit'), current_setting('temp_directory')", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            let files = std::fs::read_dir(&settings.2)?
                .map(|entry| Ok::<_, std::io::Error>(entry?.metadata()?.len()))
                .sum::<Result<u64, _>>()?;
            assert!(
                files <= maximum_bytes,
                "remaining spill files exceeded cap: {files}"
            );
            let error = result.err().with_context(|| {
                format!("small spill cap must stop the same workload: {settings:?}, files={files}")
            })?;
            assert!(
                error.to_string().contains("max_temp_directory_size"),
                "expected spill exhaustion, got: {error}"
            );
        } else {
            result?;
            let directory = &scratch
                .resources
                .as_ref()
                .context("scratch resources")?
                .directory;
            let bytes = std::fs::read_dir(directory.path())?
                .map(|entry| Ok::<_, std::io::Error>(entry?.metadata()?.len()))
                .sum::<Result<u64, _>>()?;
            assert!(
                bytes > 64 * 1024 && bytes <= maximum_bytes,
                "control must actually spill: {bytes}"
            );
        }
        scratch.close()?;
    }
    assert_eq!(std::fs::read(&sentinel)?, b"keep");
    let entries = std::fs::read_dir(output.parent().context("output parent")?)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(entries, [std::ffi::OsString::from("sentinel")]);
    Ok(())
}

#[test]
fn identifier_bytes_and_first_title_across_files_are_preserved() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let first = root.path().join("first.zip");
    let second = root.path().join("second.zip");
    let identifiers = ["0001", "1", "0001\0suffix"];
    zip(
        &first,
        &identifiers
            .iter()
            .map(|pk| title_line(pk, "", "0"))
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    zip(
        &second,
        &identifiers
            .iter()
            .map(|pk| title_line(pk, "9", "2"))
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    let config = config(root.path())?;
    let mut scratch = FloorScratch::new(
        &config.output_path,
        serving_scratch::DEFAULT_MAX_DATABASE_BYTES,
    )?;
    scratch.load_titles(&[first, second])?;
    let count: i64 =
        scratch
            .connection()?
            .query_row("SELECT count(*) FROM title_counts", [], |row| row.get(0))?;
    assert_eq!(count, 3);
    for pk in identifiers {
        let counts: (Option<i64>, Option<i64>) = scratch.connection()?.query_row(
            "SELECT above_ground, below_ground FROM title_counts WHERE pk=?",
            [pk],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(counts, (None, None));
    }
    let path = root.path().join("floor.zip");
    zip(
        &path,
        &identifiers
            .iter()
            .map(|pk| floor_line(pk))
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    let mut output =
        SilverRowWriter::new(&config.output_path, None, super::super::OutputFormat::Jsonl)?;
    assert_eq!(
        scratch
            .export_object(&config, &path, "fixture-key", &mut output, None, None)?
            .row_count,
        3
    );
    output.flush()?;
    let rows = std::fs::read_to_string(&config.output_path)?
        .lines()
        .map(serde_json::from_str::<BuildingRegisterFloorSilverRow>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        rows.iter()
            .map(|row| row.mgm_bldrgst_pk.as_str())
            .collect::<Vec<_>>(),
        identifiers
    );
    scratch.close()?;
    Ok(())
}

#[test]
fn interrupted_fetch_returns_error_and_detaches_before_cleanup() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let cancellation = Cancellation::default();
    let scratch = FloorScratch::with_cancellation(
        &root.path().join("floor.jsonl"),
        1024 * 1024,
        cancellation.clone(),
    )?;
    let mut statement = scratch
        .connection()?
        .prepare("SELECT i FROM range(100000000) t(i)")?;
    let _ = statement.stream_arrow([])?;
    assert!(statement.step()?.is_some());
    cancellation.cancel();
    let error = statement
        .step()
        .err()
        .context("cancelled fetch must fail")?;
    assert!(error.to_string().contains("Interrupted"), "{error}");
    drop(statement);
    scratch.close()?;
    cancellation.cancel();
    assert!(std::fs::read_dir(root.path())?.next().is_none());
    Ok(())
}

#[tokio::test]
async fn flush_failure_cleans_worker_scratch_without_ready_summary() -> anyhow::Result<()> {
    use super::super::{blocking, ExportReport, PreparedExport};
    let root = tempfile::tempdir()?;
    let summary = root.path().join("ready.json");
    let worker_summary = summary.clone();
    let error = blocking::run(move |cancellation| {
        let scratch = FloorScratch::with_cancellation(&worker_summary, 1024 * 1024, cancellation)?;
        let mut batch = Batch::new(scratch.connection()?, "title_observations")?;
        for _ in 0..2 {
            batch.append(params![1_i64, "same", 1_i64, 0_i64], 64)?;
        }
        batch.finish()?;
        scratch.close()?;
        Ok(PreparedExport {
            report: ExportReport {
                input_object_count: 0,
                row_count: 0,
                proposal_required_count: 0,
                normalization_proposal_count: 0,
            },
            summary: Some((worker_summary, b"must not be published".to_vec())),
            _lock: None,
        })
    })
    .await
    .err()
    .context("duplicate ordinal must fail")?;
    assert!(format!("{error:#}").contains("flush failed"));
    assert!(!summary.exists());
    assert!(std::fs::read_dir(root.path())?.next().is_none());
    Ok(())
}
