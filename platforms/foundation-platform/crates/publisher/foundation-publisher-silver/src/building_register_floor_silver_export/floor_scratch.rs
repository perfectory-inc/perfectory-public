//! Invocation-owned `DuckDB` staging; domain meaning remains in the Rust normalizer.
use super::{
    blocking::Cancellation, scratch_blob::ScratchBlobCodec, ExportConfig, JsonlRowWriter,
    ObjectExportReport, SilverRowWriter,
};
#[cfg(test)]
use crate::serving_scratch;
use crate::{
    bounded_bytes::BoundedBytes, building_register_zip_lines::decode_zip_lines,
    lakehouse_engine_contract::execution_profile, serving_scratch::CURSOR_BYTES,
};
use anyhow::{ensure, Context};
use duckdb::{params, Config, Connection};
use foundation_normalization_domain::BuildingFloorCounts;
use lakehouse_application::{
    normalize_building_register_floor_silver_rows_with_title_counts,
    parse_building_register_floor_source_row_from_hub_bulk_text_line,
    parse_building_title_floor_counts_from_hub_bulk_text_line,
    write_building_register_floor_entity_context_pack_line, BuildingRegisterFloorSilverRow,
    BuildingRegisterFloorSilverRowsInput, BuildingRegisterFloorSourceRow,
};
use std::{
    collections::HashMap,
    mem::size_of,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
mod arrow_row;
mod batch;
use batch::Batch;
#[cfg(test)]
mod tests;

struct Resources {
    writer: Connection,
    reader: Connection,
    directory: TempDir,
}

pub(super) struct FloorScratch {
    resources: Option<Resources>,
    cursor_bytes: usize,
    blobs: ScratchBlobCodec,
    cancellation: Cancellation,
}

impl FloorScratch {
    #[cfg(test)]
    fn new(output: &Path, maximum_bytes: u64) -> anyhow::Result<Self> {
        Self::with_cancellation(output, maximum_bytes, Cancellation::default())
    }

    pub(super) fn with_cancellation(
        output: &Path,
        maximum_bytes: u64,
        cancellation: Cancellation,
    ) -> anyhow::Result<Self> {
        Self::open(output, maximum_bytes, CURSOR_BYTES, cancellation)
    }

    #[cfg(test)]
    fn new_with_cursor_bytes(
        output: &Path,
        maximum_bytes: u64,
        cursor_bytes: usize,
    ) -> anyhow::Result<Self> {
        Self::open(output, maximum_bytes, cursor_bytes, Cancellation::default())
    }

    fn open(
        output: &Path,
        maximum_bytes: u64,
        cursor_bytes: usize,
        cancellation: Cancellation,
    ) -> anyhow::Result<Self> {
        cancellation.check()?;
        ensure!(
            (1..=CURSOR_BYTES).contains(&cursor_bytes),
            "invalid floor cursor byte budget"
        );
        ensure!(
            maximum_bytes > 0,
            "floor spill byte budget must be positive"
        );
        let parent = output
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(".floor-scratch-")
            .tempdir_in(parent)?;
        let profile = execution_profile()?;
        let config = Config::default()
            .max_memory(&format!("{}MiB", profile.memory_mib / 2))?
            .threads(i64::from(profile.cpu_slots))?
            .with(
                "temp_directory",
                directory
                    .path()
                    .to_str()
                    .context("floor spill path must be UTF-8")?,
            )?
            .with("autoload_known_extensions", "false")?
            .with("autoinstall_known_extensions", "false")?;
        let writer = Connection::open_in_memory_with_flags(config)?;
        // Apply to the live buffer manager. In 1.5.6 startup configuration can
        // report this setting while leaving actual spill allocation uncapped.
        writer.execute(
            "SET max_temp_directory_size = ?",
            [format!("{maximum_bytes}B")],
        )?;
        // DuckDB must accept the explicit spill directory before external access is disabled.
        writer.execute_batch("SET enable_external_access=false")?;
        let reader = writer.try_clone()?;
        let scratch = Self {
            resources: Some(Resources {
                writer,
                reader,
                directory,
            }),
            cursor_bytes,
            blobs: ScratchBlobCodec::new(),
            cancellation,
        };
        let resources = scratch.resources.as_ref().context("floor scratch closed")?;
        scratch
            .cancellation
            .attach(resources.writer.interrupt_handle())?;
        scratch
            .cancellation
            .attach(resources.reader.interrupt_handle())?;
        let initialized = scratch.connection()?.execute_batch(
            "CREATE TABLE title_observations (ordinal BIGINT PRIMARY KEY CHECK(ordinal>0), pk VARCHAR NOT NULL, above_ground BIGINT, below_ground BIGINT);
             CREATE TABLE title_counts (pk VARCHAR NOT NULL, above_ground BIGINT, below_ground BIGINT);
             CREATE TABLE source_rows (ordinal BIGINT PRIMARY KEY CHECK(ordinal>0), pk VARCHAR NOT NULL, code VARCHAR NOT NULL, kind VARCHAR NOT NULL, number VARCHAR NOT NULL, label VARCHAR, source_line_number BIGINT NOT NULL CHECK(source_line_number>0), input_bytes BIGINT NOT NULL CHECK(input_bytes>0));
             CREATE TABLE resolved_rows (ordinal BIGINT PRIMARY KEY CHECK(ordinal>0), row_json BLOB NOT NULL, row_bytes BIGINT NOT NULL CHECK(row_bytes>0), proposal_json BLOB, proposal_bytes BIGINT, CHECK((proposal_json IS NULL) = (proposal_bytes IS NULL)));"
        );
        if let Err(error) = initialized {
            let cleanup = scratch.close();
            return Err(match cleanup {
                Ok(()) => error.into(),
                Err(cleanup) => anyhow::Error::new(error)
                    .context(format!("floor cleanup also failed: {cleanup:#}")),
            });
        }
        Ok(scratch)
    }

    fn connection(&self) -> anyhow::Result<&Connection> {
        Ok(&self
            .resources
            .as_ref()
            .context("floor scratch already closed")?
            .writer)
    }

    pub(super) fn load_titles(&self, paths: &[PathBuf]) -> anyhow::Result<()> {
        let connection = self.connection()?;
        let mut batch = Batch::new(connection, "title_observations")?;
        let mut ordinal = 0_i64;
        for path in paths {
            decode_zip_lines(path, None, |line, _| {
                self.cancellation.check()?;
                let Some((pk, counts)) =
                    parse_building_title_floor_counts_from_hub_bulk_text_line(line)
                else {
                    return Ok(());
                };
                let bytes = pk
                    .len()
                    .checked_add(size_of::<BuildingFloorCounts>())
                    .context("title scratch byte overflow")?;
                ensure!(
                    bytes <= self.cursor_bytes,
                    "title scratch row exceeds byte bound"
                );
                ordinal = ordinal.checked_add(1).context("title ordinal overflow")?;
                batch.append(
                    params![
                        ordinal,
                        pk,
                        counts.above_ground.map(i64::from),
                        counts.below_ground.map(i64::from)
                    ],
                    bytes,
                )
            })?;
        }
        batch.finish()?;
        self.cancellation.check()?;
        connection.execute_batch(
            "INSERT INTO title_counts SELECT pk, above_ground, below_ground FROM title_observations
             QUALIFY row_number() OVER (PARTITION BY pk ORDER BY ordinal) = 1;
             DROP TABLE title_observations;",
        )?;
        Ok(())
    }

    pub(super) fn export_object(
        &mut self,
        config: &ExportConfig,
        object_path: &Path,
        bronze_object_key: &str,
        output_writer: &mut SilverRowWriter,
        mut proposal_writer: Option<&mut JsonlRowWriter>,
        remaining_rows: Option<usize>,
    ) -> anyhow::Result<ObjectExportReport> {
        self.cancellation.check()?;
        self.connection()?
            .execute_batch("DELETE FROM resolved_rows; DELETE FROM source_rows;")?;
        let row_count = self.stage_source_rows(object_path, bronze_object_key, remaining_rows)?;
        self.resolve_groups(config, bronze_object_key)?;
        let reader = &self
            .resources
            .as_ref()
            .context("floor scratch closed")?
            .reader;
        let mut statement = reader.prepare("SELECT ordinal, row_json, row_bytes, proposal_json, proposal_bytes FROM resolved_rows ORDER BY ordinal")?;
        // Start streaming, then use the fallible fetch API: Arrow::next panics on errors.
        let _ = statement.stream_arrow([])?;
        let mut last_ordinal = 0_i64;
        let mut proposal_required_count = 0_u64;
        while let Some(array) = {
            self.cancellation.check()?;
            statement.step()?
        } {
            let batch = duckdb::arrow::record_batch::RecordBatch::from(&array);
            for index in 0..batch.num_rows() {
                self.cancellation.check()?;
                let ordinal = arrow_row::integer(&batch, 0, index)?;
                ensure!(
                    ordinal
                        == last_ordinal
                            .checked_add(1)
                            .context("floor output ordinal overflow")?,
                    "floor scratch output ordinal is not contiguous"
                );
                last_ordinal = ordinal;
                let row_json = self.blobs.decode(
                    &arrow_row::blob(&batch, 1, index)?,
                    arrow_row::integer(&batch, 2, index)?,
                    self.cursor_bytes,
                )?;
                let row: BuildingRegisterFloorSilverRow = serde_json::from_slice(&row_json)?;
                let proposal_json = if row.normalization_status == "proposal_required" {
                    Some(self.blobs.decode(
                        &arrow_row::blob(&batch, 3, index)?,
                        arrow_row::integer(&batch, 4, index)?,
                        self.cursor_bytes - row_json.len(),
                    )?)
                } else {
                    None
                };
                output_writer.write_rows(std::slice::from_ref(&row))?;
                if let Some(proposal_json) = proposal_json {
                    proposal_required_count = proposal_required_count
                        .checked_add(1)
                        .context("floor proposal count overflow")?;
                    if let Some(writer) = proposal_writer.as_deref_mut() {
                        writer.write_jsonl(std::str::from_utf8(&proposal_json)?)?;
                    }
                }
            }
        }
        ensure!(
            usize::try_from(last_ordinal)? == row_count,
            "floor scratch output row count differs from staged input"
        );
        Ok(ObjectExportReport {
            row_count,
            proposal_required_count,
            normalization_proposal_count: proposal_required_count,
            source_snapshot_ids: vec![config.source_snapshot_id.clone()],
        })
    }

    fn stage_source_rows(
        &self,
        object_path: &Path,
        bronze_object_key: &str,
        remaining_rows: Option<usize>,
    ) -> anyhow::Result<usize> {
        let mut batch = Batch::new(self.connection()?, "source_rows")?;
        let mut count = 0_usize;
        decode_zip_lines(object_path, remaining_rows, |line, number| {
            self.cancellation.check()?;
            let row = parse_building_register_floor_source_row_from_hub_bulk_text_line(
                line,
                bronze_object_key,
                number,
            )?;
            let bytes = accounted_source_bytes(&row)?;
            ensure!(
                bytes <= self.cursor_bytes,
                "floor source row exceeds byte bound"
            );
            count = count.checked_add(1).context("floor row count overflow")?;
            batch.append(
                params![
                    i64::try_from(count)?,
                    row.mgm_bldrgst_pk,
                    row.floor_type_code_raw,
                    row.floor_type_name_raw,
                    row.floor_number_raw,
                    row.floor_label_raw,
                    i64::try_from(number)?,
                    i64::try_from(bytes)?
                ],
                bytes,
            )
        })?;
        batch.finish()?;
        Ok(count)
    }

    fn resolve_groups(
        &mut self,
        config: &ExportConfig,
        bronze_object_key: &str,
    ) -> anyhow::Result<()> {
        self.cancellation.check()?;
        let resources = self.resources.as_ref().context("floor scratch closed")?;
        // This aggregate completes before any Rust group is hydrated.
        let oversized: i64 = resources.reader.query_row(
            "SELECT count(*)::BIGINT FROM (SELECT pk FROM source_rows GROUP BY pk HAVING sum(input_bytes) > ?)",
            [i64::try_from(self.cursor_bytes)?], |row| row.get(0))?;
        ensure!(
            oversized == 0,
            "complete floor building group exceeds byte bound"
        );
        let mut query = resources.reader.prepare(
            "SELECT s.ordinal, s.pk, s.code, s.kind, s.number, s.label, s.source_line_number, s.input_bytes,
                    t.pk, t.above_ground, t.below_ground
             FROM source_rows s LEFT JOIN title_counts t ON s.pk = t.pk ORDER BY s.pk, s.ordinal")?;
        let _ = query.stream_arrow([])?;
        let mut output = Batch::new(&resources.writer, "resolved_rows")?;
        let mut group: Option<Group> = None;
        while let Some(array) = {
            self.cancellation.check()?;
            query.step()?
        } {
            let batch = duckdb::arrow::record_batch::RecordBatch::from(&array);
            for index in 0..batch.num_rows() {
                self.cancellation.check()?;
                let pk = arrow_row::text(&batch, 1, index)?;
                if group.as_ref().is_some_and(|group| group.pk != pk) {
                    resolve_group(
                        group.take().context("floor group missing")?,
                        config,
                        bronze_object_key,
                        self.cursor_bytes,
                        &mut self.blobs,
                        &mut output,
                        &self.cancellation,
                    )?;
                }
                if group.is_none() {
                    let title = if arrow_row::optional_text(&batch, 8, index)?.is_some() {
                        Some(BuildingFloorCounts {
                            above_ground: arrow_row::optional_integer(&batch, 9, index)?
                                .map(u16::try_from)
                                .transpose()?,
                            below_ground: arrow_row::optional_integer(&batch, 10, index)?
                                .map(u16::try_from)
                                .transpose()?,
                        })
                    } else {
                        None
                    };
                    group = Some(Group {
                        pk: pk.to_owned(),
                        title,
                        ordinals: Vec::new(),
                        records: Vec::new(),
                        bytes: 0,
                    });
                }
                let group = group.as_mut().context("floor group missing")?;
                group.bytes = group
                    .bytes
                    .checked_add(usize::try_from(arrow_row::integer(&batch, 7, index)?)?)
                    .context("floor group byte overflow")?;
                ensure!(
                    group.bytes <= self.cursor_bytes,
                    "complete floor building group exceeds byte bound"
                );
                group.ordinals.push(arrow_row::integer(&batch, 0, index)?);
                group.records.push(BuildingRegisterFloorSourceRow {
                    source_record_id: bronze_object_key.to_owned(),
                    mgm_bldrgst_pk: pk.to_owned(),
                    floor_type_code_raw: arrow_row::text(&batch, 2, index)?.to_owned(),
                    floor_type_name_raw: arrow_row::text(&batch, 3, index)?.to_owned(),
                    floor_number_raw: arrow_row::text(&batch, 4, index)?.to_owned(),
                    floor_label_raw: arrow_row::optional_text(&batch, 5, index)?.map(str::to_owned),
                    source_line_number: Some(u64::try_from(arrow_row::integer(&batch, 6, index)?)?),
                });
            }
        }
        if let Some(group) = group {
            resolve_group(
                group,
                config,
                bronze_object_key,
                self.cursor_bytes,
                &mut self.blobs,
                &mut output,
                &self.cancellation,
            )?;
        }
        output.finish()
    }

    pub(super) fn close(mut self) -> anyhow::Result<()> {
        if let Some(resources) = self.resources.take() {
            close_resources(resources, &self.cancellation)?;
        }
        Ok(())
    }
}

struct Group {
    pk: String,
    title: Option<BuildingFloorCounts>,
    ordinals: Vec<i64>,
    records: Vec<BuildingRegisterFloorSourceRow>,
    bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn resolve_group(
    group: Group,
    config: &ExportConfig,
    bronze_object_key: &str,
    cursor_bytes: usize,
    blobs: &mut ScratchBlobCodec,
    output: &mut Batch<'_>,
    cancellation: &Cancellation,
) -> anyhow::Result<()> {
    let counts = group
        .title
        .map(|title| HashMap::from([(group.pk, title)]))
        .unwrap_or_default();
    let rows = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: &group.records,
            source_snapshot_id: &config.source_snapshot_id,
            bronze_object_key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: config.ingested_at_utc,
        },
        &counts,
    )?;
    ensure!(
        rows.len() == group.ordinals.len(),
        "floor normalization changed group row count"
    );
    let context_rows = rows.iter().collect::<Vec<_>>();
    for (ordinal, row) in group.ordinals.into_iter().zip(rows.iter()) {
        cancellation.check()?;
        let mut silver = BoundedBytes::new(cursor_bytes);
        serde_json::to_writer(&mut silver, row)?;
        let silver = silver.into_inner();
        let proposal = if row.normalization_status == "proposal_required" {
            let mut payload = BoundedBytes::new(cursor_bytes);
            write_building_register_floor_entity_context_pack_line(
                row,
                &context_rows,
                &mut payload,
            )?;
            Some(payload.into_inner())
        } else {
            None
        };
        let bytes = silver
            .len()
            .checked_add(proposal.as_ref().map_or(0, Vec::len))
            .context("resolved floor scratch byte overflow")?;
        ensure!(
            bytes <= cursor_bytes,
            "resolved floor scratch row exceeds byte bound"
        );
        let row_bytes = i64::try_from(silver.len())?;
        let proposal_bytes = proposal
            .as_ref()
            .map(|value| i64::try_from(value.len()))
            .transpose()?;
        let silver = blobs.encode(silver)?;
        let proposal = proposal.map(|value| blobs.encode(value)).transpose()?;
        output.append(
            params![ordinal, silver, row_bytes, proposal, proposal_bytes],
            bytes,
        )?;
    }
    Ok(())
}

fn accounted_source_bytes(row: &BuildingRegisterFloorSourceRow) -> anyhow::Result<usize> {
    let mut bytes = size_of::<BuildingRegisterFloorSourceRow>();
    for part in [
        &row.source_record_id,
        &row.mgm_bldrgst_pk,
        &row.floor_type_code_raw,
        &row.floor_type_name_raw,
        &row.floor_number_raw,
    ] {
        bytes = bytes
            .checked_add(part.len())
            .context("floor group byte overflow")?;
    }
    bytes
        .checked_add(row.floor_label_raw.as_ref().map_or(0, String::len))
        .context("floor group byte overflow")
}

fn close_resources(resources: Resources, cancellation: &Cancellation) -> anyhow::Result<()> {
    let Resources {
        reader,
        writer,
        directory,
    } = resources;
    cancellation.detach(&[reader.interrupt_handle(), writer.interrupt_handle()]);
    let close = |connection: Connection| -> anyhow::Result<()> {
        connection.close().map_err(|(connection, error)| {
            drop(connection);
            error.into()
        })
    };
    let outcomes = [
        close(reader),
        close(writer),
        directory.close().map_err(anyhow::Error::from),
    ];
    let mut error = None;
    for result in outcomes {
        if let Err(next) = result {
            error = Some(match error {
                None => next,
                Some(first) => next.context(format!("earlier floor cleanup failure: {first:#}")),
            });
        }
    }
    error.map_or(Ok(()), Err)
}

impl Drop for FloorScratch {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.take() {
            if let Err(error) = close_resources(resources, &self.cancellation) {
                tracing::warn!(%error, "failed to clean floor scratch");
            }
        }
    }
}
