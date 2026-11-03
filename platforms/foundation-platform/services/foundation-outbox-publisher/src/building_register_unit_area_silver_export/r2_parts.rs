//! 전유공용면적 Silver rows as gzip JSONL handoff parts under an R2 manifest, the layout the hub
//! part lanes load part by part and resume (root ADR-0391 addendum; ADR-0092, ADR-0357).
//!
//! The manifest is the one `silver_refresh::validate_manifest` checks: every part full but the
//! last, named `part-NNNN.jsonl.gz` under one `attempt=`, rows that add up, published after the
//! last part so a reader never sees an incomplete release.

use std::io::Write as _;

use anyhow::{ensure, Context};
use flate2::{write::GzEncoder, Compression};
use foundation_publisher_silver::silver_handoff_io::{HandoffSink, OutputSink};
use lakehouse_application::{
    building_register_unit_area_silver_part_row_to_jsonl, BuildingRegisterUnitAreaSilverRow,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(super) struct Part {
    pub object_key: String,
    pub rows: u64,
    pub bytes: u64,
}

/// The manifest's fields, the hub engine's `Report` without its layout-only counters.
#[derive(Debug, Serialize)]
struct Manifest<'a> {
    schema_version: u32,
    status: &'static str,
    input_object_key: &'a str,
    source_snapshot_id: &'a str,
    output_object_prefix: &'a str,
    vintage: &'a str,
    rows_per_part: u64,
    rows_read: u64,
    rows_emitted: u64,
    rejected_rows: u64,
    pnu_ok: u64,
    pnu_bad: u64,
    parts: &'a [Part],
}

/// Where a release's parts and manifest go, from the lane's handoff prefix and the ZIP's name.
pub(super) struct Layout {
    pub prefix: String,
    pub input_object_key: String,
    pub source_snapshot_id: String,
    pub vintage: String,
    pub rows_per_part: u64,
    /// `attempt=<id>`: the parts of one export, never mixed with another's.
    pub attempt: String,
}

impl Layout {
    fn stem(&self) -> anyhow::Result<&str> {
        self.input_object_key
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".zip"))
            .filter(|stem| !stem.is_empty())
            .context("the unit-area input object must name a ZIP")
    }

    pub fn manifest_key(&self) -> anyhow::Result<String> {
        Ok(format!("{}/{}/manifest.json", self.prefix, self.stem()?))
    }

    fn part_key(&self, number: usize) -> anyhow::Result<String> {
        Ok(format!(
            "{}/{}/{}/part-{number:04}.jsonl.gz",
            self.prefix,
            self.stem()?,
            self.attempt
        ))
    }
}

/// Writes rows into parts of `rows_per_part` and, on `finish`, the manifest after the last part.
pub(super) struct PartWriter<O> {
    layout: Layout,
    open: O,
    current: Option<(GzEncoder<HandoffSink>, String, u64)>,
    parts: Vec<Part>,
    pnu_ok: u64,
    rows: u64,
}

impl<O: FnMut(&OutputSink) -> anyhow::Result<HandoffSink>> PartWriter<O> {
    pub fn new(layout: Layout, open: O) -> anyhow::Result<Self> {
        ensure!(layout.rows_per_part > 0, "a part holds at least one row");
        ensure!(
            layout.prefix.starts_with("silver-handoff/") && !layout.prefix.ends_with('/'),
            "the handoff prefix must be a silver-handoff/ key prefix"
        );
        layout.stem()?;
        Ok(Self {
            layout,
            open,
            current: None,
            parts: Vec::new(),
            pnu_ok: 0,
            rows: 0,
        })
    }

    pub fn write_rows(&mut self, rows: &[BuildingRegisterUnitAreaSilverRow]) -> anyhow::Result<()> {
        for row in rows {
            if self
                .current
                .as_ref()
                .is_none_or(|(_, _, written)| *written >= self.layout.rows_per_part)
            {
                self.finish_part()?;
                let key = self.layout.part_key(self.parts.len() + 1)?;
                let sink = (self.open)(&OutputSink::R2Object(key.clone()))?;
                // Fast compression: one thread writes the whole release, and R2 bytes are cheap.
                self.current = Some((GzEncoder::new(sink, Compression::fast()), key, 0));
            }
            let (writer, key, written) = self.current.as_mut().context("no open part")?;
            let line = building_register_unit_area_silver_part_row_to_jsonl(row, key)
                .context("failed to serialize a building-register unit-area Silver row")?;
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            *written += 1;
            self.rows += 1;
            if row.pnu.is_some() {
                self.pnu_ok += 1;
            }
        }
        Ok(())
    }

    fn finish_part(&mut self) -> anyhow::Result<()> {
        if let Some((writer, key, rows)) = self.current.take() {
            let bytes = writer
                .finish()
                .context("failed to finish a unit-area handoff part")?
                .finish()?;
            self.parts.push(Part {
                object_key: key,
                rows,
                bytes,
            });
        }
        Ok(())
    }

    /// Publishes the manifest; `lines_read` is the source lines the rows came from, one row each.
    pub fn finish(mut self, lines_read: u64) -> anyhow::Result<Vec<Part>> {
        self.finish_part()?;
        ensure!(
            self.rows > 0 && self.rows == lines_read,
            "the unit-area export wrote {} rows from {lines_read} source lines; a line is one row",
            self.rows
        );
        let manifest = Manifest {
            schema_version: 1,
            status: "complete",
            input_object_key: &self.layout.input_object_key,
            source_snapshot_id: &self.layout.source_snapshot_id,
            output_object_prefix: &self.layout.prefix,
            vintage: &self.layout.vintage,
            rows_per_part: self.layout.rows_per_part,
            rows_read: lines_read,
            rows_emitted: self.rows,
            rejected_rows: 0,
            pnu_ok: self.pnu_ok,
            pnu_bad: self.rows - self.pnu_ok,
            parts: &self.parts,
        };
        let mut sink = (self.open)(&OutputSink::R2Object(self.layout.manifest_key()?))?;
        serde_json::to_writer(&mut sink, &manifest)?;
        sink.finish()?;
        Ok(self.parts)
    }
}
