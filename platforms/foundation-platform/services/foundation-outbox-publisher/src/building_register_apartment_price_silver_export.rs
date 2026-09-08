//! Streaming HUB apartment-price handoff (root ADR-0092).

mod layout;
#[cfg(test)]
mod tests;
mod zip_stream;

use std::{
    io::{BufRead, BufReader, Read, Seek, Write},
    path::PathBuf,
};

use anyhow::{ensure, Context};
use chrono::Utc;
use flate2::{write::GzEncoder, Compression, Crc};
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::{pnu::standard_pnu_from_hub_register_codes, Pnu};
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::silver_handoff_io::{
    open_sink, open_source, optional_env, refuse_existing_outputs, required_env, HandoffSink,
    InputSource, OutputSink, PendingOutput, SeekableSource, GZIP_LEVEL,
};
use layout::Layout;

const ENV: &str = "FOUNDATION_PLATFORM_APARTMENT_PRICE";

#[derive(Clone)]
struct Config {
    input_object_key: String,
    output_prefix: OutputSink,
    source_snapshot_id: String,
    summary_path: Option<PathBuf>,
}

impl Config {
    fn output(&self, suffix: &str) -> OutputSink {
        match &self.output_prefix {
            OutputSink::R2Object(prefix) => OutputSink::R2Object(format!("{prefix}/{suffix}")),
            OutputSink::LocalPath(prefix) => OutputSink::LocalPath(prefix.join(suffix)),
        }
    }

    fn prefix_name(&self) -> String {
        match &self.output_prefix {
            OutputSink::R2Object(prefix) => prefix.clone(),
            OutputSink::LocalPath(prefix) => prefix.to_string_lossy().into_owned(),
        }
    }

    fn object_stem(&self) -> anyhow::Result<&str> {
        self.input_object_key
            .rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".zip"))
            .filter(|s| !s.is_empty())
            .context("input object must name a ZIP")
    }
}

fn output_name(output: &OutputSink) -> String {
    match output {
        OutputSink::R2Object(key) => key.clone(),
        OutputSink::LocalPath(path) => path.to_string_lossy().into_owned(),
    }
}

#[derive(Debug, Serialize)]
struct Part {
    object_key: String,
    rows: u64,
    bytes: u64,
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    status: &'static str,
    input_object_key: String,
    source_snapshot_id: String,
    output_object_prefix: String,
    vintage: String,
    rows_per_part: u64,
    rows_read: u64,
    rows_emitted: u64,
    rejected_rows: u64,
    pnu_ok: u64,
    pnu_bad: u64,
    parts: Vec<Part>,
}

impl Report {
    fn new(config: &Config, layout: &Layout) -> Self {
        Self {
            schema_version: 1,
            status: "complete",
            input_object_key: config.input_object_key.clone(),
            source_snapshot_id: config.source_snapshot_id.clone(),
            output_object_prefix: config.prefix_name(),
            vintage: layout.selected_vintage.clone(),
            rows_per_part: layout.rows_per_part,
            rows_read: 0,
            rows_emitted: 0,
            rejected_rows: 0,
            pnu_ok: 0,
            pnu_bad: 0,
            parts: Vec::new(),
        }
    }

    fn publish(
        self,
        config: &Config,
        open: &mut impl FnMut(&OutputSink) -> anyhow::Result<HandoffSink>,
    ) -> anyhow::Result<Self> {
        ensure!(
            self.rows_read == self.rows_emitted + self.rejected_rows
                && self.rows_emitted == self.pnu_ok + self.pnu_bad
                && self.rows_emitted == self.parts.iter().map(|part| part.rows).sum::<u64>(),
            "HUB handoff row accounting differs"
        );
        let manifest = config.output(&format!("{}/manifest.json", config.object_stem()?));
        let mut sink = open(&manifest)?;
        serde_json::to_writer(&mut sink, &self)?;
        sink.finish()?;
        if let Some(path) = &config.summary_path {
            let mut pending = PendingOutput::create(path)?;
            serde_json::to_writer_pretty(pending.file_mut()?, &self)?;
            pending.sync_and_commit(path)?;
        }
        Ok(self)
    }
}

/// Exports the selected measured national ZIP with bounded input/output memory.
///
/// # Errors
/// Fails on invalid configuration, changed source layout/integrity or publication errors.
pub async fn run() -> anyhow::Result<()> {
    let layout = Layout::embedded()?;
    let key = required_env(&format!("{ENV}_INPUT_OBJECT_KEY"))?;
    ensure!(
        key == layout.selected()?.object_key,
        "input must be the selected latest vintage object"
    );
    let prefix = required_env(&format!("{ENV}_OUTPUT_OBJECT_PREFIX"))?;
    ensure!(
        prefix.starts_with("silver-handoff/")
            && !prefix.ends_with('/')
            && !prefix.contains([',', '\n', '\r', '\t', '\\'])
            && prefix
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."),
        "output prefix must be a canonical silver-handoff key"
    );
    let config = Config {
        input_object_key: key.clone(),
        output_prefix: OutputSink::R2Object(prefix),
        source_snapshot_id: required_env(&format!("{ENV}_SOURCE_SNAPSHOT_ID"))?,
        summary_path: optional_env(&format!("{ENV}_SUMMARY_PATH"))?.map(PathBuf::from),
    };
    let manifest = config.output(&format!("{}/manifest.json", config.object_stem()?));
    refuse_existing_outputs(&manifest, config.summary_path.as_deref())?;
    let storage = R2ObjectStorage::from_env()?;
    ensure!(
        !storage.object_exists(&output_name(&manifest)).await?,
        "completed handoff manifest already exists; use it for loading"
    );
    let source = open_source(&InputSource::R2Object(key), Some(&storage)).await?;
    if let SeekableSource::R2(reader) = &source {
        ensure!(
            reader.object_bytes() == layout.selected()?.bytes,
            "R2 object size differs from measured source contract"
        );
    }
    let runtime = tokio::runtime::Handle::current();
    let report = tokio::task::spawn_blocking(move || {
        convert(source, &config, &layout, |output| {
            runtime.block_on(open_sink(output, Some(&storage)))
        })
    })
    .await
    .context("failed to join apartment-price conversion")??;
    tracing::info!(
        rows_read = report.rows_read,
        rows_emitted = report.rows_emitted,
        rejected_rows = report.rejected_rows,
        pnu_ok = report.pnu_ok,
        pnu_bad = report.pnu_bad,
        parts = report.parts.len(),
        "apartment-price Silver handoff published"
    );
    Ok(())
}

fn finish_part(writer: GzEncoder<HandoffSink>, key: String, rows: u64) -> anyhow::Result<Part> {
    let bytes = writer
        .finish()
        .context("failed to finish handoff gzip")?
        .finish()?;
    Ok(Part {
        object_key: key,
        rows,
        bytes,
    })
}

fn convert<R: Read + Seek>(
    source: R,
    config: &Config,
    layout: &Layout,
    mut open: impl FnMut(&OutputSink) -> anyhow::Result<HandoffSink>,
) -> anyhow::Result<Report> {
    layout.validate()?;
    let member = zip_stream::open(source, &layout.inner_file)?;
    let mut reader = BufReader::with_capacity(64 * 1024, member.decoder);
    let mut crc = Crc::new();
    let mut report = Report::new(config, layout);
    let attempt = format!("{}/attempt={}", config.object_stem()?, Uuid::new_v4());
    let ingested_at = Utc::now().to_rfc3339();
    let mut line = Vec::new();
    let mut current: Option<GzEncoder<HandoffSink>> = None;
    let mut part_key = String::new();
    let mut part_rows = 0;
    loop {
        line.clear();
        let read = (&mut reader)
            .take(layout.max_row_bytes + 1)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        ensure!(
            u64::try_from(read)? <= layout.max_row_bytes,
            "HUB row exceeds max_row_bytes"
        );
        crc.update(&line);
        report.rows_read += 1;
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let text = std::str::from_utf8(&line).context("HUB row is not UTF-8")?;
        let fields: Vec<_> = text.split('|').collect();
        if !valid_width(layout, fields.len(), &mut report, text)? {
            continue;
        }
        if current.is_none() {
            let part_number = report.parts.len() + 1;
            let output = config.output(&format!(
                "{attempt}/part-{part_number:04}{}",
                layout.handoff_suffix
            ));
            part_key = output_name(&output);
            current = Some(GzEncoder::new(open(&output)?, Compression::new(GZIP_LEVEL)));
        }
        let (row, pnu_ok) = make_row(
            layout,
            config,
            &fields,
            report.rows_read,
            &part_key,
            &ingested_at,
        )?;
        if pnu_ok {
            report.pnu_ok += 1;
        } else {
            report.pnu_bad += 1;
        }
        let writer = current.as_mut().context("missing current handoff part")?;
        serde_json::to_writer(&mut *writer, &row)?;
        writer.write_all(b"\n")?;
        report.rows_emitted += 1;
        part_rows += 1;
        if part_rows == layout.rows_per_part {
            let writer = current.take().context("missing completed handoff part")?;
            report
                .parts
                .push(finish_part(writer, part_key.clone(), part_rows)?);
            part_rows = 0;
        }
    }
    let decoder = reader.into_inner();
    ensure!(
        decoder.total_in() == member.compressed_bytes
            && decoder.total_out() == member.uncompressed_bytes
            && crc.sum() == member.crc32,
        "HUB ZIP CRC/size integrity check failed"
    );
    ensure!(
        report.rows_read > 0 && report.rows_emitted > 0,
        "HUB ZIP has no emitted rows"
    );
    if let Some(writer) = current {
        report.parts.push(finish_part(writer, part_key, part_rows)?);
    }
    report.publish(config, &mut open)
}

fn valid_width(
    layout: &Layout,
    count: usize,
    report: &mut Report,
    text: &str,
) -> anyhow::Result<bool> {
    if count == layout.column_count {
        return Ok(true);
    }
    ensure!(
        report.rows_read != 1,
        "first row column count differs from HUB contract: {count}"
    );
    report.rejected_rows += 1;
    if report.rejected_rows <= 5 {
        let sample: String = text.chars().take(160).collect();
        tracing::warn!(
            line = report.rows_read,
            columns = count,
            sample,
            "rejected HUB row with wrong column count"
        );
    }
    Ok(false)
}

fn make_row(
    layout: &Layout,
    config: &Config,
    fields: &[&str],
    line_number: u64,
    part_key: &str,
    ingested_at: &str,
) -> anyhow::Result<(Value, bool)> {
    let pnu = compose_pnu(layout, fields)?;
    let pnu_ok = pnu.is_some();
    let mut row = serde_json::Map::new();
    for name in layout.columns.keys() {
        row.insert(name.clone(), json!(layout.value(fields, name)?));
    }
    row.extend([
        ("pnu".into(), json!(pnu)),
        ("raw_columns".into(), json!(fields)),
        ("vintage".into(), json!(layout.selected_vintage)),
        ("source_record_id".into(), json!(config.input_object_key)),
        ("source_part_id".into(), json!(part_key)),
        ("source_line_number".into(), json!(line_number)),
        (
            "source_snapshot_id".into(),
            json!(config.source_snapshot_id),
        ),
        ("ingested_at_utc".into(), json!(ingested_at)),
    ]);
    Ok((Value::Object(row), pnu_ok))
}

fn compose_pnu(layout: &Layout, fields: &[&str]) -> anyhow::Result<Option<String>> {
    let sigungu = layout.value(fields, "sigungu_cd")?.trim();
    let bjdong = layout.value(fields, "bjdong_cd")?.trim();
    let bon = layout.value(fields, "bonbeon")?.trim();
    let bu = layout.value(fields, "bubeon")?.trim();
    if ![(sigungu, 5, 5), (bjdong, 5, 5), (bon, 1, 4), (bu, 1, 4)]
        .into_iter()
        .all(|(value, min, max)| {
            (min..=max).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_digit())
        })
    {
        return Ok(None);
    }
    Ok(standard_pnu_from_hub_register_codes(
        sigungu,
        bjdong,
        layout.value(fields, "san_gubun")?,
        bon,
        bu,
    )
    .and_then(|value| Pnu::parse(value).ok())
    .map(String::from))
}
