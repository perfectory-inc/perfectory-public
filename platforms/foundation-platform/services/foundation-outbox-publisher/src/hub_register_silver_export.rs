//! Shared bounded HUB register handoff engine (root ADR-0092/0094).

#[cfg(test)]
mod apartment_price_tests;
#[cfg(test)]
mod exclusive_unit_tests;
mod layout;
#[cfg(test)]
mod test_support;
mod zip_stream;

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Seek, Write},
    path::PathBuf,
};

use anyhow::{ensure, Context};
use chrono::Utc;
use flate2::{write::GzEncoder, Compression, Crc};
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::{
    pnu::{standard_pnu_from_hub_register_codes_via, SigunguCrosswalk},
    Pnu,
};
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::silver_handoff_io::{
    open_sink, open_source, optional_env, refuse_existing_outputs, required_env, HandoffSink,
    InputSource, OutputSink, PendingOutput, SeekableSource, GZIP_LEVEL,
};
pub use layout::Layout;

const GENERATED_COLUMNS: [&str; 8] = [
    "pnu",
    "raw_columns",
    "vintage",
    "source_record_id",
    "source_part_id",
    "source_line_number",
    "source_snapshot_id",
    "ingested_at_utc",
];

#[derive(Clone)]
struct Config {
    input_object_key: String,
    /// The provider month of the input, from its name (`Layout::vintage_of`).
    vintage: String,
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
    /// Where the 시군구 crosswalk came from (root ADR-0143 §5); empty for a crosswalk built in code.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    sigungu_crosswalk: BTreeMap<String, String>,
}

impl Report {
    fn new(config: &Config, layout: &Layout) -> Self {
        Self {
            schema_version: 1,
            status: "complete",
            input_object_key: config.input_object_key.clone(),
            source_snapshot_id: config.source_snapshot_id.clone(),
            output_object_prefix: config.prefix_name(),
            vintage: config.vintage.clone(),
            rows_per_part: layout.rows_per_part,
            rows_read: 0,
            rows_emitted: 0,
            rejected_rows: 0,
            pnu_ok: 0,
            pnu_bad: 0,
            parts: Vec::new(),
            sigungu_crosswalk: BTreeMap::new(),
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

/// Exports one national ZIP of the layout's source with bounded input/output memory.
///
/// Which ZIP is the caller's: `{env}_INPUT_OBJECT_KEY` with the size the Bronze ledger records
/// for it in `{env}_INPUT_OBJECT_BYTES`. The Silver refresh passes the newest complete release
/// (root ADR-0169); a manual run passes the ledger row it means.
///
/// # Errors
/// Fails on invalid configuration, changed source layout/integrity or publication errors.
pub async fn run(env: &str, layout: Layout) -> anyhow::Result<()> {
    let key = required_env(&format!("{env}_INPUT_OBJECT_KEY"))?;
    let vintage = layout.vintage_of(&key)?;
    let expected_bytes: u64 = required_env(&format!("{env}_INPUT_OBJECT_BYTES"))?
        .parse()
        .with_context(|| format!("{env}_INPUT_OBJECT_BYTES must be the ledger's byte count"))?;
    ensure!(
        expected_bytes > 0,
        "{env}_INPUT_OBJECT_BYTES must be positive"
    );
    let prefix = required_env(&format!("{env}_OUTPUT_OBJECT_PREFIX"))?;
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
        vintage,
        output_prefix: OutputSink::R2Object(prefix),
        source_snapshot_id: required_env(&format!("{env}_SOURCE_SNAPSHOT_ID"))?,
        summary_path: optional_env(&format!("{env}_SUMMARY_PATH"))?.map(PathBuf::from),
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
            reader.object_bytes() == expected_bytes,
            "R2 object size differs from the Bronze ledger's"
        );
    }
    let sigungu_crosswalk = crate::sigungu_crosswalk::hub_sigungu_crosswalk().await?;
    let layout_name = layout.inner_file.clone();
    let runtime = tokio::runtime::Handle::current();
    let report = tokio::task::spawn_blocking(move || {
        convert(source, &config, &layout, &sigungu_crosswalk, |output| {
            runtime.block_on(open_sink(output, Some(&storage)))
        })
    })
    .await
    .context("failed to join HUB register conversion")??;
    tracing::info!(
        rows_read = report.rows_read,
        rows_emitted = report.rows_emitted,
        rejected_rows = report.rejected_rows,
        pnu_ok = report.pnu_ok,
        pnu_bad = report.pnu_bad,
        parts = report.parts.len(),
        dataset = layout_name,
        "HUB register Silver handoff published"
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

/// The rows of one handoff part, as the reader cut them from the ZIP member.
struct Chunk {
    part_number: usize,
    /// The rows' bytes, without their line endings, one after another.
    bytes: Vec<u8>,
    /// Each row's source line number and where its bytes end in `bytes`.
    rows: Vec<(u64, usize)>,
}

/// What a worker wrote for one chunk.
struct Written {
    part_number: usize,
    part: Part,
    pnu_ok: u64,
}

/// Workers that encode and upload parts beside the reader. Inflating the member and splitting its
/// lines is one sequential stream; building the JSON rows and gzipping them is what took one core
/// 2h20m for the 202M-row price register (2026-09-08) and still did on 2026-10-10 (one core at
/// 85%, 1.8 MB/s out). Each part is one worker's, so the parts are the bytes a single thread
/// writes. One chunk waits per worker, so a chunk's rows times twice the workers is the memory.
fn export_workers() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .clamp(1, 8)
}

fn convert<R: Read + Seek>(
    source: R,
    config: &Config,
    layout: &Layout,
    sigungu_crosswalk: &SigunguCrosswalk,
    open: impl Fn(&OutputSink) -> anyhow::Result<HandoffSink> + Sync,
) -> anyhow::Result<Report> {
    layout.validate()?;
    let member = zip_stream::open(source, &layout.inner_file)?;
    let mut report = Report::new(config, layout);
    report.sigungu_crosswalk = sigungu_crosswalk.provenance().clone();
    let attempt = format!("{}/attempt={}", config.object_stem()?, Uuid::new_v4());
    let ingested_at = Utc::now().to_rfc3339();
    let workers = export_workers();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<Chunk>(workers);
    let receiver = std::sync::Mutex::new(receiver);
    let encode = |chunk: Chunk| {
        write_part(
            &chunk,
            config,
            layout,
            sigungu_crosswalk,
            &attempt,
            &ingested_at,
            &open,
        )
    };
    let (read, written) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| scope.spawn(|| work(&receiver, &encode)))
            .collect();
        let read = read_chunks(member.decoder, layout, &mut report, &sender);
        // Closing the channel lets every worker finish what was sent and stop.
        drop(sender);
        let mut written = Vec::new();
        let mut failure = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(parts)) => written.extend(parts),
                Ok(Err(error)) => failure = failure.or(Some(error)),
                Err(_) => {
                    failure = failure.or(Some(anyhow::anyhow!("a HUB export worker panicked")));
                }
            }
        }
        (read, failure.map_or(Ok(written), Err))
    });
    let (decoder, crc) = read?;
    let mut written = written?;
    ensure!(
        decoder.total_in() == member.compressed_bytes
            && decoder.total_out() == member.uncompressed_bytes
            && crc == member.crc32,
        "HUB ZIP CRC/size integrity check failed"
    );
    written.sort_by_key(|part| part.part_number);
    ensure!(
        written
            .iter()
            .enumerate()
            .all(|(index, part)| part.part_number == index + 1),
        "HUB export parts are not contiguous"
    );
    for part in written {
        report.rows_emitted += part.part.rows;
        report.pnu_ok += part.pnu_ok;
        report.pnu_bad += part.part.rows - part.pnu_ok;
        report.parts.push(part.part);
    }
    ensure!(
        report.rows_read > 0 && report.rows_emitted > 0,
        "HUB ZIP has no emitted rows"
    );
    report.publish(config, &mut |output| open(output))
}

/// Reads the member's lines into chunks of `rows_per_part` well-formed rows and sends them, in
/// order; counts every line and the rejected ones. Returns the decoder and the member's CRC.
fn read_chunks<D: Read>(
    decoder: D,
    layout: &Layout,
    report: &mut Report,
    sender: &std::sync::mpsc::SyncSender<Chunk>,
) -> anyhow::Result<(D, u32)> {
    let mut reader = BufReader::with_capacity(64 * 1024, decoder);
    let mut crc = Crc::new();
    let mut line = Vec::new();
    let mut chunk = Chunk {
        part_number: 1,
        bytes: Vec::new(),
        rows: Vec::new(),
    };
    let rows_per_part = usize::try_from(layout.rows_per_part)?;
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
        let width = line.split(|&byte| byte == b'|').count();
        if width != layout.column_count {
            let text = std::str::from_utf8(&line).context("HUB row is not UTF-8")?;
            valid_width(layout, width, report, text)?;
            continue;
        }
        chunk.bytes.extend_from_slice(&line);
        chunk.rows.push((report.rows_read, chunk.bytes.len()));
        if chunk.rows.len() == rows_per_part {
            let next = chunk.part_number + 1;
            let full = std::mem::replace(
                &mut chunk,
                Chunk {
                    part_number: next,
                    bytes: Vec::new(),
                    rows: Vec::new(),
                },
            );
            sender
                .send(full)
                .map_err(|_| anyhow::anyhow!("every HUB export worker stopped"))?;
        }
    }
    if !chunk.rows.is_empty() {
        sender
            .send(chunk)
            .map_err(|_| anyhow::anyhow!("every HUB export worker stopped"))?;
    }
    Ok((reader.into_inner(), crc.sum()))
}

/// One worker: writes every chunk it receives until the channel closes. After a failure it keeps
/// receiving and dropping chunks, so the reader never waits on a channel nobody drains.
fn work(
    receiver: &std::sync::Mutex<std::sync::mpsc::Receiver<Chunk>>,
    encode: &(impl Fn(Chunk) -> anyhow::Result<Written> + Sync),
) -> anyhow::Result<Vec<Written>> {
    let mut written = Vec::new();
    let mut failure = None;
    loop {
        let next = receiver
            .lock()
            .map_err(|_| anyhow::anyhow!("a HUB export worker panicked"))?
            .recv();
        let Ok(chunk) = next else { break };
        if failure.is_none() {
            match encode(chunk) {
                Ok(part) => written.push(part),
                Err(error) => failure = Some(error),
            }
        }
    }
    failure.map_or(Ok(written), Err)
}

fn write_part(
    chunk: &Chunk,
    config: &Config,
    layout: &Layout,
    sigungu_crosswalk: &SigunguCrosswalk,
    attempt: &str,
    ingested_at: &str,
    open: &impl Fn(&OutputSink) -> anyhow::Result<HandoffSink>,
) -> anyhow::Result<Written> {
    let output = config.output(&format!(
        "{attempt}/part-{:04}{}",
        chunk.part_number, layout.handoff_suffix
    ));
    let part_key = output_name(&output);
    let mut writer = GzEncoder::new(open(&output)?, Compression::new(GZIP_LEVEL));
    let mut pnu_ok = 0;
    let mut start = 0;
    for &(line_number, end) in &chunk.rows {
        let text = std::str::from_utf8(&chunk.bytes[start..end]).context("HUB row is not UTF-8")?;
        start = end;
        let fields: Vec<_> = text.split('|').collect();
        let (row, ok) = make_row(
            layout,
            config,
            sigungu_crosswalk,
            &fields,
            line_number,
            &part_key,
            ingested_at,
        )?;
        pnu_ok += u64::from(ok);
        serde_json::to_writer(&mut writer, &row)?;
        writer.write_all(b"\n")?;
    }
    let rows = u64::try_from(chunk.rows.len())?;
    Ok(Written {
        part_number: chunk.part_number,
        part: finish_part(writer, part_key, rows)?,
        pnu_ok,
    })
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
    sigungu_crosswalk: &SigunguCrosswalk,
    fields: &[&str],
    line_number: u64,
    part_key: &str,
    ingested_at: &str,
) -> anyhow::Result<(Value, bool)> {
    let pnu = compose_pnu(layout, sigungu_crosswalk, fields)?;
    let pnu_ok = pnu.is_some();
    let mut row = serde_json::Map::new();
    for name in layout.columns.keys() {
        row.insert(name.clone(), json!(layout.value(fields, name)?));
    }
    row.extend(GENERATED_COLUMNS.into_iter().map(str::to_owned).zip([
        json!(pnu),
        json!(fields),
        json!(config.vintage),
        json!(config.input_object_key),
        json!(part_key),
        json!(line_number),
        json!(config.source_snapshot_id),
        json!(ingested_at),
    ]));
    Ok((Value::Object(row), pnu_ok))
}

fn compose_pnu(
    layout: &Layout,
    sigungu_crosswalk: &SigunguCrosswalk,
    fields: &[&str],
) -> anyhow::Result<Option<String>> {
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
    Ok(standard_pnu_from_hub_register_codes_via(
        sigungu_crosswalk,
        sigungu,
        bjdong,
        layout.value(fields, "san_gubun")?,
        bon,
        bu,
    )?
    .and_then(|value| Pnu::parse(value).ok())
    .map(String::from))
}
