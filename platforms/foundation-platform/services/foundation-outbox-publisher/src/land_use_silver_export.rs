//! Land-use CSV-in-ZIP to Silver JSONL handoff commands (root ADR-0083).
//!
//! Three lanes share this module: the D155 per-parcel land-use plan attribute CSV
//! (`silver.land_use_plan`), the LMIS zone code table (`silver.land_use_zone_code`), and
//! the D151 per-parcel official land price CSV (`silver.land_individual_price`, root
//! ADR-0085).
//! Either end may be a local path or an R2 object key; an R2 source is read by ranged
//! request and the handoff is uploaded as it is produced, so a province extract never
//! lands on a disk. The row shape is the lakehouse contract's column list — the CSV
//! position mapping below is the only new fact, and a test holds it against the contract
//! so the two cannot drift apart. Transport plumbing lives in `silver_handoff_io`,
//! shared with the shapefile lane.

use std::{
    collections::BTreeMap,
    fs,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, Context};
use chrono::Utc;
use flate2::{write::GzEncoder, Compression as GzipCompression};
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::Pnu;
use lakehouse_domain::{
    LakehouseTableContract, SILVER_LAND_INDIVIDUAL_PRICE, SILVER_LAND_USE_PLAN,
    SILVER_LAND_USE_ZONE_CODES,
};
use serde_json::json;

use foundation_outbox_publisher::silver_handoff_io::{
    already_exported, one_of_path_or_key, open_sink, open_source, optional_env,
    refuse_existing_outputs, required_env, HandoffSink, InputSource, OutputSink, PathOrKey,
    SeekableSource, GZIP_LEVEL,
};

const OUTCOME_CONVERTED: &str = "converted";
const OUTCOME_ALREADY_PRESENT: &str = "already_present";
const SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.land_use_silver_handoff_export.v1";

/// Lineage columns every lane appends after its CSV-mapped columns. Exists for the test that
/// holds each lane's mapping against its contract; the writer spells the same three names when
/// it inserts the values.
#[cfg(test)]
const LINEAGE_COLUMNS: [&str; 3] = ["source_record_id", "source_snapshot_id", "ingested_at_utc"];

/// One lane = one source layout bound to one Silver contract.
struct Lane {
    env_prefix: &'static str,
    contract: &'static LakehouseTableContract,
    /// Exact header the provider ships, decoded. A drifted layout must refuse, not shift.
    expected_header: &'static [&'static str],
    /// Contract column for each CSV position, in CSV order.
    csv_columns: &'static [&'static str],
    /// Whether the member name is the lane's (the prefix also holds other datasets).
    member_matches: fn(&str) -> bool,
    /// Which CSV position carries a PNU that must parse, if any.
    pnu_position: Option<usize>,
}

const PLAN_LANE: Lane = Lane {
    env_prefix: "FOUNDATION_PLATFORM_LAND_USE_PLAN",
    contract: &SILVER_LAND_USE_PLAN,
    expected_header: &[
        "고유번호",
        "법정동코드",
        "법정동명",
        "대장구분코드",
        "대장구분명",
        "지번",
        "도면번호",
        "저촉여부코드",
        "저촉여부",
        "용도지역지구코드",
        "용도지역지구명",
        "등록일자",
        "데이터기준일자",
        "원천시도시군구코드",
        "비고내용",
    ],
    csv_columns: &[
        "pnu",
        "legal_dong_code",
        "legal_dong_name",
        "ledger_kind_code",
        "ledger_kind_name",
        "jibun",
        "drawing_number",
        "inclusion_code",
        "inclusion_name",
        "zone_code",
        "zone_name",
        "registered_date",
        "data_reference_date",
        "source_sigungu_code",
        "remark",
    ],
    member_matches: |name| name.starts_with("AL_D155_") && name.ends_with(".csv"),
    pnu_position: Some(0),
};

const ZONE_CODE_LANE: Lane = Lane {
    env_prefix: "FOUNDATION_PLATFORM_LAND_USE_ZONE_CODE",
    contract: &SILVER_LAND_USE_ZONE_CODES,
    expected_header: &[
        "UCODE",
        "UNAME",
        "DIV",
        "LAW_NM",
        "AR_GBN",
        "LAW_CD",
        "BYUL_YN",
        "EXEC_DT",
        "JO_NO",
        "JO_SUB_NO",
        "REC_SEQNO",
        "PARENT_UCODE",
        "DEL_DT",
        "DEL_TXT",
        "TERMS_NO",
        "FRST_REGIST_DT",
        "LAST_UPDT_DT",
    ],
    csv_columns: &[
        "ucode",
        "uname",
        "division_name",
        "law_name",
        "area_kind",
        "law_code",
        "annex_flag",
        "enforcement_date",
        "article_no",
        "article_sub_no",
        "record_seqno",
        "parent_ucode",
        "deleted_date",
        "deleted_text",
        "terms_no",
        "first_registered_date",
        "last_updated_date",
    ],
    member_matches: |name| name == "LART_LMISZONE.csv",
    pnu_position: None,
};

/// The D151 prefix also carries D150 DBF siblings; the member match keeps this lane on the
/// named CSV and the DBF stays a named exclusion in Bronze (root ADR-0085).
const PRICE_LANE: Lane = Lane {
    env_prefix: "FOUNDATION_PLATFORM_LAND_INDIVIDUAL_PRICE",
    contract: &SILVER_LAND_INDIVIDUAL_PRICE,
    expected_header: &[
        "고유번호",
        "법정동코드",
        "법정동명",
        "특수지구분코드",
        "특수지구분명",
        "지번",
        "기준연도",
        "기준월",
        "공시지가",
        "공시일자",
        "표준지여부",
        "데이터기준일자",
        "원천시도시군구코드",
    ],
    csv_columns: &[
        "pnu",
        "legal_dong_code",
        "legal_dong_name",
        "special_land_kind_code",
        "special_land_kind_name",
        "jibun",
        "base_year",
        "base_month",
        "price_per_m2",
        "announced_date",
        "standard_parcel_flag",
        "data_reference_date",
        "source_sigungu_code",
    ],
    member_matches: |name| name.starts_with("AL_D151_") && name.ends_with(".csv"),
    pnu_position: Some(0),
};

/// Runs the D155 per-parcel land-use plan export.
///
/// # Errors
/// Returns an error when configuration, the source layout, decoding, or publishing fails.
pub async fn run_plan() -> anyhow::Result<()> {
    run_lane(&PLAN_LANE).await
}

/// Runs the LMIS zone code table export.
///
/// # Errors
/// Returns an error when configuration, the source layout, decoding, or publishing fails.
pub async fn run_zone_code() -> anyhow::Result<()> {
    run_lane(&ZONE_CODE_LANE).await
}

/// Runs the D151 per-parcel official land price export (root ADR-0085).
///
/// # Errors
/// Returns an error when configuration, the source layout, decoding, or publishing fails.
pub async fn run_price() -> anyhow::Result<()> {
    run_lane(&PRICE_LANE).await
}

async fn run_lane(lane: &'static Lane) -> anyhow::Result<()> {
    let config = ExportConfig::from_env(lane)?;

    if already_exported(&config.output).await? {
        write_already_present_summary(&config, lane)?;
        tracing::info!(
            output = %config.output.describe(),
            table = lane.contract.table_name,
            "land-use Silver handoff already exists; nothing to do"
        );
        return Ok(());
    }

    let report = export_handoff(&config, lane).await?;
    tracing::info!(
        input_row_count = report.input_row_count,
        output_row_count = report.output_row_count,
        rejected_row_count = report.rejected_row_count,
        output_bytes = report.output_bytes,
        elapsed_milliseconds = report.elapsed_milliseconds,
        input = %config.input.describe(),
        output = %config.output.describe(),
        table = lane.contract.table_name,
        "land-use Silver handoff export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportConfig {
    input: InputSource,
    output: OutputSink,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
}

impl ExportConfig {
    fn from_env(lane: &Lane) -> anyhow::Result<Self> {
        let prefix = lane.env_prefix;
        let input = one_of_path_or_key(
            &format!("{prefix}_INPUT_PATH"),
            &format!("{prefix}_INPUT_OBJECT_KEY"),
        )?;
        let output = one_of_path_or_key(
            &format!("{prefix}_OUTPUT_PATH"),
            &format!("{prefix}_OUTPUT_OBJECT_KEY"),
        )?;
        Ok(Self {
            input: match input {
                PathOrKey::Path(path) => InputSource::LocalPath(path),
                PathOrKey::Key(key) => InputSource::R2Object(key),
            },
            output: match output {
                PathOrKey::Path(path) => OutputSink::LocalPath(path),
                PathOrKey::Key(key) => OutputSink::R2Object(key),
            },
            summary_path: optional_env(&format!("{prefix}_SUMMARY_PATH"))?.map(PathBuf::from),
            source_snapshot_id: required_env(&format!("{prefix}_SOURCE_SNAPSHOT_ID"))?,
        })
    }

    /// Derived, never passed (root ADR-0068): the key this run opened is the identity the
    /// re-run guard compares.
    fn source_record_id(&self) -> String {
        match &self.input {
            InputSource::R2Object(key) => key.clone(),
            InputSource::LocalPath(path) => path.display().to_string(),
        }
    }

    const fn uses_r2(&self) -> bool {
        self.input.is_r2() || self.output.is_r2()
    }
}

async fn export_handoff(
    config: &ExportConfig,
    lane: &'static Lane,
) -> anyhow::Result<ExportReport> {
    refuse_existing_outputs(&config.output, config.summary_path.as_deref())?;

    let storage = if config.uses_r2() {
        Some(R2ObjectStorage::from_env().context("failed to configure R2 for a streamed handoff")?)
    } else {
        None
    };
    let source = open_source(&config.input, storage.as_ref()).await?;
    let sink = open_sink(&config.output, storage.as_ref()).await?;

    let owned = config.clone();
    // Both R2 handles block their calling thread by design; a Tokio worker must not be
    // blocked that way, so the conversion runs on the blocking pool.
    tokio::task::spawn_blocking(move || convert(source, sink, &owned, lane))
        .await
        .context("failed to join the land-use Silver handoff conversion")?
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportReport {
    dataset_name: String,
    input_row_count: u64,
    output_row_count: u64,
    rejected_row_count: u64,
    rejected_row_reasons: BTreeMap<String, u64>,
    output_bytes: u64,
    elapsed_milliseconds: u128,
}

fn convert(
    source: SeekableSource,
    mut sink: HandoffSink,
    config: &ExportConfig,
    lane: &'static Lane,
) -> anyhow::Result<ExportReport> {
    let started = Instant::now();
    let mut archive =
        zip::ZipArchive::new(source).context("failed to open the land-use ZIP archive")?;
    let member_index = select_member(&mut archive, lane)?;
    let dataset_name = archive
        .by_index(member_index)
        .context("failed to reopen the selected ZIP member")?
        .name()
        .to_owned();

    let compress = config.output.is_compressed();
    let stream_report = {
        let member = archive
            .by_index(member_index)
            .context("failed to open the selected ZIP member")?;
        let mut writer = std::io::BufWriter::new(&mut sink);
        if compress {
            let mut gzip = GzEncoder::new(&mut writer, GzipCompression::new(GZIP_LEVEL));
            let report = stream_rows(member, &mut gzip, config, lane)?;
            // Explicit: a dropped encoder cannot report a failed trailer, and a truncated
            // gzip member reads as a short file rather than as an error.
            gzip.finish().context("failed to finish the gzip stream")?;
            report
        } else {
            let report = stream_rows(member, &mut writer, config, lane)?;
            writer.flush().context("failed to flush land-use JSONL")?;
            report
        }
    };
    let output_bytes = sink.finish()?;

    let report = ExportReport {
        dataset_name,
        input_row_count: stream_report.input_row_count,
        output_row_count: stream_report.output_row_count,
        rejected_row_count: stream_report.rejected_row_count,
        rejected_row_reasons: stream_report.rejected_row_reasons,
        output_bytes,
        elapsed_milliseconds: started.elapsed().as_millis(),
    };
    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, config, lane, &report)?;
    }
    Ok(report)
}

fn select_member(
    archive: &mut zip::ZipArchive<SeekableSource>,
    lane: &Lane,
) -> anyhow::Result<usize> {
    let mut matches = Vec::new();
    for index in 0..archive.len() {
        let name = archive
            .by_index(index)
            .with_context(|| format!("failed to inspect ZIP member {index}"))?
            .name()
            .to_owned();
        if (lane.member_matches)(&name) {
            matches.push((index, name));
        }
    }
    match matches.as_slice() {
        [(index, _)] => Ok(*index),
        [] => bail!(
            "the ZIP holds no member this lane reads (table {})",
            lane.contract.table_name
        ),
        many => bail!(
            "the ZIP holds {} members this lane reads; refusing to pick one silently",
            many.len()
        ),
    }
}

struct StreamReport {
    input_row_count: u64,
    output_row_count: u64,
    rejected_row_count: u64,
    rejected_row_reasons: BTreeMap<String, u64>,
}

fn stream_rows(
    member: impl Read,
    writer: &mut impl Write,
    config: &ExportConfig,
    lane: &Lane,
) -> anyhow::Result<StreamReport> {
    let ingested_at_utc = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let source_record_id = config.source_record_id();
    let mut records = CsvRecords::new(BufReader::new(member));

    let header = records
        .next_record()
        .context("failed to read the CSV header")?
        .context("the CSV member is empty")?;
    let decoded_header = decode_fields(&header)?;
    if decoded_header != lane.expected_header {
        bail!(
            "the CSV header is not the layout this lane maps (table {}): got {:?}",
            lane.contract.table_name,
            decoded_header
        );
    }

    let mut report = StreamReport {
        input_row_count: 0,
        output_row_count: 0,
        rejected_row_count: 0,
        rejected_row_reasons: BTreeMap::new(),
    };
    while let Some(record) = records
        .next_record()
        .context("failed to read a CSV record")?
    {
        report.input_row_count += 1;
        let fields = match decode_fields(&record) {
            Ok(fields) => fields,
            Err(_) => {
                reject(&mut report, "field_encoding");
                continue;
            }
        };
        if fields.len() != lane.csv_columns.len() {
            reject(&mut report, &format!("field_count_{}", fields.len()));
            continue;
        }
        if let Some(position) = lane.pnu_position {
            if Pnu::parse(fields[position].trim()).is_err() {
                reject(&mut report, &classify_rejected_pnu(fields[position].trim()));
                continue;
            }
        }
        let mut row = serde_json::Map::new();
        let mut required_blank = None;
        for (column, value) in lane.csv_columns.iter().zip(&fields) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                if is_required_column(lane.contract, column) {
                    required_blank = Some((*column).to_owned());
                    break;
                }
                row.insert((*column).to_owned(), serde_json::Value::Null);
            } else {
                row.insert((*column).to_owned(), json!(trimmed));
            }
        }
        if let Some(column) = required_blank {
            reject(&mut report, &format!("blank_{column}"));
            continue;
        }
        row.insert("source_record_id".to_owned(), json!(source_record_id));
        row.insert(
            "source_snapshot_id".to_owned(),
            json!(config.source_snapshot_id),
        );
        row.insert("ingested_at_utc".to_owned(), json!(ingested_at_utc));

        serde_json::to_writer(&mut *writer, &serde_json::Value::Object(row))
            .context("failed to write a land-use handoff row")?;
        writer
            .write_all(b"\n")
            .context("failed to end a land-use handoff row")?;
        report.output_row_count += 1;
    }
    Ok(report)
}

fn reject(report: &mut StreamReport, reason: &str) {
    report.rejected_row_count += 1;
    *report
        .rejected_row_reasons
        .entry(reason.to_owned())
        .or_insert(0) += 1;
}

fn is_required_column(contract: &LakehouseTableContract, name: &str) -> bool {
    contract
        .columns
        .iter()
        .any(|column| column.name == name && column.required)
}

fn classify_rejected_pnu(raw: &str) -> String {
    if raw.len() != 19 {
        return format!("pnu_length_{}", raw.len());
    }
    if !raw.bytes().all(|b| b.is_ascii_digit()) {
        return "pnu_non_digit".to_owned();
    }
    format!("pnu_daejang_digit_{}", &raw[10..11])
}

/// Decodes one record's raw fields from EUC-KR (which is what the provider ships; the byte
/// values CSV structure cares about — comma, quote, CR, LF — never occur inside an EUC-KR
/// multi-byte sequence, so structure was safely parsed on bytes first).
fn decode_fields(record: &[Vec<u8>]) -> anyhow::Result<Vec<String>> {
    record
        .iter()
        .map(|field| {
            let (text, _, malformed) = encoding_rs::EUC_KR.decode(field);
            if malformed {
                bail!("a CSV field holds bytes EUC-KR cannot decode");
            }
            Ok(text.into_owned())
        })
        .collect()
}

/// Minimal RFC 4180 record reader over raw bytes.
///
/// Exists because the workspace carries no CSV crate and this lane needs exactly one shape:
/// comma-separated, optionally double-quoted fields with doubled-quote escapes, records ended
/// by LF or CRLF. Structure is parsed on bytes; decoding happens per field afterwards.
struct CsvRecords<R: Read> {
    source: R,
    /// One pushed-back byte, for the CR-not-followed-by-LF case.
    pending: Option<u8>,
    finished: bool,
}

impl<R: Read> CsvRecords<R> {
    fn new(source: R) -> Self {
        Self {
            source,
            pending: None,
            finished: false,
        }
    }

    fn next_byte(&mut self) -> std::io::Result<Option<u8>> {
        if let Some(byte) = self.pending.take() {
            return Ok(Some(byte));
        }
        let mut buffer = [0_u8; 1];
        loop {
            match self.source.read(&mut buffer) {
                Ok(0) => return Ok(None),
                Ok(_) => return Ok(Some(buffer[0])),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Reads one record, or `None` at a clean end of input.
    fn next_record(&mut self) -> anyhow::Result<Option<Vec<Vec<u8>>>> {
        if self.finished {
            return Ok(None);
        }
        let mut fields: Vec<Vec<u8>> = Vec::new();
        let mut field: Vec<u8> = Vec::new();
        let mut in_quotes = false;
        let mut saw_any = false;
        loop {
            let Some(byte) = self.next_byte().context("failed to read CSV bytes")? else {
                self.finished = true;
                if in_quotes {
                    bail!("the CSV ends inside a quoted field");
                }
                if !saw_any {
                    return Ok(None);
                }
                fields.push(field);
                return Ok(Some(fields));
            };
            saw_any = true;
            if in_quotes {
                if byte == b'"' {
                    match self.next_byte().context("failed to read CSV bytes")? {
                        Some(b'"') => field.push(b'"'),
                        Some(other) => {
                            in_quotes = false;
                            self.pending = Some(other);
                        }
                        None => in_quotes = false,
                    }
                } else {
                    field.push(byte);
                }
                continue;
            }
            match byte {
                b'"' if field.is_empty() => in_quotes = true,
                b',' => fields.push(std::mem::take(&mut field)),
                b'\n' => {
                    fields.push(field);
                    return Ok(Some(fields));
                }
                b'\r' => match self.next_byte().context("failed to read CSV bytes")? {
                    Some(b'\n') | None => {
                        fields.push(field);
                        return Ok(Some(fields));
                    }
                    Some(other) => {
                        field.push(b'\r');
                        self.pending = Some(other);
                    }
                },
                other => field.push(other),
            }
        }
    }
}

fn write_summary(
    summary_path: &Path,
    config: &ExportConfig,
    lane: &Lane,
    report: &ExportReport,
) -> anyhow::Result<()> {
    let summary = json!({
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "outcome": OUTCOME_CONVERTED,
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "source": {
            "input": config.input.describe(),
            "dataset_name": report.dataset_name,
            "source_record_id": config.source_record_id(),
            "source_snapshot_id": config.source_snapshot_id,
            "input_row_count": report.input_row_count,
        },
        "output": {
            "destination": config.output.describe(),
            "contract": lane.contract.table_name,
            "row_count": report.output_row_count,
            "bytes": report.output_bytes,
        },
        "rejected_row_count": report.rejected_row_count,
        "rejected_row_reasons": report.rejected_row_reasons,
        "performance": { "elapsed_milliseconds": report.elapsed_milliseconds },
        "evidence_limitations": summary_limitations(config),
    });
    write_summary_json(summary_path, &summary)
}

fn write_already_present_summary(config: &ExportConfig, lane: &Lane) -> anyhow::Result<()> {
    let Some(summary_path) = &config.summary_path else {
        return Ok(());
    };
    if summary_path.exists() {
        bail!("refusing to overwrite an existing land-use summary output");
    }
    let summary = json!({
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "outcome": OUTCOME_ALREADY_PRESENT,
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "source": {
            "input": config.input.describe(),
            "source_record_id": config.source_record_id(),
            "source_snapshot_id": config.source_snapshot_id,
        },
        "output": {
            "destination": config.output.describe(),
            "contract": lane.contract.table_name,
        },
        "measurements_absent_because":
            "the handoff already exists in the bucket and this run did not rebuild it",
        "evidence_limitations": summary_limitations(config),
    });
    write_summary_json(summary_path, &summary)
}

fn summary_limitations(config: &ExportConfig) -> Vec<&'static str> {
    let mut limitations = vec![
        "does_not_write_iceberg_table",
        "does_not_approve_national_rollout",
    ];
    if !config.input.is_r2() {
        limitations.push("read_a_local_file_not_a_collected_object");
    }
    if !config.output.is_r2() {
        limitations.push("does_not_write_r2");
    }
    limitations
}

fn write_summary_json(summary_path: &Path, summary: &serde_json::Value) -> anyhow::Result<()> {
    if let Some(parent) = summary_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    fs::write(
        summary_path,
        format!("{}\n", serde_json::to_string_pretty(summary)?),
    )
    .with_context(|| format!("failed to write summary {}", summary_path.display()))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use uuid::Uuid;

    use super::*;

    fn records_of(bytes: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut reader = CsvRecords::new(bytes);
        let mut all = Vec::new();
        while let Some(record) = reader.next_record().expect("csv parse") {
            all.push(record);
        }
        all
    }

    #[test]
    fn csv_records_handle_quotes_crlf_and_final_line() {
        let rows = records_of(b"a,b,c\r\n\"x,\"\"y\",2,\ntail,1,2");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(rows[1], vec![b"x,\"y".to_vec(), b"2".to_vec(), Vec::new()]);
        assert_eq!(
            rows[2],
            vec![b"tail".to_vec(), b"1".to_vec(), b"2".to_vec()]
        );
    }

    #[test]
    fn csv_records_refuse_an_unterminated_quote() {
        let mut reader = CsvRecords::new(&b"\"broken"[..]);
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn csv_records_keep_a_bare_carriage_return_inside_the_field() {
        let rows = records_of(b"a\rb,c\n");
        assert_eq!(rows, vec![vec![b"a\rb".to_vec(), b"c".to_vec()]]);
    }

    /// The CSV-position mapping is the one new fact this module owns; everything else about
    /// the row shape must come from the contract. This holds them together so a contract
    /// column added without a mapping (or the reverse) refuses here rather than at load time.
    #[test]
    fn lane_mappings_cover_their_contracts_exactly() {
        for lane in [&PLAN_LANE, &ZONE_CODE_LANE, &PRICE_LANE] {
            assert_eq!(lane.expected_header.len(), lane.csv_columns.len());
            let mapped: Vec<&str> = lane
                .csv_columns
                .iter()
                .copied()
                .chain(LINEAGE_COLUMNS)
                .collect();
            let declared: Vec<&str> = lane
                .contract
                .columns
                .iter()
                .map(|column| column.name)
                .collect();
            assert_eq!(
                mapped, declared,
                "lane {} drifted",
                lane.contract.table_name
            );
        }
    }

    fn euc_kr(text: &str) -> Vec<u8> {
        let (bytes, _, malformed) = encoding_rs::EUC_KR.encode(text);
        assert!(!malformed);
        bytes.into_owned()
    }

    fn plan_zip(rows: &[&str]) -> Vec<u8> {
        let mut csv = euc_kr(&PLAN_LANE.expected_header.join(","));
        csv.push(b'\n');
        for row in rows {
            csv.extend_from_slice(&euc_kr(row));
            csv.push(b'\n');
        }
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut cursor);
            writer
                .start_file::<_, ()>(
                    "AL_D155_36_20260609.csv",
                    zip::write::FileOptions::default(),
                )
                .expect("zip member");
            writer.write_all(&csv).expect("zip body");
            writer.finish().expect("zip finish");
        }
        cursor.into_inner()
    }

    #[test]
    fn plan_rows_become_contract_shaped_jsonl_and_the_adjacent_row_is_refused_with_a_name() {
        let temp = std::env::temp_dir().join(format!("land-use-export-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).expect("temp dir");
        let zip_path = temp.join("plan.zip");
        fs::write(
            &zip_path,
            plan_zip(&[
                "9999938029104450003,9999938029,세종특별자치시 금남면 도암리,1,토지대장,445-3,1380000,3,접함,UEA110,농업진흥구역,2026-06-04,2026-06-06,99999,",
                "99999380291044500,9999938029,세종특별자치시 금남면 도암리,1,토지대장,445-3,1380000,1,포함,UQB300,보전관리지역,2026-06-04,2026-06-06,99999,",
            ]),
        )
        .expect("fixture zip");
        let out_path = temp.join("plan.jsonl");
        let summary_path = temp.join("plan.summary.json");

        let config = ExportConfig {
            input: InputSource::LocalPath(zip_path),
            output: OutputSink::LocalPath(out_path.clone()),
            summary_path: Some(summary_path.clone()),
            source_snapshot_id: "iceberg:test-vintage-20260609".to_owned(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(export_handoff(&config, &PLAN_LANE))
            .expect("export");

        assert_eq!(report.input_row_count, 2);
        assert_eq!(report.output_row_count, 1);
        assert_eq!(report.rejected_row_count, 1);
        assert_eq!(report.rejected_row_reasons.get("pnu_length_17"), Some(&1));

        let body = fs::read_to_string(&out_path).expect("handoff body");
        let row: serde_json::Value = serde_json::from_str(body.trim()).expect("row json");
        assert_eq!(row["pnu"], "9999938029104450003");
        assert_eq!(row["zone_code"], "UEA110");
        assert_eq!(row["zone_name"], "농업진흥구역");
        assert_eq!(row["inclusion_code"], "3");
        assert_eq!(row["remark"], serde_json::Value::Null);
        assert_eq!(row["source_snapshot_id"], "iceberg:test-vintage-20260609");
        let summary: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&summary_path).expect("summary"))
                .expect("summary json");
        assert_eq!(summary["output"]["contract"], "silver.land_use_plan");
        assert_eq!(summary["rejected_row_count"], 1);

        fs::remove_dir_all(&temp).expect("cleanup");
    }

    #[test]
    fn a_drifted_header_refuses_instead_of_shifting_columns() {
        let temp = std::env::temp_dir().join(format!("land-use-header-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).expect("temp dir");
        let mut csv = euc_kr("고유번호,법정동코드,새로생긴열");
        csv.push(b'\n');
        let zip_path = temp.join("drifted.zip");
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut cursor);
            writer
                .start_file::<_, ()>(
                    "AL_D155_36_20260609.csv",
                    zip::write::FileOptions::default(),
                )
                .expect("zip member");
            writer.write_all(&csv).expect("zip body");
            writer.finish().expect("zip finish");
        }
        fs::write(&zip_path, cursor.into_inner()).expect("fixture zip");

        let config = ExportConfig {
            input: InputSource::LocalPath(zip_path),
            output: OutputSink::LocalPath(temp.join("out.jsonl")),
            summary_path: None,
            source_snapshot_id: "iceberg:test".to_owned(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let error = runtime
            .block_on(export_handoff(&config, &PLAN_LANE))
            .expect_err("a drifted header must refuse");
        assert!(error.to_string().contains("layout"), "{error}");

        fs::remove_dir_all(&temp).expect("cleanup");
    }
}
