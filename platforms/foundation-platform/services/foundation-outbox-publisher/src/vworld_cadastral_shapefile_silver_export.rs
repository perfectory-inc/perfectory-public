//! VWorld cadastral zipped-shapefile to Silver JSONL handoff command.

use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use collection_domain::VWorldCadastralDedupedFeature;
use foundation_outbox::{
    object_storage::{R2MultipartUploadWriter, R2SeekableObjectReader},
    R2ObjectStorage,
};
use foundation_shapefile::{ShapefileMetadata, ZipShapefileReader};
use foundation_shared_kernel::Pnu;
use lakehouse_application::{
    build_vworld_cadastral_silver_parcel_boundary_handoff,
    normalize_vworld_cadastral_silver_parcel_boundary_rows,
    VWorldCadastralSilverParcelBoundaryRowsInput,
};
use serde_json::json;
use uuid::Uuid;

const INPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_INPUT_PATH";
const OUTPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_OUTPUT_PATH";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SUMMARY_PATH";
const SOURCE_RECORD_ID_ENV: &str =
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_RECORD_ID";
const SOURCE_SNAPSHOT_ID_ENV: &str =
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_SNAPSHOT_ID";
const VALID_FROM_UTC_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_VALID_FROM_UTC";
const INPUT_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_INPUT_OBJECT_KEY";
const OUTPUT_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_OUTPUT_OBJECT_KEY";

/// How much of an R2 object one ranged request fetches.
///
/// A ZIP is read by seeking to its central directory at the end and then back to the member
/// it names, so a per-call request count would be dominated by two jumps rather than by the
/// bytes wanted. Eight mebibytes keeps that scan to a handful of requests while never holding
/// more than one chunk.
const R2_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// How much handoff JSONL accumulates before a part is uploaded.
///
/// R2 requires at least five mebibytes for every part but the last, and caps an upload at
/// 10,000 parts. Sixteen mebibytes clears the floor and still admits a 156 GB output — an
/// order of magnitude above the 45.7 GB the national parcel run produced.
const R2_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;

const HANDOFF_CONTENT_TYPE: &str = "application/x-ndjson";
/// The handoff is an input to a load, not something served. Nothing should cache it.
const HANDOFF_CACHE_CONTROL: &str = "no-store";

/// Runs one `VWorld` cadastral shapefile ZIP to Silver JSONL handoff conversion.
///
/// Either end may be a local path or an R2 object key. With keys on both, the archive is read
/// by ranged request and the handoff is uploaded as it is produced, so a national extract
/// never lands on a disk — the run that produced 45.7 GB of intermediate JSONL staged all of
/// it, and routed it through a laptop on the way.
///
/// # Errors
///
/// Returns an error when configuration, source validation, normalization, or publishing the
/// output fails.
pub async fn run() -> anyhow::Result<()> {
    let config = ExportConfig::from_env()?;

    // Offered every object on every run, the way the loader is offered every batch. Whether this
    // one is already done is answered by the bucket rather than by a record beside the runner,
    // because a record in a third place is a record that can disagree with the other two.
    if already_exported(&config).await? {
        tracing::info!(
            output = %config.output.describe(),
            "VWorld cadastral shapefile Silver handoff already exists; nothing to do"
        );
        return Ok(());
    }

    let report = export_handoff(&config).await?;
    tracing::info!(
        input_feature_count = report.input_feature_count,
        valid_pnu_count = report.valid_pnu_count,
        invalid_pnu_count = report.invalid_pnu_count,
        output_row_count = report.output_row_count,
        output_bytes = report.output_bytes,
        elapsed_milliseconds = report.elapsed_milliseconds,
        input = %config.input.describe(),
        output = %config.output.describe(),
        "VWorld cadastral shapefile Silver handoff export succeeded"
    );
    Ok(())
}

/// Where the source ZIP is read from.
///
/// A local path and an R2 key are both legitimate: a single file being examined by hand is a
/// path, and a national run is a key. Naming them as one type keeps the conversion below from
/// caring which it got.
#[derive(Clone, Debug, Eq, PartialEq)]
enum InputSource {
    LocalPath(PathBuf),
    R2Object(String),
}

/// Where the handoff JSONL is written.
#[derive(Clone, Debug, Eq, PartialEq)]
enum OutputSink {
    LocalPath(PathBuf),
    R2Object(String),
}

impl InputSource {
    fn describe(&self) -> String {
        match self {
            Self::LocalPath(path) => path.display().to_string(),
            Self::R2Object(key) => format!("r2://{key}"),
        }
    }

    const fn is_r2(&self) -> bool {
        matches!(self, Self::R2Object(_))
    }
}

impl OutputSink {
    fn describe(&self) -> String {
        match self {
            Self::LocalPath(path) => path.display().to_string(),
            Self::R2Object(key) => format!("r2://{key}"),
        }
    }

    const fn is_r2(&self) -> bool {
        matches!(self, Self::R2Object(_))
    }
}

/// Reads exactly one of a path variable and a key variable.
///
/// Both set is refused rather than resolved by precedence: a run configured with two sources
/// is a run whose operator believed something about it that is not true, and silently
/// preferring one writes the wrong lineage into the summary.
fn one_of_path_or_key(path_env: &str, key_env: &str) -> anyhow::Result<PathOrKey> {
    match (optional_env(path_env)?, optional_env(key_env)?) {
        (Some(path), None) => Ok(PathOrKey::Path(PathBuf::from(path))),
        (None, Some(key)) => Ok(PathOrKey::Key(key)),
        (Some(_), Some(_)) => bail!("set {path_env} or {key_env}, not both"),
        (None, None) => bail!("{path_env} or {key_env} is required"),
    }
}

enum PathOrKey {
    Path(PathBuf),
    Key(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportConfig {
    input: InputSource,
    output: OutputSink,
    summary_path: Option<PathBuf>,
    source_record_id: String,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportReport {
    input_feature_count: u64,
    valid_pnu_count: u64,
    invalid_pnu_count: u64,
    invalid_pnu_reasons: BTreeMap<String, u64>,
    output_row_count: u64,
    output_bytes: u64,
    elapsed_milliseconds: u128,
    metadata: ShapefileMetadata,
}

struct StreamReport {
    input_feature_count: u64,
    valid_pnu_count: u64,
    invalid_pnu_count: u64,
    invalid_pnu_reasons: BTreeMap<String, u64>,
    output_row_count: u64,
    contract_table_name: &'static str,
    quality_metrics: BTreeMap<String, u64>,
}

impl ExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let input = match one_of_path_or_key(INPUT_PATH_ENV, INPUT_OBJECT_KEY_ENV)? {
            PathOrKey::Path(path) => InputSource::LocalPath(path),
            PathOrKey::Key(key) => InputSource::R2Object(key),
        };
        let output = match one_of_path_or_key(OUTPUT_PATH_ENV, OUTPUT_OBJECT_KEY_ENV)? {
            PathOrKey::Path(path) => OutputSink::LocalPath(path),
            PathOrKey::Key(key) => OutputSink::R2Object(key),
        };
        Ok(Self {
            input,
            output,
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
            source_record_id: required_env(SOURCE_RECORD_ID_ENV)?,
            source_snapshot_id: required_env(SOURCE_SNAPSHOT_ID_ENV)?,
            valid_from_utc: parse_utc_env(VALID_FROM_UTC_ENV)?,
        })
    }

    const fn uses_r2(&self) -> bool {
        self.input.is_r2() || self.output.is_r2()
    }
}

/// A ZIP source the shapefile reader can seek in, wherever it lives.
enum ShapefileSource {
    Local(BufReader<fs::File>),
    R2(Box<R2SeekableObjectReader>),
}

impl Read for ShapefileSource {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Local(source) => source.read(output),
            Self::R2(source) => source.read(output),
        }
    }
}

impl Seek for ShapefileSource {
    fn seek(&mut self, seek_from: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Local(source) => source.seek(seek_from),
            Self::R2(source) => source.seek(seek_from),
        }
    }
}

/// A handoff destination that only becomes visible once the whole conversion succeeded.
///
/// Both variants hold that property by construction and neither one can be half-written: the
/// local one stages beside its final name and renames, and the R2 one uploads parts that no
/// reader can address until the upload is completed.
enum HandoffSink {
    Local {
        pending: PendingOutput,
        final_path: PathBuf,
    },
    R2(Box<R2MultipartUploadWriter>),
}

impl Write for HandoffSink {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        match self {
            Self::Local { pending, .. } => pending
                .file_mut()
                .map_err(io::Error::other)
                .and_then(|file| file.write(input)),
            Self::R2(writer) => writer.write(input),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Local { pending, .. } => pending
                .file_mut()
                .map_err(io::Error::other)
                .and_then(std::io::Write::flush),
            Self::R2(writer) => writer.flush(),
        }
    }
}

impl HandoffSink {
    /// Publishes the handoff and reports how many bytes it holds.
    fn finish(self) -> anyhow::Result<u64> {
        match self {
            Self::Local {
                mut pending,
                final_path,
            } => {
                let output_bytes = pending
                    .file_mut()?
                    .metadata()
                    .context("failed to inspect staged shapefile JSONL")?
                    .len();
                pending.sync_and_commit(&final_path)?;
                Ok(output_bytes)
            }
            Self::R2(writer) => {
                let report = writer
                    .complete()
                    .context("failed to complete the handoff upload to R2")?;
                Ok(report.output_bytes)
            }
        }
    }
}

/// Names why one source PNU was refused, without deciding what the name means.
///
/// `Pnu::parse` accepts only the standard 대장구분 table (1/2/8/9). Real national cadastral
/// extracts also carry other digits in that position; the repository has no source that
/// defines them, so this reports the digit it saw instead of inventing a category for it.
fn classify_rejected_pnu(raw: &str) -> String {
    if raw.len() != 19 {
        return format!("length_{}", raw.len());
    }
    if !raw.bytes().all(|b| b.is_ascii_digit()) {
        return "non_digit".to_owned();
    }
    format!("daejang_digit_{}", &raw[10..11])
}

/// Opens both ends, then converts on a thread that is allowed to block.
///
/// The split is not stylistic. `R2SeekableObjectReader` and `R2MultipartUploadWriter` are
/// synchronous by design — the shapefile and ZIP readers want `Read + Seek`, not futures — and
/// they reach R2 by blocking the calling thread on a request. A Tokio worker thread must never
/// be blocked that way, so the conversion runs on the blocking pool while the two handles,
/// which need a runtime to be created at all, are opened here.
async fn export_handoff(config: &ExportConfig) -> anyhow::Result<ExportReport> {
    refuse_existing_outputs(config)?;

    let storage = if config.uses_r2() {
        Some(R2ObjectStorage::from_env().context("failed to configure R2 for a streamed handoff")?)
    } else {
        None
    };
    let source = open_source(&config.input, storage.as_ref()).await?;
    let sink = open_sink(&config.output, storage.as_ref()).await?;

    let owned = config.clone();
    run_where_blocking_is_allowed(move || convert(source, sink, &owned)).await
}

/// Runs `work` on a thread that may block on the runtime.
///
/// Both R2 handles block the calling thread on every request. Doing that on a runtime worker
/// thread panics, and the writer takes its cleanup path while unwinding — so the mistake
/// surfaces as a dead process during a national run rather than as an error. This function is
/// separate so that property has one place to be stated and one place to be tested.
async fn run_where_blocking_is_allowed<F, T>(work: F) -> anyhow::Result<T>
where
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .context("failed to join the shapefile Silver handoff conversion")?
}

/// Whether this run's output is already in the bucket.
///
/// Only asked of an R2 output. A local one is answered by `refuse_existing_outputs`, which stops
/// rather than skips: a path the operator typed twice is a mistake worth hearing about, while a
/// key the runner derived is the same key it derived last time.
async fn already_exported(config: &ExportConfig) -> anyhow::Result<bool> {
    let OutputSink::R2Object(key) = &config.output else {
        return Ok(false);
    };
    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 while checking for an existing handoff")?;
    storage
        .object_exists(key)
        .await
        .with_context(|| format!("failed to check whether the handoff {key} already exists"))
}

fn refuse_existing_outputs(config: &ExportConfig) -> anyhow::Result<()> {
    if let OutputSink::LocalPath(path) = &config.output {
        if path.exists() {
            bail!(
                "refusing to overwrite existing shapefile JSONL output {}",
                path.display()
            );
        }
    }
    if config
        .summary_path
        .as_ref()
        .is_some_and(|path| path.exists())
    {
        bail!("refusing to overwrite existing shapefile summary output");
    }
    Ok(())
}

async fn open_source(
    input: &InputSource,
    storage: Option<&R2ObjectStorage>,
) -> anyhow::Result<ShapefileSource> {
    match input {
        InputSource::LocalPath(path) => {
            let file = fs::File::open(path).with_context(|| {
                format!(
                    "failed to open VWorld cadastral shapefile ZIP {}",
                    path.display()
                )
            })?;
            Ok(ShapefileSource::Local(BufReader::new(file)))
        }
        InputSource::R2Object(key) => {
            let storage = storage.context("an R2 input key needs R2 configuration")?;
            let reader = storage
                .open_seekable_object(key, R2_READ_CHUNK_BYTES)
                .await
                .with_context(|| format!("failed to open R2 shapefile ZIP {key}"))?;
            Ok(ShapefileSource::R2(Box::new(reader)))
        }
    }
}

async fn open_sink(
    output: &OutputSink,
    storage: Option<&R2ObjectStorage>,
) -> anyhow::Result<HandoffSink> {
    match output {
        OutputSink::LocalPath(path) => Ok(HandoffSink::Local {
            pending: PendingOutput::create(path)?,
            final_path: path.clone(),
        }),
        OutputSink::R2Object(key) => {
            let storage = storage.context("an R2 output key needs R2 configuration")?;
            let writer = storage
                .start_create_only_multipart_upload(
                    key,
                    HANDOFF_CONTENT_TYPE,
                    HANDOFF_CACHE_CONTROL,
                    R2_UPLOAD_PART_BYTES,
                )
                .await
                .with_context(|| format!("failed to start the handoff upload to R2 {key}"))?;
            Ok(HandoffSink::R2(Box::new(writer)))
        }
    }
}

fn convert(
    source: ShapefileSource,
    mut sink: HandoffSink,
    config: &ExportConfig,
) -> anyhow::Result<ExportReport> {
    let started = Instant::now();
    let mut reader = ZipShapefileReader::new(source)?;
    let metadata = reader.metadata().clone();
    let mut writer = BufWriter::new(&mut sink);
    let mut stream_report = stream_silver_rows(&mut reader, &mut writer, config)?;
    writer.flush().context("failed to flush shapefile JSONL")?;
    drop(writer);
    let output_bytes = sink.finish()?;
    stream_report
        .quality_metrics
        .insert("row_count".to_owned(), stream_report.output_row_count);
    stream_report.quality_metrics.insert(
        "invalid_pnu_count".to_owned(),
        stream_report.invalid_pnu_count,
    );

    let report = ExportReport {
        input_feature_count: stream_report.input_feature_count,
        valid_pnu_count: stream_report.valid_pnu_count,
        invalid_pnu_count: stream_report.invalid_pnu_count,
        invalid_pnu_reasons: stream_report.invalid_pnu_reasons.clone(),
        output_row_count: stream_report.output_row_count,
        output_bytes,
        elapsed_milliseconds: started.elapsed().as_millis(),
        metadata,
    };
    if let Some(summary_path) = &config.summary_path {
        write_summary(
            summary_path,
            config,
            &report,
            stream_report.contract_table_name,
            &stream_report.quality_metrics,
        )?;
    }
    Ok(report)
}

fn stream_silver_rows(
    reader: &mut ZipShapefileReader,
    writer: &mut impl Write,
    config: &ExportConfig,
) -> anyhow::Result<StreamReport> {
    let ingested_at_utc = Utc::now();
    let empty_handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&[])
        .context("failed to initialize Silver handoff quality metrics")?;
    let mut quality_metrics = empty_handoff.quality_metrics;
    let mut valid_pnu_count = 0_u64;
    let mut invalid_pnu_count = 0_u64;
    // A single rejection total cannot be acted on: it does not say whether the source is
    // damaged or whether it carries a register class this repository does not admit. The
    // breakdown is the observation; what each class means is not decided here.
    let mut invalid_pnu_reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut output_row_count = 0_u64;
    let input_feature_count = reader.for_each_feature(|source_feature| {
        let Some(raw_pnu) = source_feature.optional_text("PNU")? else {
            invalid_pnu_count += 1;
            *invalid_pnu_reasons.entry("absent".to_owned()).or_insert(0) += 1;
            return Ok(());
        };
        let Ok(pnu) = Pnu::parse(raw_pnu.to_owned()) else {
            invalid_pnu_count += 1;
            *invalid_pnu_reasons
                .entry(classify_rejected_pnu(raw_pnu))
                .or_insert(0) += 1;
            return Ok(());
        };
        // The Silver contract does not require `jibun`, and `VWorldCadastralSilverParcelBoundaryRow`
        // holds it as `Option<String>`. A blank one is a parcel whose descriptive label the source
        // left empty, not a broken record: 17 of 584,553 rows across four national extracts. Demanding
        // it here refused those four files whole and discarded 584,536 sound parcels with them.
        let jibun = source_feature.optional_text("JIBUN")?;
        let properties = json!({
            "pnu": pnu.as_str(),
            "jibun": jibun,
            "bonbun": pnu.bonbun(),
            "bubun": pnu.bubun()
        });
        let geometry = source_feature.geometry().clone();
        let record = VWorldCadastralDedupedFeature {
            pnu: pnu.as_str().to_owned(),
            feature: json!({
                "type": "Feature",
                "properties": properties,
                "geometry": geometry
            }),
            properties,
            geometry,
            occurrence_count: 1,
        };
        let rows = normalize_vworld_cadastral_silver_parcel_boundary_rows(
            &VWorldCadastralSilverParcelBoundaryRowsInput {
                records: std::slice::from_ref(&record),
                source_record_id: &config.source_record_id,
                source_snapshot_id: &config.source_snapshot_id,
                valid_from_utc: config.valid_from_utc,
                ingested_at_utc,
            },
        )
        .context("failed to normalize shapefile feature into Silver parcel boundary")?;
        let handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&rows)
            .context("failed to serialize shapefile Silver parcel-boundary row")?;
        writer
            .write_all(handoff.jsonl.as_bytes())
            .context("failed to stream shapefile Silver JSONL row")?;
        add_quality_metrics(&mut quality_metrics, &handoff.quality_metrics);
        valid_pnu_count += 1;
        output_row_count += u64::try_from(rows.len()).context("output row count overflow")?;
        Ok(())
    })?;
    Ok(StreamReport {
        input_feature_count,
        valid_pnu_count,
        invalid_pnu_count,
        invalid_pnu_reasons,
        output_row_count,
        contract_table_name: empty_handoff.contract_table_name,
        quality_metrics,
    })
}

/// States what this run did not prove, for the run it actually was.
///
/// The list was a constant that always claimed the handoff touched neither R2 nor a local-only
/// path. A summary that says it wrote nothing to R2 beside an object it just uploaded is worse
/// than no summary: it is the document somebody reads instead of looking.
fn evidence_limitations(config: &ExportConfig) -> Vec<&'static str> {
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
    limitations.sort_unstable();
    limitations
}

fn add_quality_metrics(target: &mut BTreeMap<String, u64>, row: &BTreeMap<String, u64>) {
    for (name, count) in row {
        *target.entry(name.clone()).or_insert(0) += count;
    }
}

fn write_summary(
    path: &Path,
    config: &ExportConfig,
    report: &ExportReport,
    contract_table_name: &str,
    quality_metrics: &BTreeMap<String, u64>,
) -> anyhow::Result<()> {
    let summary = json!({
        "schema_version": "foundation-platform.vworld_cadastral_shapefile_silver_handoff_export.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "source": {
            "kind": "vworld_shapefile_zip",
            "input": config.input.describe(),
            "dataset_name": report.metadata.dataset_name,
            "cpg_label": report.metadata.cpg_label,
            "source_crs_name": report.metadata.source_crs_name,
            "shape_count": report.metadata.shape_count,
            "dbf_record_count": report.metadata.dbf_record_count,
            "seekable_member_bytes": report.metadata.seekable_member_bytes,
            "input_feature_count": report.input_feature_count,
            "source_record_id": config.source_record_id,
            "source_snapshot_id": config.source_snapshot_id
        },
        "output": {
            "destination": config.output.describe(),
            "contract": contract_table_name,
            "row_count": report.output_row_count,
            "bytes": report.output_bytes
        },
        "invalid_pnu_reasons": report.invalid_pnu_reasons,
        "quality_metrics": quality_metrics,
        "performance": {
            "elapsed_milliseconds": report.elapsed_milliseconds
        },
        "evidence_limitations": evidence_limitations(config)
    });
    let bytes = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize shapefile export summary")?;
    let mut pending = PendingOutput::create(path)?;
    pending
        .file_mut()?
        .write_all(&bytes)
        .with_context(|| format!("failed to write staged summary for {}", path.display()))?;
    pending.sync_and_commit(path)
}

struct PendingOutput {
    path: PathBuf,
    file: Option<fs::File>,
    committed: bool,
}

impl PendingOutput {
    fn create(final_path: &Path) -> anyhow::Result<Self> {
        let parent = final_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
        let file_name = final_path
            .file_name()
            .context("output path must name a file")?
            .to_string_lossy();
        let path = parent.join(format!(".{file_name}.partial-{}", Uuid::new_v4()));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("failed to create staged output {}", path.display()))?;
        Ok(Self {
            path,
            file: Some(file),
            committed: false,
        })
    }

    fn file_mut(&mut self) -> anyhow::Result<&mut fs::File> {
        self.file
            .as_mut()
            .context("staged output is already closed")
    }

    fn sync_and_commit(mut self, final_path: &Path) -> anyhow::Result<()> {
        let file = self
            .file
            .take()
            .context("staged output is already closed")?;
        file.sync_all()
            .with_context(|| format!("failed to sync staged output {}", self.path.display()))?;
        drop(file);
        fs::rename(&self.path, final_path).with_context(|| {
            format!(
                "failed to promote staged output {} to {}",
                self.path.display(),
                final_path.display()
            )
        })?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(trimmed.to_owned())
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_utc_env(name: &str) -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(name)?;
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("{name} must be an RFC3339 timestamp"))?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Cursor, Write as _},
    };

    use chrono::{DateTime, Utc};
    use shapefile::{
        dbase::{self, encoding::EncodingRs, FieldName, FieldValue},
        Point, Polygon, PolygonRing, ShapeWriter, Writer,
    };
    use uuid::Uuid;
    use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

    use std::path::PathBuf;

    use super::{
        evidence_limitations, export_handoff, run_where_blocking_is_allowed, ExportConfig,
        InputSource, OutputSink,
    };

    const KOREA_CENTRAL_BELT_2010: &str = concat!(
        r#"PROJCS["Korea_2000_Korea_Central_Belt_2010","#,
        r#"GEOGCS["GCS_Korea_2000",DATUM["D_Korea_2000","#,
        r#"SPHEROID["GRS_1980",6378137.0,298.257222101]],"#,
        r#"PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],"#,
        r#"PROJECTION["Transverse_Mercator"],"#,
        r#"PARAMETER["False_Easting",200000.0],"#,
        r#"PARAMETER["False_Northing",600000.0],"#,
        r#"PARAMETER["Central_Meridian",127.0],"#, // public-repository-safety: reviewed-runtime-coordinate
        r#"PARAMETER["Scale_Factor",1.0],"#,
        r#"PARAMETER["Latitude_Of_Origin",38.0],UNIT["Meter",1.0]]"#,
    );

    #[tokio::test]
    async fn streams_file_source_rows_through_the_existing_silver_contract() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-vworld-shapefile-export-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&root)?;
        let input_path = root.join("parcel.zip");
        fs::write(&input_path, fixture_zip()?)?;
        let output_path = root.join("parcel.jsonl");
        let summary_path = root.join("parcel.summary.json");
        let config = ExportConfig {
            input: InputSource::LocalPath(input_path),
            output: OutputSink::LocalPath(output_path.clone()),
            summary_path: Some(summary_path.clone()),
            source_record_id: "bronze:vworldkr__parcel:fixture".to_owned(),
            source_snapshot_id: "vworldkr__parcel:202606".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")?
                .with_timezone(&Utc),
        };

        let report = export_handoff(&config).await?;

        assert_eq!(report.input_feature_count, 3);
        assert_eq!(report.valid_pnu_count, 1);
        assert_eq!(report.invalid_pnu_count, 2);
        assert_eq!(report.output_row_count, 1);
        let lines = fs::read_to_string(&output_path)?
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["pnu"], "9999938029105800001");
        assert_eq!(lines[0]["sido_code"], "99");
        assert_eq!(lines[0]["sigungu_code"], "99999");
        assert_eq!(lines[0]["bjdong_code"], "9999938029");
        assert_eq!(lines[0]["jibun"], "산 580-1");
        assert_eq!(lines[0]["bonbun"], "0580");
        assert_eq!(lines[0]["bubun"], "0001");
        assert_eq!(lines[0]["geometry_srid"], 4326);
        assert_eq!(lines[0]["geometry_wkb_encoding"], "hex");
        assert_eq!(lines[0]["source_record_id"], config.source_record_id);
        let summary: serde_json::Value = serde_json::from_slice(&fs::read(summary_path)?)?;
        assert_eq!(summary["source"]["kind"], "vworld_shapefile_zip");
        assert_eq!(summary["quality_metrics"]["invalid_pnu_count"], 2);
        // A total alone cannot be acted on; the run must say which refusal it saw.
        assert_eq!(summary["invalid_pnu_reasons"]["daejang_digit_0"], 1);
        assert_eq!(summary["invalid_pnu_reasons"]["absent"], 1);
        assert_eq!(summary["output"]["row_count"], 1);
        assert_eq!(summary["output"]["contract"], "silver.parcel_boundaries");
        fs::remove_dir_all(root)?;
        Ok(())
    }

    /// The conversion must land where blocking the thread is allowed.
    ///
    /// This is the whole reason the conversion is not simply awaited inline. Both R2 handles
    /// are synchronous and reach R2 by blocking whatever thread calls them; a runtime worker
    /// thread refuses to be blocked, and the refusal arrives as a panic inside the multipart
    /// writer's cleanup — which is to say, during a national run, as a dead process rather
    /// than an error anyone can read.
    ///
    /// The body does exactly what the R2 handles do. Move the work back inline and this test
    /// stops passing; the local-path mode, which never blocks on R2, would not have noticed.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_conversion_runs_where_r2_may_block() -> anyhow::Result<()> {
        let observed = run_where_blocking_is_allowed(|| {
            Ok(tokio::runtime::Handle::current().block_on(async { "blocked" }))
        })
        .await?;

        assert_eq!(observed, "blocked");
        Ok(())
    }

    /// A summary that claims a run wrote nothing to R2 beside the object it just uploaded is
    /// the document somebody reads instead of looking at the bucket.
    #[test]
    fn the_summary_states_the_limitations_of_the_run_it_actually_was() -> anyhow::Result<()> {
        let base = ExportConfig {
            input: InputSource::LocalPath(PathBuf::from("parcel.zip")),
            output: OutputSink::LocalPath(PathBuf::from("parcel.jsonl")),
            summary_path: None,
            source_record_id: "bronze:vworldkr__parcel:fixture".to_owned(),
            source_snapshot_id: "vworldkr__parcel:202606".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")?
                .with_timezone(&Utc),
        };
        assert!(evidence_limitations(&base).contains(&"does_not_write_r2"));

        let streamed = ExportConfig {
            input: InputSource::R2Object("bronze/parcel.zip".to_owned()),
            output: OutputSink::R2Object("silver/parcel.jsonl".to_owned()),
            ..base
        };
        let limitations = evidence_limitations(&streamed);
        assert!(!limitations.contains(&"does_not_write_r2"));
        assert!(!limitations.contains(&"read_a_local_file_not_a_collected_object"));
        // What this command still does not do is unchanged by where the bytes came from.
        assert!(limitations.contains(&"does_not_write_iceberg_table"));
        Ok(())
    }

    fn fixture_zip() -> anyhow::Result<Vec<u8>> {
        let mut shp = Cursor::new(Vec::new());
        let mut shx = Cursor::new(Vec::new());
        let mut dbf = Cursor::new(Vec::new());
        {
            let shape_writer = ShapeWriter::with_shx(&mut shp, &mut shx);
            let dbase_writer =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR))
                    .add_character_field(field_name("PNU")?, 19)
                    .add_character_field(field_name("JIBUN")?, 100)
                    .build_with_dest(&mut dbf);
            let mut writer = Writer::new(shape_writer, dbase_writer);
            writer.write_shape_and_record(
                &square(200_000.0, 600_000.0),
                &record("9999938029105800001", "산 580-1"),
            )?;
            writer.write_shape_and_record(
                &square(200_100.0, 600_100.0),
                &record("9999938029005810000", "581"),
            )?;
            writer.write_shape_and_record(
                &square(200_200.0, 600_200.0),
                &record("", "PNU 없는 행"),
            )?;
        }
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for (name, bytes) in [
            ("parcel.shp", shp.into_inner()),
            ("parcel.shx", shx.into_inner()),
            ("parcel.dbf", dbf.into_inner()),
            ("parcel.prj", KOREA_CENTRAL_BELT_2010.as_bytes().to_vec()),
            ("parcel.cpg", b"EUC-KR".to_vec()),
        ] {
            zip.start_file(name, options)?;
            zip.write_all(&bytes)?;
        }
        Ok(zip.finish()?.into_inner())
    }

    fn field_name(value: &str) -> anyhow::Result<FieldName> {
        FieldName::try_from(value).map_err(|error| anyhow::anyhow!(error))
    }

    fn square(min_x: f64, min_y: f64) -> Polygon {
        Polygon::new(PolygonRing::Outer(vec![
            Point::new(min_x, min_y),
            Point::new(min_x, min_y + 10.0),
            Point::new(min_x + 10.0, min_y + 10.0),
            Point::new(min_x + 10.0, min_y),
            Point::new(min_x, min_y),
        ]))
    }

    fn record(pnu: &str, jibun: &str) -> dbase::Record {
        let mut record = dbase::Record::default();
        record.insert(
            "PNU".to_owned(),
            FieldValue::Character(Some(pnu.to_owned())),
        );
        record.insert(
            "JIBUN".to_owned(),
            FieldValue::Character(Some(jibun.to_owned())),
        );
        record
    }
}
