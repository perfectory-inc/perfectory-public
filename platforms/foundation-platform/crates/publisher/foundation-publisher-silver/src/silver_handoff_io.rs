//! Shared plumbing for Silver handoff exporters.
//!
//! Every exporter that turns a collected Bronze object into handoff JSONL shares the same
//! transport concerns: either end may be a local path or an R2 object key, the source must be
//! readable and seekable wherever it lives, and the destination must become visible only when
//! the whole conversion succeeded. These used to be copied per format module — shapefile, CSV —
//! and a fix that landed in one copy was a fix the other copy silently lacked. One module, one
//! behavior (the copies were the third named SSOT debt of 2026-09-06).

use std::{
    env, fs,
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use foundation_outbox::{
    object_storage::{R2MultipartUploadWriter, R2SeekableObjectReader},
    R2ObjectStorage,
};
use uuid::Uuid;

/// How much of an R2 object one ranged request fetches.
///
/// A ZIP is read by seeking to its central directory at the end and then back to the member
/// it names, so a per-call request count would be dominated by two jumps rather than by the
/// bytes wanted. Eight mebibytes keeps that scan to a handful of requests while never holding
/// more than one chunk.
pub const R2_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// How much handoff output accumulates before a part is uploaded.
///
/// R2 requires at least five mebibytes for every part but the last, and caps an upload at
/// 10,000 parts. Sixteen mebibytes clears the floor and still admits a 156 GB output — an
/// order of magnitude above the 45.7 GB the national parcel run produced.
pub const R2_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;

/// MIME type every handoff object is stored with.
pub const HANDOFF_CONTENT_TYPE: &str = "application/x-ndjson";
/// The handoff is an input to a load, not something served. Nothing should cache it.
pub const HANDOFF_CACHE_CONTROL: &str = "no-store";

/// Suffix that asks for the handoff to be compressed on the way out.
///
/// Named rather than flagged so the file says what it is: Spark decides by extension, and a
/// `.gz` that was not gzipped is a file the reader cannot open.
pub const GZIP_SUFFIX: &str = ".gz";

/// How hard to compress.
///
/// Six is the default and the reason to leave it there: measured 2026-08-31 on a 625 MB
/// national parcel handoff, level 6 leaves 20% and level 9 leaves barely less for noticeably
/// more CPU.
pub const GZIP_LEVEL: u32 = 6;

/// Where a source object is read from.
///
/// A local path and an R2 key are both legitimate: a single file being examined by hand is a
/// path, and a national run is a key. Naming them as one type keeps conversions from caring
/// which they got.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputSource {
    /// A file on the local filesystem.
    LocalPath(PathBuf),
    /// A canonical R2 object key.
    R2Object(String),
}

impl InputSource {
    /// Human-readable form for logs and summaries.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::LocalPath(path) => path.display().to_string(),
            Self::R2Object(key) => format!("r2://{key}"),
        }
    }

    /// Whether this end lives in R2.
    #[must_use]
    pub const fn is_r2(&self) -> bool {
        matches!(self, Self::R2Object(_))
    }
}

/// Where the handoff output is written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutputSink {
    /// A file on the local filesystem, staged and renamed on success.
    LocalPath(PathBuf),
    /// A canonical R2 object key, uploaded as a create-only multipart object.
    R2Object(String),
}

impl OutputSink {
    /// Whether the name asks for the bytes to be compressed.
    ///
    /// Taken from the name rather than a separate switch, because the reader decides the same
    /// way. Two places that answer this differently produce a file one of them cannot open.
    #[must_use]
    pub fn is_compressed(&self) -> bool {
        match self {
            Self::LocalPath(path) => path
                .to_str()
                .is_some_and(|name| name.ends_with(GZIP_SUFFIX)),
            Self::R2Object(key) => key.ends_with(GZIP_SUFFIX),
        }
    }

    /// Human-readable form for logs and summaries.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::LocalPath(path) => path.display().to_string(),
            Self::R2Object(key) => format!("r2://{key}"),
        }
    }

    /// Whether this end lives in R2.
    #[must_use]
    pub const fn is_r2(&self) -> bool {
        matches!(self, Self::R2Object(_))
    }
}

/// The one configured end an exporter run reads or writes.
pub enum PathOrKey {
    /// A local filesystem path.
    Path(PathBuf),
    /// An R2 object key.
    Key(String),
}

/// Reads exactly one of a path variable and a key variable.
///
/// Both set is refused rather than resolved by precedence: a run configured with two sources
/// is a run whose operator believed something about it that is not true, and silently
/// preferring one writes the wrong lineage into the summary.
///
/// # Errors
/// Returns an error when both or neither variable is set.
pub fn one_of_path_or_key(path_env: &str, key_env: &str) -> anyhow::Result<PathOrKey> {
    match (optional_env(path_env)?, optional_env(key_env)?) {
        (Some(path), None) => Ok(PathOrKey::Path(PathBuf::from(path))),
        (None, Some(key)) => Ok(PathOrKey::Key(key)),
        (Some(_), Some(_)) => bail!("set {path_env} or {key_env}, not both"),
        (None, None) => bail!("{path_env} or {key_env} is required"),
    }
}

/// A source a ZIP/shapefile reader can seek in, wherever it lives.
pub enum SeekableSource {
    /// A buffered local file.
    Local(BufReader<fs::File>),
    /// A ranged-read R2 object.
    R2(Box<R2SeekableObjectReader>),
}

impl Read for SeekableSource {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Local(source) => source.read(output),
            Self::R2(source) => source.read(output),
        }
    }
}

impl Seek for SeekableSource {
    fn seek(&mut self, seek_from: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Local(source) => source.seek(seek_from),
            Self::R2(source) => source.seek(seek_from),
        }
    }
}

/// Opens the input wherever it lives.
///
/// # Errors
/// Returns an error when the file or the R2 object cannot be opened.
pub async fn open_source(
    input: &InputSource,
    storage: Option<&R2ObjectStorage>,
) -> anyhow::Result<SeekableSource> {
    match input {
        InputSource::LocalPath(path) => {
            let file = fs::File::open(path)
                .with_context(|| format!("failed to open source archive {}", path.display()))?;
            Ok(SeekableSource::Local(BufReader::new(file)))
        }
        InputSource::R2Object(key) => {
            let storage = storage.context("an R2 input key needs R2 configuration")?;
            let reader = storage
                .open_seekable_object(key, R2_READ_CHUNK_BYTES)
                .await
                .with_context(|| format!("failed to open R2 source archive {key}"))?;
            Ok(SeekableSource::R2(Box::new(reader)))
        }
    }
}

/// A handoff destination that only becomes visible once the whole conversion succeeded.
///
/// Both variants hold that property by construction and neither one can be half-written: the
/// local one stages beside its final name and renames, and the R2 one uploads parts that no
/// reader can address until the upload is completed.
pub enum HandoffSink {
    /// A staged local file promoted to its final name on success.
    Local {
        /// The staged, not-yet-visible file.
        pending: PendingOutput,
        /// Where the staged file is promoted to.
        final_path: PathBuf,
    },
    /// An R2 multipart upload whose parts are unaddressable until completion.
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
    ///
    /// # Errors
    /// Returns an error when the staged file or the multipart upload cannot be completed.
    pub fn finish(self) -> anyhow::Result<u64> {
        match self {
            Self::Local {
                mut pending,
                final_path,
            } => {
                let output_bytes = pending
                    .file_mut()?
                    .metadata()
                    .context("failed to inspect staged handoff output")?
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

/// Opens the output wherever it goes.
///
/// # Errors
/// Returns an error when staging the local file or starting the upload fails.
pub async fn open_sink(
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

/// Whether this run's output is already in the bucket.
///
/// Only asked of an R2 output. A local one is answered by [`refuse_existing_outputs`], which
/// stops rather than skips: a path the operator typed twice is a mistake worth hearing about,
/// while a key the runner derived is the same key it derived last time.
///
/// # Errors
/// Returns an error when R2 configuration or the existence check fails.
pub async fn already_exported(output: &OutputSink) -> anyhow::Result<bool> {
    let OutputSink::R2Object(key) = output else {
        return Ok(false);
    };
    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 while checking for an existing handoff")?;
    storage
        .object_exists(key)
        .await
        .with_context(|| format!("failed to check whether the handoff {key} already exists"))
}

/// Refuses to overwrite a local output or summary the operator already produced.
///
/// # Errors
/// Returns an error when either target already exists.
pub fn refuse_existing_outputs(
    output: &OutputSink,
    summary_path: Option<&Path>,
) -> anyhow::Result<()> {
    if let OutputSink::LocalPath(path) = output {
        if path.exists() {
            bail!(
                "refusing to overwrite existing handoff output {}",
                path.display()
            );
        }
    }
    if summary_path.is_some_and(Path::exists) {
        bail!("refusing to overwrite existing export summary output");
    }
    Ok(())
}

/// A staged file that becomes its final name only on commit; dropped uncommitted, it vanishes.
pub struct PendingOutput {
    path: PathBuf,
    file: Option<fs::File>,
    committed: bool,
}

impl PendingOutput {
    /// Stages a hidden sibling of `final_path` that vanishes unless committed.
    ///
    /// # Errors
    /// Returns an error when the directory or staged file cannot be created.
    pub fn create(final_path: &Path) -> anyhow::Result<Self> {
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

    /// The staged file handle, while it is still open.
    ///
    /// # Errors
    /// Returns an error after the staged output was committed or closed.
    pub fn file_mut(&mut self) -> anyhow::Result<&mut fs::File> {
        self.file
            .as_mut()
            .context("staged output is already closed")
    }

    /// Syncs the staged bytes and atomically promotes them to `final_path`.
    ///
    /// # Errors
    /// Returns an error when the sync or the rename fails.
    pub fn sync_and_commit(mut self, final_path: &Path) -> anyhow::Result<()> {
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

/// Reads a required, non-blank environment variable.
///
/// # Errors
/// Returns an error when the variable is absent or blank.
pub fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(trimmed.to_owned())
}

/// Reads an optional environment variable, treating blank as absent.
///
/// # Errors
/// Returns an error when the variable holds non-Unicode data.
pub fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

/// Reads a required RFC3339 timestamp environment variable.
///
/// # Errors
/// Returns an error when the variable is absent, blank, or not RFC3339.
pub fn parse_utc_env(name: &str) -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(name)?;
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("{name} must be an RFC3339 timestamp"))?
        .with_timezone(&Utc))
}
