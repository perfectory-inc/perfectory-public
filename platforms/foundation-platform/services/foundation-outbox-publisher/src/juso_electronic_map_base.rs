//! 주소정보 전자지도 base, the one full set the daily changes start from (root ADR-0494).
//!
//! The daily change linkage carries no full set, so each kind's Silver starts from one 월전체
//! (monthly full set) of the 주소정보 다운로드 item 도로명주소 전자지도
//! (`juso-electronic-map.contract.json` `base.item`): one zip per 시도, each one folder
//! `<시도코드>/` holding the layers of the kinds (`base.layers`). Downloading it needs a login, an
//! application and the provider's approval, so it is fetched once by a person and placed, as
//! downloaded, under `<inbox>/<YYYYMM>/`. The collector's run reads each placed zip's directory,
//! refuses one without every layer, and lands it once, byte for byte, in the one Bronze source of
//! the base (`base.source`), streamed from the disk (a 시도 is up to 700 MB; the unit holds 3 GB), its
//! key named by the file's SHA-256 so the same file landed twice is the same object. The provider's
//! bytes are kept once, not once per kind: the members each kind reads are written beside the run
//! (`juso_base.members`), and a kind's Silver opens only those. A landed file is recorded under the
//! state directory's `base/` with its size and modification time, so a later run opens only files
//! it has not landed; the inbox is never written or emptied by the collector. With no file placed
//! the base is inert: one directory listing.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use chrono::{DateTime, Months, NaiveDate, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_public_data_bulk_file_storage_location, public_data_bulk_file_request_params,
    BronzeCommitter, PlannedStreamingBronzeObject, PublicDataBulkFileIdentity,
    PublicDataBulkFileStorageLocationInput, StreamingBronzeRecord,
};
use collection_domain::{
    build_bronze_content_object_key, source_slug, CollectionError, IngestionRun,
    IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_outbox::ObjectStorageStreamingService;
use foundation_publisher_collection::bulk_streaming_bronze::BronzeStreamingObjectStorageWriter;
use foundation_shared_kernel::{
    ids::{IngestionRunId, SourceCatalogId},
    ObjectKey,
};
use futures_util::{stream, stream::BoxStream, StreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

use crate::juso_electronic_map_collect::{Kind, SourceContract};
use crate::public_data_control_support::write_json_file;

const PROVIDER: &str = "juso.go.kr";
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const READ_CHUNK_BYTES: usize = 1024 * 1024;
/// A transfer still in progress (a browser's or a copy's partial file) is not a placed file.
const IN_PROGRESS_SUFFIXES: [&str; 4] = [".part", ".crdownload", ".tmp", ".partial"];

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct BaseItem {
    pub(crate) rtl_dta_dtl_sn: u32,
    pub(crate) name_ko: String,
    pub(crate) region: String,
    pub(crate) format: String,
}

/// The contract's `base`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct BaseContract {
    /// The dataset slug of the one Bronze source every placed zip lands in.
    pub(crate) source: String,
    pub(crate) inbox: PathBuf,
    pub(crate) month_format: String,
    pub(crate) replay_overlap_months: u32,
    pub(crate) page: String,
    pub(crate) item: BaseItem,
    /// The parts of a layer every zip must hold (`.shp`, `.shx`, `.dbf`).
    pub(crate) layer_parts: Vec<String>,
    /// The layer of each kind, by dataset slug. A kind without one has no base.
    pub(crate) layers: BTreeMap<String, String>,
    /// The layers the zip holds that are no kind's base; they stay in Bronze.
    pub(crate) unmatched_layers: Vec<String>,
}

/// What one placed zip holds: its one `<시도코드>/` folder and each kind's members in it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ZipLayout {
    pub(crate) folder: String,
    pub(crate) member_count: usize,
    /// Kind (dataset slug) to its layer's members, in `layer_parts` order.
    pub(crate) members: BTreeMap<String, Vec<String>>,
}

/// One file placed in the inbox.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PlacedFile {
    /// The base month, `YYYYMM`.
    pub(crate) month: String,
    pub(crate) name: String,
    #[serde(skip)]
    pub(crate) path: PathBuf,
    pub(crate) size_bytes: u64,
    /// Modification time, seconds since the epoch: a file replaced under the same name is new.
    pub(crate) modified: u64,
}

/// What a run found in the inbox.
#[derive(Debug, Default)]
pub(crate) struct Inbox {
    pub(crate) files: Vec<PlacedFile>,
    /// What the inbox holds that is no placed file of the contract; the run fails naming it.
    pub(crate) refused: Vec<String>,
}

impl BaseContract {
    pub(crate) fn of(contract: &SourceContract) -> anyhow::Result<Self> {
        let base: Self = serde_json::from_value(contract.base.clone())
            .context("the juso contract's base is not valid")?;
        let kinds: Vec<String> = all_kinds(contract)?
            .into_iter()
            .map(|(slug, _)| slug)
            .collect();
        ensure!(
            !base.layers.is_empty(),
            "the juso contract's base names no layer"
        );
        for kind in base.layers.keys() {
            ensure!(
                kinds.contains(kind),
                "the juso base names a layer for {kind}, which is no kind of the contract"
            );
        }
        ensure!(
            !base.layer_parts.is_empty(),
            "the juso contract's base names no layer part"
        );
        Ok(base)
    }

    /// The first day of a `YYYYMM` month, or `None` when the name is not one.
    pub(crate) fn month_start(&self, month: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(&format!("{month}01"), &format!("{}%d", self.month_format))
            .ok()
            .filter(|day| day.format(&self.month_format).to_string() == month)
    }

    /// The first daily change a kind's Silver replays on top of the base month: the first day of the
    /// month `replay_overlap_months` before it, whatever day the provider cut the full set on.
    pub(crate) fn replay_from(&self, month: &str) -> anyhow::Result<NaiveDate> {
        let start = self
            .month_start(month)
            .with_context(|| format!("{month} is not a base month"))?;
        start
            .checked_sub_months(Months::new(self.replay_overlap_months))
            .with_context(|| format!("no month {} before {month}", self.replay_overlap_months))
    }
}

fn in_progress(name: &str) -> bool {
    name.starts_with('.')
        || IN_PROGRESS_SUFFIXES
            .iter()
            .any(|suffix| name.to_ascii_lowercase().ends_with(suffix))
}

fn sorted_entries(dir: &Path) -> anyhow::Result<Vec<std::fs::DirEntry>> {
    let mut entries = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read {}", dir.display()))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("failed to read {}", dir.display()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    Ok(entries)
}

fn modified_seconds(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |at| at.as_secs())
}

/// Lists `<inbox>/<YYYYMM>/<file>`. A missing inbox is an empty one.
pub(crate) fn scan(base: &BaseContract, inbox: &Path) -> anyhow::Result<Inbox> {
    let mut found = Inbox::default();
    if !inbox.exists() {
        return Ok(found);
    }
    for month_entry in sorted_entries(inbox)? {
        let month = month_entry.file_name().to_string_lossy().into_owned();
        if in_progress(&month) {
            continue;
        }
        if !month_entry.file_type()?.is_dir() || base.month_start(&month).is_none() {
            found
                .refused
                .push(format!("{month}: not a base month folder (YYYYMM)"));
            continue;
        }
        for file_entry in sorted_entries(&month_entry.path())? {
            let name = file_entry.file_name().to_string_lossy().into_owned();
            if in_progress(&name) {
                continue;
            }
            let metadata = file_entry.metadata()?;
            if !metadata.is_file() {
                found.refused.push(format!(
                    "{month}/{name}: not a file (the downloaded file goes in the month folder as it is)"
                ));
                continue;
            }
            found.files.push(PlacedFile {
                month: month.clone(),
                name,
                path: file_entry.path(),
                size_bytes: metadata.len(),
                modified: modified_seconds(&metadata),
            });
        }
    }
    Ok(found)
}

/// Every kind of the contract, by dataset slug: the base holds all 11.
pub(crate) fn all_kinds(contract: &SourceContract) -> anyhow::Result<Vec<(String, Kind)>> {
    let mut kinds = Vec::new();
    for code in contract.request_codes.keys() {
        kinds.extend(contract.kinds_of(code)?);
    }
    kinds.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(kinds)
}

fn marker_path(state_dir: &Path, file: &PlacedFile) -> PathBuf {
    state_dir
        .join("base")
        .join(&file.month)
        .join(format!("{}.json", file.name))
}

/// Whether a run already landed this very file (same name, size and modification time).
pub(crate) fn already_landed(state_dir: &Path, file: &PlacedFile) -> anyhow::Result<bool> {
    let path = marker_path(state_dir, file);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()))
        }
    };
    let marker: JsonValue =
        serde_json::from_str(&text).with_context(|| format!("{} is not valid", path.display()))?;
    Ok(
        marker["size_bytes"] == json!(file.size_bytes)
            && marker["modified"] == json!(file.modified),
    )
}

/// The files of the inbox no run has landed yet.
pub(crate) fn pending(
    base: &BaseContract,
    inbox: &Path,
    state_dir: &Path,
) -> anyhow::Result<Inbox> {
    let mut found = scan(base, inbox)?;
    let mut files = Vec::new();
    for file in found.files {
        if !already_landed(state_dir, &file)? {
            files.push(file);
        }
    }
    found.files = files;
    Ok(found)
}

/// The Bronze leaf of a placed zip: its month and its `<시도코드>/` folder. The file name is Korean
/// (서울특별시.zip) and has no letter of the key's alphabet; the folder names the region the zip holds.
pub(crate) fn provider_file_id(file: &PlacedFile, layout: &ZipLayout) -> String {
    format!("base-{}-{}", file.month, layout.folder)
}

pub(crate) fn identity(
    base: &BaseContract,
    file: &PlacedFile,
    layout: &ZipLayout,
) -> PublicDataBulkFileIdentity {
    let file_id = provider_file_id(file, layout);
    PublicDataBulkFileIdentity {
        operation: format!("juso_{}", base.source),
        provider_file_period: Some(format!("{}-{}", &file.month[..4], &file.month[4..])),
        provider_snapshot_date: None,
        provider_file_name: format!("{file_id}.zip"),
        provider_file_id: file_id,
        provider_updated_at: None,
    }
}

/// The object key of a placed zip in the base's Bronze: the planned key, named by the checksum.
pub(crate) fn object_key(
    base: &BaseContract,
    file: &PlacedFile,
    layout: &ZipLayout,
    checksum_sha256: &str,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<(
    ObjectKey,
    collection_application::PublicDataBulkFileStorageLocationPlan,
)> {
    let slug = source_slug(PROVIDER, &base.source)?;
    let location =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: &slug,
            ingest_date: at.date_naive(),
            ingestion_run_id: run_id,
            identity: identity(base, file, layout),
        })
        .with_context(|| format!("failed to plan the {} object", base.source))?;
    let key = build_bronze_content_object_key(&location.object_key, checksum_sha256)
        .context("failed to name the content-addressed base key")?;
    Ok((key, location))
}

/// The file's SHA-256 and whether it starts as a zip, read in bounded chunks.
pub(crate) async fn read_file(path: &Path) -> anyhow::Result<(String, u64, bool)> {
    let mut reader = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; READ_CHUNK_BYTES];
    let mut size = 0_u64;
    let mut head = Vec::new();
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        if head.len() < ZIP_MAGIC.len() {
            head.extend_from_slice(&buffer[..read.min(ZIP_MAGIC.len() - head.len())]);
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    let checksum = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((checksum, size, head == ZIP_MAGIC))
}

async fn file_body(
    path: PathBuf,
) -> anyhow::Result<BoxStream<'static, Result<Bytes, CollectionError>>> {
    let file = tokio::fs::File::open(&path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    Ok(stream::try_unfold(file, |mut file| async move {
        let mut buffer = vec![0_u8; READ_CHUNK_BYTES];
        let read = file.read(&mut buffer).await.map_err(|error| {
            CollectionError::Infrastructure(format!("failed to read a juso base file: {error}"))
        })?;
        if read == 0 {
            return Ok(None);
        }
        buffer.truncate(read);
        Ok(Some((Bytes::from(buffer), file)))
    })
    // Polled again after the end by an upload, `try_unfold` would panic; fused, it stays ended.
    .fuse()
    .boxed())
}

/// Reads a placed zip's directory: its one `<시도코드>/` folder and every layer's parts in it, or why
/// the zip is refused (a refusal of the run, not an error: the other zips still land).
pub(crate) fn read_layout(
    path: &Path,
    base: &BaseContract,
) -> anyhow::Result<Result<ZipLayout, String>> {
    let file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let archive = match zip::ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(error) => return Ok(Err(format!("not a readable zip: {error}"))),
    };
    let names = match archive.file_names().collect::<Result<Vec<_>, _>>() {
        Ok(names) => names,
        Err(error) => return Ok(Err(format!("a member name is not readable: {error}"))),
    };
    let names: Vec<&str> = names.iter().map(AsRef::as_ref).collect();
    let mut folders: Vec<&str> = names
        .iter()
        .filter_map(|name| name.split_once('/').map(|(folder, _)| folder))
        .filter(|folder| folder.len() == 5 && folder.bytes().all(|b| b.is_ascii_digit()))
        .collect();
    folders.sort_unstable();
    folders.dedup();
    let [folder] = folders.as_slice() else {
        return Ok(Err(format!(
            "holds {} <시도코드>/ folders, not one: {folders:?}",
            folders.len()
        )));
    };
    let mut members = BTreeMap::new();
    let mut missing = Vec::new();
    for (kind, layer) in &base.layers {
        let parts: Vec<String> = base
            .layer_parts
            .iter()
            .map(|part| format!("{folder}/{layer}{part}"))
            .collect();
        missing.extend(
            parts
                .iter()
                .filter(|part| !names.contains(&part.as_str()))
                .cloned(),
        );
        members.insert(kind.clone(), parts);
    }
    if !missing.is_empty() {
        return Ok(Err(format!("lacks {}", missing.join(", "))));
    }
    Ok(Ok(ZipLayout {
        folder: (*folder).to_owned(),
        member_count: names.len(),
        members,
    }))
}

/// The base's one Bronze source.
fn base_source_entry(
    contract: &SourceContract,
    base: &BaseContract,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &base.source)?,
        name: base.item.name_ko.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: base.source.clone(),
        base_url: Some(base.page.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Zip,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!(
            "once: the {} 월전체 (item {}) a person downloads and places; the daily changes come from {}",
            base.item.region, base.item.rtl_dta_dtl_sn, contract.base_url
        )),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

/// Streams one placed zip into the base's Bronze: source row, run row, write-once object, ledger
/// row. Returns the object key.
#[allow(clippy::too_many_arguments)] // the contract, the file, its reading and the two ports
async fn commit_base<Uow, Storage>(
    contract: &SourceContract,
    base: &BaseContract,
    file: &PlacedFile,
    layout: &ZipLayout,
    checksum_sha256: &str,
    at: DateTime<Utc>,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<String>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(&base_source_entry(contract, base, at)?)
        .await
        .with_context(|| format!("failed to upsert the {} source", base.source))?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let (key, location) = object_key(base, file, layout, checksum_sha256, run_id, at)?;
    let writer = match storage
        .read_object_sha256_and_size_by_rehash(key.as_str())
        .await
        .with_context(|| format!("failed to read back {}", key.as_str()))?
    {
        None => BronzeStreamingObjectStorageWriter::new(
            storage,
            "application/zip".to_owned(),
            file_body(file.path.clone()).await?,
        ),
        Some(held)
            if held.checksum_sha256 == checksum_sha256 && held.size_bytes == file.size_bytes =>
        {
            BronzeStreamingObjectStorageWriter::already_present(
                storage,
                "application/zip".to_owned(),
            )
        }
        Some(held) => bail!(
            "{} holds sha256 {} ({} bytes), not the placed file it names",
            key.as_str(),
            held.checksum_sha256,
            held.size_bytes
        ),
    };
    let juso_base = json!({
        "month": file.month,
        "item": base.item.rtl_dta_dtl_sn,
        "file_name": file.name,
        "folder": layout.folder,
        "members": layout.members,
        "replay_from": base.replay_from(&file.month)?.to_string(),
    });
    let run = uow
        .create_ingestion_run(&IngestionRun {
            id: run_id,
            source_catalog_id: source.id,
            trigger: IngestionTrigger::Scheduled,
            status: IngestionRunStatus::Running,
            request_params: json!({
                "operation": format!("juso_{}", base.source),
                "juso_base": juso_base,
                "objects": 1,
            }),
            started_at: at,
            finished_at: None,
            logical_records_seen: 0,
            objects_written: 0,
            error_message: None,
            created_at: at,
            updated_at: at,
            version: 1,
        })
        .await
        .with_context(|| format!("failed to create the {} run", base.source))?;
    let identity = identity(base, file, layout);
    let mut request_params = public_data_bulk_file_request_params(&identity);
    request_params["juso_base"] = juso_base;
    let slug = source_slug(PROVIDER, &base.source)?;
    let planned = PlannedStreamingBronzeObject {
        cache_control: BRONZE_CACHE_CONTROL.to_owned(),
        expected_size_bytes: file.size_bytes,
        record: StreamingBronzeRecord {
            object_key: key.clone(),
            content_type: "application/zip".to_owned(),
            source_catalog_id: source.id,
            ingestion_run_id: run.id,
            source_partition_key: location.source_partition_key.clone(),
            source_identity_key: location.source_identity_key.clone(),
            dedupe_key_prefix: format!("{slug}:{}", location.source_identity_key),
            request_params,
            snapshot_period: location.snapshot_period,
            snapshot_date: location.snapshot_date,
            snapshot_granularity: location.snapshot_granularity,
            snapshot_basis: location.snapshot_basis,
            provider_file_id: Some(identity.provider_file_id),
            provider_file_name: Some(file.name.clone()),
            provider_updated_at: None,
            collected_at: at,
        },
    };
    let committed = BRONZE_COMMITTER
        .commit_streaming_bulk(&writer, uow, planned)
        .await
        .with_context(|| format!("failed to commit {}", key.as_str()));
    let (status, error_message) = match &committed {
        Ok(outcome) if outcome.checksum_sha256 == checksum_sha256 => {
            (IngestionRunStatus::Succeeded, None)
        }
        Ok(outcome) => (
            IngestionRunStatus::Failed,
            Some(format!(
                "Bronze holds sha256 {} at {}",
                outcome.checksum_sha256,
                key.as_str()
            )),
        ),
        Err(error) => (IngestionRunStatus::Failed, Some(format!("{error:#}"))),
    };
    let succeeded = status == IngestionRunStatus::Succeeded;
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status,
        finished_at: Utc::now(),
        logical_records_seen: 0,
        objects_written: u64::from(succeeded),
        error_message: error_message.clone(),
    })
    .await
    .with_context(|| format!("failed to complete the {} run", base.source))?;
    committed?;
    ensure!(succeeded, "{}", error_message.unwrap_or_default());
    Ok(key.as_str().to_owned())
}

/// The live ports a landing writes through.
pub(crate) struct Ports<'a, Uow: ?Sized, Storage: ?Sized> {
    pub(crate) uow: &'a Uow,
    pub(crate) storage: &'a Storage,
}

/// Lands every pending placed zip once in the base's Bronze. Returns the manifest's `base` and
/// what the run refused (an entry that is no placed file, or a file that is not a zip of the
/// layers): landed files stay landed, and the run fails naming the rest.
pub(crate) async fn land<Uow, Storage>(
    contract: &SourceContract,
    base: &BaseContract,
    inbox: &Path,
    state_dir: &Path,
    ports: Option<Ports<'_, Uow, Storage>>,
    at: DateTime<Utc>,
) -> anyhow::Result<(JsonValue, Vec<String>)>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let found = pending(base, inbox, state_dir)?;
    let mut refused = found.refused.clone();
    let mut landed = Vec::new();
    for file in &found.files {
        let (checksum, size, is_zip) = read_file(&file.path).await?;
        let layout = if !is_zip {
            Err("not a zip".to_owned())
        } else if size != file.size_bytes {
            Err("changed while read".to_owned())
        } else {
            let (path, reading) = (file.path.clone(), base.clone());
            tokio::task::spawn_blocking(move || read_layout(&path, &reading))
                .await
                .context("the zip directory reading stopped")??
        };
        let layout = match layout {
            Ok(layout) => layout,
            Err(reason) => {
                refused.push(format!("{}/{}: {reason}", file.month, file.name));
                continue;
            }
        };
        let object_key = if let Some(ports) = &ports {
            commit_base(
                contract,
                base,
                file,
                &layout,
                &checksum,
                at,
                ports.uow,
                ports.storage,
            )
            .await?
        } else {
            object_key(
                base,
                file,
                &layout,
                &checksum,
                IngestionRunId::new(Uuid::new_v4()),
                at,
            )?
            .0
            .as_str()
            .to_owned()
        };
        let entry = json!({
            "month": file.month,
            "name": file.name,
            "size_bytes": file.size_bytes,
            "modified": file.modified,
            "checksum_sha256": checksum,
            "replay_from": base.replay_from(&file.month)?.to_string(),
            "landed_at_utc": at.to_rfc3339(),
            "object_key": object_key,
            "written": ports.is_some(),
            "folder": layout.folder,
            "member_count": layout.member_count,
            "members": layout.members,
        });
        if ports.is_some() {
            let marker = marker_path(state_dir, file);
            if let Some(parent) = marker.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            write_json_file(&marker, &entry)?;
        }
        landed.push(entry);
    }
    Ok((
        json!({
            "inbox": inbox.display().to_string(),
            "source": source_slug(PROVIDER, &base.source)?,
            "item": {"rtl_dta_dtl_sn": base.item.rtl_dta_dtl_sn, "name_ko": base.item.name_ko,
                     "region": base.item.region, "format": base.item.format, "page": base.page},
            "landed": landed,
            "refused": refused,
        }),
        refused,
    ))
}

#[cfg(test)]
mod tests;
