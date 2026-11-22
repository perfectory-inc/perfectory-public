//! Running one changed release: stage its Bronze and export it; the publisher's `DuckDB` loads it
//! (`duckdb_release`, root ADR-0599).
//!
//! The export is the release's own publisher, started as a child of this command: it runs inside
//! the unit's memory bound (`MemoryMax`), reads the host's database and the legal-dong projection
//! directly, and needs no image of its own.
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{ensure, Context};
use foundation_outbox::R2ObjectStorage;
use sha2::{Digest, Sha256};

use super::{
    lane::{ExportKind, LaneContract},
    release::Release,
};
use crate::silver_handoff_io::{open_source, InputSource};

/// What the unit and its script hand this command about the release it runs from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct Runtime {
    /// The admitted release.
    pub release_root: PathBuf,
    /// This lane's state directory on the data disk.
    pub lane_root: PathBuf,
    /// The publisher binary that runs the export.
    pub publisher: PathBuf,
}

impl Runtime {
    pub(in crate::remote_lakehouse_job) fn work(&self) -> PathBuf {
        self.lane_root.join("work")
    }

    /// Empties the lane's work directory: one release at a time, and a failed attempt's staged
    /// ZIPs (gigabytes) never pile up beside the next one.
    pub(in crate::remote_lakehouse_job) fn reset_work(&self) -> anyhow::Result<()> {
        let work = self.work();
        if work.exists() {
            fs::remove_dir_all(&work)
                .with_context(|| format!("cannot empty {}", work.display()))?;
        }
        fs::create_dir_all(&work).with_context(|| format!("cannot create {}", work.display()))?;
        Ok(())
    }
}

/// Where the export writes its handoff, under the lane's work directory.
pub(in crate::remote_lakehouse_job) const HANDOFF: &str = "handoff";
const EXPORT_SUMMARY: &str = "export-summary.json";
/// The environment the lane's export reads, from the release the ledger picked.
pub(in crate::remote_lakehouse_job) fn export_environment(
    contract: &LaneContract,
    release: &Release,
    identity: &str,
    work: &Path,
) -> anyhow::Result<Vec<(String, String)>> {
    let path = |relative: &str| work.join(relative).to_string_lossy().into_owned();
    let mut env = Vec::new();
    let mut set = |prefix: &str, name: &str, value: String| {
        env.push((format!("{prefix}_{name}"), value));
    };
    match contract.kind {
        ExportKind::Title | ExportKind::Unit | ExportKind::UnitArea => {
            let (prefix, primary, handoff) = match contract.kind {
                ExportKind::Title => (
                    "FOUNDATION_PLATFORM_BUILDING_REGISTER_TITLE_SILVER_HANDOFF",
                    "title",
                    format!("{HANDOFF}/rows.jsonl"),
                ),
                ExportKind::Unit => (
                    "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF",
                    "unit",
                    HANDOFF.to_owned(),
                ),
                _ => (
                    "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF",
                    "unit_area",
                    HANDOFF.to_owned(),
                ),
            };
            let object = release.object(primary)?;
            set(prefix, "BRONZE_ROOT", work.to_string_lossy().into_owned());
            set(prefix, "SOURCE_SLUG", object.slug.clone());
            set(prefix, "SOURCE_OBJECT", object.file_name().to_owned());
            set(prefix, "OUTPUT_PATH", path(&handoff));
            set(prefix, "SUMMARY_PATH", path(EXPORT_SUMMARY));
            set(prefix, "SOURCE_SNAPSHOT_ID", identity.to_owned());
            set(prefix, "VALID_FROM_UTC", release.valid_from_utc());
            if contract.kind == ExportKind::UnitArea {
                // Parts under an R2 manifest, loaded part by part (root ADR-0391 addendum).
                set(prefix, "OUTPUT_FORMAT", "r2-parts".to_owned());
                set(
                    prefix,
                    "OUTPUT_OBJECT_PREFIX",
                    contract
                        .handoff_prefix
                        .clone()
                        .context("the unit-area lane has a handoff prefix")?,
                );
                set(
                    prefix,
                    "CHUNK_ROWS",
                    contract
                        .export_chunk_rows
                        .context("the unit-area lane names its part size")?
                        .to_string(),
                );
            } else if let Some(rows) = contract.export_chunk_rows {
                set(prefix, "OUTPUT_FORMAT", "parquet".to_owned());
                set(prefix, "CHUNK_ROWS", rows.to_string());
            }
            if contract.kind == ExportKind::Unit {
                for (role, name) in [("title", "TITLE"), ("basis", "BASIS")] {
                    let parent = release.object(role)?;
                    set(prefix, &format!("{name}_SOURCE_SLUG"), parent.slug.clone());
                    set(
                        prefix,
                        &format!("{name}_SOURCE_OBJECT"),
                        parent.file_name().to_owned(),
                    );
                }
                // Staff-approved unit overrides are part of the derivation (ADR-0125).
                set(prefix, "APPLY_APPROVED_OVERRIDES", "1".to_owned());
            }
        }
        ExportKind::HubParts { env_prefix } => {
            let object = release.object("source")?;
            set(env_prefix, "INPUT_OBJECT_KEY", object.object_key.clone());
            set(
                env_prefix,
                "INPUT_OBJECT_BYTES",
                object.size_bytes.to_string(),
            );
            set(
                env_prefix,
                "OUTPUT_OBJECT_PREFIX",
                contract
                    .handoff_prefix
                    .clone()
                    .context("a part lane has a handoff prefix")?,
            );
            set(env_prefix, "SOURCE_SNAPSHOT_ID", identity.to_owned());
            set(env_prefix, "SUMMARY_PATH", path(EXPORT_SUMMARY));
        }
        ExportKind::Land { .. } => {
            anyhow::bail!("a land release exports object by object (land::export_environment)")
        }
    }
    Ok(env)
}

/// Copies the release's ZIPs out of R2 into `work/<object key>`, the layout the local exports
/// read, refusing bytes that differ from the ledger's size or SHA-256.
pub(in crate::remote_lakehouse_job) async fn stage(
    release: &Release,
    work: &Path,
) -> anyhow::Result<()> {
    let storage = R2ObjectStorage::from_env().context("staging reads Bronze from R2")?;
    for object in release.objects.values() {
        let destination = work.join(&object.object_key);
        let parent = destination
            .parent()
            .context("a Bronze key has a directory")?
            .to_path_buf();
        fs::create_dir_all(&parent)?;
        let source = open_source(
            &InputSource::R2Object(object.object_key.clone()),
            Some(&storage),
        )
        .await?;
        let expected = (object.size_bytes, object.checksum_sha256.clone());
        let key = object.object_key.clone();
        tokio::task::spawn_blocking(move || {
            copy_verified(source, &parent, &destination, &expected)
        })
        .await
        .context("staging stopped")?
        .with_context(|| format!("cannot stage {key}"))?;
        println!(
            "silver-refresh staged {} bytes={}",
            object.object_key, object.size_bytes
        );
    }
    Ok(())
}

fn copy_verified(
    mut source: impl Read,
    parent: &Path,
    destination: &Path,
    (size, sha256): &(u64, String),
) -> anyhow::Result<()> {
    let mut pending = tempfile::NamedTempFile::new_in(parent)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1 << 20];
    let mut copied = 0_u64;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk = buffer.get(..read).context("read past the buffer")?;
        digest.update(chunk);
        pending.write_all(chunk)?;
        copied += u64::try_from(read)?;
    }
    pending.as_file().sync_all()?;
    ensure!(
        copied == *size && format!("{:x}", digest.finalize()) == *sha256,
        "R2 bytes differ from the ledger's size or SHA-256"
    );
    pending.persist(destination)?;
    Ok(())
}

/// Runs the lane's export: the release's publisher, inheriting this unit's environment.
pub(in crate::remote_lakehouse_job) fn export(
    runtime: &Runtime,
    contract: &LaneContract,
    environment: &[(String, String)],
) -> anyhow::Result<()> {
    let status = Command::new(&runtime.publisher)
        .arg(&contract.export)
        .envs(environment.iter().map(|(key, value)| (key, value)))
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("cannot start {}", contract.export))?;
    ensure!(status.success(), "{} failed: {status}", contract.export);
    Ok(())
}

/// The branch the batches of load `identity` stage on: one per load, the same on every retry, so a
/// resumed load continues on it (root ADR-0183). The only place a load branch is named.
pub(in crate::remote_lakehouse_job) fn load_branch(identity: &str) -> String {
    format!("load_{}", branch_digest(identity))
}

/// The branch a compare stages load `identity`'s changes on, bucket by bucket, until the lane
/// publishes it as `main` in one fast-forward (root ADR-0318). The only place it is named.
pub(in crate::remote_lakehouse_job) fn changes_branch(identity: &str) -> String {
    format!("changes_{}", branch_digest(identity))
}

fn branch_digest(identity: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(identity.as_bytes()));
    digest[..16].to_owned()
}

/// Keeps more files of the work directory under the same `evidence/<identity>/` (a land
/// release's export summaries, one per object).
pub(in crate::remote_lakehouse_job) fn keep_files(
    runtime: &Runtime,
    identity: &str,
    names: &[String],
) -> anyhow::Result<()> {
    let evidence = runtime.lane_root.join("evidence").join(identity);
    fs::create_dir_all(&evidence)?;
    for name in names {
        let source = runtime.work().join(name);
        let target = evidence.join(name);
        if source.is_file() && !target.exists() {
            fs::copy(&source, &target)
                .with_context(|| format!("cannot keep {}", source.display()))?;
        }
    }
    Ok(())
}
