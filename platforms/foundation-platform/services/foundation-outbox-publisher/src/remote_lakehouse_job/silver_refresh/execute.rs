//! Running one changed release: stage its Bronze, export, load with the scalar Spark job.
//!
//! The export is the release's own publisher, started as a child of this command: it runs inside
//! the unit's memory bound (`MemoryMax`), reads the host's database and the legal-dong projection
//! directly, and needs no image of its own. Spark runs in the compose `spark` service under its
//! 20g cap, in a Compose project named after the systemd invocation so `ExecStopPost` can remove
//! what a killed run leaves (root ADR-0128, `invocation_cleanup`).
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
    lane::{ExportKind, LaneContract, WriteMode},
    release::Release,
};
use foundation_outbox_publisher::silver_handoff_io::{open_source, InputSource};

/// What the unit and its script hand this command about the release it runs from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct Runtime {
    /// The admitted release (`compose.lakehouse.yml` and the Spark jobs live here).
    pub release_root: PathBuf,
    /// This lane's state directory on the data disk.
    pub lane_root: PathBuf,
    /// The release's frozen jars, as paths inside the Spark container's Ivy mount.
    pub spark_jars: String,
    /// The host directory those jars are in, mounted read-only.
    pub jars_dir: PathBuf,
    /// The Compose project this invocation's Spark runs in.
    pub project: String,
    /// The service account's group: Spark (uid 185) writes the work directory through it.
    pub gid: String,
    /// The publisher binary that runs the export.
    pub publisher: PathBuf,
}

impl Runtime {
    pub(in crate::remote_lakehouse_job) fn work(&self) -> PathBuf {
        self.lane_root.join("work")
    }

    fn scratch(&self) -> PathBuf {
        self.lane_root.join("spark-scratch")
    }

    /// Empties the lane's work directory: one release at a time, and a failed attempt's staged
    /// ZIPs (gigabytes) never pile up beside the next one.
    pub(in crate::remote_lakehouse_job) fn reset_work(&self) -> anyhow::Result<()> {
        let work = self.work();
        if work.exists() {
            fs::remove_dir_all(&work)
                .with_context(|| format!("cannot empty {}", work.display()))?;
        }
        for directory in [work, self.scratch()] {
            fs::create_dir_all(&directory)
                .with_context(|| format!("cannot create {}", directory.display()))?;
            open_to_spark(&directory)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn open_to_spark(directory: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    // Spark runs as uid 185 in the service account's group; the group must write here.
    fs::set_permissions(directory, fs::Permissions::from_mode(0o2770))
        .with_context(|| format!("cannot open {} to the Spark container", directory.display()))
}

#[cfg(not(unix))]
fn open_to_spark(_directory: &Path) -> anyhow::Result<()> {
    Ok(())
}

/// Where the export writes its handoff, as the host and as the Spark container see it.
pub(in crate::remote_lakehouse_job) const HANDOFF: &str = "handoff";
const EXPORT_SUMMARY: &str = "export-summary.json";
const SPARK_SUMMARY: &str = "spark-summary.json";
const CONTAINER_ROOT: &str = "/workspace/target/lakehouse";

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
            if let Some(rows) = contract.export_chunk_rows {
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

/// One Spark load: the input it reads and the rows it must hold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct SparkLoad {
    /// A path in the container, or a comma-joined list of `s3a://` handoff parts.
    pub input: String,
    pub input_format: &'static str,
    pub expected_count: Option<u64>,
    /// Whether the input is in R2 (the read needs the lakehouse reader pair).
    pub reads_r2: bool,
    /// How this load writes: a land release's first batch overwrites and the rest append.
    pub write_mode: WriteMode,
}

impl SparkLoad {
    /// The local handoff the export wrote into the work directory.
    pub(in crate::remote_lakehouse_job) fn local(contract: &LaneContract) -> Self {
        let (input, input_format) = if contract.kind == ExportKind::Title {
            (format!("{CONTAINER_ROOT}/{HANDOFF}/rows.jsonl"), "jsonl")
        } else {
            (format!("{CONTAINER_ROOT}/{HANDOFF}"), "parquet")
        };
        Self {
            input,
            input_format,
            expected_count: None,
            reads_r2: false,
            write_mode: contract.spark.iceberg_write_mode,
        }
    }
}

/// The `docker compose` arguments of one scalar Spark load.
pub(in crate::remote_lakehouse_job) fn spark_arguments(
    runtime: &Runtime,
    contract: &LaneContract,
    load: &SparkLoad,
) -> Vec<String> {
    let release = runtime.release_root.to_string_lossy().into_owned();
    let compose_file = format!("{release}/compose.lakehouse.yml");
    let scratch = format!("{}:/scratch", runtime.scratch().to_string_lossy());
    // The init service (a write check of the work directory) runs first, as for every Spark job.
    let mut args: Vec<String> = [
        "compose",
        "--project-directory",
        release.as_str(),
        "-f",
        compose_file.as_str(),
        "-p",
        runtime.project.as_str(),
        "--profile",
        "lakehouse-batch",
        "run",
        "--rm",
        "-v",
        scratch.as_str(),
    ]
    .map(str::to_owned)
    .to_vec();
    let mut names = vec![
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
    ];
    if load.reads_r2 {
        names.extend([
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
        ]);
    }
    for name in names {
        // By name only: the value stays in this process's environment, out of the argument list.
        args.extend(["-e".to_owned(), name.to_owned()]);
    }
    let spark = &contract.spark;
    let summary = format!("{CONTAINER_ROOT}/{SPARK_SUMMARY}");
    args.extend(
        [
            "spark",
            "spark-submit",
            "--master",
            spark.master.as_str(),
            "--driver-memory",
            spark.driver_memory.as_str(),
            "--conf",
            "spark.local.dir=/scratch",
            "--jars",
            runtime.spark_jars.as_str(),
            "/workspace/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py",
            "--input",
            load.input.as_str(),
            "--input-format",
            load.input_format,
            "--contract",
            contract.table.as_str(),
            "--write-mode",
            "iceberg",
            "--iceberg-write-mode",
            load.write_mode.as_str(),
            "--input-file-batch-size",
            "0",
            "--summary-output",
            summary.as_str(),
        ]
        .map(str::to_owned),
    );
    if load.write_mode == WriteMode::Overwrite {
        args.push("--allow-non-smoke-overwrite".to_owned());
    }
    if let Some(count) = load.expected_count {
        args.extend(["--expected-count".to_owned(), count.to_string()]);
    }
    args
}

/// Runs one Spark load and returns its validated summary.
pub(in crate::remote_lakehouse_job) fn spark(
    runtime: &Runtime,
    contract: &LaneContract,
    load: &SparkLoad,
) -> anyhow::Result<lakehouse_domain::SparkRunSummary> {
    let summary_path = runtime.work().join(SPARK_SUMMARY);
    if summary_path.exists() {
        fs::remove_file(&summary_path)?;
    }
    // The init container has a fixed name; a killed run of any job can leave it behind.
    let _ = Command::new("docker")
        .args(["rm", "foundation-platform-lakehouse-target-init"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let status = Command::new("docker")
        .args(spark_arguments(runtime, contract, load))
        .env("FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT", runtime.work())
        .env("FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE", &runtime.jars_dir)
        .env("FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE", "ro")
        .env("FOUNDATION_PLATFORM_LAKEHOUSE_GID", &runtime.gid)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("cannot start the Spark load")?;
    ensure!(
        status.success(),
        "the Spark load of {} failed: {status}",
        contract.table
    );
    let raw = fs::read_to_string(&summary_path)
        .with_context(|| format!("the Spark load left no {}", summary_path.display()))?;
    super::super::validate_spark_summary(&raw)
}

/// Keeps the run's summaries (the export's fill rates and crosswalk provenance, Spark's counts)
/// under `evidence/<identity>/` before the work directory is emptied. They are the per-run record
/// the exports were written to leave (root ADR-0073, ADR-0142); a repeated name keeps the first.
pub(in crate::remote_lakehouse_job) fn keep_evidence(
    runtime: &Runtime,
    identity: &str,
    batch: usize,
) -> anyhow::Result<PathBuf> {
    let evidence = runtime.lane_root.join("evidence").join(identity);
    fs::create_dir_all(&evidence)?;
    for (name, kept) in [
        (EXPORT_SUMMARY, EXPORT_SUMMARY.to_owned()),
        (SPARK_SUMMARY, format!("spark-summary-{batch:04}.json")),
    ] {
        let source = runtime.work().join(name);
        let target = evidence.join(kept);
        if source.is_file() && !target.exists() {
            fs::copy(&source, &target)
                .with_context(|| format!("cannot keep {}", source.display()))?;
        }
    }
    Ok(evidence)
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
