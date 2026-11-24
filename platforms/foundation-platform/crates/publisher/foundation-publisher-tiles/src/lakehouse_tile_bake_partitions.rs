//! Root ADR-0316: a partitioned unit's GDAL input (one CSV per partition) and the per-partition
//! archives a bake reuses when a partition's rows and bake inputs did not change.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use sha2::{Digest as _, Sha256};

use super::id_hash;

/// Where the GDAL input goes: one CSV, or one per partition ([`PartitionedCsv`]).
pub trait CsvOut {
    /// The header line, written before any row.
    fn header(&mut self, line: &str) -> std::io::Result<()>;
    fn row(&mut self, feature_id: &str, line: &str) -> std::io::Result<()>;
}

impl<W: Write> CsvOut for W {
    fn header(&mut self, line: &str) -> std::io::Result<()> {
        writeln!(self, "{line}")
    }

    fn row(&mut self, _feature_id: &str, line: &str) -> std::io::Result<()> {
        writeln!(self, "{line}")
    }
}

/// Root ADR-0316: the GDAL input split by the first `prefix_chars` of the feature id (a 시도 for
/// parcels), one CSV per partition under `dir`, each with the header.
pub struct PartitionedCsv {
    dir: PathBuf,
    prefix_chars: usize,
    header: String,
    /// Per prefix: its CSV, its row count and the wrapping sum of its rows' hashes.
    parts: std::collections::BTreeMap<String, (BufWriter<File>, usize, u128)>,
}

/// One partition's GDAL input and the content its archive is named by.
#[derive(Debug, Clone)]
pub struct CsvPart {
    pub prefix: String,
    pub csv: PathBuf,
    pub rows: usize,
    /// SHA-256 of the header, the row count and the sum of the rows' hashes: the same rows in any
    /// order give the same content (Spark's part order is not stable), any changed row another.
    pub content: String,
}

impl PartitionedCsv {
    pub fn new(dir: PathBuf, prefix_chars: usize) -> Self {
        Self {
            dir,
            prefix_chars,
            header: String::new(),
            parts: std::collections::BTreeMap::new(),
        }
    }

    /// Flushes every partition's CSV and names its content.
    pub fn finish(self) -> anyhow::Result<Vec<CsvPart>> {
        let Self {
            dir, header, parts, ..
        } = self;
        parts
            .into_iter()
            .map(|(prefix, (out, rows, sum))| {
                out.into_inner()
                    .map_err(|error| error.into_error())?
                    .sync_all()?;
                let mut content = Sha256::new();
                content.update(header.as_bytes());
                content.update(rows.to_be_bytes());
                content.update(sum.to_be_bytes());
                Ok(CsvPart {
                    csv: dir.join(format!("{prefix}.csv")),
                    prefix,
                    rows,
                    content: format!("{:x}", content.finalize()),
                })
            })
            .collect()
    }
}

impl CsvOut for PartitionedCsv {
    fn header(&mut self, line: &str) -> std::io::Result<()> {
        line.clone_into(&mut self.header);
        Ok(())
    }

    fn row(&mut self, feature_id: &str, line: &str) -> std::io::Result<()> {
        let prefix = feature_id
            .get(..self.prefix_chars)
            .filter(|prefix| prefix.bytes().all(|b| b.is_ascii_alphanumeric()))
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "feature {feature_id:?} has no {}-character alphanumeric partition prefix",
                    self.prefix_chars
                ))
            })?;
        if !self.parts.contains_key(prefix) {
            let mut out = BufWriter::with_capacity(
                1 << 20,
                File::create(self.dir.join(format!("{prefix}.csv")))?,
            );
            writeln!(out, "{}", self.header)?;
            self.parts.insert(prefix.to_owned(), (out, 0, 0));
        }
        if let Some((out, rows, sum)) = self.parts.get_mut(prefix) {
            writeln!(out, "{line}")?;
            *rows += 1;
            *sum = sum.wrapping_add(id_hash(line));
        }
        Ok(())
    }
}

/// Where a partition's archive lives under `root`: named by its content and every bake input, so a
/// partition whose rows and inputs did not change finds the archive an earlier bake left.
pub fn partition_archive(root: &Path, part: &CsvPart, bake_inputs: &str) -> PathBuf {
    let mut name = Sha256::new();
    name.update(part.content.as_bytes());
    name.update(bake_inputs.as_bytes());
    root.join(&part.prefix)
        .join(format!("{:x}.pmtiles", name.finalize()))
}

/// Bakes only the partitions whose archive is not under `root` yet, each in its own directory
/// under `work` (`bake` turns its `served.csv` into `unit.pmtiles`), and links each result in
/// create-only; a reused partition's CSV is dropped unread. Returns each partition's prefix, its
/// archive, and whether it was baked now.
pub async fn bake_partitions<F, Fut>(
    work: &Path,
    root: &Path,
    parts: Vec<CsvPart>,
    bake_inputs: &str,
    mut bake: F,
) -> anyhow::Result<Vec<(String, PathBuf, bool)>>
where
    F: FnMut(PathBuf) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut archives = Vec::with_capacity(parts.len());
    for part in parts {
        let archive = partition_archive(root, &part, bake_inputs);
        let baked = !archive.exists();
        if baked {
            let dir = work.join(format!("partition-{}", part.prefix));
            std::fs::create_dir_all(dir.join("tmp"))?;
            std::fs::rename(&part.csv, dir.join("served.csv"))?;
            bake(dir.clone()).await?;
            std::fs::create_dir_all(root.join(&part.prefix))?;
            // Create-only: a named archive is never rewritten, and appears only once complete.
            std::fs::hard_link(dir.join("unit.pmtiles"), &archive)
                .with_context(|| format!("partition archive {}", archive.display()))?;
            std::fs::remove_dir_all(&dir)?;
        } else {
            std::fs::remove_file(&part.csv)?;
        }
        archives.push((part.prefix, archive, baked));
    }
    Ok(archives)
}

/// What [`warm_partitions`] left: every partition's archive is in the cache, or some still wait.
#[derive(Debug, PartialEq, Eq)]
pub enum Warmed {
    All,
    /// The run reached its budget with these partitions not baked yet; `baked` are the ones this
    /// run added to the cache.
    Deferred {
        baked: Vec<String>,
        remaining: Vec<String>,
    },
}

/// Root ADR-0493: bakes the partitions whose archive is not in the cache under `root` yet, one at a
/// time, and starts none after `deadline`. Each archive lands create-only as it completes, so a run
/// stopped at its budget loses nothing and the next start continues where it stopped. The CSVs stay
/// for [`bake_partitions`], which then finds every archive and only joins them.
pub async fn warm_partitions<F, Fut>(
    work: &Path,
    root: &Path,
    parts: &[CsvPart],
    bake_inputs: &str,
    deadline: std::time::Instant,
    mut bake: F,
) -> anyhow::Result<Warmed>
where
    F: FnMut(PathBuf) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut baked = Vec::new();
    let waiting: Vec<&CsvPart> = parts
        .iter()
        .filter(|part| !partition_archive(root, part, bake_inputs).exists())
        .collect();
    for (at, part) in waiting.iter().enumerate() {
        if std::time::Instant::now() >= deadline {
            let remaining = waiting[at..]
                .iter()
                .map(|part| part.prefix.clone())
                .collect();
            return Ok(Warmed::Deferred { baked, remaining });
        }
        let dir = work.join(format!("warm-{}", part.prefix));
        std::fs::create_dir_all(dir.join("tmp"))?;
        std::fs::hard_link(&part.csv, dir.join("served.csv"))?;
        bake(dir.clone()).await?;
        let archive = partition_archive(root, part, bake_inputs);
        std::fs::create_dir_all(root.join(&part.prefix))?;
        std::fs::hard_link(dir.join("unit.pmtiles"), &archive)
            .with_context(|| format!("partition archive {}", archive.display()))?;
        std::fs::remove_dir_all(&dir)?;
        baked.push(part.prefix.clone());
    }
    Ok(Warmed::All)
}

/// Root ADR-0493: a run budget is positive and belongs to a partitioned unit.
pub(super) fn validate_budgets(contract: &super::BakeContract) -> anyhow::Result<()> {
    for (unit, budget) in &contract.partition_run_budget_seconds {
        anyhow::ensure!(
            *budget > 0 && contract.partition_by_feature_id_prefix.contains_key(unit),
            "{unit}'s partition_run_budget_seconds is positive and names a partitioned unit"
        );
    }
    // Root ADR-0576: the join run's bound is later than the partition budget it follows.
    for (unit, budget) in &contract.tile_join_run_budget_seconds {
        anyhow::ensure!(
            contract
                .partition_run_budget_seconds
                .get(unit)
                .is_some_and(|partitions| budget > partitions),
            "{unit}'s tile_join_run_budget_seconds is later than its partition_run_budget_seconds"
        );
    }
    Ok(())
}

/// Root ADR-0493: warms the partition cache within the contract's run budget, before any Catalog or
/// R2 write. `true` when the run reached its budget and ends deferred, said on the line the job
/// reads; the next start continues from the cache.
pub(super) async fn deferred(
    config: &super::Config,
    work: &Path,
    parts: &[CsvPart],
    bake_inputs: &str,
    summary: &super::ServedSummary,
    (images, columns, srid, layer): (&super::ImageSet, &[String], i32, &super::ServedLayer),
) -> anyhow::Result<bool> {
    let Some(deadline) = config.partition_deadline else {
        return Ok(false);
    };
    let root = partition_root(config);
    let waiting: Vec<String> = parts
        .iter()
        .filter(|part| !partition_archive(&root, part, bake_inputs).exists())
        .map(|part| part.prefix.clone())
        .collect();
    let warmed = warm_partitions(
        work,
        &root,
        parts,
        bake_inputs,
        deadline,
        |dir| async move { super::bake_unit(config, images, &dir, columns, srid, layer).await },
    )
    .await?;
    if let Warmed::Deferred { baked, remaining } = warmed {
        println!(
            "lakehouse-tile-bake-deferred unit={} gold_snapshot={} baked={} remaining={}",
            config.unit_key,
            summary.canonical_iceberg_snapshot_id,
            baked.join(","),
            remaining.join(",")
        );
        return Ok(true);
    }
    // Root ADR-0576: every partition is cached. The join runs alone at the next start, which reads
    // no handoff, unless an earlier run already left the joined archive.
    let archives: Vec<(String, PathBuf)> = parts
        .iter()
        .map(|part| {
            (
                part.prefix.clone(),
                partition_archive(&root, part, bake_inputs),
            )
        })
        .collect();
    if joined_archive(&root, &archives).exists() {
        return Ok(false);
    }
    JoinNext {
        gold_snapshot: summary.canonical_iceberg_snapshot_id.clone(),
        archives,
    }
    .write(&root)?;
    println!(
        "lakehouse-tile-bake-deferred unit={} gold_snapshot={} baked={} remaining={TILE_JOIN}",
        config.unit_key,
        summary.canonical_iceberg_snapshot_id,
        waiting.join(",")
    );
    Ok(true)
}

/// What a deferred line names when every partition is cached and only the join is left.
pub const TILE_JOIN: &str = "tile-join";

/// The partition cache of the run's unit: `<work_root>/<unit>-partitions` (root ADR-0316).
pub(super) fn partition_root(config: &super::Config) -> PathBuf {
    config
        .work_root
        .join(format!("{}-partitions", config.unit_key))
}

/// Root ADR-0576: where the join of these partition archives lives under `root`, named by the
/// archives it joins (each already named by its rows and every bake input), so the same partitions
/// find the same joined archive and any changed one another.
pub fn joined_archive(root: &Path, archives: &[(String, PathBuf)]) -> PathBuf {
    let mut names: Vec<String> = archives
        .iter()
        .map(|(prefix, archive)| {
            let file = archive
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            format!("{prefix}/{file}")
        })
        .collect();
    names.sort();
    let mut name = Sha256::new();
    for each in &names {
        name.update(each.as_bytes());
        name.update(b"\n");
    }
    root.join("joined")
        .join(format!("{:x}.pmtiles", name.finalize()))
}

/// Root ADR-0576: the join a run left for the next start, for the Gold snapshot it warmed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JoinNext {
    pub gold_snapshot: String,
    pub archives: Vec<(String, PathBuf)>,
}

impl JoinNext {
    fn path(root: &Path) -> PathBuf {
        root.join("join-next.json")
    }

    pub fn read(root: &Path) -> anyhow::Result<Option<Self>> {
        let path = Self::path(root);
        match std::fs::read(&path) {
            Ok(bytes) => {
                Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                    format!("join state {}", path.display())
                })?))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("join state {}", path.display())),
        }
    }

    /// Written whole beside its final name, then renamed: a reader sees the old state or the new.
    pub fn write(&self, root: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(root)?;
        let path = Self::path(root);
        let staged = path.with_extension("json.tmp");
        std::fs::write(&staged, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&staged, &path)?;
        Ok(())
    }
}

/// Root ADR-0316, ADR-0576, ADR-0613: the partition archives merged into `work/unit.pmtiles`
/// (`pmtiles_merge`: a tile one archive holds is copied as stored, a tile several hold is merged).
/// The result is kept create-only as the joined archive; a later run that finds it links it
/// instead of merging again.
pub(super) async fn join_partitions(
    config: &super::Config,
    work: &Path,
    archives: &[(String, PathBuf)],
    bound: std::time::Duration,
) -> anyhow::Result<PathBuf> {
    let root = partition_root(config);
    let joined = joined_archive(&root, archives);
    let out = work.join("unit.pmtiles");
    if joined.exists() {
        std::fs::hard_link(&joined, &out)
            .with_context(|| format!("joined archive {}", joined.display()))?;
        println!(
            "lakehouse-tile-bake-joined unit={} reused={}",
            config.unit_key,
            joined.display()
        );
        return Ok(out);
    }
    let started = std::time::Instant::now();
    let inputs: Vec<PathBuf> = archives
        .iter()
        .map(|(_, archive)| archive.clone())
        .collect();
    let (target, deadline) = (out.clone(), started + bound);
    // The merge reads and writes files and never waits on the network: one blocking task.
    let merged = tokio::task::spawn_blocking(move || {
        super::pmtiles_merge::merge_archives(&inputs, &target, deadline)
    })
    .await
    .context("the partition merge stopped")??;
    std::fs::create_dir_all(root.join("joined"))?;
    // Create-only: a named joined archive is never rewritten, and appears only once complete.
    std::fs::hard_link(&out, &joined)
        .with_context(|| format!("joined archive {}", joined.display()))?;
    println!(
        "lakehouse-tile-bake-joined unit={} partitions={} tiles={} copied={} merged={} seconds={}",
        config.unit_key,
        merged.archives,
        merged.tiles,
        merged.copied,
        merged.merged,
        started.elapsed().as_secs()
    );
    Ok(out)
}

/// Root ADR-0576: a run that finds the join the last run left for this Gold snapshot joins only,
/// before reading the handoff, and ends deferred; the next start gates, uploads and promotes the
/// joined archive. `false` when there is no such join to do (and the run goes on as before).
pub(super) async fn join_ahead(
    config: &super::Config,
    summary: &super::ServedSummary,
    work: &Path,
) -> anyhow::Result<bool> {
    let (Some(_), Some(deadline)) = (config.partition_deadline, config.join_deadline) else {
        return Ok(false);
    };
    let root = partition_root(config);
    let Some(next) = JoinNext::read(&root)? else {
        return Ok(false);
    };
    if next.gold_snapshot != summary.canonical_iceberg_snapshot_id
        || joined_archive(&root, &next.archives).exists()
        || next.archives.iter().any(|(_, archive)| !archive.exists())
    {
        return Ok(false);
    }
    // The run does nothing else, so its join may take what the unit's limit leaves (11-20: the
    // one-tool bound stopped tile-join at 3,600 s).
    let bound = deadline.saturating_duration_since(std::time::Instant::now());
    join_partitions(config, work, &next.archives, bound).await?;
    println!(
        "lakehouse-tile-bake-deferred unit={} gold_snapshot={} baked= remaining=gate,upload,promote",
        config.unit_key, summary.canonical_iceberg_snapshot_id
    );
    Ok(true)
}
