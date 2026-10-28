//! Root ADR-0316: a partitioned unit's GDAL input (one CSV per partition) and the per-partition
//! archives a bake reuses when a partition's rows and bake inputs did not change.

use std::ffi::OsString;
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

/// tile-join's arguments after the image: every partition archive (linked as
/// `/w/join/<prefix>.pmtiles`) into one archive, every tile kept whole.
pub fn tile_join_args(prefixes: &[String]) -> Vec<OsString> {
    let mut args: Vec<OsString> = [
        "-o",
        "/w/unit.pmtiles",
        "--no-tile-size-limit",
        "--force",
        "--quiet",
    ]
    .map(OsString::from)
    .into();
    args.extend(
        prefixes
            .iter()
            .map(|prefix| format!("/w/join/{prefix}.pmtiles").into()),
    );
    args
}
