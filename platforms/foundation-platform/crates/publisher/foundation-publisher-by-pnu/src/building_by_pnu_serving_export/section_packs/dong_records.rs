//! The record a pack bake keeps of each dong it finished, so a run stopped inside a shard resumes
//! at the dong (W83c, root ADR-0147).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::bake::{Cut, PackEntry, WithheldDocument};

/// Where a bake records each dong it finished, and the build whose records it may reuse.
///
/// A shard is resumed whole or not at all (its summary), and a re-run used to render and check
/// every dong again before finding its packs already written. A first shard longer than the unit's
/// time limit then made no progress at all: the real-transaction lane's full bake was stopped at
/// 1 h 55 three times (2026-11-18..19) without one shard finishing. A dong's record holds what its
/// write produced: the packs (keys and sha256), the gate's counts and its cut. A re-run of the
/// same build, snapshot, generations and sections takes the record instead of rendering the dong:
/// the build renders a dong to the same bytes every time (create-only reports `reused`), so the
/// record is what the render would give.
#[derive(Clone, Debug)]
pub struct DongProgress {
    pub dir: PathBuf,
    /// The publisher's identity (the sha256 of its binary): another build may render otherwise.
    pub build: String,
}

/// One finished dong, as `DongProgress` keeps it.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct DongRecord {
    pub(super) identity: DongIdentity,
    pub(super) packs: Vec<PackEntry>,
    pub(super) compared: u64,
    pub(super) equal: u64,
    pub(super) candidates: Vec<String>,
    pub(super) paged: u64,
    pub(super) withheld: Vec<WithheldDocument>,
    pub(super) cut: Cut,
    #[serde(default)]
    pub(super) takeover_compared: u64,
}

/// What a record must match to stand in for rendering its dong.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DongIdentity {
    pub(super) build: String,
    pub(super) document_schema_version: String,
    pub(super) gold_iceberg_snapshot_id: String,
    pub(super) generations: BTreeMap<String, u64>,
    pub(super) patch: Option<u64>,
    pub(super) sections: Vec<String>,
    pub(super) rows: u64,
    pub(super) deleted: u64,
}

pub(super) fn read_dong_record(path: &Path) -> anyhow::Result<Option<DongRecord>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("{} is not a dong record", path.display())
            })?))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Writes `record` at `path` whole and renamed: a record is either all there or absent.
pub(super) fn write_dong_record(
    progress: &DongProgress,
    path: &Path,
    record: &DongRecord,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(&progress.dir)
        .with_context(|| format!("failed to create {}", progress.dir.display()))?;
    let partial = path.with_extension("json.partial");
    std::fs::write(&partial, serde_json::to_vec(record)?)
        .with_context(|| format!("failed to write {}", partial.display()))?;
    std::fs::rename(&partial, path)
        .with_context(|| format!("failed to record {}", path.display()))?;
    Ok(())
}
