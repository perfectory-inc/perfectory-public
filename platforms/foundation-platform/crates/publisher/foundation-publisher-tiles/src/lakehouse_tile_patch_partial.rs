//! Root ADR-0375 §2: a partial handoff — only the zones of the tiles a change touches, and the list
//! of changed ids — patches a unit without anyone reading every row.
//!
//! The served snapshot's feature index (ADR-0323 §1) already describes every other feature, so the
//! new index is the old one with the change list applied: a deleted id leaves it, an upserted id
//! takes the record its handoff row makes. Every other handed row must match the old index exactly,
//! so a handoff that drifted from what is served is declined rather than patched. The handoff must
//! also hold every row the patch's neighbourhood asks for: the tiles it names cover the minzoom
//! ancestors of every dirty tile. A partial handoff is never baked as a base: when the patch path
//! declines it, the bake exits with `lakehouse-tile-bake-partial-declined` and the caller hands
//! every row instead.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{BufRead as _, BufReader};
use std::path::{Component, Path};
use std::sync::Arc;

use anyhow::{bail, ensure, Context as _};
use foundation_publisher_core::refusal::Refused;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use super::patch::{PatchPolicy, Record};
use super::{id_hash, patch_run, BakeContract, Config, ServedSummary};

/// The summary's `partial` block.
#[derive(Debug, Clone, Deserialize)]
pub struct PartialHandoff {
    /// The snapshot the tiles serve, which the change list is relative to.
    pub base_snapshot: String,
    pub neighbourhood_zoom: u8,
    /// Every tile `(x, y)` at `neighbourhood_zoom` whose current rows the handoff holds.
    pub neighbourhood_tiles: Vec<(u32, u32)>,
    pub change_list: ChangeListFile,
}

/// The change list beside the handoff: one `{"feature_id", "op": "upsert" | "delete"}` per line.
#[derive(Debug, Clone, Deserialize)]
pub struct ChangeListFile {
    /// A file name in the handoff's directory.
    pub path: String,
    pub rows: usize,
    pub sha256: String,
}

impl PartialHandoff {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.base_snapshot.is_empty(),
            "a partial handoff names the snapshot it applies to"
        );
        let mut parts = Path::new(&self.change_list.path).components();
        ensure!(
            matches!(parts.next(), Some(Component::Normal(_))) && parts.next().is_none(),
            "the change list {:?} is not a file name beside the handoff",
            self.change_list.path
        );
        ensure!(
            self.change_list.sha256.len() == 64
                && self
                    .change_list
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "the change list has no lowercase hex SHA-256"
        );
        Ok(())
    }

    /// Why the handoff cannot serve this neighbourhood, or `None` when it holds every tile of it.
    pub fn uncovered(&self, needed: &HashSet<(u32, u32)>, min_zoom: u8) -> Option<String> {
        if self.neighbourhood_zoom != min_zoom {
            return Some(format!(
                "the handoff holds zoom-{} tiles, the layer's minzoom is {min_zoom}",
                self.neighbourhood_zoom
            ));
        }
        let held: HashSet<(u32, u32)> = self.neighbourhood_tiles.iter().copied().collect();
        let missing = needed.iter().filter(|tile| !held.contains(tile)).count();
        (missing > 0).then(|| {
            format!(
                "the handoff lacks {missing} of the {} tiles the patch neighbourhood needs",
                needed.len()
            )
        })
    }
}

/// The changed ids, by their 16-byte hashes.
#[derive(Debug, Default, PartialEq)]
pub struct ChangeList {
    pub upserted: BTreeSet<u128>,
    pub deleted: BTreeSet<u128>,
}

/// Reads the change list beside `handoff`, checking its count and SHA-256 against the summary.
pub fn read_change_list(file: &ChangeListFile, handoff: &Path) -> anyhow::Result<ChangeList> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Line {
        feature_id: String,
        op: String,
    }
    let path = handoff
        .parent()
        .context("the handoff has no directory")?
        .join(&file.path);
    let bytes = std::fs::read(&path).with_context(|| format!("change list {}", path.display()))?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    ensure!(
        sha256 == file.sha256,
        "the change list hashes to {sha256}, the summary promises {}",
        file.sha256
    );
    let mut changes = ChangeList::default();
    let mut rows = 0;
    for line in BufReader::new(bytes.as_slice()).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        rows += 1;
        let parsed: Line = serde_json::from_str(&line).context("a change list line")?;
        let id = id_hash(&parsed.feature_id);
        let fresh = match parsed.op.as_str() {
            "upsert" => changes.upserted.insert(id),
            "delete" => changes.deleted.insert(id),
            other => bail!("change list op {other:?} is not upsert or delete"),
        };
        ensure!(fresh, "the change list names {} twice", parsed.feature_id);
    }
    ensure!(
        rows == file.rows,
        "the change list holds {rows} lines, the summary promises {}",
        file.rows
    );
    ensure!(
        changes.upserted.is_disjoint(&changes.deleted),
        "the change list both upserts and deletes one id"
    );
    Ok(changes)
}

/// The new snapshot's index: `old` with the change list applied. `handed` is the index of the
/// handoff's rows; an upserted id takes its record from it, and every other handed row must be in
/// `old` unchanged. `Err` is why the patch path declines the handoff.
pub fn merge(
    old: &[Record],
    handed: &[Record],
    changes: &ChangeList,
) -> Result<Vec<Record>, String> {
    let mut replaced: BTreeMap<u128, Option<Record>> =
        changes.deleted.iter().map(|id| (*id, None)).collect();
    let mut drifted = 0_usize;
    for record in handed {
        if changes.deleted.contains(&record.id) {
            return Err("the handoff carries a row the change list deletes".to_owned());
        }
        if changes.upserted.contains(&record.id) {
            replaced.insert(record.id, Some(*record));
        } else if old
            .binary_search_by_key(&record.id, |served| served.id)
            .map_or(true, |at| old[at] != *record)
        {
            drifted += 1;
        }
    }
    if drifted > 0 {
        return Err(format!(
            "{drifted} handed row(s) the change list does not name differ from the served index"
        ));
    }
    let absent = changes
        .upserted
        .iter()
        .filter(|id| !replaced.contains_key(id))
        .count();
    if absent > 0 {
        return Err(format!(
            "{absent} upserted id(s) have no row in the handoff"
        ));
    }
    let mut merged = Vec::with_capacity(old.len() + replaced.len());
    let mut pending = replaced.into_iter().peekable();
    // Both sides are sorted by id: a merge walk, each changed id replacing or dropping its record.
    for record in old {
        while let Some(&(id, replacement)) = pending.peek() {
            if id >= record.id {
                break;
            }
            pending.next();
            merged.extend(replacement);
        }
        match pending.peek() {
            Some(&(id, replacement)) if id == record.id => {
                pending.next();
                merged.extend(replacement);
            }
            _ => merged.push(*record),
        }
    }
    merged.extend(pending.filter_map(|(_, replacement)| replacement));
    Ok(merged)
}

/// Bakes a partial handoff: the patch path only. Anything that would need a new base declines with
/// a distinct line and a refusal (exit 65), and the caller hands every row instead.
pub(super) async fn bake_partial(
    config: &Config,
    contract: &BakeContract,
    policy: &PatchPolicy,
    summary: &Arc<ServedSummary>,
    work: &Path,
    newborn: bool,
) -> anyhow::Result<()> {
    let unit = config.unit_key.as_str();
    let reason = if newborn {
        "the unit has no base to patch yet".to_owned()
    } else if !policy.covers(unit) {
        format!("{unit} is not a patched unit")
    } else {
        let mut indexed = patch_run::index_handoff(summary, &config.served_handoff, work).await?;
        if patch_run::try_patch(config, contract, policy, summary, work, &mut indexed).await? {
            return Ok(());
        }
        "the patch path declined it (lakehouse-tile-bake-patch-skipped above)".to_owned()
    };
    println!("lakehouse-tile-bake-partial-declined unit={unit} reason={reason:?}");
    Err(Refused(format!(
        "a partial handoff is never baked as a base ({reason}); hand the bake every row"
    ))
    .into())
}

#[cfg(test)]
#[path = "lakehouse_tile_patch_partial_tests.rs"]
mod tests;
