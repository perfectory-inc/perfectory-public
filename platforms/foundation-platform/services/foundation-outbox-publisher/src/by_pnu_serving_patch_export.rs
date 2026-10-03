//! What both by-PNU exports share when they bake a patch generation (root ADR-0141 §1, §2).
//!
//! A full bake writes every Gold row into a new base generation. A patch writes only the change
//! set the delta job named (`by_pnu_panel_delta.py`) into `{root}/v{base}/p{patch}/`: a document
//! for every changed or new PNU, a tombstone for every PNU gone from Gold. The scan keeps only the
//! rows of the change set, so a patch's memory and time follow the number of changes.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use anyhow::{ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};

use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_serving_manifest::tombstone_body;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::r2_layout::by_pnu;

/// The patch generation a run writes, and the PNUs it must write tombstones for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatchTarget {
    pub(crate) patch: u64,
    pub(crate) deleted: BTreeSet<String>,
}

/// Reads the patch target from the lane's `TARGET_PATCH` and `DELETE_LIST_PATH`.
///
/// # Errors
/// Refuses a patch without a delete list (an empty file is the empty list), a delete list
/// without a patch, a patch below 1, and a list that is not one PNU of the contract grammar per
/// line.
pub(crate) fn from_env(
    lane: ByPnuLane,
    optional_env: impl Fn(&str) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Option<PatchTarget>> {
    let patch = optional_env(&lane.env("TARGET_PATCH"))?;
    let deletes = optional_env(&lane.env("DELETE_LIST_PATH"))?;
    match (patch, deletes) {
        (None, None) => Ok(None),
        (Some(patch), Some(deletes)) => {
            let patch = patch.parse::<u64>().with_context(|| {
                format!("{} must be a positive integer", lane.env("TARGET_PATCH"))
            })?;
            ensure!(
                patch >= 1,
                "{} must be at least 1",
                lane.env("TARGET_PATCH")
            );
            Ok(Some(PatchTarget {
                patch,
                deleted: read_pnu_list(lane, Path::new(&deletes))?,
            }))
        }
        _ => anyhow::bail!(
            "{} and {} come together: a patch names its deletes, even when there are none",
            lane.env("TARGET_PATCH"),
            lane.env("DELETE_LIST_PATH")
        ),
    }
}

/// One PNU per line, in the contract grammar, each once. An empty file is the empty list.
///
/// # Errors
/// Refuses an unreadable file, a PNU outside the grammar and a repeated PNU.
pub(crate) fn read_pnu_list(lane: ByPnuLane, path: &Path) -> anyhow::Result<BTreeSet<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the PNU list {}", path.display()))?;
    let mut pnus = BTreeSet::new();
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        by_pnu::check_pnu(lane, line)?;
        ensure!(
            pnus.insert(line.to_owned()),
            "{} names {line} twice",
            path.display()
        );
    }
    Ok(pnus)
}

/// The key one PNU's object goes to: the base generation, or the patch over it.
///
/// # Errors
/// Refuses numbers or a PNU outside the contract grammar.
pub(crate) fn object_key(
    lane: ByPnuLane,
    generation: u64,
    patch: Option<&PatchTarget>,
    pnu: &str,
) -> anyhow::Result<String> {
    match patch {
        Some(target) => by_pnu::patch_object_key(lane, generation, target.patch, pnu),
        None => by_pnu::object_key(lane, generation, pnu),
    }
}

/// The keys the run's directory (base generation or patch) already holds in the shard.
///
/// # Errors
/// Returns an error when the listing fails.
pub(crate) async fn list_existing(
    store: &ByPnuServingStore,
    generation: u64,
    patch: Option<&PatchTarget>,
    pnu_prefix: Option<&str>,
) -> anyhow::Result<HashSet<String>> {
    match patch {
        Some(target) => {
            store
                .list_patch_keys(generation, target.patch, pnu_prefix)
                .await
        }
        None => {
            store
                .list_existing_generation_keys(generation, pnu_prefix)
                .await
        }
    }
}

/// Whether the scan keeps a row with `pnu`: inside the shard, and — when a change set or an
/// allowlist scopes the run — named by it. A patch keeps its deleted PNUs too, so a delete the
/// table still holds is refused instead of tombstoned.
pub(crate) fn keeps(
    pnu: &str,
    pnu_prefix: Option<&str>,
    allowlist: Option<&BTreeSet<String>>,
    patch: Option<&PatchTarget>,
) -> bool {
    pnu_prefix.is_none_or(|prefix| pnu.starts_with(prefix))
        && (allowlist.is_none_or(|list| list.contains(pnu))
            || patch.is_some_and(|target| target.deleted.contains(pnu)))
}

/// Refuses a delete-listed PNU the snapshot still holds.
///
/// # Errors
/// Names the first such PNU.
pub(crate) fn refuse_live_deletes<'a>(
    patch: Option<&PatchTarget>,
    kept_pnus: impl Iterator<Item = &'a str>,
) -> anyhow::Result<()> {
    if let Some(target) = patch {
        for pnu in kept_pnus {
            ensure!(
                !target.deleted.contains(pnu),
                "{pnu} is on the delete list but the Gold snapshot still holds it; the change set \
                 is not of this snapshot"
            );
        }
    }
    Ok(())
}

/// One tombstone the run wrote or found.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TombstoneEntry {
    pub(crate) pnu: String,
    pub(crate) object_key: String,
    pub(crate) object_size_bytes: u64,
    pub(crate) object_checksum_sha256: String,
    pub(crate) write_outcome: &'static str,
}

/// Writes the tombstones of the shard's deleted PNUs, create-only.
///
/// # Errors
/// Returns an error when a key is not canonical or a write fails or collides.
#[allow(clippy::too_many_arguments)] // one call site per lane; a struct would only rename them
pub(crate) async fn write_tombstones(
    store: &ByPnuServingStore,
    generation: u64,
    patch: &PatchTarget,
    pnu_prefix: Option<&str>,
    gold_table: &str,
    snapshot: &str,
    existing_keys: &HashSet<String>,
    max_concurrency: usize,
) -> anyhow::Result<Vec<TombstoneEntry>> {
    let lane = store.lane();
    let mut writes = Vec::new();
    for pnu in patch
        .deleted
        .iter()
        .filter(|pnu| pnu_prefix.is_none_or(|prefix| pnu.starts_with(prefix)))
    {
        let key = by_pnu::patch_object_key(lane, generation, patch.patch, pnu)?;
        let (body, checksum) = tombstone_body(pnu, gold_table, snapshot)?;
        writes.push(async move {
            let write_outcome = if existing_keys.contains(&key) {
                ensure!(
                    store.read_bytes(&key).await? == body,
                    "listed tombstone {key} differs from the one this change set writes"
                );
                "listed"
            } else if store
                .write_object_create_only(&key, &body, &checksum)
                .await?
            {
                "created"
            } else {
                "reused"
            };
            Ok::<_, anyhow::Error>(TombstoneEntry {
                pnu: pnu.clone(),
                object_key: key,
                object_size_bytes: u64::try_from(body.len())?,
                object_checksum_sha256: checksum,
                write_outcome,
            })
        });
    }
    let mut entries = stream::iter(writes)
        .buffer_unordered(max_concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    entries.sort_by(|a, b| a.pnu.cmp(&b.pnu));
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::by_pnu_serving_manifest::read_tombstone;
    use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;

    const PNU_A: &str = "9999900000100000000";
    const PNU_B: &str = "9999900000200000000";

    fn target(deleted: &[&str]) -> PatchTarget {
        PatchTarget {
            patch: 2,
            deleted: deleted.iter().map(|pnu| (*pnu).to_owned()).collect(),
        }
    }

    #[test]
    fn the_scan_keeps_the_change_set_and_its_deletes_in_the_shard() {
        let allow = BTreeSet::from([PNU_A.to_owned()]);
        let patch = target(&[PNU_B]);
        assert!(keeps(PNU_A, None, Some(&allow), Some(&patch)));
        assert!(keeps(PNU_B, None, Some(&allow), Some(&patch)));
        assert!(!keeps(
            "9999900000300000000",
            None,
            Some(&allow),
            Some(&patch)
        ));
        assert!(!keeps(PNU_A, Some("8"), Some(&allow), Some(&patch)));
        assert!(keeps("9999900000300000000", None, None, None));
    }

    #[test]
    fn a_delete_the_snapshot_still_holds_is_refused() {
        let patch = target(&[PNU_B]);
        assert!(refuse_live_deletes(Some(&patch), [PNU_A].into_iter()).is_ok());
        assert!(refuse_live_deletes(Some(&patch), [PNU_B].into_iter()).is_err());
        assert!(refuse_live_deletes(None, [PNU_B].into_iter()).is_ok());
    }

    #[test]
    fn a_patch_and_its_delete_list_come_together() {
        let lane = ByPnuLane::Parcel;
        let only_patch = from_env(lane, |name| {
            Ok((name == lane.env("TARGET_PATCH")).then(|| "1".to_owned()))
        });
        assert!(only_patch.is_err());
        assert_eq!(from_env(lane, |_| Ok(None)).ok(), Some(None));
    }

    #[tokio::test]
    async fn tombstones_are_written_create_only_inside_the_patch() -> anyhow::Result<()> {
        let lane = ByPnuLane::Building;
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-by-pnu-tombstones-{}",
            uuid::Uuid::now_v7()
        ));
        let store =
            ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
        let patch = target(&[PNU_A, PNU_B]);
        let first = write_tombstones(
            &store,
            3,
            &patch,
            Some("99999000002"),
            "gold.building_panel",
            "999990000000000002",
            &HashSet::new(),
            4,
        )
        .await?;
        let again = write_tombstones(
            &store,
            3,
            &patch,
            None,
            "gold.building_panel",
            "999990000000000002",
            &HashSet::new(),
            4,
        )
        .await?;
        let stored = store
            .read_bytes(&by_pnu::patch_object_key(lane, 3, 2, PNU_B)?)
            .await?;
        std::fs::remove_dir_all(&root)?;
        assert_eq!(
            first.len(),
            1,
            "the shard wrote a tombstone outside its prefix"
        );
        assert_eq!(
            again
                .iter()
                .map(|entry| entry.write_outcome)
                .collect::<Vec<_>>(),
            vec!["created", "reused"]
        );
        assert!(read_tombstone(&stored).is_some_and(|t| t.pnu == PNU_B));
        Ok(())
    }
}
