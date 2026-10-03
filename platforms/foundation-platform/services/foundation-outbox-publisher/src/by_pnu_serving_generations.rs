//! Which generations a by-PNU serving lane's bucket holds objects in (ADR-0096, ADR-0100).
//!
//! A directory exists in R2 only while an object sits under it, so one delimited listing of the
//! lane root names every generation anything was written to — including one an earlier bake left
//! half written and never published: the hand-kept server script before 2026-10, or a scheduled
//! run whose state root was wiped. The scheduled bake (`scripts/ops/by-pnu-serving-bake.sh`)
//! starts a new generation above all of them. Reusing such a directory would let the resumed
//! export count another snapshot's objects as its own, because a key the listing already holds is
//! recorded without being read back.
//!
//! Read-only. A prefix the lane would not itself produce (`v01/`, a stray file) is not a
//! generation and cannot collide with one the bake picks.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{bail, Context};
use foundation_outbox::{
    object_storage::{R2InventoryRequest, MAX_R2_INVENTORY_MAX_KEYS},
    R2ObjectStorage,
};

use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::r2_layout::by_pnu;

/// The lane's `generation -> "<root>/v<generation>/"` builder.
type GenerationPrefix = Box<dyn Fn(u64) -> anyhow::Result<String> + Send + Sync>;

fn generation_prefix(lane: ByPnuLane) -> GenerationPrefix {
    Box::new(move |generation| by_pnu::generation_prefix(lane, generation))
}

/// The directory every generation of the lane sits in, trailing slash included.
fn lane_root(generation_prefix: &GenerationPrefix) -> anyhow::Result<String> {
    let first = generation_prefix(1)?;
    first
        .strip_suffix("v1/")
        .map(ToOwned::to_owned)
        .with_context(|| format!("the generation prefix {first} does not end in v1/"))
}

/// The generation `prefix` names, when it is exactly the prefix the lane builds for it.
fn generation_of(prefix: &str, root: &str, generation_prefix: &GenerationPrefix) -> Option<u64> {
    prefix
        .strip_prefix(root)?
        .strip_prefix('v')?
        .strip_suffix('/')?
        .parse::<u64>()
        .ok()
        .filter(|generation| {
            generation_prefix(*generation).is_ok_and(|canonical| canonical == prefix)
        })
}

/// Generations named by one delimited listing page of the lane root.
///
/// # Errors
/// A truncated page is refused: the generations it did not return might be the highest.
fn generations_from_listing(
    common_prefixes: &[String],
    truncated: bool,
    root: &str,
    generation_prefix: &GenerationPrefix,
) -> anyhow::Result<BTreeSet<u64>> {
    if truncated {
        bail!(
            "the listing of {root} was cut short at {MAX_R2_INVENTORY_MAX_KEYS} entries; which \
             generations hold objects is unknown, so no new generation can be chosen"
        );
    }
    Ok(common_prefixes
        .iter()
        .filter_map(|prefix| generation_of(prefix, root, generation_prefix))
        .collect())
}

/// Every generation with at least one object in the bucket.
///
/// # Errors
/// Returns an error when the provider rejects the listing or the listing is truncated.
pub(crate) async fn in_bucket(
    storage: &R2ObjectStorage,
    lane: ByPnuLane,
) -> anyhow::Result<BTreeSet<u64>> {
    let generation_prefix = &generation_prefix(lane);
    let root = lane_root(generation_prefix)?;
    let request = R2InventoryRequest::new(Some(&root), Some(MAX_R2_INVENTORY_MAX_KEYS))
        .context("failed to build the serving generation listing request")?;
    let page = storage
        .inventory(request)
        .await
        .with_context(|| format!("failed to list the serving generations under {root}"))?;
    generations_from_listing(
        page.common_prefixes(),
        page.is_truncated(),
        &root,
        generation_prefix,
    )
}

/// Every generation directory with at least one entry under a local serving root.
///
/// # Errors
/// Returns an error when the directory exists but cannot be read.
pub(crate) fn in_directory(local_root: &Path, lane: ByPnuLane) -> anyhow::Result<BTreeSet<u64>> {
    let generation_prefix = &generation_prefix(lane);
    let root = lane_root(generation_prefix)?;
    let directory = local_root.join(&root);
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to list {}", directory.display()));
        }
    };
    let mut generations = BTreeSet::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to list {}", directory.display()))?;
        let path = entry.path();
        // R2 has no empty directories; an empty local one is not a generation either.
        if !path.is_dir()
            || std::fs::read_dir(&path)
                .with_context(|| format!("failed to list {}", path.display()))?
                .next()
                .is_none()
        {
            continue;
        }
        let prefix = format!("{root}{}/", entry.file_name().to_string_lossy());
        if let Some(generation) = generation_of(&prefix, &root, generation_prefix) {
            generations.insert(generation);
        }
    }
    Ok(generations)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "foundation-platform-by-pnu-generations-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    #[test]
    fn a_listing_names_only_the_generations_the_lane_would_build() -> anyhow::Result<()> {
        let parcel = &generation_prefix(ByPnuLane::Parcel);
        let root = lane_root(parcel)?;
        let prefixes = [
            format!("{root}v3/"),
            format!("{root}v12/"),
            format!("{root}v01/"),
            format!("{root}v0/"),
            format!("{root}vx/"),
            "serving/other/v9/".to_owned(),
        ];
        let generations = generations_from_listing(&prefixes, false, &root, parcel)?;
        assert_eq!(generations, BTreeSet::from([3, 12]));
        Ok(())
    }

    #[test]
    fn a_truncated_listing_is_refused() -> anyhow::Result<()> {
        let building = &generation_prefix(ByPnuLane::Building);
        let root = lane_root(building)?;
        let refused = generations_from_listing(&[format!("{root}v3/")], true, &root, building);
        assert!(
            refused.is_err(),
            "a cut-short listing must not pass as complete"
        );
        Ok(())
    }

    #[test]
    fn a_half_written_generation_on_disk_is_named_and_an_empty_one_is_not() -> anyhow::Result<()> {
        let local = temporary_root("local");
        let parcel = &generation_prefix(ByPnuLane::Parcel);
        let root = lane_root(parcel)?;
        std::fs::create_dir_all(local.join(format!("{root}v2")))?;
        std::fs::write(local.join(format!("{root}v2/object.json")), b"{}")?;
        std::fs::create_dir_all(local.join(format!("{root}v5")))?;
        std::fs::write(local.join(format!("{root}v5/object.json")), b"{}")?;
        std::fs::create_dir_all(local.join(format!("{root}v7")))?;
        std::fs::create_dir_all(local.join(format!("{root}v05")))?;
        std::fs::write(local.join(format!("{root}v05/object.json")), b"{}")?;
        std::fs::write(local.join(format!("{root}manifest.json")), b"{}")?;

        let generations = in_directory(&local, ByPnuLane::Parcel)?;
        let nothing = in_directory(&temporary_root("absent"), ByPnuLane::Parcel)?;

        std::fs::remove_dir_all(&local)?;
        assert_eq!(generations, BTreeSet::from([2, 5]));
        assert!(nothing.is_empty());
        Ok(())
    }
}
