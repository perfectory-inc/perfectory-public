//! Pins the Gold snapshot a published by-PNU manifest reflects (root ADR-0146 §2).
//!
//! The next change set is computed against `reflected_gold_iceberg_snapshot_id` (root ADR-0141
//! §3). When Iceberg snapshot expiry removes that snapshot, the change set is unknown and the
//! lane can only be re-based (a full bake, or the verified re-base of ADR-0146 §1). So every
//! publish tags the snapshot its manifest reflects, and Iceberg's expiry keeps every snapshot a
//! live tag names.
//!
//! - The tag is named after the publish that made it:
//!   `served-{unit}-{published_at}-{snapshot}-{publish_id}`. The publish id is a fresh UUID v7, so
//!   two publishes never share a name, even of one snapshot in one second, and a tag is never
//!   moved. The manifest records its tag (`reflected_gold_snapshot_tag`).
//! - The tag is created before the manifest is written. Once the manifest is live, the lane's
//!   tags older than the one the live manifest names are released; a newer tag belongs to a
//!   publish still running and stays. When the write fails, the manifest is read again: a write
//!   that landed keeps its pin, and only a confirmed lost compare-and-swap, with a live manifest
//!   that does not reflect this snapshot, releases the new tag. Anything unclear leaves an extra
//!   pin, which is the safe direction; the next publish releases it.
//! - Creating a tag is an Iceberg table commit, so the catalog token must be allowed to write
//!   the catalog; a token that may only read is refused naming that permission.
//! - A bake pins the snapshot it reads the same way, `baking-{unit}-{snapshot}`, before every
//!   shard reads it, and the lane's next publish releases those pins (root ADR-0338): a long
//!   generation finishes at its pin (root ADR-0310) however old that snapshot is.
//! - The production pins are tags in the Iceberg catalog. A local rehearsal store keeps the same
//!   tags in a file beside its objects, so a rehearsal never touches the catalog.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use uuid::Uuid;

use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;

/// File a local rehearsal store keeps its tags in, at the store root.
pub const LOCAL_TAGS_FILE: &str = "gold-snapshot-tags.json";

/// Where a lane's pins live.
pub enum SnapshotPins {
    /// Tags in the Iceberg catalog: the production pins.
    Catalog(Box<IcebergRestCatalog>),
    /// `{tag: snapshot}` per table in a JSON file beside a local rehearsal store.
    LocalFile(PathBuf),
}

/// The prefix every pin of `lane` carries; nothing else is ever released by a publish.
pub fn tag_prefix(lane: ByPnuLane) -> String {
    format!("served-{}-", lane.unit())
}

/// The pin a publish at `published_at_utc` (RFC 3339) makes of `snapshot`, under `publish_id`.
///
/// # Errors
/// Refuses a timestamp that is not RFC 3339 and a publish id that is not a UUID v7.
pub fn tag_name(
    lane: ByPnuLane,
    published_at_utc: &str,
    snapshot: i64,
    publish_id: Uuid,
) -> anyhow::Result<String> {
    ensure!(
        publish_id.get_version_num() == 7,
        "a pin's publish id must be a UUID v7 (time-ordered), got {publish_id}"
    );
    let published = DateTime::parse_from_rfc3339(published_at_utc)
        .with_context(|| format!("published_at_utc {published_at_utc:?} is not RFC 3339"))?
        .with_timezone(&Utc)
        .format("%Y%m%dT%H%M%SZ");
    Ok(format!(
        "{}{published}-{snapshot}-{}",
        tag_prefix(lane),
        publish_id.simple()
    ))
}

/// Where a pin of `lane` stands in publish order: its publish id, a UUID v7, which orders by the
/// time the pin was made. `None` for a name this lane's publish did not make.
pub fn tag_order(lane: ByPnuLane, name: &str) -> Option<Uuid> {
    let rest = name.strip_prefix(&tag_prefix(lane))?;
    let id = Uuid::try_parse(rest.rsplit('-').next()?).ok()?;
    (id.get_version_num() == 7).then_some(id)
}

impl SnapshotPins {
    /// The pins that go with the output store: the catalog for R2, a file for a local root.
    ///
    /// # Errors
    /// Returns an error when the catalog is not configured.
    pub fn for_output(output: &ProfileStoreConfig) -> anyhow::Result<Self> {
        Ok(match output {
            ProfileStoreConfig::Local { root } => Self::LocalFile(root.join(LOCAL_TAGS_FILE)),
            ProfileStoreConfig::R2 => Self::Catalog(Box::new(
                IcebergRestCatalog::new(
                    LakehouseCatalogConfig::from_env()
                        .context("the publish pins the reflected Gold snapshot in the catalog")?,
                )
                .context("failed to build the Iceberg catalog client")?,
            )),
        })
    }

    /// Every tag of `table` whose name starts with `prefix`, with the snapshot it names.
    ///
    /// # Errors
    /// Returns an error when the tags cannot be read.
    pub async fn tags(&self, table: &str, prefix: &str) -> anyhow::Result<BTreeMap<String, i64>> {
        let all = match self {
            Self::Catalog(catalog) => catalog
                .load_snapshot_refs(table)
                .await?
                .with_context(|| format!("{table} does not exist"))?
                .refs
                .into_iter()
                .filter(|(_, reference)| reference.kind == "tag")
                .map(|(name, reference)| (name, reference.snapshot_id))
                .collect(),
            Self::LocalFile(path) => read_local(path)?.remove(table).unwrap_or_default(),
        };
        Ok(all
            .into_iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .collect())
    }

    /// Creates tag `name` on `snapshot` of `table`; the same tag on the same snapshot is a re-run.
    ///
    /// # Errors
    /// Refuses a name another snapshot holds, or a snapshot the catalog no longer has.
    pub async fn create(&self, table: &str, name: &str, snapshot: i64) -> anyhow::Result<()> {
        match self {
            Self::Catalog(catalog) => Ok(catalog.create_tag(table, name, snapshot).await?),
            Self::LocalFile(path) => {
                let mut all = read_local(path)?;
                let tags = all.entry(table.to_owned()).or_default();
                if let Some(existing) = tags.get(name) {
                    ensure!(
                        *existing == snapshot,
                        "{table} already has a tag named {name} on snapshot {existing}; a pin is \
                         never moved"
                    );
                    return Ok(());
                }
                tags.insert(name.to_owned(), snapshot);
                write_local(path, &all)
            }
        }
    }

    /// Removes tag `name` of `table` while it still names `snapshot`; an absent tag is removed.
    ///
    /// # Errors
    /// Refuses a tag that names another snapshot.
    pub async fn remove(&self, table: &str, name: &str, snapshot: i64) -> anyhow::Result<()> {
        match self {
            Self::Catalog(catalog) => Ok(catalog.remove_tag(table, name, snapshot).await?),
            Self::LocalFile(path) => {
                let mut all = read_local(path)?;
                let Some(tags) = all.get_mut(table) else {
                    return Ok(());
                };
                match tags.get(name) {
                    None => return Ok(()),
                    Some(existing) => ensure!(
                        *existing == snapshot,
                        "{table}'s tag {name} names snapshot {existing}, not {snapshot}"
                    ),
                }
                tags.remove(name);
                write_local(path, &all)
            }
        }
    }
}

/// Pins `snapshot` of `table` for the manifest published at `published_at_utc`, under a new
/// publish id; returns the tag.
///
/// # Errors
/// Refuses a snapshot id that is not a number and any refusal of [`SnapshotPins::create`]; the
/// manifest must then not be written.
pub async fn pin(
    pins: &SnapshotPins,
    lane: ByPnuLane,
    table: &str,
    snapshot: &str,
    published_at_utc: &str,
) -> anyhow::Result<String> {
    let snapshot = parse_snapshot(snapshot)?;
    let tag = tag_name(lane, published_at_utc, snapshot, Uuid::now_v7())?;
    pins.create(table, &tag, snapshot).await.with_context(|| {
        format!("could not pin Gold snapshot {snapshot} of {table} as {tag}; the manifest stays")
    })?;
    tracing::info!(table, tag = %tag, snapshot, "reflected Gold snapshot pinned");
    Ok(tag)
}

/// Releases every pin of `lane` on `table` older than `live`, the tag the live manifest names.
/// Returns the released tags. A newer tag is a publish still running and stays, and so does a
/// tag whose order cannot be read. A tag that cannot be released stays too; it only keeps a
/// snapshot alive longer, and the next publish releases it.
pub async fn release_older(
    pins: &SnapshotPins,
    lane: ByPnuLane,
    table: &str,
    live: &str,
) -> Vec<String> {
    let Some(live_order) = tag_order(lane, live) else {
        tracing::warn!(
            table,
            live,
            "the live manifest's pin is not one this lane made; nothing is released"
        );
        return Vec::new();
    };
    let tags = match pins.tags(table, &tag_prefix(lane)).await {
        Ok(tags) => tags,
        Err(error) => {
            tracing::warn!(table, error = %format!("{error:#}"), "older pins not read; they stay until the next publish");
            return Vec::new();
        }
    };
    let mut released = Vec::new();
    for (name, snapshot) in tags {
        if tag_order(lane, &name).is_none_or(|order| order >= live_order) {
            continue;
        }
        match pins.remove(table, &name, snapshot).await {
            Ok(()) => released.push(name),
            Err(error) => tracing::warn!(
                table,
                tag = %name,
                error = %format!("{error:#}"),
                "older pin not released; it stays until the next publish"
            ),
        }
    }
    released
}

/// The prefix of the pins a running bake of `lane` makes (root ADR-0338); a publish of the lane
/// releases them all.
pub fn bake_tag_prefix(lane: ByPnuLane) -> String {
    format!("baking-{}-", lane.unit())
}

/// Pins `snapshot` of `table`, the Gold snapshot a bake of `lane` reads, before it is read (root
/// ADR-0338). A generation can take longer than the expiry window, and the window counts from the
/// snapshot's commit, not from the bake's start: a snapshot committed 29 days before the bake began
/// had one day left. Every shard pins again (the same name on the same snapshot is a re-run), so a
/// pin released by a publish in between is made again before the next read. Returns the tag.
///
/// # Errors
/// Refuses a snapshot id that is not a number and any refusal of [`SnapshotPins::create`]; an
/// expired snapshot is refused with "no longer holds snapshot", which the bake script reads.
pub async fn pin_bake(
    pins: &SnapshotPins,
    lane: ByPnuLane,
    table: &str,
    snapshot: &str,
) -> anyhow::Result<String> {
    let snapshot = parse_snapshot(snapshot)?;
    let tag = format!("{}{snapshot}", bake_tag_prefix(lane));
    pins.create(table, &tag, snapshot).await.with_context(|| {
        format!("could not pin the baked Gold snapshot {snapshot} of {table} as {tag}")
    })?;
    Ok(tag)
}

/// Releases every bake pin of `lane` on `table` once a publish of the lane is live (root ADR-0338):
/// the generation it pinned is then served, and its snapshot carries a `served-` pin, or it was left
/// unserved and is never resumed. Returns the released tags; one that cannot be released stays
/// until the next publish.
pub async fn release_bake_pins(pins: &SnapshotPins, lane: ByPnuLane, table: &str) -> Vec<String> {
    let tags = match pins.tags(table, &bake_tag_prefix(lane)).await {
        Ok(tags) => tags,
        Err(error) => {
            tracing::warn!(table, error = %format!("{error:#}"), "bake pins not read; they stay until the next publish");
            return Vec::new();
        }
    };
    let mut released = Vec::new();
    for (name, snapshot) in tags {
        match pins.remove(table, &name, snapshot).await {
            Ok(()) => released.push(name),
            Err(error) => tracing::warn!(
                table,
                tag = %name,
                error = %format!("{error:#}"),
                "bake pin not released; it stays until the next publish"
            ),
        }
    }
    released
}

/// The Gold snapshot a bake shard reads: the one the bake pinned (`…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID`)
/// wherever the table is now, or the current one when the bake names none. A long bake finishes
/// at its pin instead of stopping when Gold moves (root ADR-0310); an expired pin is refused with
/// the catalog's "no longer holds snapshot", which the bake script reads.
///
/// # Errors
/// Refuses a pin that is not a snapshot id, an expired pin, a missing table and a table with no
/// snapshot.
pub async fn load_baked_snapshot(
    catalog: &IcebergRestCatalog,
    table: &str,
    pinned: Option<&str>,
) -> anyhow::Result<lakehouse_infrastructure::IcebergSnapshotManifestList> {
    let loaded = match pinned {
        Some(pinned) => {
            catalog
                .load_snapshot_manifest_list(table, parse_snapshot(pinned)?)
                .await
        }
        None => catalog.load_current_snapshot_manifest_list(table).await,
    };
    loaded
        .with_context(|| format!("failed to resolve the {table} snapshot to bake"))?
        .with_context(|| format!("{table} has no Iceberg snapshot to bake"))
}

/// An Iceberg snapshot id as the manifest stores it (a decimal string).
///
/// # Errors
/// Refuses anything but a decimal 64-bit integer.
pub fn parse_snapshot(snapshot: &str) -> anyhow::Result<i64> {
    snapshot
        .parse::<i64>()
        .with_context(|| format!("Gold snapshot id {snapshot:?} is not an Iceberg snapshot id"))
}

type LocalTags = BTreeMap<String, BTreeMap<String, i64>>;

fn read_local(path: &Path) -> anyhow::Result<LocalTags> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("{} does not hold local pins", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(LocalTags::new()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn write_local(path: &Path, tags: &LocalTags) -> anyhow::Result<()> {
    let body = serde_json::to_vec_pretty(tags).context("failed to serialize local pins")?;
    std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{
        pin, pin_bake, release_bake_pins, release_older, tag_name, tag_order, SnapshotPins,
    };
    use crate::by_pnu_gateway_contract::ByPnuLane;
    use uuid::Uuid;

    const TABLE: &str = "gold.parcel_panel";
    const SNAPSHOT: &str = "999990000000000001";

    fn pins(label: &str) -> (SnapshotPins, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-by-pnu-pins-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        let _ = std::fs::create_dir_all(&root);
        (SnapshotPins::LocalFile(root.join("tags.json")), root)
    }

    #[test]
    fn a_tag_names_its_lane_its_manifest_and_its_publish() -> anyhow::Result<()> {
        let id = Uuid::now_v7();
        let name = tag_name(ByPnuLane::Parcel, "2026-01-02T03:04:05Z", 7, id)?;
        assert_eq!(
            name,
            format!("served-parcel-by-pnu-20260102T030405Z-7-{}", id.simple())
        );
        assert_eq!(tag_order(ByPnuLane::Parcel, &name), Some(id));
        assert_eq!(tag_order(ByPnuLane::Building, &name), None);
        assert!(tag_name(ByPnuLane::Parcel, "yesterday", 7, id).is_err());
        assert!(
            tag_name(ByPnuLane::Parcel, "2026-01-02T03:04:05Z", 7, Uuid::new_v4()).is_err(),
            "a publish id that is not time-ordered was accepted"
        );
        Ok(())
    }

    /// Two publishes of one snapshot in one second get two pins: the one that loses can never
    /// release the winner's by name.
    #[tokio::test]
    async fn two_publishes_of_one_snapshot_in_one_second_get_two_pins() -> anyhow::Result<()> {
        let (pins, root) = pins("same-second");
        let at = "2026-01-01T00:00:00Z";
        let first = pin(&pins, ByPnuLane::Parcel, TABLE, SNAPSHOT, at).await?;
        let second = pin(&pins, ByPnuLane::Parcel, TABLE, SNAPSHOT, at).await?;
        let left = pins.tags(TABLE, "").await?;
        std::fs::remove_dir_all(&root)?;
        assert_ne!(first, second);
        assert_eq!(left.len(), 2);
        Ok(())
    }

    /// The live manifest's pin and every newer one stay: a newer pin is a publish that has not
    /// written its manifest yet, and releasing it would leave that manifest unpinned.
    #[tokio::test]
    async fn release_keeps_the_live_pin_and_every_newer_one() -> anyhow::Result<()> {
        let (pins, root) = pins("ordered");
        let at = "2026-01-01T00:00:00Z";
        let older = pin(&pins, ByPnuLane::Parcel, TABLE, "999990000000000001", at).await?;
        let live = pin(&pins, ByPnuLane::Parcel, TABLE, "999990000000000002", at).await?;
        let running = pin(&pins, ByPnuLane::Parcel, TABLE, "999990000000000003", at).await?;
        // A re-run is the same pin; another snapshot under a taken name is refused.
        pins.create(TABLE, &live, 999_990_000_000_000_002).await?;
        assert!(pins
            .create(TABLE, &live, 999_990_000_000_000_009)
            .await
            .is_err());
        // Another lane's pin, a hand-made tag and a lane-prefixed name without a publish id are
        // not this publish's to release.
        pins.create(TABLE, "served-building-by-pnu-20260101T000000Z-7", 7)
            .await?;
        pins.create(TABLE, "audit-2026", 7).await?;
        pins.create(TABLE, "served-parcel-by-pnu-by-hand", 7)
            .await?;

        let released = release_older(&pins, ByPnuLane::Parcel, TABLE, &live).await;
        let left = pins.tags(TABLE, "").await?;
        std::fs::remove_dir_all(&root)?;

        assert_eq!(released, vec![older]);
        let mut expected = vec![
            "audit-2026".to_owned(),
            "served-building-by-pnu-20260101T000000Z-7".to_owned(),
            "served-parcel-by-pnu-by-hand".to_owned(),
            live,
            running,
        ];
        expected.sort();
        assert_eq!(left.keys().cloned().collect::<Vec<_>>(), expected);
        Ok(())
    }

    /// Root ADR-0338: a bake pins the snapshot it reads under the lane's bake prefix, every shard
    /// again, and a publish of the lane releases every bake pin and nothing else.
    #[tokio::test]
    async fn a_bake_pin_lasts_until_the_lanes_next_publish() -> anyhow::Result<()> {
        let (pins, root) = pins("bake");
        let tag = pin_bake(&pins, ByPnuLane::Parcel, TABLE, SNAPSHOT).await?;
        assert_eq!(tag, format!("baking-parcel-by-pnu-{SNAPSHOT}"));
        assert_eq!(
            pin_bake(&pins, ByPnuLane::Parcel, TABLE, SNAPSHOT).await?,
            tag
        );
        pins.create(TABLE, "baking-building-by-pnu-7", 7).await?;
        let served = pin(
            &pins,
            ByPnuLane::Parcel,
            TABLE,
            SNAPSHOT,
            "2026-01-01T00:00:00Z",
        )
        .await?;

        let released = release_bake_pins(&pins, ByPnuLane::Parcel, TABLE).await;
        let left = pins.tags(TABLE, "").await?;
        std::fs::remove_dir_all(&root)?;

        assert_eq!(released, vec![tag]);
        let mut expected = vec!["baking-building-by-pnu-7".to_owned(), served];
        expected.sort();
        assert_eq!(left.keys().cloned().collect::<Vec<_>>(), expected);
        Ok(())
    }
}
