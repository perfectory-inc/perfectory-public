//! `retire-<lane>-superseded-pack-generations` (root ADR-0242): deletes the packs of every section
//! generation older than the one the manifest serves, once that one has been live for the
//! contract's `superseded_generation_keep_days`. Until then the older generation is the rollback.
//!
//! "Went live" is read from the manifests themselves: the earliest `published_at_utc` among the
//! live manifest and every replaced one in the manifest history that names the served generation.
//! A generation the manifest serves, or a newer one (a rollback serves an older generation while a
//! newer one waits), is never touched. Every key under a retired generation must be one of its
//! packs or its parts index; one that is not stops the run before anything is deleted.
//!
//! Without `<LANE>_PACK_RETIRE_EXECUTE=true` the command only says what it would delete.

use anyhow::{ensure, Context};
use chrono::{DateTime, Duration, Utc};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::super::optional_env;
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{by_pnu, by_pnu_packs};

const DELETE_CONCURRENCY: usize = 32;

/// What a manifest says that retirement needs: when it was published and which generation each
/// section served.
#[derive(Deserialize)]
struct Served {
    published_at_utc: String,
    #[serde(default)]
    section_packs: Option<ServedPacks>,
}

#[derive(Deserialize)]
struct ServedPacks {
    sections: Vec<ServedSection>,
}

#[derive(Deserialize)]
struct ServedSection {
    name: String,
    generation: u64,
}

impl Served {
    fn serves(&self, section: &str, generation: u64) -> bool {
        self.section_packs.as_ref().is_some_and(|packs| {
            packs
                .sections
                .iter()
                .any(|served| served.name == section && served.generation == generation)
        })
    }
}

/// One superseded section generation: deleted, or kept until its rollback window ends.
#[derive(Debug, Serialize)]
pub struct Superseded {
    pub section: String,
    pub generation: u64,
    pub superseded_by: u64,
    pub successor_live_since: DateTime<Utc>,
    /// `None` when the window has passed and the generation goes.
    pub kept_until: Option<DateTime<Utc>>,
    pub keys: Vec<String>,
}

/// Runs the command for `lane`.
///
/// # Errors
/// Refuses an unreadable manifest or history, a key under a retired generation that is not one of
/// its packs, and a delete the store rejects.
pub async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    let env = |name: &str| optional_env(&lane.env(name));
    let output = ProfileStoreConfig::parse(
        env("OUTPUT_STORAGE_DRIVER")?
            .unwrap_or_else(|| "local".to_owned())
            .as_str(),
        local_root(env("OUTPUT_ROOT")?),
    )?;
    let execute =
        env("PACK_RETIRE_EXECUTE")?.is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let store = ByPnuServingStore::open(lane, &output)?;
    let superseded = plan(&store, Utc::now()).await?;
    if execute {
        retire(&store, &superseded).await?;
    }
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "lane": lane.unit(),
            "executed": execute,
            "generations": superseded.iter().map(|one| serde_json::json!({
                "section": one.section,
                "generation": one.generation,
                "superseded_by": one.superseded_by,
                "successor_live_since": one.successor_live_since,
                "kept_until": one.kept_until,
                "objects": one.keys.len(),
            })).collect::<Vec<_>>(),
        }))?
    );
    Ok(())
}

/// Every section generation older than the served one, with what retiring it at `now` deletes.
///
/// # Errors
/// Refuses an unreadable manifest or history, and a key under a retired generation that is not
/// one of its packs.
pub async fn plan(
    store: &ByPnuServingStore,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<Superseded>> {
    let lane = store.lane();
    let manifest = by_pnu::manifest_key(lane)?;
    if !store.exists(manifest).await? {
        return Ok(Vec::new());
    }
    let live: Served = serde_json::from_slice(&store.read_bytes(manifest).await?)
        .with_context(|| format!("{manifest} is not a manifest"))?;
    let Some(packs) = &live.section_packs else {
        return Ok(Vec::new());
    };
    let mut served = Vec::new();
    for key in store
        .list_all(&by_pnu::manifest_history_prefix(lane)?)
        .await?
    {
        if by_pnu::is_manifest_history_key(lane, &key) {
            let replaced: Served = serde_json::from_slice(&store.read_bytes(&key).await?)
                .with_context(|| format!("{key} is not a manifest"))?;
            served.push(replaced);
        }
    }
    let keep = Duration::days(section_pack_policy()?.superseded_generation_keep_days);
    let mut superseded = Vec::new();
    for section in &packs.sections {
        let live_since = served
            .iter()
            .chain(std::iter::once(&live))
            .filter(|manifest| manifest.serves(&section.name, section.generation))
            .map(|manifest| {
                DateTime::parse_from_rfc3339(&manifest.published_at_utc)
                    .map(|at| at.with_timezone(&Utc))
                    .with_context(|| format!("{:?} is not a UTC time", manifest.published_at_utc))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .min()
            .context("the live manifest names a generation no manifest published")?;
        let (generations, _) = store.list_pack_numbers(&section.name).await?;
        for generation in generations
            .into_iter()
            .filter(|older| *older < section.generation)
        {
            let kept_until = (now < live_since + keep).then_some(live_since + keep);
            let keys = if kept_until.is_some() {
                Vec::new()
            } else {
                generation_keys(store, lane, &section.name, generation).await?
            };
            superseded.push(Superseded {
                section: section.name.clone(),
                generation,
                superseded_by: section.generation,
                successor_live_since: live_since,
                kept_until,
                keys,
            });
        }
    }
    Ok(superseded)
}

/// Every key under one section generation, each one of its packs (base or patch) or its parts
/// index: anything else there stops the retirement before anything is deleted.
async fn generation_keys(
    store: &ByPnuServingStore,
    lane: ByPnuLane,
    section: &str,
    generation: u64,
) -> anyhow::Result<Vec<String>> {
    let directory = by_pnu_packs::directory(lane, section, generation, None)?;
    let parts = by_pnu_packs::parts_index_key(lane, section, generation)?;
    let keys = store.list_all(&directory).await?;
    for key in &keys {
        ensure!(
            *key == parts
                || by_pnu_packs::parse_pack_key(lane, key).is_some_and(|parsed| {
                    parsed.section == section && parsed.generation == generation
                }),
            "{key} under {directory} is not a pack of {section} generation {generation}; nothing \
             was deleted"
        );
    }
    Ok(keys)
}

/// Deletes what `plan` found past its window.
async fn retire(store: &ByPnuServingStore, superseded: &[Superseded]) -> anyhow::Result<()> {
    // Owned keys: a stream borrowing them would make the command's future not `Send`.
    let keys = superseded
        .iter()
        .filter(|one| one.kept_until.is_none())
        .flat_map(|one| one.keys.clone())
        .collect::<Vec<_>>();
    let results = stream::iter(keys)
        .map(|key| async move { store.delete(&key).await })
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    match results.into_iter().find_map(Result::err) {
        Some(first) => Err(first.context("a delete failed; rerun to finish the retirement")),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LANE_NAME: &str = "parcel";
    const SECTION: &str = "documents";
    const UNIT: &str = "9999900000";

    fn lane() -> anyhow::Result<ByPnuLane> {
        ByPnuLane::all()?
            .into_iter()
            .find(|lane| lane.name() == LANE_NAME)
            .context("no parcel lane")
    }

    fn manifest(published: &str, generation: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "published_at_utc": published,
            "section_packs": {"sections": [{"name": SECTION, "generation": generation}]},
        }))
        .unwrap_or_default()
    }

    fn put(root: &std::path::Path, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().context("no parent")?)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }

    /// A served g2 that went live on 2026-01-10 over g1 (two history manifests), with g1's base,
    /// patch and parts index and g3 baked but not yet published.
    fn lane_root(label: &str) -> anyhow::Result<(std::path::PathBuf, ByPnuServingStore)> {
        let lane = lane()?;
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-retire-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        let history = by_pnu::manifest_history_prefix(lane)?;
        put(
            &root,
            by_pnu::manifest_key(lane)?,
            &manifest("2026-01-12T00:00:00Z", 2),
        )?;
        put(
            &root,
            &format!("{history}20260110T000000Z-{}.json", "a".repeat(64)),
            &manifest("2026-01-10T00:00:00Z", 2),
        )?;
        put(
            &root,
            &format!("{history}20260101T000000Z-{}.json", "b".repeat(64)),
            &manifest("2026-01-01T00:00:00Z", 1),
        )?;
        for (generation, patch) in [(1, None), (1, Some(1)), (2, None), (3, None)] {
            put(
                &root,
                &by_pnu_packs::pack_key(lane, SECTION, generation, patch, UNIT)?,
                b"pack",
            )?;
        }
        put(
            &root,
            &by_pnu_packs::parts_index_key(lane, SECTION, 1)?,
            b"{}",
        )?;
        let store =
            ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
        Ok((root, store))
    }

    /// The publisher binary boxes every command's future as `Send` (`main.rs`); a stream that
    /// borrows across an await breaks that only there, so it is held here too.
    #[test]
    fn the_command_future_is_send() -> anyhow::Result<()> {
        fn send<T: Send>(_: &T) {}
        let future = run(lane()?);
        send(&future);
        drop(future);
        Ok(())
    }

    fn at(text: &str) -> anyhow::Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(text)?.with_timezone(&Utc))
    }

    #[tokio::test]
    async fn a_superseded_generation_is_kept_for_the_window_then_deleted_whole(
    ) -> anyhow::Result<()> {
        let (root, store) = lane_root("window")?;
        let lane = store.lane();
        let keep = section_pack_policy()?.superseded_generation_keep_days;
        let went_live = at("2026-01-10T00:00:00Z")?;

        // Inside the window: g1 is the rollback, nothing goes.
        let early = plan(
            &store,
            went_live + Duration::days(keep) - Duration::seconds(1),
        )
        .await?;
        assert_eq!(early.len(), 1);
        assert_eq!((early[0].generation, early[0].superseded_by), (1, 2));
        assert_eq!(
            early[0].successor_live_since, went_live,
            "the first manifest naming g2"
        );
        assert!(early[0].kept_until.is_some() && early[0].keys.is_empty());

        // Past it: g1's base, its patch and its parts index go; g2 (served) and g3 (newer) stay.
        let late = plan(&store, went_live + Duration::days(keep)).await?;
        assert_eq!(late[0].kept_until, None);
        assert_eq!(late[0].keys.len(), 3);
        retire(&store, &late).await?;
        let left = store
            .list_all(&format!("{}/{SECTION}/", lane.section_packs()?.root))
            .await?;
        assert_eq!(left.len(), 2, "{left:?}");
        assert!(left.iter().all(|key| !key.contains("/g1/")), "{left:?}");
        assert!(store.exists(by_pnu::manifest_key(lane)?).await?);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_foreign_key_under_a_superseded_generation_stops_everything() -> anyhow::Result<()> {
        let (root, store) = lane_root("foreign")?;
        let lane = store.lane();
        let directory = by_pnu_packs::directory(lane, SECTION, 1, None)?;
        put(&root, &format!("{directory}notes.txt"), b"not a pack")?;
        let refused = plan(&store, at("2027-01-01T00:00:00Z")?)
            .await
            .err()
            .context("a foreign key was planned for deletion")?;
        assert!(refused.to_string().contains("notes.txt"), "{refused:#}");
        assert_eq!(
            store.list_all(&directory).await?.len(),
            4,
            "nothing was deleted"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_rollback_to_an_older_generation_never_deletes_the_newer_one() -> anyhow::Result<()> {
        let (root, store) = lane_root("rollback")?;
        let lane = store.lane();
        put(
            &root,
            by_pnu::manifest_key(lane)?,
            &manifest("2026-02-01T00:00:00Z", 1),
        )?;
        let planned = plan(&store, at("2027-01-01T00:00:00Z")?).await?;
        assert!(
            planned.is_empty(),
            "nothing is older than the served g1: {planned:?}"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
