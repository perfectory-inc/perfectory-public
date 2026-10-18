//! Rolling a pack lane back to its previous pack generation (root ADR-0274 §5).
//!
//! The rollback lever of a lane that serves section packs is the manifest the history holds for
//! an older generation of its packs, not the per-PNU objects (the pack bake does not refresh
//! them). A superseded generation and the manifests naming it are kept for ADR-0242's window
//! after its successor went live; past it the generation is deleted and the way back is a bake
//! from Gold, so a target whose packs are gone is refused here rather than served half.

use anyhow::{ensure, Context};

use crate::by_pnu_section_pack_manifest::SectionPacksState;
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::r2_layout::by_pnu;

/// What `ROLLBACK_TO_PREVIOUS_GENERATION=true` stands for until the history is read.
pub const PREVIOUS_GENERATION: &str = "previous-generation";

/// Whether `back` names the same sections as `live`, each at its generation or an older one, and at
/// least one older: a step back to a previous pack generation.
pub fn older_generation(back: &SectionPacksState, live: &SectionPacksState) -> bool {
    back.sections.len() == live.sections.len()
        && back
            .sections
            .iter()
            .zip(&live.sections)
            .all(|(back, live)| back.name == live.name && back.generation <= live.generation)
        && back
            .sections
            .iter()
            .zip(&live.sections)
            .any(|(back, live)| back.generation < live.generation)
}

/// Refuses a step back to a generation that no longer holds every base pack it served.
///
/// # Errors
/// Returns an error when a section's older generation lost packs, or the listing fails.
pub async fn ensure_still_held(
    store: &ByPnuServingStore,
    back: &SectionPacksState,
    live: &SectionPacksState,
) -> anyhow::Result<()> {
    for (section, newer) in back.sections.iter().zip(&live.sections) {
        if section.generation == newer.generation {
            continue;
        }
        let held = store
            .list_pack_keys(&section.name, section.generation, None)
            .await?
            .len() as u64;
        ensure!(
            held > 0 && held == section.pack_count,
            "section {} generation {} holds {held} of the {} packs it served: past the superseded \
             generation window (root ADR-0242) it is deleted, and the way back is a bake from Gold",
            section.name,
            section.generation,
            section.pack_count
        );
    }
    Ok(())
}

/// The newest manifest the history holds that is a step back to a previous pack generation still
/// held, with the same object patches as the live one.
///
/// # Errors
/// Returns an error when the live manifest serves no packs, or no such manifest is kept.
pub async fn previous_generation_key(
    store: &ByPnuServingStore,
    served: &ServedManifest,
) -> anyhow::Result<String> {
    let lane = store.lane();
    let live = served.section_packs.as_ref().context(
        "the live manifest serves no section packs, so it has no previous pack generation",
    )?;
    let mut keys = store
        .list_all(&by_pnu::manifest_history_prefix(lane)?)
        .await?
        .into_iter()
        .filter(|key| by_pnu::is_manifest_history_key(lane, key))
        .collect::<Vec<_>>();
    // History keys begin with their publish time: the newest sorts last.
    keys.sort();
    for key in keys.into_iter().rev() {
        let Ok(target) = ServedManifest::parse(lane, &store.read_bytes(&key).await?) else {
            continue;
        };
        let Some(back) = target.section_packs.as_ref() else {
            continue;
        };
        if target.patches == served.patches
            && older_generation(back, live)
            && ensure_still_held(store, back, live).await.is_ok()
        {
            return Ok(key);
        }
    }
    anyhow::bail!(
        "the {} manifest history holds no manifest of a previous pack generation that is still \
         kept (root ADR-0242); the way back is a bake from Gold",
        lane.noun()
    )
}
