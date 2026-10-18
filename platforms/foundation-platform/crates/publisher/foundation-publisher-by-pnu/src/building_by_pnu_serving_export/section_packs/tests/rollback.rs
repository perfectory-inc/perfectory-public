//! The rollback lever of a pack lane is its previous pack generation (root ADR-0274 §5).

use anyhow::Context as _;

use super::{
    changed_row, empty_row, joined, live_answering, object, spark_row, Lane, PNU_A, PNU_B, SNAPSHOT,
};
use crate::building_by_pnu_serving_export::section_packs::{monitor, sections};
use crate::by_pnu_serving_manifest_publish::previous_generation::PREVIOUS_GENERATION;
use crate::by_pnu_serving_manifest_publish::{
    check_rollback, publish, PublishConfig, PublishInput,
};

fn generations(
    lane_state: Option<&crate::by_pnu_section_pack_manifest::SectionPacksState>,
) -> Vec<u64> {
    lane_state
        .map(|packs| {
            packs
                .sections
                .iter()
                .map(|section| section.generation)
                .collect()
        })
        .unwrap_or_default()
}

/// Generation 1 cut over, generation 2 published over it; then the lane steps back to 1, which a
/// preview reads whole first, a dry run names without writing, and the retention window bounds.
#[tokio::test]
async fn a_rollback_serves_the_previous_pack_generation_while_it_is_kept() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("rollback-generation", &rows).await?;
    lane.cut_over(&rows).await?;
    let changed = vec![changed_row()?, empty_row(PNU_B)];
    let documents = vec![sections::DOCUMENTS.to_owned()];
    let rebaked = lane
        .bake_sections(&changed, SNAPSHOT, None, &documents, 2)
        .await?;
    lane.publish(lane.summaries("rebake", &[&rebaked])?, SNAPSHOT, None, None)
        .await?;
    assert_eq!(
        generations(lane.live().await?.section_packs.as_ref()),
        vec![2]
    );
    assert_eq!(
        joined(lane.answer(PNU_A).await?)?,
        object(&changed[0], SNAPSHOT)?
    );

    // The rehearsal's read: a preview serving generation 1 is held to generation 1's packs, not to
    // what the lane serves now.
    let pnus = vec![PNU_A.to_owned(), PNU_B.to_owned()];
    let previous = live_answering(&rows, 200, 0, |body| body).await?;
    let held = monitor::check_at(&lane.store, &previous.uri(), &pnus, 3_000.0, Some(1)).await?;
    assert!(held.passed, "{held:?}");
    let against_live =
        monitor::check_at(&lane.store, &previous.uri(), &pnus, 3_000.0, None).await?;
    assert_eq!(against_live.drifted_from_served, vec![PNU_A.to_owned()]);

    let config = |dry_run| PublishConfig {
        output: lane.output(),
        input: PublishInput::Rollback(PREVIOUS_GENERATION.to_owned()),
        first_publication: false,
        dry_run,
    };
    // The dry run names generation 1 and writes nothing.
    let (key, manifest) = check_rollback(&config(true), &lane.store).await?;
    assert!(key.contains("history"), "{key}");
    assert_eq!(generations(manifest.section_packs.as_ref()), vec![1]);
    assert_eq!(
        generations(lane.live().await?.section_packs.as_ref()),
        vec![2]
    );

    // Past the retention window generation 1 is gone: the step back is refused, never served half.
    let kept = lane.pack_keys(sections::DOCUMENTS, 1, None);
    assert!(!kept.is_empty());
    let saved = kept
        .iter()
        .map(|path| Ok((path.clone(), std::fs::read(path)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (path, _) in &saved {
        std::fs::remove_file(path)?;
    }
    let refused = check_rollback(&config(true), &lane.store)
        .await
        .err()
        .context("a deleted generation was offered")?;
    assert!(format!("{refused:#}").contains("ADR-0242"), "{refused:#}");
    for (path, bytes) in &saved {
        std::fs::write(path, bytes)?;
    }

    // The rollback: generation 1 is served again.
    publish(&config(false), &lane.store, &lane.gateway.uri()).await?;
    assert_eq!(
        generations(lane.live().await?.section_packs.as_ref()),
        vec![1]
    );
    assert_eq!(
        joined(lane.answer(PNU_A).await?)?,
        object(&rows[0], SNAPSHOT)?
    );
    Ok(())
}
