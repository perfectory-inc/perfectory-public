//! The rollback lever of a pack lane is its previous pack state (root ADR-0274 §5): the previous
//! generation, or the live generation without its newest pack patch.

use anyhow::Context as _;

use super::{
    changed_row, empty_row, joined, live_answering, object, spark_row, write_change_set, Lane,
    NEXT_SNAPSHOT, PNU_A, PNU_B, SNAPSHOT,
};
use crate::building_by_pnu_serving_export::section_packs::{monitor, sections};
use crate::by_pnu_serving_manifest_publish::previous_generation::PREVIOUS_PACK_STATE;
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
    let held = monitor::check_at(
        &lane.store,
        &previous.uri(),
        &pnus,
        3_000.0,
        &monitor::Target::Generation(1),
    )
    .await?;
    assert!(held.passed, "{held:?}");
    let against_live = monitor::check_at(
        &lane.store,
        &previous.uri(),
        &pnus,
        3_000.0,
        &monitor::Target::Served,
    )
    .await?;
    assert_eq!(against_live.drifted_from_served, vec![PNU_A.to_owned()]);

    let config = |dry_run| PublishConfig {
        output: lane.output(),
        input: PublishInput::Rollback(PREVIOUS_PACK_STATE.to_owned()),
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

/// A lane that has only ever had one pack generation (the building lane, 2026-10-18): its previous
/// pack state is that generation without its newest patch, which the preview serves as the base.
#[tokio::test]
async fn a_lane_of_one_generation_rolls_back_its_newest_pack_patch() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("rollback-patch", &rows).await?;
    lane.cut_over(&rows).await?;
    let changed = changed_row()?;
    let patch = lane
        .bake(
            std::slice::from_ref(&changed),
            NEXT_SNAPSHOT,
            Some((1, &[PNU_A], &[])),
        )
        .await?;
    let change_set = write_change_set(
        &lane.work,
        (SNAPSHOT, NEXT_SNAPSHOT),
        (0, 1, 0),
        &[PNU_A],
        &[],
    )?;
    lane.publish_with(
        Some(lane.summaries("patch", &[&patch])?),
        NEXT_SNAPSHOT,
        2,
        None,
        Some(change_set),
    )
    .await?;
    let live = lane.live().await?.section_packs.context("packs")?;
    assert_eq!((generations(Some(&live)), live.patches.len()), (vec![1], 1));
    assert_eq!(
        joined(lane.answer(PNU_A).await?)?,
        object(&changed, NEXT_SNAPSHOT)?
    );

    // The preview serves generation 1's base, which is the state the rollback returns to.
    let pnus = vec![PNU_A.to_owned(), PNU_B.to_owned()];
    let base = live_answering(&rows, 200, 0, |body| body).await?;
    let held = monitor::check_at(
        &lane.store,
        &base.uri(),
        &pnus,
        3_000.0,
        &monitor::Target::Generation(1),
    )
    .await?;
    assert!(held.passed, "{held:?}");

    let config = |dry_run| PublishConfig {
        output: lane.output(),
        input: PublishInput::Rollback(PREVIOUS_PACK_STATE.to_owned()),
        first_publication: false,
        dry_run,
    };
    let (_, manifest) = check_rollback(&config(true), &lane.store).await?;
    let back = manifest.section_packs.context("the dry run names packs")?;
    assert_eq!((generations(Some(&back)), back.patches.len()), (vec![1], 0));
    assert_eq!(
        lane.live()
            .await?
            .section_packs
            .context("packs")?
            .patches
            .len(),
        1
    );

    publish(&config(false), &lane.store, &lane.gateway.uri()).await?;
    let served = lane.live().await?.section_packs.context("packs")?;
    assert_eq!(
        (generations(Some(&served)), served.patches.len()),
        (vec![1], 0)
    );
    assert_eq!(
        joined(lane.answer(PNU_A).await?)?,
        object(&rows[0], SNAPSHOT)?
    );
    Ok(())
}

/// The parcel lane's shape (2026-10-18): generation 1 carried a pack patch when generation 2
/// replaced it, so the previous pack state is generation 1 with that patch. The preview serves that
/// recorded state exactly (`?packs=h<name>`) and the rehearsal holds it to the state's own packs.
#[tokio::test]
async fn the_preview_read_is_held_to_the_recorded_state_with_its_pack_patch() -> anyhow::Result<()>
{
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("rollback-recorded", &rows).await?;
    lane.cut_over(&rows).await?;
    let changed = changed_row()?;
    let patch = lane
        .bake(
            std::slice::from_ref(&changed),
            NEXT_SNAPSHOT,
            Some((1, &[PNU_A], &[])),
        )
        .await?;
    let change_set = write_change_set(
        &lane.work,
        (SNAPSHOT, NEXT_SNAPSHOT),
        (0, 1, 0),
        &[PNU_A],
        &[],
    )?;
    lane.publish_with(
        Some(lane.summaries("patch", &[&patch])?),
        NEXT_SNAPSHOT,
        2,
        None,
        Some(change_set),
    )
    .await?;
    let current = vec![changed.clone(), empty_row(PNU_B)];
    let documents = vec![sections::DOCUMENTS.to_owned()];
    let rebaked = lane
        .bake_sections(&current, NEXT_SNAPSHOT, None, &documents, 2)
        .await?;
    lane.publish(
        lane.summaries("rebake", &[&rebaked])?,
        NEXT_SNAPSHOT,
        None,
        None,
    )
    .await?;

    let config = |dry_run| PublishConfig {
        output: lane.output(),
        input: PublishInput::Rollback(PREVIOUS_PACK_STATE.to_owned()),
        first_publication: false,
        dry_run,
    };
    let (key, manifest) = check_rollback(&config(true), &lane.store).await?;
    let back = manifest.section_packs.context("packs")?;
    assert_eq!((generations(Some(&back)), back.patches.len()), (vec![1], 1));
    let name = key
        .rsplit('/')
        .next()
        .and_then(|file| file.strip_suffix(".json"))
        .context("a history file name")?
        .to_owned();

    // A preview answering the recorded state, and only when asked for it by name.
    let base_b = object(&rows[1], SNAPSHOT)?;
    let pnus = vec![PNU_A.to_owned(), PNU_B.to_owned()];
    let target = monitor::Target::recorded(&lane.store, &name).await?;
    let faithful = recorded_preview(
        &name,
        &[
            (PNU_A, object(&changed, NEXT_SNAPSHOT)?),
            (PNU_B, base_b.clone()),
        ],
    )
    .await?;
    let held = monitor::check_at(&lane.store, &faithful.uri(), &pnus, 3_000.0, &target).await?;
    assert!(held.passed, "{held:?}");
    // Generation 1's base alone is not the recorded state: the patched PNU drifts.
    let base_only = recorded_preview(
        &name,
        &[(PNU_A, object(&rows[0], SNAPSHOT)?), (PNU_B, base_b)],
    )
    .await?;
    let drifted = monitor::check_at(&lane.store, &base_only.uri(), &pnus, 3_000.0, &target).await?;
    assert_eq!(drifted.drifted_from_served, vec![PNU_A.to_owned()]);

    publish(&config(false), &lane.store, &lane.gateway.uri()).await?;
    let served = lane.live().await?.section_packs.context("packs")?;
    assert_eq!(
        (generations(Some(&served)), served.patches.len()),
        (vec![1], 1)
    );
    assert_eq!(
        joined(lane.answer(PNU_A).await?)?,
        object(&changed, NEXT_SNAPSHOT)?
    );
    Ok(())
}

/// A preview that answers `answers` only to a request for the recorded state `name`.
async fn recorded_preview(
    name: &str,
    answers: &[(&str, Vec<u8>)],
) -> anyhow::Result<wiremock::MockServer> {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let prefix = &super::LANE.policy()?.request_path.prefix;
    let server = MockServer::start().await;
    for (pnu, body) in answers {
        Mock::given(method("GET"))
            .and(path(format!("{prefix}{pnu}")))
            .and(query_param("packs", format!("h{name}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;
    }
    Ok(server)
}
