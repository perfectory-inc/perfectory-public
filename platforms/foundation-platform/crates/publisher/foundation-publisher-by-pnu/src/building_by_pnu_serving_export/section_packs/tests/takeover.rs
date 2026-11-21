//! The switch of a takeover (root ADR-0564 §6-5): the one full bake of the versions table holds
//! every document to the one the replaced table's packs serve, and refuses any difference, also a
//! served PNU the new table no longer answers.

use super::super::bake::DongProgress;
use super::super::takeover::Takeover;
use super::*;

/// The versions table the lane switches to; the cut-over baked `gold.building_panel`.
const VERSIONS: &str = "gold.building_panel_versions";

impl Lane {
    /// The switch's check over what the lane serves.
    async fn takeover(&self) -> anyhow::Result<Takeover> {
        let served = self.live().await?.section_packs;
        Takeover::needed(LANE, VERSIONS, served, false, &self.store)
            .await?
            .context("the packs were baked from another table")
    }

    /// A base bake of `generation` checked by `takeover`.
    async fn bake_taking_over(
        &self,
        rows: &[JsonMap<String, JsonValue>],
        generation: u64,
        takeover: &Takeover,
        progress: Option<DongProgress>,
    ) -> anyhow::Result<PackExportSummary> {
        let config = BakeConfig {
            lane: LANE,
            output: self.output(),
            generation: Some(generation),
            sections: LANE.section_packs()?.sections.clone(),
            patch: None,
            upserts: None,
            pnu_prefix: None,
            expected_gold_snapshot: None,
            max_concurrency: 4,
            summary_path: self.work.join("unused.json"),
            sample_candidates_per_million: None,
            progress,
        };
        bake::bake(
            &config,
            &self.store,
            &pack_provenance(NEXT_SNAPSHOT),
            rows,
            &Renderer::Building(ApprovedBuildingLinks::default()),
            Some(takeover),
        )
        .await
    }
}

#[tokio::test]
async fn the_switch_refuses_any_document_the_served_packs_do_not_answer() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("takeover", &rows).await?;
    lane.cut_over(&rows).await?;

    // The same content from the new table, under another snapshot: every document answers as served.
    let same = lane
        .bake_taking_over(&rows, 2, &lane.takeover().await?, None)
        .await?;
    let record = same.takeover.context("the switch records its check")?;
    assert_eq!(
        (record.served_table.as_str(), record.compared),
        ("gold.building_panel", 2)
    );

    // A changed document: refused, nothing of the dong written.
    let changed = vec![changed_row()?, empty_row(PNU_B)];
    let refused = lane
        .bake_taking_over(&changed, 3, &lane.takeover().await?, None)
        .await
        .expect_err("a changed document was baked at the switch");
    assert!(format!("{refused:#}").contains(PNU_A), "{refused:#}");
    assert!(lane.pack_keys(sections::DOCUMENTS, 3, None).is_empty());

    // A served PNU the new table no longer answers is a difference too.
    let gone = vec![spark_row()?];
    let refused = lane
        .bake_taking_over(&gone, 4, &lane.takeover().await?, None)
        .await
        .expect_err("a PNU the new table lost was baked");
    assert!(format!("{refused:#}").contains(PNU_B), "{refused:#}");
    assert!(lane.pack_keys(sections::DOCUMENTS, 4, None).is_empty());
    Ok(())
}

#[tokio::test]
async fn the_switch_check_runs_for_another_table_and_refuses_a_patch() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("takeover-needed", &rows).await?;
    lane.cut_over(&rows).await?;
    let served = || async { anyhow::Ok(lane.live().await?.section_packs) };
    // A patch of the new table over the old table's packs.
    assert!(
        Takeover::needed(LANE, VERSIONS, served().await?, true, &lane.store)
            .await
            .is_err()
    );
    // The lane's own table, or nothing served yet: no switch.
    assert!(Takeover::needed(
        LANE,
        "gold.building_panel",
        served().await?,
        false,
        &lane.store
    )
    .await?
    .is_none());
    assert!(Takeover::needed(LANE, VERSIONS, None, false, &lane.store)
        .await?
        .is_none());
    Ok(())
}

/// After the switch publishes, the packs name the lane's table; until then nothing but a whole
/// generation may follow packs of another table: on publish, a patch, a reflect and a section
/// re-baked alone are refused (no change set spans two tables).
#[test]
fn packs_of_another_table_are_followed_only_by_a_whole_generation() -> anyhow::Result<()> {
    let mut state = crate::by_pnu_section_pack_manifest::tests::state(2)?;
    state.gold_table = sections::gold_table(LANE).table_name.to_owned();
    publish::same_table(LANE, &state)?;
    state.gold_table = "gold.building_panel_replaced".to_owned();
    let refused =
        publish::same_table(LANE, &state).expect_err("another table's packs were patched");
    assert!(
        format!("{refused:#}").contains("only a whole generation"),
        "{refused:#}"
    );
    Ok(())
}
