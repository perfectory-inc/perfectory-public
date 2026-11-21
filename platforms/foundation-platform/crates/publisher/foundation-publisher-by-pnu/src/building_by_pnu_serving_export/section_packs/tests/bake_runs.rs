//! Bake runs of the test lane: a base or a patch of chosen sections, and a run that records or
//! resumes its finished dongs (W83c).

use super::super::bake::DongProgress;
use super::*;

impl Lane {
    /// Bakes `sections`: a base of `generation`, or a patch (which names no generation).
    pub(super) async fn bake_sections(
        &self,
        rows: &[JsonMap<String, JsonValue>],
        snapshot: &str,
        patch: Option<(u64, &[&str], &[&str])>,
        sections: &[String],
        generation: u64,
    ) -> anyhow::Result<PackExportSummary> {
        self.bake_with(rows, snapshot, patch, sections, generation, None)
            .await
    }

    /// `bake_sections`, recording finished dongs in `progress` (or resuming them from it).
    pub(super) async fn bake_with(
        &self,
        rows: &[JsonMap<String, JsonValue>],
        snapshot: &str,
        patch: Option<(u64, &[&str], &[&str])>,
        sections: &[String],
        generation: u64,
        progress: Option<DongProgress>,
    ) -> anyhow::Result<PackExportSummary> {
        let config = BakeConfig {
            lane: LANE,
            output: self.output(),
            generation: patch.is_none().then_some(generation),
            sections: sections.to_vec(),
            patch: patch.map(|(number, _, deletes)| PatchTarget {
                patch: number,
                deleted: deletes.iter().map(|pnu| (*pnu).to_owned()).collect(),
            }),
            upserts: patch
                .map(|(_, upserts, _)| upserts.iter().map(|pnu| (*pnu).to_owned()).collect()),
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
            &pack_provenance(snapshot),
            rows,
            &Renderer::Building(ApprovedBuildingLinks::default()),
            None,
        )
        .await
    }
}

/// A run stopped inside a shard resumes at the dong (W83c): a re-run of the same build takes each
/// finished dong's record instead of rendering it and states what the first run did; another build
/// renders again, to the same bytes (create-only reports every pack `reused`).
#[tokio::test]
async fn a_rerun_of_the_same_build_resumes_each_finished_dong() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("resume", &rows).await?;
    let sections = LANE.section_packs()?.sections.clone();
    let dir = lane.work.join("progress");
    let progress = |build: &str| {
        Some(DongProgress {
            dir: dir.clone(),
            build: build.to_owned(),
        })
    };
    let first = lane
        .bake_with(&rows, SNAPSHOT, None, &sections, 1, progress("build-a"))
        .await?;
    let dongs: BTreeSet<&str> = first.packs.iter().map(|pack| pack.unit.as_str()).collect();
    assert_eq!(std::fs::read_dir(&dir)?.count(), dongs.len());
    let shape = |summary: &PackExportSummary| {
        summary
            .packs
            .iter()
            .map(|pack| {
                (
                    pack.key.clone(),
                    pack.sha256.clone(),
                    pack.documents,
                    pack.tombstones,
                )
            })
            .collect::<Vec<_>>()
    };

    let resumed = lane
        .bake_with(&rows, SNAPSHOT, None, &sections, 1, progress("build-a"))
        .await?;
    assert!(resumed.packs.iter().all(|pack| pack.outcome == "resumed"));
    assert_eq!(shape(&resumed), shape(&first));
    assert_eq!(resumed.equality, first.equality);
    assert_eq!(resumed.totals, first.totals);
    assert_eq!(resumed.parts, first.parts);

    // Another build may render otherwise: it renders, and the bytes are the same.
    let rendered = lane
        .bake_with(&rows, SNAPSHOT, None, &sections, 1, progress("build-b"))
        .await?;
    assert!(rendered.packs.iter().all(|pack| pack.outcome == "reused"));
    assert_eq!(shape(&rendered), shape(&first));
    Ok(())
}
