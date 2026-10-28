//! Gate (가), the equality gate, and the candidate rate every lane is baked at. Moved out of
//! `tests.rs` unchanged.

use super::*;

/// Gate (가) refuses: packs whose bytes do not read back as the documents they were cut from stop
/// the bake before any write; summaries that do not add up to the Gold row count, that mix
/// snapshots, or that are of a patch do not make passing evidence; and evidence of another
/// snapshot or row count does not open the publish.
#[tokio::test]
async fn the_equality_gate_refuses_every_kind_of_difference() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("equality", &rows).await?;
    let config = BakeConfig {
        lane: LANE,
        output: lane.output(),
        generation: Some(1),
        sections: LANE.section_packs()?.sections.clone(),
        patch: None,
        upserts: None,
        pnu_prefix: None,
        expected_gold_snapshot: None,
        max_concurrency: 1,
        summary_path: lane.work.join("unused.json"),
        sample_candidates_per_million: None,
    };
    let approvals = ApprovedBuildingLinks::default();
    let documents = rows
        .iter()
        .map(|row| {
            building_document::document_with_approvals(&provenance(SNAPSHOT), row, &approvals)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let changed = [
        building_document::document_with_approvals(
            &provenance(SNAPSHOT),
            &changed_row()?,
            &approvals,
        )?,
        documents[1].clone(),
    ];
    let documents = documents
        .iter()
        .map(as_pack)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let changed = changed
        .iter()
        .map(as_pack)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let laid_out = |from: &[PackDocument]| {
        config
            .sections
            .iter()
            .map(|section| {
                Ok(bake::LaidOut {
                    section: section.clone(),
                    unit: UNIT.to_owned(),
                    bytes: bake::lay_out_pack(
                        &config,
                        1,
                        &pack_provenance(SNAPSHOT),
                        section,
                        UNIT,
                        from,
                        &[],
                    )?,
                    documents: documents.clone(),
                    deleted: Vec::new(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()
    };
    let checked = bake::check_round_trip(
        LANE,
        &laid_out(&documents)?,
        &[],
        &documents,
        &[],
        None,
        None,
    )?;
    assert_eq!((checked.compared, checked.equal), (2, 2));
    checked.require_equal(UNIT)?;
    assert!(
        bake::check_round_trip(LANE, &laid_out(&changed)?, &[], &documents, &[], None, None)
            .is_err(),
        "packs of another document were accepted"
    );
    let base = lane.bake(&rows, SNAPSHOT, None).await?;
    let one = lane.summaries("one", &[&base])?;
    let (evidence, _) = gate::read::<gate::EqualityEvidence>(&lane.equality(&one, 2)?)?;
    assert!(evidence.passed);
    assert!(gate::require_equality(LANE, &evidence, 1, SNAPSHOT, 2).is_ok());
    assert!(
        gate::require_equality(LANE, &evidence, 1, NEXT_SNAPSHOT, 2).is_err(),
        "another snapshot"
    );
    assert!(
        gate::require_equality(LANE, &evidence, 1, SNAPSHOT, 3).is_err(),
        "another row count"
    );
    assert!(
        gate::require_equality(LANE, &evidence, 2, SNAPSHOT, 2).is_err(),
        "another generation"
    );
    let (missing, _) = gate::read::<gate::EqualityEvidence>(&lane.equality(&one, 5)?)?;
    assert!(!missing.passed, "a missing shard passed");

    let mut other = base.clone();
    other.gold_iceberg_snapshot_id = NEXT_SNAPSHOT.to_owned();
    let mixed = lane.summaries("mixed", &[&base, &other])?;
    assert!(
        lane.equality(&mixed, 4).is_err(),
        "two snapshots were added up"
    );
    let mut unequal = base.clone();
    unequal.equality.equal = 1;
    let (evidence, _) = gate::read::<gate::EqualityEvidence>(
        &lane.equality(&lane.summaries("unequal", &[&unequal])?, 2)?,
    )?;
    assert_eq!((evidence.compared, evidence.equal), (2, 1));
    assert!(
        !evidence.passed,
        "a summary with an unequal document passed"
    );
    let mut uncompared = base.clone();
    uncompared.equality.compared = 1;
    let partial = lane.summaries("partial", &[&uncompared])?;
    assert!(
        lane.equality(&partial, 2).is_err(),
        "a shard that compared fewer rows was added up"
    );
    let mut patched = base;
    patched.patch = Some(1);
    let patch_dir = lane.summaries("patch-summary", &[&patched])?;
    assert!(
        lane.equality(&patch_dir, 2).is_err(),
        "a patch was taken as a base"
    );
    Ok(())
}

/// Gate (가) holds the member's raw bytes to the object's: a served pack whose member parses to
/// the same document in other bytes (compact, not the object's pretty form) is unequal, because
/// the Worker hands that member out as it is.
#[test]
fn the_equality_gate_compares_the_member_bytes_not_the_parsed_document() -> anyhow::Result<()> {
    let approvals = ApprovedBuildingLinks::default();
    let documents = [spark_row()?, empty_row(PNU_B)]
        .iter()
        .map(|row| {
            building_document::document_with_approvals(&provenance(SNAPSHOT), row, &approvals)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut writer = crate::by_pnu_pack::PackWriter::new(crate::by_pnu_pack::PackIdentity {
        lane: LANE.unit().to_owned(),
        section: sections::DOCUMENTS.to_owned(),
        generation: 1,
        patch: None,
        unit: UNIT.to_owned(),
        gold_table: provenance(SNAPSHOT).table,
        gold_iceberg_snapshot_id: SNAPSHOT.to_owned(),
    })?;
    for document in &documents {
        let mut compact = serde_json::to_vec(document)?;
        compact.push(b'\n');
        assert_ne!(compact, document.to_bytes()?);
        writer.push_document(&document.pnu, &compact)?;
    }
    let served = read::SectionPacksOfUnit {
        name: sections::DOCUMENTS.to_owned(),
        patches: Vec::new(),
        bases: vec![crate::by_pnu_pack::Pack::read(&writer.finish()?)?],
    };
    let answer = joined(read::resolve(
        &read::UnitPacks {
            lane: LANE,
            sections: vec![served.clone()],
        },
        PNU_A,
    )?)?;
    assert_ne!(
        answer,
        documents[0].to_bytes()?,
        "the member was re-serialised"
    );
    let documents = documents
        .iter()
        .map(as_pack)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let checked = bake::check_round_trip(LANE, &[], &[served], &documents, &[], None, None)?;
    assert_eq!((checked.compared, checked.equal), (2, 0));
    assert!(checked.require_equal(UNIT).is_err());
    Ok(())
}

/// A lane too small to draw gate (나)'s sample at the contract's rate is baked at a rate that does
/// (2026-10-14: land-rights, 422,592 PNUs, drew 877 and could never pass); a large lane keeps the
/// contract's rate, and a lane smaller than the sample offers every PNU.
#[test]
fn every_lane_is_baked_at_a_candidate_rate_that_yields_the_sample() -> anyhow::Result<()> {
    let gate_policy = &section_pack_policy()?.cutover_gate;
    let sample = u64::try_from(gate_policy.latency_sample_size)?;
    for documents in [1_u64, 5_000, sample, 422_592, 2_000_000, 39_861_511] {
        let rate = gate::candidate_rate(documents)?;
        assert!(
            rate >= gate_policy.sample_candidates_per_million,
            "{documents}: {rate}"
        );
        assert!(rate <= 1_000_000, "{documents}: {rate}");
        // The expected draw reaches the sample, or takes every PNU of a smaller lane.
        let expected = documents.saturating_mul(rate) / 1_000_000;
        assert!(
            expected >= sample.min(documents),
            "{documents} PNUs at {rate} per million draw {expected}"
        );
    }
    // At the rate the bake records, every document of a one-dong lane of 3,000 is a candidate
    // (no pack is needed to see which documents the bake would record).
    let rows = (0..3_000_u32)
        .map(|n| empty_row(&format!("{UNIT}1{n:04}0000")))
        .collect::<Vec<_>>();
    let documents = rows
        .iter()
        .map(|row| {
            let document = building_document::document_with_approvals(
                &provenance(SNAPSHOT),
                row,
                &ApprovedBuildingLinks::default(),
            )?;
            as_pack(&document)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let all = bake::check_round_trip(
        LANE,
        &[],
        &[],
        &documents,
        &[],
        None,
        Some(gate::candidate_rate(3_000)?),
    )?;
    assert_eq!(all.candidates.len(), documents.len());
    let contract = bake::check_round_trip(LANE, &[], &[], &documents, &[], None, None)?;
    assert!(
        contract.candidates.len() < documents.len() / 10,
        "{}",
        contract.candidates.len()
    );
    Ok(())
}
