//! Bake, gate and upload one archive: what a lakehouse bake and a unit's birth (root ADR-0374)
//! share once the handoff is prepared.

use super::*;

/// What the bake produced before anything is written: the archive, the maxzoom id gate (the SHA-256
/// of its id hashes and the sub-pixel ids it allowed to be missing, ADR-0262), and — when the active release came from another oven — the first-release equivalence
/// outcome with the file its whole evidence was written to.
pub(super) struct Gated {
    pub(super) archive: PathBuf,
    pub(super) ids: IdGate,
    pub(super) equivalence: Option<(tile_equivalence::Equivalence, PathBuf)>,
}

/// Bakes and runs every gate, writing nothing but the work directory and the equivalence evidence
/// (named after `label`) beside it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn bake_and_gate(
    config: &Config,
    images: &ImageSet,
    label: &str,
    work: &Path,
    (columns, ids, partitions): Prepared,
    (layer, compare_with): (&ServedLayer, Option<&str>),
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<Gated> {
    let archive = build_archive(
        config,
        images,
        work,
        &columns,
        summary.geometry_srid,
        layer,
        partitions,
    )
    .await?;
    let (id_gate, collector, ids) = {
        let (archive, layer, summary, handoff) = (
            archive.clone(),
            layer.clone(),
            summary.clone(),
            config.served_handoff.clone(),
        );
        let mut collector = SampleCollector::new(samples, &config.unit_key);
        let budget = config.tile_bytes_budget;
        tokio::task::spawn_blocking(move || {
            let largest = gate::tile_bytes_gate(&archive, budget)?;
            let largest: Vec<String> = largest
                .iter()
                .map(|(zoom, (bytes, _))| format!("z{zoom}:{bytes}"))
                .collect();
            println!("lakehouse-tile-bake-largest-tiles {}", largest.join(" "));
            let render_max_zoom = layer.render_max_zoom;
            let gate = gate_archive(
                &archive,
                &layer,
                &columns,
                &ids,
                |missing| classify_missing(&summary, &handoff, missing, render_max_zoom),
                &mut |tile, id| collector.observe(tile, id),
            )?;
            anyhow::Ok((gate, collector, ids))
        })
        .await
        .context("the archive gate stopped")??
    };
    let Some(active_key) = compare_with else {
        return Ok(Gated {
            archive,
            ids: id_gate,
            equivalence: None,
        });
    };
    let storage = TileDerivativeR2Config::from_env()?;
    // The active archive is read by range: its directory and the sampled tiles, never whole.
    let active_reader = R2ObjectStorage::from_config(storage.reader_config())
        .open_seekable_object(active_key, EQUIVALENCE_READ_CHUNK_BYTES)
        .await
        .with_context(|| format!("the active archive {active_key}"))?;
    let (new_archive, layer, samples) = (archive.clone(), layer.clone(), samples.clone());
    let outcome = tokio::task::spawn_blocking(move || {
        tile_equivalence::first_release_gate(
            active_reader,
            File::open(&new_archive)?,
            &samples,
            (&layer.source_layer, &layer.feature_id_property),
            (layer.tile_min_zoom, layer.tile_max_zoom),
            &collector,
            &ids,
        )
    })
    .await
    .context("the equivalence gate stopped")??;
    // Beside the work directory, which is removed: the whole evidence outlives the bake.
    let evidence_file = config.work_root.join(format!(
        "{}-first-release-equivalence-{label}.json",
        config.unit_key
    ));
    std::fs::write(&evidence_file, serde_json::to_vec_pretty(&outcome)?)
        .with_context(|| format!("equivalence evidence {}", evidence_file.display()))?;
    Ok(Gated {
        archive,
        ids: id_gate,
        equivalence: Some((outcome, evidence_file)),
    })
}

/// ADR-0253: bakes and gates the snapshot like a bake, then stops — no build row, no upload, no
/// promotion, no fold — and prints what a person needs before the contract serves the edition.
pub(super) async fn rehearse(
    config: &Config,
    images: &ImageSet,
    work: &Path,
    prepared: Prepared,
    target: (&ServedLayer, Option<&str>),
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<()> {
    let label = format!("rehearsal-{}", Uuid::now_v7());
    let gated = bake_and_gate(
        config,
        images,
        &label,
        work,
        prepared,
        target,
        samples,
        summary.clone(),
    )
    .await?;
    let pmtiles_bytes = std::fs::metadata(&gated.archive)?.len();
    let equivalence = gated.equivalence.as_ref().map(|(outcome, file)| {
        serde_json::json!({
            "passed": outcome.passed(),
            "evidence": file.display().to_string(),
            "counts": outcome.refusal(),
        })
    });
    println!(
        "lakehouse-tile-bake-rehearsal unit={} gold_snapshot={} served_row_count={} pmtiles_bytes={pmtiles_bytes} maxzoom_feature_id_hashes_sha256={} maxzoom_subpixel_ids={} first_release_equivalence={}",
        config.unit_key,
        summary.canonical_iceberg_snapshot_id,
        summary.served_row_count,
        gated.ids.digest,
        serde_json::to_string(&gated.ids.subpixel)?,
        serde_json::to_string(&equivalence)?
    );
    Ok(())
}

/// The active release a bake compares against, unless it is itself a lakehouse bake.
pub(super) fn compared_with(active: &ActiveStatic) -> Option<&str> {
    tile_equivalence::skipped(active.origin)
        .is_none()
        .then_some(active.pmtiles_object_key.as_str())
}

/// Refuses, before anything is baked, a bake whose first-release equivalence gate could not read
/// its own archive: the gate matches features by id at the zooms both archives carry, and a unit
/// with a `low_zoom_generalization` keeps its id only at its maxzoom. 2026-11-12 00:08Z..01:15Z the
/// zoning bake held the whole spark pool for 67 minutes, baked all 15 partitions and then stopped at
/// "a sampled feature has no zone_id".
pub(super) fn ensure_comparable(
    unit: &str,
    compare_with: Option<&str>,
    generalization: Option<&LowZoomGeneralization>,
) -> anyhow::Result<()> {
    ensure!(
        compare_with.is_none() || generalization.is_none(),
        "the active {unit} release was not baked from the lakehouse, so the first-release          equivalence gate would compare it, but {unit}'s low_zoom_generalization keeps no feature          id below its maxzoom for the gate to match; nothing was baked"
    );
    Ok(())
}

/// What one upload is: its preallocated identities, the evidence kind and the build it belongs to
/// (none for a unit's birth, root ADR-0374), and what its archive is gated against.
pub(super) struct Upload<'a> {
    pub(super) label: String,
    pub(super) release_id: VectorTileReleaseId,
    pub(super) file_asset_id: FileAssetId,
    pub(super) kind: &'static str,
    pub(super) build_job_id: Option<VectorTileBuildJobId>,
    pub(super) layer: &'a ServedLayer,
    pub(super) compare_with: Option<&'a str>,
    /// The equivalence evidence when nothing is compared.
    pub(super) skipped: Value,
}

pub(super) async fn bake_and_upload(
    config: &Config,
    images: &ImageSet,
    upload: Upload<'_>,
    work: &Path,
    prepared: Prepared,
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<(BuildEvidenceDigest, ValidatedPmtilesArtifact)> {
    let Gated {
        archive,
        ids: id_gate,
        equivalence,
    } = bake_and_gate(
        config,
        images,
        &upload.label,
        work,
        prepared,
        (upload.layer, upload.compare_with),
        samples,
        summary.clone(),
    )
    .await?;
    let equivalence = match &equivalence {
        Some((outcome, file)) => tile_equivalence::verdict(outcome, &file.display().to_string())?,
        None => upload.skipped,
    };
    let storage = TileDerivativeR2Config::from_env()?;
    let release_id = upload.release_id;
    let object_key = storage.release_key(&config.unit_key, &release_id.to_string())?;
    ensure!(
        object_key == static_release_pmtiles_object_key(&config.unit_key, release_id),
        "the storage prefix does not produce the release-addressed key"
    );
    let reader = R2ObjectStorage::from_config(storage.reader_config());
    let writer = R2ObjectStorage::from_config(storage.writer);
    let verified = create_only_upload_and_rehash(&writer, &reader, &archive, &object_key).await?;
    let source_id = static_release_martin_source_id(&config.unit_key, release_id);
    let evidence = serde_json::to_vec(&serde_json::json!({
        "schema_version": 2,
        "kind": upload.kind,
        "build_job_id": upload.build_job_id.map(|id| id.to_string()),
        "release_id": release_id.to_string(),
        "gold_snapshot": summary.canonical_iceberg_snapshot_id,
        "source_snapshot_id": summary.source_snapshot_id,
        "served_summary_schema": summary.schema_version,
        "edits_through_change_seq": summary.edits_through_change_seq,
        "served_row_count": summary.served_row_count,
        "maxzoom_feature_id_hashes_sha256": id_gate.digest,
        "maxzoom_subpixel_ids": id_gate.subpixel,
        "first_release_equivalence": equivalence,
        "pmtiles_sha256": verified.checksum_sha256,
        "pmtiles_bytes": verified.size_bytes,
    }))?;
    Ok((
        BuildEvidenceDigest::new(format!("{:x}", Sha256::digest(evidence)))
            .map_err(anyhow::Error::msg)?,
        ValidatedPmtilesArtifact {
            release_id,
            file_asset_id: upload.file_asset_id,
            object_key,
            tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
                "{}/{source_id}/{{z}}/{{x}}/{{y}}",
                config.public_tiles_base_url
            ))
            .map_err(anyhow::Error::msg)?,
            checksum: PmtilesChecksum::new(verified.checksum_sha256).map_err(anyhow::Error::msg)?,
            size_bytes: verified.size_bytes,
        },
    ))
}
