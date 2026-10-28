//! Root ADR-0323: the patch path of a lakehouse bake, end to end — index the handoff, choose patch
//! or full, bake the neighbourhood, assemble and gate the cumulative patch, upload it create-only
//! and move the manifest by compare-and-swap. The pure parts are in `lakehouse_tile_patch.rs`.

use std::fs::File;
use std::io::{BufWriter, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, ensure, Context as _};
use catalog_domain::static_release_martin_source_id;
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{
    ConditionalWrite, ObjectStorageService as _, ObjectWriteMode, PutObjectRequest, R2ObjectStorage,
};
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;

use super::patch::{self, Manifest, PatchObject, PatchPolicy, Record};
use super::pmtiles_feature_ids::{Archive, Header, COMPRESSION_GZIP, HEADER_BYTES};
use super::{
    bake_inputs, bake_unit, create_only_upload_and_rehash, gate::classify_missing, prepare_handoff,
    read_active_static, record_fold, required_env_value, ActiveStatic, BakeContract, Config,
    ServedSummary, TileDerivativeR2Config,
};

/// Range reads of a base or patch archive: a directory or a few tiles per request.
const READ_CHUNK_BYTES: usize = 256 * 1024;
/// Patch generations a rollback may have left above the manifest's; the next one goes past them.
const MAX_SKIPPED_GENERATIONS: u64 = 1000;

/// The new snapshot's feature index: written beside the work directory's other files, kept under
/// the work root only once the snapshot is served.
pub struct Indexed {
    pending: PathBuf,
    records: Arc<Vec<Record>>,
    pub columns: Vec<String>,
}

impl Indexed {
    /// Keeps the index of a snapshot the tiles now serve, under the inputs that baked them.
    pub fn keep(&self, config: &Config, snapshot: &str, inputs: &str) -> anyhow::Result<()> {
        let path = patch::index_path(&config.work_root, &config.unit_key, snapshot, inputs)?;
        patch::keep_index(&self.pending, &path)
    }

    /// Frees the records; a full bake keeps only the file.
    pub fn without_records(self) -> Self {
        Self {
            records: Arc::new(Vec::new()),
            ..self
        }
    }
}

pub async fn index_handoff(
    summary: &Arc<ServedSummary>,
    handoff: &Path,
    work: &Path,
) -> anyhow::Result<Indexed> {
    let (summary, handoff, pending) = (
        summary.clone(),
        handoff.to_owned(),
        work.join("feature-index.idx"),
    );
    tokio::task::spawn_blocking(move || {
        let (records, columns) = patch::build_index(&summary, &handoff)?;
        patch::write_index(&pending, &records)?;
        Ok(Indexed {
            pending,
            records: Arc::new(records),
            columns,
        })
    })
    .await
    .context("the feature index stopped")?
}

fn full(unit: &str, reason: &str) -> anyhow::Result<bool> {
    println!("lakehouse-tile-bake-patch-skipped unit={unit} reason={reason:?}");
    Ok(false)
}

/// The served manifest of the active base, with the version a compare-and-swap must still find.
async fn read_manifest(
    reader: &R2ObjectStorage,
    policy: &PatchPolicy,
    key: &str,
    base_source_id: &str,
) -> anyhow::Result<Option<(Manifest, String)>> {
    // Absent is the state of a base no patch was published on; a later create-only write still
    // refuses a manifest that appears in between.
    if !reader.object_exists(key).await? {
        return Ok(None);
    }
    let (bytes, e_tag) = reader
        .get_object_bytes_and_e_tag(key)
        .await
        .with_context(|| format!("tile patch manifest {key}"))?;
    Ok(Some((
        Manifest::parse(&bytes, policy, base_source_id)?,
        e_tag,
    )))
}

async fn base_header(reader: &R2ObjectStorage, key: &str) -> anyhow::Result<(Vec<u8>, Header)> {
    let mut base = reader.open_seekable_object(key, READ_CHUNK_BYTES).await?;
    tokio::task::spawn_blocking(move || {
        let mut bytes = vec![0_u8; HEADER_BYTES];
        base.read_exact(&mut bytes)?;
        let header = Header::parse(&bytes)?;
        Ok((bytes, header))
    })
    .await
    .context("the base header read stopped")?
}

/// Runs the patch path when it applies. `Ok(false)` hands the snapshot to the full bake, which
/// makes a new base (ADR-0111 §9); `Ok(true)` means it is served (or rehearsed).
pub async fn try_patch(
    config: &Config,
    contract: &BakeContract,
    policy: &PatchPolicy,
    summary: &Arc<ServedSummary>,
    work: &Path,
    indexed: &Indexed,
) -> anyhow::Result<bool> {
    let unit = config.unit_key.as_str();
    let pool = PgPool::connect(&required_env_value("DATABASE_URL")?).await?;
    let active = read_active_static(&pool, unit).await?;
    let storage = TileDerivativeR2Config::from_env()?;
    let reader = R2ObjectStorage::from_config(storage.reader_config());
    let base_source_id = static_release_martin_source_id(unit, active.release_id);
    let manifest_key = policy.manifest_key(&active.pmtiles_object_key)?;
    let current = read_manifest(&reader, policy, &manifest_key, &base_source_id).await?;
    let served = current
        .as_ref()
        .map_or(active.snapshot.as_str(), |(manifest, _)| {
            manifest.gold_snapshot.as_str()
        });
    ensure!(
        served != summary.canonical_iceberg_snapshot_id,
        "the {unit} tiles already serve Gold snapshot {served}"
    );
    let inputs = bake_inputs(
        &contract.images,
        summary.geometry_srid,
        &active.layer,
        &indexed.columns,
        config.maxzoom_detail,
    );
    let old_path = patch::index_path(&config.work_root, unit, served, &inputs)?;
    if !old_path.exists() {
        return full(
            unit,
            &format!("snapshot {served} has no feature index under these bake inputs"),
        );
    }
    let (template, base) = base_header(&reader, &active.pmtiles_object_key).await?;
    if base.addressed_tiles == 0 || base.tile_compression != COMPRESSION_GZIP {
        return full(
            unit,
            "the base archive does not count its tiles or is not gzip",
        );
    }
    let limit = (policy.max_patched_tile_ratio * base.addressed_tiles as f64) as usize;
    let previous_tiles = current
        .as_ref()
        .and_then(|(manifest, _)| manifest.patch.as_ref())
        .map_or(0, |patch| patch.tiles as usize);
    let records = indexed.records.clone();
    let changes = tokio::task::spawn_blocking(move || {
        anyhow::Ok(patch::diff(&patch::read_index(&old_path)?, &records))
    })
    .await
    .context("the change detection stopped")??;
    let zooms = (active.layer.tile_min_zoom, active.layer.tile_max_zoom);
    let Some(dirty) =
        patch::dirty_tiles(&changes.boxes, zooms, limit.saturating_sub(previous_tiles))
    else {
        return full(
            unit,
            &format!(
                "the patch would pass {limit} tiles, {} of the base",
                policy.max_patched_tile_ratio
            ),
        );
    };
    println!(
        "lakehouse-tile-bake-patch-plan unit={unit} served={served} upserted={} deleted={} dirty_tiles={} previous_tiles={previous_tiles} limit={limit}",
        changes.upserted.len(),
        changes.deleted.len(),
        dirty.len()
    );
    let generation = current
        .as_ref()
        .map_or(0, |(manifest, _)| manifest.generation)
        + 1;
    let publish = Publish {
        config,
        policy,
        writer: R2ObjectStorage::from_config(storage.writer.clone()),
        reader,
        active: &active,
        base_source_id,
        manifest_key,
        current,
        generation,
        summary,
    };
    let patch_object = if changes.is_empty() {
        // Nothing the tiles show changed: the manifest moves to the snapshot, the patch stays.
        publish
            .current
            .as_ref()
            .and_then(|(manifest, _)| manifest.patch.clone())
    } else {
        let built = build_patch(
            config, contract, &publish, &template, work, indexed, &changes, &dirty,
        )
        .await?;
        let Some((path, tiles, empty_tiles)) = built else {
            return full(
                unit,
                "the cumulative patch would pass the contract's share of the base",
            );
        };
        if config.rehearsal {
            println!("lakehouse-tile-bake-patch-rehearsal unit={unit} tiles={tiles} empty_tiles={empty_tiles} bytes={}", std::fs::metadata(&path)?.len());
            return Ok(true);
        }
        Some(publish.upload(&path, tiles, empty_tiles).await?)
    };
    if config.rehearsal {
        println!("lakehouse-tile-bake-patch-rehearsal unit={unit} tiles=0 (no change)");
        return Ok(true);
    }
    publish.manifest(patch_object.clone()).await?;
    // Only after the tiles that contain the edits are served may the overlay forget them.
    record_fold(config, summary.edits_through_change_seq, active.release_id).await?;
    indexed.keep(config, &summary.canonical_iceberg_snapshot_id, &inputs)?;
    println!(
        "lakehouse-tile-bake-patch-ok unit={unit} base_release_id={} manifest_generation={generation} patch={} gold_snapshot={}",
        active.release_id,
        serde_json::to_string(&patch_object)?,
        summary.canonical_iceberg_snapshot_id
    );
    Ok(true)
}

/// Bakes the neighbourhood, assembles the cumulative patch and gates it. `None` when the patch
/// would pass the contract's share of the base.
#[allow(clippy::too_many_arguments)]
async fn build_patch(
    config: &Config,
    contract: &BakeContract,
    publish: &Publish<'_>,
    template: &[u8],
    work: &Path,
    indexed: &Indexed,
    changes: &patch::Changes,
    dirty: &std::collections::BTreeSet<u64>,
) -> anyhow::Result<Option<(PathBuf, u64, u64)>> {
    let layer = &publish.active.layer;
    let dir = work.join("neighbourhood");
    std::fs::create_dir_all(dir.join("tmp"))?;
    let tiles = patch::neighbourhood(dirty, layer.tile_min_zoom)?;
    let (summary, handoff, csv, records, min_zoom) = (
        publish.summary.clone(),
        config.served_handoff.clone(),
        dir.join("served.csv"),
        indexed.records.clone(),
        layer.tile_min_zoom,
    );
    let (columns, ids, kept) = tokio::task::spawn_blocking(move || {
        let out = BufWriter::with_capacity(1 << 20, File::create(&csv)?);
        let csv = patch::NeighbourhoodCsv {
            inner: out,
            index: records,
            min_zoom,
            tiles,
            kept: 0,
        };
        let prepared = prepare_handoff(&summary, &handoff, csv)?;
        let kept = prepared.csv.kept;
        prepared
            .csv
            .inner
            .into_inner()
            .map_err(|error| error.into_error())?
            .sync_all()?;
        anyhow::Ok((prepared.columns, prepared.ids, kept))
    })
    .await
    .context("the neighbourhood reader stopped")??;
    let missing: Vec<&String> = layer
        .properties
        .iter()
        .filter(|name| !columns.contains(name))
        .collect();
    ensure!(
        missing.is_empty(),
        "the served snapshot lacks properties the served layer promises: {missing:?}"
    );
    let neighbourhood = dir.join("unit.pmtiles");
    if kept > 0 {
        bake_unit(
            config,
            &contract.images,
            &dir,
            &columns,
            publish.summary.geometry_srid,
            layer,
        )
        .await?;
    }
    println!(
        "lakehouse-tile-bake-patch-neighbourhood unit={} rows={kept}",
        config.unit_key
    );
    let previous = match publish
        .current
        .as_ref()
        .and_then(|(manifest, _)| manifest.patch.as_ref())
    {
        Some(patch) => {
            let key = publish
                .policy
                .patch_key(&publish.active.pmtiles_object_key, patch.generation)?;
            Some(
                publish
                    .reader
                    .open_seekable_object(&key, READ_CHUNK_BYTES)
                    .await?,
            )
        }
        None => None,
    };
    let (dirty, path, template) = (dirty.clone(), work.join("patch.pmtiles"), template.to_vec());
    let limit = (publish.policy.max_patched_tile_ratio
        * Header::parse(&template)?.addressed_tiles as f64) as usize;
    let written = {
        let path = path.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<(u64, u64)>> {
            let (mut baked, metadata) = if kept > 0 {
                (
                    Some(Archive::open(File::open(&neighbourhood)?)?),
                    patch::template_and_metadata(&neighbourhood)?.1,
                )
            } else {
                (None, patch::empty_metadata()?)
            };
            let mut previous = previous.map(Archive::open).transpose()?;
            let (tiles, empty) = patch::assemble(baked.as_mut(), previous.as_mut(), &dirty)?;
            if tiles.len() > limit {
                return Ok(None);
            }
            let mut out = BufWriter::new(File::create(&path)?);
            super::pmtiles_write::write_archive(&mut out, &template, &metadata, &tiles)?;
            out.into_inner()
                .map_err(|error| error.into_error())?
                .sync_all()?;
            Ok(Some((tiles.len() as u64, empty)))
        })
        .await
        .context("the patch writer stopped")??
    };
    let Some((tile_count, empty)) = written else {
        return Ok(None);
    };
    let gated = {
        let (path, layer, summary, handoff, changes) = (
            path.clone(),
            layer.clone(),
            publish.summary.clone(),
            config.served_handoff.clone(),
            patch::Changes {
                upserted: changes.upserted.clone(),
                deleted: changes.deleted.clone(),
                boxes: Vec::new(),
            },
        );
        tokio::task::spawn_blocking(move || {
            let render_max_zoom = layer.render_max_zoom;
            patch::gate_patch(&path, &layer, &columns, &ids, &changes, |missing| {
                classify_missing(&summary, &handoff, missing, render_max_zoom)
            })
        })
        .await
        .context("the patch gate stopped")??
    };
    println!(
        "lakehouse-tile-bake-patch-gate unit={} maxzoom_tiles={} subpixel={}",
        config.unit_key,
        gated.maxzoom_tiles,
        serde_json::to_string(&gated.subpixel)?
    );
    Ok(Some((path, tile_count, empty)))
}

/// What a patch publish writes to: the base's manifest and its next patch generation.
struct Publish<'a> {
    config: &'a Config,
    policy: &'a PatchPolicy,
    writer: R2ObjectStorage,
    reader: R2ObjectStorage,
    active: &'a ActiveStatic,
    base_source_id: String,
    manifest_key: String,
    current: Option<(Manifest, String)>,
    generation: u64,
    summary: &'a Arc<ServedSummary>,
}

impl Publish<'_> {
    /// Uploads the patch create-only under the first generation no earlier publish (or a rollback)
    /// left an object at, and rehashes it.
    async fn upload(
        &self,
        path: &Path,
        tiles: u64,
        empty_tiles: u64,
    ) -> anyhow::Result<PatchObject> {
        let mut generation = self.generation;
        let base = &self.active.pmtiles_object_key;
        while self
            .reader
            .object_exists(&self.policy.patch_key(base, generation)?)
            .await?
        {
            generation += 1;
            ensure!(
                generation <= self.generation + MAX_SKIPPED_GENERATIONS,
                "{MAX_SKIPPED_GENERATIONS} patch generations above the manifest's already exist"
            );
        }
        let key = self.policy.patch_key(base, generation)?;
        let verified =
            create_only_upload_and_rehash(&self.writer, &self.reader, path, &key).await?;
        Ok(PatchObject {
            generation,
            sha256: verified.checksum_sha256,
            bytes: verified.size_bytes,
            tiles,
            empty_tiles,
        })
    }

    /// Moves the manifest by compare-and-swap over the version read at the start.
    async fn manifest(&self, patch: Option<PatchObject>) -> anyhow::Result<()> {
        let generation = patch.as_ref().map_or(self.generation, |patch| {
            patch.generation.max(self.generation)
        });
        let manifest = Manifest {
            schema_version: self.policy.manifest_schema_version.clone(),
            base_source_id: self.base_source_id.clone(),
            generation,
            gold_snapshot: self.summary.canonical_iceberg_snapshot_id.clone(),
            folded_through_change_seq: self.summary.edits_through_change_seq,
            patch,
        };
        let body = serde_json::to_vec_pretty(&manifest)?;
        let request = |write_mode| PutObjectRequest {
            key: self.manifest_key.clone(),
            body: body.clone(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store".to_owned(),
            write_mode,
            sha256: Some(format!("{:x}", Sha256::digest(&body))),
        };
        match &self.current {
            None => match self
                .writer
                .put_object(request(ObjectWriteMode::CreateOnly))
                .await
            {
                Ok(()) => Ok(()),
                Err(PublishError::ObjectAlreadyExists { .. }) => {
                    bail!(
                        "another publish wrote the {} tile patch manifest first",
                        self.config.unit_key
                    )
                }
                Err(error) => Err(error.into()),
            },
            Some((_, e_tag)) => match self
                .writer
                .put_object_if_match(request(ObjectWriteMode::OverwriteAllowed), e_tag)
                .await?
            {
                ConditionalWrite::Written => Ok(()),
                ConditionalWrite::VersionChanged => {
                    bail!(
                        "the {} tile patch manifest moved since it was read",
                        self.config.unit_key
                    )
                }
            },
        }
    }
}
