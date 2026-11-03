//! Root ADR-0374: a unit the bake contract lists is born from its first lakehouse bake — its layer,
//! the check that it does not exist yet, and the birth itself.

use super::upload::{bake_and_upload, rehearse, Upload};
use super::*;

/// The one layer of a unit born from its first lakehouse bake, and the lakehouse table its
/// snapshots belong to (`catalog.source_record.source`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewbornUnit {
    pub source_layer: String,
    pub feature_id_property: String,
    pub tile_min_zoom: u8,
    pub tile_max_zoom: u8,
    pub render_min_zoom: u8,
    pub render_max_zoom: u8,
    /// Every tile property besides the id, sorted: the order Catalog returns them in.
    pub feature_filter_properties: Vec<String>,
    pub source_record_source: String,
}

impl NewbornUnit {
    /// The layer exactly as a later bake reads it back from Catalog (`read_active_static`).
    pub fn served_layer(&self) -> ServedLayer {
        ServedLayer {
            source_layer: self.source_layer.clone(),
            feature_id_property: self.feature_id_property.clone(),
            tile_min_zoom: self.tile_min_zoom,
            tile_max_zoom: self.tile_max_zoom,
            render_max_zoom: self.render_max_zoom,
            properties: std::iter::once(self.feature_id_property.clone())
                .chain(self.feature_filter_properties.iter().cloned())
                .collect(),
        }
    }

    pub(super) fn runtime_layer(&self) -> anyhow::Result<RuntimeTileLayer> {
        Ok(RuntimeTileLayer {
            source_layer: self.source_layer.clone(),
            feature_id_property: FeatureIdProperty::new(self.feature_id_property.clone())
                .map_err(anyhow::Error::msg)?,
            tile_min_zoom: self.tile_min_zoom,
            tile_max_zoom: self.tile_max_zoom,
            render_min_zoom: self.render_min_zoom,
            render_max_zoom: self.render_max_zoom,
            feature_filter_properties: self
                .feature_filter_properties
                .iter()
                .map(|name| (name.clone(), name.clone()))
                .collect(),
        })
    }

    pub(super) fn validate(&self, unit: &str) -> anyhow::Result<()> {
        ensure!(
            self.tile_min_zoom <= self.tile_max_zoom
                && self.render_min_zoom <= self.render_max_zoom
                && self.render_max_zoom <= 24,
            "the {unit} layer zooms must be ordered and at most 24"
        );
        ensure!(
            self.feature_filter_properties
                .windows(2)
                .all(|pair| pair[0] < pair[1])
                && !self
                    .feature_filter_properties
                    .contains(&self.feature_id_property),
            "the {unit} layer filter properties are sorted, distinct and exclude the id"
        );
        ensure!(
            self.source_record_source.starts_with("lakehouse:"),
            "the {unit} layer source_record_source names a lakehouse table"
        );
        Ok(())
    }
}

/// Whether Catalog has a publication unit under this key (root ADR-0374: a unit is born with its
/// first release, so an existing unit always has an active one).
pub(super) async fn unit_exists(pool: &PgPool, unit_key: &str) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM catalog.vector_tile_publication_unit WHERE unit_key = $1)",
    )
    .bind(unit_key)
    .fetch_one(pool)
    .await?)
}

/// Root ADR-0374: bakes, gates and uploads a unit's first release like any bake, then registers
/// the unit with it in one Catalog transaction. There is no earlier release to compare with and no
/// build row (a build replaces a release; this one replaces nothing).
#[allow(clippy::too_many_arguments)]
pub(super) async fn bear(
    config: &Config,
    contract: &BakeContract,
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
    work: &Path,
    prepared: Prepared,
    pool: PgPool,
    newborn: &NewbornUnit,
    indexed: Option<&patch_run::Indexed>,
) -> anyhow::Result<()> {
    ensure!(
        summary.feature_id_property == newborn.feature_id_property,
        "the served summary's id property {} is not the {} layer's {}",
        summary.feature_id_property,
        config.unit_key,
        newborn.feature_id_property
    );
    let layer = newborn.served_layer();
    let inputs = bake_inputs(
        &contract.images,
        summary.geometry_srid,
        &layer,
        &prepared.0,
        config.maxzoom_detail,
    );
    if config.rehearsal {
        return rehearse(
            config,
            &contract.images,
            work,
            prepared,
            (&layer, None),
            samples,
            summary,
        )
        .await;
    }
    let release_id = VectorTileReleaseId::new(Uuid::now_v7());
    let (evidence, artifact) = bake_and_upload(
        config,
        &contract.images,
        Upload {
            label: format!("birth-{release_id}"),
            release_id,
            file_asset_id: FileAssetId::new(Uuid::now_v7()),
            kind: "lakehouse_unit_birth",
            build_job_id: None,
            layer: &layer,
            compare_with: None,
            skipped: serde_json::json!({
                "skipped": "a unit born from its first lakehouse bake has no earlier release; the maxzoom id gate checks it (root ADR-0374)"
            }),
        },
        work,
        prepared,
        samples,
        summary.clone(),
    )
    .await?;
    let manifest = PgCatalogUnitOfWork::new(pool)
        .with_runtime_manifest_publication(RuntimeManifestPublicationCapability::enabled())
        .register_lakehouse_tile_unit(RegisterLakehouseTileUnitCommand {
            unit_key: config.unit_key.clone(),
            layer_id: newborn.source_layer.clone(),
            layer: newborn.runtime_layer()?,
            canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(
                summary.canonical_iceberg_snapshot_id.clone(),
            )
            .map_err(anyhow::Error::msg)?,
            source_record_source: newborn.source_record_source.clone(),
            artifact,
            evidence,
            idempotency_key: config.promote_idempotency_key.clone(),
            operator_staff_id: config.operator_staff_id,
        })
        .await?;
    record_fold(config, summary.edits_through_change_seq, release_id).await?;
    if let Some(indexed) = indexed {
        indexed.keep(config, &summary.canonical_iceberg_snapshot_id, &inputs)?;
    }
    println!(
        "lakehouse-tile-bake-ok unit={} born=true release_id={release_id} manifest_generation={} gold_snapshot={} folded_through_change_seq={}",
        config.unit_key,
        manifest.manifest_generation.value(),
        summary.canonical_iceberg_snapshot_id,
        summary.edits_through_change_seq
    );
    Ok(())
}
