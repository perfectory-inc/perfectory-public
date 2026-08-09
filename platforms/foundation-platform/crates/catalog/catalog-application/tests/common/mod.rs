//! Shared Catalog unit-of-work doubles for application use-case tests.
//!
//! `CatalogUnitOfWork` has eight required methods, so a per-file double meant writing the same eight
//! stubs in every test binary. One definition lives here instead, and it grows with the trait rather
//! than once per test file.
//!
//! Every required method returns an error naming itself. A test that reaches a method it did not
//! intend to exercise fails instead of quietly passing on a default value.

// Each test binary uses a subset of these doubles and accessors. `mod common` is private in each
// binary, so anything that binary does not touch reads as dead code even though another binary
// depends on it.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use catalog_application::ports::{
    CatalogUnitOfWork, MarkTileLayerDynamicCommand, PromoteTileLayerStaticCommand,
    PublishedRuntimeManifest, RecordVectorTileBuildResultCommand, RollbackTileLayerSourceCommand,
    StartVectorTileBuildCommand, UpsertIndustrialComplexCommand,
    VectorTileManifestPromotionCommand, VectorTileManifestRollbackCommand,
};
use catalog_domain::{
    static_release_martin_source_id, static_release_pmtiles_object_key, ActiveTileSource,
    CatalogError, ComplexMutation, DynamicPostgisSource, FeatureIdProperty, IndustrialComplex,
    ManifestGeneration, Parcel, ParcelKind, PublicationUnit, RuntimeTileLayer, RuntimeTileLineage,
    ServingGeneration, StaticPmtilesSource, VectorTileManifest, VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, ParcelId, SourceRecordId, StaffId, VectorTileBuildJobId,
    VectorTileDataRevisionId, VectorTileReleaseId, VectorTileRuntimeManifestId,
};
use uuid::Uuid;

/// An error naming the method a test reached but did not mean to.
pub fn unused(method: &str) -> CatalogError {
    CatalogError::Infrastructure(format!("{method} is not exercised by this test"))
}

fn poisoned<T>(_error: T) -> CatalogError {
    CatalogError::Infrastructure("recording mutex poisoned".to_owned())
}

/// Unit of work that records the v2 publication commands it is handed.
#[derive(Default)]
pub struct RecordingUnitOfWork {
    activations: Mutex<Vec<MarkTileLayerDynamicCommand>>,
    started_builds: Mutex<Vec<StartVectorTileBuildCommand>>,
    recorded_results: Mutex<Vec<RecordVectorTileBuildResultCommand>>,
    static_promotions: Mutex<Vec<PromoteTileLayerStaticCommand>>,
    rollbacks: Mutex<Vec<RollbackTileLayerSourceCommand>>,
}

impl RecordingUnitOfWork {
    /// Dynamic activations received, in order.
    pub fn activations(&self) -> Result<Vec<MarkTileLayerDynamicCommand>, CatalogError> {
        Ok(self.activations.lock().map_err(poisoned)?.clone())
    }

    /// Build starts received, in order.
    pub fn started_builds(&self) -> Result<Vec<StartVectorTileBuildCommand>, CatalogError> {
        Ok(self.started_builds.lock().map_err(poisoned)?.clone())
    }

    /// Build results received, in order.
    pub fn recorded_results(
        &self,
    ) -> Result<Vec<RecordVectorTileBuildResultCommand>, CatalogError> {
        Ok(self.recorded_results.lock().map_err(poisoned)?.clone())
    }

    /// Static promotions received, in order.
    pub fn static_promotions(&self) -> Result<Vec<PromoteTileLayerStaticCommand>, CatalogError> {
        Ok(self.static_promotions.lock().map_err(poisoned)?.clone())
    }

    /// Rollbacks received, in order.
    pub fn rollbacks(&self) -> Result<Vec<RollbackTileLayerSourceCommand>, CatalogError> {
        Ok(self.rollbacks.lock().map_err(poisoned)?.clone())
    }
}

/// Unit of work that implements only the required methods, inheriting every v2 default.
///
/// Its purpose is to prove those defaults are errors: a double that inherited a silent success
/// would report a publication that never happened.
pub struct SilentUnitOfWork;

/// Builds the manifest a real transaction would publish for a first activation.
///
/// It goes through the domain validator, so a command the use case let through that could not
/// produce a valid manifest fails here rather than being asserted away.
fn published_manifest(
    command: &MarkTileLayerDynamicCommand,
) -> Result<VectorTileRuntimeManifest, CatalogError> {
    let invalid = CatalogError::InvalidVectorTileRuntimeManifest;
    let unit = PublicationUnit {
        data_revision: command.data_revision,
        serving_generation: ServingGeneration::new(1).map_err(invalid)?,
        active_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        canonical_iceberg_snapshot_id: command.canonical_iceberg_snapshot_id.clone(),
        source: ActiveTileSource::DynamicPostgis(DynamicPostgisSource {
            martin_source_id: command.martin_source_id.clone(),
            tiles_url_template: command.tiles_url_template.clone(),
            cache_policy: "no_store".to_owned(),
        }),
        layers: command.layers.clone(),
        lineage: command.lineage.clone(),
    };
    let manifest = VectorTileRuntimeManifest {
        schema_version: 2,
        current_version: VectorTileRuntimeManifestId::new(Uuid::now_v7()),
        manifest_generation: ManifestGeneration::new(1).map_err(invalid)?,
        refresh_after_seconds: 4,
        published_at: chrono::Utc::now(),
        publication_units: BTreeMap::from([(command.unit_key.clone(), unit)]),
    };
    manifest.validate().map_err(invalid)?;
    Ok(manifest)
}

/// Builds the manifest a real transaction would publish for a static promotion.
///
/// The Martin source name and the object key are derived, not taken from the command, exactly as the
/// transaction will derive them — so a promotion the use case let through with a mismatched URL
/// template fails the domain validator here instead of being asserted away.
fn promoted_manifest(
    command: &PromoteTileLayerStaticCommand,
) -> Result<VectorTileRuntimeManifest, CatalogError> {
    let invalid = CatalogError::InvalidVectorTileRuntimeManifest;
    let unit = PublicationUnit {
        data_revision: VectorTileDataRevisionId::new(Uuid::now_v7()),
        serving_generation: ServingGeneration::new(command.expected_serving_generation.value() + 1)
            .map_err(invalid)?,
        active_release_id: command.release_id,
        canonical_iceberg_snapshot_id: command.frozen_source_snapshot_id.clone(),
        source: ActiveTileSource::StaticPmtiles(StaticPmtilesSource {
            martin_source_id: static_release_martin_source_id(
                &command.unit_key,
                command.release_id,
            ),
            tiles_url_template: command.tiles_url_template.clone(),
            pmtiles_object_key: static_release_pmtiles_object_key(
                &command.unit_key,
                command.release_id,
            ),
            pmtiles_file_asset_id: command.pmtiles_file_asset_id,
            pmtiles_sha256: command.pmtiles_sha256.as_str().to_owned(),
            pmtiles_bytes: command.pmtiles_bytes,
        }),
        layers: BTreeMap::from([("parcels".to_owned(), promoted_layer()?)]),
        lineage: RuntimeTileLineage {
            source_record_id: SourceRecordId::new(Uuid::now_v7()),
            source_file_asset_ids: vec![FileAssetId::new(Uuid::now_v7())],
        },
    };
    let manifest = VectorTileRuntimeManifest {
        schema_version: 2,
        current_version: VectorTileRuntimeManifestId::new(Uuid::now_v7()),
        manifest_generation: ManifestGeneration::new(2).map_err(invalid)?,
        refresh_after_seconds: 4,
        published_at: chrono::Utc::now(),
        publication_units: BTreeMap::from([(command.unit_key.clone(), unit)]),
    };
    manifest.validate().map_err(invalid)?;
    Ok(manifest)
}

fn promoted_layer() -> Result<RuntimeTileLayer, CatalogError> {
    Ok(RuntimeTileLayer {
        source_layer: "parcels".to_owned(),
        feature_id_property: FeatureIdProperty::new("pnu".to_owned())
            .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
        tile_min_zoom: 8,
        tile_max_zoom: 16,
        render_min_zoom: 10,
        render_max_zoom: 22,
        feature_filter_properties: BTreeMap::new(),
    })
}

/// Implements `CatalogUnitOfWork` for one double: the required methods as self-naming errors, plus
/// whatever methods the caller supplies.
///
/// A macro rather than a shared base type because Rust traits have no inheritance. The macro has to
/// generate the whole `impl` including `#[async_trait]`: an invocation *inside* an already-attributed
/// impl block expands after `async_trait` has rewritten its neighbours, leaving raw `async fn` in a
/// desugared impl and seven `E0195` lifetime mismatches.
macro_rules! catalog_unit_of_work_double {
    ($double:ty { $($overrides:tt)* }) => {
#[async_trait]
impl CatalogUnitOfWork for $double {
        async fn create_complex(&self, _complex: &IndustrialComplex) -> Result<(), CatalogError> {
            Err(unused("create_complex"))
        }

        async fn upsert_complexes_by_official_code(
            &self,
            _commands: &[UpsertIndustrialComplexCommand],
        ) -> Result<Vec<IndustrialComplex>, CatalogError> {
            Err(unused("upsert_complexes_by_official_code"))
        }

        async fn update_complex(
            &self,
            _id: ComplexId,
            _expected_version: i64,
            _mutate: ComplexMutation,
        ) -> Result<IndustrialComplex, CatalogError> {
            Err(unused("update_complex"))
        }

        async fn archive_complex(
            &self,
            _id: ComplexId,
            _expected_version: i64,
            _operator_staff_id: StaffId,
            _reason: Option<String>,
            _request_id: Option<String>,
        ) -> Result<IndustrialComplex, CatalogError> {
            Err(unused("archive_complex"))
        }

        async fn update_parcel_kind(
            &self,
            _id: ParcelId,
            _expected_version: i64,
            _new_kind: ParcelKind,
            _applied_by: StaffId,
        ) -> Result<Parcel, CatalogError> {
            Err(unused("update_parcel_kind"))
        }

        async fn rollback_vector_tile_manifest(
            &self,
            _command: VectorTileManifestRollbackCommand,
        ) -> Result<VectorTileManifest, CatalogError> {
            Err(unused("rollback_vector_tile_manifest"))
        }

        async fn promote_vector_tile_manifest(
            &self,
            _command: VectorTileManifestPromotionCommand,
        ) -> Result<VectorTileManifest, CatalogError> {
            Err(unused("promote_vector_tile_manifest"))
        }

        $($overrides)*
}
    };
}

catalog_unit_of_work_double!(RecordingUnitOfWork {
    async fn mark_tile_layer_dynamic(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<PublishedRuntimeManifest, CatalogError> {
        self.activations
            .lock()
            .map_err(poisoned)?
            .push(command.clone());
        // Always `Published`: this double records commands, it does not model the ledger. A double
        // that replayed would let a use-case test pass while the real replay path was untested.
        published_manifest(&command).map(PublishedRuntimeManifest::Published)
    }

    async fn start_vector_tile_build(
        &self,
        command: StartVectorTileBuildCommand,
    ) -> Result<VectorTileBuildJobId, CatalogError> {
        self.started_builds.lock().map_err(poisoned)?.push(command);
        Ok(VectorTileBuildJobId::new(Uuid::now_v7()))
    }

    async fn record_vector_tile_build_result(
        &self,
        command: RecordVectorTileBuildResultCommand,
    ) -> Result<(), CatalogError> {
        self.recorded_results
            .lock()
            .map_err(poisoned)?
            .push(command);
        Ok(())
    }

    async fn promote_tile_layer_static(
        &self,
        command: PromoteTileLayerStaticCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.static_promotions
            .lock()
            .map_err(poisoned)?
            .push(command.clone());
        promoted_manifest(&command)
    }

    async fn rollback_tile_layer_source(
        &self,
        command: RollbackTileLayerSourceCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.rollbacks.lock().map_err(poisoned)?.push(command);
        Err(unused("rollback manifest projection"))
    }
});

// No overrides: the v2 methods keep the trait's default error, which is the thing under test.
catalog_unit_of_work_double!(SilentUnitOfWork {});
