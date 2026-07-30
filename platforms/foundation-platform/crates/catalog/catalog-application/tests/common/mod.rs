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
    CatalogUnitOfWork, MarkTileLayerDynamicCommand, RecordVectorTileBuildResultCommand,
    StartVectorTileBuildCommand, UpsertIndustrialComplexCommand,
    VectorTileManifestPromotionCommand, VectorTileManifestRollbackCommand,
};
use catalog_domain::{
    ActiveTileSource, CatalogError, ComplexMutation, DynamicPostgisSource, IndustrialComplex,
    ManifestGeneration, Parcel, ParcelKind, PublicationUnit, ServingGeneration, VectorTileManifest,
    VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::{
    ComplexId, ParcelId, StaffId, VectorTileBuildJobId, VectorTileReleaseId,
    VectorTileRuntimeManifestId,
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
            postgis_projection_revision: command.postgis_projection_revision,
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
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.activations
            .lock()
            .map_err(poisoned)?
            .push(command.clone());
        published_manifest(&command)
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
});

// No overrides: the v2 methods keep the trait's default error, which is the thing under test.
catalog_unit_of_work_double!(SilentUnitOfWork {});
