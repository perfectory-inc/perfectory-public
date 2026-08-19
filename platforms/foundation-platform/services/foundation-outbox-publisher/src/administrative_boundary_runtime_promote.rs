//! Promotes one validated administrative PostGIS projection as a complete runtime-manifest unit.
//! The CAS function remains the only visibility switch; this command only prepares its immutable
//! release and complete next manifest.
//!
//! The procedure itself is [`crate::vector_tile_runtime_promote`], which every unit's promotion
//! shares. What is here is what is administrative about it: the environment-variable names an
//! operator types, the revision ledger this unit's revisions live in, and the layer contract the
//! release publishes.

use std::collections::BTreeMap;

use crate::administrative_boundary_postgis_publish::ADMINISTRATIVE_UNIT_KEY;
use crate::vector_tile_runtime_promote::{EnvNames, RevisionLedger, UnitPromotion};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CONFIRM";
const DATA_REVISION_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID";
const SOURCE_FILE_ASSET_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID";
const EXPECTED_MANIFEST_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID";
const RELEASE_ID_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID";
const MANIFEST_ID_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID";
const TILES_URL_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE";
const PROJECTION_LOAD_ENV: &str =
    "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID";

/// The zoom range `admin` releases have published since `20260724000001`, tile and render alike.
const ZOOM: (i16, i16) = (5, 16);

/// Runs `promote-administrative-boundary-runtime`.
///
/// # Errors
/// Propagates every refusal in [`crate::vector_tile_runtime_promote::run`].
pub async fn run() -> anyhow::Result<()> {
    crate::vector_tile_runtime_promote::run(&unit()).await
}

fn unit() -> UnitPromotion {
    UnitPromotion {
        unit_key: ADMINISTRATIVE_UNIT_KEY,
        ok_prefix: "administrative-boundary-runtime-promote-ok",
        label: "administrative",
        env: EnvNames {
            confirm: CONFIRM_ENV,
            data_revision: DATA_REVISION_ENV,
            canonical_snapshot: CANONICAL_SNAPSHOT_ENV,
            source_record: SOURCE_RECORD_ENV,
            source_file_asset: SOURCE_FILE_ASSET_ENV,
            expected_manifest: EXPECTED_MANIFEST_ENV,
            release_id: RELEASE_ID_ENV,
            manifest_id: MANIFEST_ID_ENV,
            tiles_url: TILES_URL_ENV,
            projection_load: PROJECTION_LOAD_ENV,
        },
        // `catalog.administrative_boundary_revision`: administrative boundaries are the one unit
        // whose revisions are a validated domain fact rather than only a publication of one
        // (`20260731000002`).
        revision_ledger: RevisionLedger::AdministrativeBoundary,
        feature_id_property: "administrative_unit_id",
        tile_zoom: ZOOM,
        render_zoom: ZOOM,
        // Not `catalog_domain::vector_tile_feature_filter_properties`: that contract returns no
        // properties for `admin` today, and widening it changes what every consumer of
        // `VectorTileArtifact::feature_filter_properties` is told. These two are what the `admin`
        // release has published since it existed.
        feature_filter_properties: BTreeMap::from([
            ("canonical_code".to_owned(), "canonical_code".to_owned()),
            ("scope_kind".to_owned(), "scope_kind".to_owned()),
        ]),
    }
}
