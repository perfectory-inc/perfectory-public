//! Promotes one validated industrial-complex PostGIS projection as a complete runtime-manifest
//! unit. The CAS function remains the only visibility switch; this command only prepares its
//! immutable release and complete next manifest.
//!
//! `publish-industrial-complex-boundary-postgis` writes
//! `serving_postgis.industrial_complex_boundary_publication` and stops there on purpose:
//! `serving_postgis.industrial_complex_boundary_current` returns rows only for the projection load
//! the *selected release* names, so a load that nothing has promoted is invisible to Martin. This
//! command is what selects one.
//!
//! The procedure itself is [`crate::vector_tile_runtime_promote`], which every unit's promotion
//! shares. What is here is what is industrial-complex about it.

use catalog_domain::vector_tile_feature_filter_properties;

use crate::industrial_complex_boundary_postgis_publish::COMPLEX_UNIT_KEY;
use crate::vector_tile_runtime_promote::{EnvNames, RevisionLedger, UnitPromotion};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_CONFIRM";
const DATA_REVISION_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID";
/// The collected object `publish-industrial-complex-boundary-postgis` registered this revision
/// against (root ADR-0046). Distinct from `SOURCE_RECORD_ENV`, which names the release's own
/// lineage record: one is the file the polygons came from, the other describes the release.
const BRONZE_OBJECT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_BRONZE_OBJECT_ID";
const SOURCE_FILE_ASSET_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID";
const EXPECTED_MANIFEST_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID";
const RELEASE_ID_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID";
const MANIFEST_ID_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID";
const TILES_URL_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE";
const PROJECTION_LOAD_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID";

/// The zooms `complex` tiles are cut over, and the same range `scripts/tiles/martin-dynamic.yaml`
/// declares for this source.
///
/// Wider at the low end than `parcels` (14) and one step narrower than `admin` (5), because the
/// features are neither. There are 1,343 designation polygons in the whole country, so one tile at
/// the minimum holds every complex in view rather than thousands of them, and the median polygon is
/// still several tile units across there. Below it the polygon degenerates to a dot and a marker
/// layer is the honest answer instead.
const TILE_ZOOM: (i16, i16) = (6, 16);

/// The zooms a client should draw `complex` at, which is not where the tiles stop.
///
/// 22, as `parcels` and `parcel_anchor` already publish, because zooming past 16 is exactly when a
/// user is looking at one complex in detail and the last cut tile overzooms correctly. `admin`
/// stops at 16 for both; that is not a precedent this layer should copy, because a boundary that
/// disappears when you zoom into it is the one behaviour a designation boundary must not have.
const RENDER_ZOOM: (i16, i16) = (6, 22);

/// Runs `promote-industrial-complex-boundary-runtime`.
///
/// # Errors
/// Propagates every refusal in [`crate::vector_tile_runtime_promote::run`].
pub async fn run() -> anyhow::Result<()> {
    crate::vector_tile_runtime_promote::run(&unit()).await
}

fn unit() -> UnitPromotion {
    UnitPromotion {
        unit_key: COMPLEX_UNIT_KEY,
        ok_prefix: "industrial-complex-boundary-runtime-promote-ok",
        label: "industrial complex",
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
        // `publish-industrial-complex-boundary-postgis` registers its revision in
        // `catalog.publication_revision`, scoped to this unit, and never in the administrative
        // ledger: a designation boundary asserts nothing about an administrative boundary. That
        // revision names the Bronze object the polygons were decoded from, so this promotion is
        // checked against the same object rather than against a record of one (root ADR-0046).
        revision_ledger: RevisionLedger::PublicationRevisionOnBronzeObject(BRONZE_OBJECT_ENV),
        // The lakehouse id, which is what `silver.industrial_complex_boundaries` and every Gold
        // artifact key on, and the column `serving_postgis.industrial_complex_boundary_current`
        // exposes for exactly this. It is the feature's own identity, not a claim about anything
        // else (ADR-0024).
        feature_id_property: "complex_id",
        tile_zoom: TILE_ZOOM,
        render_zoom: RENDER_ZOOM,
        // Read from the contract rather than restated here.
        // `catalog_domain::vector_tile_feature_filter_properties` already names
        // `official_complex_code` as the public property of the `complex` layer, and a second list
        // in this file would be a copy that a change to the contract could not reach.
        feature_filter_properties: vector_tile_feature_filter_properties(COMPLEX_UNIT_KEY),
    }
}
