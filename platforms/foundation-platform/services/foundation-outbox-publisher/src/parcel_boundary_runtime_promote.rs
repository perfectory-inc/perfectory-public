//! Promotes one validated parcels PostGIS projection as a complete runtime-manifest unit. The CAS
//! function remains the only visibility switch; this command only prepares its immutable release
//! and complete next manifest.
//!
//! `publish-parcel-boundary-postgis` writes `serving_postgis.parcel_boundary_publication` and stops
//! there on purpose: `serving_postgis.parcel_boundary_current` returns rows only for the projection
//! load the *selected release* names, so a load that nothing has promoted is invisible to Martin.
//! This command is what selects one.
//!
//! The procedure itself is [`crate::vector_tile_runtime_promote`], which every unit's promotion
//! shares. What is here is what is parcels about it.

use catalog_domain::vector_tile_feature_filter_properties;

use crate::parcel_boundary_postgis_publish::PARCEL_UNIT_KEY;
use crate::vector_tile_runtime_promote::{EnvNames, RevisionLedger, UnitPromotion};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_CONFIRM";
const DATA_REVISION_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID";
/// The lineage record `publish-parcel-boundary-postgis` registered this revision against — the one
/// the sealed evidence row names (root ADR-0025). Distinct from `SOURCE_RECORD_ENV`, which names
/// the release's own lineage record: one is where the rows came from, the other describes the
/// release. The two may name the same row when the release states exactly the published lineage.
const REVISION_SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_REVISION_SOURCE_RECORD_ID";
const SOURCE_FILE_ASSET_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID";
const EXPECTED_MANIFEST_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID";
const RELEASE_ID_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID";
const MANIFEST_ID_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID";
const TILES_URL_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE";
const PROJECTION_LOAD_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID";

/// The zooms `parcels` tiles are cut over, and the same range `scripts/tiles/martin-dynamic.yaml`
/// declares for this source.
///
/// 14 at the low end because there are tens of millions of parcels: below it a tile holds so many
/// polygons that each degenerates past legibility, and the anchor layers are the honest answer
/// there instead.
const TILE_ZOOM: (i16, i16) = (14, 16);

/// The zooms a client should draw `parcels` at, which is not where the tiles stop.
///
/// 22, because zooming past 16 is exactly when a user is looking at one parcel in detail and the
/// last cut tile overzooms correctly.
const RENDER_ZOOM: (i16, i16) = (14, 22);

/// Runs `promote-parcel-boundary-runtime`.
///
/// # Errors
/// Propagates every refusal in [`crate::vector_tile_runtime_promote::run`].
pub async fn run() -> anyhow::Result<()> {
    crate::vector_tile_runtime_promote::run(&unit()).await
}

fn unit() -> UnitPromotion {
    UnitPromotion {
        unit_key: PARCEL_UNIT_KEY,
        ok_prefix: "parcel-boundary-runtime-promote-ok",
        label: "parcels",
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
        // `publish-parcel-boundary-postgis` registers its revision in
        // `catalog.publication_revision`, scoped to this unit, anchored on the lineage record the
        // sealed evidence names (root ADR-0025) — a parcels publish is built from an Iceberg
        // snapshot, not one collected object, so a Bronze anchor would misname its provenance.
        revision_ledger: RevisionLedger::PublicationRevisionOnSourceRecord(
            REVISION_SOURCE_RECORD_ENV,
        ),
        // The parcel's own identity, which `silver.parcel_boundaries` keys on and
        // `serving_postgis.parcel_boundary_current` exposes for exactly this. It is the feature's
        // own identity, not a claim about anything else (ADR-0024, ADR-0020).
        feature_id_property: "pnu",
        tile_zoom: TILE_ZOOM,
        render_zoom: RENDER_ZOOM,
        // Read from the contract rather than restated here:
        // `catalog_domain::vector_tile_feature_filter_properties` already names `pnu` as the public
        // property of the `parcels` layer.
        feature_filter_properties: vector_tile_feature_filter_properties(PARCEL_UNIT_KEY),
    }
}
