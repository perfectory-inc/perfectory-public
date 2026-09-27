//! Publishes the active parcel boundary source as one immutable static release.
//!
//! The orchestration itself is [`crate::boundary_static_release_publish`], which every boundary
//! unit's static publication shares (ADR-0053, ADR-0110). What is here is what is parcels about it:
//! the publication-unit key, the operator environment-variable prefix, the success label, and the
//! `serving_postgis` view whose extent a bake must cover. That view stores geometry in EPSG:5179, so
//! the shared build-condition query reprojects it to WGS84 before comparing it with the TileJSON
//! bounds.

use crate::boundary_static_release_publish::{self, UnitStaticReleaseSpec};
use crate::parcel_boundary_postgis_publish::PARCEL_UNIT_KEY;

const SPEC: UnitStaticReleaseSpec = UnitStaticReleaseSpec {
    unit_key: PARCEL_UNIT_KEY,
    env_prefix: "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_STATIC_RELEASE",
    display_label: "parcel boundary",
    serving_current_table: "serving_postgis.parcel_boundary_current",
};

/// Runs the production static-release publisher for the parcel boundary unit.
pub async fn run() -> anyhow::Result<()> {
    boundary_static_release_publish::run(&SPEC).await
}
