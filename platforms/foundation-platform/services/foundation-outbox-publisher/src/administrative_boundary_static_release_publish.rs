//! Publishes the active administrative boundary source as one immutable static release.
//!
//! The orchestration itself is [`crate::boundary_static_release_publish`], which every boundary
//! unit's static publication shares (ADR-0053, ADR-0110). What is here is what is administrative
//! about it: the publication-unit key, the operator environment-variable prefix, the success label,
//! and the `serving_postgis` view whose extent a bake must cover.

use crate::administrative_boundary_postgis_publish::ADMINISTRATIVE_UNIT_KEY;
use crate::boundary_static_release_publish::{self, UnitStaticReleaseSpec};

const SPEC: UnitStaticReleaseSpec = UnitStaticReleaseSpec {
    unit_key: ADMINISTRATIVE_UNIT_KEY,
    env_prefix: "FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_STATIC_RELEASE",
    display_label: "administrative boundary",
    serving_current_table: "serving_postgis.administrative_unit_boundary_current",
};

/// Runs the production static-release publisher for the administrative boundary unit.
pub async fn run() -> anyhow::Result<()> {
    boundary_static_release_publish::run(&SPEC).await
}
