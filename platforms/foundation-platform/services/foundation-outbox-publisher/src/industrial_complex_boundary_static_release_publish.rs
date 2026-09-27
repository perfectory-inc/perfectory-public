//! Publishes the active industrial-complex boundary source as one immutable static release.
//!
//! The orchestration itself is [`crate::boundary_static_release_publish`], which every boundary
//! unit's static publication shares (ADR-0053, ADR-0110). What is here is what is
//! industrial-complex about it: the publication-unit key, the operator environment-variable prefix,
//! the success label, and the `serving_postgis` view whose extent a bake must cover.

use crate::boundary_static_release_publish::{self, UnitStaticReleaseSpec};
use crate::industrial_complex_boundary_postgis_publish::COMPLEX_UNIT_KEY;

const SPEC: UnitStaticReleaseSpec = UnitStaticReleaseSpec {
    unit_key: COMPLEX_UNIT_KEY,
    env_prefix: "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE",
    display_label: "industrial complex boundary",
    serving_current_table: "serving_postgis.industrial_complex_boundary_current",
};

/// Runs the production static-release publisher.
pub async fn run() -> anyhow::Result<()> {
    boundary_static_release_publish::run(&SPEC).await
}

/// Runs the destructive local-store experiments used by the disposable boundary proof.
pub async fn run_local_mutation_guard_proof() -> anyhow::Result<()> {
    boundary_static_release_publish::run_local_mutation_guard_proof(&SPEC).await
}
