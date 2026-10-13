#![allow(
    clippy::nursery,
    clippy::pedantic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    // These modules lived inside the publisher executable, where nothing was public API; they keep
    // that lint posture across the move (root ADR-0200 §4). Documenting the public surface is per-lane
    // follow-up work, not part of a behaviour-preserving move.
    missing_docs
)]
#![cfg_attr(
    test,
    allow(
        clippy::err_expect,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used
    )
)]

//! The outbox-publisher parcel marker anchor lane (root ADR-0200).
//!
//! Anchor artifact export, the anchor PBF artifact build and its manifest promotion, the streaming
//! anchor rebuild, and the parcel boundary handoff they read. The publisher executable parses the
//! commands and calls `run` here; this crate depends on `foundation-publisher-core` and on no other
//! lane.

// The shared modules keep their `crate::` paths in this lane.
use foundation_publisher_core::r2_layout;

pub mod parcel_boundary_handoff;
pub mod parcel_marker_anchor_artifact_export;
pub mod parcel_marker_anchor_pbf_artifact_build;
pub mod parcel_marker_anchor_pbf_manifest_promote;
pub mod parcel_marker_anchor_streaming_rebuild;
