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

//! The outbox-publisher tile and static-release lane (root ADR-0200).
//!
//! The lakehouse tile bake, boundary static-release publication, vector tile runtime promotion,
//! static-release readdressing, the static-release toolchain identity and the tile derivative
//! object storage they share. The publisher executable parses the commands and calls `run` here;
//! this crate depends on `foundation-publisher-core` and on no other lane.

// The shared modules keep their `crate::` paths in this lane.
use foundation_publisher_core::public_data_control_support;
use foundation_publisher_core::r2_layout;
use foundation_publisher_core::runtime_environment;

pub mod boundary_static_release_publish;
pub mod lakehouse_bake_verdict;
pub mod lakehouse_tile_bake;
pub mod static_release_readdress;
pub mod static_release_toolchain;
pub mod static_release_url;
pub mod tile_derivative_object_storage;
pub mod vector_tile_runtime_promote;
