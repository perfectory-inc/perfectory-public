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

//! What every outbox-publisher lane shares (root ADR-0200).
//!
//! Control-plane support, the R2 key layout, the gateway contracts, handoff manifests, lakehouse
//! snapshot scans, the Gold profile store and building link evidence. This crate depends on no lane;
//! a module two lanes need comes down here.

pub mod building_link_evidence;
pub mod by_pnu_gateway_contract;
pub mod free_space;
pub mod handoff_manifest_support;
pub mod industrial_complex_gold_profile_store;
pub mod lakehouse_snapshot_scan;
pub mod profile_gateway_contract;
pub mod public_data_control_support;
pub mod r2_command_support;
pub mod r2_layout;
pub mod runtime_environment;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_support;
