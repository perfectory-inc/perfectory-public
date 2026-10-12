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

//! The outbox-publisher by-PNU serving lane (root ADR-0200).
//!
//! Building and parcel by-PNU exports, section packs, manifest publication, serving state, rebase,
//! pins and patches. The publisher executable parses the commands and calls `run` here; this crate
//! depends on `foundation-publisher-core` and on no other lane.

// The shared modules keep their `crate::` paths in this lane.
use foundation_publisher_core::building_link_evidence;
use foundation_publisher_core::by_pnu_gateway_contract;
use foundation_publisher_core::industrial_complex_gold_profile_store;
use foundation_publisher_core::lakehouse_snapshot_scan;
use foundation_publisher_core::public_data_control_support;
use foundation_publisher_core::r2_layout;

pub mod building_by_pnu_serving_export;
pub mod by_pnu_pack;
pub mod by_pnu_section_pack_manifest;
pub mod by_pnu_serving_generations;
pub mod by_pnu_serving_manifest;
pub mod by_pnu_serving_manifest_publish;
pub mod by_pnu_serving_patch_export;
pub mod by_pnu_serving_pins;
pub mod by_pnu_serving_rebase;
pub mod by_pnu_serving_state;
pub mod by_pnu_serving_store;
pub mod pack_lane_document;
pub mod parcel_by_pnu_serving_export;
