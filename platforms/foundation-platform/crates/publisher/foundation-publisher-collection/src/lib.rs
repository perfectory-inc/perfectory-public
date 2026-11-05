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

//! The outbox-publisher national collection lane (root ADR-0200).
//!
//! Bronze collection from the public providers (VWorld, HUB, MOLIT real transactions, public data
//! APIs): plans, ledgers, page and bulk ingest, rate control, provider acquisition and Bronze
//! catalog recovery. It depends on `foundation-publisher-core` and on no other lane.

// The shared modules keep their `crate::` paths in this lane.
#[cfg(test)]
use foundation_publisher_core::test_support;
use foundation_publisher_core::{
    free_space, public_data_control_support, r2_command_support, runtime_environment,
};

pub mod bronze_object_members;
pub mod bronze_object_storage;
pub mod bronze_schema_profile;
pub mod building_hub_bulk_collection_plan;
pub mod building_hub_bulk_ingest;
pub mod bulk_streaming_bronze;
pub mod content_spool;
pub mod ingestion_run_recovery;
pub mod national_data_collection_rollout_approval_check;
pub mod page_collector;
pub mod provider_acquisition_import;
pub mod provider_acquisition_plan;
pub mod provider_lane;
pub mod provider_request_spacing;
pub mod public_data_bronze_lane_orchestrator;
pub mod public_data_bronze_lane_registry;
pub mod public_provider_rate_controller;
pub mod public_provider_rate_policy;
pub mod rt_molit_real_transaction_collect;
pub mod rt_molit_real_transaction_export_ingest;
pub mod vworld_cadastral_silver_shard_export;
pub mod vworld_credentials;
pub mod vworld_dataset_collection_plan;
pub mod vworld_dataset_file_ingest;
pub mod vworld_dataset_file_inventory;
