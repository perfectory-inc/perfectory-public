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
    free_space, public_data_control_support, r2_command_support, r2_layout, runtime_environment,
};

pub mod bronze_catalog_recovery_evidence;
pub mod bronze_catalog_recovery_execute;
pub mod bronze_catalog_recovery_manifest;
pub mod bronze_object_members;
pub mod bronze_object_storage;
pub mod bronze_schema_profile;
pub mod building_hub_bulk_collection_plan;
pub mod building_hub_bulk_ingest;
pub mod building_register_ingest;
pub mod building_register_page_count_batch;
pub mod building_register_page_count_plan_writer;
pub mod bulk_streaming_bronze;
pub mod collection_job_requeue;
pub mod content_spool;
pub mod ingestion_run_recovery;
pub mod national_bronze_object_manifest;
pub mod national_data_collection_async;
pub mod national_data_collection_coverage_ledger_check;
pub mod national_data_collection_ledger_execute;
pub mod national_data_collection_ledger_resume;
pub mod national_data_collection_plan_compile;
pub mod national_data_collection_rollout_approval_check;
pub mod national_data_collection_run;
pub mod national_data_collection_scope_writer;
pub mod national_data_collection_shard_manifest_writer;
pub mod national_page_count_plan_writer;
pub mod page_collector;
pub mod page_count_plan_contract;
pub mod pagination_guard;
pub mod provider_acquisition_import;
pub mod provider_acquisition_plan;
pub mod provider_file_bronze_catalog_recovery;
pub mod provider_lane;
pub mod provider_rate_limiter;
pub mod provider_request_spacing;
pub mod public_api_metric_writer;
pub mod public_data_bronze_lane_orchestrator;
pub mod public_data_bronze_lane_registry;
pub mod public_provider_rate_controller;
pub mod public_provider_rate_policy;
pub mod real_transaction_ingest;
pub mod rt_molit_real_transaction_collect;
pub mod rt_molit_real_transaction_export_collection_plan;
pub mod rt_molit_real_transaction_export_ingest;
pub mod vworld_bronze_catalog_recovery;
pub mod vworld_cadastral_ingest;
pub mod vworld_cadastral_silver_export;
pub mod vworld_cadastral_silver_shard_export;
pub mod vworld_cadastral_smoke;
pub mod vworld_credentials;
pub mod vworld_dataset_collection_plan;
pub mod vworld_dataset_file_ingest;
pub mod vworld_dataset_file_inventory;
pub mod vworld_land_register_ingest;
pub mod vworld_ned_attribute_ingest;
pub mod vworld_page_count_batch;
