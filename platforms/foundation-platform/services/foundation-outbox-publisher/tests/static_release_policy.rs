//! Focused offline tests of the real static release modules, without linking the whole CLI.
// Match the CLI's lint context for the source modules included below.
#![allow(
    dead_code,
    clippy::nursery,
    clippy::pedantic,
    clippy::err_expect,
    clippy::panic,
    clippy::print_stderr,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout
)]

#[path = "../src/building_by_pnu_gateway_contract.rs"]
mod building_by_pnu_gateway_contract;
#[path = "../src/parcel_by_pnu_gateway_contract.rs"]
mod parcel_by_pnu_gateway_contract;
#[path = "../src/profile_gateway_contract.rs"]
mod profile_gateway_contract;
#[path = "../src/public_data_control_support.rs"]
mod public_data_control_support;
#[path = "../src/r2_layout.rs"]
mod r2_layout;
#[path = "../src/runtime_environment.rs"]
mod runtime_environment;
#[path = "../src/static_release_readdress.rs"]
mod static_release_readdress;
#[path = "../src/static_release_url.rs"]
mod static_release_url;
#[path = "../src/tile_derivative_object_storage.rs"]
mod tile_derivative_object_storage;
#[path = "../src/vector_tile_runtime_promote.rs"]
mod vector_tile_runtime_promote;
