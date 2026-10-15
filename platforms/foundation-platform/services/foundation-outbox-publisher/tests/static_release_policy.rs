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

// The shared modules are a library crate now (root ADR-0200); the publisher modules below keep
// reaching them through their `crate::` paths.
use foundation_publisher_core::{public_data_control_support, r2_layout, runtime_environment};
#[path = "../src/static_release_readdress.rs"]
mod static_release_readdress;
#[path = "../src/static_release_url.rs"]
mod static_release_url;
#[path = "../src/tile_derivative_object_storage.rs"]
mod tile_derivative_object_storage;
#[path = "../src/vector_tile_runtime_promote.rs"]
mod vector_tile_runtime_promote;
