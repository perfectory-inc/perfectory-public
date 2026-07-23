//! Runs the proof decoder's own unit tests inside Foundation's authoritative Cargo test graph.

#![allow(
    dead_code,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::unwrap_used
)]

// Keep the executable decoder as the single source: Cargo compiles this exact file
// with `cfg(test)`, so its parser/geometry tests cannot silently fall outside
// `cargo xtask verify foundation`.
#[path = "../../../../../scripts/tiles/mvt_assert.rs"]
mod mvt_assert;
