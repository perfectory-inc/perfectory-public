//! The static-release toolchain check on its own (root ADR-0581).
//!
//! `foundation-outbox-publisher verify-static-release-toolchain` runs the same
//! [`static_release_toolchain::verify`]. The Windows CI gate needs only this crate, and building
//! the whole publisher cold on Windows, DuckDB included, took 783-840 s of the gate's 823-898 s
//! (2026-11-18).

use std::time::Duration;

use foundation_publisher_tiles::static_release_toolchain;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    static_release_toolchain::verify(Duration::from_secs(30))
        .await
        .map(|_| ())
}
