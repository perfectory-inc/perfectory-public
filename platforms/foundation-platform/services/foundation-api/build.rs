//! Cargo invalidation boundary for the embedded Foundation migration set.

// Build scripts must emit Cargo directives on stdout; this is not application logging.
#![allow(clippy::print_stdout)]

fn main() {
    // `sqlx::migrate!` tracks the contents of known migration files, but stable
    // Rust cannot make the macro track newly added directory entries. Make the
    // embedded production migrator rebuild whenever that set changes.
    println!("cargo:rerun-if-changed=../../migrations");
}
