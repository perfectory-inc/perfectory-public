//! Cargo invalidation boundary for the migration set this package embeds.

// Build scripts must emit Cargo directives on stdout; this is not application logging.
#![allow(clippy::print_stdout)]

fn main() {
    // The sqlx migrate macro tracks the contents of known migration files, but stable
    // Rust cannot make the macro track newly added directory entries. Rebuild
    // whenever that set changes, so an embedded migrator never misses a file.
    println!("cargo:rerun-if-changed=../../../migrations");
}
