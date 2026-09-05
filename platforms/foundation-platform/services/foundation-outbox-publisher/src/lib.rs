//! Focused reusable command modules for the Foundation outbox-publisher executable.

/// Shared transport plumbing for Silver handoff exporters.
pub mod silver_handoff_io;

/// VWorld cadastral zipped-shapefile to Silver JSONL handoff command.
pub mod vworld_cadastral_shapefile_silver_export;
