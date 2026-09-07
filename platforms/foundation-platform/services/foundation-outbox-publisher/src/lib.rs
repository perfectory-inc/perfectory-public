//! Focused reusable command modules for the Foundation outbox-publisher executable.

/// Shared transport plumbing for Silver handoff exporters.
pub mod silver_handoff_io;

/// Headerless HUB apartment-price ZIP to partitioned Silver handoff.
pub mod building_register_apartment_price_silver_export;

/// VWorld cadastral zipped-shapefile to Silver JSONL handoff command.
pub mod vworld_cadastral_shapefile_silver_export;
