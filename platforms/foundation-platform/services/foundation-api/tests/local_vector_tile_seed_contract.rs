//! Contract test for the local vector tile seed manifest.

#![allow(clippy::expect_used)]

use uuid::Uuid;

#[test]
fn local_vector_tile_seed_matches_object_key_manifest_contract() {
    let seed = include_str!("../../../infra/db/seeds/local_vector_tile_manifest.sql");

    assert!(seed.contains("INSERT INTO catalog.vector_tile_manifest"));
    assert!(seed.contains("gold/manifest.json"));
    assert!(seed.contains("'{object_key_prefix}/{z}/{x}/{y}.pbf'"));
    assert!(seed.contains("'parcels'"));
    assert!(seed.contains("235"));
    assert!(seed.contains("265407"));
    let manifest_insert = seed
        .split_once("INSERT INTO catalog.vector_tile_manifest")
        .expect("vector tile manifest INSERT must exist")
        .1;
    let values = manifest_insert
        .split_once("VALUES")
        .expect("manifest INSERT must have VALUES")
        .1
        .split_once("ON CONFLICT")
        .expect("manifest INSERT must end before ON CONFLICT")
        .0;
    let quoted_values: Vec<_> = values
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim().trim_end_matches(',');
            trimmed
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .collect();
    let current_version = quoted_values[1];
    let previous_version = quoted_values[2];

    assert_eq!(current_version, "019d2b87-3fd1-7e3a-8d88-0b72c8741001");
    assert_eq!(previous_version, "019d2b87-3fd1-7e3a-8d88-0b72c8741000");
    assert_ne!(current_version, previous_version);
    Uuid::parse_str(current_version).expect("current manifest version must be a UUID");
    Uuid::parse_str(previous_version).expect("previous manifest version must be a UUID");
    let upsert = manifest_insert
        .split_once("ON CONFLICT")
        .expect("manifest INSERT must have an idempotent upsert")
        .1;
    assert!(upsert.starts_with(" (id) DO UPDATE"));
    assert!(upsert.contains("current_version = EXCLUDED.current_version"));
    assert!(upsert.contains("previous_version = EXCLUDED.previous_version"));
}
