//! Contract test for the local vector tile seed manifest.

#[test]
fn local_vector_tile_seed_matches_object_key_manifest_contract() {
    let seed = include_str!("../../../infra/db/seeds/local_vector_tile_manifest.sql");

    assert!(seed.contains("INSERT INTO catalog.vector_tile_manifest"));
    assert!(seed.contains("gold/manifest.json"));
    assert!(seed.contains("'{object_key_prefix}/{z}/{x}/{y}.pbf'"));
    assert!(seed.contains("'parcels'"));
    assert!(seed.contains("235"));
    assert!(seed.contains("265407"));
}
