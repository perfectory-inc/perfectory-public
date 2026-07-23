//! Contract tests for provider-neutral object storage keys.

use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};

#[test]
fn object_key_rejects_ambiguous_or_empty_values() {
    assert!(ObjectKey::parse("complexes/2820000000/blueprints/master.pdf").is_ok());
    assert!(ObjectKey::parse("").is_err());
    assert!(ObjectKey::parse("/leading/slash.pdf").is_err());
    assert!(ObjectKey::parse("complexes\\bad\\slash.pdf").is_err());
    assert!(ObjectKey::parse("complexes/../escape.pdf").is_err());
}

#[test]
fn gold_object_keys_reject_semantic_versions_in_physical_paths() {
    for key in [
        "gold/v1/parcels.json",
        "gold/datasets/parcels/version=20260714/part-000001.parquet",
        "gold/industrial-complex/profiles/gold-v2.json",
        "gold/vector-tiles/manifests/manifest.v43.json",
    ] {
        assert!(
            ObjectKey::parse(key).is_err(),
            "semantic data version leaked into Gold object key: {key}"
        );
    }

    assert!(ObjectKey::parse(
        "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json"
    )
    .is_ok());
}

#[test]
fn gold_object_key_prefixes_reject_semantic_version_directories() {
    assert!(ObjectKeyPrefix::parse("gold/v1/parcels/").is_err());
    assert!(ObjectKeyPrefix::parse("gold/datasets/parcels/version=20260714/").is_err());
    assert!(ObjectKeyPrefix::parse(
        "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels/"
    )
    .is_ok());
}
