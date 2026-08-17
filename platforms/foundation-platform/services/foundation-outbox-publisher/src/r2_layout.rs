//! Canonical physical R2 addresses for immutable Foundation artifacts.

use anyhow::Context;
use foundation_shared_kernel::ids::VectorTileReleaseId;
use uuid::Uuid;

pub const VECTOR_TILE_ARTIFACT_ROOT: &str = "gold/vector-tiles/artifacts";
pub const VECTOR_TILE_MANIFEST_ROOT: &str = "gold/vector-tiles/manifests";
// The private immutable PMTiles derivative root used to be a constant here. Its only reader was
// `vector_tile_release_key`, which now derives the whole key from
// `catalog_domain::STATIC_RELEASE_OBJECT_ROOT`, so keeping a second copy of the root would only
// reintroduce the drift this change removed.
pub const PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT: &str = "gold/parcel-marker-anchors/artifacts";
pub const INDUSTRIAL_COMPLEX_GOLD_PROFILE_ROOT: &str = "gold/industrial-complex/profiles";
pub const BRONZE_CATALOG_RECOVERY_EVIDENCE_ROOT: &str = "control/evidence/bronze-catalog-recovery";
pub const PARCEL_PUBLICATION_EXECUTION_EVIDENCE_ROOT: &str =
    "control/evidence/parcel-publication/execution";

const BRONZE_CATALOG_RECOVERY_EVIDENCE_KINDS: [&str; 4] = [
    "endpoint-catalog",
    "provider-inventory",
    "r2-inventory",
    "manifests",
];

pub fn vector_tile_artifact_prefix(artifact_id: &str) -> anyhow::Result<String> {
    let artifact_id = parse_artifact_id(artifact_id, "vector tile artifact_id")?;
    Ok(format!("{VECTOR_TILE_ARTIFACT_ROOT}/{artifact_id}"))
}

pub fn vector_tile_manifest_key(manifest_id: &str) -> anyhow::Result<String> {
    let manifest_id = parse_artifact_id(manifest_id, "vector tile manifest_id")?;
    Ok(format!("{VECTOR_TILE_MANIFEST_ROOT}/{manifest_id}.json"))
}

/// Returns the write-once PMTiles object key for one publication unit and release.
///
/// The layout itself comes from `catalog_domain::static_release_pmtiles_object_key`, which the
/// runtime-manifest validator also derives from. Restating it here is how the shipped key and the
/// documented key came to differ while both passed validation.
///
/// The spelling rule comes from `catalog_domain::is_publication_unit_key`, which is also what
/// `catalog.vector_tile_publication_unit_key_check` states in SQL. Restating it here was how this
/// function came to disagree with the column in three directions at once (ADR-0013 남은 부채 1).
pub fn vector_tile_release_key(publication_unit: &str, release_id: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        catalog_domain::is_publication_unit_key(publication_unit),
        "publication unit must be a lower-case identifier"
    );
    let release_id = parse_artifact_id(release_id, "vector tile release_id")?;
    Ok(catalog_domain::static_release_pmtiles_object_key(
        publication_unit,
        VectorTileReleaseId::new(release_id),
    ))
}

pub fn parcel_marker_anchor_artifact_prefix(artifact_id: &str) -> anyhow::Result<String> {
    let artifact_id = parse_artifact_id(artifact_id, "parcel marker anchor artifact_id")?;
    Ok(format!(
        "{PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT}/{artifact_id}"
    ))
}

pub fn industrial_complex_gold_profile_key(artifact_id: &str) -> anyhow::Result<String> {
    let artifact_id =
        parse_artifact_id(artifact_id, "industrial complex gold profile artifact_id")?;
    Ok(format!(
        "{INDUSTRIAL_COMPLEX_GOLD_PROFILE_ROOT}/{artifact_id}.json"
    ))
}

pub fn bronze_catalog_recovery_evidence_key(kind: &str, sha256: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        BRONZE_CATALOG_RECOVERY_EVIDENCE_KINDS.contains(&kind),
        "unsupported recovery evidence kind {kind:?}"
    );
    anyhow::ensure!(
        is_lowercase_sha256(sha256),
        "recovery evidence checksum must be lowercase SHA-256"
    );
    Ok(format!(
        "{BRONZE_CATALOG_RECOVERY_EVIDENCE_ROOT}/{kind}/sha256={sha256}.json"
    ))
}

pub fn is_bronze_catalog_recovery_evidence_key(key: &str) -> bool {
    let Some(relative) = key.strip_prefix(BRONZE_CATALOG_RECOVERY_EVIDENCE_ROOT) else {
        return false;
    };
    let Some(relative) = relative.strip_prefix('/') else {
        return false;
    };
    let mut segments = relative.split('/');
    let (Some(kind), Some(file_name), None) = (segments.next(), segments.next(), segments.next())
    else {
        return false;
    };
    let Some(sha256) = file_name
        .strip_prefix("sha256=")
        .and_then(|value| value.strip_suffix(".json"))
    else {
        return false;
    };
    bronze_catalog_recovery_evidence_key(kind, sha256).is_ok_and(|canonical| canonical == key)
}

pub fn parcel_publication_execution_evidence_key(sha256: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        is_lowercase_sha256(sha256),
        "parcel publication evidence checksum must be lowercase SHA-256"
    );
    Ok(format!(
        "{PARCEL_PUBLICATION_EXECUTION_EVIDENCE_ROOT}/sha256={sha256}.json"
    ))
}

pub fn is_parcel_publication_execution_evidence_key(key: &str) -> bool {
    let Some(sha256) = key
        .strip_prefix(PARCEL_PUBLICATION_EXECUTION_EVIDENCE_ROOT)
        .and_then(|relative| relative.strip_prefix("/sha256="))
        .and_then(|value| value.strip_suffix(".json"))
    else {
        return false;
    };
    parcel_publication_execution_evidence_key(sha256).is_ok_and(|canonical| canonical == key)
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_artifact_id(raw: &str, label: &'static str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(raw).with_context(|| format!("{label} must be a UUID"))
}

#[cfg(test)]
mod tests {
    use super::{
        bronze_catalog_recovery_evidence_key, industrial_complex_gold_profile_key,
        is_bronze_catalog_recovery_evidence_key, is_parcel_publication_execution_evidence_key,
        parcel_marker_anchor_artifact_prefix, parcel_publication_execution_evidence_key,
        vector_tile_artifact_prefix, vector_tile_manifest_key, vector_tile_release_key,
    };

    const ID: &str = "018f0000-0000-7000-8000-000000000001";

    #[test]
    fn compiles_artifact_ids_into_canonical_physical_paths() -> anyhow::Result<()> {
        assert_eq!(
            vector_tile_artifact_prefix(ID)?,
            "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001"
        );
        assert_eq!(
            vector_tile_manifest_key(ID)?,
            "gold/vector-tiles/manifests/018f0000-0000-7000-8000-000000000001.json"
        );
        assert_eq!(
            parcel_marker_anchor_artifact_prefix(ID)?,
            "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001"
        );
        assert_eq!(
            vector_tile_release_key("parcels", ID)?,
            "gold/vector-tiles/releases/parcels-018f0000-0000-7000-8000-000000000001.pmtiles"
        );
        assert_eq!(
            industrial_complex_gold_profile_key(ID)?,
            "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json"
        );
        Ok(())
    }

    #[test]
    fn rejects_dates_and_semantic_versions_as_physical_artifact_ids() {
        for invalid in ["2026-07-14", "v1", "version=1"] {
            assert!(vector_tile_artifact_prefix(invalid).is_err());
            assert!(vector_tile_manifest_key(invalid).is_err());
            assert!(parcel_marker_anchor_artifact_prefix(invalid).is_err());
            assert!(vector_tile_release_key("parcels", invalid).is_err());
            assert!(industrial_complex_gold_profile_key(invalid).is_err());
        }
    }

    #[test]
    fn release_key_rejects_ambiguous_or_noncanonical_units() {
        for unit in ["Parcels", "parcels/other", "parcels,anchors", "../parcels"] {
            assert!(vector_tile_release_key(unit, ID).is_err());
        }
    }

    #[test]
    fn recovery_evidence_paths_require_known_kind_and_content_identity() -> anyhow::Result<()> {
        let checksum = "a".repeat(64);
        let key = bronze_catalog_recovery_evidence_key("manifests", &checksum)?;

        assert!(is_bronze_catalog_recovery_evidence_key(&key));
        assert!(!is_bronze_catalog_recovery_evidence_key(
            "control/evidence/bronze-catalog-recovery/other/sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
        ));
        assert!(!is_bronze_catalog_recovery_evidence_key(
            "control/evidence/bronze-catalog-recovery/manifests/arbitrary.json"
        ));
        Ok(())
    }

    #[test]
    fn parcel_publication_evidence_path_is_content_addressed_and_canonical() -> anyhow::Result<()> {
        let checksum = "b".repeat(64);
        let key = parcel_publication_execution_evidence_key(&checksum)?;

        assert_eq!(
            key,
            format!("control/evidence/parcel-publication/execution/sha256={checksum}.json")
        );
        assert!(is_parcel_publication_execution_evidence_key(&key));
        assert!(!is_parcel_publication_execution_evidence_key(
            "control/evidence/parcel-publication/execution/latest.json"
        ));
        assert!(parcel_publication_execution_evidence_key(&"B".repeat(64)).is_err());
        Ok(())
    }
}
