//! The Gold profile document one industrial complex publishes to object storage.
//!
//! The document is a pure function of the Gold snapshot it was read from and the row it carries.
//! No wall clock, no run id, no address: a re-export of the same snapshot produces byte-identical
//! objects at the same key, so a create-only collision is an idempotent re-run rather than a
//! conflict, and moving the bucket does not invalidate the artifact.

use anyhow::Context;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::r2_layout::industrial_complex_gold_profile_key;

/// Wire schema version of the published profile document.
pub(super) const PROFILE_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_gold_profile.v1";

/// One profile artifact, ready to be written create-only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProfileArtifact {
    /// Industrial complex the profile describes.
    pub(super) complex_id: String,
    /// Immutable artifact identity, also the pointer's `current_version`.
    pub(super) artifact_id: Uuid,
    /// Provider-neutral object key the artifact is written to.
    pub(super) object_key: String,
    /// Exact bytes written to object storage.
    pub(super) body: Vec<u8>,
    /// SHA-256 of `body`.
    pub(super) checksum_sha256: String,
}

/// Which Gold snapshot a profile artifact was derived from.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct GoldSnapshotProvenance {
    /// Lakehouse table the rows were read from.
    pub(super) table: String,
    /// Iceberg snapshot of that table which this artifact represents.
    pub(super) iceberg_snapshot_id: String,
    /// Table metadata document the snapshot was resolved from.
    pub(super) metadata_location: String,
    /// Manifest list every scanned data file was reached through.
    pub(super) manifest_list_location: String,
}

#[derive(Debug, Serialize)]
struct ProfileDocument<'a> {
    schema_version: &'static str,
    artifact_id: Uuid,
    complex_id: &'a str,
    source: &'a GoldSnapshotProvenance,
    attributes: &'a JsonMap<String, JsonValue>,
}

/// Builds the immutable profile artifact for one Gold catalog row.
pub(super) fn build(
    provenance: &GoldSnapshotProvenance,
    attributes: &JsonMap<String, JsonValue>,
) -> anyhow::Result<ProfileArtifact> {
    let complex_id = attributes
        .get("complex_id")
        .and_then(JsonValue::as_str)
        .context("Gold catalog row is missing complex_id")?;
    let artifact_id = artifact_id(provenance.iceberg_snapshot_id.as_str(), complex_id);
    let object_key = industrial_complex_gold_profile_key(&artifact_id.to_string())?;

    let mut body = serde_json::to_vec_pretty(&ProfileDocument {
        schema_version: PROFILE_SCHEMA_VERSION,
        artifact_id,
        complex_id,
        source: provenance,
        attributes,
    })
    .context("failed to serialize the industrial-complex Gold profile document")?;
    body.push(b'\n');

    Ok(ProfileArtifact {
        complex_id: complex_id.to_owned(),
        artifact_id,
        object_key,
        checksum_sha256: format!("{:x}", Sha256::digest(&body)),
        body,
    })
}

/// Derives the artifact identity from the snapshot and complex it describes.
///
/// A UUIDv5 keeps `gold/industrial-complex/profiles/{artifact_id}.json` (FP-ADR-0005) while
/// making the address a function of its inputs, which is what lets a re-run be idempotent.
fn artifact_id(iceberg_snapshot_id: &str, complex_id: &str) -> Uuid {
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, PROFILE_SCHEMA_VERSION.as_bytes());
    Uuid::new_v5(
        &namespace,
        format!("{iceberg_snapshot_id}:{complex_id}").as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    use super::{build, GoldSnapshotProvenance};

    // Snapshot ids here sit in the repository-reserved synthetic namespace
    // (`scripts/guard/public-fixture-safety.py`), not the production values: a real Iceberg
    // snapshot id is a 19-digit integer and is indistinguishable by shape from a parcel number.
    // The production snapshot this command was proven against is recorded in root ADR-0036.
    fn provenance() -> GoldSnapshotProvenance {
        GoldSnapshotProvenance {
            table: "gold.complex_catalog".to_owned(),
            iceberg_snapshot_id: "999990000000000001".to_owned(),
            metadata_location: "s3://lakehouse/metadata/00001.metadata.json".to_owned(),
            manifest_list_location: "s3://lakehouse/metadata/snap-1.avro".to_owned(),
        }
    }

    fn attributes() -> JsonMap<String, JsonValue> {
        let JsonValue::Object(map) = json!({
            "complex_id": "0196e7e0-3c20-7000-8000-100000000002",
            "official_complex_code": "446400",
            "name": "fixture complex",
            "kind": "general",
            "status": "active",
            "sido_code": "41",
            "sigungu_code": null,
            "address_text": "fixture address",
            "official_area_sqm": "1234.56",
            "calculated_area_sqm": null,
            "parcel_count": 0,
            "boundary_object_key": null,
            "source_snapshot_id": "vworldkr__sandan_profile-202506",
            "iceberg_snapshot_id": "999990000000000003",
            "published_at_utc": "2026-08-18T00:00:00Z"
        }) else {
            unreachable!("fixture literal is an object")
        };
        map
    }

    #[test]
    fn the_same_snapshot_and_row_produce_byte_identical_artifacts() -> anyhow::Result<()> {
        let first = build(&provenance(), &attributes())?;
        let second = build(&provenance(), &attributes())?;

        assert_eq!(first, second);
        assert_eq!(
            first.object_key,
            format!(
                "gold/industrial-complex/profiles/{}.json",
                first.artifact_id
            )
        );
        Ok(())
    }

    #[test]
    fn a_different_gold_snapshot_produces_a_different_artifact() -> anyhow::Result<()> {
        let first = build(&provenance(), &attributes())?;
        let mut next_snapshot = provenance();
        next_snapshot.iceberg_snapshot_id = "999990000000000002".to_owned();
        let second = build(&next_snapshot, &attributes())?;

        assert_ne!(first.artifact_id, second.artifact_id);
        assert_ne!(first.object_key, second.object_key);
        Ok(())
    }

    #[test]
    fn the_document_carries_the_row_verbatim() -> anyhow::Result<()> {
        let artifact = build(&provenance(), &attributes())?;
        let document: JsonValue = serde_json::from_slice(&artifact.body)?;

        assert_eq!(
            document["schema_version"],
            "foundation-platform.industrial_complex_gold_profile.v1"
        );
        assert_eq!(document["source"]["table"], "gold.complex_catalog");
        // parcel_count 0 and calculated_area_sqm null are the values the Gold table holds today.
        // They are carried, not corrected: see root ADR-0036.
        assert_eq!(document["attributes"]["parcel_count"], 0);
        assert_eq!(
            document["attributes"]["calculated_area_sqm"],
            JsonValue::Null
        );
        assert_eq!(document["attributes"]["official_area_sqm"], "1234.56");
        Ok(())
    }
}
