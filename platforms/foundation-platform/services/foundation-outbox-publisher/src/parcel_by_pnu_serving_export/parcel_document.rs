//! The by-PNU serving document one parcel publishes to object storage (root ADR-0096).
//!
//! The document is a pure function of the Gold snapshot it was read from and the row it carries.
//! No wall clock, no run id, no address: a re-export of the same snapshot produces byte-identical
//! objects at the same key, so a create-only collision is an idempotent re-run rather than a
//! conflict.
//!
//! The section payloads reuse the `foundation-contracts` response DTOs the Catalog API serves
//! from Postgres today (`ParcelResponse` minus its Postgres-runtime fields `id`, `version`, and
//! `updated_at`). The baked object and the API response are then the same shape by construction,
//! which is what makes root ADR-0096's "same JSON as postgres" proof a field-for-field diff
//! instead of a judgement call.

use anyhow::Context;
use foundation_contracts::catalog::{
    ParcelCharacteristicResponse, ParcelForestLedgerResponse, ParcelLandRightResponse,
    ParcelPriceResponse, ParcelTransferEventResponse, ParcelZoningResponse,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

/// Wire schema version of the published by-PNU serving document.
pub(super) const PARCEL_DOCUMENT_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_by_pnu_profile.v1";

/// One serving artifact, ready to be written under a generation directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ParcelServingArtifact {
    /// Parcel the document describes; the export derives the object key from it.
    pub(super) pnu: String,
    /// Exact bytes written to object storage.
    pub(super) body: Vec<u8>,
    /// SHA-256 of `body`.
    pub(super) checksum_sha256: String,
}

/// Which Gold snapshot a serving artifact was derived from.
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
struct ParcelByPnuDocument<'a> {
    schema_version: &'static str,
    pnu: &'a str,
    source: &'a GoldSnapshotProvenance,
    /// Postgres-curated parcel kind (root ADR-0070). The lakehouse carries no producer for it,
    /// so the Gold projection leaves it null until a merge path is decided; the field stays in
    /// the schema so that decision changes data, not shape.
    kind: Option<String>,
    area_m2: Option<u64>,
    zonings: Vec<ParcelZoningResponse>,
    price: Option<ParcelPriceResponse>,
    characteristics: Option<ParcelCharacteristicResponse>,
    forest_ledger: Option<ParcelForestLedgerResponse>,
    transfer_history: Vec<ParcelTransferEventResponse>,
    land_rights: Vec<ParcelLandRightResponse>,
    land_right_total: u64,
}

/// Builds the serving artifact for one `gold.parcel_panel` row.
///
/// # Errors
/// Returns an error when a required column is absent or a section column does not parse as the
/// contract DTO it must carry; the message names the offending column.
pub(super) fn build(
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
) -> anyhow::Result<ParcelServingArtifact> {
    let pnu = row
        .get("pnu")
        .and_then(JsonValue::as_str)
        .context("gold.parcel_panel row is missing pnu")?;

    let document = ParcelByPnuDocument {
        schema_version: PARCEL_DOCUMENT_SCHEMA_VERSION,
        pnu,
        source: provenance,
        kind: optional_string(row, "kind")?,
        area_m2: optional_u64(row, "area_m2")?,
        zonings: required_section(row, "zonings_json")?,
        price: optional_section(row, "price_json")?,
        characteristics: optional_section(row, "characteristics_json")?,
        forest_ledger: optional_section(row, "forest_ledger_json")?,
        transfer_history: required_section(row, "transfer_history_json")?,
        land_rights: required_section(row, "land_rights_json")?,
        land_right_total: required_u64(row, "land_right_total")?,
    };

    let mut body = serde_json::to_vec_pretty(&document)
        .context("failed to serialize the parcel by-PNU serving document")?;
    body.push(b'\n');

    Ok(ParcelServingArtifact {
        pnu: pnu.to_owned(),
        checksum_sha256: format!("{:x}", Sha256::digest(&body)),
        body,
    })
}

/// Parses a required JSON-string section column into its contract DTO shape.
fn required_section<T: DeserializeOwned>(
    row: &JsonMap<String, JsonValue>,
    column: &str,
) -> anyhow::Result<T> {
    let raw = row
        .get(column)
        .and_then(JsonValue::as_str)
        .with_context(|| format!("gold.parcel_panel row is missing {column}"))?;
    serde_json::from_str(raw).with_context(|| {
        format!("gold.parcel_panel column {column} does not parse as its contract shape")
    })
}

/// Parses an optional JSON-string section column; a null or absent column is an absent section.
fn optional_section<T: DeserializeOwned>(
    row: &JsonMap<String, JsonValue>,
    column: &str,
) -> anyhow::Result<Option<T>> {
    match row.get(column) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(raw)) => serde_json::from_str(raw).map(Some).with_context(|| {
            format!("gold.parcel_panel column {column} does not parse as its contract shape")
        }),
        Some(other) => anyhow::bail!(
            "gold.parcel_panel column {column} must be a JSON string or null, got {other}"
        ),
    }
}

fn optional_string(
    row: &JsonMap<String, JsonValue>,
    column: &str,
) -> anyhow::Result<Option<String>> {
    match row.get(column) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(other) => {
            anyhow::bail!("gold.parcel_panel column {column} must be a string or null, got {other}")
        }
    }
}

fn optional_u64(row: &JsonMap<String, JsonValue>, column: &str) -> anyhow::Result<Option<u64>> {
    match row.get(column) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).with_context(|| {
            format!("gold.parcel_panel column {column} must be a non-negative integer")
        }),
    }
}

fn required_u64(row: &JsonMap<String, JsonValue>, column: &str) -> anyhow::Result<u64> {
    row.get(column)
        .with_context(|| format!("gold.parcel_panel row is missing {column}"))?
        .as_u64()
        .with_context(|| {
            format!("gold.parcel_panel column {column} must be a non-negative integer")
        })
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    use super::{build, GoldSnapshotProvenance};

    // Snapshot ids and PNUs sit in the repository-reserved synthetic namespaces
    // (`scripts/guard/public-fixture-safety.py`): a real Iceberg snapshot id is a 19-digit
    // integer and is indistinguishable by shape from a parcel number.
    fn provenance() -> GoldSnapshotProvenance {
        GoldSnapshotProvenance {
            table: "gold.parcel_panel".to_owned(),
            iceberg_snapshot_id: "999990000000000001".to_owned(),
            metadata_location: "s3://lakehouse/metadata/00001.metadata.json".to_owned(),
            manifest_list_location: "s3://lakehouse/metadata/snap-1.avro".to_owned(),
        }
    }

    fn row() -> JsonMap<String, JsonValue> {
        let JsonValue::Object(map) = json!({
            "pnu": "9999900000100000000",
            "area_m2": 331,
            "zonings_json": "[{\"zone_code\":\"UQA320\",\"zone_name\":\"synthetic zone\",\"anchor_code\":\"UQA320\",\"inclusion_code\":\"1\"}]",
            "price_json": "{\"price_per_m2\":123000,\"base_year\":2026,\"base_month\":1,\"announced_date\":\"2026-01-01\"}",
            "characteristics_json": null,
            "forest_ledger_json": null,
            "transfer_history_json": "[]",
            "land_rights_json": "[]",
            "land_right_total": 0,
            "source_snapshot_id": "999990000000000002"
        }) else {
            unreachable!("fixture literal is an object");
        };
        map
    }

    #[test]
    fn builds_a_deterministic_document_from_snapshot_and_row() -> anyhow::Result<()> {
        let first = build(&provenance(), &row())?;
        let second = build(&provenance(), &row())?;

        assert_eq!(first, second, "the document must be a pure function");
        assert_eq!(first.pnu, "9999900000100000000");

        let document: JsonValue = serde_json::from_slice(&first.body)?;
        assert_eq!(
            document["schema_version"],
            "foundation-platform.parcel_by_pnu_profile.v1"
        );
        assert_eq!(document["pnu"], "9999900000100000000");
        assert_eq!(document["source"]["table"], "gold.parcel_panel");
        assert_eq!(document["kind"], JsonValue::Null);
        assert_eq!(document["area_m2"], 331);
        assert_eq!(document["zonings"][0]["zone_code"], "UQA320");
        assert_eq!(document["price"]["price_per_m2"], 123_000);
        assert_eq!(document["characteristics"], JsonValue::Null);
        assert_eq!(document["forest_ledger"], JsonValue::Null);
        assert_eq!(document["transfer_history"], json!([]));
        assert_eq!(document["land_rights"], json!([]));
        assert_eq!(document["land_right_total"], 0);
        assert!(first.body.ends_with(b"\n"));
        Ok(())
    }

    #[test]
    fn refuses_a_row_missing_its_identity_or_required_sections() {
        let mut no_pnu = row();
        no_pnu.remove("pnu");
        let error = build(&provenance(), &no_pnu).expect_err("missing pnu must refuse");
        assert!(error.to_string().contains("pnu"), "{error}");

        let mut no_zonings = row();
        no_zonings.remove("zonings_json");
        let error = build(&provenance(), &no_zonings).expect_err("missing zonings must refuse");
        assert!(error.to_string().contains("zonings_json"), "{error}");
    }

    #[test]
    fn refuses_a_section_that_does_not_parse_as_its_contract_shape() {
        let mut bad_price = row();
        bad_price.insert(
            "price_json".to_owned(),
            JsonValue::String("{\"price_per_m2\":\"not-a-number\"}".to_owned()),
        );
        let error = build(&provenance(), &bad_price).expect_err("malformed price must refuse");
        assert!(error.to_string().contains("price_json"), "{error}");

        let mut wrong_type = row();
        wrong_type.insert(
            "transfer_history_json".to_owned(),
            json!({"not": "a string"}),
        );
        let error = build(&provenance(), &wrong_type).expect_err("non-string section must refuse");
        assert!(
            error.to_string().contains("transfer_history_json"),
            "{error}"
        );
    }
}
