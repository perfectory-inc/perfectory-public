//! The industrial-complex index (root ADR-0456): every complex's list fields and the key of its
//! current profile, one document the parcel edge serves as the list and the panel's lookup.
//!
//! The index is what makes the export partial. Each entry records the hash of its complex's Gold
//! row without the run columns; a complex whose hash is the one the current index holds keeps its
//! profile and is not written again, so a Gold rebuild that changed ten complexes writes ten
//! profiles, not every one.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

/// The index's subject in the schema registry: its owner is this block of the connection contract.
const INDEX_SURFACE: &str = "industrial_complex_documents";

/// Gold columns a rebuild writes afresh whatever the row says: left out of the content hash.
const RUN_COLUMNS: [&str; 2] = ["iceberg_snapshot_id", "published_at_utc"];

/// One complex in the index.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct IndexEntry {
    pub(super) complex_id: String,
    pub(super) official_complex_code: String,
    pub(super) name: String,
    pub(super) kind: String,
    pub(super) status: String,
    pub(super) development_stage: Option<String>,
    pub(super) sido_code: Option<String>,
    pub(super) sido_name: Option<String>,
    pub(super) sigungu_code: Option<String>,
    pub(super) address_text: Option<String>,
    pub(super) official_area_sqm: Option<String>,
    pub(super) profile_object_key: String,
    pub(super) content_sha256: String,
}

/// The Gold snapshot an index generation was read from.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct IndexSource {
    pub(super) table: String,
    pub(super) iceberg_snapshot_id: String,
}

/// The index document.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct IndexDocument {
    pub(super) schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) schema_revision: Option<u32>,
    pub(super) generation: u64,
    pub(super) source: IndexSource,
    pub(super) complexes: Vec<IndexEntry>,
}

impl IndexDocument {
    /// The next generation over `complexes` (sorted here), or `None` when they are the entries
    /// `previous` already serves: then nothing is written.
    pub(super) fn next(
        previous: Option<&Self>,
        source: IndexSource,
        mut complexes: Vec<IndexEntry>,
    ) -> anyhow::Result<Option<Self>> {
        complexes.sort_by(|a, b| a.complex_id.cmp(&b.complex_id));
        if previous.is_some_and(|previous| previous.complexes == complexes) {
            return Ok(None);
        }
        let subject =
            foundation_publisher_core::schema_registry::subject_of_surface(INDEX_SURFACE)?;
        Ok(Some(Self {
            schema_version: format!("{}.v{}", subject.subject, subject.major),
            schema_revision: subject.written_revision(),
            generation: previous.map_or(1, |previous| previous.generation + 1),
            source,
            complexes,
        }))
    }

    /// The exact bytes written and their SHA-256.
    pub(super) fn body(&self) -> anyhow::Result<(Vec<u8>, String)> {
        let mut body =
            serde_json::to_vec(self).context("failed to serialize the industrial-complex index")?;
        body.push(b'\n');
        let sha256 = format!("{:x}", Sha256::digest(&body));
        Ok((body, sha256))
    }
}

/// The hash of a Gold row without its run columns. `serde_json`'s map is ordered by key here (no
/// `preserve_order`), so equal rows serialize to equal bytes.
pub(super) fn content_sha256(row: &JsonMap<String, JsonValue>) -> anyhow::Result<String> {
    let mut content = row.clone();
    for column in RUN_COLUMNS {
        content.remove(column);
    }
    let bytes = serde_json::to_vec(&content).context("failed to serialize a Gold row")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// The index entry of one Gold row whose profile is at `profile_object_key`.
pub(super) fn entry(
    row: &JsonMap<String, JsonValue>,
    profile_object_key: String,
    content_sha256: String,
) -> anyhow::Result<IndexEntry> {
    let text = |name: &str| {
        row.get(name)
            .and_then(JsonValue::as_str)
            .map(ToOwned::to_owned)
    };
    let required =
        |name: &str| text(name).with_context(|| format!("Gold catalog row is missing {name}"));
    Ok(IndexEntry {
        complex_id: required("complex_id")?,
        official_complex_code: required("official_complex_code")?,
        name: required("name")?,
        kind: required("kind")?,
        status: required("status")?,
        development_stage: text("development_stage"),
        sido_code: text("sido_code"),
        sido_name: text("sido_name"),
        sigungu_code: text("sigungu_code"),
        address_text: text("address_text"),
        official_area_sqm: text("official_area_sqm"),
        profile_object_key,
        content_sha256,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    use super::{content_sha256, entry, IndexDocument, IndexSource};

    fn row(name: &str, snapshot: &str) -> JsonMap<String, JsonValue> {
        let JsonValue::Object(map) = json!({
            "complex_id": "0196e7e0-3c20-7000-8000-100000000002",
            "official_complex_code": "999990",
            "name": name,
            "kind": "general",
            "status": "operating",
            "sido_code": "99",
            "address_text": null,
            "official_area_sqm": "1234.56",
            "iceberg_snapshot_id": snapshot,
            "published_at_utc": "2026-11-09T00:00:00Z"
        }) else {
            unreachable!("fixture literal is an object")
        };
        map
    }

    fn source() -> IndexSource {
        IndexSource {
            table: "gold.complex_catalog".to_owned(),
            iceberg_snapshot_id: "999990000000000001".to_owned(),
        }
    }

    #[test]
    fn a_rebuild_that_changed_nothing_keeps_the_hash() -> anyhow::Result<()> {
        let first = content_sha256(&row("fixture complex", "999990000000000001"))?;
        assert_eq!(
            first,
            content_sha256(&row("fixture complex", "999990000000000002"))?
        );
        assert_ne!(
            first,
            content_sha256(&row("renamed complex", "999990000000000002"))?
        );
        Ok(())
    }

    #[test]
    fn the_same_entries_write_no_generation_and_a_change_writes_the_next() -> anyhow::Result<()> {
        let key = "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000003.json";
        let one = |name: &str| -> anyhow::Result<_> {
            let row = row(name, "999990000000000001");
            entry(&row, key.to_owned(), content_sha256(&row)?)
        };
        let first = IndexDocument::next(None, source(), vec![one("fixture complex")?])?
            .ok_or_else(|| anyhow::anyhow!("the first index is written"))?;
        assert_eq!(first.generation, 1);
        assert!(
            IndexDocument::next(Some(&first), source(), vec![one("fixture complex")?])?.is_none()
        );
        let second = IndexDocument::next(Some(&first), source(), vec![one("renamed complex")?])?
            .ok_or_else(|| anyhow::anyhow!("a changed entry writes a generation"))?;
        assert_eq!(second.generation, 2);
        let (body, _) = second.body()?;
        let parsed: IndexDocument = serde_json::from_slice(&body)?;
        assert_eq!(parsed.complexes, second.complexes);
        Ok(())
    }
}
