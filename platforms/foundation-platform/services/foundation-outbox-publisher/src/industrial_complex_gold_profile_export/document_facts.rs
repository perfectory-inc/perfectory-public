//! The facts a complex's notice attachments state, as foundation admits them for its panel.
//!
//! `gold.complex_document_facts` holds one row per complex per fold run (root ADR-0477 §1·§5);
//! the newest run is the complex's current row, chosen by the contract's own rule
//! (`LakehouseTableContract::current_rows`, root ADR-0564 §4), never here. A row whose
//! document admits no kind is the record that the complex's facts left the panel, so it yields no
//! entry here and the profile carries no `document_facts`.

use std::collections::HashMap;

use anyhow::{ensure, Context};
use lakehouse_domain::GOLD_COMPLEX_DOCUMENT_FACTS;
use lakehouse_infrastructure::IcebergSnapshotManifestList;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};

/// One complex's admitted facts, as the profile carries them under `document_facts`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct DocumentFacts {
    /// Lakehouse table the facts were read from.
    pub(super) table: String,
    /// Iceberg snapshot of that table which was read.
    pub(super) iceberg_snapshot_id: String,
    /// Fold run whose row this is: part of the profile's identity, since the facts change apart
    /// from the catalog row.
    pub(super) run_id: String,
    /// Per fact kind, its source notice attachment and its facts with their pages.
    pub(super) kinds: JsonMap<String, JsonValue>,
}

/// The current facts of every complex that has any, keyed by `official_complex_code`.
pub(super) async fn current(
    lakehouse: &LakehouseObjectReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<HashMap<String, DocumentFacts>> {
    let rows = scan_snapshot_rows(&GOLD_COMPLEX_DOCUMENT_FACTS, lakehouse, snapshot).await?;
    newest_per_complex(
        &rows.rows,
        snapshot.table_name.as_str(),
        &snapshot.snapshot_id.to_string(),
    )
}

/// The newest row per complex, decoded; complexes whose newest row admits no kind are left out.
pub(super) fn newest_per_complex(
    rows: &[JsonMap<String, JsonValue>],
    table: &str,
    iceberg_snapshot_id: &str,
) -> anyhow::Result<HashMap<String, DocumentFacts>> {
    for row in rows {
        text(row, "official_complex_code")?;
        text(row, "run_id")?;
    }
    // Two rows of one complex from one run tie, and the rule refuses them.
    let current = GOLD_COMPLEX_DOCUMENT_FACTS.current_rows(rows)?;
    let mut facts = HashMap::with_capacity(current.len());
    for row in current {
        let code = text(row, "official_complex_code")?.to_owned();
        let document: JsonValue = serde_json::from_str(text(row, "document_facts_json")?)
            .with_context(|| {
                format!("{table}: complex {code}'s document_facts_json is not JSON")
            })?;
        let kinds = document
            .get("kinds")
            .and_then(JsonValue::as_object)
            .with_context(|| format!("{table}: complex {code}'s document has no kinds object"))?;
        ensure!(
            kinds.values().all(JsonValue::is_object),
            "{table}: complex {code}'s kinds are not objects"
        );
        if kinds.is_empty() {
            continue;
        }
        facts.insert(
            code,
            DocumentFacts {
                table: table.to_owned(),
                iceberg_snapshot_id: iceberg_snapshot_id.to_owned(),
                run_id: text(row, "run_id")?.to_owned(),
                kinds: kinds.clone(),
            },
        );
    }
    Ok(facts)
}

/// The row as its content is compared with the served index: the catalog row plus the facts'
/// kinds (not their run or snapshot, so a refold that states the same facts writes nothing).
pub(super) fn content_row(
    row: &JsonMap<String, JsonValue>,
    facts: Option<&DocumentFacts>,
) -> JsonMap<String, JsonValue> {
    let mut content = row.clone();
    if let Some(facts) = facts {
        content.insert(
            "document_facts".to_owned(),
            JsonValue::Object(facts.kinds.clone()),
        );
    }
    content
}

fn text<'a>(row: &'a JsonMap<String, JsonValue>, name: &str) -> anyhow::Result<&'a str> {
    row.get(name)
        .and_then(JsonValue::as_str)
        .with_context(|| format!("gold.complex_document_facts row is missing {name}"))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    use super::{content_row, newest_per_complex};

    fn row(code: &str, run_id: &str, kinds: &JsonValue) -> JsonMap<String, JsonValue> {
        let JsonValue::Object(row) = json!({
            "official_complex_code": code,
            "document_facts_json": json!({"schema": "complex-document-facts.v1", "kinds": kinds}).to_string(),
            "fact_count": 1,
            "run_id": run_id,
        }) else {
            unreachable!()
        };
        row
    }

    #[test]
    fn the_newest_run_of_each_complex_is_its_facts() -> anyhow::Result<()> {
        let old = json!({"land_use_area": {"facts": [1]}});
        let new = json!({"land_use_area": {"facts": [2]}});
        let rows = [
            row("999990", "20991231T000000Z", &new),
            row("999990", "20990101T000000Z", &old),
        ];
        let facts = newest_per_complex(&rows, "gold.complex_document_facts", "7")?;
        let held = &facts["999990"];
        assert_eq!(held.run_id, "20991231T000000Z");
        assert_eq!(held.kinds["land_use_area"]["facts"], json!([2]));
        assert_eq!(held.iceberg_snapshot_id, "7");
        Ok(())
    }

    #[test]
    fn a_complex_whose_newest_row_admits_nothing_has_no_facts() -> anyhow::Result<()> {
        let rows = [
            row(
                "999990",
                "20990101T000000Z",
                &json!({"land_use_area": {"facts": [1]}}),
            ),
            row("999990", "20991231T000000Z", &json!({})),
        ];
        assert!(newest_per_complex(&rows, "t", "7")?.is_empty());
        Ok(())
    }

    #[test]
    fn two_rows_of_one_run_are_refused() {
        let kinds = json!({"land_use_area": {}});
        let rows = [
            row("999990", "20990101T000000Z", &kinds),
            row("999990", "20990101T000000Z", &kinds),
        ];
        assert!(newest_per_complex(&rows, "t", "7").is_err());
    }

    #[test]
    fn the_content_changes_with_the_kinds_and_not_with_the_run() -> anyhow::Result<()> {
        let catalog = row("999990", "x", &json!({}));
        let kinds = json!({"land_use_area": {"facts": [1]}});
        let first = newest_per_complex(&[row("999990", "20990101T000000Z", &kinds)], "t", "7")?;
        let refold = newest_per_complex(&[row("999990", "20991231T000000Z", &kinds)], "t", "8")?;
        assert_eq!(
            content_row(&catalog, first.get("999990")),
            content_row(&catalog, refold.get("999990"))
        );
        assert_ne!(
            content_row(&catalog, first.get("999990")),
            content_row(&catalog, None)
        );
        Ok(())
    }
}
