// one-off: with `gold_takeover_sides` (the last served Gold moved to its `_versions` table).
//! The documents a takeover verdict's candidate serves, held to the schema registry (root ADR-0564,
//! Revision 2026-11-22 (둘째); rules: a port does not reproduce Spark's bytes, and every document it
//! serves validates against its subject's schema).
//!
//! The rows of the verdict's one 시도 are compared as values (`gold_takeover_sides`), which says the
//! two tables agree; it does not say the documents the lane renders from the new table are what the
//! registry promises its readers. So every candidate row of that 시도 is rendered by the lane's own
//! renderer (the one the bake runs, `section_packs::sections::Renderer`) and the document is
//! validated against the JSON Schema of the subject's current revision as the release compiled it
//! in (`schema_registry::lane_document_schema`). A row the renderer refuses, or a document the
//! schema refuses, fails the verdict's result checks.
//!
//! What it does not see: the other 시도 (the switch's full bake renders every document and holds it
//! to the served one); what the lane schemas leave open (they declare `type`, `required`, `enum` and
//! `pattern`, never `additionalProperties`, so an extra field passes); the `source` block, stamped
//! here with the candidate's table and snapshot and placeholder locations; and a row as the bake's
//! Iceberg reader decodes it, for the rows reach the renderer through `DuckDB`'s `to_json`. A table no
//! by-PNU lane bakes (the 실거래 region statistics) or whose lane the registry lists no schema for
//! (the building lane) is not checked, and the record says why.

use anyhow::Context;
use foundation_publisher_by_pnu::building_by_pnu_serving_export::section_packs::sections::{
    self, PackProvenance, Renderer,
};
use foundation_publisher_core::{by_pnu_gateway_contract::ByPnuLane, schema_registry};
use foundation_publisher_silver::duckdb::Connection;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// How many refused documents the record names.
const MAX_SAMPLES: usize = 20;

/// What the document check found, recorded in `checks.json`.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct DocumentCheck {
    /// Why the documents were not checked, when they were not.
    pub skipped: Option<String>,
    /// The lane whose renderer and schema were used.
    pub lane: String,
    /// The candidate rows rendered.
    pub rendered: u64,
    /// The rows the renderer refused or whose document the schema refused.
    pub refused: u64,
    /// The first refusals: the PNU and why.
    pub samples: Vec<String>,
}

/// The lane's renderer and its schema's validator, loaded before the `DuckDB` session opens (the
/// session is not `Send` across an await).
pub(crate) struct DocumentRules {
    lane: ByPnuLane,
    renderer: Renderer,
    validator: jsonschema::Validator,
}

impl DocumentRules {
    /// The rules of the lane that bakes `table`, or why its documents cannot be checked: no lane
    /// bakes it, or the registry lists no document schema for the lane.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read, the schema does not compile, or the
    /// renderer's inputs cannot be read.
    pub(crate) async fn load(table: &str) -> anyhow::Result<Result<Self, String>> {
        let Some(lane) = ByPnuLane::all()?
            .into_iter()
            .find(|lane| sections::gold_table(*lane).table_name == table)
        else {
            return Ok(Err(format!("no by-PNU lane bakes {table}")));
        };
        if schema_registry::lane_document_schema(lane.name()).is_none() {
            return Ok(Err(format!(
                "the schema registry lists no document schema for the {} lane",
                lane.name()
            )));
        }
        let validator = validator(lane)?;
        Ok(Ok(Self {
            lane,
            renderer: Renderer::load(lane).await?,
            validator,
        }))
    }

    /// Renders and validates every row `query` returns (one 시군구 of the candidate: `DuckDB` hands
    /// a result over whole), read as `to_json` writes them, and adds what it found to `found`.
    ///
    /// # Errors
    /// Returns an error when `DuckDB` cannot run the query; a refused row is counted, not an error.
    pub(crate) fn check(
        &self,
        connection: &Connection,
        query: &str,
        provenance: &PackProvenance,
        found: &mut DocumentCheck,
    ) -> anyhow::Result<()> {
        self.lane.name().clone_into(&mut found.lane);
        let mut statement = connection
            .prepare(&format!("SELECT to_json(q)::VARCHAR FROM ({query}) AS q"))
            .context("DuckDB refused the candidate's rows")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let text: String = row.get(0)?;
            found.rendered += 1;
            if let Err(why) = self.one(&text, provenance) {
                found.refused += 1;
                if found.samples.len() < MAX_SAMPLES {
                    found.samples.push(why);
                }
            }
        }
        Ok(())
    }

    /// Why one row's document is refused, if it is.
    fn one(&self, text: &str, provenance: &PackProvenance) -> Result<(), String> {
        let row: JsonMap<String, JsonValue> =
            serde_json::from_str(text).map_err(|error| format!("a row is not JSON: {error}"))?;
        let pnu = row
            .get("pnu")
            .and_then(JsonValue::as_str)
            .unwrap_or("-")
            .to_owned();
        let document = self
            .renderer
            .render(provenance, &row)
            .map_err(|error| format!("{pnu}: the renderer refuses the row: {error:#}"))?;
        let value: JsonValue = serde_json::from_slice(&document.bytes)
            .map_err(|error| format!("{pnu}: the document is not JSON: {error}"))?;
        // The first error, as text, before `value` goes: an error borrows the value it is about.
        let refused = self
            .validator
            .iter_errors(&value)
            .next()
            .map(|error| format!("{pnu}: {error} at {}", error.instance_path()));
        refused.map_or(Ok(()), Err)
    }
}

/// The validator of `lane`'s documents: the registry's schema of its subject's current revision.
fn validator(lane: ByPnuLane) -> anyhow::Result<jsonschema::Validator> {
    let text = schema_registry::lane_document_schema(lane.name()).with_context(|| {
        format!(
            "the schema registry lists no document schema for the {} lane",
            lane.name()
        )
    })?;
    let schema: JsonValue = serde_json::from_str(text)
        .with_context(|| format!("the {} lane's schema is not JSON", lane.name()))?;
    jsonschema::validator_for(&schema).map_err(|error| {
        anyhow::anyhow!(
            "the {} lane's schema does not compile: {error}",
            lane.name()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provenance() -> PackProvenance {
        PackProvenance {
            table: "gold.parcel_panel_versions".to_owned(),
            iceberg_snapshot_id: "1".to_owned(),
            metadata_location: "s3://lakehouse/metadata/1.json".to_owned(),
            manifest_list_location: "s3://lakehouse/metadata/snap-1.avro".to_owned(),
        }
    }

    fn parcel_rules() -> anyhow::Result<DocumentRules> {
        Ok(DocumentRules {
            lane: ByPnuLane::Parcel,
            renderer: Renderer::Parcel,
            validator: validator(ByPnuLane::Parcel)?,
        })
    }

    /// The parcel panel's lane is found and loaded; a table no lane bakes and a lane without a
    /// registered schema are not checked, and say why.
    #[tokio::test]
    async fn the_rules_are_the_lane_s_or_the_reason_there_are_none() -> anyhow::Result<()> {
        let parcel = DocumentRules::load("gold.parcel_panel").await?;
        assert!(matches!(
            parcel.as_ref().map(|rules| rules.lane),
            Ok(ByPnuLane::Parcel)
        ));
        assert_eq!(
            DocumentRules::load("gold.real_transaction_region_stats")
                .await?
                .err()
                .as_deref(),
            Some("no by-PNU lane bakes gold.real_transaction_region_stats")
        );
        Ok(())
    }

    /// A synthetic parcel row (99999) renders a document the registry's schema takes; a row whose
    /// section leaves the DTO's shape (a zone code as a number) is refused, counted and named, never
    /// an error of the check.
    #[test]
    fn every_candidate_row_is_rendered_and_held_to_its_schema() -> anyhow::Result<()> {
        let rules = parcel_rules()?;
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE c (pnu VARCHAR, kind VARCHAR, area_m2 BIGINT, zonings_json VARCHAR, \
               price_json VARCHAR, characteristics_json VARCHAR, forest_ledger_json VARCHAR, \
               transfer_history_json VARCHAR, land_rights_json VARCHAR, land_right_total BIGINT, \
               lineage_json VARCHAR, attached_via_json VARCHAR, legal_dong_json VARCHAR); \
             INSERT INTO c VALUES \
               ('9999900000100000000', NULL, 512, \
                '[{\"zone_code\":\"UQA320\",\"zone_name\":\"synthetic zone\",\"anchor_code\":\"UQA320\",\"inclusion_code\":\"1\"}]', \
                '{\"price_per_m2\":123000,\"base_year\":2026,\"base_month\":1,\"announced_date\":\"2026-01-01\"}', \
                NULL, NULL, '[]', '[]', 0, NULL, NULL, NULL), \
               ('9999900000100010000', NULL, NULL, '[{\"zone_code\":1}]', NULL, NULL, NULL, '[]', '[]', \
                0, NULL, NULL, NULL);",
        )?;
        let mut found = DocumentCheck::default();
        rules.check(
            &connection,
            "SELECT * FROM c ORDER BY pnu",
            &provenance(),
            &mut found,
        )?;
        assert_eq!(found.lane, "parcel");
        assert_eq!(found.rendered, 2);
        assert_eq!(found.refused, 1, "{:?}", found.samples);
        assert!(
            found.samples[0].starts_with("9999900000100010000: the renderer refuses the row"),
            "{:?}",
            found.samples
        );
        Ok(())
    }

    #[test]
    fn a_document_outside_its_schema_is_refused() -> anyhow::Result<()> {
        let validator = validator(ByPnuLane::Parcel)?;
        let wrong = serde_json::json!({"schema_version": 2, "pnu": "9999910100100010000"});
        assert!(validator.iter_errors(&wrong).next().is_some());
        Ok(())
    }

    #[test]
    fn every_pack_lane_s_schema_compiles() -> anyhow::Result<()> {
        for lane in ByPnuLane::all()? {
            if schema_registry::lane_document_schema(lane.name()).is_some() {
                validator(lane)?;
            }
        }
        Ok(())
    }
}
