// one-off: with `gold_takeover_sides` (the last served Gold moved to its `_versions` table).
//! The documents a takeover verdict's candidate serves, held to the schema registry (root ADR-0564,
//! Revision 2026-11-22; rules: a port does not reproduce Spark's bytes, and every document it serves
//! validates against its subject's schema).
//!
//! The rows of the verdict's one 시도 are compared as values (`gold_takeover_sides`), which says the
//! two tables agree; it does not say the documents the lane renders from the new table are what the
//! registry promises its readers. So every candidate row of that 시도 is rendered by the lane's own
//! renderer (the one the bake runs, `section_packs::sections::Renderer`) and the document is
//! validated against the JSON Schema of the subject's current revision (`schema_registry`). A row
//! the renderer refuses, or a document the schema refuses, fails the verdict's result checks.

use anyhow::Context;
use foundation_publisher_by_pnu::building_by_pnu_serving_export::section_packs::sections::{
    self, PackProvenance, Renderer,
};
use foundation_publisher_core::{by_pnu_gateway_contract::ByPnuLane, schema_registry};
use foundation_publisher_silver::duckdb::Connection;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// How many refused documents the record names.
const MAX_SAMPLES: usize = 20;

/// The lane that serves `table` (the table a versions table replaces), as the bake resolves it.
///
/// # Errors
/// Returns an error when the contract cannot be read or no lane bakes `table`.
pub(crate) fn lane_of(table: &str) -> anyhow::Result<ByPnuLane> {
    ByPnuLane::all()?
        .into_iter()
        .find(|lane| sections::gold_table(*lane).table_name == table)
        .with_context(|| format!("no by-PNU lane bakes {table}"))
}

/// What the document check found, recorded in `checks.json`.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct DocumentCheck {
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
    /// The rules of the lane that bakes `table`.
    ///
    /// # Errors
    /// Returns an error when no lane bakes `table`, the registry lists no schema for it, the schema
    /// does not compile, or the renderer's inputs cannot be read.
    pub(crate) async fn load(table: &str) -> anyhow::Result<Self> {
        let lane = lane_of(table)?;
        let validator = validator(lane)?;
        Ok(Self {
            lane,
            renderer: Renderer::load(lane).await?,
            validator,
        })
    }

    /// Renders and validates every row `query` returns, read as `to_json` writes them.
    ///
    /// # Errors
    /// Returns an error when `DuckDB` cannot run the query; a refused row is counted, not an error.
    pub(crate) fn check(
        &self,
        connection: &Connection,
        query: &str,
        provenance: &PackProvenance,
    ) -> anyhow::Result<DocumentCheck> {
        let mut found = DocumentCheck {
            lane: self.lane.name().to_owned(),
            ..DocumentCheck::default()
        };
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
        Ok(found)
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
        match self.validator.iter_errors(&value).next() {
            None => Ok(()),
            Some(error) => Err(format!("{pnu}: {error} at {}", error.instance_path())),
        }
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

    #[test]
    fn the_parcel_panel_is_baked_by_the_parcel_lane() -> anyhow::Result<()> {
        assert!(matches!(lane_of("gold.parcel_panel")?, ByPnuLane::Parcel));
        assert!(lane_of("gold.no_such_table").is_err());
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
        let found = rules.check(&connection, "SELECT * FROM c ORDER BY pnu", &provenance())?;
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
