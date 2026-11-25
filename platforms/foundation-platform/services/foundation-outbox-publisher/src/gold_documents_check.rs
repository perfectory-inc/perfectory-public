//! The documents a Gold of row versions serves, held to the schema registry before a reader sees
//! them (root ADR-0564 Revision 2026-11-24, ADR-0615 §7; rules: a port does not reproduce Spark's
//! bytes, and every document it serves validates against its subject's schema).
//!
//! Each row is rendered by the lane's own renderer (the one the bake runs,
//! `section_packs::sections::Renderer`) and the document is validated against the JSON Schema of the
//! subject's current revision as the release compiled it in (`schema_registry::lane_document_schema`).
//! A row the renderer refuses, or a document the schema refuses, is counted and named.
//!
//! Two callers. The versions build, on every commit: the rows a 시도 is about to append
//! ([`DocumentRules::changed_documents_hold_their_schema`]); a refused document stops that 시도's
//! commit. The takeover verdict (`gold_takeover_sides`), once per lane: every candidate row of its
//! one 시도, beside the comparison of the two tables as values, which says the tables agree and not
//! that the documents are what the registry promises.
//!
//! What it does not see: what the lane schemas leave open (they declare `type`, `required`, `enum`
//! and `pattern`, never `additionalProperties`, so an extra field passes); the `source` block,
//! stamped here with the table and placeholder locations (a build's commit does not exist yet); and
//! a row as the bake's Iceberg reader decodes it, for the rows reach the renderer through `DuckDB`'s
//! `to_json`. A table no by-PNU lane bakes (the 실거래 region statistics) or whose lane the registry
//! lists no schema for (the building lane) is not checked, and the record says why.

use anyhow::Context;
use foundation_publisher_by_pnu::building_by_pnu_serving_export::section_packs::sections::{
    self, PackProvenance, Renderer,
};
use foundation_publisher_core::{by_pnu_gateway_contract::ByPnuLane, schema_registry};
use foundation_publisher_silver::duckdb::Connection;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// How many refused documents the record names.
const MAX_SAMPLES: usize = 20;

/// What the document check found: the build's summary and the verdict's `checks.json` record it.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct DocumentCheck {
    /// Why the documents were not checked, when they were not.
    pub skipped: Option<String>,
    /// The lane whose renderer and schema were used.
    pub lane: String,
    /// The rows rendered.
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
    /// The lane whose documents a build of `table` holds to the registry, or why none: no by-PNU
    /// lane bakes it, or the registry lists no document schema for the lane. [`Self::load`] loads
    /// that lane's rules; `describe-checks` declares the check where this finds a lane.
    ///
    /// # Errors
    /// Returns an error when the by-PNU lane contract cannot be read.
    pub(crate) fn checked_lane(table: &str) -> anyhow::Result<Result<ByPnuLane, String>> {
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
        Ok(Ok(lane))
    }

    /// The rules of the lane that bakes `table`, or why its documents cannot be checked: no lane
    /// bakes it, or the registry lists no document schema for the lane.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read, the schema does not compile, or the
    /// renderer's inputs cannot be read.
    pub(crate) async fn load(table: &str) -> anyhow::Result<Result<Self, String>> {
        let lane = match Self::checked_lane(table)? {
            Ok(lane) => lane,
            Err(why) => return Ok(Err(why)),
        };
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

    /// The documents of the rows `appended` holds, the 시도's rows the versions build is about to
    /// append (`gold_versions_commit::diff`): every live row is rendered and held to the schema; a
    /// removal marker has no document. `table` is the table the rows go to, for the `source` block.
    ///
    /// The check that refused all 863 documents of the public auction verdict's 시도 on 2026-11-22
    /// (a null where the schema says a list) ran once, after the table was written; here it runs
    /// on what each commit changes, before it is written.
    ///
    /// # Errors
    /// Returns an error when `DuckDB` cannot read `appended`; a refused row is counted, not an error.
    pub(crate) fn changed_documents_hold_their_schema(
        &self,
        connection: &Connection,
        table: &str,
    ) -> anyhow::Result<DocumentCheck> {
        let provenance = PackProvenance {
            table: table.to_owned(),
            iceberg_snapshot_id: "-".to_owned(),
            metadata_location: "-".to_owned(),
            manifest_list_location: "-".to_owned(),
        };
        let mut found = DocumentCheck::default();
        self.check(
            connection,
            "SELECT * EXCLUDE (change) FROM appended WHERE row_state IS NULL ORDER BY pnu",
            &provenance,
            &mut found,
        )?;
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

    /// The rows a build is about to append: a live row is rendered and held to its schema, a
    /// removal marker is not a document, and the helper column of the diff is not a field.
    #[test]
    fn the_rows_about_to_be_appended_are_held_to_their_schema() -> anyhow::Result<()> {
        let rules = parcel_rules()?;
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE appended (pnu VARCHAR, kind VARCHAR, area_m2 BIGINT, zonings_json VARCHAR, \
               price_json VARCHAR, characteristics_json VARCHAR, forest_ledger_json VARCHAR, \
               transfer_history_json VARCHAR, land_rights_json VARCHAR, land_right_total BIGINT, \
               lineage_json VARCHAR, attached_via_json VARCHAR, legal_dong_json VARCHAR, \
               version_seq BIGINT, row_state VARCHAR, change VARCHAR); \
             INSERT INTO appended VALUES \
               ('9999900000100000000', NULL, 512, \
                '[{\"zone_code\":\"UQA320\",\"zone_name\":\"synthetic zone\",\"anchor_code\":\"UQA320\",\"inclusion_code\":\"1\"}]', \
                '{\"price_per_m2\":123000,\"base_year\":2026,\"base_month\":1,\"announced_date\":\"2026-01-01\"}', \
                NULL, NULL, '[]', '[]', 0, NULL, NULL, NULL, 2, NULL, 'changed'), \
               ('9999900000100010000', NULL, NULL, '[{\"zone_code\":1}]', NULL, NULL, NULL, '[]', '[]', \
                0, NULL, NULL, NULL, 2, NULL, 'added'), \
               ('9999900000100020000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
                NULL, 2, 'removed', 'removed');",
        )?;
        let found =
            rules.changed_documents_hold_their_schema(&connection, "gold.parcel_panel_versions")?;
        assert_eq!((found.lane.as_str(), found.rendered), ("parcel", 2));
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
