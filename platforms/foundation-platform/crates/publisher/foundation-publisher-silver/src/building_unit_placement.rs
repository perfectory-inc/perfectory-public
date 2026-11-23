//! In `DuckDB`, written once for the Golds that place units.
//!
//! A unit is served under its building's PNU when its link to that building has evidence, under its
//! own PNU when it has none: the twin of `building_panel_silver_to_gold.unit_placement` and
//! `building_link_evidence.verified_building_links`, for the unit price Gold now and the building
//! panel when it moves off Spark.
//!
//! The evidence rule is the building unit handoff contract's (`relationship_evidence_policy`), the one
//! the Spark writer and the serving boundary (`foundation_publisher_core::building_link_evidence`)
//! read: a link stands when the unit names its row, the method and no refusal, and either the source
//! method's record and input SHA-256 (no application) or an approved application id (no record, no
//! digest); a link without that evidence is dropped and the unit is served under its own PNU. The
//! catalog ids are the catalog domain's `UUIDv8` (`catalog_domain::building_id_for_register_pk`,
//! `building_unit_id_for_register_pk`): the SHA-256 of `perfectory.catalog.<kind>.v1`, a NUL and the
//! register key, its first 16 bytes with the version 8 and the RFC 4122 variant.

use anyhow::Context;
use serde::Deserialize;

const HANDOFF_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/building-unit-handoff.json");

/// The nil UUID an application id may not be.
const NIL_UUID: &str = "00000000-0000-0000-0000-000000000000";

#[derive(Deserialize)]
struct Handoff {
    relationship_evidence_policy: Policy,
}

/// The handoff contract's relationship evidence policy.
#[derive(Clone, Debug, Deserialize)]
pub struct Policy {
    /// The method a link derived from the source register carries.
    pub source_method: String,
    /// What a link's input SHA-256 must match.
    pub sha256_pattern: String,
    /// What an approved application id must match.
    pub application_id_pattern: String,
    /// The evidence columns a unit row carries.
    pub columns: Vec<String>,
}

/// The policy of the building unit handoff contract.
///
/// # Errors
/// Returns an error when the contract does not parse.
pub fn policy() -> anyhow::Result<Policy> {
    Ok(serde_json::from_str::<Handoff>(HANDOFF_CONTRACT)
        .context("the building unit handoff contract has no relationship_evidence_policy")?
        .relationship_evidence_policy)
}

/// `text` as a SQL string literal.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A text column of `alias` as the evidence reads it: trimmed of spaces, an empty one null.
fn text(alias: &str, column: &str) -> String {
    format!(
        "CASE WHEN trim({alias}.{column}, ' ') = '' THEN NULL ELSE trim({alias}.{column}, ' ') END"
    )
}

/// Whether the building link of the unit row `alias` has the evidence it needs
/// (`building_link_evidence.evidence_expressions`'s `valid`), as a SQL condition.
///
/// # Errors
/// Returns an error when the contract does not parse or names a column the rule does not know.
pub fn evidence_valid_sql(alias: &str) -> anyhow::Result<String> {
    let policy = policy()?;
    for needed in [
        "unit_row_id",
        "building_link_method",
        "building_link_source_record_id",
        "building_link_input_sha256",
        "building_link_reason",
        "normalization_application_id",
    ] {
        anyhow::ensure!(
            policy.columns.iter().any(|column| column == needed),
            "the evidence policy does not list {needed}"
        );
    }
    let method = text(alias, "building_link_method");
    let source = text(alias, "building_link_source_record_id");
    let digest = text(alias, "building_link_input_sha256");
    let reason = text(alias, "building_link_reason");
    let application = text(alias, "normalization_application_id");
    let unit_row = text(alias, "unit_row_id");
    let source_valid = format!(
        "(({method}) = {} AND ({source}) IS NOT NULL AND regexp_matches(({digest}), {}) \
         AND ({application}) IS NULL)",
        literal(&policy.source_method),
        literal(&policy.sha256_pattern)
    );
    let approved = format!(
        "(regexp_matches(({application}), {}) AND ({application}) <> {} AND ({source}) IS NULL \
         AND ({digest}) IS NULL)",
        literal(&policy.application_id_pattern),
        literal(NIL_UUID)
    );
    let parent = format!("{alias}.building_mgm_bldrgst_pk");
    Ok(format!(
        "(({parent} IS NULL OR (length({parent}) > 0 AND {parent} = trim({parent}, ' '))) \
         AND ({unit_row}) IS NOT NULL AND ({method}) IS NOT NULL AND ({reason}) IS NULL \
         AND coalesce({source_valid} OR {approved}, FALSE))"
    ))
}

/// The SHA-256 (hex) a catalog id of the id kind `kind` (`building`, `building_unit`) is cut from,
/// for the register key `key` (a SQL expression).
#[must_use]
pub fn catalog_digest_sql(kind: &str, key: &str) -> String {
    format!(
        "sha256({} || chr(0) || {key})",
        literal(&format!("perfectory.catalog.{kind}.v1"))
    )
}

/// The catalog id cut from the digest column `digest` ([`catalog_digest_sql`]), as
/// `catalog_domain`'s `UUIDv8`: the first 16 bytes, version 8, the RFC 4122 variant.
#[must_use]
pub fn catalog_id_sql(digest: &str) -> String {
    format!(
        "(substr({digest}, 1, 8) || '-' || substr({digest}, 9, 4) || '-8' || substr({digest}, 14, 3) \
         || '-' || lower(printf('%x', (CAST(('0x' || substr({digest}, 17, 1)) AS INTEGER) & 3) + 8)) \
         || substr({digest}, 18, 3) || '-' || substr({digest}, 21, 12))"
    )
}

/// `placed(mgm_bldrgst_pk, serving_pnu, building_id, unit_id)` from `units` (the unit rows: `pnu`
/// trimmed and present, `mgm_bldrgst_pk`, `building_mgm_bldrgst_pk` and the evidence columns) and
/// `titles` (`mgm_bldrgst_pk`, `pnu`, `id`, one row per key): each unit with the building its
/// evidenced link names (`building_id` null when none) and the PNU whose document carries it.
const PLACEMENT_SQL: &str = "\
CREATE OR REPLACE TABLE @placed@ AS \
SELECT u.mgm_bldrgst_pk, \
  CASE WHEN t.id IS NOT NULL THEN t.pnu ELSE u.pnu END AS serving_pnu, \
  t.id AS building_id, @unit_id@ AS unit_id \
FROM (SELECT *, @unit_digest@ AS unit_digest FROM @units@) u \
LEFT JOIN @titles@ t ON t.mgm_bldrgst_pk = CASE WHEN @valid@ \
  THEN CASE WHEN trim(u.building_mgm_bldrgst_pk, ' ') = '' THEN NULL \
    ELSE trim(u.building_mgm_bldrgst_pk, ' ') END END;";

/// [`PLACEMENT_SQL`] as it runs for these tables: the evidence rule and the unit ids filled in.
fn placement_sql(units: &str, titles: &str, placed: &str) -> anyhow::Result<String> {
    for name in [units, titles, placed] {
        anyhow::ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
            "{name:?} is not a plain table name"
        );
    }
    Ok(PLACEMENT_SQL
        .replace("@placed@", placed)
        .replace("@units@", units)
        .replace("@titles@", titles)
        .replace("@valid@", &evidence_valid_sql("u")?)
        .replace(
            "@unit_digest@",
            &catalog_digest_sql("building_unit", "mgm_bldrgst_pk"),
        )
        .replace("@unit_id@", &catalog_id_sql("u.unit_digest")))
}

/// Leaves `placed` from `units` and `titles` (see [`PLACEMENT_SQL`]).
///
/// # Errors
/// Returns an error when the policy does not parse or `DuckDB` refuses the statement.
pub fn place(
    connection: &duckdb::Connection,
    units: &str,
    titles: &str,
    placed: &str,
) -> anyhow::Result<()> {
    connection.execute_batch(&placement_sql(units, titles, placed)?)?;
    Ok(())
}

/// The texts a Gold that places units folds into its code version: the placement as it runs (so a
/// change of the evidence rule or of the ids moves it) and the contract it reads.
#[must_use]
pub fn code_texts() -> [String; 2] {
    [
        placement_sql("units", "titles", "placed").unwrap_or_else(|error| error.to_string()),
        HANDOFF_CONTRACT.to_owned(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// A unit with source evidence hangs off its building; one whose evidence is incomplete, one
    /// with a refusal and one without a building are served under their own PNU.
    #[test]
    fn a_unit_hangs_off_its_building_only_with_evidence() -> anyhow::Result<()> {
        let connection = duckdb::Connection::open_in_memory()?;
        connection.execute_batch(&format!(
            "CREATE TABLE titles (mgm_bldrgst_pk VARCHAR, pnu VARCHAR, id VARCHAR); \
             INSERT INTO titles VALUES ('b1', '9999910100100010000', 'building-1'); \
             CREATE TABLE units (mgm_bldrgst_pk VARCHAR, pnu VARCHAR, building_mgm_bldrgst_pk VARCHAR, \
               unit_row_id VARCHAR, building_link_method VARCHAR, building_link_source_record_id VARCHAR, \
               building_link_input_sha256 VARCHAR, building_link_reason VARCHAR, \
               normalization_application_id VARCHAR); \
             INSERT INTO units VALUES \
               ('u1', '9999910100100020000', 'b1', 'r1', 'parent_key', 'rec', '{SHA}', NULL, NULL), \
               ('u2', '9999910100100020000', 'b1', 'r2', 'parent_key', 'rec', 'short', NULL, NULL), \
               ('u3', '9999910100100020000', 'b1', 'r3', 'parent_key', 'rec', '{SHA}', 'refused', NULL), \
               ('u4', '9999910100100030000', NULL, 'r4', NULL, NULL, NULL, NULL, NULL), \
               ('u5', '9999910100100020000', 'b1', 'r5', 'steward', NULL, NULL, NULL, \
                '12345678-1234-1234-1234-123456789abc');"
        ))?;
        place(&connection, "units", "titles", "placed")?;
        let rows: Vec<(String, String, Option<String>)> = connection
            .prepare("SELECT mgm_bldrgst_pk, serving_pnu, building_id FROM placed ORDER BY 1")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_, _>>()?;
        let linked = |pk: &str| {
            (
                pk.to_owned(),
                "9999910100100010000".to_owned(),
                Some("building-1".to_owned()),
            )
        };
        let own = |pk: &str, pnu: &str| (pk.to_owned(), pnu.to_owned(), None);
        assert_eq!(
            rows,
            [
                linked("u1"),
                own("u2", "9999910100100020000"),
                own("u3", "9999910100100020000"),
                own("u4", "9999910100100030000"),
                linked("u5"),
            ]
        );
        Ok(())
    }

    /// The SQL id is the catalog domain's construction (`catalog_domain::building_id_for_register_pk`
    /// and `building_unit_id_for_register_pk` give these two; the outbox publisher, which links
    /// `catalog-domain`, holds the SQL to them).
    #[test]
    fn a_catalog_id_is_the_catalog_domain_s_uuid_v8() -> anyhow::Result<()> {
        let connection = duckdb::Connection::open_in_memory()?;
        for (kind, key, expected) in [
            (
                "building",
                "99999-000000001",
                "1a4070a2-e72f-8c5e-8d21-f507e177d791",
            ),
            (
                "building_unit",
                "99999-000000002",
                "3662f666-59c2-8383-b603-04d3351479d4",
            ),
        ] {
            let id: String = connection.query_row(
                &format!(
                    "SELECT {} FROM (SELECT {} AS d)",
                    catalog_id_sql("d"),
                    catalog_digest_sql(kind, &literal(key))
                ),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(id, expected, "{kind}");
        }
        Ok(())
    }
}
