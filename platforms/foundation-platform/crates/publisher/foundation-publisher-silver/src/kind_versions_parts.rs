//! The build of one 시도, one 시군구 at a time (root ADR-0615 §7): the memory chunk of every Gold
//! versions lane, here once.
//!
//! The driver loads a 시도's tables; [`build_in_parts`] shows the stage's build one part of them at
//! a time under the same names, so no lane splits its own input and no statement holds a 시도.

use anyhow::{ensure, Context};
use duckdb::Connection;

use crate::kind_versions_stage::{is_plain, KindVersionsStage, PLACE_IDS};

/// The leading digits of a PNU one part of a 시도's build shares: its 시군구.
pub const PART_DIGITS: usize = 5;

/// `build_part(part)`: the one part a build is shown, for a stage that must keep its documents to
/// the part ([`KindVersionsStage::part_tables`]); it is there only while [`build_in_parts`] builds.
pub const BUILD_PART: &str = "build_part";

/// The suffix a 시도's table carries while a view of one part has its name.
const WHOLE_REGION: &str = "__region";

/// [`KindVersionsStage::build`] over the loaded 시도, one 시군구 at a time; returns the parts built.
///
/// A statement over a whole 시도 holds what does not spill (an ordered aggregate, a list) in
/// memory at once: on 2026-11-23 the ownership shadow passed its 4.3 GiB four times out of four
/// (15,345,613 owner rows in one 시도; its largest 시군구 has 1,299,963) and never had a first
/// commit. So every kind is built in parts, here, and no stage carries its own split.
///
/// # Errors
/// Returns the stage's refusal, or an error when `DuckDB` refuses a statement.
pub fn build_in_parts(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    all: bool,
    source_snapshot_id: &str,
    published_at_utc: &str,
) -> anyhow::Result<u64> {
    build_by_prefix(
        stage,
        connection,
        all,
        source_snapshot_id,
        published_at_utc,
        PART_DIGITS,
    )
}

/// [`build_in_parts`] with parts of `digits` leading digits (a test's PNUs share their first five).
///
/// The stage reads the same names: each table of the 시도 that has a `pnu` column (the served
/// parcels, the row sets, what `load_more` left, the place ids) is renamed aside and a view of one
/// part's rows takes its name, then the tables take their names back, also when a part fails.
/// A partial run builds only the parts `targets` names.
pub(crate) fn build_by_prefix(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    all: bool,
    source_snapshot_id: &str,
    published_at_utc: &str,
    digits: usize,
) -> anyhow::Result<u64> {
    if stage.whole_region_reason().is_some() {
        stage.build(connection, all, source_snapshot_id, published_at_utc)?;
        return Ok(1);
    }
    let mut names = vec!["served".to_owned()];
    names.extend(stage.row_sets().iter().map(|set| set.name.to_owned()));
    names.extend(stage.pnu_spools().iter().map(|name| (*name).to_owned()));
    names.extend(stage.part_tables().iter().map(|name| (*name).to_owned()));
    names.push(PLACE_IDS.to_owned());
    names.sort();
    names.dedup();
    // Each table with the expression of the part a row of it belongs to: its `part` column when it
    // has one, the first digits of its PNU otherwise.
    let pnu_part = format!("substr(pnu, 1, {digits})");
    let mut tables: Vec<(String, &str)> = Vec::new();
    for name in names {
        ensure!(is_plain(&name), "{name:?} is not a plain table name");
        let columns = |column: &str| -> anyhow::Result<bool> {
            let held: i64 = connection.query_row(
                "SELECT count(*) FROM information_schema.columns c \
                 JOIN information_schema.tables t USING (table_schema, table_name) \
                 WHERE t.table_schema = 'main' AND t.table_type = 'BASE TABLE' \
                   AND c.table_name = ? AND c.column_name = ?",
                [name.as_str(), column],
                |row| row.get(0),
            )?;
            Ok(held == 1)
        };
        let (by_part, by_pnu) = (columns("part")?, columns("pnu")?);
        if by_part {
            tables.push((name, "part"));
        } else if by_pnu {
            tables.push((name, pnu_part.as_str()));
        }
    }
    let prefixes = tables
        .iter()
        .map(|(name, part)| format!("SELECT {part} AS part FROM {name}"))
        .collect::<Vec<_>>()
        .join(" UNION ");
    let targeted = if all {
        String::new()
    } else {
        format!(" WHERE part IN (SELECT substr(pnu, 1, {digits}) FROM targets)")
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE build_parts AS \
         SELECT part, row_number() OVER (ORDER BY part NULLS FIRST) AS part_no \
         FROM ({prefixes}){targeted};"
    ))?;
    let parts: i64 =
        connection.query_row("SELECT count(*) FROM build_parts", [], |row| row.get(0))?;
    for (name, _) in &tables {
        connection.execute_batch(&format!(
            "ALTER TABLE {name} RENAME TO {name}{WHOLE_REGION};"
        ))?;
    }
    let built = (1..=parts).try_for_each(|number| {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {BUILD_PART} AS \
             SELECT part FROM build_parts WHERE part_no = {number};"
        ))?;
        for (name, part) in &tables {
            connection.execute_batch(&format!(
                "CREATE OR REPLACE VIEW {name} AS SELECT * FROM {name}{WHOLE_REGION} \
                 WHERE {part} IS NOT DISTINCT FROM (SELECT part FROM {BUILD_PART});"
            ))?;
        }
        stage
            .build(connection, all, source_snapshot_id, published_at_utc)
            .with_context(|| format!("part {number} of {parts} of the 시도"))
    });
    for (name, _) in &tables {
        connection.execute_batch(&format!(
            "DROP VIEW IF EXISTS {name}; ALTER TABLE {name}{WHOLE_REGION} RENAME TO {name};"
        ))?;
    }
    connection.execute_batch(&format!(
        "DROP TABLE build_parts; DROP TABLE IF EXISTS {BUILD_PART};"
    ))?;
    built?;
    Ok(u64::try_from(parts)?)
}

#[cfg(test)]
mod part_table_tests {
    use super::*;
    use crate::kind_versions_stage::{RowSet, PARCEL_SOURCE};

    /// A kind whose documents read rows that carry no PNU of their own part: a deal is spooled once
    /// for every part that reads it.
    struct Deals;

    impl KindVersionsStage for Deals {
        fn section_column(&self) -> &'static str {
            "deals_json"
        }

        fn inputs(&self) -> Vec<&'static str> {
            vec![PARCEL_SOURCE]
        }

        fn code_version(&self) -> String {
            "test".to_owned()
        }

        fn row_sets(&self) -> Vec<RowSet> {
            Vec::new()
        }

        fn part_tables(&self) -> Vec<&'static str> {
            vec!["deals"]
        }

        fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
            connection.execute_batch(
                "CREATE OR REPLACE TABLE built (pnu VARCHAR, deals_json VARCHAR);",
            )?;
            Ok(())
        }

        fn build(
            &self,
            connection: &Connection,
            _all: bool,
            _source_snapshot_id: &str,
            _published_at_utc: &str,
        ) -> anyhow::Result<()> {
            // Only the documents of the part: the deals it is shown may be placed anywhere.
            connection.execute_batch(&format!(
                "INSERT INTO built SELECT placed_pnu, CAST(count(*) AS VARCHAR) FROM deals \
                 WHERE substr(placed_pnu, 1, 8) = (SELECT part FROM {BUILD_PART}) GROUP BY placed_pnu;"
            ))?;
            Ok(())
        }
    }

    #[test]
    fn a_part_is_shown_the_rows_spooled_for_it_whatever_part_they_came_from() -> anyhow::Result<()>
    {
        let connection = Connection::open_in_memory()?;
        let (here, there) = ("9999910100100010000", "9999910300100010000");
        // Part 99999101 reads its own deal and one written under the other part's code; the other
        // part reads its own.
        connection.execute_batch(&format!(
            "CREATE TABLE deals (part VARCHAR, written_under VARCHAR, placed_pnu VARCHAR); \
             INSERT INTO deals VALUES ('99999101', '99999101', '{here}'), \
               ('99999101', '99999103', '{here}'), ('99999103', '99999103', '{there}');"
        ))?;
        Deals.create_built(&connection)?;
        let parts = build_by_prefix(
            &Deals,
            &connection,
            true,
            "999990000000000001",
            "2099-03-01T00:00:00Z",
            8,
        )?;
        assert_eq!(parts, 2);
        let built = connection
            .prepare("SELECT pnu, deals_json FROM built ORDER BY pnu")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            built,
            [
                (here.to_owned(), "2".to_owned()),
                (there.to_owned(), "1".to_owned())
            ]
        );
        let left: i64 = connection.query_row(
            &format!(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('{BUILD_PART}', 'build_parts')"
            ),
            [],
            |row| row.get(0),
        )?;
        assert_eq!(left, 0);
        Ok(())
    }
}
