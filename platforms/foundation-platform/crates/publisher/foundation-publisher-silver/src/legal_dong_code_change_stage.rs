//! The views of `reference.legal_dong_code_change` a `DuckDB` stage reads (root ADR-0145, ADR-0329).
//!
//! The SQL twins of `legal_dong_code_change_views.py`: every stage that reads the code change
//! table runs these texts, so the rule of which pairs are in force and which codes a renumbering
//! translates is written once on this side.

/// `pair_rows`: the pairs in force of `code_changes` (`legal_dong_code_change_views.pair_rows`).
pub const PAIR_ROWS_SQL: &str = include_str!("legal_dong_code_change_stage/pair_rows.sql");

/// `renumbering(src, dst)` from `pair_rows` and `served_codes(code)`
/// (`legal_dong_code_change_views.renumbered_into`).
pub const RENUMBERED_INTO_SQL: &str =
    include_str!("legal_dong_code_change_stage/renumbered_into.sql");

#[cfg(test)]
mod tests {
    use super::*;

    /// The Python view's cases: a 1:1 pair translates either way into the served side; a split, a
    /// merge and a code the served edition holds are not translated.
    #[test]
    fn a_one_to_one_pair_translates_into_the_served_code_and_nothing_else_does(
    ) -> anyhow::Result<()> {
        let connection = duckdb::Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE code_changes AS SELECT * FROM (VALUES \
               ('pair', '9999910100', '9999920100', 'eupmyeondong', '2099-01-01', 'official:x'), \
               ('pair', '9999910200', '9999920200', 'eupmyeondong', '2099-01-01', 'official:x'), \
               ('pair', '9999910300', '9999920300', 'ri', '2099-01-01', 'official:x'), \
               ('pair', '9999910300', '9999920301', 'ri', '2099-01-01', 'official:x'), \
               ('pair', '9999910400', '9999920400', 'eupmyeondong', '2099-01-01', 'official:x'), \
               ('pair', '9999910401', '9999920400', 'eupmyeondong', '2099-01-01', 'official:x'), \
               ('pair', '9999910500', '9999920500', 'eupmyeondong', '2099-01-01', 'derived:children'), \
               ('supersession', '9999910500', '9999920500', 'eupmyeondong', '', 'official:x'), \
               ('pair', '9999910600', '9999920600', 'sigungu', '2099-01-01', 'official:x') \
             ) t(kind, old_code, new_code, level, effective_date, source); \
             CREATE TABLE served_codes AS SELECT * FROM (VALUES ('9999920100'), ('9999910200'), \
               ('9999920200'), ('9999920300'), ('9999920301'), ('9999920400'), ('9999920500'), \
               ('9999920600')) t(code);",
        )?;
        connection.execute_batch(PAIR_ROWS_SQL)?;
        connection.execute_batch(RENUMBERED_INTO_SQL)?;
        let pairs = connection
            .prepare("SELECT src, dst FROM renumbering ORDER BY src")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        // 10100 -> 20100 only: 10200 and 20200 are both served (never translated), 10300 split,
        // 10400/10401 merged, 10500's pair withdrawn, 10600 not a leaf level.
        assert_eq!(pairs, [("9999910100".to_owned(), "9999920100".to_owned())]);
        Ok(())
    }
}
