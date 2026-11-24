// Fixture for tools/semgrep/rules/lakehouse-time-travel.yml (`opengrep scan --test`). Not compiled.

fn pinned_by_hand(table: &str, head: i64) -> String {
    // ruleid: a-lakehouse-table-is-read-at-a-snapshot-through-table-at
    format!("{LAKEHOUSE_CATALOG}.{table} AT (VERSION => {head})")
}

fn inline(head: i64) -> String {
    // ruleid: a-lakehouse-table-is-read-at-a-snapshot-through-table-at
    format!("SELECT count(*) FROM lakehouse.silver.parcels AT (VERSION => {}) r", head)
}

fn through_the_one_function(table: &str, head: i64) -> String {
    // ok: a-lakehouse-table-is-read-at-a-snapshot-through-table-at
    format!("SELECT count(*) FROM {} r", table_at(table, head))
}

fn a_tests_expected_text() -> &'static str {
    // ok: a-lakehouse-table-is-read-at-a-snapshot-through-table-at
    "(SELECT * FROM lakehouse.silver.parcels AT (VERSION => 42))"
}

// A SQL file is held to the same rule (`paths.include` names `*.sql`); its caller fills a placeholder
// from table_at. The engine's test annotations do not read `--` comments, so the form is shown here.
const INPUTS_SQL_BY_HAND: &str =
    // ruleid: a-lakehouse-table-is-read-at-a-snapshot-through-table-at
    "SELECT DISTINCT pnu FROM {lakehouse}.silver.parcel_boundaries AT (VERSION => {boundaries_from})";
