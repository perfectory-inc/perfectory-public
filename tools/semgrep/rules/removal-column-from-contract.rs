// Fixture for tools/semgrep/rules/removal-column-from-contract.yml (`opengrep scan --test`). Not compiled.

fn the_follow_of_2026_11_27(current: &str, affected: &str) -> String {
    // ruleid: a-silver-lane-takes-the-removal-column-from-its-contract
    format!("SELECT * FROM ({current}) c WHERE c.row_state IS NULL AND ({affected})")
}

fn a_marker_by_name(columns: &[&str]) -> Vec<String> {
    columns
        .iter()
        .map(|column| match *column {
            // ruleid: a-silver-lane-takes-the-removal-column-from-its-contract
            "row_state" => format!("'{REMOVED}' AS row_state"),
            other => format!("c.{other}"),
        })
        .collect()
}

fn markers_counted_by_name() -> &'static str {
    // ruleid: a-silver-lane-takes-the-removal-column-from-its-contract
    "SELECT count(*) FROM onbid_changes WHERE row_state = 'removed'"
}

fn a_marker_from_the_contract(contract: &LakehouseTableContract, columns: &[&str]) -> Vec<String> {
    let removal = silver_changes::removal_column(contract);
    columns
        .iter()
        .map(|column| match *column {
            // ok: a-silver-lane-takes-the-removal-column-from-its-contract
            name if removal == Some(name) => format!("'{REMOVED}' AS {name}"),
            other => format!("c.{other}"),
        })
        .collect()
}

fn markers_counted_from_the_contract(contract: &LakehouseTableContract) -> String {
    // ok: a-silver-lane-takes-the-removal-column-from-its-contract
    format!("SELECT count(*) FROM onbid_changes WHERE {}", silver_changes::removed(contract))
}

fn another_state_column() -> &'static str {
    // ok: a-silver-lane-takes-the-removal-column-from-its-contract
    "SELECT item_state, row_states FROM items"
}
