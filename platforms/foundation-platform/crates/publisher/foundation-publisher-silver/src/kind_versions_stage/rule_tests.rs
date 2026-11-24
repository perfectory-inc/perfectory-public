use super::*;

fn found_and_allowed(rule: &CurrentRule, rows: &str) -> anyhow::Result<(i64, i64)> {
    let connection = Connection::open_in_memory()?;
    let dir = tempfile::tempdir()?;
    let dir_text = sql_dir(dir.path())?;
    let carried = (0..rule.carried.len())
        .map(|index| format!("{TIE_PREFIX}{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {rows}) v({carried})) TO '{dir_text}/set.parquet' (FORMAT parquet);"
    ))?;
    Ok(connection.query_row(
        &rule.check_sql(&format!("'{dir_text}/set.parquet'")),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

#[test]
fn two_editions_tied_on_the_order_are_refused() -> anyhow::Result<()> {
    let boundaries = contract("silver.administrative_boundaries")?;
    let rule = CurrentRule::of(boundaries)?;
    assert_eq!(
        (rule.carried.as_slice(), rule.group, rule.editions),
        (&["source_snapshot_id"][..], 0, true)
    );
    // Two rounds loaded at one instant: both are the newest, and the check refuses them.
    assert_eq!(
        found_and_allowed(&rule, "('round-a'), ('round-b')")?,
        (2, 1)
    );
    assert_eq!(
        found_and_allowed(&rule, "('round-b'), ('round-b')")?,
        (1, 1)
    );
    Ok(())
}

#[test]
fn each_group_holds_one_newest_edition() -> anyhow::Result<()> {
    let schedule = contract("silver.court_auction_item_schedule")?;
    let rule = CurrentRule::of(schedule)?;
    assert_eq!((rule.carried.len(), rule.group), (4, 3));
    let one_each = "('c', '1', '1', '2099-01-02'), ('c', '2', '1', '2099-01-01')";
    assert_eq!(found_and_allowed(&rule, one_each)?, (2, 2));
    let tied = "('c', '1', '1', '2099-01-02'), ('c', '1', '1', '2099-01-03')";
    assert_eq!(found_and_allowed(&rule, tied)?, (2, 1));
    Ok(())
}

#[test]
fn a_key_read_twice_is_refused_and_a_pinned_edition_table_is_not_a_row_set() -> anyhow::Result<()> {
    let rule = CurrentRule::of(contract("silver.onbid_item_bid_info")?)?;
    assert!(!rule.editions);
    assert_eq!(found_and_allowed(&rule, "('k1'), ('k1')")?, (2, 1));
    assert_eq!(found_and_allowed(&rule, "('k1'), ('k2')")?, (2, 2));
    // The parcels are read by the edition the caller names, never as a row set's newest.
    assert!(CurrentRule::of(contract(PARCEL_SOURCE)?).is_err());
    Ok(())
}
