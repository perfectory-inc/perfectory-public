use super::*;

fn price() -> PriceRow {
    PriceRow {
        pnu: "9999900000100000001".to_owned(),
        dong_name: "101동".to_owned(),
        ho_name: "101호".to_owned(),
        base_date: "20100601".to_owned(),
        price_won: 232_000_000,
    }
}

#[test]
fn copy_preserves_labels_without_turning_them_into_columns() -> anyhow::Result<()> {
    let mut row = price();
    row.dong_name = "동\t이름\n\\N".to_owned();
    let line = row.copy_line("fixture")?;
    assert_eq!(line.split('\t').count(), 6);
    assert_eq!(line.lines().count(), 1);
    assert!(line.contains("동\\t이름\\n\\\\N"));
    Ok(())
}

#[test]
fn invalid_identity_date_and_price_are_refused_before_copy() {
    let mut row = price();
    row.pnu = "bad".to_owned();
    assert!(row.copy_line("fixture").is_err());
    row = price();
    for date in ["2010", "2010-06-01", "2010060x", "２０１００６０１", ""] {
        row.base_date = date.to_owned();
        assert!(row.copy_line("fixture").is_err());
    }
    row = price();
    row.price_won = -1;
    assert!(row.copy_line("fixture").is_err());
}

fn config_with_snapshot(source_snapshot: &str) -> Config {
    Config {
        database_url: "postgres://ignored".to_owned(),
        container: "ignored".to_owned(),
        iceberg_snapshot: 1,
        source_snapshot: source_snapshot.to_owned(),
    }
}

#[test]
fn price_snapshot_is_parsed_from_the_stamped_lineage() -> anyhow::Result<()> {
    let config = config_with_snapshot("price:42|exclusive:7|vintage:202608");
    assert_eq!(config.price_snapshot()?, 42);
    Ok(())
}

#[test]
fn a_lineage_without_a_positive_price_snapshot_is_refused() {
    for bad in [
        "exclusive:7|vintage:202608",
        "price:0|exclusive:7",
        "price:-3|exclusive:7",
        "price:abc|exclusive:7",
    ] {
        assert!(config_with_snapshot(bad).price_snapshot().is_err(), "{bad}");
    }
}
