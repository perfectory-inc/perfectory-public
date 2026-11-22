//! Every contract says how its rows pile up, in terms of its own columns (root ADR-0286).

use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseRowKey, LakehouseRowVersions,
    LakehouseTableContract,
};

/// The columns a declaration names, and the reasons it gives.
fn named(contract: &LakehouseTableContract) -> (Vec<&'static str>, Vec<&'static str>) {
    match contract.row_versions {
        LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Columns(key),
        } => (key.to_vec(), Vec::new()),
        LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Keyless { reason },
        }
        | LakehouseRowVersions::Unwritten { reason } => (Vec::new(), vec![reason]),
        LakehouseRowVersions::LatestPerKey(latest) => {
            let mut columns = [latest.key, latest.newest_first].concat();
            columns.extend(latest.removal_column);
            (columns, Vec::new())
        }
        LakehouseRowVersions::Editions {
            edition_column,
            newest_first,
            per,
            ..
        } => {
            assert!(
                !newest_first.is_empty(),
                "{}: an edition order is required",
                contract.table_name
            );
            ([&[edition_column], newest_first, per].concat(), Vec::new())
        }
        LakehouseRowVersions::Events { .. } => (Vec::new(), Vec::new()),
    }
}

#[test]
fn every_declaration_names_only_its_own_columns_and_says_why_when_it_has_no_key() {
    for contract in industrial_complex_lakehouse_contracts() {
        let (columns, reasons) = named(contract);
        for column in columns {
            assert!(
                contract
                    .columns
                    .iter()
                    .any(|declared| declared.name == column),
                "{}: row_versions names {column}, which is not one of its columns",
                contract.table_name
            );
        }
        for reason in reasons {
            // A reason names the source or the planned work, not a placeholder.
            assert!(
                reason.split_whitespace().count() >= 5,
                "{}: say why in a sentence, not {reason:?}",
                contract.table_name
            );
        }
        if let LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Columns(key),
        } = contract.row_versions
        {
            assert!(
                !key.is_empty(),
                "{}: an empty key is Keyless with a reason",
                contract.table_name
            );
        }
    }
}

#[test]
fn the_newest_per_key_rule_is_the_latest_per_key_declaration() {
    for contract in industrial_complex_lakehouse_contracts() {
        assert_eq!(
            contract.current_row_latest().is_some(),
            matches!(contract.row_versions, LakehouseRowVersions::LatestPerKey(_)),
            "{}",
            contract.table_name
        );
    }
}

#[test]
fn the_newest_edition_is_the_one_whose_order_is_greatest_and_a_tie_is_refused() {
    use lakehouse_domain::{NewestEditionError, REFERENCE_LEGAL_DONG_CODE_SNAPSHOT as CODES};
    use serde_json::{json, Map, Value};

    let row = |date: &str, code: &str| -> Map<String, Value> {
        json!({"snapshot_date": date, "region_cd": code})
            .as_object()
            .cloned()
            .unwrap_or_default()
    };
    let rows = [
        row("2099-01-01", "9900000000"),
        row("2099-02-01", "9900000000"),
        row("2099-02-01", "9800000000"),
    ];
    let newest = CODES.newest_edition_rows(&rows);
    assert_eq!(newest.as_ref().map(Vec::len), Ok(2));
    assert!(newest
        .unwrap_or_default()
        .iter()
        .all(|row| row["snapshot_date"] == "2099-02-01"));
    assert_eq!(CODES.newest_edition_rows(&[]).map(|rows| rows.len()), Ok(0));
    // A table that keeps versions per key has no editions to pick.
    let units = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
        "silver.building_register_units",
    );
    assert!(matches!(
        units.map(|contract| contract.newest_edition_rows(&rows)),
        Some(Err(NewestEditionError::NotEditions(_)))
    ));
}

#[test]
fn two_editions_tied_on_the_order_are_refused_and_the_factory_orders_by_its_day() {
    use lakehouse_domain::{
        NewestEditionError, SILVER_ADMINISTRATIVE_BOUNDARIES as ADMIN,
        SILVER_REGISTERED_FACTORY as FACTORY,
    };
    use serde_json::{json, Map, Value};

    let row = |edition: &str, at: &str| -> Map<String, Value> {
        json!({"source_snapshot_id": edition, "ingested_at_utc": at})
            .as_object()
            .cloned()
            .unwrap_or_default()
    };
    // Two administrative rounds loaded at one instant: nothing orders them.
    let tied = [
        row("round-a", "2099-12-31T00:00:00Z"),
        row("round-b", "2099-12-31T00:00:00Z"),
    ];
    assert!(matches!(
        ADMIN.newest_edition_rows(&tied),
        Err(NewestEditionError::Tied { .. })
    ));
    // An older 기준일 loaded later does not outrank a newer one (root ADR-0328).
    let older_loaded_later = [
        row("20990630", "2099-12-31T00:00:00Z"),
        row("20991231", "2099-07-01T00:00:00Z"),
    ];
    assert_eq!(
        FACTORY
            .newest_edition_rows(&older_loaded_later)
            .map(|rows| rows
                .iter()
                .map(|row| row["source_snapshot_id"].clone())
                .collect::<Vec<_>>()),
        Ok(vec![json!("20991231")])
    );
}

/// Root ADR-0318 §8: the official parcel-number history appends only the events the table does not
/// hold, so a 시도's newest file holds a few rows. Read as editions (its declaration until
/// 2026-11-20) the second load hid the first: in production 1,669 of the pairing window's 3,702
/// links, in 7 시도, after the 2026-11-15 load.
#[test]
fn a_load_of_the_parcel_number_history_that_appends_a_few_rows_hides_none_of_the_earlier_ones() {
    use lakehouse_domain::{
        LakehouseEditionRead, NewestEditionError, SILVER_PARCEL_NUMBER_CHANGE_HISTORY as HISTORY,
    };
    use serde_json::{json, Map, Value};

    let row = |old: &str, edition: &str, updated: &str| -> Map<String, Value> {
        json!({"old_pnu": old, "source_region_code": "99", "source_snapshot_id": edition,
               "provider_updated_on": updated})
        .as_object()
        .cloned()
        .unwrap_or_default()
    };
    // The first load appends the file whole; the next month's file repeats it and adds one event,
    // and the load appends that one row.
    let rows = [
        row("9999910100100010000", "file-1@2099-01-15", "2099-01-15"),
        row("9999910100100020000", "file-1@2099-01-15", "2099-01-15"),
        row("9999910100100030000", "file-1@2099-01-15", "2099-01-15"),
        row("9999910100100040000", "file-1@2099-02-15", "2099-02-15"),
    ];
    // Planted: the declaration it had. The newest edition of the 시도 is the one row.
    let as_editions = LakehouseTableContract {
        row_versions: LakehouseRowVersions::Editions {
            edition_column: "source_snapshot_id",
            newest_first: &["provider_updated_on"],
            per: &["source_region_code"],
            read: LakehouseEditionRead::Newest,
        },
        ..HISTORY
    };
    assert_eq!(
        as_editions
            .newest_edition_rows(&rows)
            .map(|rows| rows.len()),
        Ok(1)
    );
    // Every row is an event and every row is current: there is no edition to pick, and the one
    // reader folds them all.
    assert!(matches!(
        HISTORY.newest_edition_rows(&rows),
        Err(NewestEditionError::NotEditions(_))
    ));
    assert!(matches!(
        HISTORY.row_versions,
        LakehouseRowVersions::Events {
            canonical_reader: Some("vworld_parcel_number_change_history.official_links")
        }
    ));
}

/// Root ADR-0564 §4: the SQL twin of the newest edition exists exactly for the tables read by
/// their newest edition, and chooses the edition before the caller's filter (the factories read
/// it in `DuckDB`; `factory_kind_stage` holds it to `newest_edition_rows` on rows).
#[test]
fn the_newest_edition_sql_is_rendered_only_for_editions_read_newest_first() {
    for contract in industrial_complex_lakehouse_contracts() {
        let rendered = contract.newest_edition_sql("t", &["a"], "f");
        let newest = matches!(
            contract.row_versions,
            LakehouseRowVersions::Editions {
                read: lakehouse_domain::LakehouseEditionRead::Newest,
                ..
            }
        );
        assert_eq!(rendered.is_some(), newest, "{}", contract.table_name);
    }
    let factory = lakehouse_domain::SILVER_REGISTERED_FACTORY
        .newest_edition_sql("t", &["a"], "f")
        .unwrap_or_default();
    assert_eq!(
        factory,
        "SELECT a FROM (SELECT *, dense_rank() OVER ( ORDER BY foundation_edition_newest_0 DESC NULLS LAST, \
         foundation_edition_newest_1 DESC NULLS LAST) AS foundation_edition_rank FROM (SELECT *, \
         max(source_snapshot_id) OVER (PARTITION BY source_snapshot_id) AS foundation_edition_newest_0, \
         max(ingested_at_utc) OVER (PARTITION BY source_snapshot_id) AS foundation_edition_newest_1 FROM t)) \
         WHERE foundation_edition_rank = 1 AND (f)"
    );
}
