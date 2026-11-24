//! The release rules on synthetic inputs (PNUs and codes under `99999`), the cases of
//! `test_parcel_place_id_release_spark.py` and `test_place_id_release.py`: an id's newest row, a
//! closed period, an open PNU carried under its current code, a merge's code two ids would take, an
//! id whose code moved, an undated row, the served editions only, and the admin unit's snapshots
//! and pairs. The PNUs are of the reserved range, one 시도: an id that moved 시도 is the takeover
//! verdict's to judge, on real data, as is the rest against the Spark job.

use std::collections::{BTreeMap, BTreeSet};

use duckdb::Connection;

use super::*;

const SERVED: &str = "synthetic__parcel-209906";
const LATER: &str = "synthetic__parcel-209909";

/// One registry row: (`parcel_id`, pnu, status, `valid_from`, `valid_to`, `redirect_to`, recorded,
/// `to_snapshot_id`).
type RegistryRow = (
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
    &'static str,
    &'static str,
);

const T1: &str = "2099-06-01 00:00:00+00";
const T2: &str = "2099-09-01 00:00:00+00";

/// The Spark test's rows, and a row of an edition the served one does not hold yet.
const ROWS: &[RegistryRow] = &[
    (
        "a",
        "9999910200100070000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "b",
        "9999910200100080000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "b",
        "9999910200100080000",
        "historic",
        Some("2099-06-01"),
        Some("2099-09-01"),
        None,
        T2,
        SERVED,
    ),
    (
        "b",
        "9999910200100090000",
        "current",
        Some("2099-09-01"),
        None,
        None,
        T2,
        SERVED,
    ),
    (
        "c",
        "9999910300100070000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "d",
        "9999910400100070000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "e",
        "9999910500100010000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "e",
        "9999910500100010000",
        "redirected",
        Some("2099-09-01"),
        None,
        Some("a"),
        T2,
        SERVED,
    ),
    // An id whose code later moved keeps the region that issued it (the reserved range holds one
    // 시도, so the move across 시도 the rule is for is the verdict's to judge on real data).
    (
        "m",
        "9999910600100010000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "m",
        "9999910600100010000",
        "historic",
        Some("2099-06-01"),
        Some("2099-09-01"),
        None,
        T2,
        SERVED,
    ),
    (
        "m",
        "9999920600100010000",
        "current",
        Some("2099-09-01"),
        None,
        None,
        T2,
        SERVED,
    ),
    // A row without a date sorts after the dated ones: its id's region is the dated row's.
    (
        "n",
        "9999930700100010000",
        "historic",
        None,
        None,
        None,
        T1,
        SERVED,
    ),
    (
        "n",
        "9999910700100010000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ),
    // Not served yet: the release does not read it.
    (
        "a",
        "9999910200100070000",
        "historic",
        Some("2099-06-01"),
        Some("2099-10-01"),
        None,
        T2,
        LATER,
    ),
];

/// The Spark test's crosswalk: two code changes, and a merge of two old dongs into one.
const PAIRS: &[(&str, &str)] = &[
    ("9999910200", "9999920200"),
    ("9999910300", "9999920300"),
    ("9999910400", "9999920300"),
];

fn text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| format!("'{value}'"))
}

fn connection(rows: &[RegistryRow], pairs: &[(&str, &str)]) -> anyhow::Result<Connection> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE silver_parcel_registry (parcel_id VARCHAR, pnu VARCHAR, status VARCHAR, \
           valid_from VARCHAR, valid_to VARCHAR, redirect_to VARCHAR, to_snapshot_id VARCHAR, \
           id_rules_version VARCHAR, derivation_run_id VARCHAR, recorded_at_utc TIMESTAMPTZ); \
         CREATE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
           new_code VARCHAR, level VARCHAR, effective_date DATE, source VARCHAR);",
    )?;
    for (id, pnu, status, from, to, redirect, recorded, snapshot) in rows {
        connection.execute_batch(&format!(
            "INSERT INTO silver_parcel_registry VALUES ('{id}', '{pnu}', '{status}', {}, {}, {}, \
             '{snapshot}', 'v1', 'run', TIMESTAMPTZ '{recorded}');",
            text(*from),
            text(*to),
            text(*redirect)
        ))?;
    }
    for (old, new) in pairs {
        connection.execute_batch(&format!(
            "INSERT INTO reference_legal_dong_code_change VALUES ('pair', '{old}', '{new}', \
             'dong', DATE '2099-07-01', 'synthetic');"
        ))?;
    }
    Ok(connection)
}

type Registry = BTreeMap<String, (String, String, Option<String>, Option<String>)>;
type Bridge = BTreeMap<(String, String), (Option<String>, Option<String>)>;

/// The parcel unit's release: each id's (scope, status, current code, redirect), and each (id, code)
/// period.
fn parcel_release(
    rows: &[RegistryRow],
    pairs: &[(&str, &str)],
) -> anyhow::Result<(Registry, Bridge)> {
    let connection = connection(rows, pairs)?;
    load_remap(&connection, &Source::Local)?;
    create_built(&connection)?;
    let dir = tempfile::tempdir()?;
    let regions = load_parcel_rows(
        &connection,
        &Source::Local,
        &[SERVED.to_owned()],
        dir.path(),
    )?;
    for scope in regions.keys() {
        build_parcel_scope(&connection, scope)?;
    }
    let registry = connection
        .prepare("SELECT place_id, scope, status, current_code, redirect_to FROM built_registry WHERE unit = 'parcel'")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, (row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)))
        })?
        .collect::<Result<Registry, _>>()?;
    let bridge = connection
        .prepare(
            "SELECT place_id, code, valid_from, valid_to FROM built_bridge WHERE unit = 'parcel'",
        )?
        .query_map([], |row| {
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                (row.get(2)?, row.get(3)?),
            ))
        })?
        .collect::<Result<Bridge, _>>()?;
    Ok((registry, bridge))
}

#[test]
fn the_registry_is_each_ids_newest_row_of_the_served_editions() -> anyhow::Result<()> {
    let (registry, _) = parcel_release(ROWS, PAIRS)?;
    assert_eq!(
        registry["b"],
        (
            "99".to_owned(),
            "current".to_owned(),
            Some("9999910200100090000".to_owned()),
            None
        )
    );
    assert_eq!(
        registry["e"],
        (
            "99".to_owned(),
            "redirected".to_owned(),
            None,
            Some("a".to_owned())
        )
    );
    // `a`'s historic row is of an edition the served one does not hold.
    assert_eq!(registry["a"].1, "current");
    Ok(())
}

#[test]
fn an_ids_region_is_the_one_its_first_dated_row_names() -> anyhow::Result<()> {
    let (registry, _) = parcel_release(ROWS, PAIRS)?;
    assert_eq!(
        registry["m"],
        (
            "99".to_owned(),
            "current".to_owned(),
            Some("9999920600100010000".to_owned()),
            None
        )
    );
    assert_eq!(registry["n"].0, "99");
    assert!(registry.values().all(|(scope, ..)| scope == "99"));
    Ok(())
}

#[test]
fn the_bridge_closes_a_period_and_carries_an_open_pnu_under_its_current_code() -> anyhow::Result<()>
{
    let (_, bridge) = parcel_release(ROWS, PAIRS)?;
    let pair = |id: &str, code: &str| (id.to_owned(), code.to_owned());
    assert_eq!(
        bridge[&pair("b", "9999910200100080000")],
        (Some("2099-06-01".to_owned()), Some("2099-09-01".to_owned()))
    );
    assert_eq!(
        bridge[&pair("b", "9999910200100090000")],
        (Some("2099-09-01".to_owned()), None)
    );
    assert_eq!(
        bridge[&pair("a", "9999920200100070000")],
        (Some(PAIR_SOURCE.to_owned()), Some(PAIR_SOURCE.to_owned()))
    );
    assert!(bridge.contains_key(&pair("b", "9999920200100090000")));
    // A closed period is not carried.
    assert!(!bridge.contains_key(&pair("b", "9999920200100080000")));
    // Two merged old dongs with the same lot would give one new code two ids: neither is bridged.
    assert!(bridge.keys().all(|(_, code)| code != "9999920300100070000"));
    // A redirected id's period closes on its redirect.
    assert_eq!(
        bridge[&pair("e", "9999910500100010000")],
        (Some("2099-06-01".to_owned()), Some("2099-09-01".to_owned()))
    );
    Ok(())
}

#[test]
fn a_code_a_period_holds_already_is_not_carried_again() -> anyhow::Result<()> {
    let mut rows = ROWS.to_vec();
    rows.push((
        "z",
        "9999920200100070000",
        "current",
        Some("2099-06-01"),
        None,
        None,
        T1,
        SERVED,
    ));
    let (_, bridge) = parcel_release(&rows, PAIRS)?;
    let holders: BTreeSet<&str> = bridge
        .keys()
        .filter(|(_, code)| code == "9999920200100070000")
        .map(|(id, _)| id.as_str())
        .collect();
    assert_eq!(holders, BTreeSet::from(["z"]));
    Ok(())
}

#[test]
fn without_pairs_the_bridge_is_the_periods() -> anyhow::Result<()> {
    let (_, bridge) = parcel_release(ROWS, &[])?;
    assert!(bridge
        .values()
        .all(|(from, _)| from.as_deref() != Some(PAIR_SOURCE)));
    assert_eq!(bridge.len(), 9);
    Ok(())
}

fn units(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(id, code)| ((*id).to_owned(), (*code).to_owned()))
        .collect()
}

#[test]
fn the_admin_release_tells_renumbered_added_and_retired_apart() {
    let snapshots = vec![
        (
            "209906".to_owned(),
            units(&[
                ("a", "9999910100"),
                ("b", "9999910200"),
                ("c", "9999910300"),
            ]),
        ),
        (
            "209909".to_owned(),
            units(&[
                ("a", "9999920100"),
                ("b", "9999910200"),
                ("d", "9999910400"),
            ]),
        ),
    ];
    let (registry, bridge) = admin_release(&snapshots, &BTreeMap::new(), &BTreeSet::new());
    let status: BTreeMap<_, _> = registry
        .iter()
        .map(|[id, status, ..]| {
            (
                id.clone().unwrap_or_default(),
                status.clone().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        status,
        BTreeMap::from([
            ("a".to_owned(), "current".to_owned()),
            ("b".to_owned(), "current".to_owned()),
            ("c".to_owned(), "historic".to_owned()),
            ("d".to_owned(), "current".to_owned()),
        ])
    );
    assert!(bridge.contains(&[
        Some("a".to_owned()),
        Some("9999910100".to_owned()),
        Some("209906".to_owned()),
        Some("209909".to_owned())
    ]));
    assert!(bridge.contains(&[
        Some("a".to_owned()),
        Some("9999920100".to_owned()),
        Some("209909".to_owned()),
        None
    ]));
    assert!(bridge.contains(&[
        Some("c".to_owned()),
        Some("9999910300".to_owned()),
        Some("209906".to_owned()),
        Some("209909".to_owned())
    ]));
}

#[test]
fn an_old_code_the_snapshots_never_carried_bridges_to_its_successor_unless_ambiguous_or_served() {
    let snapshots = vec![("209906".to_owned(), units(&[("a", "9999920100")]))];
    let remap = BTreeMap::from([("9999910100".to_owned(), "9999920100".to_owned())]);
    let (_, bridge) = admin_release(&snapshots, &remap, &BTreeSet::new());
    assert!(bridge.contains(&[
        Some("a".to_owned()),
        Some("9999910100".to_owned()),
        Some(PAIR_SOURCE.to_owned()),
        Some(PAIR_SOURCE.to_owned())
    ]));
    let (_, split) = admin_release(
        &snapshots,
        &remap,
        &BTreeSet::from(["9999910100".to_owned()]),
    );
    assert!(split
        .iter()
        .all(|row| row[1].as_deref() != Some("9999910100")));
    let both = vec![(
        "209906".to_owned(),
        units(&[("a", "9999920100"), ("b", "9999910100")]),
    )];
    let (_, served) = admin_release(&both, &remap, &BTreeSet::new());
    let holders: Vec<_> = served
        .iter()
        .filter(|row| row[1].as_deref() == Some("9999910100"))
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(holders, vec![Some("b".to_owned())]);
}

#[test]
fn the_admin_rows_join_the_built_tables_under_the_national_scope() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    create_built(&connection)?;
    let snapshots = vec![("209906".to_owned(), units(&[("a", "9999920100")]))];
    let (registry, bridge) = admin_release(&snapshots, &BTreeMap::new(), &BTreeSet::new());
    insert_admin(&connection, &registry, &bridge)?;
    assert_eq!(built_counts(&connection)?, (1, 1));
    let scope: String = connection.query_row(
        "SELECT DISTINCT unit || '/' || scope FROM built_registry",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(scope, "admin/national");
    Ok(())
}
