use super::*;

#[test]
fn the_head_is_read_by_the_literal_ranges_of_the_changed_sigungu() -> anyhow::Result<()> {
    assert_eq!(
        part_ranges(&["41110".to_owned(), "41199".to_owned(), "99999".to_owned()])?,
        "(pnu >= '41110' AND pnu < '41111') OR (pnu >= '41199' AND pnu < '41200') OR (pnu >= '99999')"
    );
    assert_eq!(
        part_ranges(&["09999".to_owned()])?,
        "(pnu >= '09999' AND pnu < '10000')"
    );
    assert!(part_ranges(&[]).is_err());
    for bad in ["4111", "411100", "4111a", ""] {
        assert!(part_ranges(&[bad.to_owned()]).is_err(), "{bad:?} was taken");
    }
    Ok(())
}

#[test]
fn a_run_is_asked_for_sido_by_their_two_digits_or_for_none() -> anyhow::Result<()> {
    assert_eq!(requested_partitions(None)?, None);
    assert_eq!(
        requested_partitions(Some("41, 11,41"))?,
        Some(BTreeSet::from(["11".to_owned(), "41".to_owned()]))
    );
    for bad in ["", " , ", "4", "411", "4a", "11,national"] {
        assert!(
            requested_partitions(Some(bad)).is_err(),
            "{bad:?} was taken"
        );
    }
    Ok(())
}

/// Every lane on the shared driver.
const LANES: [&KindBuild; 13] = [
    &REGISTER_MASTERS_BUILD,
    &DEMOLITIONS_BUILD,
    &ENERGY_BUILD,
    &SUB_PARCELS_BUILD,
    &BUILDING_PERMITS_BUILD,
    &PARCEL_PANEL_BUILD,
    &OWNERSHIP_BUILD,
    &FACTORIES_BUILD,
    &PUBLIC_AUCTION_BUILD,
    &REAL_TRANSACTION_BUILD,
    &UNIT_PRICE_BUILD,
    &NEARBY_BUILD,
    &COURT_AUCTION_BUILD,
];

/// A kind line that names `place_identity` gives its table a `place_id` column (by_pnu_kind
/// derive); a stage that did not carry it would write every row's id as null, and the takeover
/// verdict would fail on every row (the factories are the first lane that carries it).
#[test]
fn every_lane_s_stage_carries_the_place_id_its_table_has() -> anyhow::Result<()> {
    for kind in LANES {
        let has = contract(kind.replaced)?
            .columns
            .iter()
            .any(|column| column.name == "place_id");
        assert_eq!(has, kind.stage.carries_place_id(), "{}", kind.table);
    }
    Ok(())
}

/// Root ADR-0596's cases the driver decides: an explanation is judged only on a run with a base
/// in which exactly its inputs moved and neither the code nor the served edition did.
#[test]
fn an_explanation_is_judged_only_where_it_alone_can_explain() {
    let stores = foundation_publisher_silver::parcel_nearby_stage::STORE_SOURCE.to_owned();
    let declared = || NEARBY.change_explanation();
    assert!(explaining(declared(), &BTreeSet::from([stores.clone()]), true).is_some());
    assert!(
        explaining(
            declared(),
            &BTreeSet::from([stores.clone(), PARCEL_SOURCE.to_owned()]),
            true
        )
        .is_none(),
        "the parcels moved too"
    );
    assert!(
        explaining(declared(), &BTreeSet::from([stores]), false).is_none(),
        "the code or the edition moved, or there is no base"
    );
    assert!(explaining(None, &BTreeSet::new(), true).is_none());
}

/// A check of the whole build and a batch read hold only for a table committed whole (the run
/// refuses the rest at its start), and a check's bound is one the rebuild contract names.
#[test]
fn what_a_lane_declares_beyond_its_sql_fits_how_it_commits() -> anyhow::Result<()> {
    let rebuild: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    for kind in LANES {
        let national = kind.stage.national_checks();
        if kind.per_sido_commits {
            assert!(national.is_empty(), "{}", kind.table);
            assert!(kind.stage.batch_input().is_none(), "{}", kind.table);
            assert!(kind.stage.change_explanation().is_none(), "{}", kind.table);
        }
        if let Some(explanation) = kind.stage.change_explanation() {
            assert!(
                explanation
                    .moved
                    .iter()
                    .all(|input| kind.stage.inputs().contains(input)
                        && !kind.stage.whole_table_inputs().contains(input)),
                "{} explains a move a partial run never sees",
                kind.table
            );
        }
        for bound in national.iter().filter_map(|check| check.bound) {
            assert!(
                rebuild["tables"][kind.replaced][bound].as_f64().is_some(),
                "{} names {bound}, which the rebuild contract does not give {}",
                kind.table,
                kind.replaced
            );
        }
        if let Some(batch) = kind.stage.batch_input() {
            assert!(kind.stage.inputs().contains(&batch.input), "{}", kind.table);
        }
    }
    Ok(())
}

#[test]
fn an_instant_is_a_utc_rfc3339_time_and_nothing_else() {
    for good in ["2099-01-02T03:04:05Z", "2099-01-02T03:04:05.123Z"] {
        assert!(is_instant(good), "{good}");
    }
    for bad in [
        "2099-01-02 03:04:05",
        "2099-01-02T03:04:05+09:00",
        "2099-01-02T03:04:05Z'",
        "",
    ] {
        assert!(!is_instant(bad), "{bad}");
    }
}

#[test]
fn a_sido_s_range_bounds_its_keys_and_the_last_one_is_open() -> anyhow::Result<()> {
    assert_eq!(sido_range("41")?, "(pnu >= '41' AND pnu < '42')");
    assert_eq!(sido_range("09")?, "(pnu >= '09' AND pnu < '10')");
    assert_eq!(sido_range("99")?, "(pnu >= '99')");
    for bad in ["4", "411", "4'", "+1"] {
        assert!(sido_range(bad).is_err(), "{bad} was taken");
    }
    Ok(())
}

#[test]
fn a_spool_is_removed_when_its_run_ends() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let path = work.path().join("spool");
    std::fs::create_dir_all(path.join("stale"))?;
    {
        let spool = Spool::new(path.clone())?;
        assert!(!spool.path().join("stale").exists());
        std::fs::write(spool.path().join("part.parquet"), b"x")?;
    }
    assert!(!path.exists());
    Ok(())
}
