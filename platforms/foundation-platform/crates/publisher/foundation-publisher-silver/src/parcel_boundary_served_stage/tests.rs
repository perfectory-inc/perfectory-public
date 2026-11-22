use super::*;

/// A little-endian ISO WKB polygon of one ring.
fn polygon(points: &[(f64, f64)]) -> Vec<u8> {
    let mut wkb = vec![1];
    wkb.extend(WKB_POLYGON.to_le_bytes());
    wkb.extend(1_u32.to_le_bytes());
    wkb.extend(
        u32::try_from(points.len())
            .unwrap_or_default()
            .to_le_bytes(),
    );
    for (x, y) in points {
        wkb.extend(x.to_le_bytes());
        wkb.extend(y.to_le_bytes());
    }
    wkb
}

fn square(x: f64, y: f64, side: f64) -> Vec<u8> {
    polygon(&[
        (x, y),
        (x + side, y),
        (x + side, y + side),
        (x, y + side),
        (x, y),
    ])
}

const SUM: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn edit(seq: i64, pnu: &str, op: Op, wkb: Option<Vec<u8>>) -> LedgerEdit {
    LedgerEdit {
        change_seq: seq,
        feature_id: pnu.to_owned(),
        op,
        geometry_srid: GEOMETRY_SRID,
        geometry_checksum_sha256: wkb.as_ref().map(|_| SUM.to_owned()),
        geometry_wkb: wkb,
    }
}

/// `tile_range_golden.json`: the boxes and ranges `land_use_zone_area_by_tile.py` and the bake's
/// `lakehouse_tile_patch.rs` both compute; this port computes them too.
#[test]
fn the_tile_range_is_the_one_both_other_ports_compute() -> anyhow::Result<()> {
    let golden: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../infra/lakehouse/spark/tests/fixtures/tile_range_golden.json"
    ))?;
    let zoom = u8::try_from(golden["zoom"].as_u64().unwrap_or_default())?;
    let buffer = golden["buffer_pixels"].as_f64().unwrap_or_default();
    let cases = golden["cases"].as_array().context("cases")?;
    assert!(!cases.is_empty());
    for case in cases {
        let b: Vec<f64> = serde_json::from_value(case["bbox"].clone())?;
        let expected: Option<(u32, u32, u32, u32)> =
            serde_json::from_value::<Option<Vec<u32>>>(case["tiles"].clone())?
                .map(|t| (t[0], t[1], t[2], t[3]));
        assert_eq!(
            tile_range((b[0], b[1], b[2], b[3]), zoom, buffer),
            expected,
            "{}",
            case["case"]
        );
    }
    Ok(())
}

#[test]
fn a_polygon_s_box_is_read_from_its_wkb_and_anything_else_is_refused() -> anyhow::Result<()> {
    assert_eq!(
        wkb_bbox(&square(10.0, 20.0, 0.5))?,
        (10.0, 20.0, 10.5, 20.5)
    );
    let mut multi = vec![1];
    multi.extend(WKB_MULTIPOLYGON.to_le_bytes());
    multi.extend(2_u32.to_le_bytes());
    multi.extend(square(1.0, 1.0, 1.0));
    multi.extend(square(5.0, 6.0, 1.0));
    assert_eq!(wkb_bbox(&multi)?, (1.0, 1.0, 6.0, 7.0));
    let mut trailing = square(0.0, 0.0, 1.0);
    trailing.push(0);
    assert!(wkb_bbox(&trailing).is_err());
    assert!(wkb_bbox(&square(0.0, 0.0, 1.0)[..20]).is_err());
    let mut point = vec![1];
    point.extend(1_u32.to_le_bytes());
    assert!(wkb_bbox(&point).is_err());
    Ok(())
}

#[test]
fn the_ledger_is_ordered_and_an_edit_the_table_cannot_hold_is_refused() -> anyhow::Result<()> {
    let ledger = check_ledger(vec![
        edit(2, "9999910100100020000", Op::Delete, None),
        edit(
            1,
            "9999910100100010000",
            Op::Upsert,
            Some(square(0.0, 0.0, 1.0)),
        ),
    ])?;
    assert_eq!(
        ledger.iter().map(|e| e.change_seq).collect::<Vec<_>>(),
        [1, 2]
    );
    let refused = |edits: Vec<LedgerEdit>| check_ledger(edits).is_err();
    assert!(refused(vec![
        edit(1, "9999910100100010000", Op::Delete, None),
        edit(1, "9999910100100020000", Op::Delete, None),
    ]));
    assert!(refused(vec![edit(
        0,
        "9999910100100010000",
        Op::Delete,
        None
    )]));
    assert!(refused(vec![edit(
        1,
        "99999101001000X0000",
        Op::Delete,
        None
    )]));
    assert!(refused(vec![edit(
        1,
        "9999910100100010000",
        Op::Upsert,
        None
    )]));
    let mut other_crs = edit(1, "9999910100100010000", Op::Delete, None);
    other_crs.geometry_srid = 5186;
    assert!(refused(vec![other_crs]));
    let mut no_sum = edit(
        1,
        "9999910100100010000",
        Op::Upsert,
        Some(square(0.0, 0.0, 1.0)),
    );
    no_sum.geometry_checksum_sha256 = Some("ABC".to_owned());
    assert!(refused(vec![no_sum]));
    Ok(())
}

#[test]
fn each_edited_parcel_ends_as_its_last_edit_and_a_delete_of_nothing_is_counted() {
    let ledger = vec![
        edit(
            1,
            "9999910100100010000",
            Op::Upsert,
            Some(square(0.0, 0.0, 1.0)),
        ),
        edit(2, "9999910100100010000", Op::Delete, None),
        edit(3, "9999910100100020000", Op::Delete, None),
        edit(4, "9999910100100030000", Op::Delete, None),
        edit(
            5,
            "9999910100100030000",
            Op::Upsert,
            Some(square(2.0, 2.0, 1.0)),
        ),
    ];
    let present = BTreeSet::from(["9999910100100030000".to_owned()]);
    let (states, counts) = fold_edits(&ledger, &present);
    assert!(states["9999910100100010000"].is_none());
    assert!(states["9999910100100020000"].is_none());
    assert_eq!(states["9999910100100030000"].map(|e| e.change_seq), Some(5));
    assert_eq!(
        counts,
        EditCounts {
            upserts: 2,
            deletes: 3,
            deletes_of_absent_features: 1
        }
    );
}

#[test]
fn a_fold_hands_the_neighbourhood_of_its_changes_and_a_wide_one_is_not_partial(
) -> anyhow::Result<()> {
    let buffer = membership_buffer_pixels()?;
    let ledger = vec![
        edit(
            1,
            "9999910100100010000",
            Op::Upsert,
            Some(square(10.0, 10.0, 0.0001)),
        ),
        edit(
            2,
            "9999910100100020000",
            Op::Upsert,
            Some(square(10.0002, 10.0, 0.0001)),
        ),
        edit(
            3,
            "9999910100100030000",
            Op::Upsert,
            Some(square(10.5, 10.5, 0.0001)),
        ),
    ];
    // Served through 2: only edit 3 is new; its old box is the edition's.
    let boxes = BTreeMap::from([(
        "9999910100100030000".to_owned(),
        (10.4, 10.4, 10.4001, 10.4001),
    )]);
    let FoldPlan::Partial {
        after,
        tiles,
        ledger_pnus,
        edited_in_neighbourhood,
    } = plan(&ledger, 2, &boxes, buffer)?
    else {
        bail!("a small fold is partial");
    };
    assert_eq!(after.keys().collect::<Vec<_>>(), ["9999910100100030000"]);
    assert!(!tiles.is_empty() && tiles.len() <= MAX_FOLD_TILES);
    assert_eq!(ledger_pnus.len(), 3);
    assert_eq!(edited_in_neighbourhood, ["9999910100100030000"]);
    assert!(silver_predicate(&tiles, buffer).starts_with("((bbox_max_x >= "));
    assert_eq!(silver_predicate(&[], buffer), "false");

    let wide = vec![edit(
        9,
        "9999910100100040000",
        Op::Upsert,
        Some(square(10.0, 10.0, 1.0)),
    )];
    let FoldPlan::Full(why) = plan(&wide, 0, &BTreeMap::new(), buffer)? else {
        bail!("a fold over a degree of tiles is not partial");
    };
    assert!(why.contains("past 512"), "{why}");
    Ok(())
}

/// Printed by `CPython` for the same row with `served_gold_common.bake_handoff_line`.
#[test]
fn a_handoff_line_is_the_one_the_spark_job_wrote() {
    assert_eq!(
        handoff_line("9999910100100010000", &[0x01, 0x03, 0, 0, 0], "edit"),
        r#"{"feature_id":"9999910100100010000","geometry_srid":4326,"geometry_wkb_hex":"0103000000","origin":"edit","properties":{}}"#
    );
}

#[test]
fn the_built_rows_are_the_edition_but_its_edited_parcels_and_the_digest_is_the_sql_s(
) -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE edition (pnu VARCHAR, geometry_wkb BLOB, geometry_srid INTEGER,
                               geometry_checksum_sha256 VARCHAR, source_snapshot_id VARCHAR);
         INSERT INTO edition VALUES
           ('9999910100100010000', '\\x01'::BLOB, 4326, 'aa', 'edition-b'),
           ('9999910100100020000', '\\x02'::BLOB, 4326, 'bb', 'edition-b'),
           ('9999920200100010000', '\\x03'::BLOB, 4326, 'cc', 'edition-b');",
    )?;
    let ledger = vec![
        edit(1, "9999910100100010000", Op::Delete, None),
        edit(
            2,
            "9999910100100030000",
            Op::Upsert,
            Some(square(0.0, 0.0, 1.0)),
        ),
    ];
    let present = BTreeSet::from(["9999910100100010000".to_owned()]);
    let (states, _) = fold_edits(&ledger, &present);
    load_edits(&connection, &states)?;
    // The fixture's 시도 is 99 and every PNU here starts with it.
    connection.execute_batch(&build_sido_sql(
        "SELECT * FROM edition",
        "(pnu >= '99')",
        true,
        "42",
        2,
        "2099-01-01T00:00:00Z",
    )?)?;
    let rows: Vec<(String, String, String)> = connection
        .prepare("SELECT pnu, origin, source_snapshot_id FROM built ORDER BY pnu")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        [
            (
                "9999910100100020000".into(),
                "source".into(),
                "edition-b".into()
            ),
            (
                "9999910100100030000".into(),
                "edit".into(),
                "map-edit-2".into()
            ),
            (
                "9999920200100010000".into(),
                "source".into(),
                "edition-b".into()
            ),
        ]
    );
    let digest: String = connection.query_row(
        "SELECT row_digest FROM built WHERE pnu = '9999910100100020000'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(digest, row_digest("9999910100100020000", "bb", "source"));
    assert!(build_sido_sql("SELECT 1", "(pnu >= '99')", true, "1'", 0, "x").is_err());
    Ok(())
}

fn handoff_line_of(seq: i64, op: &str, carries: bool) -> String {
    let geometry = if carries {
        format!(
            r#""geometry_geojson":"{{}}","geometry_wkb_hex":"01","geometry_checksum_sha256":"{SUM}""#
        )
    } else {
        r#""geometry_geojson":null,"geometry_wkb_hex":null,"geometry_checksum_sha256":null"#
            .to_owned()
    };
    format!(
        r#"{{"unit":"parcels","change_seq":{seq},"feature_id":"9999910100100010000","op":"{op}",{geometry},"geometry_srid":4326,"properties_json":"{{}}","editor":"e","edited_at":"2099-01-01T00:00:00Z"}}"#
    )
}

#[test]
fn the_edit_handoff_is_read_as_the_ledger_contract_reads_it() -> anyhow::Result<()> {
    let text = format!(
        "{}\n\n{}\n",
        handoff_line_of(2, "delete", false),
        handoff_line_of(1, "upsert", true)
    );
    let rows = read_edit_handoff(&text)?;
    assert_eq!(
        rows.iter().map(|r| r.change_seq).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(rows[0].ledger_edit()?.geometry_wkb, Some(vec![1]));
    let refused = |text: String| read_edit_handoff(&text).is_err();
    assert!(
        refused(handoff_line_of(1, "upsert", false)),
        "an upsert without geometry"
    );
    assert!(
        refused(handoff_line_of(1, "delete", true)),
        "a delete with one"
    );
    assert!(refused(handoff_line_of(1, "move", false)));
    assert!(refused(handoff_line_of(0, "delete", false)));
    assert!(refused(format!(
        "{}\n{}",
        handoff_line_of(1, "delete", false),
        handoff_line_of(1, "delete", false)
    )));
    assert!(refused(
        handoff_line_of(1, "delete", false).replace("parcels", "admin")
    ));
    assert!(refused(
        handoff_line_of(1, "delete", false).replace("4326", "5186")
    ));
    assert!(refused(handoff_line_of(1, "delete", false).replace(
        r#""properties_json":"{}""#,
        r#""properties_json":"[]""#
    )));
    Ok(())
}

#[test]
fn only_unledgered_edits_are_fresh_and_a_rewritten_one_is_refused() -> anyhow::Result<()> {
    let rows = read_edit_handoff(&format!(
        "{}\n{}",
        handoff_line_of(1, "upsert", true),
        handoff_line_of(2, "delete", false)
    ))?;
    let ledgered = BTreeMap::from([(1, rows[0].fingerprint())]);
    let fresh = fresh_edits(&ledgered, &rows)?;
    assert_eq!(fresh.iter().map(|r| r.change_seq).collect::<Vec<_>>(), [2]);
    let mut other = rows[0].fingerprint();
    other.5 = "someone else".to_owned();
    assert!(fresh_edits(&BTreeMap::from([(1, other)]), &rows).is_err());
    Ok(())
}
