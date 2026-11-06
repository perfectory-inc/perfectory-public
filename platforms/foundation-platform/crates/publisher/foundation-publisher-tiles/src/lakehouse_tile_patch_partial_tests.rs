use serde_json::json;

use super::super::patch::tests::{square, zone, zoning_index};
use super::super::patch::{diff, Record};
use super::super::pmtiles_feature_ids::tests::written;
use super::*;

// Synthetic zones of the reserved layers D998/D999, a degree apart near 10°E 10°N.
const A: &str = "D999:SYN-0001";
const B: &str = "D998:SYN-0001";
const C: &str = "D999:SYN-0002";
const D: &str = "D998:SYN-0002";

fn month_1() -> Vec<String> {
    vec![
        zone(A, "합성 A", &square(10.0, 10.0, 0.01)),
        zone(B, "합성 B", &square(11.0, 10.0, 0.01)),
        zone(C, "합성 C", &square(12.0, 10.0, 0.01)),
    ]
}

/// Next month: A moves, C is gone, D is new, B is untouched.
fn month_2() -> Vec<String> {
    vec![
        zone(A, "합성 A", &square(10.002, 10.0, 0.01)),
        zone(B, "합성 B", &square(11.0, 10.0, 0.01)),
        zone(D, "합성 D", &square(13.0, 10.0, 0.01)),
    ]
}

fn changes(upserted: &[&str], deleted: &[&str]) -> ChangeList {
    ChangeList {
        upserted: upserted.iter().map(|id| id_hash(id)).collect(),
        deleted: deleted.iter().map(|id| id_hash(id)).collect(),
    }
}

#[test]
fn the_merged_index_is_the_index_a_full_handoff_would_make() -> anyhow::Result<()> {
    let old = zoning_index(&month_1())?;
    let full = zoning_index(&month_2())?;
    // The partial handoff holds only the zones of the tiles A and D touch, not B.
    let handed = zoning_index(&[month_2()[0].clone(), month_2()[2].clone()])?;
    let merged = merge(&old, &handed, &changes(&[A, D], &[C])).map_err(anyhow::Error::msg)?;
    assert_eq!(merged, full);
    assert_eq!(diff(&old, &merged), diff(&old, &full));
    Ok(())
}

#[test]
fn a_handoff_that_drifted_from_the_served_index_is_declined() -> anyhow::Result<()> {
    let old = zoning_index(&month_1())?;
    // B is handed renamed but the change list does not name it: the handoff is not of this base.
    let handed = zoning_index(&[zone(B, "합성 B 변경", &square(11.0, 10.0, 0.01))])?;
    let refused = merge(&old, &handed, &changes(&[], &[]))
        .err()
        .unwrap_or_default();
    assert!(
        refused.contains("differ from the served index"),
        "{refused}"
    );
    // An upserted id with no row, or a handed row the list deletes, is declined too.
    let refused = merge(&old, &[], &changes(&[D], &[]))
        .err()
        .unwrap_or_default();
    assert!(refused.contains("no row in the handoff"), "{refused}");
    let handed = zoning_index(&[month_1()[2].clone()])?;
    let refused = merge(&old, &handed, &changes(&[], &[C]))
        .err()
        .unwrap_or_default();
    assert!(refused.contains("deletes"), "{refused}");
    Ok(())
}

#[test]
fn a_deletion_of_an_id_the_index_never_held_changes_nothing() -> anyhow::Result<()> {
    let old: Vec<Record> = zoning_index(&month_1())?;
    let merged = merge(&old, &[], &changes(&[], &[D])).map_err(anyhow::Error::msg)?;
    assert_eq!(merged, old);
    Ok(())
}

fn list_file(text: &str) -> anyhow::Result<(tempfile::NamedTempFile, ChangeListFile)> {
    let file = written(text.as_bytes())?;
    let name = file
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();
    let rows = text.lines().filter(|line| !line.trim().is_empty()).count();
    let sha256 = format!("{:x}", Sha256::digest(text.as_bytes()));
    Ok((
        file,
        ChangeListFile {
            path: name,
            rows,
            sha256,
        },
    ))
}

#[test]
fn the_change_list_is_read_against_its_count_and_digest() -> anyhow::Result<()> {
    let text = format!(
        "{}\n{}\n",
        json!({"feature_id": A, "op": "upsert"}),
        json!({"feature_id": C, "op": "delete"})
    );
    let (file, recorded) = list_file(&text)?;
    // The handoff sits in the same directory; only its parent is read.
    let handoff = file.path().with_file_name("served.jsonl");
    assert_eq!(read_change_list(&recorded, &handoff)?, changes(&[A], &[C]));
    let wrong_hash = ChangeListFile {
        sha256: "0".repeat(64),
        ..recorded.clone()
    };
    assert!(read_change_list(&wrong_hash, &handoff).is_err());
    let wrong_rows = ChangeListFile {
        rows: 3,
        ..recorded
    };
    assert!(read_change_list(&wrong_rows, &handoff).is_err());
    for bad in [
        format!("{}\n", json!({"feature_id": A, "op": "rename"})),
        format!(
            "{}\n{}\n",
            json!({"feature_id": A, "op": "upsert"}),
            json!({"feature_id": A, "op": "delete"})
        ),
    ] {
        let (file, recorded) = list_file(&bad)?;
        assert!(
            read_change_list(&recorded, &file.path().with_file_name("h")).is_err(),
            "{bad}"
        );
    }
    Ok(())
}

fn partial(tiles: &[(u32, u32)]) -> PartialHandoff {
    PartialHandoff {
        base_snapshot: "1".to_owned(),
        neighbourhood_zoom: 10,
        neighbourhood_tiles: tiles.to_vec(),
        change_list: ChangeListFile {
            path: "served.jsonl.changes.jsonl".to_owned(),
            rows: 0,
            sha256: "0".repeat(64),
        },
    }
}

#[test]
fn a_neighbourhood_the_handoff_does_not_hold_is_declined() {
    let held = partial(&[(540, 483), (541, 483)]);
    let needed: HashSet<(u32, u32)> = [(540, 483)].into_iter().collect();
    assert_eq!(held.uncovered(&needed, 10), None);
    let wider: HashSet<(u32, u32)> = [(540, 483), (875, 396)].into_iter().collect();
    assert!(held
        .uncovered(&wider, 10)
        .is_some_and(|why| why.contains("lacks 1")));
    assert!(held.uncovered(&needed, 11).is_some());
}

#[test]
fn a_partial_summary_names_its_block_and_may_hand_no_row() -> anyhow::Result<()> {
    let summary = |scope: serde_json::Value, block: serde_json::Value, rows: usize| {
        serde_json::from_value::<ServedSummary>(json!({
            "schema_version": "foundation-platform.polygon_served_gold.v1",
            "unit": "zoning", "feature_id_property": "zone_id", "geometry_srid": 4326,
            "canonical_iceberg_snapshot_id": "2", "edits_through_change_seq": 0,
            "served_row_count": rows, "status": "ready",
            "handoff_scope": scope, "partial": block,
        }))
    };
    let block = serde_json::to_value(json!({
        "base_snapshot": "1", "neighbourhood_zoom": 10, "neighbourhood_tiles": [[540, 483]],
        "change_list": {"path": "served.jsonl.changes.jsonl", "rows": 1, "sha256": "0".repeat(64)},
        "by_tile_rows_appended": 1,
    }))?;
    summary(json!("partial"), block.clone(), 0)?.validate("zoning")?;
    assert!(summary(json!("full"), block.clone(), 1)?
        .validate("zoning")
        .is_err());
    assert!(summary(json!("partial"), json!(null), 1)?
        .validate("zoning")
        .is_err());
    assert!(
        summary(json!("full"), json!(null), 0)?
            .validate("zoning")
            .is_err(),
        "empty"
    );
    summary(json!(null), json!(null), 1)?.validate("zoning")?;
    let mut escaping = block;
    escaping["change_list"]["path"] = json!("../changes.jsonl");
    assert!(summary(json!("partial"), escaping, 0)?
        .validate("zoning")
        .is_err());
    Ok(())
}
