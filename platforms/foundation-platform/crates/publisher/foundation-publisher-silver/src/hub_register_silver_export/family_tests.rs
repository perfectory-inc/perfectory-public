//! Every HUB family lane (root ADR-0551), converted from rows its own entry describes: a lane is
//! tested by being an entry, not by a file of its own.

use super::*;

/// A synthetic row of `layout`: each named column holds its own name, the 지번 a synthetic lot,
/// `key` the key column when the lane has one.
fn row(layout: &Layout, key: &str, san: &str) -> String {
    let mut fields = vec![String::new(); layout.column_count];
    for (name, column) in &layout.columns {
        fields[column.index] = format!("v-{name}");
    }
    for (name, value) in [
        ("sigungu_cd", "99999"),
        ("bjdong_cd", "00101"),
        ("san_gubun", san),
        ("bonbeon", "0012"),
        ("bubeon", "0003"),
        ("mgmt_key", key),
    ] {
        if let Some(column) = layout.columns.get(name) {
            fields[column.index] = value.to_owned();
        }
    }
    fields.join("|")
}

fn convert_text(layout: &Layout, text: &str) -> anyhow::Result<Value> {
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout.clone(), 2)
}

#[test]
fn every_family_lane_converts_its_own_rows() -> anyhow::Result<()> {
    assert_eq!(lakehouse_domain::hub_family_lane_error(), None);
    for lane in lakehouse_domain::hub_family_lanes() {
        let layout = crate::hub_family_silver_export::layout(lane)?;
        let text = [
            row(&layout, "SYNTHETIC-1", "0"),
            row(&layout, "SYNTHETIC-2", "1"),
            row(&layout, "SYNTHETIC-3", "2"),
        ]
        .join("\n");
        let report = convert_text(&layout, &text).with_context(|| lane.id)?;
        assert_eq!(report["rows_emitted"], 3, "{}", lane.id);
        // HUB 대지구분 2 (블록) names no cadastral lot (FP-ADR-0023).
        assert_eq!(report["pnu_ok"], 2, "{}", lane.id);
        let rows = report["decoded_rows"].as_array().context("expected rows")?;
        assert_eq!(rows[0]["pnu"], "9999900101100120003", "{}", lane.id);
        assert_eq!(rows[1]["pnu"], "9999900101200120003", "{}", lane.id);
        assert!(rows[2]["pnu"].is_null(), "{}", lane.id);
        let raw = rows[0]["raw_columns"].as_array().context("raw")?;
        assert_eq!(raw.len(), layout.column_count, "{}", lane.id);
        for (name, column) in &layout.columns {
            assert_eq!(rows[0][name], raw[column.index], "{}: {name}", lane.id);
        }
        let key = |index: usize| rows[index]["mgmt_key"].as_str().map(str::to_owned);
        let expected = if layout.row_key.is_none() {
            "SYNTHETIC-1".to_owned()
        } else {
            format!("{:x}", sha2::Sha256::digest(row(&layout, "", "0")))
        };
        assert_eq!(key(0), Some(expected), "{}", lane.id);
        assert_ne!(key(0), key(1), "{}", lane.id);
        // Every column the export writes is one the lane's Silver table declares.
        for name in rows[0].as_object().context("row")?.keys() {
            assert!(
                lane.table.columns.iter().any(|column| column.name == name),
                "{}: {name} is not a column of {}",
                lane.id,
                lane.table.table_name
            );
        }
    }
    Ok(())
}

/// 건축인허가 전유공용면적 2026-10: a value's line break cut 10 rows in two (root ADR-0551 §4).
#[test]
fn a_row_a_line_break_cut_is_joined_and_an_overfull_join_is_refused() -> anyhow::Result<()> {
    let lane = lakehouse_domain::hub_family_lanes()
        .iter()
        .find(|lane| lane.source_document.contains("\"joins_broken_lines\":true"))
        .context("a family lane joins broken lines")?;
    let layout = crate::hub_family_silver_export::layout(lane)?;
    assert!(layout.joins_broken_lines);
    let whole = row(&layout, "SYNTHETIC-1", "0");
    // Cut inside the second-to-last column: the first line is one column short.
    let cut = whole.rfind('|').context("a row has columns")?;
    let broken = format!("{}가\n나{}", &whole[..cut], &whole[cut..]);
    let after = row(&layout, "SYNTHETIC-2", "0");
    // A short line whose join overfills the row takes the next line with it, and both are refused.
    let short = whole[..cut].to_owned();
    let text = [
        broken.as_str(),
        after.as_str(),
        short.as_str(),
        after.as_str(),
    ]
    .join("\n");
    let report = convert_text(&layout, &text)?;
    assert_eq!(report["rows_emitted"], 2);
    assert_eq!(report["joined_lines"], 2);
    assert_eq!(report["rejected_rows"], 1);
    assert_eq!(report["rows_read"], 3);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    assert_eq!(rows[0]["source_line_number"], 1);
    assert_eq!(rows[1]["source_line_number"], 3);
    let raw = rows[0]["raw_columns"].as_array().context("raw")?;
    let joined = raw[layout.column_count - 2]
        .as_str()
        .context("the joined row's cut column")?;
    assert!(joined.ends_with("가\n나"), "{joined:?}");
    Ok(())
}
