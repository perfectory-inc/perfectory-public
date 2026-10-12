//! The served document of a pack-only lane (root ADR-0190): its Gold panel row, rendered by rule.
//!
//! A pack-only lane's Gold is one row per PNU with at least one `*_json` content column (checked
//! when the contract is read). The document is the lane's document schema, the PNU, the Gold
//! snapshot it was read from, then each content column in the Gold contract's column order: a
//! `*_json` column's JSON under its name without `_json`, any other column's value as it is under
//! its own name (a kind's `attached_via` path or a count, root ADR-0193 §2):
//!
//! `{"schema_version":…,"pnu":…,"source":{"table":…,"iceberg_snapshot_id":…},"units":[…]}`
//!
//! It is a pure function of the snapshot and the row, so a re-bake writes the same bytes. No code
//! is written per lane: a new kind of data is a Gold panel and a contract entry.

use anyhow::{ensure, Context};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::by_pnu_gateway_contract::{PackLane, PACK_LANE_NON_CONTENT_COLUMNS};

/// Where a document was read from: the Gold table and its snapshot.
#[derive(Serialize)]
struct Source<'a> {
    table: &'a str,
    iceberg_snapshot_id: &'a str,
}

/// The content fields of `lane`'s document: (field name, Gold column), in column order.
pub(crate) fn fields(lane: &PackLane) -> Vec<(&'static str, &'static str)> {
    lane.gold
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !PACK_LANE_NON_CONTENT_COLUMNS.contains(name))
        .map(|name| (name.strip_suffix("_json").unwrap_or(name), name))
        .collect()
}

/// `lane`'s document of one Gold row, and the PNU it answers for.
///
/// # Errors
/// Refuses a row without a PNU of the lane's grammar, and a content column that is absent or not
/// JSON text.
pub(crate) fn render(
    lane: &PackLane,
    table: &str,
    iceberg_snapshot_id: &str,
    row: &JsonMap<String, JsonValue>,
) -> anyhow::Result<(String, Vec<u8>)> {
    let pnu = row
        .get("pnu")
        .and_then(JsonValue::as_str)
        .filter(|pnu| is_pnu(pnu))
        .with_context(|| format!("a {} row has no PNU", lane.gold.table_name))?
        .to_owned();
    let mut document = Vec::with_capacity(256);
    document.extend_from_slice(b"{\"schema_version\":");
    serde_json::to_writer(&mut document, &lane.document_schema_version)?;
    document.extend_from_slice(b",\"pnu\":");
    serde_json::to_writer(&mut document, &pnu)?;
    document.extend_from_slice(b",\"source\":");
    serde_json::to_writer(
        &mut document,
        &Source {
            table,
            iceberg_snapshot_id,
        },
    )?;
    for (field, column) in fields(lane) {
        let value = if column.ends_with("_json") {
            let text = row
                .get(column)
                .and_then(JsonValue::as_str)
                .with_context(|| format!("{pnu}: {column} is absent or not text"))?;
            serde_json::from_str(text).with_context(|| format!("{pnu}: {column} is not JSON"))?
        } else {
            match row.get(column) {
                None | Some(JsonValue::Null) => JsonValue::Null,
                Some(
                    value @ (JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_)),
                ) => value.clone(),
                Some(_) => anyhow::bail!("{pnu}: {column} is not a scalar"),
            }
        };
        document.push(b',');
        serde_json::to_writer(&mut document, field)?;
        document.push(b':');
        serde_json::to_writer(&mut document, &value)?;
    }
    document.push(b'}');
    Ok((pnu, document))
}

/// Checks that `bytes` is a document of `lane`: its schema, a PNU, its source and every field.
///
/// # Errors
/// Names what is missing.
pub(crate) fn check(lane: &PackLane, bytes: &[u8]) -> anyhow::Result<()> {
    let document: JsonValue =
        serde_json::from_slice(bytes).context("the documents fragment is not JSON")?;
    let schema = document.get("schema_version").and_then(JsonValue::as_str);
    ensure!(
        schema == Some(lane.document_schema_version.as_str())
            && document
                .get("pnu")
                .and_then(JsonValue::as_str)
                .is_some_and(is_pnu)
            && document.get("source").is_some_and(JsonValue::is_object)
            && fields(lane)
                .iter()
                .all(|(field, _)| document.get(*field).is_some()),
        "the documents fragment is not a {} document",
        lane.document_schema_version
    );
    Ok(())
}

fn is_pnu(pnu: &str) -> bool {
    pnu.len() == 19 && pnu.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::by_pnu_gateway_contract::ByPnuLane;

    fn lane() -> anyhow::Result<&'static PackLane> {
        ByPnuLane::named("unit-price")?
            .and_then(ByPnuLane::pack_lane)
            .context("the contract names the unit-price lane")
    }

    fn row(units: &str) -> JsonMap<String, JsonValue> {
        let mut row = JsonMap::new();
        row.insert("pnu".to_owned(), "9999900000100000000".into());
        row.insert(
            "building_id".to_owned(),
            "00000000-0000-0000-0000-00000000000b".into(),
        );
        row.insert("units_json".to_owned(), units.into());
        row.insert("row_digest".to_owned(), "a".repeat(64).into());
        row.insert("source_snapshot_id".to_owned(), "synthetic".into());
        row.insert("published_at_utc".to_owned(), "2099-01-01T00:00:00Z".into());
        row
    }

    #[test]
    fn a_row_renders_its_content_columns_by_rule_and_the_same_bytes_twice() -> anyhow::Result<()> {
        let lane = lane()?;
        // Root ADR-0204: the Gold is one row per building of a PNU; the document names the building.
        assert_eq!(
            fields(lane),
            vec![("building_id", "building_id"), ("units", "units_json")]
        );
        let units =
            r#"[{"id":"u","official_price_history":[{"base_date":"20250101","price_won":7}]}]"#;
        let (pnu, bytes) = render(
            lane,
            "gold.unit_price_panel",
            "999990000000000001",
            &row(units),
        )?;
        assert_eq!(pnu, "9999900000100000000");
        assert_eq!(
            String::from_utf8(bytes.clone())?,
            format!(
                "{{\"schema_version\":\"{}\",\"pnu\":\"9999900000100000000\",\"source\":{{\"table\":\
                 \"gold.unit_price_panel\",\"iceberg_snapshot_id\":\"999990000000000001\"}},\
                 \"building_id\":\"00000000-0000-0000-0000-00000000000b\",\"units\":{units}}}",
                lane.document_schema_version
            )
        );
        let (_, again) = render(
            lane,
            "gold.unit_price_panel",
            "999990000000000001",
            &row(units),
        )?;
        assert_eq!(bytes, again);
        check(lane, &bytes)
    }

    #[test]
    fn a_parcel_kind_carries_its_lineage_path_as_text_or_null() -> anyhow::Result<()> {
        // Root ADR-0193 §2: a column that is not `*_json` (the lineage path) goes out as its value.
        let lane = ByPnuLane::named("parcel-zoning")?
            .and_then(ByPnuLane::pack_lane)
            .context("the contract names the zoning lane")?;
        assert_eq!(
            fields(lane),
            vec![
                ("zonings", "zonings_json"),
                ("attached_via", "attached_via")
            ]
        );
        let mut row = row("[]");
        row.remove("units_json");
        row.insert("zonings_json".to_owned(), r#"[{"code":"x"}]"#.into());
        for (via, expected) in [
            (JsonValue::Null, "null"),
            (
                "9999900000100000001>split".into(),
                r#""9999900000100000001>split""#,
            ),
        ] {
            row.insert("attached_via".to_owned(), via);
            let (_, bytes) = render(lane, "gold.parcel_zoning", "1", &row)?;
            let text = String::from_utf8(bytes.clone())?;
            assert!(
                text.ends_with(&format!(
                    r#""zonings":[{{"code":"x"}}],"attached_via":{expected}}}"#
                )),
                "{text}"
            );
            check(lane, &bytes)?;
        }
        row.insert(
            "attached_via".to_owned(),
            serde_json::json!(["not", "a", "scalar"]),
        );
        assert!(render(lane, "gold.parcel_zoning", "1", &row).is_err());
        Ok(())
    }

    #[test]
    fn a_row_or_a_member_that_is_not_the_lanes_is_refused() -> anyhow::Result<()> {
        let lane = lane()?;
        assert!(render(lane, "t", "1", &row("not json")).is_err());
        let mut nameless = row("[]");
        nameless.remove("pnu");
        assert!(render(lane, "t", "1", &nameless).is_err());
        assert!(check(
            lane,
            br#"{"schema_version":"other","pnu":"9999900000100000000"}"#
        )
        .is_err());
        let (_, bytes) = render(lane, "t", "1", &row("[]"))?;
        let mut without: JsonValue = serde_json::from_slice(&bytes)?;
        without
            .as_object_mut()
            .context("a document is an object")?
            .remove("units");
        assert!(check(lane, &serde_json::to_vec(&without)?).is_err());
        Ok(())
    }
}
