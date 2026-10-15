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
//!
//! A lane that reads the building links (a building kind, root ADR-0252) checks every object of its
//! content that carries `building_link_evidence` against the approved links, as the building lane
//! does (root ADR-0125 §4), and serves the object without it.

use anyhow::{ensure, Context};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::building_link_evidence::{ApprovedBuildingLinks, BuildingLinkEvidence};
use crate::by_pnu_gateway_contract::{PackLane, PACK_LANE_NON_CONTENT_COLUMNS};

/// Where a document was read from: the Gold table and its snapshot.
#[derive(Serialize)]
struct Source<'a> {
    table: &'a str,
    iceberg_snapshot_id: &'a str,
}

/// The content fields of `lane`'s document: (field name, Gold column), in column order.
pub fn fields(lane: &PackLane) -> Vec<(&'static str, &'static str)> {
    lane.gold
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !PACK_LANE_NON_CONTENT_COLUMNS.contains(name))
        .map(|name| (name.strip_suffix("_json").unwrap_or(name), name))
        .collect()
}

/// The key `row` answers for in `lane`: its PNU, then its key suffix when the lane has one (root
/// ADR-0204). The bake selects, groups and writes by this key.
///
/// # Errors
/// Refuses a row without a PNU, or without a suffix of the lane's pattern.
pub fn key(lane: &PackLane, row: &JsonMap<String, JsonValue>) -> anyhow::Result<String> {
    let pnu = row
        .get("pnu")
        .and_then(JsonValue::as_str)
        .filter(|pnu| is_pnu(pnu))
        .with_context(|| format!("a {} row has no PNU", lane.gold.table_name))?;
    let Some(suffix) = &lane.key_suffix else {
        return Ok(pnu.to_owned());
    };
    let value = row
        .get(suffix.column)
        .and_then(JsonValue::as_str)
        .filter(|value| suffix.admits(value))
        .with_context(|| {
            format!(
                "{pnu}: {} is not a {} key suffix",
                suffix.column, lane.gold.table_name
            )
        })?;
    Ok(format!("{pnu}{value}"))
}

/// `lane`'s document of one Gold row, and the key it answers for.
///
/// # Errors
/// Refuses a row without a PNU of the lane's grammar, a content column that is absent or not
/// JSON text, and building link evidence the approved `links` do not hold.
pub fn render(
    lane: &PackLane,
    table: &str,
    iceberg_snapshot_id: &str,
    row: &JsonMap<String, JsonValue>,
    links: Option<&ApprovedBuildingLinks>,
) -> anyhow::Result<(String, Vec<u8>)> {
    let key = key(lane, row)?;
    let pnu = key[..19].to_owned();
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
            let mut value = serde_json::from_str(text)
                .with_context(|| format!("{pnu}: {column} is not JSON"))?;
            if let Some(links) = links {
                checked_links(&mut value, links).with_context(|| format!("{pnu}: {column}"))?;
            }
            value
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
    Ok((key, document))
}

/// Checks that `bytes` is a document of `lane`: its schema, a PNU, its source and every field.
///
/// # Errors
/// Names what is missing.
pub fn check(lane: &PackLane, bytes: &[u8]) -> anyhow::Result<()> {
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
                .all(|(field, _)| document.get(*field).is_some())
            && lane.key_suffix.as_ref().is_none_or(|suffix| {
                document
                    .get(suffix.column)
                    .and_then(JsonValue::as_str)
                    .is_some_and(|value| suffix.admits(value))
            }),
        "the documents fragment is not a {} document",
        lane.document_schema_version
    );
    Ok(())
}

/// Checks every object under `value` that carries `building_link_evidence` against `links` (its
/// `register_pk` and nullable `building_register_pk`, as the building lane does) and drops it.
fn checked_links(value: &mut JsonValue, links: &ApprovedBuildingLinks) -> anyhow::Result<()> {
    match value {
        JsonValue::Array(items) => items
            .iter_mut()
            .try_for_each(|item| checked_links(item, links)),
        JsonValue::Object(object) => {
            if let Some(evidence) = object.remove("building_link_evidence") {
                let evidence: BuildingLinkEvidence = serde_json::from_value(evidence)
                    .context("building_link_evidence is not evidence")?;
                let unit = object
                    .get("register_pk")
                    .and_then(JsonValue::as_str)
                    .context("an object with building link evidence has no register_pk")?;
                let parent = match object.get("building_register_pk") {
                    Some(JsonValue::Null) => None,
                    Some(JsonValue::String(parent)) => Some(parent.as_str()),
                    _ => anyhow::bail!("{unit}: building_register_pk is absent or not text"),
                };
                links
                    .validate(&evidence, unit, parent)
                    .with_context(|| format!("{unit}: building link"))?;
            }
            object
                .values_mut()
                .try_for_each(|item| checked_links(item, links))
        }
        _ => Ok(()),
    }
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

    /// A building id of the unit-price lane's key suffix (root ADR-0204).
    const BUILDING: &str = "00000000-0000-0000-0000-00000000000b";

    fn row(units: &str) -> JsonMap<String, JsonValue> {
        let mut row = JsonMap::new();
        row.insert("pnu".to_owned(), "9999900000100000000".into());
        row.insert("building_id".to_owned(), BUILDING.into());
        row.insert("units_json".to_owned(), units.into());
        row.insert("row_digest".to_owned(), "a".repeat(64).into());
        row.insert("source_snapshot_id".to_owned(), "synthetic".into());
        row.insert("published_at_utc".to_owned(), "2099-01-01T00:00:00Z".into());
        row
    }

    #[test]
    fn a_building_kind_checks_its_units_links_and_serves_no_evidence() -> anyhow::Result<()> {
        // Root ADR-0252 §3: the units kind's evidence is checked as the building lane checks it
        // (ADR-0125 §4) and never served.
        let units = ByPnuLane::named("building-units")?
            .and_then(ByPnuLane::pack_lane)
            .context("the contract names the building-units lane")?;
        assert!(units.reads_building_links && !lane()?.reads_building_links);
        let unit = |evidence: JsonValue| {
            serde_json::json!([{"register_pk": "UNIT-1", "building_register_pk": "BLDG-1",
                                "ho_name": "101", "building_link_evidence": evidence}])
            .to_string()
        };
        let links = ApprovedBuildingLinks::default();
        let (_, bytes) = render(
            units,
            "t",
            "1",
            &row(&unit(crate::building_link_evidence::source_fixture())),
            Some(&links),
        )?;
        let document: JsonValue = serde_json::from_slice(&bytes)?;
        assert_eq!(document["units"][0]["ho_name"], "101");
        assert!(document["units"][0].get("building_link_evidence").is_none());
        let mut unapproved = crate::building_link_evidence::source_fixture();
        unapproved["normalization_application_id"] = "inactive-application".into();
        assert!(render(units, "t", "1", &row(&unit(unapproved)), Some(&links)).is_err());
        Ok(())
    }

    #[test]
    fn a_row_renders_its_content_columns_by_rule_and_the_same_bytes_twice() -> anyhow::Result<()> {
        let lane = lane()?;
        assert_eq!(
            fields(lane),
            vec![("building_id", "building_id"), ("units", "units_json")]
        );
        let units =
            r#"[{"id":"u","official_price_history":[{"base_date":"20250101","price_won":7}]}]"#;
        let (key, bytes) = render(
            lane,
            "gold.unit_price",
            "999990000000000001",
            &row(units),
            None,
        )?;
        // Root ADR-0204: the key is the PNU then the building id; the document names both.
        assert_eq!(key, format!("9999900000100000000{BUILDING}"));
        assert_eq!(
            String::from_utf8(bytes.clone())?,
            format!(
                "{{\"schema_version\":\"{}\",\"pnu\":\"9999900000100000000\",\"source\":{{\"table\":\
                 \"gold.unit_price\",\"iceberg_snapshot_id\":\"999990000000000001\"}},\
                 \"building_id\":\"{BUILDING}\",\"units\":{units}}}",
                lane.document_schema_version
            )
        );
        let (_, again) = render(
            lane,
            "gold.unit_price",
            "999990000000000001",
            &row(units),
            None,
        )?;
        assert_eq!(bytes, again);
        check(lane, &bytes)
    }

    #[test]
    fn a_keyed_lane_refuses_a_row_or_a_document_without_its_suffix() -> anyhow::Result<()> {
        // Root ADR-0204: a unit-price row without a building id, or with one outside the
        // contract's pattern (a slash would split the request path), has no key.
        let lane = lane()?;
        for bad in [
            JsonValue::Null,
            "".into(),
            "not-a-building".into(),
            format!("{BUILDING}/x").into(),
        ] {
            let mut row = row("[]");
            row.insert("building_id".to_owned(), bad.clone());
            assert!(key(lane, &row).is_err(), "{bad}");
            assert!(render(lane, "t", "1", &row, None).is_err(), "{bad}");
        }
        let (_, bytes) = render(lane, "t", "1", &row("[]"), None)?;
        let mut without: JsonValue = serde_json::from_slice(&bytes)?;
        without
            .as_object_mut()
            .context("a document is an object")?
            .insert("building_id".to_owned(), JsonValue::Null);
        assert!(check(lane, &serde_json::to_vec(&without)?).is_err());
        // A lane keyed by PNU alone keeps the PNU as its key.
        let zoning = ByPnuLane::named("parcel-zoning")?
            .and_then(ByPnuLane::pack_lane)
            .context("the contract names the zoning lane")?;
        assert!(zoning.key_suffix.is_none());
        assert_eq!(key(zoning, &row("[]"))?, "9999900000100000000");
        Ok(())
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
            let (_, bytes) = render(lane, "gold.parcel_zoning", "1", &row, None)?;
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
        assert!(render(lane, "gold.parcel_zoning", "1", &row, None).is_err());
        Ok(())
    }

    #[test]
    fn a_row_or_a_member_that_is_not_the_lanes_is_refused() -> anyhow::Result<()> {
        let lane = lane()?;
        assert!(render(lane, "t", "1", &row("not json"), None).is_err());
        let mut nameless = row("[]");
        nameless.remove("pnu");
        assert!(render(lane, "t", "1", &nameless, None).is_err());
        assert!(check(
            lane,
            br#"{"schema_version":"other","pnu":"9999900000100000000"}"#
        )
        .is_err());
        let (_, bytes) = render(lane, "t", "1", &row("[]"), None)?;
        let mut without: JsonValue = serde_json::from_slice(&bytes)?;
        without
            .as_object_mut()
            .context("a document is an object")?
            .remove("units");
        assert!(check(lane, &serde_json::to_vec(&without)?).is_err());
        Ok(())
    }
}
