//! Every by-PNU document's fields, generated from the types that write it and held to the
//! committed record of its schema version: `contracts/by-pnu-documents/<schema_version>.json`.
//!
//! The bake writes a whole new generation only when the export bakes another document schema
//! version than the base holds. A document that gains, loses or renames a field under the same
//! version is weighed as a patch of every PNU and stops at the mass-change brake (#746 did that).
//! So a change of fields is a new version, and a version's record never changes
//! (`scripts/guard/by-pnu-document-records-are-append-only.sh`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::building_by_pnu_serving_export::building_document::BuildingByPnuDocument;
use crate::by_pnu_gateway_contract::{ByPnuLane, PackLane};
use crate::by_pnu_serving_manifest_publish::document_schema_version;
use crate::pack_lane_document::{fields, Source};
use crate::parcel_by_pnu_serving_export::parcel_document::ParcelByPnuDocument;

/// A document's fields: path (an array's items under `<field>[]`) to whether it is required.
type Shape = BTreeMap<String, bool>;

/// Pack lane JSON fields no Rust type writes: the Spark job that writes them is held to the
/// record's subtree by its own test (lane, field, test).
const SPARK_HELD: [(&str, &str, &str); 1] = [(
    "unit-price",
    "units",
    "infra/lakehouse/spark/tests/test_unit_price_panel_iceberg.py",
)];

const RULE: &str = "A by-PNU document gained, lost or renamed a field, or changed whether one is \
    required, under the schema version it already had. The bake rewrites a whole generation only \
    when the version changes, so bump the document's schema version \
    (PARCEL_DOCUMENT_SCHEMA_VERSION, BUILDING_DOCUMENT_SCHEMA_VERSION, or the by_pnu_lanes line's \
    document_schema_version) and commit the new version's file under \
    platforms/foundation-platform/contracts/by-pnu-documents/ with the fields printed above. \
    Never edit an existing version's file: the documents served under it have its fields.";

fn records() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../contracts/by-pnu-documents")
}

/// `T`'s fields, `$ref`s resolved through the components it names.
fn shape_of<T: ToSchema>() -> anyhow::Result<Shape> {
    let mut named = Vec::new();
    T::schemas(&mut named);
    let mut components = BTreeMap::new();
    for (name, schema) in named {
        components.insert(name, serde_json::to_value(schema)?);
    }
    let mut shape = Shape::new();
    walk(
        &serde_json::to_value(T::schema())?,
        "",
        &components,
        &mut shape,
    )?;
    Ok(shape)
}

fn walk(
    schema: &Value,
    at: &str,
    components: &BTreeMap<String, Value>,
    shape: &mut Shape,
) -> anyhow::Result<()> {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or(reference);
        let target = components
            .get(name)
            .with_context(|| format!("{at}: the schema names no component {name}"))?;
        return walk(target, at, components, shape);
    }
    for composite in ["oneOf", "anyOf", "allOf"] {
        for member in schema
            .get(composite)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            walk(member, at, components, shape)?;
        }
    }
    if let Some(items) = schema.get("items") {
        walk(items, &format!("{at}[]"), components, shape)?;
    }
    let required = schema.get("required").and_then(Value::as_array);
    for (name, field) in schema
        .get("properties")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let path = if at.is_empty() {
            name.clone()
        } else {
            format!("{at}.{name}")
        };
        let is_required = required.is_some_and(|required| required.contains(&json!(name)));
        shape.insert(path.clone(), is_required);
        walk(field, &path, components, shape)?;
    }
    Ok(())
}

fn is_under(path: &str, field: &str) -> bool {
    path.strip_prefix(field)
        .is_some_and(|rest| rest.starts_with('.') || rest.starts_with("[]"))
}

/// A pack lane's document as `pack_lane_document::render` writes it: its schema, the PNU, the
/// source, then each content column; a `*_json` column's inner fields are its host document's
/// section of that name. Also returns the fields `SPARK_HELD` names.
fn pack_shape(
    name: &str,
    lane: &PackLane,
    hosts: &BTreeMap<&str, &Shape>,
) -> anyhow::Result<(Shape, Vec<String>)> {
    let mut shape = Shape::from([
        ("schema_version".to_owned(), true),
        ("pnu".to_owned(), true),
        ("source".to_owned(), true),
    ]);
    for (path, required) in shape_of::<Source<'static>>()? {
        shape.insert(format!("source.{path}"), required);
    }
    // A kind line's latency baseline is its host (root ADR-0193 §4).
    let host = hosts
        .get(lane.latency_baseline_lane.as_str())
        .with_context(|| format!("{name}: no host document {}", lane.latency_baseline_lane))?;
    let mut held = Vec::new();
    for (field, column) in fields(lane) {
        let required = lane
            .gold
            .columns
            .iter()
            .any(|gold| gold.name == column && gold.required);
        shape.insert(field.to_owned(), required);
        if !column.ends_with("_json") {
            continue;
        }
        if SPARK_HELD
            .iter()
            .any(|(lane, held, _)| (*lane, *held) == (name, field))
        {
            held.push(field.to_owned());
            continue;
        }
        ensure!(
            host.contains_key(field),
            "{name}: {column} is no section of the {} document; name the type that writes it",
            lane.latency_baseline_lane
        );
        for (path, required) in host.iter().filter(|(path, _)| is_under(path, field)) {
            shape.insert(path.clone(), *required);
        }
    }
    Ok((shape, held))
}

fn render(shape: &Shape) -> String {
    let lines = shape
        .iter()
        .map(|(path, required)| {
            format!("  {{\"path\": {}, \"required\": {required}}}", json!(path))
        })
        .collect::<Vec<_>>();
    format!("[\n{}\n]\n", lines.join(",\n"))
}

/// Holds `shape` to the record of `version`, leaving the subtrees of `held` to their own tests.
fn check(version: &str, shape: &Shape, held: &[String]) -> anyhow::Result<()> {
    let path = records().join(format!("{version}.json"));
    let Ok(text) = std::fs::read_to_string(&path) else {
        bail!(
            "{version} has no record {}; commit it as:\n{}",
            path.display(),
            render(shape)
        );
    };
    let entries: Vec<Value> =
        serde_json::from_str(&text).with_context(|| format!("{} is not JSON", path.display()))?;
    let mut recorded = Shape::new();
    for entry in entries {
        let field = entry["path"]
            .as_str()
            .with_context(|| format!("{}: an entry names no path", path.display()))?;
        let required = entry["required"]
            .as_bool()
            .with_context(|| format!("{}: {field} states no required", path.display()))?;
        ensure!(
            recorded.insert(field.to_owned(), required).is_none(),
            "{}: {field} is recorded twice",
            path.display()
        );
    }
    let mut expected = shape.clone();
    for (field, required) in &recorded {
        if held.iter().any(|held| is_under(field, held)) {
            expected.insert(field.clone(), *required);
        }
    }
    if recorded == expected {
        return newest(version);
    }
    let gained = expected
        .keys()
        .filter(|field| !recorded.contains_key(*field))
        .collect::<Vec<_>>();
    let lost = recorded
        .keys()
        .filter(|field| !expected.contains_key(*field))
        .collect::<Vec<_>>();
    let changed = expected
        .iter()
        .filter(|(field, required)| recorded.get(*field).is_some_and(|was| was != *required))
        .map(|(field, _)| field)
        .collect::<Vec<_>>();
    bail!(
        "{version} differs from {}: gained {gained:?}, lost {lost:?}, required changed {changed:?}; \
         the new version's fields:\n{}",
        path.display(),
        render(&expected)
    )
}

/// The current version is the newest record of its document: a version only grows, and the Spark
/// tests read a kind's newest record.
fn newest(version: &str) -> anyhow::Result<()> {
    let (document, number) = version
        .rsplit_once(".v")
        .and_then(|(document, number)| Some((document, number.parse::<u32>().ok()?)))
        .with_context(|| format!("{version} does not end in .v<number>"))?;
    for entry in std::fs::read_dir(records())? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        let other = name
            .strip_suffix(".json")
            .and_then(|stem| stem.strip_prefix(document))
            .and_then(|rest| rest.strip_prefix(".v"))
            .and_then(|number| number.parse::<u32>().ok());
        ensure!(
            other.is_none_or(|other| other <= number),
            "{name} is newer than the version the export bakes, {version}"
        );
    }
    Ok(())
}

#[test]
fn every_by_pnu_document_has_the_fields_its_schema_version_recorded() -> anyhow::Result<()> {
    let parcel = shape_of::<ParcelByPnuDocument<'static>>()?;
    let building = shape_of::<BuildingByPnuDocument>()?;
    let hosts = BTreeMap::from([("parcel", &parcel), ("building", &building)]);
    let mut failures = Vec::new();
    for lane in ByPnuLane::all()? {
        let (shape, held) = match lane {
            ByPnuLane::Parcel => (parcel.clone(), Vec::new()),
            ByPnuLane::Building => (building.clone(), Vec::new()),
            ByPnuLane::Pack(pack) => pack_shape(lane.name(), pack, &hosts)?,
        };
        if let Err(error) = check(document_schema_version(lane), &shape, &held) {
            failures.push(format!("{} lane: {error:#}", lane.name()));
        }
    }
    ensure!(failures.is_empty(), "{}\n\n{RULE}", failures.join("\n\n"));
    Ok(())
}

#[test]
fn a_document_that_gains_a_field_under_its_recorded_version_is_refused() -> anyhow::Result<()> {
    let version = document_schema_version(ByPnuLane::Parcel);
    let mut gained = shape_of::<ParcelByPnuDocument<'static>>()?;
    check(version, &gained, &[])?;
    gained.insert("legal_dong.code".to_owned(), false);
    let error = check(version, &gained, &[]).expect_err("a gained field must refuse");
    assert!(error.to_string().contains("legal_dong.code"), "{error}");
    let error = check(
        "foundation-platform.parcel_by_pnu_profile.v999",
        &gained,
        &[],
    )
    .expect_err("a version without a record must refuse");
    assert!(error.to_string().contains("has no record"), "{error}");
    Ok(())
}
