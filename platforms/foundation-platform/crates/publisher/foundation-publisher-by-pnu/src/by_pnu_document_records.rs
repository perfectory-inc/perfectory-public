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

use crate::building_by_pnu_serving_export::building_document::{
    BuildingByPnuDocument, BuildingByPnuPage, BUILDING_PAGE_SCHEMA_VERSION,
};
use crate::by_pnu_gateway_contract::{ByPnuLane, PackLane};
use crate::by_pnu_serving_manifest_publish::{
    document_schema_version, readable_document_schema_versions,
};
use crate::pack_lane_document::{fields, Source};
use crate::parcel_by_pnu_serving_export::parcel_document::ParcelByPnuDocument;
use crate::schema_registry;

/// A document's fields: path (an array's items under `<field>[]`) to whether it is required.
type Shape = BTreeMap<String, bool>;

/// Pack lane JSON fields no Rust type writes: the Spark job that writes them is held to the
/// record's subtree by its own test (lane, field, test).
const SPARK_HELD: [(&str, &str, &str); 3] = [
    (
        "unit-price",
        "units",
        "infra/lakehouse/spark/tests/test_unit_price_panel_iceberg.py",
    ),
    (
        "parcel-public-auction",
        "public_auction",
        "infra/lakehouse/spark/tests/test_onbid_public_auction_gold.py",
    ),
    (
        "parcel-court-auction",
        "court_auction",
        "infra/lakehouse/spark/tests/test_court_auction_gold.py",
    ),
];

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
    if lane.document_schema_revision.is_some() {
        // Optional: the subject's earlier documents carry none (root ADR-0336).
        shape.insert("schema_revision".to_owned(), false);
    }
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

/// The fields `version`'s record holds; `None` when it has no record.
fn recorded(version: &str) -> anyhow::Result<Option<Shape>> {
    let path = records().join(format!("{version}.json"));
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(None);
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
    Ok(Some(recorded))
}

/// Holds `shape` to the record of `version`, leaving the subtrees of `held` to their own tests.
fn check(version: &str, shape: &Shape, held: &[String]) -> anyhow::Result<()> {
    let path = records().join(format!("{version}.json"));
    let Some(recorded) = recorded(version)? else {
        bail!(
            "{version} has no record {}; commit it as:\n{}",
            path.display(),
            render(shape)
        );
    };
    compare(&recorded, shape, held, &path.display().to_string())?;
    newest(version)
}

/// A registered subject's current revision as fields (root ADR-0336): the registry's schema file,
/// walked as a type's schema is. `None` for a lane whose subject the registry does not list.
fn registered(lane: &str) -> anyhow::Result<Option<(String, Shape)>> {
    let Some(subject) = schema_registry::subject_of_lane(lane)? else {
        return Ok(None);
    };
    let current = subject.current();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../contracts/schema-registry")
        .join(
            subject
                .subject
                .strip_prefix("foundation-platform.")
                .unwrap_or(&subject.subject),
        )
        .join(current.major.to_string())
        .join(format!("{}.schema.json", current.revision));
    let schema: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).with_context(|| format!("{}", path.display()))?,
    )
    .with_context(|| format!("{} is not JSON", path.display()))?;
    let mut shape = Shape::new();
    walk(&schema, "", &BTreeMap::new(), &mut shape)?;
    Ok(Some((path.display().to_string(), shape)))
}

/// Holds `shape` to `recorded` (what `source` holds), the subtrees of `held` taken as recorded.
fn compare(recorded: &Shape, shape: &Shape, held: &[String], source: &str) -> anyhow::Result<()> {
    let mut expected = shape.clone();
    for (field, required) in recorded {
        if held.iter().any(|held| is_under(field, held)) {
            expected.insert(field.clone(), *required);
        }
    }
    if *recorded == expected {
        return Ok(());
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
        "the document differs from {source}: gained {gained:?}, lost {lost:?}, required changed \
         {changed:?}; the new version's fields:\n{}",
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

/// Why a reader of `new` cannot read every `old` document as one of its own (root ADR-0320); empty
/// when `new` expands `old`. An `old` document has no field `new` added, so an added field must be
/// optional, or lie under an added optional field (a new optional object may require its parts);
/// every `old` field stays, and none becomes required. The records hold paths and requiredness
/// only: a field that keeps its path but changes its type is a new version that expands nothing.
fn expansion_violations(old: &Shape, new: &Shape) -> Vec<String> {
    let mut violations = Vec::new();
    for (path, was_required) in old {
        match new.get(path) {
            None => violations.push(format!("{path} is gone")),
            Some(true) if !was_required => violations.push(format!("{path} became required")),
            Some(_) => {}
        }
    }
    let added_optional = |field: &str| !old.contains_key(field) && new.get(field) == Some(&false);
    for (path, required) in new {
        if old.contains_key(path) || !required {
            continue;
        }
        let under_added_optional = path
            .match_indices('.')
            .map(|(at, _)| path[..at].trim_end_matches("[]"))
            .any(added_optional);
        if !under_added_optional {
            violations.push(format!("{path} is added as required"));
        }
    }
    violations
}

/// Every version a lane declares its current one expands has a record, and the current record
/// only adds optional fields to it (root ADR-0320): the bake then patches a served base of that
/// version instead of rebaking every document, and the documents of it stay served beside the new.
#[test]
fn every_declared_expansion_only_adds_optional_fields() -> anyhow::Result<()> {
    let mut failures = Vec::new();
    for lane in ByPnuLane::all()? {
        // A registered subject's revisions are held to FULL compatibility by the registry's own
        // check (scripts/schema_registry.py, root ADR-0336), its records frozen at revision 2.
        if schema_registry::subject_of_lane(lane.name())?.is_some() {
            continue;
        }
        let current = document_schema_version(lane);
        let readable = readable_document_schema_versions(lane);
        ensure!(
            readable.first() == Some(&current),
            "{}: the readable versions start with the baked one",
            lane.name()
        );
        let Some(new) = recorded(current)? else {
            failures.push(format!("{}: {current} has no record", lane.name()));
            continue;
        };
        for earlier in &readable[1..] {
            match recorded(earlier)? {
                None => failures.push(format!(
                    "{}: {current} expands {earlier}, which has no record",
                    lane.name()
                )),
                Some(old) => {
                    let violations = expansion_violations(&old, &new);
                    if !violations.is_empty() {
                        failures.push(format!(
                            "{}: {current} does not expand {earlier}: {violations:?}",
                            lane.name()
                        ));
                    }
                }
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "{}\n\nA version may expand an earlier one only by adding optional fields; any other \
         change is a new version that expands nothing, and its first bake is a whole generation",
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn an_expansion_adds_only_optional_fields() {
    let shape = |fields: &[(&str, bool)]| -> Shape {
        fields
            .iter()
            .map(|(path, required)| ((*path).to_owned(), *required))
            .collect()
    };
    let old = shape(&[("pnu", true), ("price", false), ("price.amount", true)]);
    // A new optional object may require its parts; an optional field anywhere is an expansion.
    let new = shape(&[
        ("pnu", true),
        ("price", false),
        ("price.amount", true),
        ("price.note", false),
        ("legal_dong", false),
        ("legal_dong.name", true),
        ("rounds", false),
        ("rounds[].photos", false),
        ("rounds[].photos[].sha256", true),
    ]);
    assert!(expansion_violations(&old, &new).is_empty());
    // A field gone, a field made required, a required field added outside a new optional one.
    let mut narrowed = new.clone();
    narrowed.remove("price.amount");
    narrowed.insert("price".to_owned(), true);
    narrowed.insert("price.currency".to_owned(), true);
    assert_eq!(
        expansion_violations(&old, &narrowed),
        [
            "price became required",
            "price.amount is gone",
            "price.currency is added as required"
        ]
    );
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
        // A registered subject is held to its current revision in the registry (root ADR-0336),
        // so a new revision there is the whole change; every other lane to its record.
        let held_to = match registered(lane.name())? {
            Some((source, revision)) => compare(&revision, &shape, &held, &source),
            None => check(document_schema_version(lane), &shape, &held),
        };
        if let Err(error) = held_to {
            failures.push(format!("{} lane: {error:#}", lane.name()));
        }
    }
    // A building document past one request is served as pages of their own schema (root
    // ADR-0325), beside the lane's whole documents.
    if let Err(error) = check(
        BUILDING_PAGE_SCHEMA_VERSION,
        &shape_of::<BuildingByPnuPage>()?,
        &[],
    ) {
        failures.push(format!("building lane pages: {error:#}"));
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
