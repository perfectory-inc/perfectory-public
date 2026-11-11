//! Every by-PNU document's fields, generated from the types that write it and held to its subject's
//! current revision in the schema registry (root ADR-0336), whose revisions never change
//! (`scripts/schema_registry.py check --base`).
//!
//! A document that gains, loses or renames a field without a new revision is weighed as a patch of
//! every PNU and stops at the mass-change brake (#746 did that), so a change of fields is a new
//! revision of the subject, held FULL compatible with the earlier ones of its major.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, ensure, Context};
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::building_by_pnu_serving_export::building_document::{
    BuildingByPnuDocument, BuildingByPnuPage,
};
use crate::by_pnu_gateway_contract::{ByPnuLane, PackLane};
use crate::pack_lane_document::{fields, Source};
use crate::parcel_by_pnu_serving_export::parcel_document::ParcelByPnuDocument;
use crate::schema_registry;

/// A document's fields: path (an array's items under `<field>[]`) to whether it is required.
type Shape = BTreeMap<String, bool>;

const RULE: &str = "A by-PNU document gained, lost or renamed a field, or changed whether one is \
    required, without a new revision of its subject. Add the revision to the subject in its \
    definition (published_schemas) with its schema file under \
    platforms/foundation-platform/contracts/schema-registry/ (root ADR-0336), and for a host lane's \
    document regenerate its constants (scripts/schema_registry.py generate). Never edit a merged \
    revision's file: the documents served under it have its fields.";

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
/// section of that name. Also returns the fields a Spark job writes (the lane's `spark_held_test`).
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
        // A Spark job writes this lane's content; its own test holds it to the record (the lane
        // line's `spark_held_test`, root ADR-0418 §8c).
        if lane.spark_held_test.is_some() {
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

/// A lane's current revision as fields (root ADR-0336): every lane's documents are a subject.
fn registered(lane: &str) -> anyhow::Result<(String, Shape)> {
    let subject = schema_registry::subject_of_lane(lane)?.with_context(|| {
        format!("the schema registry lists no subject for the {lane} lane's documents")
    })?;
    current_shape(subject)
}

/// A subject's current revision as fields: the registry's schema file, walked as a type's schema is.
fn current_shape(subject: &schema_registry::Subject) -> anyhow::Result<(String, Shape)> {
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
    Ok((path.display().to_string(), shape))
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

#[test]
fn every_by_pnu_document_has_the_fields_of_its_registered_revision() -> anyhow::Result<()> {
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
        // Held to its subject's current revision (root ADR-0336), so a new revision there is the
        // whole change.
        let (source, revision) = registered(lane.name())?;
        if let Err(error) = compare(&revision, &shape, &held, &source) {
            failures.push(format!("{} lane: {error:#}", lane.name()));
        }
    }
    // A building document past one request is served as pages of their own schema (root
    // ADR-0325), beside the lane's whole documents.
    let pages = schema_registry::subject_of_part(ByPnuLane::Building.name(), "page")?
        .context("the registry lists the building pages' subject")?;
    let (source, revision) = current_shape(pages)?;
    if let Err(error) = compare(&revision, &shape_of::<BuildingByPnuPage>()?, &[], &source) {
        failures.push(format!("building lane pages: {error:#}"));
    }
    ensure!(failures.is_empty(), "{}\n\n{RULE}", failures.join("\n\n"));
    Ok(())
}

#[test]
fn a_document_that_gains_a_field_under_its_registered_revision_is_refused() -> anyhow::Result<()> {
    let (source, revision) = registered(ByPnuLane::Parcel.name())?;
    let mut gained = shape_of::<ParcelByPnuDocument<'static>>()?;
    compare(&revision, &gained, &[], &source)?;
    gained.insert("legal_dong.code".to_owned(), false);
    let error = compare(&revision, &gained, &[], &source).expect_err("a gained field must refuse");
    assert!(error.to_string().contains("legal_dong.code"), "{error}");
    let error = registered("no-such-lane").expect_err("a lane without a subject must refuse");
    assert!(error.to_string().contains("lists no subject"), "{error}");
    Ok(())
}
