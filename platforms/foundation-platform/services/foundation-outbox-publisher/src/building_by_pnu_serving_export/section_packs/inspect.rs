//! `inspect-<lane>-by-pnu-section-packs`: what the packs say about one PNU (root ADR-0147 §7).
//!
//! Prints, as one JSON document on stdout: for every section, which pack answered (base or patch),
//! the entry's state and the section's fragment; then the gateway's answer for the PNU and, when
//! it is a document, the joined document. Reads the live manifest's `section_packs`, or with
//! `PACK_GENERATION` an unpublished generation. Read-only.

use anyhow::Context;
use serde_json::{json, Value as JsonValue};

use super::super::optional_env;
use super::read::{self, Found, PackView, Resolved};
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{by_pnu, by_pnu_packs};

/// Runs the inspection.
///
/// # Errors
/// Returns an error when the PNU is malformed or a pack cannot be read.
pub(crate) async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    let env = |name: &str| optional_env(&lane.env(name));
    let pnu =
        env("INSPECT_PNU")?.with_context(|| format!("{} is required", lane.env("INSPECT_PNU")))?;
    let output = ProfileStoreConfig::parse(
        env("OUTPUT_STORAGE_DRIVER")?
            .unwrap_or_else(|| "local".to_owned())
            .as_str(),
        local_root(env("OUTPUT_ROOT")?),
    )?;
    let generation = env("PACK_GENERATION")?
        .map(|raw| raw.parse::<u64>())
        .transpose()
        .context("the pack generation must be a number")?;
    let store = ByPnuServingStore::open(lane, &output)?;
    let report = inspect(&store, &pnu, generation).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// The report of one PNU.
///
/// # Errors
/// Returns an error when the PNU is malformed or a pack cannot be read.
pub(crate) async fn inspect(
    store: &ByPnuServingStore,
    pnu: &str,
    generation: Option<u64>,
) -> anyhow::Result<JsonValue> {
    let lane = store.lane();
    by_pnu::check_pnu(lane, pnu)?;
    let dong = by_pnu_packs::unit_of(pnu)?.to_owned();
    let (view, from) = match generation {
        Some(generation) => (
            PackView::unpublished(store, lane, generation).await?,
            format!("generation {generation}"),
        ),
        None => {
            let (bytes, _) = store.read_manifest().await?;
            let served = ServedManifest::parse(lane, &bytes)?;
            let state = served
                .section_packs
                .context("the live manifest names no section packs; name PACK_GENERATION")?;
            (
                PackView::served(store, lane, &state).await?,
                "manifest".to_owned(),
            )
        }
    };
    // The one base pack per section that can hold the PNU, asked for by key: listing a parted
    // generation would read hundreds of thousands of keys for one PNU.
    let mut bases = Vec::new();
    for section in &view.sections {
        let unit = section.parts.unit_of(pnu)?;
        let key = by_pnu_packs::pack_key(lane, &section.name, section.generation, None, &unit)?;
        if store.exists(&key).await? {
            bases.push((section.name.clone(), unit));
        }
    }
    let packs = read::load_unit(store, &view, &dong, &|section, unit| {
        bases
            .iter()
            .any(|(name, known)| name == section && known == unit)
    })
    .await?;
    let mut sections = Vec::new();
    for (section, of_unit) in view.sections.iter().zip(&packs.sections) {
        let (state, origin, fragment) = match read::find(of_unit, pnu)? {
            Found::Document { patch, bytes } => (
                "document",
                origin(section.generation, patch),
                serde_json::from_slice::<JsonValue>(&bytes)?,
            ),
            Found::Tombstone { patch } => (
                "tombstone",
                origin(section.generation, Some(patch)),
                JsonValue::Null,
            ),
            Found::Absent => ("absent", JsonValue::Null, JsonValue::Null),
        };
        sections.push(json!({
            "section": section.name,
            "state": state,
            "pack": origin,
            "fragment": fragment,
        }));
    }
    let (answer, document) = match read::resolve(&packs, pnu) {
        Ok(Resolved::Document(fragments)) => (
            "document".to_owned(),
            serde_json::from_slice::<JsonValue>(&read::joined_bytes(lane, &fragments)?)?,
        ),
        Ok(Resolved::Tombstone) => ("tombstone".to_owned(), JsonValue::Null),
        Ok(Resolved::Absent) => ("absent".to_owned(), JsonValue::Null),
        Err(error) => (format!("inconsistent: {error:#}"), JsonValue::Null),
    };
    Ok(json!({
        "pnu": pnu,
        "dong": dong,
        "units": bases.iter().map(|(section, unit)| json!({"section": section, "unit": unit})).collect::<Vec<_>>(),
        "view": from,
        "sections": sections,
        "answer": answer,
        "document": document,
    }))
}

fn origin(generation: u64, patch: Option<u64>) -> JsonValue {
    match patch {
        Some(patch) => json!(format!("g{generation}/p{patch}")),
        None => json!(format!("g{generation}")),
    }
}
