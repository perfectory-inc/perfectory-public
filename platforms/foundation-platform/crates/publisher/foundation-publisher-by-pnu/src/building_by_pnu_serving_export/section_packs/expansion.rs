//! The first patch of a document schema that expands the served one (root ADR-0320).
//!
//! A version that only adds optional fields is patched over the served documents: the change set
//! (Gold `row_digest`, ADR-0141) decides which documents the new version re-renders, and every
//! other one stays served as the older version rendered it until the next whole generation. That
//! is right only when the new renderer turns an unchanged Gold row into the same content as the
//! served document. The records (`contracts/by-pnu-documents`) prove the shape half of the claim;
//! this probe tests the content half before anything is written: it renders a deterministic
//! sample of the rows the change set does not name, reads each one's served answer the way the
//! gateway does, and compares the two with the version stamp and the source snapshot set aside
//! and a member that is null read as absent. One difference refuses the patch: the new version
//! changes documents the change set does not see (a field read from a column Gold already held,
//! a renderer that writes `[]` where it wrote nothing), and only a whole generation serves it.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::gate;
use super::read::{self, PackView, Resolved};
use super::sections::{PackDocument, PackProvenance, Renderer};
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_section_pack_manifest::SectionPacksState;
use crate::by_pnu_serving_manifest_publish::document_schema_version;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::r2_layout::by_pnu_packs;

/// How many unchanged documents the probe draws. A renderer change that alters a share `f` of
/// the unchanged documents escapes it with probability `(1 - f)^2000`: under 1% for `f` = 0.25%.
/// Whatever escapes is bounded in time, since the next whole generation re-renders every
/// document under the newest version.
const PROBE_DOCUMENTS: u64 = 2_000;
/// How many differing documents a refusal names.
const MAX_DIFFERING: usize = 20;

/// What the probe compared, recorded in the patch's export summary.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExpansionProbe {
    /// The versions the served documents were baked under.
    pub served_document_schema_versions: Vec<String>,
    /// The version this patch bakes.
    pub document_schema_version: String,
    pub compared: u64,
    pub equal: u64,
}

/// A patch that needs the probe: the newest served documents are of another version than this
/// release bakes, so this is the first patch of it.
pub(super) struct Probe {
    state: SectionPacksState,
    per_million: u64,
}

impl Probe {
    /// The probe a patch over `served` needs, if any.
    pub(super) fn needed(lane: ByPnuLane, served: Option<SectionPacksState>) -> Option<Self> {
        let state = served?;
        if state.newest_document_schema_version() == document_schema_version(lane) {
            return None;
        }
        let per_million = PROBE_DOCUMENTS
            .saturating_mul(1_000_000)
            .div_ceil(state.document_count.max(1))
            .min(1_000_000);
        Some(Self { state, per_million })
    }

    /// Whether the scan keeps `pnu` for the probe: the same draw as gate (나)'s sample, at the
    /// probe's rate.
    pub(super) fn keeps(&self, pnu: &str) -> bool {
        gate::is_sample_candidate_at(pnu, self.per_million).unwrap_or(false)
    }
}

/// Compares the probe's documents with their served answers.
///
/// # Errors
/// Refuses when a sampled unchanged document renders other content than it is served with, or a
/// pack cannot be read.
pub(super) async fn run(
    probe: &Probe,
    store: &ByPnuServingStore,
    provenance: &PackProvenance,
    renderer: &Renderer,
    rows: &[JsonMap<String, JsonValue>],
    changed: Option<&BTreeSet<String>>,
    max_concurrency: usize,
) -> anyhow::Result<ExpansionProbe> {
    let lane = store.lane();
    let mut by_dong: BTreeMap<String, Vec<PackDocument>> = BTreeMap::new();
    for row in rows {
        let pnu = row
            .get("pnu")
            .and_then(JsonValue::as_str)
            .context("a Gold row is missing pnu")?;
        if !probe.keeps(pnu) {
            continue;
        }
        let key = renderer.key(row)?;
        if changed.is_some_and(|changed| changed.contains(&key)) {
            continue;
        }
        by_dong
            .entry(by_pnu_packs::unit_of(pnu)?.to_owned())
            .or_default()
            .push(renderer.render(provenance, row)?);
    }
    let view = PackView::served(store, lane, &probe.state).await?;
    // Served answers are read as every reader reads them: under the recorded versions and the
    // ones the contract lets the lane serve (a record that lags its documents, 2026-10-28).
    let schemas = probe
        .state
        .servable_document_schema_versions(lane)
        .into_iter()
        .collect::<Vec<_>>();
    // Futures are built in a loop, as the bake's are (the command table's `Send` check).
    let mut jobs = Vec::with_capacity(by_dong.len());
    for (dong, documents) in &by_dong {
        jobs.push(compare_dong(store, &view, &schemas, dong, documents));
    }
    let compared_dongs = stream::iter(jobs)
        .buffer_unordered(max_concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    let mut outcome = ExpansionProbe {
        served_document_schema_versions: probe
            .state
            .served_document_schema_versions()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect(),
        document_schema_version: document_schema_version(lane).to_owned(),
        ..ExpansionProbe::default()
    };
    let mut differing = Vec::new();
    for (compared, unequal) in compared_dongs {
        outcome.compared += compared;
        outcome.equal += compared - u64::try_from(unequal.len())?;
        differing.extend(unequal);
    }
    differing.sort_unstable();
    differing.truncate(MAX_DIFFERING);
    ensure!(
        outcome.equal == outcome.compared,
        "{} of {} sampled documents the change set does not name render other content under {} \
         than the {schemas:?} documents they are served as (first: {differing:?}): the new version \
         changes documents no Gold row change marks, so it does not expand the served ones. Carry \
         the new field in the Gold rows it fills, or bake a whole generation \
         (FOUNDATION_BY_PNU_BAKE_FORCE_FULL with a reason) (root ADR-0320)",
        outcome.compared - outcome.equal,
        outcome.compared,
        outcome.document_schema_version
    );
    Ok(outcome)
}

/// One dong's probe documents against their served answers: how many were compared, and the
/// ones that differ.
async fn compare_dong(
    store: &ByPnuServingStore,
    view: &PackView,
    schemas: &[&str],
    dong: &str,
    documents: &[PackDocument],
) -> anyhow::Result<(u64, Vec<String>)> {
    let mut bases = Vec::new();
    for section in &view.sections {
        for unit in section.parts.units_of_dong(dong)? {
            let key =
                by_pnu_packs::pack_key(view.lane, &section.name, section.generation, None, &unit)?;
            if store.exists(&key).await? {
                bases.push((section.name.clone(), unit));
            }
        }
    }
    let packs = read::load_unit(store, view, dong, &|section, unit| {
        bases
            .iter()
            .any(|(name, known)| name == section && known == unit)
    })
    .await?;
    let mut differing = Vec::new();
    for document in documents {
        let same = match read::resolve(&packs, &document.pnu) {
            Ok(Resolved::Document(fragments)) => {
                match read::joined_bytes(view.lane, schemas, &fragments) {
                    Ok(served) => same_content(&served, &document.bytes)?,
                    Err(_) => false,
                }
            }
            // An unchanged Gold row is served: anything else is a difference too.
            _ => false,
        };
        if !same {
            differing.push(document.pnu.clone());
        }
    }
    Ok((u64::try_from(documents.len())?, differing))
}

/// Whether two documents of the lane say the same, whatever version and snapshot stamped them: a
/// null member is the field's absence (an optional field an older version never wrote).
fn same_content(served: &[u8], rendered: &[u8]) -> anyhow::Result<bool> {
    Ok(content(served)? == content(rendered)?)
}

fn content(bytes: &[u8]) -> anyhow::Result<JsonValue> {
    let mut document: JsonValue =
        serde_json::from_slice(bytes).context("a document is not JSON")?;
    if let Some(fields) = document.as_object_mut() {
        fields.remove("schema_version");
        fields.remove("source");
    }
    drop_nulls(&mut document);
    Ok(document)
}

fn drop_nulls(value: &mut JsonValue) {
    match value {
        JsonValue::Object(fields) => {
            fields.retain(|_, field| !field.is_null());
            fields.values_mut().for_each(drop_nulls);
        }
        JsonValue::Array(items) => items.iter_mut().for_each(drop_nulls),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stamp and the source differ between any two bakes; a null is an absent optional field.
    #[test]
    fn content_sets_the_stamp_and_source_aside_and_reads_null_as_absent() -> anyhow::Result<()> {
        let served = br#"{"schema_version":"doc.v1","pnu":"9999900000100000000",
            "source":{"table":"gold.x","iceberg_snapshot_id":"1"},"price":{"amount":1}}"#;
        let rendered = br#"{"schema_version":"doc.v2","pnu":"9999900000100000000",
            "source":{"table":"gold.x","iceberg_snapshot_id":"2"},
            "price":{"amount":1,"note":null},"legal_dong":null}"#;
        assert!(same_content(served, rendered)?);
        // A value the served document lacks is a difference, and so is an empty list for nothing.
        for rendered in [
            br#"{"schema_version":"doc.v2","pnu":"9999900000100000000","price":{"amount":1,"note":"n"}}"#
                .as_slice(),
            br#"{"schema_version":"doc.v2","pnu":"9999900000100000000","price":{"amount":1},"photos":[]}"#
                .as_slice(),
            br#"{"schema_version":"doc.v2","pnu":"9999900000100000000","price":{"amount":2}}"#
                .as_slice(),
        ] {
            assert!(!same_content(served, rendered)?);
        }
        Ok(())
    }
}
