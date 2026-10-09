//! Which pack answers for a PNU, read the way the gateway reads it (root ADR-0147 §3, §4).
//!
//! Per section: the newest patch whose unit list holds the PNU's unit and whose index holds the
//! PNU, else the section's base pack. A PNU's unit is its legal dong, or the part of it the
//! section generation's parts index cuts it into (root ADR-0163). The anchor section decides whether the PNU answers;
//! every other section must agree with it (a document beside a document, a tombstone or nothing
//! beside a tombstone), and a disagreement is an error, never a partial document. The Worker
//! (`foundation-by-pnu-gateway/src/packs.ts`, one source for both lanes) follows the
//! same rules; the golden fixtures pin both.

use anyhow::{bail, ensure, Context};
use sha2::{Digest, Sha256};

use super::sections;
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_pack::{EntryState, Pack};
use crate::by_pnu_section_pack_manifest::{PartsRef, SectionPacksState};
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::r2_layout::by_pnu_packs::{self, Parts, PartsIndexFile};

/// What is read, per section: the base generation and the patches above the section's floor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackView {
    pub(crate) lane: ByPnuLane,
    pub(crate) sections: Vec<SectionView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SectionView {
    pub(crate) name: String,
    pub(crate) generation: u64,
    /// Newest first: (patch, the units it wrote packs for).
    pub(crate) patches: Vec<(u64, Vec<String>)>,
    /// How `generation` cuts its dongs; empty when it cuts none.
    pub(crate) parts: Parts,
}

impl PackView {
    /// What a manifest's `section_packs` block serves, with each section's parts index read and
    /// checked against the sha256 the block names.
    ///
    /// # Errors
    /// Returns an error when a named parts index cannot be read or is not the one named.
    pub(crate) async fn served(
        store: &ByPnuServingStore,
        lane: ByPnuLane,
        state: &SectionPacksState,
    ) -> anyhow::Result<Self> {
        let mut sections = Vec::with_capacity(state.sections.len());
        for section in &state.sections {
            let parts = match &section.parts {
                Some(named) => {
                    read_parts(store, lane, &section.name, section.generation, named).await?
                }
                None => Parts::default(),
            };
            sections.push(SectionView {
                name: section.name.clone(),
                generation: section.generation,
                patches: state
                    .patches_of(section)
                    .map(|patch| (patch.patch, patch.units.clone()))
                    .collect(),
                parts,
            });
        }
        Ok(Self { lane, sections })
    }

    /// Every contract section at one base generation with no patches: a generation baked and not
    /// yet published, the one the cut-over gate examines. Its parts are the index the bake's
    /// publish would name, read where it is written; a generation without one cuts no dong.
    ///
    /// # Errors
    /// Returns an error when the contract has no sections for the lane, or an index there does
    /// not read as this generation's.
    pub(crate) async fn unpublished(
        store: &ByPnuServingStore,
        lane: ByPnuLane,
        generation: u64,
    ) -> anyhow::Result<Self> {
        let mut sections = Vec::new();
        for name in &lane.section_packs()?.sections {
            let key = by_pnu_packs::parts_index_key(lane, name, generation)?;
            let parts = match store.read_optional_bytes(&key).await? {
                Some(bytes) => parse_parts(lane, name, generation, &key, &bytes)?,
                None => Parts::default(),
            };
            sections.push(SectionView {
                name: name.clone(),
                generation,
                patches: Vec::new(),
                parts,
            });
        }
        Ok(Self { lane, sections })
    }
}

/// A section generation's parts index, holding it to the key and bytes the manifest names.
async fn read_parts(
    store: &ByPnuServingStore,
    lane: ByPnuLane,
    section: &str,
    generation: u64,
    named: &PartsRef,
) -> anyhow::Result<Parts> {
    let key = by_pnu_packs::parts_index_key(lane, section, generation)?;
    ensure!(
        named.key == key,
        "section {section} names its parts index at {}, not at {key}",
        named.key
    );
    let bytes = store
        .read_bytes(&key)
        .await
        .with_context(|| format!("the manifest names {key} but it cannot be read"))?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    ensure!(
        sha256 == named.sha256,
        "{key} hashes to {sha256}, not the {} the manifest names",
        named.sha256
    );
    let parts = parse_parts(lane, section, generation, &key, &bytes)?;
    ensure!(
        u64::try_from(parts.counts().len())? == named.parted_units,
        "{key} cuts {} dongs, not the {} the manifest names",
        parts.counts().len(),
        named.parted_units
    );
    Ok(parts)
}

fn parse_parts(
    lane: ByPnuLane,
    section: &str,
    generation: u64,
    key: &str,
    bytes: &[u8],
) -> anyhow::Result<Parts> {
    let file: PartsIndexFile =
        serde_json::from_slice(bytes).with_context(|| format!("{key} is not a parts index"))?;
    file.parts_of(lane, section, generation)
}

/// One section's packs of one legal dong: its patches (newest first) and its base packs, one per
/// unit of the dong (its parts, or the dong itself). A PNU is in exactly one unit, so searching
/// them all finds the one the gateway would read.
#[derive(Clone, Debug)]
pub(crate) struct SectionPacksOfUnit {
    pub(crate) name: String,
    pub(crate) patches: Vec<(u64, Pack)>,
    pub(crate) bases: Vec<Pack>,
}

/// Every section's packs of one legal dong.
#[derive(Clone, Debug)]
pub(crate) struct UnitPacks {
    pub(crate) lane: ByPnuLane,
    pub(crate) sections: Vec<SectionPacksOfUnit>,
}

/// Where one section's entry for a PNU came from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Found {
    Document { patch: Option<u64>, bytes: Vec<u8> },
    Tombstone { patch: u64 },
    Absent,
}

/// A PNU's answer, as the gateway gives it.
#[derive(Clone, Debug)]
pub(crate) enum Resolved {
    /// Every section's fragment, in contract order.
    Document(Vec<(String, Found)>),
    Tombstone,
    Absent,
}

/// Reads one legal dong's packs, every unit of it in each section. `base_units` and the view's
/// patch unit lists say which packs exist; a pack that should exist and cannot be read is an
/// error, not an absence.
///
/// # Errors
/// Returns an error when a pack cannot be read or does not check out.
pub(crate) async fn load_unit(
    store: &ByPnuServingStore,
    view: &PackView,
    dong: &str,
    base_units: &(dyn Fn(&str, &str) -> bool + Sync),
) -> anyhow::Result<UnitPacks> {
    let mut sections = Vec::with_capacity(view.sections.len());
    for section in &view.sections {
        let units = section.parts.units_of_dong(dong)?;
        let mut patches = Vec::new();
        for (patch, written) in &section.patches {
            for unit in &units {
                if written
                    .binary_search_by(|candidate| candidate.as_str().cmp(unit))
                    .is_ok()
                {
                    let key = by_pnu_packs::pack_key(
                        view.lane,
                        &section.name,
                        section.generation,
                        Some(*patch),
                        unit,
                    )?;
                    patches.push((*patch, read_pack(store, &key).await?));
                }
            }
        }
        let mut bases = Vec::new();
        for unit in &units {
            if base_units(&section.name, unit) {
                let key = by_pnu_packs::pack_key(
                    view.lane,
                    &section.name,
                    section.generation,
                    None,
                    unit,
                )?;
                bases.push(read_pack(store, &key).await?);
            }
        }
        sections.push(SectionPacksOfUnit {
            name: section.name.clone(),
            patches,
            bases,
        });
    }
    Ok(UnitPacks {
        lane: view.lane,
        sections,
    })
}

async fn read_pack(store: &ByPnuServingStore, key: &str) -> anyhow::Result<Pack> {
    let bytes = store.read_bytes(key).await?;
    Pack::read(&bytes).with_context(|| format!("{key} is not a readable section pack"))
}

/// One section's entry for `pnu`.
///
/// # Errors
/// Returns an error when an entry does not decode.
pub(crate) fn find(section: &SectionPacksOfUnit, pnu: &str) -> anyhow::Result<Found> {
    for (patch, pack) in &section.patches {
        if let Some(entry) = pack.find(pnu) {
            return Ok(match entry.state {
                EntryState::Tombstone => Found::Tombstone { patch: *patch },
                EntryState::Document => Found::Document {
                    patch: Some(*patch),
                    bytes: pack
                        .document(entry)?
                        .context("a document entry has no body")?,
                },
            });
        }
    }
    for pack in &section.bases {
        if let Some(entry) = pack.find(pnu) {
            return Ok(Found::Document {
                patch: None,
                bytes: pack
                    .document(entry)?
                    .context("a document entry has no body")?,
            });
        }
    }
    Ok(Found::Absent)
}

/// The gateway's answer for `pnu`.
///
/// # Errors
/// Refuses sections that disagree with the anchor.
pub(crate) fn resolve(packs: &UnitPacks, pnu: &str) -> anyhow::Result<Resolved> {
    let anchor = &packs.lane.section_packs()?.anchor_section;
    let mut found = Vec::with_capacity(packs.sections.len());
    for section in &packs.sections {
        found.push((section.name.clone(), find(section, pnu)?));
    }
    let anchor_found = found
        .iter()
        .find(|(name, _)| name == anchor)
        .map(|(_, found)| found.clone())
        .context("the pack view has no anchor section")?;
    match anchor_found {
        Found::Document { .. } => {
            for (name, entry) in &found {
                ensure!(
                    matches!(entry, Found::Document { .. }),
                    "{pnu} has a {anchor} document but its {name} section has none"
                );
            }
            Ok(Resolved::Document(found))
        }
        Found::Tombstone { .. } | Found::Absent => {
            if let Some((name, _)) = found
                .iter()
                .find(|(_, entry)| matches!(entry, Found::Document { .. }))
            {
                bail!("{pnu} has no {anchor} document but its {name} section has one");
            }
            Ok(if matches!(anchor_found, Found::Tombstone { .. }) {
                Resolved::Tombstone
            } else {
                Resolved::Absent
            })
        }
    }
}

/// The served bytes of a resolved document: the decompressed member exactly as the pack holds it,
/// since the Worker hands the member out as it is (root ADR-0151). Never a re-serialisation: a
/// member that parses to the same document in other bytes (key order, spacing, a number's form)
/// is another answer than the object's, and gate (가) must see it as one.
///
/// # Errors
/// Refuses fragments that do not join or are not a document of the lane.
pub(crate) fn joined_bytes(
    lane: ByPnuLane,
    fragments: &[(String, Found)],
) -> anyhow::Result<Vec<u8>> {
    let mut parts = Vec::with_capacity(fragments.len());
    for (name, found) in fragments {
        let Found::Document { bytes, .. } = found else {
            bail!("section {name} has no document to join");
        };
        parts.push((name.as_str(), bytes.as_slice()));
    }
    sections::join(lane, &parts)?;
    let [(_, bytes)] = parts.as_slice() else {
        bail!(
            "a served document is one member, got {} fragments",
            parts.len()
        );
    };
    Ok(bytes.to_vec())
}
