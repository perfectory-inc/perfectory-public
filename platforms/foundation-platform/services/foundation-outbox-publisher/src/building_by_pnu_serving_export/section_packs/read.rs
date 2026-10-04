//! Which pack answers for a PNU, read the way the gateway reads it (root ADR-0147 §3, §4).
//!
//! Per section: the newest patch whose unit list holds the PNU's legal dong and whose index holds
//! the PNU, else the section's base pack. The anchor section decides whether the PNU answers;
//! every other section must agree with it (a document beside a document, a tombstone or nothing
//! beside a tombstone), and a disagreement is an error, never a partial document. The Worker
//! (`foundation-building-gateway/src/packs.ts`) follows the same rules; the golden fixtures pin
//! both.

use anyhow::{bail, ensure, Context};

use super::sections;
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_pack::{EntryState, Pack};
use crate::by_pnu_section_pack_manifest::SectionPacksState;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::r2_layout::by_pnu_packs;

const LANE: ByPnuLane = ByPnuLane::Building;

/// What is read, per section: the base generation and the patches above the section's floor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackView {
    pub(crate) sections: Vec<SectionView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SectionView {
    pub(crate) name: String,
    pub(crate) generation: u64,
    /// Newest first: (patch, the units it wrote packs for).
    pub(crate) patches: Vec<(u64, Vec<String>)>,
}

impl PackView {
    /// What a manifest's `section_packs` block serves.
    pub(crate) fn served(state: &SectionPacksState) -> Self {
        Self {
            sections: state
                .sections
                .iter()
                .map(|section| SectionView {
                    name: section.name.clone(),
                    generation: section.generation,
                    patches: state
                        .patches_of(section)
                        .map(|patch| (patch.patch, patch.units.clone()))
                        .collect(),
                })
                .collect(),
        }
    }

    /// Every contract section at one base generation with no patches: a generation baked and not
    /// yet published, the one the cut-over gate examines.
    ///
    /// # Errors
    /// Returns an error when the contract has no building sections.
    pub(crate) fn unpublished(generation: u64) -> anyhow::Result<Self> {
        Ok(Self {
            sections: LANE
                .section_packs()?
                .sections
                .iter()
                .map(|name| SectionView {
                    name: name.clone(),
                    generation,
                    patches: Vec::new(),
                })
                .collect(),
        })
    }
}

/// One section's packs of one legal dong: its patches (newest first) and its base.
#[derive(Clone, Debug)]
pub(crate) struct SectionPacksOfUnit {
    pub(crate) name: String,
    pub(crate) patches: Vec<(u64, Pack)>,
    pub(crate) base: Option<Pack>,
}

/// Every section's packs of one legal dong.
#[derive(Clone, Debug)]
pub(crate) struct UnitPacks {
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

/// Reads one legal dong's packs. `base_units` and the view's patch unit lists say which packs
/// exist; a pack that should exist and cannot be read is an error, not an absence.
///
/// # Errors
/// Returns an error when a pack cannot be read or does not check out.
pub(crate) async fn load_unit(
    store: &ByPnuServingStore,
    view: &PackView,
    unit: &str,
    base_units: &(dyn Fn(&str, &str) -> bool + Sync),
) -> anyhow::Result<UnitPacks> {
    let mut sections = Vec::with_capacity(view.sections.len());
    for section in &view.sections {
        let mut patches = Vec::new();
        for (patch, units) in &section.patches {
            if units
                .binary_search_by(|candidate| candidate.as_str().cmp(unit))
                .is_ok()
            {
                let key = by_pnu_packs::pack_key(
                    LANE,
                    &section.name,
                    section.generation,
                    Some(*patch),
                    unit,
                )?;
                patches.push((*patch, read_pack(store, &key).await?));
            }
        }
        let base = if base_units(&section.name, unit) {
            let key = by_pnu_packs::pack_key(LANE, &section.name, section.generation, None, unit)?;
            Some(read_pack(store, &key).await?)
        } else {
            None
        };
        sections.push(SectionPacksOfUnit {
            name: section.name.clone(),
            patches,
            base,
        });
    }
    Ok(UnitPacks { sections })
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
    if let Some(pack) = &section.base {
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
    let anchor = &LANE.section_packs()?.anchor_section;
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

/// The served bytes of a resolved document.
///
/// # Errors
/// Refuses fragments that do not join.
pub(crate) fn joined_bytes(fragments: &[(String, Found)]) -> anyhow::Result<Vec<u8>> {
    let mut parts = Vec::with_capacity(fragments.len());
    for (name, found) in fragments {
        let Found::Document { bytes, .. } = found else {
            bail!("section {name} has no document to join");
        };
        parts.push((name.as_str(), bytes.as_slice()));
    }
    sections::join(&parts)?.to_bytes()
}
