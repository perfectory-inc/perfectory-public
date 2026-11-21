// one-off: until the last served Gold has moved to its `_versions` table (root ADR-0564 §6): no
// lane is left whose packs were baked from the table its Gold replaces, so no switch is left to
// check; this module and `show-gold-takeover-switch` go.
//! The content check of a takeover's one full bake (root ADR-0564 §6-5, the switch).
//!
//! The lane's packs were baked from the replaced table (`gold.<x>`); the lane now bakes its
//! versions table (`gold.<x>_versions`). The verdict before the switch said the two tables hold
//! the same rows for one build pair: the replaced table's build-tag commit and the versions
//! table's shadow commit of the same input pins. It said nothing of what the lane serves.
//!
//! So the switch is baked from that pair and nothing else: the bake script pins the full bake to
//! the shadow commit and refuses unless the packs reflect the replaced table's build
//! (`show-gold-takeover-switch`). Then every document must answer exactly as the served one: its
//! content (the version stamp and the source set aside, a null read as absent, as the expansion
//! probe reads it, root ADR-0320) equal, and no served PNU left without a document. One difference
//! is the switch changing documents no Gold change explains (a renderer or a current-row rule that
//! reads the new table otherwise than the old) and refuses its dong before anything of it is
//! written, so nothing is published. Changes after the pair come by the next run's patch, through
//! the versions table's own change set.
//!
//! The check is not a switch to turn on: every base bake whose packs were baked from another
//! table than the lane's runs it, and a patch over such packs is refused. It reads each served
//! pack of a dong once (one GET per section and unit) and lists each section's served generation
//! once per shard. A document served as pages is compared by its first page.

use std::collections::BTreeSet;

use anyhow::ensure;

use super::expansion::{load_served_dong, same_content};
use super::read::{self, PackView, Resolved};
use super::sections::PackDocument;
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_section_pack_manifest::SectionPacksState;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::r2_layout::by_pnu_packs;

/// How many differing PNUs a refusal names.
const MAX_DIFFERING: usize = 20;

/// The switch's check: the served packs of the replaced table.
pub(super) struct Takeover {
    pub(super) served_table: String,
    view: PackView,
    schemas: Vec<String>,
}

/// What the check compared, recorded in the export summary.
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct TakeoverCheck {
    /// The table the served packs were baked from.
    pub served_table: String,
    /// Documents of this run held to their served answer, every one equal.
    pub compared: u64,
}

impl Takeover {
    /// The check a base bake of `table` needs: its packs serve another table's documents.
    ///
    /// # Errors
    /// Refuses a patch over another table's packs: no change set spans two tables.
    pub(super) async fn needed(
        lane: ByPnuLane,
        table: &str,
        served: Option<SectionPacksState>,
        is_patch: bool,
        store: &ByPnuServingStore,
    ) -> anyhow::Result<Option<Self>> {
        let Some(state) = served.filter(|state| state.gold_table != table) else {
            return Ok(None);
        };
        ensure!(
            !is_patch,
            "the lane's packs were baked from {}, and a patch of {table} cannot go over them: no \
             change set spans two tables; the switch bakes a whole generation (root ADR-0564 §6)",
            state.gold_table
        );
        let servable = state.servable_document_schema_versions(lane);
        Ok(Some(Self {
            served_table: state.gold_table.clone(),
            view: PackView::served(store, lane, &state).await?,
            schemas: servable.into_iter().collect(),
        }))
    }

    /// The dongs of the shard the served packs answer for: every one is checked, also those the
    /// new table holds no row of.
    pub(super) async fn served_dongs(
        &self,
        store: &ByPnuServingStore,
        in_shard: &(dyn Fn(&str) -> bool + Sync),
    ) -> anyhow::Result<BTreeSet<String>> {
        let mut dongs = BTreeSet::new();
        for section in &self.view.sections {
            for key in store
                .list_pack_keys(&section.name, section.generation, None)
                .await?
            {
                if let Some(parsed) = by_pnu_packs::parse_pack_key(self.view.lane, &key) {
                    dongs.insert(by_pnu_packs::dong_of(&parsed.unit).to_owned());
                }
            }
            for (_, units) in &section.patches {
                dongs.extend(
                    units
                        .iter()
                        .map(|unit| by_pnu_packs::dong_of(unit).to_owned()),
                );
            }
        }
        dongs.retain(|dong| in_shard(dong));
        Ok(dongs)
    }

    /// The summary record of the whole run.
    pub(super) fn record(&self, compared: u64) -> TakeoverCheck {
        TakeoverCheck {
            served_table: self.served_table.clone(),
            compared,
        }
    }

    /// Holds one dong's documents to its served answers; returns how many were compared.
    ///
    /// # Errors
    /// Refuses the dong when a document differs from its served one, or a served PNU has no
    /// document now.
    pub(super) async fn check_dong(
        &self,
        store: &ByPnuServingStore,
        dong: &str,
        documents: &[PackDocument],
    ) -> anyhow::Result<u64> {
        let packs = load_served_dong(store, &self.view, dong).await?;
        let schemas = self.schemas.iter().map(String::as_str).collect::<Vec<_>>();
        let mut differing = Vec::new();
        let rendered = documents
            .iter()
            .map(|document| document.pnu.as_str())
            .collect::<BTreeSet<_>>();
        for document in documents {
            let same = match read::resolve(&packs, &document.pnu)? {
                Resolved::Document(fragments) => {
                    match read::joined_bytes(self.view.lane, &schemas, &fragments) {
                        Ok(served) => same_content(&served, &document.bytes)?,
                        Err(_) => false,
                    }
                }
                Resolved::Tombstone | Resolved::Absent => false,
            };
            if !same {
                differing.push(document.pnu.clone());
            }
        }
        // A PNU served now that the new table no longer answers.
        let mut served = BTreeSet::new();
        for section in &packs.sections {
            for pack in section
                .bases
                .iter()
                .chain(section.patches.iter().map(|(_, pack)| pack))
            {
                served.extend(pack.entries.iter().map(|entry| entry.pnu.clone()));
            }
        }
        for pnu in served.iter().filter(|pnu| !rendered.contains(pnu.as_str())) {
            if matches!(read::resolve(&packs, pnu)?, Resolved::Document(_)) {
                differing.push(pnu.clone());
            }
        }
        let count = differing.len();
        differing.sort_unstable();
        differing.truncate(MAX_DIFFERING);
        ensure!(
            count == 0,
            "legal dong {dong}: {count} PNUs answer otherwise than the {} packs serve them (first: \
             {differing:?}): the switch changes documents its verdict-equal build pair does not. \
             Nothing of this dong is written and nothing is published (root ADR-0564 §6-5)",
            self.served_table
        );
        Ok(u64::try_from(documents.len())?)
    }
}
