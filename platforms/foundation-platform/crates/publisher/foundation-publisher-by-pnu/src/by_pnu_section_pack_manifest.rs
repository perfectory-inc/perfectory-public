//! The manifest's `section_packs` block (root ADR-0147 §4, §5; its own `schema_version` 3).
//!
//! The manifest envelope stays a v2 manifest: every v2 field keeps describing the object lane as
//! it was when packs took over, so a rolled-back Worker or publisher still reads it and serves
//! objects. The block is additive; v2 readers ignore it (they tolerate unknown fields), and a
//! Worker that knows it serves from packs exactly when it is present.
//!
//! - `sections`: per section, the generation its base packs are in, and `patch_floor`, the newest
//!   patch already folded into that generation (a section re-baked alone starts above it).
//! - `patches`: the lane's patches, newest first. Patch `m` of a section lives under that
//!   section's generation (`g{generation}/p{m}/`) and only for sections whose floor is below `m`.
//!   `units` lists the legal dongs the patch wrote a pack for, so a reader skips a patch without
//!   a read.
//!
//! Reading and writing check different things, as for the v2 envelope: a reader holds the block
//! to what the format needs, a writer also to the contract as it is now.

use std::collections::BTreeSet;

use anyhow::ensure;
use serde::{Deserialize, Serialize};

use crate::by_pnu_gateway_contract::{
    by_pnu_serving_patch_policy, section_pack_policy, ByPnuLane, PNU_PREFIX_LENGTHS,
};

/// What the served lane reads from packs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SectionPacksState {
    pub schema_version: u32,
    pub format_version: u16,
    pub unit_prefix_length: usize,
    pub document_schema_version: String,
    pub gold_table: String,
    /// The Gold snapshot whose changes the packs fully reflect: the next change set of the pack
    /// lane is computed against it.
    pub reflected_gold_iceberg_snapshot_id: String,
    /// The Iceberg tag pinning `reflected_gold_iceberg_snapshot_id` (root ADR-0146 §2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reflected_gold_snapshot_tag: Option<String>,
    /// PNUs that answer.
    pub document_count: u64,
    pub sections: Vec<SectionState>,
    /// Newest first.
    pub patches: Vec<PackPatch>,
    /// The cut-over gate evidence the first publish of packs was shown (root ADR-0147 §6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutover: Option<CutoverRecord>,
}

/// One section's base.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SectionState {
    pub name: String,
    pub generation: u64,
    pub gold_iceberg_snapshot_id: String,
    pub document_count: u64,
    pub pack_count: u64,
    /// Patches up to this number are already in `generation`; 0 when none are.
    pub patch_floor: u64,
    /// The parts index of `generation` (root ADR-0163); `None` when no dong of it is cut.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<PartsRef>,
}

/// Where a section generation's parts index is, and the bytes it must have.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartsRef {
    pub key: String,
    pub sha256: String,
    /// How many dongs the index cuts into more than one part.
    pub parted_units: u64,
}

/// One patch of the pack lane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackPatch {
    pub patch: u64,
    pub gold_iceberg_snapshot_id: String,
    pub upserted: u64,
    pub deleted: u64,
    /// The legal dongs the patch wrote packs for, sorted.
    pub units: Vec<String>,
    /// The document schema the patch's documents were baked under, when it is not the base's
    /// (`SectionPacksState::document_schema_version`): a version that expands the base's adds
    /// optional fields only, so a patch of it goes over the base, and the base's documents stay
    /// served as they are until the next whole generation (root ADR-0320). Absent: the base's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_schema_version: Option<String>,
}

/// The evidence files the first pack publish was gated on, by checksum.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CutoverRecord {
    pub equality_evidence_sha256: String,
    pub latency_evidence_sha256: String,
    /// The contract waiver that accepted a latency verdict the gate refused (root ADR-0162).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_waiver: Option<String>,
}

impl SectionPacksState {
    /// The block's `schema_version` for its sections: the parted one when any names a parts index.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub fn schema_version_for_sections(&self) -> anyhow::Result<u32> {
        let policy = section_pack_policy()?;
        Ok(
            if self.sections.iter().any(|section| section.parts.is_some()) {
                policy.manifest_section_packs_parted_schema_version
            } else {
                policy.manifest_section_packs_schema_version
            },
        )
    }

    /// The newest patch number any section holds, 0 when there is none.
    pub fn newest_patch(&self) -> u64 {
        self.patches.first().map_or(0, |patch| patch.patch).max(
            self.sections
                .iter()
                .map(|s| s.patch_floor)
                .max()
                .unwrap_or(0),
        )
    }

    /// The document schema `patch`'s documents were baked under.
    pub fn document_schema_of<'a>(&'a self, patch: &'a PackPatch) -> &'a str {
        patch
            .document_schema_version
            .as_deref()
            .unwrap_or(&self.document_schema_version)
    }

    /// The document schema of the newest documents the lane serves: the newest patch's, else the
    /// base's. The next patch is baked under it or under a version that expands it.
    pub fn newest_document_schema_version(&self) -> &str {
        self.patches
            .first()
            .map_or(self.document_schema_version.as_str(), |patch| {
                self.document_schema_of(patch)
            })
    }

    /// Every document schema an answer of the lane may be stamped with: the base's and every
    /// listed patch's (root ADR-0320). One until a patch expands the base's version.
    pub fn served_document_schema_versions(&self) -> BTreeSet<&str> {
        std::iter::once(self.document_schema_version.as_str())
            .chain(
                self.patches
                    .iter()
                    .map(|patch| self.document_schema_of(patch)),
            )
            .collect()
    }

    /// Every document schema a reader holds the lane's served answers to: the ones this state
    /// records ([`Self::served_document_schema_versions`]) and the ones the contract lets the lane
    /// serve now (`readable_document_schema_versions`: the version the release bakes and the ones
    /// it expands, root ADR-0320). A schema bump alone never makes a served document unreadable.
    ///
    /// The records alone are not enough: until 2026-10-29 a whole generation kept the record of
    /// the generation it replaced, so parcel generation 6 and public-auction generation 2
    /// (2026-10-28) served v2 documents under a v1 record, and every hourly serving monitor run of
    /// both lanes failed "the documents fragment is not a ...v1 document".
    ///
    /// A lane whose subject the schema registry lists (root ADR-0336) is read by its subject and
    /// major alone: every string of every major the registry lists, whatever the state records,
    /// and nothing else; a revision added to the same major changes neither.
    pub fn servable_document_schema_versions(&self, lane: ByPnuLane) -> BTreeSet<String> {
        // The registry is compiled in and read before any lane exists (resolve_lane_entry), so an
        // unreadable one never reaches here.
        if let Ok(Some(subject)) = crate::schema_registry::subject_of_lane(lane.name()) {
            return subject
                .majors()
                .into_iter()
                .flat_map(|major| subject.wire_versions(major))
                .collect();
        }
        let mut schemas = self
            .served_document_schema_versions()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        schemas.extend(
            crate::by_pnu_serving_manifest_publish::readable_document_schema_versions(lane)
                .into_iter()
                .map(ToOwned::to_owned),
        );
        schemas
    }

    /// The patches that apply to `section`, newest first.
    pub fn patches_of<'a>(
        &'a self,
        section: &'a SectionState,
    ) -> impl Iterator<Item = &'a PackPatch> + 'a {
        self.patches
            .iter()
            .filter(move |patch| patch.patch > section.patch_floor)
    }

    /// Checks what every reader relies on, and nothing a contract change can move.
    ///
    /// # Errors
    /// Refuses another schema, a unit length no PNU has, an empty or repeated section list, and
    /// patches that are not strictly newest first with sorted distinct units of the block's length.
    pub fn check_readable(&self) -> anyhow::Result<()> {
        let policy = section_pack_policy()?;
        // A block whose sections name a parts index is the parted version, and only it may: a
        // reader of the older version would look a cut dong up under its whole key (ADR-0163).
        let wanted = self.schema_version_for_sections()?;
        ensure!(
            self.schema_version == wanted,
            "section_packs schema_version {} is not the {wanted} its sections need (this reader \
             knows {} and {})",
            self.schema_version,
            policy.manifest_section_packs_schema_version,
            policy.manifest_section_packs_parted_schema_version
        );
        for section in &self.sections {
            if let Some(parts) = &section.parts {
                ensure!(
                    parts.sha256.len() == 64
                        && parts.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
                        && !parts.key.is_empty()
                        && parts.parted_units > 0,
                    "section {} names a malformed parts index",
                    section.name
                );
            }
        }
        ensure!(
            PNU_PREFIX_LENGTHS.contains(&self.unit_prefix_length),
            "section_packs unit_prefix_length {} is not a PNU prefix length",
            self.unit_prefix_length
        );
        let mut names = BTreeSet::new();
        for section in &self.sections {
            ensure!(
                section.generation >= 1 && names.insert(section.name.as_str()),
                "section_packs lists section {:?} twice or at generation 0",
                section.name
            );
        }
        ensure!(!self.sections.is_empty(), "section_packs lists no sections");
        ensure!(
            self.patches.len() <= by_pnu_serving_patch_policy()?.manifest_patch_ceiling,
            "section_packs lists more patches than the manifest_patch_ceiling"
        );
        for pair in self.patches.windows(2) {
            ensure!(
                pair[0].patch > pair[1].patch,
                "section_packs patches must be newest first with distinct numbers"
            );
        }
        for patch in &self.patches {
            ensure!(
                patch.patch >= 1
                    && patch.upserted + patch.deleted > 0
                    && !patch.units.is_empty()
                    && patch.units.windows(2).all(|pair| pair[0] < pair[1])
                    && patch.units.iter().all(|unit| {
                        crate::r2_layout::by_pnu_packs::dong_of(unit).len()
                            == self.unit_prefix_length
                            && crate::r2_layout::by_pnu_packs::check_unit(unit).is_ok()
                    }),
                "section_packs patch {} must hold changes under sorted distinct {}-digit units",
                patch.patch,
                self.unit_prefix_length
            );
        }
        Ok(())
    }

    /// Checks a block about to be written: readable, and within the contract of `lane` now.
    ///
    /// # Errors
    /// Refuses what [`Self::check_readable`] refuses, sections other than the contract's in its
    /// order, another unit length or format, more live patches than `max_patches`, and patches no
    /// section still applies.
    pub fn check_writable(&self, lane: ByPnuLane) -> anyhow::Result<()> {
        self.check_readable()?;
        let policy = section_pack_policy()?;
        let contract = lane.section_packs()?;
        ensure!(
            self.sections
                .iter()
                .map(|section| section.name.as_str())
                .eq(contract.sections.iter().map(String::as_str)),
            "section_packs must list the contract's sections {:?} in order",
            contract.sections
        );
        ensure!(
            self.unit_prefix_length == policy.unit_prefix_length
                && self.format_version == policy.format_version,
            "section_packs must use the contract's unit length {} and pack format {}",
            policy.unit_prefix_length,
            policy.format_version
        );
        let lowest_floor = self
            .sections
            .iter()
            .map(|s| s.patch_floor)
            .min()
            .unwrap_or(0);
        ensure!(
            self.patches.iter().all(|patch| patch.patch > lowest_floor),
            "section_packs keeps a patch every section has already folded in"
        );
        let max = by_pnu_serving_patch_policy()?.max_patches;
        for section in &self.sections {
            let live = self.patches_of(section).count();
            ensure!(
                live <= max,
                "section {} would read {live} patches, over the contract's max_patches {max}; a \
                 new generation of it compacts them",
                section.name
            );
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn state(generation: u64) -> anyhow::Result<SectionPacksState> {
        let lane = ByPnuLane::Building;
        Ok(SectionPacksState {
            schema_version: section_pack_policy()?.manifest_section_packs_schema_version,
            format_version: section_pack_policy()?.format_version,
            unit_prefix_length: section_pack_policy()?.unit_prefix_length,
            document_schema_version: "doc.v2".to_owned(),
            gold_table: "gold.building_panel".to_owned(),
            reflected_gold_iceberg_snapshot_id: "999990000000000001".to_owned(),
            reflected_gold_snapshot_tag: None,
            document_count: 2,
            sections: lane
                .section_packs()?
                .sections
                .iter()
                .map(|name| SectionState {
                    name: name.clone(),
                    generation,
                    gold_iceberg_snapshot_id: "999990000000000001".to_owned(),
                    document_count: 2,
                    pack_count: 1,
                    patch_floor: 0,
                    parts: None,
                })
                .collect(),
            patches: Vec::new(),
            cutover: None,
        })
    }

    pub fn patch(number: u64, units: &[&str]) -> PackPatch {
        PackPatch {
            patch: number,
            gold_iceberg_snapshot_id: "999990000000000002".to_owned(),
            upserted: 1,
            deleted: 1,
            units: units.iter().map(|unit| (*unit).to_owned()).collect(),
            document_schema_version: None,
        }
    }

    /// A patch of a version that expands the base's names it; one of the base's does not, and
    /// a block written before root ADR-0320 reads as all of the base's.
    #[test]
    fn a_patch_of_an_expanding_version_names_it_beside_the_base() -> anyhow::Result<()> {
        let mut block = state(1)?;
        assert_eq!(block.newest_document_schema_version(), "doc.v2");
        block.patches.push(patch(1, &["9999900001"]));
        assert_eq!(block.newest_document_schema_version(), "doc.v2");
        let mut expanding = patch(2, &["9999900001"]);
        expanding.document_schema_version = Some("doc.v3".to_owned());
        block.patches.insert(0, expanding);
        block.check_writable(ByPnuLane::Building)?;
        assert_eq!(block.newest_document_schema_version(), "doc.v3");
        assert_eq!(
            block.served_document_schema_versions(),
            BTreeSet::from(["doc.v2", "doc.v3"])
        );
        let written = serde_json::to_value(&block)?;
        assert_eq!(written["patches"][0]["document_schema_version"], "doc.v3");
        assert!(written["patches"][1]
            .get("document_schema_version")
            .is_none());
        let read: SectionPacksState = serde_json::from_value(written)?;
        assert_eq!(read, block);
        Ok(())
    }

    /// 2026-10-28: parcel and public-auction served v2 documents under a v1 record, and their
    /// hourly monitors failed every run. A reader takes what the state records and what the
    /// contract lets the lane serve, so neither a lying record nor a schema bump refuses a
    /// document the lane may serve; a version in neither stays refused.
    #[test]
    fn a_reader_takes_the_records_and_what_the_contract_lets_the_lane_serve() -> anyhow::Result<()>
    {
        let block = state(1)?;
        let lane = ByPnuLane::Building;
        let servable = block.servable_document_schema_versions(lane);
        assert!(servable.contains("doc.v2"), "{servable:?}");
        for readable in
            crate::by_pnu_serving_manifest_publish::readable_document_schema_versions(lane)
        {
            assert!(
                servable.contains(readable),
                "{readable} missing from {servable:?}"
            );
        }
        assert!(!servable.contains("doc.v0"));
        Ok(())
    }

    /// Root ADR-0336: the public auction lane's readers take its subject and major from the
    /// registry, never the state's record (which #929 found lying) nor a list of their own.
    #[test]
    fn a_registered_lane_is_read_by_its_subject_and_major_alone() -> anyhow::Result<()> {
        let block = state(1)?;
        let lane = ByPnuLane::named("parcel-public-auction")?
            .ok_or_else(|| anyhow::anyhow!("the contract names the public auction lane"))?;
        let servable = block.servable_document_schema_versions(lane);
        let subject = "foundation-platform.parcel_public_auction_by_pnu";
        assert_eq!(
            servable.into_iter().collect::<Vec<_>>(),
            [format!("{subject}.v1"), format!("{subject}.v2")]
        );
        Ok(())
    }

    #[test]
    fn a_contract_shaped_block_is_writable_and_round_trips() -> anyhow::Result<()> {
        let mut block = state(1)?;
        block.patches = vec![patch(2, &["9999900000"]), patch(1, &["9999900000"])];
        block.check_writable(ByPnuLane::Building)?;
        let read: SectionPacksState = serde_json::from_slice(&serde_json::to_vec(&block)?)?;
        assert_eq!(read, block);
        assert_eq!(read.newest_patch(), 2);
        Ok(())
    }

    #[test]
    fn blocks_that_break_the_format_are_refused() -> anyhow::Result<()> {
        let refused = |label: &str, block: SectionPacksState| {
            assert!(
                block.check_writable(ByPnuLane::Building).is_err(),
                "{label} was written"
            );
        };
        let mut wrong_schema = state(1)?;
        wrong_schema.schema_version = 4;
        assert!(
            wrong_schema.check_readable().is_err(),
            "an unknown schema was read"
        );
        let mut missing = state(1)?;
        missing.sections.pop();
        refused("a missing section", missing);
        let mut renamed = state(1)?;
        renamed.sections[0].name = "zonings".to_owned();
        refused("a section the contract does not name", renamed);
        let mut oldest_first = state(1)?;
        oldest_first.patches = vec![patch(1, &["9999900000"]), patch(2, &["9999900000"])];
        refused("patches oldest first", oldest_first);
        let mut short_unit = state(1)?;
        short_unit.patches = vec![patch(1, &["99999"])];
        refused("a five-digit unit", short_unit);
        let mut folded = state(1)?;
        for section in &mut folded.sections {
            section.patch_floor = 3;
        }
        folded.patches = vec![patch(3, &["9999900000"])];
        refused("a patch every section folded in", folded);
        let mut too_many = state(1)?;
        let max = u64::try_from(by_pnu_serving_patch_policy()?.max_patches)?;
        too_many.patches = (1..=max + 1)
            .rev()
            .map(|n| patch(n, &["9999900000"]))
            .collect();
        refused("more patches than max_patches", too_many.clone());
        too_many.check_readable()?;
        Ok(())
    }
}
