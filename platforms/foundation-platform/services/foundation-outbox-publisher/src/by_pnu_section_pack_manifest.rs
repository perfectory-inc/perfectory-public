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
pub(crate) struct SectionPacksState {
    pub(crate) schema_version: u32,
    pub(crate) format_version: u16,
    pub(crate) unit_prefix_length: usize,
    pub(crate) document_schema_version: String,
    pub(crate) gold_table: String,
    /// The Gold snapshot whose changes the packs fully reflect: the next change set of the pack
    /// lane is computed against it.
    pub(crate) reflected_gold_iceberg_snapshot_id: String,
    /// The Iceberg tag pinning `reflected_gold_iceberg_snapshot_id` (root ADR-0146 §2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reflected_gold_snapshot_tag: Option<String>,
    /// PNUs that answer.
    pub(crate) document_count: u64,
    pub(crate) sections: Vec<SectionState>,
    /// Newest first.
    pub(crate) patches: Vec<PackPatch>,
    /// The cut-over gate evidence the first publish of packs was shown (root ADR-0147 §6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cutover: Option<CutoverRecord>,
}

/// One section's base.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SectionState {
    pub(crate) name: String,
    pub(crate) generation: u64,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) document_count: u64,
    pub(crate) pack_count: u64,
    /// Patches up to this number are already in `generation`; 0 when none are.
    pub(crate) patch_floor: u64,
}

/// One patch of the pack lane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PackPatch {
    pub(crate) patch: u64,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) upserted: u64,
    pub(crate) deleted: u64,
    /// The legal dongs the patch wrote packs for, sorted.
    pub(crate) units: Vec<String>,
}

/// The evidence files the first pack publish was gated on, by checksum.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CutoverRecord {
    pub(crate) equality_evidence_sha256: String,
    pub(crate) latency_evidence_sha256: String,
}

impl SectionPacksState {
    /// The newest patch number any section holds, 0 when there is none.
    pub(crate) fn newest_patch(&self) -> u64 {
        self.patches.first().map_or(0, |patch| patch.patch).max(
            self.sections
                .iter()
                .map(|s| s.patch_floor)
                .max()
                .unwrap_or(0),
        )
    }

    /// The patches that apply to `section`, newest first.
    pub(crate) fn patches_of<'a>(
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
    pub(crate) fn check_readable(&self) -> anyhow::Result<()> {
        let policy = section_pack_policy()?;
        ensure!(
            self.schema_version == policy.manifest_section_packs_schema_version,
            "section_packs schema_version {} is not the {} this reader knows",
            self.schema_version,
            policy.manifest_section_packs_schema_version
        );
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
                        unit.len() == self.unit_prefix_length
                            && unit.bytes().all(|byte| byte.is_ascii_digit())
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
    pub(crate) fn check_writable(&self, lane: ByPnuLane) -> anyhow::Result<()> {
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
pub(crate) mod tests {
    use super::*;

    pub(crate) fn state(generation: u64) -> anyhow::Result<SectionPacksState> {
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
                })
                .collect(),
            patches: Vec::new(),
            cutover: None,
        })
    }

    pub(crate) fn patch(number: u64, units: &[&str]) -> PackPatch {
        PackPatch {
            patch: number,
            gold_iceberg_snapshot_id: "999990000000000002".to_owned(),
            upserted: 1,
            deleted: 1,
            units: units.iter().map(|unit| (*unit).to_owned()).collect(),
        }
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
