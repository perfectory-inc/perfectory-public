//! Keys of the by-PNU section packs, built and recognised from the R2 connection contract.
//!
//! - base pack   `{root}/{section}/g{generation}/{unit}{suffix}`
//! - patch pack  `{root}/{section}/g{generation}/p{patch}/{unit}{suffix}`
//!
//! `root` and the sections are the lane's `section_packs` block; the directory grammar, the unit
//! length (a legal dong, root ADR-0147 §1) and the suffix are `by_pnu_section_packs`. Every
//! recogniser round-trips through its builder.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{ensure, Context};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};

struct PackGrammar {
    generation_dir: Regex,
    patch_dir: Regex,
    unit: Regex,
}

fn grammar() -> anyhow::Result<&'static PackGrammar> {
    static GRAMMAR: OnceLock<Result<PackGrammar, String>> = OnceLock::new();
    GRAMMAR
        .get_or_init(|| {
            let policy = section_pack_policy().map_err(|error| error.to_string())?;
            let anchored = |pattern: &str| {
                Regex::new(&format!("^(?:{pattern})$")).map_err(|error| error.to_string())
            };
            Ok(PackGrammar {
                generation_dir: anchored(&policy.generation_dir_pattern)?,
                patch_dir: anchored(&policy.patch_dir_pattern)?,
                unit: anchored(&policy.parts.unit_pattern)?,
            })
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}

fn check_section(lane: ByPnuLane, section: &str) -> anyhow::Result<()> {
    ensure!(
        lane.section_packs()?
            .sections
            .iter()
            .any(|known| known == section),
        "{section:?} is not a {} section of the contract",
        lane.noun()
    );
    Ok(())
}

/// Refuses a unit that is neither a legal dong code of the contract's length nor one part of one
/// (`{dong}-{part}`, root ADR-0163; the contract's `parts.unit_pattern`).
///
/// # Errors
/// Returns the violation.
pub(crate) fn check_unit(unit: &str) -> anyhow::Result<()> {
    let length = section_pack_policy()?.unit_prefix_length;
    ensure!(
        dong_of(unit).len() == length && grammar()?.unit.is_match(unit),
        "{unit:?} is not a {length}-digit pack unit or a part of one"
    );
    Ok(())
}

/// The legal dong of a PNU: what a bake groups by and a parts index counts.
///
/// # Errors
/// Refuses a PNU shorter than the unit.
pub(crate) fn unit_of(pnu: &str) -> anyhow::Result<&str> {
    let length = section_pack_policy()?.unit_prefix_length;
    let unit = pnu
        .get(..length)
        .with_context(|| format!("PNU {pnu:?} is shorter than a pack unit"))?;
    check_unit(unit)?;
    Ok(unit)
}

/// The legal dong a unit belongs to (itself, or the dong a part is cut from).
pub(crate) fn dong_of(unit: &str) -> &str {
    unit.split_once('-').map_or(unit, |(dong, _)| dong)
}

/// The contract's part hash (`parts.hash_definition`): FNV-1a, 32 bit, over the PNU's ASCII bytes.
/// The Worker computes the same number (`packs.ts`); both are held to `parts.hash_test_vectors`.
pub(crate) fn fnv1a32(input: &str) -> u32 {
    input.bytes().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

/// The unit of part `part` of `dong`, or the dong itself when it has one part.
///
/// # Errors
/// Refuses a part outside `0..parts`.
pub(crate) fn part_unit(dong: &str, part: u32, parts: u32) -> anyhow::Result<String> {
    ensure!(
        part < parts.max(1),
        "part {part} of {dong} is not one of its {parts} parts"
    );
    let unit = if parts <= 1 {
        dong.to_owned()
    } else {
        format!("{dong}-{part}")
    };
    check_unit(&unit)?;
    Ok(unit)
}

/// The part counts of one section generation's dongs (root ADR-0163): the dongs cut into more
/// than one part, each with its count. A dong it does not name has one part.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Parts {
    counts: BTreeMap<String, u32>,
}

impl Parts {
    /// Parts from per-dong counts; a count of 0 or 1 is one part and is not kept.
    ///
    /// # Errors
    /// Refuses a key that is not a legal dong code.
    pub(crate) fn new(counts: BTreeMap<String, u32>) -> anyhow::Result<Self> {
        let mut kept = BTreeMap::new();
        for (dong, count) in counts {
            ensure!(
                !dong.contains('-'),
                "{dong:?} is a part, not a dong a parts index counts"
            );
            check_unit(&dong)?;
            if count > 1 {
                kept.insert(dong, count);
            }
        }
        Ok(Self { counts: kept })
    }

    /// How many parts `dong` is cut into.
    pub(crate) fn count(&self, dong: &str) -> u32 {
        self.counts.get(dong).copied().unwrap_or(1)
    }

    /// The dongs cut into more than one part, with their counts.
    pub(crate) fn counts(&self) -> &BTreeMap<String, u32> {
        &self.counts
    }

    /// The unit that holds `pnu`.
    ///
    /// # Errors
    /// Refuses a PNU shorter than a dong.
    pub(crate) fn unit_of(&self, pnu: &str) -> anyhow::Result<String> {
        let dong = unit_of(pnu)?;
        let count = self.count(dong);
        part_unit(dong, fnv1a32(pnu) % count, count)
    }

    /// Every unit of `dong`: the dong itself, or each of its parts.
    ///
    /// # Errors
    /// Refuses a dong that is not a unit.
    pub(crate) fn units_of_dong(&self, dong: &str) -> anyhow::Result<Vec<String>> {
        let count = self.count(dong);
        (0..count)
            .map(|part| part_unit(dong, part, count))
            .collect()
    }
}

/// The parts index object of one section generation, as stored (`parts.index_file_name`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PartsIndexFile {
    pub(crate) schema_version: String,
    pub(crate) unit: String,
    pub(crate) section: String,
    pub(crate) generation: u64,
    pub(crate) hash: String,
    pub(crate) parts: BTreeMap<String, u32>,
}

impl PartsIndexFile {
    /// The index of `parts` for one section generation of `lane`.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub(crate) fn new(
        lane: ByPnuLane,
        section: &str,
        generation: u64,
        parts: &Parts,
    ) -> anyhow::Result<Self> {
        let policy = &section_pack_policy()?.parts;
        Ok(Self {
            schema_version: policy.index_schema_version.clone(),
            unit: lane.unit().to_owned(),
            section: section.to_owned(),
            generation,
            hash: policy.hash.clone(),
            parts: parts.counts().clone(),
        })
    }

    /// The parts it names, after checking it is the index of `section` generation `generation`.
    ///
    /// # Errors
    /// Refuses another schema, lane, section, generation or hash, and counts below 2.
    pub(crate) fn parts_of(
        &self,
        lane: ByPnuLane,
        section: &str,
        generation: u64,
    ) -> anyhow::Result<Parts> {
        let policy = &section_pack_policy()?.parts;
        ensure!(
            self.schema_version == policy.index_schema_version
                && self.unit == lane.unit()
                && self.section == section
                && self.generation == generation
                && self.hash == policy.hash,
            "the parts index is of {} {} generation {} ({}, {}), not {} {section} generation \
             {generation}",
            self.unit,
            self.section,
            self.generation,
            self.schema_version,
            self.hash,
            lane.unit()
        );
        ensure!(
            self.parts.values().all(|count| *count >= 2),
            "a parts index names only dongs of two or more parts"
        );
        Parts::new(self.parts.clone())
    }
}

/// The key of a section generation's parts index.
///
/// # Errors
/// Returns an error when the section or generation violates the contract.
pub(crate) fn parts_index_key(
    lane: ByPnuLane,
    section: &str,
    generation: u64,
) -> anyhow::Result<String> {
    Ok(format!(
        "{}{}",
        directory(lane, section, generation, None)?,
        section_pack_policy()?.parts.index_file_name
    ))
}

/// `{root}/{section}/g{generation}/`, or with `p{patch}/` when the pack is a patch.
///
/// # Errors
/// Returns an error when the section, generation or patch violates the contract.
pub(crate) fn directory(
    lane: ByPnuLane,
    section: &str,
    generation: u64,
    patch: Option<u64>,
) -> anyhow::Result<String> {
    check_section(lane, section)?;
    let grammar = grammar()?;
    let generation_dir = format!("g{generation}");
    ensure!(
        generation >= 1 && grammar.generation_dir.is_match(&generation_dir),
        "pack generation {generation} violates the contract grammar"
    );
    let root = &lane.section_packs()?.root;
    let mut directory = format!("{root}/{section}/{generation_dir}/");
    if let Some(patch) = patch {
        let patch_dir = format!("p{patch}");
        ensure!(
            patch >= 1 && grammar.patch_dir.is_match(&patch_dir),
            "pack patch {patch} violates the contract grammar"
        );
        directory.push_str(&patch_dir);
        directory.push('/');
    }
    Ok(directory)
}

/// The key of one pack.
///
/// # Errors
/// Returns an error when any part violates the contract.
pub(crate) fn pack_key(
    lane: ByPnuLane,
    section: &str,
    generation: u64,
    patch: Option<u64>,
    unit: &str,
) -> anyhow::Result<String> {
    check_unit(unit)?;
    Ok(format!(
        "{}{unit}{}",
        directory(lane, section, generation, patch)?,
        section_pack_policy()?.suffix
    ))
}

/// A recognised pack key: section, generation, patch and unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackKey {
    pub(crate) section: String,
    pub(crate) generation: u64,
    pub(crate) patch: Option<u64>,
    pub(crate) unit: String,
}

/// Recognises a pack key of `lane`; `None` for anything this module would not have built.
pub(crate) fn parse_pack_key(lane: ByPnuLane, key: &str) -> Option<PackKey> {
    let root = &lane.section_packs().ok()?.root;
    let suffix = &section_pack_policy().ok()?.suffix;
    let rest = key.strip_prefix(root.as_str())?.strip_prefix('/')?;
    let parts = rest.split('/').collect::<Vec<_>>();
    let (section, generation, patch, file) = match parts.as_slice() {
        [section, generation, file] => (*section, *generation, None, *file),
        [section, generation, patch, file] => (*section, *generation, Some(*patch), *file),
        _ => return None,
    };
    let generation = generation.strip_prefix('g')?.parse::<u64>().ok()?;
    let patch = match patch {
        Some(patch) => Some(patch.strip_prefix('p')?.parse::<u64>().ok()?),
        None => None,
    };
    let unit = file.strip_suffix(suffix.as_str())?;
    let parsed = PackKey {
        section: section.to_owned(),
        generation,
        patch,
        unit: unit.to_owned(),
    };
    (pack_key(lane, section, generation, patch, unit).ok()? == key).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const UNIT: &str = "9999900000";

    #[test]
    fn a_pack_key_round_trips_and_names_the_contract_root() -> anyhow::Result<()> {
        let base = pack_key(ByPnuLane::Building, "documents", 3, None, UNIT)?;
        assert_eq!(
            base,
            format!("serving/buildings/packs/documents/g3/{UNIT}.pack")
        );
        let patch = pack_key(ByPnuLane::Building, "documents", 3, Some(2), UNIT)?;
        assert_eq!(
            patch,
            format!("serving/buildings/packs/documents/g3/p2/{UNIT}.pack")
        );
        assert_eq!(
            parse_pack_key(ByPnuLane::Building, &patch),
            Some(PackKey {
                section: "documents".to_owned(),
                generation: 3,
                patch: Some(2),
                unit: UNIT.to_owned()
            })
        );
        assert_eq!(unit_of("9999900000100000000")?, UNIT);
        Ok(())
    }

    /// The part hash is the contract's: every test vector it names (the Worker is held to the same).
    #[test]
    fn the_part_hash_is_the_contracts() -> anyhow::Result<()> {
        let policy = &section_pack_policy()?.parts;
        assert_eq!(policy.hash, "fnv1a32");
        assert!(!policy.hash_test_vectors.is_empty());
        for vector in &policy.hash_test_vectors {
            assert_eq!(fnv1a32(&vector.input), vector.fnv1a32, "{}", vector.input);
        }
        Ok(())
    }

    /// A unit is a dong or one part of one, and a parted dong's units cover each PNU exactly once.
    #[test]
    fn a_parted_dong_names_each_pnu_one_unit() -> anyhow::Result<()> {
        for good in [UNIT, "9999900000-0", "9999900000-17", "9999900000-9999"] {
            check_unit(good)?;
        }
        for bad in [
            "9999900000-",
            "999990000-1",
            "9999900000-10000",
            "99999000001",
            "9999900000-a",
        ] {
            assert!(check_unit(bad).is_err(), "{bad}");
        }
        let parts = Parts::new(BTreeMap::from([
            (UNIT.to_owned(), 5),
            ("9999900001".to_owned(), 1),
        ]))?;
        assert_eq!(parts.counts().len(), 1, "a one-part dong is not kept");
        let units = parts.units_of_dong(UNIT)?;
        assert_eq!(units.len(), 5);
        assert_eq!(
            parts.units_of_dong("9999900001")?,
            vec!["9999900001".to_owned()]
        );
        let mut hit = BTreeSet::new();
        for n in 0..500_u32 {
            let pnu = format!("{UNIT}1{n:04}0000");
            let unit = parts.unit_of(&pnu)?;
            assert!(units.contains(&unit), "{unit}");
            assert_eq!(dong_of(&unit), UNIT);
            hit.insert(unit);
        }
        assert_eq!(hit.len(), 5, "500 PNUs reach every part");
        assert!(Parts::new(BTreeMap::from([("9999900000-1".to_owned(), 3)])).is_err());
        Ok(())
    }

    /// Each lane's packs live under its own root, and one lane never reads the other's keys.
    #[test]
    fn each_lane_keys_its_packs_under_its_own_root() -> anyhow::Result<()> {
        let parcel = pack_key(ByPnuLane::Parcel, "documents", 1, None, UNIT)?;
        let building = pack_key(ByPnuLane::Building, "documents", 1, None, UNIT)?;
        assert!(parcel.starts_with(&format!("{}/", ByPnuLane::Parcel.section_packs()?.root)));
        assert_ne!(parcel, building);
        assert!(parse_pack_key(ByPnuLane::Parcel, &parcel).is_some());
        assert_eq!(parse_pack_key(ByPnuLane::Building, &parcel), None);
        assert_eq!(parse_pack_key(ByPnuLane::Parcel, &building), None);
        Ok(())
    }

    #[test]
    fn keys_outside_the_grammar_are_refused() {
        for bad in [
            pack_key(ByPnuLane::Building, "zonings", 1, None, UNIT),
            pack_key(ByPnuLane::Building, "documents", 0, None, UNIT),
            pack_key(ByPnuLane::Building, "documents", 1, Some(0), UNIT),
            pack_key(ByPnuLane::Building, "documents", 1, None, "99999"),
            pack_key(ByPnuLane::Parcel, "zonings", 1, None, UNIT),
        ] {
            assert!(bad.is_err(), "{bad:?}");
        }
        for key in [
            "serving/buildings/packs/documents/g01/9999900000.pack",
            "serving/buildings/packs/documents/g1/9999900000.json",
            "serving/buildings/packs/documents/g1/x/9999900000.pack",
            "serving/buildings/by-pnu/v1/9999900000100000000.json",
        ] {
            assert_eq!(parse_pack_key(ByPnuLane::Building, key), None, "{key}");
        }
    }
}
