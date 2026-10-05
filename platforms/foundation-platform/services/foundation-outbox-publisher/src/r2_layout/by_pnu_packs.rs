//! Keys of the by-PNU section packs, built and recognised from the R2 connection contract.
//!
//! - base pack   `{root}/{section}/g{generation}/{unit}{suffix}`
//! - patch pack  `{root}/{section}/g{generation}/p{patch}/{unit}{suffix}`
//!
//! `root` and the sections are the lane's `section_packs` block; the directory grammar, the unit
//! length (a legal dong, root ADR-0147 §1) and the suffix are `by_pnu_section_packs`. Every
//! recogniser round-trips through its builder.

use std::sync::OnceLock;

use anyhow::{ensure, Context};
use regex::Regex;

use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};

struct PackGrammar {
    generation_dir: Regex,
    patch_dir: Regex,
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

/// Refuses a unit that is not a legal dong code of the contract's length.
///
/// # Errors
/// Returns the violation.
pub(crate) fn check_unit(unit: &str) -> anyhow::Result<()> {
    let length = section_pack_policy()?.unit_prefix_length;
    ensure!(
        unit.len() == length && unit.bytes().all(|byte| byte.is_ascii_digit()),
        "{unit:?} is not a {length}-digit pack unit"
    );
    Ok(())
}

/// The pack unit (legal dong) of a PNU.
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

    #[test]
    fn keys_outside_the_grammar_are_refused() {
        for bad in [
            pack_key(ByPnuLane::Building, "zonings", 1, None, UNIT),
            pack_key(ByPnuLane::Building, "documents", 0, None, UNIT),
            pack_key(ByPnuLane::Building, "documents", 1, Some(0), UNIT),
            pack_key(ByPnuLane::Building, "documents", 1, None, "99999"),
            pack_key(ByPnuLane::Parcel, "documents", 1, None, UNIT),
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
