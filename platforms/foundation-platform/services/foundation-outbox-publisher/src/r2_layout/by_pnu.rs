//! Keys of both by-PNU serving lanes, built and recognised from the R2 connection contract.
//!
//! One grammar per lane block (root ADR-0096 parcels, ADR-0100 buildings, ADR-0141 patches):
//!
//! - base object   `{root}/v{generation}/{pnu}{suffix}`
//! - patch object  `{root}/v{generation}/p{patch}/{pnu}{suffix}` — a changed document or a
//!   tombstone of one patch generation over base `generation`
//! - manifest      `{manifest_object}` — the lane's one mutable object
//! - history       `{manifest_history_dir}/{published}-{sha256}.json` — every manifest a publish
//!   replaced, create-only, so a rollback names a stored record instead of a local file
//!
//! Every recogniser round-trips through its builder, so a key it accepts is one this module would
//! itself have produced.

use std::sync::OnceLock;

use regex::Regex;

use crate::by_pnu_gateway_contract::{ByPnuLane, ByPnuObjectKeyPolicy};

struct LaneGrammar {
    generation_dir: Regex,
    patch_dir: Regex,
    pnu: Regex,
}

fn anchored(pattern: &str) -> Result<Regex, String> {
    Regex::new(&format!("^(?:{pattern})$")).map_err(|error| error.to_string())
}

fn grammar(lane: ByPnuLane) -> anyhow::Result<&'static LaneGrammar> {
    static PARCEL: OnceLock<Result<LaneGrammar, String>> = OnceLock::new();
    static BUILDING: OnceLock<Result<LaneGrammar, String>> = OnceLock::new();
    let cell = match lane {
        ByPnuLane::Parcel => &PARCEL,
        ByPnuLane::Building => &BUILDING,
    };
    cell.get_or_init(|| {
        let layout = &lane.policy().map_err(|error| error.to_string())?.object_key;
        Ok(LaneGrammar {
            generation_dir: anchored(&layout.generation_dir_pattern)?,
            patch_dir: anchored(&layout.patch_dir_pattern)?,
            pnu: anchored(&layout.pnu_pattern)?,
        })
    })
    .as_ref()
    .map_err(|message| anyhow::anyhow!(message.clone()))
}

fn layout(lane: ByPnuLane) -> anyhow::Result<&'static ByPnuObjectKeyPolicy> {
    Ok(&lane.policy()?.object_key)
}

fn generation_dir(lane: ByPnuLane, generation: u64) -> anyhow::Result<String> {
    anyhow::ensure!(generation >= 1, "serving generation must be at least 1");
    let dir = format!("v{generation}");
    anyhow::ensure!(
        grammar(lane)?.generation_dir.is_match(&dir),
        "serving generation {generation} violates the R2 connection contract grammar"
    );
    Ok(dir)
}

fn patch_dir(lane: ByPnuLane, patch: u64) -> anyhow::Result<String> {
    anyhow::ensure!(patch >= 1, "serving patch generation must be at least 1");
    let dir = format!("p{patch}");
    anyhow::ensure!(
        grammar(lane)?.patch_dir.is_match(&dir),
        "serving patch generation {patch} violates the R2 connection contract grammar"
    );
    Ok(dir)
}

/// Refuses a PNU outside the contract grammar.
///
/// # Errors
/// Returns an error when the PNU violates the lane's PNU grammar.
pub(crate) fn check_pnu(lane: ByPnuLane, pnu: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        grammar(lane)?.pnu.is_match(pnu),
        "PNU {pnu:?} violates the R2 connection contract grammar"
    );
    Ok(())
}

/// `{root}/v{generation}/{pnu}{suffix}`.
///
/// # Errors
/// Returns an error when the generation or the PNU violates the contract grammar.
pub(crate) fn object_key(lane: ByPnuLane, generation: u64, pnu: &str) -> anyhow::Result<String> {
    let dir = generation_dir(lane, generation)?;
    check_pnu(lane, pnu)?;
    let layout = layout(lane)?;
    Ok(format!("{}/{dir}/{pnu}{}", layout.root, layout.suffix))
}

/// `{root}/v{generation}/`, trailing slash included.
///
/// # Errors
/// Returns an error when the generation violates the contract grammar.
pub(crate) fn generation_prefix(lane: ByPnuLane, generation: u64) -> anyhow::Result<String> {
    let dir = generation_dir(lane, generation)?;
    Ok(format!("{}/{dir}/", layout(lane)?.root))
}

/// `{root}/v{generation}/p{patch}/{pnu}{suffix}`.
///
/// # Errors
/// Returns an error when the generation, the patch or the PNU violates the contract grammar.
pub(crate) fn patch_object_key(
    lane: ByPnuLane,
    generation: u64,
    patch: u64,
    pnu: &str,
) -> anyhow::Result<String> {
    let prefix = patch_prefix(lane, generation, patch)?;
    check_pnu(lane, pnu)?;
    Ok(format!("{prefix}{pnu}{}", layout(lane)?.suffix))
}

/// `{root}/v{generation}/p{patch}/`, trailing slash included.
///
/// # Errors
/// Returns an error when the generation or the patch violates the contract grammar.
pub(crate) fn patch_prefix(lane: ByPnuLane, generation: u64, patch: u64) -> anyhow::Result<String> {
    let base = generation_prefix(lane, generation)?;
    Ok(format!("{base}{}/", patch_dir(lane, patch)?))
}

/// `{root}/v{generation}/p` — lists the patch directories of one base and nothing else: base
/// object file names start with a digit.
///
/// # Errors
/// Returns an error when the generation violates the contract grammar.
pub(crate) fn patch_directories_prefix(lane: ByPnuLane, generation: u64) -> anyhow::Result<String> {
    Ok(format!("{}p", generation_prefix(lane, generation)?))
}

/// The patch number a delimited listing's common prefix names, when it is exactly a prefix
/// [`patch_prefix`] builds for `generation`.
pub(crate) fn patch_of_prefix(lane: ByPnuLane, generation: u64, prefix: &str) -> Option<u64> {
    let base = generation_prefix(lane, generation).ok()?;
    prefix
        .strip_prefix(base.as_str())?
        .strip_prefix('p')?
        .strip_suffix('/')?
        .parse::<u64>()
        .ok()
        .filter(|patch| patch_prefix(lane, generation, *patch).is_ok_and(|built| built == prefix))
}

/// Whether `key` is a canonical base object key.
pub(crate) fn is_object_key(lane: ByPnuLane, key: &str) -> bool {
    let Some([generation_dir, pnu]) = relative_segments::<2>(lane, key) else {
        return false;
    };
    generation_dir
        .strip_prefix('v')
        .and_then(|digits| digits.parse::<u64>().ok())
        .is_some_and(|generation| {
            object_key(lane, generation, pnu).is_ok_and(|canonical| canonical == key)
        })
}

/// The `(generation, patch, pnu)` a canonical patch object key names.
pub(crate) fn parse_patch_object_key(lane: ByPnuLane, key: &str) -> Option<(u64, u64, String)> {
    let [generation_dir, patch_dir, pnu] = relative_segments::<3>(lane, key)?;
    let generation = generation_dir.strip_prefix('v')?.parse::<u64>().ok()?;
    let patch = patch_dir.strip_prefix('p')?.parse::<u64>().ok()?;
    patch_object_key(lane, generation, patch, pnu)
        .is_ok_and(|canonical| canonical == key)
        .then(|| (generation, patch, pnu.to_owned()))
}

/// Whether `key` is a canonical patch object key.
pub(crate) fn is_patch_object_key(lane: ByPnuLane, key: &str) -> bool {
    parse_patch_object_key(lane, key).is_some()
}

fn relative_segments<const N: usize>(lane: ByPnuLane, key: &str) -> Option<[&str; N]> {
    let layout = layout(lane).ok()?;
    let relative = key
        .strip_prefix(layout.root.as_str())?
        .strip_prefix('/')?
        .strip_suffix(layout.suffix.as_str())?;
    let segments = relative.split('/').collect::<Vec<_>>();
    segments.try_into().ok()
}

/// The lane's manifest key; it must live directly under the serving root so the gateway reads
/// it with the binding it reads the objects with.
///
/// # Errors
/// Returns an error when the contract places the manifest outside the serving root.
pub(crate) fn manifest_key(lane: ByPnuLane) -> anyhow::Result<&'static str> {
    let layout = layout(lane)?;
    anyhow::ensure!(
        directly_under(&layout.manifest_object, &layout.root),
        "the serving manifest {} must live directly under the serving root {}",
        layout.manifest_object,
        layout.root
    );
    Ok(layout.manifest_object.as_str())
}

/// Whether `key` is the lane's manifest key.
pub(crate) fn is_manifest_key(lane: ByPnuLane, key: &str) -> bool {
    manifest_key(lane).is_ok_and(|canonical| canonical == key)
}

/// `{manifest_history_dir}/{published}-{sha256}.json`: `published` is the replaced manifest's
/// `published_at_utc` in compact form (`20261003T101500Z`), `sha256` its exact bytes' digest.
///
/// # Errors
/// Returns an error when either part is not in its canonical form, or the contract places the
/// history directory outside the serving root.
pub(crate) fn manifest_history_key(
    lane: ByPnuLane,
    published_compact: &str,
    sha256: &str,
) -> anyhow::Result<String> {
    let layout = layout(lane)?;
    anyhow::ensure!(
        directly_under(&layout.manifest_history_dir, &layout.root),
        "the manifest history {} must live directly under the serving root {}",
        layout.manifest_history_dir,
        layout.root
    );
    anyhow::ensure!(
        is_compact_utc(published_compact),
        "{published_compact:?} is not a compact UTC time (YYYYMMDDTHHMMSSZ)"
    );
    anyhow::ensure!(
        sha256.len() == 64
            && sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{sha256:?} is not a lowercase sha256"
    );
    Ok(format!(
        "{}/{published_compact}-{sha256}.json",
        layout.manifest_history_dir
    ))
}

/// Whether `key` is a canonical manifest history key.
pub(crate) fn is_manifest_history_key(lane: ByPnuLane, key: &str) -> bool {
    let Ok(layout) = layout(lane) else {
        return false;
    };
    key.strip_prefix(layout.manifest_history_dir.as_str())
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_suffix(".json"))
        .and_then(|rest| rest.split_once('-'))
        .is_some_and(|(published, sha256)| {
            manifest_history_key(lane, published, sha256).is_ok_and(|canonical| canonical == key)
        })
}

fn directly_under(path: &str, root: &str) -> bool {
    path.strip_prefix(root)
        .and_then(|relative| relative.strip_prefix('/'))
        .is_some_and(|name| !name.is_empty() && !name.contains('/'))
}

fn is_compact_utc(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 16
        && bytes[8] == b'T'
        && bytes[15] == b'Z'
        && bytes[..8].iter().all(u8::is_ascii_digit)
        && bytes[9..15].iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNU: &str = "9999900000100000000";
    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn a_base_key_pins_generation_directory_and_pnu_grammar() -> anyhow::Result<()> {
        for (lane, root) in [
            (ByPnuLane::Parcel, "serving/parcels/by-pnu"),
            (ByPnuLane::Building, "serving/buildings/by-pnu"),
        ] {
            assert_eq!(object_key(lane, 1, PNU)?, format!("{root}/v1/{PNU}.json"));
            assert_eq!(manifest_key(lane)?, format!("{root}/manifest.json"));
            assert!(object_key(lane, 0, PNU).is_err());
            for invalid_pnu in [
                "999990000010000000",   // 18 digits
                "99999000001000000001", // 20 digits
                "999990000010000000a",  // non-digit
                "9999900000000000000",  // 11th digit outside the cadastral-register kinds [1289]
                "",
            ] {
                assert!(
                    object_key(lane, 1, invalid_pnu).is_err(),
                    "PNU {invalid_pnu:?} must be refused"
                );
            }
            let prefix = generation_prefix(lane, 7)?;
            assert_eq!(prefix, format!("{root}/v7/"));
            assert!(object_key(lane, 7, PNU)?.starts_with(&prefix));
            assert!(!object_key(lane, 8, PNU)?.starts_with(&prefix));
            assert!(!manifest_key(lane)?.starts_with(prefix.as_str()));
            assert!(generation_prefix(lane, 0).is_err());
        }
        Ok(())
    }

    #[test]
    fn only_a_canonical_base_key_is_recognised_as_one() -> anyhow::Result<()> {
        let lane = ByPnuLane::Parcel;
        let key = object_key(lane, 7, PNU)?;
        assert!(is_object_key(lane, &key));
        assert!(!is_object_key(ByPnuLane::Building, &key));
        for other in [
            "serving/parcels/by-pnu/manifest.json",
            "serving/parcels/by-pnu/v01/9999900000100000000.json",
            "serving/parcels/by-pnu/v0/9999900000100000000.json",
            "serving/parcels/by-pnu/v1/999990000010000000.json",
            "serving/parcels/by-pnu/v1/nested/9999900000100000000.json",
            "serving/parcels/by-pnu/v1/9999900000100000000.json.bak",
            "serving/parcels/by-pnu/9999900000100000000.json",
            "serving/other/v1/9999900000100000000.json",
            "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json",
        ] {
            assert!(!is_object_key(lane, other), "{other}");
        }
        assert!(is_manifest_key(
            lane,
            "serving/parcels/by-pnu/manifest.json"
        ));
        assert!(!is_manifest_key(lane, &key));
        Ok(())
    }

    #[test]
    fn a_patch_key_sits_inside_its_base_and_is_not_a_base_key() -> anyhow::Result<()> {
        for lane in [ByPnuLane::Parcel, ByPnuLane::Building] {
            let key = patch_object_key(lane, 3, 2, PNU)?;
            assert!(key.starts_with(&generation_prefix(lane, 3)?));
            assert!(key.starts_with(&patch_prefix(lane, 3, 2)?));
            assert!(key.starts_with(&patch_directories_prefix(lane, 3)?));
            assert!(is_patch_object_key(lane, &key));
            assert!(
                !is_object_key(lane, &key),
                "a patch key passed as a base key"
            );
            assert!(!is_patch_object_key(lane, &object_key(lane, 3, PNU)?));
            assert_eq!(
                parse_patch_object_key(lane, &key),
                Some((3, 2, PNU.to_owned()))
            );
            // Base object file names start with a digit, so the patch-directory prefix holds
            // no base object.
            assert!(!object_key(lane, 3, PNU)?.starts_with(&patch_directories_prefix(lane, 3)?));
        }
        Ok(())
    }

    #[test]
    fn non_canonical_patch_keys_are_refused() -> anyhow::Result<()> {
        let lane = ByPnuLane::Parcel;
        assert!(patch_object_key(lane, 3, 0, PNU).is_err());
        for other in [
            "serving/parcels/by-pnu/v3/p02/9999900000100000000.json",
            "serving/parcels/by-pnu/v3/p0/9999900000100000000.json",
            "serving/parcels/by-pnu/v3/q2/9999900000100000000.json",
            "serving/parcels/by-pnu/v3/p2/x/9999900000100000000.json",
            "serving/buildings/by-pnu/v3/p2/9999900000100000000.json",
        ] {
            assert!(!is_patch_object_key(lane, other), "{other}");
        }
        assert_eq!(
            patch_of_prefix(lane, 3, "serving/parcels/by-pnu/v3/p12/"),
            Some(12)
        );
        assert_eq!(
            patch_of_prefix(lane, 3, "serving/parcels/by-pnu/v3/p012/"),
            None
        );
        assert_eq!(
            patch_of_prefix(lane, 4, "serving/parcels/by-pnu/v3/p1/"),
            None
        );
        Ok(())
    }

    #[test]
    fn history_keys_are_content_addressed_and_canonical() -> anyhow::Result<()> {
        let lane = ByPnuLane::Building;
        let key = manifest_history_key(lane, "20261003T101500Z", SHA)?;
        assert_eq!(
            key,
            format!("serving/buildings/by-pnu/manifest-history/20261003T101500Z-{SHA}.json")
        );
        assert!(is_manifest_history_key(lane, &key));
        assert!(!is_manifest_history_key(ByPnuLane::Parcel, &key));
        assert!(!is_object_key(lane, &key) && !is_patch_object_key(lane, &key));
        assert!(manifest_history_key(lane, "2026-10-03T10:15:00Z", SHA).is_err());
        assert!(manifest_history_key(lane, "20261003T101500Z", &SHA.to_uppercase()).is_err());
        Ok(())
    }
}
