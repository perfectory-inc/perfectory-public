//! Building lane key construction and recognition from the R2 contract.
use crate::building_by_pnu_gateway_contract::building_by_pnu_gateway_policy;
use regex::Regex;
use std::sync::OnceLock;

/// Returns the canonical serving key of one building's by-PNU JSON object (root ADR-0100).
///
/// The root, the generation-directory grammar, and the PNU grammar all come from the
/// `building_by_pnu_gateway` block of the R2 connection contract, which the
/// `foundation-building-gateway` Worker also reads: the key this writes and the key the Worker
/// resolves are the same fact.
///
/// # Errors
/// Returns an error when the generation is zero, the generation violates the contract grammar,
/// or the PNU violates the contract grammar.
pub fn building_by_pnu_serving_object_key(generation: u64, pnu: &str) -> anyhow::Result<String> {
    anyhow::ensure!(generation >= 1, "serving generation must be at least 1");
    let layout = &building_by_pnu_gateway_policy()?.object_key;
    let generation_dir = format!("v{generation}");
    anyhow::ensure!(
        building_generation_dir_regex()?.is_match(&generation_dir),
        "serving generation {generation} violates the R2 connection contract grammar"
    );
    anyhow::ensure!(
        building_pnu_regex()?.is_match(pnu),
        "PNU {pnu:?} violates the R2 connection contract grammar"
    );
    Ok(format!(
        "{}/{generation_dir}/{pnu}{}",
        layout.root, layout.suffix
    ))
}

/// Returns the directory prefix (trailing slash included) of one serving generation.
///
/// A resumed bake lists this prefix to learn what the bucket already holds. Derived from the
/// same contract layout as [`building_by_pnu_serving_object_key`], so every key that builder
/// produces for `generation` starts with this prefix and nothing outside the generation does.
///
/// # Errors
/// Returns an error when the generation is zero or violates the contract grammar.
pub fn building_by_pnu_serving_generation_prefix(generation: u64) -> anyhow::Result<String> {
    anyhow::ensure!(generation >= 1, "serving generation must be at least 1");
    let layout = &building_by_pnu_gateway_policy()?.object_key;
    let generation_dir = format!("v{generation}");
    anyhow::ensure!(
        building_generation_dir_regex()?.is_match(&generation_dir),
        "serving generation {generation} violates the R2 connection contract grammar"
    );
    Ok(format!("{}/{generation_dir}/", layout.root))
}

/// Returns whether `key` is the canonical serving key of one building's by-PNU JSON object.
///
/// Derived by round-tripping through [`building_by_pnu_serving_object_key`], so a key this accepts
/// is one this module would itself have produced — a leading-zero generation directory or a
/// non-canonical PNU cannot pass.
pub fn is_building_by_pnu_serving_object_key(key: &str) -> bool {
    let Ok(policy) = building_by_pnu_gateway_policy() else {
        return false;
    };
    let layout = &policy.object_key;
    let Some(relative) = key
        .strip_prefix(layout.root.as_str())
        .and_then(|relative| relative.strip_prefix('/'))
        .and_then(|relative| relative.strip_suffix(layout.suffix.as_str()))
    else {
        return false;
    };
    let mut segments = relative.split('/');
    let (Some(generation_dir), Some(pnu), None) =
        (segments.next(), segments.next(), segments.next())
    else {
        return false;
    };
    generation_dir
        .strip_prefix('v')
        .and_then(|digits| digits.parse::<u64>().ok())
        .is_some_and(|generation| {
            building_by_pnu_serving_object_key(generation, pnu)
                .is_ok_and(|canonical| canonical == key)
        })
}

/// Returns the canonical key of the building by-PNU serving manifest (root ADR-0100).
///
/// This is the one mutable object of the lane: the pointer that pins the currently served
/// generation. Its address comes from the contract and must live under the serving root, so the
/// gateway can read the pointer with the same binding it reads the objects with.
///
/// # Errors
/// Returns an error when the contract places the manifest outside the serving root.
pub fn building_by_pnu_serving_manifest_key() -> anyhow::Result<&'static str> {
    let layout = &building_by_pnu_gateway_policy()?.object_key;
    anyhow::ensure!(
        layout
            .manifest_object
            .strip_prefix(layout.root.as_str())
            .and_then(|relative| relative.strip_prefix('/'))
            .is_some_and(|file_name| !file_name.contains('/')),
        "the serving manifest {} must live directly under the serving root {}",
        layout.manifest_object,
        layout.root
    );
    Ok(layout.manifest_object.as_str())
}

/// Returns whether `key` is the canonical building by-PNU serving manifest key.
pub fn is_building_by_pnu_serving_manifest_key(key: &str) -> bool {
    building_by_pnu_serving_manifest_key().is_ok_and(|canonical| canonical == key)
}

fn building_generation_dir_regex() -> anyhow::Result<&'static Regex> {
    static GENERATION_DIR_REGEX: OnceLock<Result<Regex, String>> = OnceLock::new();
    GENERATION_DIR_REGEX
        .get_or_init(|| {
            let pattern = &building_by_pnu_gateway_policy()
                .map_err(|error| error.to_string())?
                .object_key
                .generation_dir_pattern;
            Regex::new(&format!("^(?:{pattern})$")).map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}

fn building_pnu_regex() -> anyhow::Result<&'static Regex> {
    static PNU_REGEX: OnceLock<Result<Regex, String>> = OnceLock::new();
    PNU_REGEX
        .get_or_init(|| {
            let pattern = &building_by_pnu_gateway_policy()
                .map_err(|error| error.to_string())?
                .object_key
                .pnu_pattern;
            Regex::new(&format!("^(?:{pattern})$")).map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}
