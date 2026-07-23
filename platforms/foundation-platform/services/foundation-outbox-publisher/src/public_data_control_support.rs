use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value as JsonValue;

pub fn read_json(path: &Path, label: &str) -> anyhow::Result<JsonValue> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))?;
    serde_json::from_slice(strip_utf8_bom(&bytes))
        .with_context(|| format!("failed to parse {label} {}", path.display()))
}

pub fn write_json_file<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value).context("failed to serialize JSON report")?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

pub fn env_path(name: &str, default: &str) -> anyhow::Result<PathBuf> {
    let value = match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw,
        Ok(_) | Err(env::VarError::NotPresent) => default.to_owned(),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    };
    Ok(PathBuf::from(value))
}

/// Reads an optional environment variable, returning the trimmed value or `None` when the variable
/// is unset or blank (whitespace-only). This is the single shared env reader for the crate; every
/// other typed helper below funnels through it so trimming/blank/error semantics stay identical.
///
/// # Errors
/// Bails when the variable is present but not valid Unicode.
pub fn optional_env_value(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

/// Reads a required environment variable (trimmed). Bails when unset or blank.
///
/// # Errors
/// Bails when the variable is unset/blank, or present but not valid Unicode.
pub fn required_env_value(name: &str) -> anyhow::Result<String> {
    optional_env_value(name)?.with_context(|| format!("{name} is required"))
}

/// Reads an optional unsigned 64-bit env value (trimmed). `None` when unset/blank.
///
/// # Errors
/// Bails when the value is not a valid `u64`.
pub fn optional_u64_env(name: &str) -> anyhow::Result<Option<u64>> {
    optional_env_value(name)?
        .map(|raw| raw.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

/// Reads an optional unsigned 32-bit env value (trimmed). `None` when unset/blank.
///
/// # Errors
/// Bails when the value is not a valid `u32`.
pub fn optional_u32_env(name: &str) -> anyhow::Result<Option<u32>> {
    optional_env_value(name)?
        .map(|raw| raw.parse::<u32>())
        .transpose()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

/// Reads an optional unsigned pointer-sized env value (trimmed). `None` when unset/blank.
///
/// # Errors
/// Bails when the value is not a valid `usize`.
pub fn optional_usize_env(name: &str) -> anyhow::Result<Option<usize>> {
    optional_env_value(name)?
        .map(|raw| raw.parse::<usize>())
        .transpose()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

/// Reads an optional, strictly-positive `u32` env value (trimmed). `None` when unset/blank.
///
/// # Errors
/// Bails when the value is not a valid `u32` or is zero.
pub fn optional_positive_u32_env(name: &str) -> anyhow::Result<Option<u32>> {
    match optional_u32_env(name)? {
        Some(0) => bail!("{name} must be greater than zero"),
        value => Ok(value),
    }
}

/// Reads an optional boolean env value (trimmed). Accepts `1`/`true`/`TRUE` and `0`/`false`/`FALSE`.
/// `None` when unset/blank.
///
/// # Errors
/// Bails when the value is not one of the accepted tokens.
pub fn optional_bool_env(name: &str) -> anyhow::Result<Option<bool>> {
    optional_env_value(name)?
        .map(|raw| match raw.as_str() {
            "1" | "true" | "TRUE" => Ok(true),
            "0" | "false" | "FALSE" => Ok(false),
            _ => bail!("{name} must be one of 1, 0, true, false"),
        })
        .transpose()
}

/// Reads an optional duration-in-seconds env value (a `u64` count of seconds). `None` when
/// unset/blank.
///
/// # Errors
/// Bails when the value is not a valid `u64`.
pub fn optional_duration_seconds_env(name: &str) -> anyhow::Result<Option<Duration>> {
    optional_u64_env(name).map(|value| value.map(Duration::from_secs))
}

/// Reads an optional duration-in-milliseconds env value (a `u64` count of millis). `None` when
/// unset/blank.
///
/// # Errors
/// Bails when the value is not a valid `u64`.
pub fn optional_duration_millis_env(name: &str) -> anyhow::Result<Option<Duration>> {
    optional_u64_env(name).map(|value| value.map(Duration::from_millis))
}

/// Reads an optional comma-separated env value into a vector of trimmed, non-empty parts. `None`
/// when the variable itself is unset/blank.
///
/// # Errors
/// Bails when the variable is present but not valid Unicode.
pub fn optional_csv_env(name: &str) -> anyhow::Result<Option<Vec<String>>> {
    optional_env_value(name).map(|value| {
        value.map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
    })
}

/// Validates a `*_SOURCE_SLUG` env override against the canonical generator slug, failing fast at
/// config-build time (before any download) when the override is wrong.
///
/// `expected_dataset_slug` is the canonical semantic dataset identity for this producer/run; the
/// canonical slug is `collection_domain::source_slug(provider, expected_dataset_slug)`. Behavior:
/// - `override_value == None` -> returns the generated canonical slug (generator owns the value).
/// - `override_value == Some(v)` and `v == expected` -> returns `v` (override is redundant but
///   harmless).
/// - `override_value == Some(v)` and `v != expected` -> hard error naming the env, the bad value,
///   and the canonical value, so an old/mismatched override can never silently reach the write path.
///
/// This is the early (config-time) companion to the Bronze write-boundary backstop
/// (`assert_canonical_source_slug` inside `build_bronze_object_key`).
///
/// # Errors
/// - the `(provider, expected_dataset_slug)` pair is not a valid canonical slug input (see
///   [`collection_domain::source_slug`]);
/// - the override is present and does not equal the canonical slug.
pub fn resolve_canonical_source_slug(
    env_name: &str,
    override_value: Option<String>,
    provider: &str,
    expected_dataset_slug: &str,
) -> anyhow::Result<String> {
    let expected = collection_domain::source_slug(provider, expected_dataset_slug)?;
    match override_value {
        Some(value) if value == expected => Ok(value),
        Some(value) => bail!(
            "{env_name}={value} is not the canonical source_slug for this dataset; expected \
             {expected}. Unset it to let the generator derive it, or set it correctly."
        ),
        None => Ok(expected),
    }
}

/// Name of the shared opt-in force-refetch flag, read by [`bronze_force_refetch_enabled`].
pub const BRONZE_FORCE_REFETCH_ENV: &str = "FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH";

/// Whether the operator has opted in to forcing a re-download of bulk-file Bronze objects whose
/// `source_partition_key` (request fingerprint, which includes `provider_file_id`) already exists.
///
/// Default off (returns `false` when unset/blank), preserving the request-fingerprint pre-download
/// skip. When set, bulk-file ingest BYPASSES that skip and re-downloads, so the post-download SHA256
/// content check runs again — the policy's "full or hash-verified snapshot is the correctness
/// baseline" (see `docs/catalog/source-change-detection-policy.md`). This is the escape hatch for
/// the rare case a provider reuses a file id with changed bytes (see the
/// `provider_file_id` content-stability assumption in that policy).
///
/// # Errors
/// Bails when the variable is present but is not one of the accepted boolean tokens.
pub fn bronze_force_refetch_enabled() -> anyhow::Result<bool> {
    Ok(optional_bool_env(BRONZE_FORCE_REFETCH_ENV)?.unwrap_or(false))
}

pub fn resolve_repo_path(root: &Path, path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{label} must not contain parent directory segments");
    }
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    if !path_is_within_root(root, &resolved) {
        bail!("{label} must stay within repo root");
    }
    Ok(resolved)
}

pub fn resolve_cargo_exe(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(path) = env::var_os("PATH") {
        for dir in env::split_paths(&path) {
            for candidate in ["cargo.exe", "cargo"] {
                let path = dir.join(candidate);
                if path.is_file() {
                    return Ok(path);
                }
            }
        }
    }
    for profile_root in [env::var_os("USERPROFILE"), env::var_os("HOME")]
        .into_iter()
        .flatten()
    {
        for candidate in ["cargo.exe", "cargo"] {
            let path = PathBuf::from(&profile_root)
                .join(".cargo")
                .join("bin")
                .join(candidate);
            if path.is_file() {
                return Ok(path);
            }
        }
    }
    Ok(PathBuf::from("cargo"))
}

fn path_is_within_root(root: &Path, candidate: &Path) -> bool {
    let normalized_root = normalize_path_for_prefix(root);
    let normalized_candidate = normalize_path_for_prefix(candidate);
    normalized_candidate == normalized_root
        || normalized_candidate
            .strip_prefix(normalized_root.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn normalize_path_for_prefix(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let normalized = normalized
        .strip_prefix("//?/")
        .unwrap_or(normalized.as_str());
    if cfg!(windows) {
        normalized.trim_end_matches('/').to_ascii_lowercase()
    } else {
        normalized.trim_end_matches('/').to_owned()
    }
}

pub fn repo_relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

pub fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// GitHub Actions secrets that must be configured before a dispatch in the given mode is allowed to
/// run, keyed by dispatch mode. This is the single source of truth shared by the cutover dispatcher
/// and the secret configurator. `SupplyChainReleaseGates` intentionally requires none.
pub const REQUIRED_SECRETS: &[(&str, &[&str])] = &[
    (
        "FOUNDATION_PLATFORM_DATABASE_URL",
        &["ProductionOrchestrator"],
    ),
    (
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
        &["ProductionOrchestrator"],
    ),
    (
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
        &["ProductionOrchestrator"],
    ),
    (
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
        &["ProductionOrchestrator"],
    ),
    ("R2_BUCKET_NAME", &["ProductionOrchestrator"]),
    ("R2_ENDPOINT", &["ProductionOrchestrator"]),
    ("R2_ACCESS_KEY_ID", &["ProductionOrchestrator"]),
    ("R2_SECRET_ACCESS_KEY", &["ProductionOrchestrator"]),
    (
        "FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET",
        &["ConsumerReceiverE2E"],
    ),
];

/// Resolve the secrets required for a dispatch `mode` from [`REQUIRED_SECRETS`]. `mode == "All"`
/// returns every managed secret name.
pub fn required_secrets_for_mode(mode: &str) -> Vec<&'static str> {
    REQUIRED_SECRETS
        .iter()
        .filter(|(_, modes)| mode == "All" || modes.contains(&mode))
        .map(|(name, _)| *name)
        .collect()
}

pub fn git_head(root: &Path) -> String {
    Command::new("git")
        .args(["-C", &root.to_string_lossy(), "rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

#[cfg(test)]
mod env_helper_tests {
    use super::{
        bronze_force_refetch_enabled, optional_bool_env, optional_csv_env, optional_env_value,
        optional_positive_u32_env, optional_u64_env, required_env_value, BRONZE_FORCE_REFETCH_ENV,
    };

    // Each test uses a unique env-var name so the (concurrent) test runner never shares mutable
    // state between tests.

    #[test]
    fn optional_env_value_trims_and_treats_blank_as_unset() -> anyhow::Result<()> {
        let name = "FOUNDATION_PLATFORM_ENV_HELPER_TEST_TRIM";
        std::env::set_var(name, "  spaced-value  ");
        assert_eq!(optional_env_value(name)?, Some("spaced-value".to_owned()));
        std::env::set_var(name, "   ");
        assert_eq!(optional_env_value(name)?, None);
        std::env::remove_var(name);
        assert_eq!(optional_env_value(name)?, None);
        Ok(())
    }

    #[test]
    fn required_env_value_bails_when_blank() {
        let name = "FOUNDATION_PLATFORM_ENV_HELPER_TEST_REQUIRED";
        std::env::set_var(name, "   ");
        let error = required_env_value(name).expect_err("blank must be treated as missing");
        assert!(error.to_string().contains("is required"), "{error}");
        std::env::remove_var(name);
    }

    #[test]
    fn optional_u64_env_parses_trimmed_digits() -> anyhow::Result<()> {
        let name = "FOUNDATION_PLATFORM_ENV_HELPER_TEST_U64";
        std::env::set_var(name, "  42 ");
        assert_eq!(optional_u64_env(name)?, Some(42));
        std::env::set_var(name, "nope");
        assert!(optional_u64_env(name).is_err());
        std::env::remove_var(name);
        Ok(())
    }

    #[test]
    fn optional_positive_u32_env_rejects_zero() {
        let name = "FOUNDATION_PLATFORM_ENV_HELPER_TEST_POSITIVE";
        std::env::set_var(name, "0");
        assert!(optional_positive_u32_env(name).is_err());
        std::env::remove_var(name);
    }

    #[test]
    fn optional_bool_env_accepts_known_tokens() -> anyhow::Result<()> {
        let name = "FOUNDATION_PLATFORM_ENV_HELPER_TEST_BOOL";
        std::env::set_var(name, "true");
        assert_eq!(optional_bool_env(name)?, Some(true));
        std::env::set_var(name, "0");
        assert_eq!(optional_bool_env(name)?, Some(false));
        std::env::set_var(name, "maybe");
        assert!(optional_bool_env(name).is_err());
        std::env::remove_var(name);
        Ok(())
    }

    #[test]
    fn bronze_force_refetch_defaults_off_and_honors_the_flag() -> anyhow::Result<()> {
        // Fixed env name (no other test reads it), saved/restored so the suite stays clean.
        let saved = std::env::var(BRONZE_FORCE_REFETCH_ENV).ok();

        std::env::remove_var(BRONZE_FORCE_REFETCH_ENV);
        assert!(
            !bronze_force_refetch_enabled()?,
            "unset must default to off"
        );

        std::env::set_var(BRONZE_FORCE_REFETCH_ENV, "1");
        assert!(
            bronze_force_refetch_enabled()?,
            "1 must enable force-refetch"
        );

        std::env::set_var(BRONZE_FORCE_REFETCH_ENV, "0");
        assert!(!bronze_force_refetch_enabled()?, "0 must keep it off");

        std::env::set_var(BRONZE_FORCE_REFETCH_ENV, "maybe");
        assert!(
            bronze_force_refetch_enabled().is_err(),
            "an unrecognized token must be a hard error, not silently off"
        );

        match saved {
            Some(value) => std::env::set_var(BRONZE_FORCE_REFETCH_ENV, value),
            None => std::env::remove_var(BRONZE_FORCE_REFETCH_ENV),
        }
        Ok(())
    }

    #[test]
    fn optional_csv_env_splits_and_trims_parts() -> anyhow::Result<()> {
        let name = "FOUNDATION_PLATFORM_ENV_HELPER_TEST_CSV";
        std::env::set_var(name, " a , b ,, c ");
        assert_eq!(
            optional_csv_env(name)?,
            Some(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
        std::env::remove_var(name);
        Ok(())
    }
}

#[cfg(test)]
mod resolve_canonical_source_slug_tests {
    use super::resolve_canonical_source_slug;

    #[test]
    fn matching_override_is_accepted() -> anyhow::Result<()> {
        let resolved = resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG",
            Some("datagokr__building_register_main".to_owned()),
            "data.go.kr",
            "building_register_main",
        )?;
        assert_eq!(resolved, "datagokr__building_register_main");
        Ok(())
    }

    #[test]
    fn unset_override_returns_generated_canonical_slug() -> anyhow::Result<()> {
        let resolved = resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SOURCE_SLUG",
            None,
            "VWorld",
            "cadastral",
        )?;
        assert_eq!(resolved, "vworldkr__cadastral");
        Ok(())
    }

    #[test]
    fn mismatched_old_override_bails_with_actionable_message() {
        let error = resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG",
            // Old, pre-rename slug a human might still have in their env.
            Some("molit-building-register".to_owned()),
            "data.go.kr",
            "building_register_main",
        )
        .expect_err("a mismatched override must fail fast at config time");
        let message = error.to_string();
        assert!(
            message.contains(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG=molit-building-register"
            ),
            "message must name the offending env=value: {message}"
        );
        assert!(
            message.contains("expected datagokr__building_register_main"),
            "message must name the canonical expected slug: {message}"
        );
        assert!(
            message.contains("Unset it to let the generator derive it"),
            "message must tell the operator how to recover: {message}"
        );
    }

    #[test]
    fn invalid_provider_dataset_pair_is_a_hard_error() {
        // Unknown provider can never produce a canonical slug, so even an "unset" call fails loudly
        // rather than silently inventing a slug.
        assert!(resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_SOME_SOURCE_SLUG",
            None,
            "not-a-provider",
            "cadastral",
        )
        .is_err());
    }
}
