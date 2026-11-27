//! Canonical Bronze `source_slug` generator (the single SSOT producer).
//!
//! `source_slug = providerid + "__" + dataset_slug` (FP-ADR-0014 D1/D3, root ADR-0626 rule 2).
//!
//! - The `providerid` is a fixed key of the naming registry (root ADR-0626 §3,
//!   `config/naming.contract.json`, generated from `definitions/platform.definition.json`). It is
//!   never derived from a host name at run time: the catalog-native provider label (e.g.
//!   `data.go.kr`) is one of the key's `domains`, an attribute. [`provider_id`] looks it up.
//! - `dataset_slug` is the canonical *semantic* dataset identity in `snake_case` (e.g.
//!   `building_register_main`). It is **distinct from** the provider-native `operation`
//!   (e.g. `getBrTitleInfo`); the generator takes `dataset_slug`, never `operation`.
//!
//! Every Bronze producer (catalog authoring, `OPERATION_SPECS`, per-binary defaults, pilot/derived
//! slugs, fallback formatters) is intended to route through [`source_slug`] so the slug has exactly
//! one origin and cannot silently diverge.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::Deserialize;
use thiserror::Error;

/// The naming registry (root ADR-0626 §1): generated, never committed (root ADR-0418).
const NAMING_CONTRACT: &str = include_str!("../../../../config/naming.contract.json");

/// Validation error returned by the canonical Collection source-slug rules.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SourceSlugError {
    /// Provider is outside the owner-approved provider registry.
    #[error("unknown provider for source_slug generation: {0:?}")]
    UnknownProvider(String),

    /// Dataset slug is not canonical lowercase `snake_case`.
    #[error(
        "invalid dataset_slug {0:?}: must match ^[a-z0-9][a-z0-9_]*$ (lowercase snake_case, no '-', no leading/trailing '_')"
    )]
    InvalidDatasetSlug(String),

    /// Complete source slug does not match the canonical provider/dataset grammar.
    #[error(
        "non-canonical Bronze source_slug {0:?}: must use a registered providerid and lowercase snake_case dataset_slug"
    )]
    NonCanonicalSourceSlug(String),
}

/// One provider of the naming registry: its fixed id and the domain labels that name it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Provider {
    /// The fixed registry key, the left half of every source id (root ADR-0626 rule 1).
    #[serde(skip)]
    pub id: String,
    /// The catalog-native provider labels (domains) of this provider, attributes of the key.
    pub domains: Vec<String>,
}

#[derive(Deserialize)]
struct Registry {
    providers: BTreeMap<String, Provider>,
}

fn registry() -> &'static Result<Vec<Provider>, String> {
    static PROVIDERS: OnceLock<Result<Vec<Provider>, String>> = OnceLock::new();
    PROVIDERS.get_or_init(|| parse_registry(NAMING_CONTRACT))
}

fn parse_registry(text: &str) -> Result<Vec<Provider>, String> {
    let registry: Registry = serde_json::from_str(text).map_err(|error| {
        format!("config/naming.contract.json does not parse (run declared_definitions.py generate): {error}")
    })?;
    Ok(registry
        .providers
        .into_iter()
        .map(|(id, provider)| Provider { id, ..provider })
        .collect())
}

/// The registered providers, in id order; empty when the registry cannot be read
/// ([`provider_registry_error`] says why, and this crate's tests refuse it).
#[must_use]
pub fn providers() -> &'static [Provider] {
    registry().as_ref().map_or(&[], Vec::as_slice)
}

/// Why the provider registry could not be read, or `None`.
#[must_use]
pub fn provider_registry_error() -> Option<&'static str> {
    registry().as_ref().err().map(String::as_str)
}

/// Maps a catalog-native `provider` label (a domain) to its registered `providerid`.
///
/// Returns `None` for any label no registered provider names. An unknown provider is a hard error
/// for the slug generator. The id is the registry's key, never computed from the label.
#[must_use]
pub fn provider_id(provider: &str) -> Option<String> {
    providers()
        .iter()
        .find(|entry| entry.domains.iter().any(|domain| domain == provider))
        .map(|entry| entry.id.clone())
}

/// Produces the canonical Bronze `source_slug` for `(provider, dataset_slug)`.
///
/// # Errors
/// - no registered provider names the `provider` label (see [`provider_id`]);
/// - the `dataset_slug` is empty, or does not match `^[a-z0-9][a-z0-9_]*$` (lowercase ASCII,
///   `snake_case`, no `-`, and no leading/trailing `_`).
///
/// On success returns `format!("{providerid}__{dataset_slug}")`, which is guaranteed to satisfy the
/// `validate_source_slug` charset used by `build_bronze_object_key`.
pub fn source_slug(provider: &str, dataset_slug: &str) -> Result<String, SourceSlugError> {
    let Some(id) = provider_id(provider) else {
        return Err(SourceSlugError::UnknownProvider(provider.to_owned()));
    };
    if !is_canonical_dataset_slug(dataset_slug) {
        return Err(SourceSlugError::InvalidDatasetSlug(dataset_slug.to_owned()));
    }
    Ok(format!("{id}__{dataset_slug}"))
}

/// Returns `true` iff `dataset_slug` matches `^[a-z0-9][a-z0-9_]*$`.
fn is_canonical_dataset_slug(dataset_slug: &str) -> bool {
    let mut bytes = dataset_slug.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return false;
    }
    // Remaining bytes may be lowercase, digits, or '_'. A trailing '_' is rejected.
    if dataset_slug.ends_with('_') {
        return false;
    }
    bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Returns `true` iff `slug` is exactly `"{providerid}__{dataset_slug}"` where `providerid` is a
/// key of the naming registry and `dataset_slug` matches `^[a-z0-9][a-z0-9_]*$`.
///
/// This is the canonical Bronze `source_slug` shape (ADR 0014). It is the backstop the Bronze write
/// boundary uses so a non-canonical slug (old hyphenated names, single-underscore variants, unknown
/// providers, uppercase) can never be written, regardless of which producer assembled it.
#[must_use]
pub fn is_canonical_source_slug(slug: &str) -> bool {
    let Some((provider, dataset_slug)) = slug.split_once("__") else {
        return false;
    };
    providers().iter().any(|entry| entry.id == provider) && is_canonical_dataset_slug(dataset_slug)
}

/// Asserts that `slug` is a canonical Bronze `source_slug` (see [`is_canonical_source_slug`]).
///
/// # Errors
/// Returns an error naming the offending `slug` and the required `{providerid}__{dataset_slug}`
/// format when `slug` is not canonical.
pub fn assert_canonical_source_slug(slug: &str) -> Result<(), SourceSlugError> {
    if is_canonical_source_slug(slug) {
        return Ok(());
    }
    Err(SourceSlugError::NonCanonicalSourceSlug(slug.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        assert_canonical_source_slug, is_canonical_source_slug, parse_registry, provider_id,
        provider_registry_error, providers, source_slug,
    };
    use crate::bronze::{build_bronze_object_key, BronzeObjectKeyParts};

    #[test]
    fn is_canonical_source_slug_accepts_canonical_examples() {
        for slug in [
            "datagokr__building_register_main",
            "rtmolitgokr__real_transaction_apartment_trade",
            "hubgokr__building_register",
            "vworldkr__cadastral",
            "jusogokr__legal_emd",
            "moisgokr__dong_population",
            "factoryongokr__factory_registration",
        ] {
            assert!(
                is_canonical_source_slug(slug),
                "expected canonical slug to be accepted: {slug:?}"
            );
            assert!(
                assert_canonical_source_slug(slug).is_ok(),
                "expected assert_canonical_source_slug ok for {slug:?}"
            );
        }
    }

    #[test]
    fn is_canonical_source_slug_rejects_non_canonical_examples() {
        for slug in [
            // Old hyphenated names.
            "molit-building-register",
            "vworld-cadastral",
            "vworld-cadastral-national-99999-00101",
            "data-go-kr-building-register",
            "hub-building-building-register-main",
            // Uppercase dataset.
            "datagokr__Building",
            // Unknown provider.
            "unknownprov__x",
            // Empty dataset.
            "datagokr__",
            // Single underscore (not the `__` separator).
            "datagokr_building",
            // Hyphen inside dataset.
            "datagokr__a-b",
        ] {
            assert!(
                !is_canonical_source_slug(slug),
                "expected non-canonical slug to be rejected: {slug:?}"
            );
            assert!(
                assert_canonical_source_slug(slug).is_err(),
                "expected assert_canonical_source_slug err for {slug:?}"
            );
        }
    }

    #[test]
    fn provider_id_maps_every_in_scope_provider() {
        assert_eq!(provider_id("vworld.kr").as_deref(), Some("vworldkr"));
        assert_eq!(provider_id("data.go.kr").as_deref(), Some("datagokr"));
        assert_eq!(
            provider_id("rt.molit.go.kr").as_deref(),
            Some("rtmolitgokr")
        );
        assert_eq!(provider_id("hub.go.kr").as_deref(), Some("hubgokr"));
        assert_eq!(provider_id("juso.go.kr").as_deref(), Some("jusogokr"));
        assert_eq!(provider_id("mois.go.kr").as_deref(), Some("moisgokr"));
        assert_eq!(
            provider_id("factoryon.go.kr").as_deref(),
            Some("factoryongokr")
        );
        assert_eq!(
            provider_id("courtauction.go.kr").as_deref(),
            Some("courtauctiongokr")
        );
    }

    #[test]
    fn provider_id_keeps_every_rt_molit_domain_label() {
        assert_eq!(
            provider_id("rt.molit.go.kr").as_deref(),
            Some("rtmolitgokr")
        );
    }

    #[test]
    fn provider_id_derives_industryland_domain() {
        assert_eq!(
            provider_id("industryland.or.kr").as_deref(),
            Some("industrylandorkr")
        );
    }

    #[test]
    fn provider_id_derives_the_industrial_complex_corporation_domain() {
        assert_eq!(provider_id("kicox.or.kr").as_deref(), Some("kicoxorkr"));
    }

    #[test]
    fn provider_id_derives_the_code_change_source_domain() {
        assert_eq!(provider_id("code.go.kr").as_deref(), Some("codegokr"));
    }

    #[test]
    fn provider_id_rejects_out_of_scope_providers() {
        assert_eq!(provider_id("VWorld"), None);
        assert_eq!(provider_id("juso"), None);
        assert_eq!(provider_id("mixed_public_source"), None);
        assert_eq!(provider_id("data-go-kr"), None);
        assert_eq!(provider_id(""), None);
    }

    /// The owner kept today's values as fixed keys (root ADR-0626 rule 1, option K, 2026-11-28): a
    /// key once written never changes, so the set is frozen here and a change must edit this test.
    #[test]
    fn the_registry_holds_the_owners_eleven_fixed_provider_ids() {
        assert_eq!(provider_registry_error(), None);
        let ids: Vec<&str> = providers().iter().map(|entry| entry.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "codegokr",
                "courtauctiongokr",
                "datagokr",
                "factoryongokr",
                "hubgokr",
                "industrylandorkr",
                "jusogokr",
                "kicoxorkr",
                "moisgokr",
                "rtmolitgokr",
                "vworldkr",
            ]
        );
    }

    #[test]
    fn every_domain_names_one_provider() {
        let mut seen = HashSet::new();
        for entry in providers() {
            assert!(!entry.domains.is_empty(), "{} has no domain", entry.id);
            for domain in &entry.domains {
                assert!(seen.insert(domain.as_str()), "{domain} names two providers");
                assert_eq!(provider_id(domain).as_deref(), Some(entry.id.as_str()));
            }
        }
    }

    #[test]
    fn an_unreadable_registry_is_an_error_not_an_empty_list() {
        assert!(parse_registry("{}").is_err());
        assert!(parse_registry(r#"{"providers": {"x": {}}}"#).is_err());
    }

    #[test]
    fn source_slug_matches_owner_approved_examples() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            source_slug("data.go.kr", "building_register_main")?,
            "datagokr__building_register_main"
        );
        assert_eq!(
            source_slug("rt.molit.go.kr", "real_transaction_apartment_trade")?,
            "rtmolitgokr__real_transaction_apartment_trade"
        );
        assert_eq!(
            source_slug("hub.go.kr", "building_register_basis_outline")?,
            "hubgokr__building_register_basis_outline"
        );
        assert_eq!(source_slug("juso.go.kr", "building")?, "jusogokr__building");
        Ok(())
    }

    #[test]
    fn source_slug_rejects_unknown_provider() {
        assert!(source_slug("mixed_public_source", "highway_ic").is_err());
        assert!(source_slug("openapi", "anything").is_err());
    }

    #[test]
    fn source_slug_rejects_non_canonical_dataset_slug() {
        // Uppercase.
        assert!(source_slug("data.go.kr", "Foo").is_err());
        // Leading underscore.
        assert!(source_slug("data.go.kr", "_x").is_err());
        // Trailing underscore.
        assert!(source_slug("data.go.kr", "x_").is_err());
        // Hyphen disallowed.
        assert!(source_slug("data.go.kr", "x-y").is_err());
        // Empty.
        assert!(source_slug("data.go.kr", "").is_err());
    }

    #[test]
    fn generated_slug_passes_bronze_key_validation() -> Result<(), Box<dyn std::error::Error>> {
        let slug = source_slug("data.go.kr", "building_register_main")?;
        let key = build_bronze_object_key(BronzeObjectKeyParts {
            source_slug: &slug,
            partition_path: "sigungu=11680",
            leaf_name: "page-000001",
            extension: "json",
        })?;
        assert!(
            key.as_str()
                .starts_with("bronze/source=datagokr__building_register_main/"),
            "unexpected key: {}",
            key.as_str()
        );
        Ok(())
    }
}
