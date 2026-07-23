//! Canonical Bronze `source_slug` generator (the single SSOT producer).
//!
//! `source_slug = providerid(provider) + "__" + dataset_slug` (ADR 0014 D1/D2/D3).
//!
//! - `provider` is the catalog-native provider label (e.g. `data.go.kr`), mapped to a stable,
//!   engine-portable `providerid` (e.g. `datagokr`) by [`provider_id`].
//! - `dataset_slug` is the canonical *semantic* dataset identity in `snake_case` (e.g.
//!   `building_register_main`). It is **distinct from** the provider-native `operation`
//!   (e.g. `getBrTitleInfo`); the generator takes `dataset_slug`, never `operation`.
//!
//! Every Bronze producer (catalog authoring, `OPERATION_SPECS`, per-binary defaults, pilot/derived
//! slugs, fallback formatters) is intended to route through [`source_slug`] so the slug has exactly
//! one origin and cannot silently diverge.

use thiserror::Error;

/// Validation error returned by the canonical Collection source-slug rules.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SourceSlugError {
    /// Provider is outside the owner-approved provider map.
    #[error("unknown provider for source_slug generation: {0:?}")]
    UnknownProvider(String),

    /// Dataset slug is not canonical lowercase `snake_case`.
    #[error(
        "invalid dataset_slug {0:?}: must match ^[a-z0-9][a-z0-9_]*$ (lowercase snake_case, no '-', no leading/trailing '_')"
    )]
    InvalidDatasetSlug(String),

    /// Complete source slug does not match the canonical provider/dataset grammar.
    #[error(
        "non-canonical Bronze source_slug {0:?}: must use an approved providerid and lowercase snake_case dataset_slug"
    )]
    NonCanonicalSourceSlug(String),
}

/// The owner-approved set of canonical `providerid` values (ADR 0014 D2).
///
/// This is the SSOT for "which `providerid` may appear on the left of `__` in a canonical
/// `source_slug`". [`provider_id`] maps every in-scope provider label onto one of these, and
/// [`is_canonical_source_slug`] checks membership against it.
pub const KNOWN_PROVIDER_IDS: [&str; 7] = [
    "vworldkr",
    "datagokr",
    "rtmolitkr",
    "hubgokr",
    "jusogokr",
    "moisgokr",
    "factoryongokr",
];

/// Maps a catalog-native `provider` label to its canonical, engine-portable `providerid`.
///
/// Returns `None` for any provider outside the owner-approved 7-provider map (ADR 0014 D2). An
/// unknown provider is a hard error for the slug generator: it forces the map to stay complete.
///
/// Every value returned here is a member of [`KNOWN_PROVIDER_IDS`].
#[must_use]
pub fn provider_id(provider: &str) -> Option<&'static str> {
    match provider {
        "VWorld" => Some("vworldkr"),
        "data.go.kr" => Some("datagokr"),
        "rt.molit.go.kr" => Some("rtmolitkr"),
        "hub.go.kr" => Some("hubgokr"),
        "juso" => Some("jusogokr"),
        "mois.go.kr" => Some("moisgokr"),
        "factoryon.go.kr" => Some("factoryongokr"),
        _ => None,
    }
}

/// Produces the canonical Bronze `source_slug` for `(provider, dataset_slug)`.
///
/// # Errors
/// - the `provider` is not in the 7-provider map (see [`provider_id`]);
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

/// Returns `true` iff `slug` is exactly `"{providerid}__{dataset_slug}"` where `providerid` is one
/// of the seven [`KNOWN_PROVIDER_IDS`] and `dataset_slug` matches `^[a-z0-9][a-z0-9_]*$`.
///
/// This is the canonical Bronze `source_slug` shape (ADR 0014). It is the backstop the Bronze write
/// boundary uses so a non-canonical slug (old hyphenated names, single-underscore variants, unknown
/// providers, uppercase) can never be written, regardless of which producer assembled it.
#[must_use]
pub fn is_canonical_source_slug(slug: &str) -> bool {
    let Some((provider, dataset_slug)) = slug.split_once("__") else {
        return false;
    };
    KNOWN_PROVIDER_IDS.contains(&provider) && is_canonical_dataset_slug(dataset_slug)
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
    use super::{assert_canonical_source_slug, is_canonical_source_slug, provider_id, source_slug};
    use crate::bronze::{build_bronze_object_key, BronzeObjectKeyParts};

    #[test]
    fn is_canonical_source_slug_accepts_canonical_examples() {
        for slug in [
            "datagokr__building_register_main",
            "rtmolitkr__real_transaction_apartment_trade",
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
        assert_eq!(provider_id("VWorld"), Some("vworldkr"));
        assert_eq!(provider_id("data.go.kr"), Some("datagokr"));
        assert_eq!(provider_id("rt.molit.go.kr"), Some("rtmolitkr"));
        assert_eq!(provider_id("hub.go.kr"), Some("hubgokr"));
        assert_eq!(provider_id("juso"), Some("jusogokr"));
        assert_eq!(provider_id("mois.go.kr"), Some("moisgokr"));
        assert_eq!(provider_id("factoryon.go.kr"), Some("factoryongokr"));
    }

    #[test]
    fn provider_id_rejects_out_of_scope_providers() {
        assert_eq!(provider_id("mixed_public_source"), None);
        assert_eq!(provider_id("data-go-kr"), None);
        assert_eq!(provider_id(""), None);
    }

    #[test]
    fn source_slug_matches_owner_approved_examples() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            source_slug("data.go.kr", "building_register_main")?,
            "datagokr__building_register_main"
        );
        assert_eq!(
            source_slug("rt.molit.go.kr", "real_transaction_apartment_trade")?,
            "rtmolitkr__real_transaction_apartment_trade"
        );
        assert_eq!(
            source_slug("hub.go.kr", "building_register_basis_outline")?,
            "hubgokr__building_register_basis_outline"
        );
        assert_eq!(source_slug("juso", "building")?, "jusogokr__building");
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
