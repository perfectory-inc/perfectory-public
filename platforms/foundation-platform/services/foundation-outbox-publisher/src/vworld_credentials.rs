use std::{collections::BTreeMap, sync::OnceLock};

use anyhow::Context;
use serde::Deserialize;

use crate::public_data_control_support::optional_env_value;

const ENVIRONMENT_VARIABLE_NAMING_CONTRACT: &str =
    include_str!("../../../config/environment-variable-naming.contract.json");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VWorldCredential {
    ApiKey,
    Domain,
    Username,
    Password,
}

const VWORLD_CREDENTIALS: [VWorldCredential; 4] = [
    VWorldCredential::ApiKey,
    VWorldCredential::Domain,
    VWorldCredential::Username,
    VWorldCredential::Password,
];

#[derive(Debug, Deserialize)]
struct EnvironmentVariableNamingContract {
    schema_version: u64,
    compatibility_migrations: CompatibilityMigrations,
}

#[derive(Debug, Deserialize)]
struct CompatibilityMigrations {
    #[serde(rename = "foundation-vworld-credentials")]
    foundation_vworld_credentials: VWorldCredentialMigration,
}

#[derive(Debug, Deserialize)]
struct VWorldCredentialMigration {
    precedence: String,
    credentials: VWorldCredentialNames,
}

#[derive(Debug, Deserialize)]
struct VWorldCredentialNames {
    api_key: CredentialNames,
    domain: CredentialNames,
    username: CredentialNames,
    password: CredentialNames,
}

#[derive(Debug, Deserialize)]
struct CredentialNames {
    canonical: String,
    deprecated_aliases: Vec<String>,
    sensitive: bool,
}

impl VWorldCredentialNames {
    fn get(&self, credential: VWorldCredential) -> &CredentialNames {
        match credential {
            VWorldCredential::ApiKey => &self.api_key,
            VWorldCredential::Domain => &self.domain,
            VWorldCredential::Username => &self.username,
            VWorldCredential::Password => &self.password,
        }
    }
}

fn vworld_migration() -> anyhow::Result<&'static VWorldCredentialMigration> {
    static CONTRACT: OnceLock<Result<EnvironmentVariableNamingContract, String>> = OnceLock::new();
    let contract = CONTRACT
        .get_or_init(|| {
            let contract: EnvironmentVariableNamingContract =
                serde_json::from_str(ENVIRONMENT_VARIABLE_NAMING_CONTRACT).map_err(|error| {
                    format!("invalid environment-variable naming contract: {error}")
                })?;
            if contract.schema_version != 1 {
                return Err(format!(
                    "environment-variable naming contract schema must be 1, got {}",
                    contract.schema_version
                ));
            }
            if contract
                .compatibility_migrations
                .foundation_vworld_credentials
                .precedence
                != "canonical-first"
            {
                return Err("VWorld credential precedence must be canonical-first".to_owned());
            }
            Ok(contract)
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))?;
    Ok(&contract
        .compatibility_migrations
        .foundation_vworld_credentials)
}

fn resolve_vworld_credential_with_warning<F, W>(
    credential: VWorldCredential,
    mut lookup: F,
    mut warning: W,
) -> anyhow::Result<Option<String>>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
    W: FnMut(&str, &str),
{
    let names = vworld_migration()?.credentials.get(credential);
    if let Some(value) = lookup(&names.canonical)? {
        return Ok(Some(value));
    }
    for alias in &names.deprecated_aliases {
        if let Some(value) = lookup(alias)? {
            warning(alias, &names.canonical);
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn map_value(values: &BTreeMap<String, String>, name: &str) -> anyhow::Result<Option<String>> {
    Ok(values.get(name).and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }))
}

fn resolve_vworld_credential_from_map_with_warning<W>(
    values: &BTreeMap<String, String>,
    credential: VWorldCredential,
    warning: W,
) -> anyhow::Result<Option<String>>
where
    W: FnMut(&str, &str),
{
    resolve_vworld_credential_with_warning(credential, |name| map_value(values, name), warning)
}

fn emit_deprecated_alias_warning(alias: &str, canonical: &str) {
    tracing::warn!(
        deprecated_environment_variable = alias,
        replacement_environment_variable = canonical,
        "deprecated VWorld environment variable alias supplied the value"
    );
}

fn resolve_process_credential(credential: VWorldCredential) -> anyhow::Result<Option<String>> {
    resolve_vworld_credential_with_warning(
        credential,
        optional_env_value,
        emit_deprecated_alias_warning,
    )
}

pub(crate) fn required_vworld_api_key() -> anyhow::Result<String> {
    let canonical = &vworld_migration()?
        .credentials
        .get(VWorldCredential::ApiKey)
        .canonical;
    resolve_process_credential(VWorldCredential::ApiKey)?
        .with_context(|| format!("{canonical} is required"))
}

pub(crate) fn optional_vworld_domain() -> anyhow::Result<Option<String>> {
    resolve_process_credential(VWorldCredential::Domain)
}

pub(crate) fn optional_vworld_username() -> anyhow::Result<Option<String>> {
    resolve_process_credential(VWorldCredential::Username)
}

pub(crate) fn optional_vworld_password() -> anyhow::Result<Option<String>> {
    resolve_process_credential(VWorldCredential::Password)
}

pub(crate) fn normalize_vworld_credentials_in_map(
    values: &mut BTreeMap<String, String>,
) -> anyhow::Result<()> {
    normalize_vworld_credentials_in_map_with_warning(values, emit_deprecated_alias_warning)
}

fn normalize_vworld_credentials_in_map_with_warning<W>(
    values: &mut BTreeMap<String, String>,
    mut warning: W,
) -> anyhow::Result<()>
where
    W: FnMut(&str, &str),
{
    for credential in VWORLD_CREDENTIALS {
        let names = vworld_migration()?.credentials.get(credential);
        let resolved =
            resolve_vworld_credential_from_map_with_warning(values, credential, &mut warning)?;
        for alias in &names.deprecated_aliases {
            values.remove(alias);
        }
        if let Some(value) = resolved {
            values.insert(names.canonical.clone(), value);
        }
    }
    Ok(())
}

pub(crate) fn vworld_api_key_name() -> anyhow::Result<&'static str> {
    Ok(vworld_migration()?
        .credentials
        .get(VWorldCredential::ApiKey)
        .canonical
        .as_str())
}

pub(crate) fn vworld_username_name() -> anyhow::Result<&'static str> {
    Ok(vworld_migration()?
        .credentials
        .get(VWorldCredential::Username)
        .canonical
        .as_str())
}

pub(crate) fn vworld_password_name() -> anyhow::Result<&'static str> {
    Ok(vworld_migration()?
        .credentials
        .get(VWorldCredential::Password)
        .canonical
        .as_str())
}

pub(crate) fn sensitive_vworld_environment_names() -> anyhow::Result<Vec<&'static str>> {
    let credentials = &vworld_migration()?.credentials;
    let mut names = Vec::new();
    for credential in VWORLD_CREDENTIALS {
        let credential = credentials.get(credential);
        if credential.sensitive {
            names.push(credential.canonical.as_str());
            names.extend(credential.deprecated_aliases.iter().map(String::as_str));
        }
    }
    Ok(names)
}

pub(crate) fn sensitive_vworld_environment_name_in(
    value: &str,
) -> anyhow::Result<Option<&'static str>> {
    Ok(sensitive_vworld_environment_names()?
        .into_iter()
        .find(|name| value.contains(name)))
}

pub(crate) fn redact_sensitive_vworld_environment_names(value: &str) -> String {
    let Ok(names) = sensitive_vworld_environment_names() else {
        tracing::error!(
            "environment-variable naming contract is invalid; redacting the complete error"
        );
        return "[redacted: invalid environment-variable naming contract]".to_owned();
    };
    names.into_iter().fold(value.to_owned(), |redacted, name| {
        redacted.replace(name, "[redacted]")
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        normalize_vworld_credentials_in_map_with_warning,
        resolve_vworld_credential_from_map_with_warning, VWorldCredential,
    };

    #[test]
    fn canonical_value_wins_without_reading_a_deprecated_alias() {
        let values = BTreeMap::from([
            (
                "FOUNDATION_PLATFORM_VWORLD_API_KEY".to_owned(),
                "canonical-key".to_owned(),
            ),
            ("VWORLD_API_KEY".to_owned(), "legacy-key".to_owned()),
        ]);
        let mut warnings = Vec::new();

        let resolved = resolve_vworld_credential_from_map_with_warning(
            &values,
            VWorldCredential::ApiKey,
            |legacy, canonical| warnings.push((legacy.to_owned(), canonical.to_owned())),
        )
        .expect("canonical VWorld credential must resolve");

        assert_eq!(resolved.as_deref(), Some("canonical-key"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn deprecated_alias_is_accepted_and_names_only_are_warned() {
        let values = BTreeMap::from([("VWORLD_DOMAIN".to_owned(), "example.test".to_owned())]);
        let mut warnings = Vec::new();

        let resolved = resolve_vworld_credential_from_map_with_warning(
            &values,
            VWorldCredential::Domain,
            |legacy, canonical| warnings.push((legacy.to_owned(), canonical.to_owned())),
        )
        .expect("legacy VWorld credential must resolve during the compatibility window");

        assert_eq!(resolved.as_deref(), Some("example.test"));
        assert_eq!(
            warnings,
            vec![(
                "VWORLD_DOMAIN".to_owned(),
                "FOUNDATION_PLATFORM_VWORLD_DOMAIN".to_owned()
            )]
        );
    }

    #[test]
    fn map_normalization_replaces_all_username_aliases_with_one_canonical_key() {
        let mut values = BTreeMap::from([
            (
                "FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME".to_owned(),
                "dataset-user".to_owned(),
            ),
            ("VWORLD_USERNAME".to_owned(), "legacy-user".to_owned()),
        ]);
        let mut warnings = Vec::new();

        normalize_vworld_credentials_in_map_with_warning(&mut values, |legacy, canonical| {
            warnings.push((legacy.to_owned(), canonical.to_owned()));
        })
        .expect("VWorld aliases must normalize");

        assert_eq!(
            values
                .get("FOUNDATION_PLATFORM_VWORLD_USERNAME")
                .map(String::as_str),
            Some("dataset-user")
        );
        assert!(!values.contains_key("FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME"));
        assert!(!values.contains_key("VWORLD_USERNAME"));
        assert_eq!(
            warnings,
            vec![(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME".to_owned(),
                "FOUNDATION_PLATFORM_VWORLD_USERNAME".to_owned()
            )]
        );
    }
}
