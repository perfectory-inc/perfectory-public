//! The lakehouse bucket's settings: what they need, and loading them from that.
//!
//! The requirements are data ([`LAKEHOUSE_WRITER_REQUIREMENTS`], [`LAKEHOUSE_READER_REQUIREMENTS`])
//! and the loader refuses by them before it reads a value, so what a command says it needs
//! (`describe-requirements`) is what its loader holds it to. The tests hold the two together: every
//! declared name alone makes the difference between loading and refusing.

use foundation_shared_kernel::env_source::{first_unmet, EnvSource, Requirement};

use super::{R2KeyNamespace, R2ObjectStorageConfig};
use super::{RUNTIME_ENVIRONMENT_ENV, STAGING_MULTIPART_THRESHOLD_ENV};
use crate::errors::PublishError;

const ENDPOINT: &str = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT";
const ACCOUNT_ID: &str = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID";
const BUCKET: &str = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET";
const REGION: &str = "FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION";
const WRITER_KEY: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID";
const WRITER_SECRET: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY";
const READER_KEY: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID";
const READER_SECRET: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY";
// With R2's temporary credentials, the key pair's session token (root ADR-0616 A2).
const WRITER_SESSION: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SESSION_TOKEN";
const READER_SESSION: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SESSION_TOKEN";
// A command for fresh temporary credentials, asked again before they expire (credential_process, W101c).
const WRITER_PROCESS: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_CREDENTIAL_PROCESS";
const READER_PROCESS: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_READER_CREDENTIAL_PROCESS";

/// What reading and writing the lakehouse bucket needs: the writer key pair.
pub const LAKEHOUSE_WRITER_REQUIREMENTS: &[Requirement] = &[
    Requirement::OneOf(&[ENDPOINT, ACCOUNT_ID]),
    Requirement::Required(BUCKET),
    Requirement::Required(WRITER_KEY),
    Requirement::Required(WRITER_SECRET),
    Requirement::Optional(WRITER_SESSION),
    Requirement::Optional(WRITER_PROCESS),
    Requirement::Optional(REGION),
    Requirement::Optional(RUNTIME_ENVIRONMENT_ENV),
    Requirement::Optional(STAGING_MULTIPART_THRESHOLD_ENV),
];

/// What only reading the lakehouse bucket needs: the read-only key pair (root ADR-0152).
pub const LAKEHOUSE_READER_REQUIREMENTS: &[Requirement] = &[
    Requirement::OneOf(&[ENDPOINT, ACCOUNT_ID]),
    Requirement::Required(BUCKET),
    Requirement::Required(READER_KEY),
    Requirement::Required(READER_SECRET),
    Requirement::Optional(READER_SESSION),
    Requirement::Optional(READER_PROCESS),
    Requirement::Optional(REGION),
    Requirement::Optional(RUNTIME_ENVIRONMENT_ENV),
    Requirement::Optional(STAGING_MULTIPART_THRESHOLD_ENV),
];

impl R2ObjectStorageConfig {
    /// The lakehouse configuration with the writer key pair, from `source`.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` naming the first of [`LAKEHOUSE_WRITER_REQUIREMENTS`] `source` does
    /// not meet, or when the staging multipart threshold is not usable.
    pub fn lakehouse_writer_from(source: &impl EnvSource) -> Result<Self, PublishError> {
        lakehouse_from(
            source,
            LAKEHOUSE_WRITER_REQUIREMENTS,
            [WRITER_KEY, WRITER_SECRET, WRITER_SESSION, WRITER_PROCESS],
        )
    }

    /// The lakehouse configuration with the read-only key pair, from `source`.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` naming the first of [`LAKEHOUSE_READER_REQUIREMENTS`] `source` does
    /// not meet, or when the staging multipart threshold is not usable.
    pub fn lakehouse_reader_from(source: &impl EnvSource) -> Result<Self, PublishError> {
        lakehouse_from(
            source,
            LAKEHOUSE_READER_REQUIREMENTS,
            [READER_KEY, READER_SECRET, READER_SESSION, READER_PROCESS],
        )
    }
}

fn lakehouse_from(
    source: &impl EnvSource,
    requirements: &[Requirement],
    [key, secret, session, process]: [&str; 4],
) -> Result<R2ObjectStorageConfig, PublishError> {
    if let Some(unmet) = first_unmet(requirements, source) {
        return Err(PublishError::Infrastructure(unmet_message(unmet)));
    }
    // Every name read below is met above, so a missing one is a declaration that lost a name.
    let held = |name: &str| {
        source.get(name).ok_or_else(|| {
            PublishError::Infrastructure(format!("{name} environment variable is required"))
        })
    };
    let endpoint = match source.get(ENDPOINT) {
        Some(endpoint) => endpoint,
        None => format!("https://{}.r2.cloudflarestorage.com", held(ACCOUNT_ID)?),
    };
    Ok(R2ObjectStorageConfig {
        bucket_name: held(BUCKET)?,
        endpoint,
        region: source.get(REGION).unwrap_or_else(|| "auto".to_owned()),
        access_key_id: held(key)?,
        secret_access_key: held(secret)?,
        session_token: source.get(session),
        credential_process: source.get(process),
        namespace: R2KeyNamespace::from_settings(
            source.get(RUNTIME_ENVIRONMENT_ENV).as_deref(),
            source.get(STAGING_MULTIPART_THRESHOLD_ENV).as_deref(),
        )?,
    })
}

/// A missing setting in the words the journal has always had, and both names when either would do
/// (2026-11-21: a unit without the endpoint was told only that the account id was required).
fn unmet_message(unmet: &Requirement) -> String {
    match unmet {
        Requirement::Required(name) | Requirement::Optional(name) => {
            format!("{name} environment variable is required")
        }
        Requirement::OneOf(names) => format!(
            "one of the {} environment variables is required",
            names.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// A source that holds a value under every name the requirements say must be there (the first
    /// alternative of a one-of), and nothing optional.
    fn exactly(requirements: &[Requirement]) -> BTreeMap<String, String> {
        requirements
            .iter()
            .filter_map(|requirement| match requirement {
                Requirement::Required(name) => Some(*name),
                Requirement::OneOf(names) => names.first().copied(),
                Requirement::Optional(_) => None,
            })
            .map(|name| {
                (
                    name.to_owned(),
                    format!("synthetic-{}", name.to_lowercase()),
                )
            })
            .collect()
    }

    type Load = fn(&BTreeMap<String, String>) -> Result<R2ObjectStorageConfig, PublishError>;

    fn loaders() -> [(&'static [Requirement], Load); 2] {
        [
            (LAKEHOUSE_WRITER_REQUIREMENTS, |source| {
                R2ObjectStorageConfig::lakehouse_writer_from(source)
            }),
            (LAKEHOUSE_READER_REQUIREMENTS, |source| {
                R2ObjectStorageConfig::lakehouse_reader_from(source)
            }),
        ]
    }

    #[test]
    fn what_is_declared_is_enough_and_every_declared_name_is_needed() {
        for (requirements, load) in loaders() {
            let enough = exactly(requirements);
            assert!(load(&enough).is_ok(), "the declared names alone load");
            for name in enough.keys() {
                let mut without = enough.clone();
                without.remove(name);
                let refused = load(&without).err().map(|error| error.to_string());
                assert!(
                    refused
                        .as_deref()
                        .is_some_and(|message| message.contains(name.as_str())),
                    "without {name}: {refused:?}"
                );
            }
        }
    }

    #[test]
    fn either_address_will_do_and_the_refusal_names_both() -> Result<(), PublishError> {
        let mut by_account = exactly(LAKEHOUSE_WRITER_REQUIREMENTS);
        by_account.remove(ENDPOINT);
        let refused = R2ObjectStorageConfig::lakehouse_writer_from(&by_account)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            refused.contains(ENDPOINT) && refused.contains(ACCOUNT_ID),
            "{refused}"
        );
        by_account.insert(ACCOUNT_ID.to_owned(), "synthetic".to_owned());
        let config = R2ObjectStorageConfig::lakehouse_writer_from(&by_account)?;
        assert_eq!(
            config.endpoint,
            "https://synthetic.r2.cloudflarestorage.com"
        );
        assert_eq!(config.region, "auto");
        Ok(())
    }

    #[test]
    fn a_pair_takes_its_own_session_token_and_none_without_one() -> Result<(), PublishError> {
        for (requirements, load, session) in [
            (
                LAKEHOUSE_WRITER_REQUIREMENTS,
                loaders()[0].1,
                WRITER_SESSION,
            ),
            (
                LAKEHOUSE_READER_REQUIREMENTS,
                loaders()[1].1,
                READER_SESSION,
            ),
        ] {
            let mut source = exactly(requirements);
            assert_eq!(
                load(&source)?.session_token,
                None,
                "an API token's pair has none"
            );
            source.insert(session.to_owned(), "synthetic-session".to_owned());
            assert_eq!(
                load(&source)?.session_token.as_deref(),
                Some("synthetic-session"),
                "{session}"
            );
        }
        // Neither pair takes the other's token.
        let mut writer = exactly(LAKEHOUSE_WRITER_REQUIREMENTS);
        writer.insert(READER_SESSION.to_owned(), "synthetic-session".to_owned());
        assert_eq!(
            R2ObjectStorageConfig::lakehouse_writer_from(&writer)?.session_token,
            None
        );
        Ok(())
    }

    #[test]
    fn the_reader_never_takes_the_writer_pair() -> Result<(), PublishError> {
        let mut both = exactly(LAKEHOUSE_READER_REQUIREMENTS);
        both.extend(exactly(LAKEHOUSE_WRITER_REQUIREMENTS));
        let reader = R2ObjectStorageConfig::lakehouse_reader_from(&both)?;
        assert_eq!(reader.access_key_id, both[READER_KEY]);
        assert_eq!(reader.secret_access_key, both[READER_SECRET]);
        let writer = R2ObjectStorageConfig::lakehouse_writer_from(&both)?;
        assert_eq!(writer.access_key_id, both[WRITER_KEY]);
        Ok(())
    }
}
