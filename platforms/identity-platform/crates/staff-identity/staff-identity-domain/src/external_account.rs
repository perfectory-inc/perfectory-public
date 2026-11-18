//! A source's account tied to a staff member for a validity interval (root ADR-0569).

use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;

/// Source whose accounts can be tied to a staff member.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExternalAccountProvider {
    /// A GitHub user, by its numeric user id.
    Github,
    /// A host login, by `<uid>@<host>`.
    Unix,
}

impl ExternalAccountProvider {
    /// Wire value of the provider.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Github => "github",
            Self::Unix => "unix",
        }
    }
}

/// Rejected external account input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalAccountError {
    /// The provider is not `github` or `unix`.
    UnknownProvider,
    /// The id is not the provider's immutable id form.
    MalformedId,
}

impl Display for ExternalAccountError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnknownProvider => "external account provider must be github or unix",
            Self::MalformedId => "external account id is not the provider's immutable id",
        })
    }
}

impl std::error::Error for ExternalAccountError {}

/// A source's immutable account id. A login name is renamed and reused, so it is never one.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExternalAccount {
    provider: ExternalAccountProvider,
    external_id: String,
}

impl ExternalAccount {
    /// Parses a provider and its immutable id: `github` takes the numeric user id, `unix` takes
    /// `<uid>@<host>` with a lowercase host name.
    ///
    /// # Errors
    /// Returns [`ExternalAccountError`] for any other provider or id form.
    pub fn parse(provider: &str, external_id: &str) -> Result<Self, ExternalAccountError> {
        let provider = match provider {
            "github" => ExternalAccountProvider::Github,
            "unix" => ExternalAccountProvider::Unix,
            _ => return Err(ExternalAccountError::UnknownProvider),
        };
        let valid = match provider {
            ExternalAccountProvider::Github => is_github_user_id(external_id),
            ExternalAccountProvider::Unix => is_unix_account(external_id),
        };
        if !valid {
            return Err(ExternalAccountError::MalformedId);
        }
        Ok(Self {
            provider,
            external_id: external_id.to_owned(),
        })
    }

    /// Source of the account.
    #[must_use]
    pub const fn provider(&self) -> ExternalAccountProvider {
        self.provider
    }

    /// The source's immutable id, without the provider prefix.
    #[must_use]
    pub fn external_id(&self) -> &str {
        &self.external_id
    }

    /// The actor form an audit event carries: `github:<id>` or `unix:<uid>@<host>`.
    #[must_use]
    pub fn actor_external(&self) -> String {
        format!("{}:{}", self.provider.as_str(), self.external_id)
    }
}

fn is_github_user_id(value: &str) -> bool {
    (1..=20).contains(&value.len())
        && !value.starts_with('0')
        && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_unix_account(value: &str) -> bool {
    let Some((uid, host)) = value.split_once('@') else {
        return false;
    };
    (1..=10).contains(&uid.len())
        && uid.bytes().all(|byte| byte.is_ascii_digit())
        && (1..=63).contains(&host.len())
        && host
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && host
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// One record of a link. The latest record of `(staff_id, account, valid_from)` is its state:
/// `valid_to` is empty while the link holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalAccountLink {
    /// Staff member the account belongs to.
    pub staff_id: StaffId,
    /// The source's account.
    pub account: ExternalAccount,
    /// When the link starts to hold.
    pub valid_from: DateTime<Utc>,
    /// When the link stopped holding, if it did.
    pub valid_to: Option<DateTime<Utc>>,
    /// Staff member who recorded this record.
    pub linked_by: StaffId,
}

#[cfg(test)]
mod tests {
    use super::{ExternalAccount, ExternalAccountError};

    #[test]
    fn only_a_sources_immutable_id_is_an_account() {
        let github = ExternalAccount::parse("github", "583231");
        assert_eq!(
            github.map(|account| account.actor_external()),
            Ok("github:583231".to_owned())
        );
        let unix = ExternalAccount::parse("unix", "1000@ai-server");
        assert_eq!(
            unix.map(|account| account.actor_external()),
            Ok("unix:1000@ai-server".to_owned())
        );
        for (provider, id) in [
            ("github", "octocat"),
            ("github", "0123"),
            ("github", ""),
            ("unix", "1000"),
            ("unix", "admin@ai-server"),
            ("unix", "1000@AI-Server"),
            ("unix", "1000@-host"),
        ] {
            assert_eq!(
                ExternalAccount::parse(provider, id),
                Err(ExternalAccountError::MalformedId),
                "{provider}:{id}"
            );
        }
        assert_eq!(
            ExternalAccount::parse("gitlab", "1"),
            Err(ExternalAccountError::UnknownProvider)
        );
    }
}
