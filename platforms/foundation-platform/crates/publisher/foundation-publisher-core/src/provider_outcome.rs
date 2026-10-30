//! Whose side a failed command is on: the provider's or the release's (root ADR-0363).
//!
//! 2026-10-30 13:07Z the deploy of 59c0b68 stopped at the staging smoke because VWorld answered
//! the collection account's login `useChk` (new terms to accept). No release can accept a
//! provider's terms, yet every deploy failed and every VWorld job failed with an opaque error. A
//! command whose error carries a provider-side [`CollectionError`] exits with its own code and
//! prints one [`LINE_PREFIX`] line, so the staging smoke skips that input instead of refusing the
//! release, and the jobs page the owner with the action the provider wants
//! (`scripts/ops/provider_alert.py`, `config/provider-accounts.contract.json`). Every other
//! error keeps exit 1: an unmarked failure is the release's until shown otherwise.

use collection_domain::CollectionError;
use serde_json::json;

/// The prefix of the one line a provider outcome is reported on (stderr, the run log).
pub const LINE_PREFIX: &str = "foundation-provider-outcome ";

/// `EX_TEMPFAIL` of sysexits(3): the provider did not serve; try again later.
pub const PROVIDER_UNAVAILABLE_EXIT_CODE: i32 = 75;

/// `EX_NOPERM` of sysexits(3): the provider refuses the account until its owner acts.
pub const PROVIDER_ACCOUNT_ACTION_EXIT_CODE: i32 = 77;

/// The provider-side failure in `error`'s chain, as its outcome line and exit code.
pub fn provider_side(error: &anyhow::Error) -> Option<(String, i32)> {
    error
        .chain()
        .find_map(|cause| match cause.downcast_ref::<CollectionError>()? {
            CollectionError::ProviderAccountActionRequired {
                provider,
                reason,
                message,
            } => Some((
                line(
                    &json!({"outcome": "provider_account_action_required", "provider": provider,
                         "reason": reason, "message": message}),
                ),
                PROVIDER_ACCOUNT_ACTION_EXIT_CODE,
            )),
            CollectionError::ProviderUnavailable(message) => Some((
                line(&json!({"outcome": "provider_unavailable", "message": message})),
                PROVIDER_UNAVAILABLE_EXIT_CODE,
            )),
            _ => None,
        })
}

/// The line a successful login prints: what clears an open account alert for `provider`.
#[must_use]
pub fn login_ok_line(provider: &str) -> String {
    line(&json!({"outcome": "ok", "provider": provider}))
}

fn line(value: &serde_json::Value) -> String {
    format!("{LINE_PREFIX}{value}")
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::*;

    #[test]
    fn the_2026_10_30_terms_refusal_is_the_providers_side_under_any_context() {
        let error = anyhow::Error::from(CollectionError::ProviderAccountActionRequired {
            provider: "vworld.kr".to_owned(),
            reason: "useChk".to_owned(),
            message: "VWorld dataset login failed result=useChk".to_owned(),
        })
        .context("failed to acquire VWorld dataset login session");
        let (line, code) = provider_side(&error).expect("provider side");
        assert_eq!(code, PROVIDER_ACCOUNT_ACTION_EXIT_CODE);
        let value: serde_json::Value =
            serde_json::from_str(line.strip_prefix(LINE_PREFIX).expect("prefix")).expect("json");
        assert_eq!(value["outcome"], "provider_account_action_required");
        assert_eq!(value["provider"], "vworld.kr");
        assert_eq!(value["reason"], "useChk");
    }

    #[test]
    fn an_unavailable_provider_exits_tempfail_and_anything_else_is_the_releases() {
        let unavailable = Err::<(), _>(CollectionError::ProviderUnavailable("HTTP 503".to_owned()))
            .context("plan")
            .unwrap_err();
        assert_eq!(
            provider_side(&unavailable).map(|(_, code)| code),
            Some(PROVIDER_UNAVAILABLE_EXIT_CODE)
        );
        for ours in [
            anyhow::Error::from(CollectionError::Infrastructure("parse failed".to_owned())),
            anyhow::anyhow!("VWorld dataset login failed result=useChk"),
        ] {
            assert_eq!(
                provider_side(&ours),
                None,
                "a string is not a class: {ours}"
            );
        }
        assert_eq!(
            login_ok_line("vworld.kr"),
            r#"foundation-provider-outcome {"outcome":"ok","provider":"vworld.kr"}"#
        );
    }
}
