//! A run refused for its inputs, which a retry with the same inputs refuses again.
//!
//! The bakes retry a failed export (`scripts/ops/by-pnu-serving-bake.sh`, `by-pnu-pack-bake.sh`)
//! because R2 and the catalog fail transiently. A refusal of the inputs is not transient: on
//! 2026-10-30 the unit-price lane refused its change set the same way six times, 45 seconds apart,
//! twice an hour. An error carrying [`Refused`] makes the publisher exit [`REFUSED_EXIT_CODE`], and
//! the bakes stop at that code instead of retrying. Every other error keeps exit 1 and its retries,
//! so an unmarked transient failure is never cut short (the retry classifiers of the AWS SDKs and
//! Kubernetes' `podFailurePolicy` `FailJob` on exit codes draw the same line).

use std::fmt;

/// `EX_DATAERR` of sysexits(3): the input data was incorrect. The bakes already exit with it for
/// their own refusals.
pub const REFUSED_EXIT_CODE: i32 = 65;

/// A deterministic refusal; `ensure!(cond, Refused(format!(..)))` keeps its type in the chain.
#[derive(Debug)]
pub struct Refused(pub String);

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// Whether `error`, or any error it wraps, is a [`Refused`].
pub fn is_refused(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<Refused>())
}

#[cfg(test)]
mod tests {
    use anyhow::{ensure, Context as _};

    use super::*;

    fn refuse() -> anyhow::Result<()> {
        ensure!(
            false,
            Refused("the change set is not of this snapshot".to_owned())
        );
        Ok(())
    }

    #[test]
    fn a_refusal_is_found_under_context_and_nothing_else_is_one() {
        let wrapped = refuse().context("shard all").unwrap_err();
        assert!(is_refused(&wrapped));
        assert_eq!(
            format!("{:#}", wrapped),
            "shard all: the change set is not of this snapshot"
        );
        assert!(!is_refused(&anyhow::anyhow!("R2 answered 503")));
    }
}
