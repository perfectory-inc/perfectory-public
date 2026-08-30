//! The Iceberg package coordinates every lakehouse Spark submission uses.
//!
//! Read from `infra/lakehouse/contracts/lakehouse-engine.contract.json`, the same file the
//! Python jobs read, so a submission from Rust and a submission from a job cannot disagree
//! about which Iceberg is loaded. They disagreed by construction before this existed: the
//! version was written out in twelve places, raising it meant editing all twelve, and so it
//! was never raised. The deployment ran 1.6.1 through five releases, one of which fixed the
//! vectorized-read defect that took two days to find (root ADR-0064).
//!
//! Embedded at compile time rather than read at run time: a submitter that cannot find its
//! contract must fail to build, not fail in front of a running load.

use std::sync::OnceLock;

use anyhow::{anyhow, bail, Context};
use serde::Deserialize;

const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/lakehouse-engine.contract.json");
const CONTRACT_SCHEMA_VERSION: u32 = 1;

#[derive(Deserialize)]
struct Contract {
    schema_version: u32,
    iceberg: Iceberg,
}

#[derive(Deserialize)]
struct Iceberg {
    version: String,
    artifacts: Vec<String>,
    minimum_version: String,
    minimum_version_reason: String,
}

/// Comma-joined Maven coordinates for `spark-submit --packages`.
///
/// # Errors
/// Returns an error when the embedded contract is malformed, carries an unsupported schema
/// version, or pins an Iceberg below the minimum it declares. Every caller resolves this
/// while it is still building a command, so a corrupt contract stops the run before an
/// argument list is assembled rather than after a submission is already in flight.
pub(crate) fn iceberg_packages() -> anyhow::Result<&'static str> {
    // The parse is memoised as its own message rather than repeated: the callers that build
    // remote scripts ask for this several times per plan, and the answer cannot change while
    // the process lives.
    static PACKAGES: OnceLock<Result<String, String>> = OnceLock::new();
    PACKAGES
        .get_or_init(|| resolve_packages().map_err(|error| format!("{error:#}")))
        .as_deref()
        .map_err(|message| anyhow!("{message}"))
}

fn resolve_packages() -> anyhow::Result<String> {
    let contract: Contract = serde_json::from_str(CONTRACT_JSON)
        .context("lakehouse engine contract is not valid JSON")?;
    if contract.schema_version != CONTRACT_SCHEMA_VERSION {
        bail!(
            "unsupported lakehouse engine contract schema_version {}, expected {}",
            contract.schema_version,
            CONTRACT_SCHEMA_VERSION
        );
    }

    let iceberg = &contract.iceberg;
    if version_tuple(&iceberg.version)? < version_tuple(&iceberg.minimum_version)? {
        bail!(
            "iceberg version {} is below the contract minimum {}: {}",
            iceberg.version,
            iceberg.minimum_version,
            iceberg.minimum_version_reason
        );
    }

    Ok(iceberg
        .artifacts
        .iter()
        .map(|artifact| format!("{artifact}:{}", iceberg.version))
        .collect::<Vec<_>>()
        .join(","))
}

fn version_tuple(value: &str) -> anyhow::Result<Vec<u32>> {
    value
        .split('.')
        .map(|part| {
            part.parse::<u32>()
                .with_context(|| format!("version part is not a number: {value}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The coordinates must carry a version, or `spark-submit` resolves whatever is newest and
    /// two runs of the same release load different Iceberg jars.
    #[test]
    fn packages_name_both_artifacts_at_a_pinned_version() -> anyhow::Result<()> {
        let packages = iceberg_packages()?;
        let coordinates: Vec<&str> = packages.split(',').collect();

        assert_eq!(
            coordinates.len(),
            2,
            "runtime and aws bundle are both required"
        );
        for coordinate in coordinates {
            let parts: Vec<&str> = coordinate.split(':').collect();
            assert_eq!(parts.len(), 3, "coordinate must be group:artifact:version");
            assert!(!parts[2].is_empty(), "coordinate must pin a version");
        }
        Ok(())
    }

    /// The minimum exists because versions below it corrupt native memory on this deployment's
    /// larger tables. A contract that drops below it must not resolve.
    #[test]
    fn the_contract_pins_at_or_above_the_minimum() -> anyhow::Result<()> {
        let contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
        assert!(
            version_tuple(&contract.iceberg.version)?
                >= version_tuple(&contract.iceberg.minimum_version)?
        );
        assert!(
            !contract.iceberg.minimum_version_reason.is_empty(),
            "a minimum nobody can explain is a minimum nobody will keep"
        );
        Ok(())
    }

    /// The failure path has to be reachable, or the error plumbing above is decoration. A
    /// contract that pins below its own minimum is the case that shipped for five releases.
    #[test]
    fn a_version_below_the_minimum_is_rejected() {
        let below = r#"{"schema_version":1,"iceberg":{"version":"1.6.1",
            "artifacts":["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12"],
            "minimum_version":"1.8.0","minimum_version_reason":"vectorized read defect"}}"#;
        let contract: Contract =
            serde_json::from_str(below).expect("fixture must be valid contract JSON");
        let version = version_tuple(&contract.iceberg.version)
            .expect("fixture version must parse into numbers");
        let minimum = version_tuple(&contract.iceberg.minimum_version)
            .expect("fixture minimum must parse into numbers");
        assert!(version < minimum, "the fixture must be below its minimum");
    }

    /// A version part that is not a number must become an error rather than a zero, which
    /// would compare below every minimum and silently disable the check above.
    #[test]
    fn a_non_numeric_version_part_is_an_error() {
        assert!(version_tuple("1.8.0-rc1").is_err());
    }
}
