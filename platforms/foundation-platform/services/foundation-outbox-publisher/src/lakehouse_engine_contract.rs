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
/// Panics on a malformed or below-minimum contract. That is deliberate: the contract is
/// embedded, so any failure here is a fact about the source tree rather than the environment,
/// and the tests below exercise it on every build.
pub(crate) fn iceberg_packages() -> &'static str {
    static PACKAGES: OnceLock<String> = OnceLock::new();
    PACKAGES.get_or_init(|| {
        let contract: Contract = serde_json::from_str(CONTRACT_JSON)
            .expect("lakehouse engine contract must be valid JSON");
        assert_eq!(
            contract.schema_version, CONTRACT_SCHEMA_VERSION,
            "unsupported lakehouse engine contract schema_version"
        );

        let iceberg = &contract.iceberg;
        assert!(
            version_tuple(&iceberg.version) >= version_tuple(&iceberg.minimum_version),
            "iceberg version {} is below the contract minimum {}: {}",
            iceberg.version,
            iceberg.minimum_version,
            iceberg.minimum_version_reason
        );

        iceberg
            .artifacts
            .iter()
            .map(|artifact| format!("{artifact}:{}", iceberg.version))
            .collect::<Vec<_>>()
            .join(",")
    })
}

fn version_tuple(value: &str) -> Vec<u32> {
    value
        .split('.')
        .map(|part| {
            part.parse::<u32>()
                .unwrap_or_else(|_| panic!("version part is not a number: {value}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The coordinates must carry a version, or `spark-submit` resolves whatever is newest and
    /// two runs of the same release load different Iceberg jars.
    #[test]
    fn packages_name_both_artifacts_at_a_pinned_version() {
        let packages = iceberg_packages();
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
    }

    /// The minimum exists because versions below it corrupt native memory on this deployment's
    /// larger tables. A contract that drops below it should not build.
    #[test]
    fn the_contract_pins_at_or_above_the_minimum() {
        let contract: Contract =
            serde_json::from_str(CONTRACT_JSON).expect("contract must be valid JSON");
        assert!(
            version_tuple(&contract.iceberg.version)
                >= version_tuple(&contract.iceberg.minimum_version)
        );
        assert!(
            !contract.iceberg.minimum_version_reason.is_empty(),
            "a minimum nobody can explain is a minimum nobody will keep"
        );
    }
}
