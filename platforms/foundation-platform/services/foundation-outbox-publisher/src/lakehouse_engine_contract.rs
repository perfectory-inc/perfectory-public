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

use anyhow::{anyhow, bail, ensure, Context};
use serde::{Deserialize, Serialize};

const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/lakehouse-engine.contract.json");
const CONTRACT_SCHEMA_VERSION: u32 = 3;

#[derive(Deserialize)]
struct Contract {
    schema_version: u32,
    iceberg: Iceberg,
    hadoop: Hadoop,
    serving_parquet: ServingParquetPolicy,
    execution_profile: ExecutionProfile,
}

/// Native process limits shared by staging and the actual container invocation.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProfile {
    /// Container memory ceiling in MiB; `DuckDB` uses half for its buffer manager.
    pub memory_mib: u32,
    /// Allocated CPU slots.
    pub cpu_slots: u32,
    /// Container process/thread ceiling.
    pub pids_limit: u32,
    /// Additional swap allowance in MiB (only zero is supported).
    pub swap_mib: u32,
}

impl ExecutionProfile {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            (2..(1_u32 << 31)).contains(&self.memory_mib)
                && (1..(1_u32 << 31)).contains(&self.cpu_slots)
                && (1..(1_u32 << 31)).contains(&self.pids_limit)
                && self.cpu_slots <= self.pids_limit
                && self.swap_mib == 0,
            "execution_profile violates the supported native physical envelope"
        );
        Ok(())
    }
}

/// 포함된 공통 엔진 계약을 검증하고 실행 profile을 읽는다.
/// # Errors
/// 계약 형식·버전·필수 조건이 잘못되었으면 실패한다.
pub fn execution_profile() -> anyhow::Result<ExecutionProfile> {
    let contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
    packages_from(&contract)?;
    Ok(contract.execution_profile)
}

/// native writer와 snapshot reader가 공유하는 Parquet 바이트 정책.
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServingParquetPolicy {
    row_group_target_bytes: i64,
    /// 압축 전후 행 그룹 각각에 허용하는 바이트 상한.
    pub max_row_group_bytes: i64,
    row_group_check_min_records: i64,
    row_group_check_max_records: i64,
}

impl ServingParquetPolicy {
    /// 유효한 행 그룹 출력 목표 크기를 반환한다.
    /// # Errors
    /// 정책 값이 잘못되었거나 크기를 usize로 표현할 수 없으면 실패한다.
    pub fn row_group_target_bytes(self) -> anyhow::Result<usize> {
        self.validate()?;
        usize::try_from(self.row_group_target_bytes)
            .context("Parquet byte target does not fit usize")
    }

    /// Shared numeric footer gate for native writers and the snapshot scanner.
    /// # Errors
    /// footer의 압축 전후 바이트 수가 음수이거나 상한을 넘으면 실패한다.
    pub fn validate_row_group(
        self,
        index: usize,
        uncompressed: i64,
        compressed: i64,
    ) -> anyhow::Result<()> {
        let maximum = self.max_row_group_bytes;
        ensure!(
            (0..=maximum).contains(&uncompressed) && (0..=maximum).contains(&compressed),
            "Parquet row group {index} exceeds scan byte bound: uncompressed={uncompressed}, compressed={compressed}, maximum={maximum}"
        );
        Ok(())
    }

    fn validate(self) -> anyhow::Result<()> {
        ensure!(
            self.row_group_target_bytes > 0
                && self.row_group_target_bytes < self.max_row_group_bytes
                && self.max_row_group_bytes <= i64::from(i32::MAX)
                && self.row_group_check_min_records > 0
                && self.row_group_check_min_records <= self.row_group_check_max_records
                && self.row_group_check_max_records <= i64::from(i32::MAX),
            "invalid serving Parquet writer/reader limits"
        );
        Ok(())
    }
}

/// The same hard footer bound used by Spark readback and the native scanner.
/// # Errors
/// 포함된 공통 엔진 계약의 파싱 또는 검증에 실패하면 오류를 반환한다.
pub fn serving_parquet_policy() -> anyhow::Result<&'static ServingParquetPolicy> {
    static POLICY: OnceLock<Result<ServingParquetPolicy, String>> = OnceLock::new();
    POLICY
        .get_or_init(|| {
            (|| -> anyhow::Result<_> {
                let contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
                packages_from(&contract)?;
                Ok(contract.serving_parquet)
            })()
            .map_err(|error| format!("{error:#}"))
        })
        .as_ref()
        .map_err(|message| anyhow!("{message}"))
}

#[derive(Deserialize)]
struct Iceberg {
    version: String,
    artifacts: Vec<String>,
    minimum_version: String,
    minimum_version_reason: String,
}

/// The Hadoop side of the submission, versioned separately because it is pinned to the Spark
/// image's own Hadoop rather than to Iceberg.
#[derive(Deserialize)]
struct Hadoop {
    version: String,
    artifacts: Vec<String>,
}

/// Comma-joined Maven coordinates for `spark-submit --packages`.
///
/// # Errors
/// Returns an error when the embedded contract is malformed, carries an unsupported schema
/// version, or pins an Iceberg below the minimum it declares. Every caller resolves this
/// while it is still building a command, so a corrupt contract stops the run before an
/// argument list is assembled rather than after a submission is already in flight.
pub fn iceberg_packages() -> anyhow::Result<&'static str> {
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
    packages_from(&contract)
}

/// Split from the embedded read so a test can hand it a contract the repository does not
/// contain. A test that only compares two version numbers proves `version_tuple` works; it
/// does not prove anything refuses to submit.
fn packages_from(contract: &Contract) -> anyhow::Result<String> {
    if contract.schema_version != CONTRACT_SCHEMA_VERSION {
        bail!(
            "unsupported lakehouse engine contract schema_version {}, expected {}",
            contract.schema_version,
            CONTRACT_SCHEMA_VERSION
        );
    }
    contract.serving_parquet.validate()?;
    contract.execution_profile.validate()?;

    let iceberg = &contract.iceberg;
    if version_tuple(&iceberg.version)? < version_tuple(&iceberg.minimum_version)? {
        bail!(
            "iceberg version {} is below the contract minimum {}: {}",
            iceberg.version,
            iceberg.minimum_version,
            iceberg.minimum_version_reason
        );
    }

    // Both blocks, always. Iceberg's bundle backs its own storage layer and Hadoop's backs the
    // `s3a://` filesystem a job reads a handoff object through; submitting only the block a
    // given job seems to need would put that judgement in every submitter.
    let hadoop = &contract.hadoop;
    let coordinates = iceberg
        .artifacts
        .iter()
        .map(|artifact| format!("{artifact}:{}", iceberg.version))
        .chain(
            hadoop
                .artifacts
                .iter()
                .map(|artifact| format!("{artifact}:{}", hadoop.version)),
        )
        .collect::<Vec<_>>();

    Ok(coordinates.join(","))
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

        // Counted rather than listed: the count is what a submission that quietly dropped a
        // block would change, and listing the names here would restate the contract.
        assert!(
            coordinates.len() >= 3,
            "the Iceberg runtime, its aws bundle, and the s3a filesystem are all required: {packages}"
        );
        assert!(
            packages.contains("hadoop-aws"),
            "without it `spark.read` cannot open an s3a object: {packages}"
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
    fn a_version_below_the_minimum_is_rejected() -> anyhow::Result<()> {
        let mut contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
        contract.iceberg.version = "1.6.1".into();

        let refused = packages_from(&contract);

        assert!(
            refused.is_err(),
            "a contract below its own minimum must not produce a submission: {refused:?}"
        );
        assert!(
            format!("{:#}", refused.err().context("old version must fail")?).contains("1.6.1"),
            "the error must name the version it refused"
        );
        Ok(())
    }

    /// A contract from a newer schema must be refused rather than read for the parts this
    /// build happens to recognise. Adding the Hadoop block bumped the version for exactly
    /// this reason: a reader left behind would have submitted without the s3a filesystem and
    /// the job would have failed on its own input.
    #[test]
    fn a_contract_from_another_schema_version_is_refused() -> anyhow::Result<()> {
        let mut contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
        for other in [CONTRACT_SCHEMA_VERSION - 1, CONTRACT_SCHEMA_VERSION + 1] {
            contract.schema_version = other;
            assert!(packages_from(&contract).is_err());
        }
        Ok(())
    }

    #[test]
    fn serving_parquet_policy_is_required_and_invalid_limits_block_submission() -> anyhow::Result<()>
    {
        let mut value: serde_json::Value = serde_json::from_str(CONTRACT_JSON)?;
        value
            .as_object_mut()
            .context("contract object")?
            .remove("serving_parquet");
        assert!(serde_json::from_value::<Contract>(value).is_err());
        let valid = *serving_parquet_policy()?;
        for policy in [
            ServingParquetPolicy {
                row_group_target_bytes: 0,
                ..valid
            },
            ServingParquetPolicy {
                row_group_target_bytes: valid.max_row_group_bytes,
                ..valid
            },
            ServingParquetPolicy {
                max_row_group_bytes: i64::MAX,
                ..valid
            },
            ServingParquetPolicy {
                row_group_check_min_records: 0,
                ..valid
            },
            ServingParquetPolicy {
                row_group_check_max_records: 0,
                ..valid
            },
        ] {
            let mut contract: Contract = serde_json::from_str(CONTRACT_JSON)?;
            contract.serving_parquet = policy;
            assert!(packages_from(&contract).is_err());
        }
        Ok(())
    }

    /// A version part that is not a number must become an error rather than a zero, which
    /// would compare below every minimum and silently disable the check above.
    #[test]
    fn a_non_numeric_version_part_is_an_error() {
        assert!(version_tuple("1.8.0-rc1").is_err());
    }

    #[test]
    fn footer_gate_checks_both_dimensions_and_inclusive_bounds() -> anyhow::Result<()> {
        let policy = *serving_parquet_policy()?;
        let maximum = policy.max_row_group_bytes;
        policy.validate_row_group(0, 0, 0)?;
        policy.validate_row_group(0, maximum, maximum)?;
        for (uncompressed, compressed) in [(-1, 0), (0, -1), (maximum + 1, 0), (0, maximum + 1)] {
            assert!(policy
                .validate_row_group(0, uncompressed, compressed)
                .is_err());
        }
        Ok(())
    }
    #[test]
    fn native_profile_is_required_bounded_and_has_no_legacy_fields() -> anyhow::Result<()> {
        let value: serde_json::Value = serde_json::from_str(CONTRACT_JSON)?;
        for (field, invalid) in [
            ("memory_mib", serde_json::json!(0)),
            ("memory_mib", serde_json::json!(1)),
            ("memory_mib", serde_json::json!(1_u64 << 31)),
            ("cpu_slots", serde_json::json!(0)),
            ("pids_limit", serde_json::json!(0)),
            ("swap_mib", serde_json::json!(1)),
            ("memory_mib", serde_json::json!(true)),
            ("memory_mib", serde_json::json!("4096")),
            ("memory_mib", serde_json::json!(-1)),
            ("memory_mib", serde_json::json!(2.5)),
            ("admission_gid", serde_json::json!(1)),
        ] {
            let mut invalid_contract = value.clone();
            invalid_contract["execution_profile"][field] = invalid;
            let parsed = serde_json::from_value::<Contract>(invalid_contract);
            assert!(
                parsed.map_or(true, |contract| packages_from(&contract).is_err()),
                "accepted {field}"
            );
        }
        let mut missing = value.clone();
        missing
            .as_object_mut()
            .context("contract object")?
            .remove("execution_profile");
        assert!(serde_json::from_value::<Contract>(missing).is_err());
        let mut inverted: Contract = serde_json::from_value(value)?;
        inverted.execution_profile.pids_limit = inverted.execution_profile.cpu_slots - 1;
        assert!(packages_from(&inverted).is_err());
        Ok(())
    }
}
