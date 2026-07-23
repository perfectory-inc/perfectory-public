use std::{env, process::Command, sync::Arc};

use anyhow::{bail, Context};
use chrono::NaiveDate;
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::{RecordLakehouseBatchRun, RecordLakehouseBatchRunInput};
use lakehouse_domain::{industrial_complex_lakehouse_contract_by_table_name, SparkRunSummary};
use lakehouse_infrastructure::PgLakehouseBatchRunAudit;
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use uuid::Uuid;

const SUMMARY_BEGIN_MARKER: &str = "__FOUNDATION_PLATFORM_SPARK_SUMMARY_BEGIN__";
const SUMMARY_END_MARKER: &str = "__FOUNDATION_PLATFORM_SPARK_SUMMARY_END__";
const LAKEHOUSE_COMPOSE_COMMAND: &str = "docker compose -f compose.lakehouse.yml";
const BUILDING_REGISTER_SNAPSHOT_DATE_ENV: &str =
    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_SNAPSHOT_DATE";
const BUILDING_REGISTER_UNIT_SOURCE_OBJECT_ENV: &str =
    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_SOURCE_OBJECT";
const BUILDING_REGISTER_TITLE_SOURCE_OBJECT_ENV: &str =
    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT";
const BUILDING_REGISTER_UNIT_AREA_SOURCE_OBJECT_ENV: &str =
    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_UNIT_AREA_SOURCE_OBJECT";

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteLakehouseJobConfig {
    ssh_target: String,
    remote_root: String,
    env_file: String,
    ssh_path: String,
    execute: bool,
    job: RemoteLakehouseJob,
    input_path_override: Option<String>,
    input_file_batch_size_override: Option<u32>,
    source_snapshot: BuildingRegisterSourceSnapshotConfig,
    audit: RemoteLakehouseAuditConfig,
}

impl RemoteLakehouseJobConfig {
    fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let job = RemoteLakehouseJob::parse(
            optional_lookup(&mut lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB")
                .unwrap_or_else(|| "building_register_floors_smoke".to_owned())
                .as_str(),
        )?;
        let source_snapshot = BuildingRegisterSourceSnapshotConfig::from_lookup(job, &mut lookup)?;
        Ok(Self {
            ssh_target: required_lookup(
                &mut lookup,
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET",
            )?,
            remote_root: required_lookup(&mut lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT")?,
            env_file: optional_lookup(&mut lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ENV_FILE")
                .unwrap_or_else(|| ".env.lakehouse".to_owned()),
            ssh_path: optional_lookup(&mut lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_PATH")
                .unwrap_or_else(|| "ssh".to_owned()),
            execute: optional_bool_lookup(
                &mut lookup,
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_EXECUTE",
            )?
            .unwrap_or(false),
            job,
            input_path_override: optional_lookup(
                &mut lookup,
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_INPUT_PATH",
            ),
            input_file_batch_size_override: optional_u32_lookup(
                &mut lookup,
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_INPUT_FILE_BATCH_SIZE",
            )?,
            source_snapshot,
            audit: RemoteLakehouseAuditConfig::from_lookup(&mut lookup)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BuildingRegisterSourceSnapshotConfig {
    NotRequired,
    Unit(BuildingRegisterUnitSourceSnapshot),
    UnitArea(BuildingRegisterUnitAreaSourceSnapshot),
}

impl BuildingRegisterSourceSnapshotConfig {
    fn from_lookup(
        job: RemoteLakehouseJob,
        lookup: &mut impl FnMut(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        match job {
            RemoteLakehouseJob::UnitPipelineSmoke | RemoteLakehouseJob::UnitPipelineFull => {
                let metadata = BuildingRegisterSnapshotMetadata::from_lookup(lookup)?;
                Ok(Self::Unit(BuildingRegisterUnitSourceSnapshot {
                    source_object: required_snapshot_object(
                        lookup,
                        BUILDING_REGISTER_UNIT_SOURCE_OBJECT_ENV,
                        &metadata,
                    )?,
                    title_source_object: required_snapshot_object(
                        lookup,
                        BUILDING_REGISTER_TITLE_SOURCE_OBJECT_ENV,
                        &metadata,
                    )?,
                    metadata,
                }))
            }
            RemoteLakehouseJob::UnitAreaPipelineSmoke
            | RemoteLakehouseJob::UnitAreaPipelineFull => {
                let metadata = BuildingRegisterSnapshotMetadata::from_lookup(lookup)?;
                Ok(Self::UnitArea(BuildingRegisterUnitAreaSourceSnapshot {
                    source_object: required_snapshot_object(
                        lookup,
                        BUILDING_REGISTER_UNIT_AREA_SOURCE_OBJECT_ENV,
                        &metadata,
                    )?,
                    metadata,
                }))
            }
            _ => Ok(Self::NotRequired),
        }
    }

    fn unit(&self) -> &BuildingRegisterUnitSourceSnapshot {
        match self {
            Self::Unit(source) => source,
            Self::NotRequired | Self::UnitArea(_) => {
                unreachable!("unit pipeline config must carry unit source metadata")
            }
        }
    }

    fn unit_area(&self) -> &BuildingRegisterUnitAreaSourceSnapshot {
        match self {
            Self::UnitArea(source) => source,
            Self::NotRequired | Self::Unit(_) => {
                unreachable!("unit-area pipeline config must carry unit-area source metadata")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingRegisterUnitSourceSnapshot {
    source_object: String,
    title_source_object: String,
    metadata: BuildingRegisterSnapshotMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingRegisterUnitAreaSourceSnapshot {
    source_object: String,
    metadata: BuildingRegisterSnapshotMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingRegisterSnapshotMetadata {
    date: NaiveDate,
}

impl BuildingRegisterSnapshotMetadata {
    fn from_lookup(lookup: &mut impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let raw = required_lookup(lookup, BUILDING_REGISTER_SNAPSHOT_DATE_ENV)?;
        let date = NaiveDate::parse_from_str(&raw, "%Y-%m-%d").with_context(|| {
            format!("{BUILDING_REGISTER_SNAPSHOT_DATE_ENV} must be a valid YYYY-MM-DD date")
        })?;
        if date.format("%Y-%m-%d").to_string() != raw {
            bail!("{BUILDING_REGISTER_SNAPSHOT_DATE_ENV} must use canonical YYYY-MM-DD format");
        }
        Ok(Self { date })
    }

    fn compact_date(&self) -> String {
        self.date.format("%Y%m%d").to_string()
    }

    fn valid_from_utc(&self) -> String {
        format!("{}T00:00:00Z", self.date)
    }
}

fn required_snapshot_object(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    metadata: &BuildingRegisterSnapshotMetadata,
) -> anyhow::Result<String> {
    let source_object = required_lookup(lookup, name)?;
    if source_object.contains('/') || source_object.contains('\\') {
        bail!("{name} must be a Bronze object file name, not a path");
    }
    if !source_object.ends_with(".zip")
        || !source_object
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
    {
        bail!("{name} must be a safe .zip object file name");
    }
    let expected_prefix = format!("OPN{}", metadata.compact_date());
    if !source_object.starts_with(&expected_prefix) {
        bail!(
            "{name} must embed snapshot date {} after the OPN prefix",
            metadata.compact_date()
        );
    }
    Ok(source_object)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoteLakehouseAuditConfig {
    Disabled,
    Record {
        recorded_by_staff_id: StaffId,
        request_id: Option<String>,
    },
}

impl RemoteLakehouseAuditConfig {
    fn from_lookup(lookup: &mut impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let enabled =
            optional_bool_lookup(lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT")?
                .unwrap_or(false);
        if !enabled {
            return Ok(Self::Disabled);
        }

        let staff_id = required_lookup(
            lookup,
            "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID",
        )?;
        let staff_uuid = Uuid::parse_str(&staff_id).with_context(|| {
            "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID must be a UUID"
        })?;
        if staff_uuid.is_nil() {
            bail!("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID must not be nil");
        }

        Ok(Self::Record {
            recorded_by_staff_id: StaffId::new(staff_uuid),
            request_id: optional_lookup(lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_REQUEST_ID"),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoteLakehouseJob {
    Smoke,
    HandoffSmoke,
    PipelineSmoke,
    PipelineHubSmoke,
    PipelineFull,
    UnitPipelineSmoke,
    UnitPipelineFull,
    UnitProposalContextFull,
    UnitAreaPipelineSmoke,
    UnitAreaPipelineFull,
    UnitFloorResolutionsFull,
}

impl RemoteLakehouseJob {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw.trim() {
            "building_register_floors_smoke" => Ok(Self::Smoke),
            "building_register_floors_handoff_smoke" => Ok(Self::HandoffSmoke),
            "building_register_floors_pipeline_smoke" => Ok(Self::PipelineSmoke),
            "building_register_floors_pipeline_hub_smoke" => Ok(Self::PipelineHubSmoke),
            "building_register_floors_pipeline_full" => Ok(Self::PipelineFull),
            "building_register_units_pipeline_smoke" => Ok(Self::UnitPipelineSmoke),
            "building_register_units_pipeline_full" => Ok(Self::UnitPipelineFull),
            "building_register_units_proposals_full" => Ok(Self::UnitProposalContextFull),
            "building_register_unit_areas_pipeline_smoke" => Ok(Self::UnitAreaPipelineSmoke),
            "building_register_unit_areas_pipeline_full" => Ok(Self::UnitAreaPipelineFull),
            "building_register_unit_floor_resolutions_full" => Ok(Self::UnitFloorResolutionsFull),
            other => bail!("unknown remote lakehouse job: {other}"),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "building_register_floors_smoke",
            Self::HandoffSmoke => "building_register_floors_handoff_smoke",
            Self::PipelineSmoke => "building_register_floors_pipeline_smoke",
            Self::PipelineHubSmoke => "building_register_floors_pipeline_hub_smoke",
            Self::PipelineFull => "building_register_floors_pipeline_full",
            Self::UnitPipelineSmoke => "building_register_units_pipeline_smoke",
            Self::UnitPipelineFull => "building_register_units_pipeline_full",
            Self::UnitProposalContextFull => "building_register_units_proposals_full",
            Self::UnitAreaPipelineSmoke => "building_register_unit_areas_pipeline_smoke",
            Self::UnitAreaPipelineFull => "building_register_unit_areas_pipeline_full",
            Self::UnitFloorResolutionsFull => "building_register_unit_floor_resolutions_full",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteCommandPlan {
    program: String,
    args: Vec<String>,
    remote_script: String,
}

pub async fn run() -> anyhow::Result<()> {
    let config = RemoteLakehouseJobConfig::from_env()?;
    let plan = build_remote_lakehouse_job_plan(&config);
    if !config.execute {
        println!(
            "remote-lakehouse-job-plan-ok job={} execute=false ssh_target={} remote_root={}",
            config.job.as_str(),
            config.ssh_target,
            config.remote_root
        );
        println!("remote_command={}", plan.remote_command());
        return Ok(());
    }

    let output = Command::new(&plan.program)
        .args(&plan.args)
        .output()
        .with_context(|| format!("failed to run {}", plan.program))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "remote lakehouse job failed job={} status={} stderr={}",
            config.job.as_str(),
            output.status,
            outbound_http_infrastructure::redact_url_query_secrets(&stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let summary_json = extract_marked_summary_json(&stdout)?;
    if config.job == RemoteLakehouseJob::UnitProposalContextFull {
        if !matches!(config.audit, RemoteLakehouseAuditConfig::Disabled) {
            bail!("lakehouse batch audit is not supported for AI proposal input artifacts");
        }
        let proposal_count = validate_unit_proposal_context_summary(&summary_json)?;
        println!(
            "remote-lakehouse-job-ok job={} contract=ai_proposal_input row_count={} persisted_row_count=0",
            config.job.as_str(),
            proposal_count
        );
        return Ok(());
    }
    if config.job == RemoteLakehouseJob::UnitFloorResolutionsFull {
        if !matches!(config.audit, RemoteLakehouseAuditConfig::Disabled) {
            bail!("lakehouse batch audit is not supported for floor resolution artifacts");
        }
        let (conflict_rows, resolved) = validate_unit_floor_resolution_summary(&summary_json)?;
        println!(
            "remote-lakehouse-job-ok job={} contract=floor_resolution_table row_count={} resolved_count={} persisted_row_count=0",
            config.job.as_str(),
            conflict_rows,
            resolved
        );
        return Ok(());
    }
    let summary = validate_spark_summary(&summary_json)?;
    let audit_recorded = record_lakehouse_batch_run_if_enabled(summary_json, &config.audit).await?;
    println!(
        "remote-lakehouse-job-ok job={} contract={} row_count={} persisted_row_count={}",
        config.job.as_str(),
        summary.contract,
        summary.row_count,
        summary.persisted_row_count.unwrap_or(0)
    );
    if audit_recorded {
        println!(
            "remote-lakehouse-job-audit-recorded job={} contract={}",
            config.job.as_str(),
            summary.contract
        );
    }
    Ok(())
}

impl RemoteCommandPlan {
    fn remote_command(&self) -> String {
        let mut parts = vec![self.program.clone()];
        parts.extend(self.args.iter().map(|arg| shell_quote(arg)));
        parts.join(" ")
    }
}

fn build_remote_lakehouse_job_plan(config: &RemoteLakehouseJobConfig) -> RemoteCommandPlan {
    let remote_script = build_remote_script(config);
    let remote_command = format!("bash -lc {}", shell_quote(&remote_script));
    RemoteCommandPlan {
        program: config.ssh_path.clone(),
        args: vec![config.ssh_target.clone(), remote_command],
        remote_script,
    }
}

fn build_remote_script(config: &RemoteLakehouseJobConfig) -> String {
    match config.job {
        RemoteLakehouseJob::Smoke | RemoteLakehouseJob::HandoffSmoke => {
            build_silver_scalar_remote_script(config, config.job.silver_scalar_spec())
        }
        RemoteLakehouseJob::PipelineSmoke => build_building_register_floor_pipeline_script(
            config,
            BuildingRegisterFloorPipelineSpec::smoke(),
        ),
        RemoteLakehouseJob::PipelineHubSmoke => build_building_register_floor_pipeline_script(
            config,
            BuildingRegisterFloorPipelineSpec::hub_smoke(),
        ),
        RemoteLakehouseJob::PipelineFull => build_building_register_floor_pipeline_script(
            config,
            BuildingRegisterFloorPipelineSpec::full(),
        ),
        RemoteLakehouseJob::UnitPipelineSmoke => build_building_register_unit_pipeline_script(
            config,
            BuildingRegisterUnitPipelineSpec::smoke(),
            config.source_snapshot.unit(),
        ),
        RemoteLakehouseJob::UnitPipelineFull => build_building_register_unit_pipeline_script(
            config,
            BuildingRegisterUnitPipelineSpec::full(),
            config.source_snapshot.unit(),
        ),
        RemoteLakehouseJob::UnitProposalContextFull => {
            build_building_register_unit_proposal_context_script(config)
        }
        RemoteLakehouseJob::UnitAreaPipelineSmoke => {
            build_building_register_unit_area_pipeline_script(
                config,
                BuildingRegisterUnitAreaPipelineSpec::smoke(),
                config.source_snapshot.unit_area(),
            )
        }
        RemoteLakehouseJob::UnitAreaPipelineFull => {
            build_building_register_unit_area_pipeline_script(
                config,
                BuildingRegisterUnitAreaPipelineSpec::full(),
                config.source_snapshot.unit_area(),
            )
        }
        RemoteLakehouseJob::UnitFloorResolutionsFull => {
            build_building_register_unit_floor_resolution_script(config)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SilverScalarRemoteJobSpec {
    input_path: &'static str,
    summary_path: &'static str,
    iceberg_table: &'static str,
    contract: &'static str,
    input_format: &'static str,
    spark_master: &'static str,
    spark_driver_memory: &'static str,
    spark_java_extra_options: Option<&'static str>,
    default_input_file_batch_size: u32,
    expected_count: Option<u64>,
    require_non_empty_input: bool,
    allow_non_smoke_overwrite: bool,
}

impl RemoteLakehouseJob {
    fn silver_scalar_spec(self) -> SilverScalarRemoteJobSpec {
        match self {
            Self::Smoke => SilverScalarRemoteJobSpec {
                input_path: "infra/lakehouse/spark/fixtures/silver_handoff/building_register_floors.jsonl",
                summary_path: "target/lakehouse/smoke/building_register_floors-summary.json",
                iceberg_table: "building_register_floors_smoke",
                contract: "silver.building_register_floors",
                input_format: "jsonl",
                spark_master: "local[1]",
                spark_driver_memory: "2g",
                spark_java_extra_options: Some("-Xint"),
                default_input_file_batch_size: 1,
                expected_count: Some(2),
                require_non_empty_input: false,
                allow_non_smoke_overwrite: false,
            },
            Self::HandoffSmoke => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_floors.jsonl",
                summary_path: "target/lakehouse/smoke/building_register_floors-handoff-summary.json",
                iceberg_table: "building_register_floors_smoke",
                contract: "silver.building_register_floors",
                input_format: "jsonl",
                spark_master: "local[1]",
                spark_driver_memory: "2g",
                spark_java_extra_options: Some("-Xint"),
                default_input_file_batch_size: 1,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: false,
            },
            Self::PipelineSmoke => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_floors_parquet_smoke",
                summary_path: "target/lakehouse/smoke/building_register_floors-pipeline-summary.json",
                iceberg_table: "building_register_floors_smoke",
                contract: "silver.building_register_floors",
                input_format: "parquet",
                spark_master: "local[1]",
                spark_driver_memory: "2g",
                spark_java_extra_options: Some("-Xint"),
                default_input_file_batch_size: 1,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: false,
            },
            Self::PipelineHubSmoke => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_floors_hub_parquet_smoke",
                summary_path: "target/lakehouse/smoke/building_register_floors-pipeline-hub-smoke-summary.json",
                iceberg_table: "building_register_floors_smoke",
                contract: "silver.building_register_floors",
                input_format: "parquet",
                spark_master: "local[*]",
                spark_driver_memory: "12g",
                spark_java_extra_options: None,
                default_input_file_batch_size: 128,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: false,
            },
            Self::PipelineFull => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_floors_hub_parquet",
                summary_path: "target/lakehouse/smoke/building_register_floors-pipeline-full-summary.json",
                iceberg_table: "building_register_floors",
                contract: "silver.building_register_floors",
                input_format: "parquet",
                spark_master: "local[*]",
                spark_driver_memory: "12g",
                spark_java_extra_options: None,
                default_input_file_batch_size: 128,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: true,
            },
            Self::UnitPipelineSmoke => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_units_smoke.jsonl",
                summary_path: "target/lakehouse/smoke/building_register_units-pipeline-summary.json",
                iceberg_table: "building_register_units_smoke",
                contract: "silver.building_register_units",
                input_format: "jsonl",
                spark_master: "local[1]",
                spark_driver_memory: "2g",
                spark_java_extra_options: Some("-Xint"),
                default_input_file_batch_size: 1,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: false,
            },
            Self::UnitPipelineFull => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_units_parquet",
                summary_path: "target/lakehouse/smoke/building_register_units-pipeline-full-summary.json",
                iceberg_table: "building_register_units",
                contract: "silver.building_register_units",
                input_format: "parquet",
                spark_master: "local[*]",
                spark_driver_memory: "12g",
                spark_java_extra_options: None,
                default_input_file_batch_size: 128,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: true,
            },
            Self::UnitProposalContextFull => {
                unreachable!("unit proposal context export is not a Silver scalar write")
            }
            Self::UnitFloorResolutionsFull => {
                unreachable!("unit floor resolution export is not a Silver scalar write")
            }
            Self::UnitAreaPipelineSmoke => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_unit_areas_smoke.jsonl",
                summary_path: "target/lakehouse/smoke/building_register_unit_areas-pipeline-summary.json",
                iceberg_table: "building_register_unit_areas_smoke",
                contract: "silver.building_register_unit_areas",
                input_format: "jsonl",
                spark_master: "local[1]",
                spark_driver_memory: "2g",
                spark_java_extra_options: Some("-Xint"),
                default_input_file_batch_size: 1,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: false,
            },
            Self::UnitAreaPipelineFull => SilverScalarRemoteJobSpec {
                input_path: "target/lakehouse/silver_handoff/building_register_unit_areas_parquet",
                summary_path: "target/lakehouse/smoke/building_register_unit_areas-pipeline-full-summary.json",
                iceberg_table: "building_register_unit_areas",
                contract: "silver.building_register_unit_areas",
                input_format: "parquet",
                // Bound local task concurrency and heap explicitly because this
                // high-fanout write must not inherit host-wide parallelism.
                spark_master: "local[8]",
                spark_driver_memory: "28g",
                spark_java_extra_options: None,
                default_input_file_batch_size: 128,
                expected_count: None,
                require_non_empty_input: true,
                allow_non_smoke_overwrite: true,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BuildingRegisterFloorPipelineSpec {
    source_slug: &'static str,
    output_path: &'static str,
    proposal_path: &'static str,
    export_summary_path: &'static str,
    source_snapshot_prefix: &'static str,
    output_format: &'static str,
    chunk_rows: Option<u32>,
    lakehouse_job: RemoteLakehouseJob,
}

impl BuildingRegisterFloorPipelineSpec {
    const fn smoke() -> Self {
        Self {
            source_slug: "datagokr__building_register_floor_overview",
            output_path: "target/lakehouse/silver_handoff/building_register_floors_parquet_smoke",
            proposal_path: "target/remote-lakehouse/ai/building_register_floor_proposals.jsonl",
            export_summary_path:
                "target/remote-lakehouse/summaries/building_register_floors-export-summary.json",
            source_snapshot_prefix: "remote-building-register-floor-pipeline-smoke",
            output_format: "parquet",
            chunk_rows: Some(100_000),
            lakehouse_job: RemoteLakehouseJob::PipelineSmoke,
        }
    }

    const fn hub_smoke() -> Self {
        Self {
            source_slug: "hubgokr__building_register_floor_overview",
            output_path: "target/lakehouse/silver_handoff/building_register_floors_hub_parquet_smoke",
            proposal_path: "target/remote-lakehouse/ai/building_register_floor_proposals-hub-smoke.jsonl",
            export_summary_path: "target/remote-lakehouse/summaries/building_register_floors-export-hub-smoke-summary.json",
            source_snapshot_prefix: "remote-building-register-floor-pipeline-hub-smoke",
            output_format: "parquet",
            chunk_rows: Some(250_000),
            lakehouse_job: RemoteLakehouseJob::PipelineHubSmoke,
        }
    }

    const fn full() -> Self {
        Self {
            source_slug: "hubgokr__building_register_floor_overview",
            output_path: "target/lakehouse/silver_handoff/building_register_floors_hub_parquet",
            proposal_path: "target/remote-lakehouse/ai/building_register_floor_proposals-full.jsonl",
            export_summary_path: "target/remote-lakehouse/summaries/building_register_floors-export-full-summary.json",
            source_snapshot_prefix: "remote-building-register-floor-pipeline-full",
            output_format: "parquet",
            chunk_rows: Some(250_000),
            lakehouse_job: RemoteLakehouseJob::PipelineFull,
        }
    }
}

fn build_building_register_floor_pipeline_script(
    config: &RemoteLakehouseJobConfig,
    pipeline: BuildingRegisterFloorPipelineSpec,
) -> String {
    let root = shell_quote(&config.remote_root);
    let env_file = shell_quote(&config.env_file);
    let spark_script =
        build_silver_scalar_remote_script(config, pipeline.lakehouse_job.silver_scalar_spec());
    let chunk_rows_env = pipeline.chunk_rows.map_or_else(String::new, |chunk_rows| {
        format!(
            "  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_CHUNK_ROWS='{chunk_rows}' \\\n"
        )
    });
    format!(
        "\
set -euo pipefail
cd {root}
if [ ! -f {env_file} ]; then
  echo 'missing remote lakehouse env file' >&2
  exit 2
fi
if ! test -d 'target/lakehouse/bronze/source={source_slug}'; then
  echo 'missing building-register floor Bronze source under target/lakehouse' >&2
  exit 5
fi
mkdir -p 'target/remote-lakehouse/ai' 'target/remote-lakehouse/summaries'
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-control build lakehouse-control
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-control run --rm \\
  --user \"$(id -u):$(id -g)\" \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_BRONZE_ROOT='target/lakehouse' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG='{source_slug}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_PATH='{output_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT='{output_format}' \\
{chunk_rows_env}  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_NORMALIZATION_PROPOSAL_INPUT_PATH='{proposal_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SUMMARY_PATH='{export_summary_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID=\"{source_snapshot_prefix}-$(date -u +%Y%m%dT%H%M%SZ)\" \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_VALID_FROM_UTC=\"$(date -u +%Y-%m-%dT00:00:00Z)\" \\
  lakehouse-control export-building-register-floor-silver-handoff
{spark_script}",
        source_slug = pipeline.source_slug,
        output_path = pipeline.output_path,
        output_format = pipeline.output_format,
        proposal_path = pipeline.proposal_path,
        export_summary_path = pipeline.export_summary_path,
        source_snapshot_prefix = pipeline.source_snapshot_prefix,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BuildingRegisterUnitPipelineSpec {
    source_slug: &'static str,
    title_source_slug: &'static str,
    output_path: &'static str,
    export_summary_path: &'static str,
    source_snapshot_prefix: &'static str,
    output_format: &'static str,
    chunk_rows: Option<u32>,
    max_rows: Option<u32>,
    lakehouse_job: RemoteLakehouseJob,
}

impl BuildingRegisterUnitPipelineSpec {
    const fn smoke() -> Self {
        Self {
            source_slug: "hubgokr__building_register_exclusive_unit",
            title_source_slug: "hubgokr__building_register_main",
            output_path: "target/lakehouse/silver_handoff/building_register_units_smoke.jsonl",
            export_summary_path: "target/remote-lakehouse/summaries/building_register_units-export-smoke-summary.json",
            source_snapshot_prefix: "remote-building-register-unit-pipeline-smoke",
            output_format: "jsonl",
            chunk_rows: None,
            max_rows: Some(10_000),
            lakehouse_job: RemoteLakehouseJob::UnitPipelineSmoke,
        }
    }

    const fn full() -> Self {
        Self {
            source_slug: "hubgokr__building_register_exclusive_unit",
            title_source_slug: "hubgokr__building_register_main",
            output_path: "target/lakehouse/silver_handoff/building_register_units_parquet",
            export_summary_path:
                "target/remote-lakehouse/summaries/building_register_units-export-full-summary.json",
            source_snapshot_prefix: "remote-building-register-unit-pipeline-full",
            output_format: "parquet",
            chunk_rows: Some(250_000),
            max_rows: None,
            lakehouse_job: RemoteLakehouseJob::UnitPipelineFull,
        }
    }
}

fn build_building_register_unit_pipeline_script(
    config: &RemoteLakehouseJobConfig,
    pipeline: BuildingRegisterUnitPipelineSpec,
    source: &BuildingRegisterUnitSourceSnapshot,
) -> String {
    let root = shell_quote(&config.remote_root);
    let env_file = shell_quote(&config.env_file);
    let spark_script =
        build_silver_scalar_remote_script(config, pipeline.lakehouse_job.silver_scalar_spec());
    let snapshot_date = source.metadata.compact_date();
    let valid_from_utc = source.metadata.valid_from_utc();
    let max_rows_env = pipeline.max_rows.map_or_else(String::new, |max_rows| {
        format!(
            "  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_MAX_ROWS='{max_rows}' \\\n"
        )
    });
    let chunk_rows_env = pipeline.chunk_rows.map_or_else(String::new, |chunk_rows| {
        format!(
            "  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_CHUNK_ROWS='{chunk_rows}' \\\n"
        )
    });
    format!(
        "\
set -euo pipefail
cd {root}
if [ ! -f {env_file} ]; then
  echo 'missing remote lakehouse env file' >&2
  exit 2
fi
set -a
. {env_file}
set +a
if [ -z \"${{R2_BUCKET_NAME:-}}\" ]; then
  echo 'missing R2_BUCKET_NAME in remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"${{R2_ACCESS_KEY_ID:-}}\" ]; then
  echo 'missing R2_ACCESS_KEY_ID in remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"${{R2_SECRET_ACCESS_KEY:-}}\" ]; then
  echo 'missing R2_SECRET_ACCESS_KEY in remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"${{R2_ENDPOINT:-}}\" ]; then
  if [ -z \"${{R2_ACCOUNT_ID:-}}\" ]; then
    echo 'missing R2_ENDPOINT or R2_ACCOUNT_ID in remote lakehouse env file' >&2
    exit 2
  fi
  R2_ENDPOINT=\"https://${{R2_ACCOUNT_ID}}.r2.cloudflarestorage.com\"
fi
if [ -z \"${{DATABASE_URL:-}}\" ]; then
  echo 'DATABASE_URL is required for building-register unit Silver override load' >&2
  exit 2
fi
lakehouse_uid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}\"
lakehouse_gid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\"
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/lakehouse:/lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p \\\"/lakehouse/bronze/source={source_slug}\\\" \\\"/lakehouse/bronze/source={title_source_slug}\\\" /lakehouse/silver_handoff && chown -R \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" \\\"/lakehouse/bronze/source={source_slug}\\\" \\\"/lakehouse/bronze/source={title_source_slug}\\\" && chown \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" /lakehouse /lakehouse/silver_handoff\"
stage_bronze_object() {{
  source_slug=\"$1\"
  source_object=\"$2\"
  docker run --rm \\
    --user \"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}:${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\" \\
    -e AWS_ACCESS_KEY_ID=\"${{R2_ACCESS_KEY_ID}}\" \\
    -e AWS_SECRET_ACCESS_KEY=\"${{R2_SECRET_ACCESS_KEY}}\" \\
    -e AWS_DEFAULT_REGION=\"${{R2_REGION:-auto}}\" \\
    -v \"$PWD/target/lakehouse/bronze/source=${{source_slug}}:/stage\" \\
    amazon/aws-cli:2.17.62 s3 sync \\
    \"s3://$R2_BUCKET_NAME/bronze/source=${{source_slug}}/\" \\
    /stage/ \\
    --exclude '*' \\
    --include \"${{source_object}}\" \\
    --endpoint-url \"$R2_ENDPOINT\" \\
    --only-show-errors
}}
stage_bronze_object '{source_slug}' '{source_object}'
stage_bronze_object '{title_source_slug}' '{title_source_object}'
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/remote-lakehouse:/remote-lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p /remote-lakehouse/summaries && chown -R \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" /remote-lakehouse\"
if ! test -f 'target/lakehouse/bronze/source={source_slug}/{source_object}'; then
  echo 'missing pinned building-register unit Bronze zip under target/lakehouse' >&2
  exit 5
fi
if ! test -f 'target/lakehouse/bronze/source={title_source_slug}/{title_source_object}'; then
  echo 'missing pinned building-register title Bronze zip under target/lakehouse' >&2
  exit 5
fi
mkdir -p 'target/remote-lakehouse/summaries'
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-control build lakehouse-control
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-control run --rm \\
  --user \"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}:${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\" \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_BRONZE_ROOT='target/lakehouse' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_SLUG='{source_slug}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_OBJECT='{source_object}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_TITLE_SOURCE_SLUG='{title_source_slug}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_TITLE_SOURCE_OBJECT='{title_source_object}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_OUTPUT_PATH='{output_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_OUTPUT_FORMAT='{output_format}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_APPLY_APPROVED_OVERRIDES='1' \\
  -e DATABASE_URL=\"$DATABASE_URL\" \\
{chunk_rows_env}\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SUMMARY_PATH='{export_summary_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID=\"{source_snapshot_prefix}-{snapshot_date}-$(date -u +%Y%m%dT%H%M%SZ)\" \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_VALID_FROM_UTC='{valid_from_utc}' \\
{max_rows_env}\
  lakehouse-control export-building-register-unit-silver-handoff
{spark_script}",
        source_slug = pipeline.source_slug,
        source_object = source.source_object,
        title_source_slug = pipeline.title_source_slug,
        title_source_object = source.title_source_object,
        output_path = pipeline.output_path,
        output_format = pipeline.output_format,
        chunk_rows_env = chunk_rows_env,
        export_summary_path = pipeline.export_summary_path,
        source_snapshot_prefix = pipeline.source_snapshot_prefix,
        snapshot_date = snapshot_date,
        valid_from_utc = valid_from_utc,
        max_rows_env = max_rows_env,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BuildingRegisterUnitAreaPipelineSpec {
    source_slug: &'static str,
    output_path: &'static str,
    export_summary_path: &'static str,
    source_snapshot_prefix: &'static str,
    output_format: &'static str,
    chunk_rows: Option<u32>,
    max_rows: Option<u32>,
    lakehouse_job: RemoteLakehouseJob,
}

impl BuildingRegisterUnitAreaPipelineSpec {
    const fn smoke() -> Self {
        Self {
            source_slug: "hubgokr__building_register_exclusive_common_area",
            output_path: "target/lakehouse/silver_handoff/building_register_unit_areas_smoke.jsonl",
            export_summary_path: "target/remote-lakehouse/summaries/building_register_unit_areas-export-smoke-summary.json",
            source_snapshot_prefix: "remote-building-register-unit-area-pipeline-smoke",
            output_format: "jsonl",
            chunk_rows: None,
            max_rows: Some(10_000),
            lakehouse_job: RemoteLakehouseJob::UnitAreaPipelineSmoke,
        }
    }

    const fn full() -> Self {
        Self {
            source_slug: "hubgokr__building_register_exclusive_common_area",
            output_path: "target/lakehouse/silver_handoff/building_register_unit_areas_parquet",
            export_summary_path:
                "target/remote-lakehouse/summaries/building_register_unit_areas-export-full-summary.json",
            source_snapshot_prefix: "remote-building-register-unit-area-pipeline-full",
            output_format: "parquet",
            chunk_rows: Some(250_000),
            max_rows: None,
            lakehouse_job: RemoteLakehouseJob::UnitAreaPipelineFull,
        }
    }
}

/// 전유공용면적 파이프라인 스크립트. 전유부판과 같은 골격이되, 승인 override가
/// 없어 DB 접속이 없고 (자격증명 env 파일 불요), PK 직결이라 표제부 스테이징도
/// 없다.
fn build_building_register_unit_area_pipeline_script(
    config: &RemoteLakehouseJobConfig,
    pipeline: BuildingRegisterUnitAreaPipelineSpec,
    source: &BuildingRegisterUnitAreaSourceSnapshot,
) -> String {
    let root = shell_quote(&config.remote_root);
    let env_file = shell_quote(&config.env_file);
    let spark_script =
        build_silver_scalar_remote_script(config, pipeline.lakehouse_job.silver_scalar_spec());
    let snapshot_date = source.metadata.compact_date();
    let valid_from_utc = source.metadata.valid_from_utc();
    let max_rows_env = pipeline.max_rows.map_or_else(String::new, |max_rows| {
        format!(
            "  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_MAX_ROWS='{max_rows}' \\\n"
        )
    });
    let chunk_rows_env = pipeline.chunk_rows.map_or_else(String::new, |chunk_rows| {
        format!(
            "  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_CHUNK_ROWS='{chunk_rows}' \\\n"
        )
    });
    format!(
        "\
set -euo pipefail
cd {root}
if [ ! -f {env_file} ]; then
  echo 'missing remote lakehouse env file' >&2
  exit 2
fi
set -a
. {env_file}
set +a
if [ -z \"${{R2_BUCKET_NAME:-}}\" ]; then
  echo 'missing R2_BUCKET_NAME in remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"${{R2_ACCESS_KEY_ID:-}}\" ]; then
  echo 'missing R2_ACCESS_KEY_ID in remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"${{R2_SECRET_ACCESS_KEY:-}}\" ]; then
  echo 'missing R2_SECRET_ACCESS_KEY in remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"${{R2_ENDPOINT:-}}\" ]; then
  if [ -z \"${{R2_ACCOUNT_ID:-}}\" ]; then
    echo 'missing R2_ENDPOINT or R2_ACCOUNT_ID in remote lakehouse env file' >&2
    exit 2
  fi
  R2_ENDPOINT=\"https://${{R2_ACCOUNT_ID}}.r2.cloudflarestorage.com\"
fi
lakehouse_uid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}\"
lakehouse_gid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\"
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/lakehouse:/lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p \\\"/lakehouse/bronze/source={source_slug}\\\" /lakehouse/silver_handoff && chown -R \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" \\\"/lakehouse/bronze/source={source_slug}\\\" && chown \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" /lakehouse /lakehouse/silver_handoff\"
docker run --rm \\
  --user \"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}:${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\" \\
  -e AWS_ACCESS_KEY_ID=\"${{R2_ACCESS_KEY_ID}}\" \\
  -e AWS_SECRET_ACCESS_KEY=\"${{R2_SECRET_ACCESS_KEY}}\" \\
  -e AWS_DEFAULT_REGION=\"${{R2_REGION:-auto}}\" \\
  -v \"$PWD/target/lakehouse/bronze/source={source_slug}:/stage\" \\
  amazon/aws-cli:2.17.62 s3 sync \\
  \"s3://$R2_BUCKET_NAME/bronze/source={source_slug}/\" \\
  /stage/ \\
  --exclude '*' \\
  --include '{source_object}' \\
  --endpoint-url \"$R2_ENDPOINT\" \\
  --only-show-errors
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/remote-lakehouse:/remote-lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p /remote-lakehouse/summaries && chown -R \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" /remote-lakehouse\"
if ! test -f 'target/lakehouse/bronze/source={source_slug}/{source_object}'; then
  echo 'missing pinned building-register unit-area Bronze zip under target/lakehouse' >&2
  exit 5
fi
mkdir -p 'target/remote-lakehouse/summaries'
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-control build lakehouse-control
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-control run --rm \\
  --user \"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}:${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\" \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_BRONZE_ROOT='target/lakehouse' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_SLUG='{source_slug}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_OBJECT='{source_object}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_PATH='{output_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_FORMAT='{output_format}' \\
{chunk_rows_env}\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SUMMARY_PATH='{export_summary_path}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID=\"{source_snapshot_prefix}-{snapshot_date}-$(date -u +%Y%m%dT%H%M%SZ)\" \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_VALID_FROM_UTC='{valid_from_utc}' \\
{max_rows_env}\
  lakehouse-control export-building-register-unit-area-silver-handoff
{spark_script}",
        source_slug = pipeline.source_slug,
        source_object = source.source_object,
        output_path = pipeline.output_path,
        output_format = pipeline.output_format,
        chunk_rows_env = chunk_rows_env,
        export_summary_path = pipeline.export_summary_path,
        source_snapshot_prefix = pipeline.source_snapshot_prefix,
        snapshot_date = snapshot_date,
        valid_from_utc = valid_from_utc,
        max_rows_env = max_rows_env,
    )
}

/// 층충돌 3증인 다수결 교정표 산출 스크립트. 원본 Silver 불변 — Iceberg 쓰기
/// 없음. 산출물은 서빙 조인용 별도 parquet.
fn build_building_register_unit_floor_resolution_script(
    config: &RemoteLakehouseJobConfig,
) -> String {
    let root = shell_quote(&config.remote_root);
    let env_file = shell_quote(&config.env_file);
    let units_path = "target/lakehouse/silver_handoff/building_register_units_parquet";
    let areas_path = "target/lakehouse/silver_handoff/building_register_unit_areas_parquet";
    let output_path =
        "target/remote-lakehouse/resolutions/building_register_unit_floor_resolutions";
    let summary_path =
        "target/remote-lakehouse/summaries/building_register_unit_floor_resolutions-summary.json";
    format!(
        "\
set -euo pipefail
cd {root}
if [ ! -f {env_file} ]; then
  echo 'missing remote lakehouse env file' >&2
  exit 2
fi
for input in '{units_path}' '{areas_path}'; do
  if [ -z \"$(find -L \"$input\" -type f -name '*.parquet' -size +0c -print -quit)\" ]; then
    echo \"missing or empty Silver parquet handoff: $input\" >&2
    exit 3
  fi
done
lakehouse_uid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}\"
lakehouse_gid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\"
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/remote-lakehouse:/remote-lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p /remote-lakehouse/resolutions /remote-lakehouse/summaries && chown -R \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" /remote-lakehouse\"
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-batch run --rm \\
  -v \"$PWD/target/remote-lakehouse:/workspace/target/remote-lakehouse\" \\
  spark spark-submit \\
  --master local[8] \\
  --driver-memory 12g \\
  --conf spark.jars.ivy=/tmp/.ivy2 \\
  /workspace/infra/lakehouse/spark/jobs/building_register_unit_floor_resolution_export.py \\
  --units-parquet /workspace/{units_path} \\
  --areas-parquet /workspace/{areas_path} \\
  --output /workspace/{output_path} \\
  --summary-output /workspace/{summary_path} \\
  --resolved-at \"$(date -u +%Y-%m-%d)\"
echo '{SUMMARY_BEGIN_MARKER}'
cat {summary_path}
echo '{SUMMARY_END_MARKER}'
"
    )
}

fn build_building_register_unit_proposal_context_script(
    config: &RemoteLakehouseJobConfig,
) -> String {
    let root = shell_quote(&config.remote_root);
    let env_file = shell_quote(&config.env_file);
    let input_path = "target/lakehouse/silver_handoff/building_register_units_parquet";
    let output_path = "target/remote-lakehouse/ai/building_register_unit_proposals-full";
    let summary_path =
        "target/remote-lakehouse/summaries/building_register_unit_proposals-full-summary.json";
    format!(
        "\
set -euo pipefail
cd {root}
if [ ! -f {env_file} ]; then
  echo 'missing remote lakehouse env file' >&2
  exit 2
fi
if [ -z \"$(find -L '{input_path}' -type f -name '*.parquet' -size +0c -print -quit)\" ]; then
  echo 'missing or empty building-register unit Silver Parquet handoff' >&2
  exit 3
fi
lakehouse_uid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}}\"
lakehouse_gid=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}}\"
host_uid=\"$(id -u)\"
host_gid=\"$(id -g)\"
mkdir -p 'target/remote-lakehouse/ai' 'target/remote-lakehouse/summaries'
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/remote-lakehouse:/remote-lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p /remote-lakehouse/ai /remote-lakehouse/summaries && chown -R \\\"${{host_uid}}:${{host_gid}}\\\" /remote-lakehouse\"
rm -rf '{output_path}'
docker run --rm --entrypoint sh \\
  -v \"$PWD/target/remote-lakehouse:/remote-lakehouse\" \\
  amazon/aws-cli:2.17.62 \\
  -c \"mkdir -p /remote-lakehouse/ai /remote-lakehouse/summaries && chown -R \\\"${{lakehouse_uid}}:${{lakehouse_gid}}\\\" /remote-lakehouse\"
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-batch run --rm \\
  -v \"$PWD/target/remote-lakehouse:/workspace/target/remote-lakehouse\" \\
  spark spark-submit \\
  --master local[*] \\
  --driver-memory 4g \\
  /workspace/infra/lakehouse/spark/jobs/building_register_unit_proposal_context_export.py \\
  --input-parquet /workspace/{input_path} \\
  --output /workspace/{output_path} \\
  --summary-output /workspace/{summary_path}
echo '{SUMMARY_BEGIN_MARKER}'
cat {summary_path}
echo '{SUMMARY_END_MARKER}'
"
    )
}

fn build_silver_scalar_remote_script(
    config: &RemoteLakehouseJobConfig,
    spec: SilverScalarRemoteJobSpec,
) -> String {
    let root = shell_quote(&config.remote_root);
    let env_file = shell_quote(&config.env_file);
    let spec_input_path = config
        .input_path_override
        .as_deref()
        .unwrap_or(spec.input_path);
    let input_path = shell_quote(spec_input_path);
    let summary_path = shell_quote(spec.summary_path);
    let input_file_batch_size = config
        .input_file_batch_size_override
        .unwrap_or(spec.default_input_file_batch_size);
    let input_preflight = if spec.require_non_empty_input {
        format!(
            "\
if [ -f {input_path} ]; then
  if ! test -s {input_path}; then
    echo 'missing or empty Silver handoff input' >&2
    exit 3
  fi
elif [ -d {input_path} ]; then
  if [ -z \"$(find -L {input_path} -type f -size +0c -print -quit)\" ]; then
    echo 'missing or empty Silver handoff input' >&2
    exit 3
  fi
else
  echo 'missing or empty Silver handoff input' >&2
  exit 3
fi
"
        )
    } else {
        String::new()
    };
    let expected_count_arg = spec
        .expected_count
        .map(|count| format!("  --expected-count {count} \\\n"))
        .unwrap_or_default();
    let java_extra_options_args =
        spec.spark_java_extra_options
            .map_or_else(String::new, |java_extra_options| {
                format!(
                    "  --conf spark.driver.extraJavaOptions={java_extra_options} \\\n  --conf spark.executor.extraJavaOptions={java_extra_options} \\\n"
                )
            });
    let allow_non_smoke_overwrite_arg = if spec.allow_non_smoke_overwrite {
        "  --allow-non-smoke-overwrite \\\n"
    } else {
        ""
    };
    format!(
        "\
set -euo pipefail
cd {root}
if [ ! -f {env_file} ]; then
  echo 'missing remote lakehouse env file' >&2
  exit 2
fi
{input_preflight}\
set -a
. {env_file}
set +a
if [ -z \"${{R2_BUCKET_NAME:-}}\" ]; then
  echo 'lakehouse catalog bucket mismatch: R2_BUCKET_NAME is missing' >&2
  exit 9
fi
catalog_bucket=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI##*/}}\"
if [ \"$catalog_bucket\" != \"$R2_BUCKET_NAME\" ]; then
  echo \"lakehouse catalog bucket mismatch: catalog_uri_bucket=$catalog_bucket r2_bucket=$R2_BUCKET_NAME\" >&2
  exit 9
fi
case \"${{FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE:-}}\" in
  *_\"$R2_BUCKET_NAME\") ;;
  *)
    echo \"lakehouse warehouse bucket mismatch: warehouse=$FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE r2_bucket=$R2_BUCKET_NAME\" >&2
    exit 9
    ;;
esac
if [ -n \"${{FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI:-}}\" ]; then
  expected_oauth_uri=\"${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI%/}}/v1/oauth/tokens\"
  if [ \"$FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI\" != \"$expected_oauth_uri\" ]; then
    echo 'lakehouse oauth uri mismatch: expected catalog_uri + /v1/oauth/tokens' >&2
    exit 9
  fi
fi
render_trino_catalog_from_env() {{
  mkdir -p 'infra/lakehouse/trino/catalog'
  cat > 'infra/lakehouse/trino/catalog/r2.properties' <<TRINO_CATALOG
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI}}
iceberg.rest-catalog.warehouse=${{FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE}}
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN}}
iceberg.rest-catalog.oauth2.server-uri=${{FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI%/}}/v1/oauth/tokens
fs.s3.enabled=true
s3.region=${{R2_REGION:-auto}}
s3.endpoint=${{R2_ENDPOINT}}
s3.aws-access-key=${{R2_ACCESS_KEY_ID}}
s3.aws-secret-key=${{R2_SECRET_ACCESS_KEY}}
s3.path-style-access=true
TRINO_CATALOG
  chmod 600 'infra/lakehouse/trino/catalog/r2.properties'
}}
render_trino_catalog_from_env
mkdir -p 'target/lakehouse/smoke'
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-batch run --rm \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \\
  -e FOUNDATION_PLATFORM_SPARK_SKIP_STOP_ON_SUCCESS=1 \\
  spark spark-submit \\
  --master {spec_spark_master} \\
  --driver-memory {spec_spark_driver_memory} \\
  --conf spark.jars.ivy=/tmp/.ivy2 \\
{java_extra_options_args}\
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \\
  /workspace/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py \\
  --input /workspace/{spec_input_path} \\
  --input-format {spec_input_format} \\
  --contract {spec_contract} \\
  --write-mode iceberg \\
  --iceberg-write-mode overwrite \\
  --iceberg-table {spec_iceberg_table} \\
{expected_count_arg}{allow_non_smoke_overwrite_arg}  --input-file-batch-size {input_file_batch_size} \\
  --defer-iceberg-readback-validation \\
  --summary-output /workspace/{spec_summary_path}
expected_rows=\"$(grep -o '\"row_count\":[0-9][0-9]*' {summary_path} | tail -n 1 | sed 's/[^0-9]//g')\"
if [ -z \"$expected_rows\" ]; then
  echo 'missing row_count in Spark summary' >&2
  exit 6
fi
{LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-query up -d --force-recreate trino
for attempt in $(seq 1 60); do
  if {LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-query exec -T trino trino --catalog r2 --schema silver --execute \"SELECT 1\" >/dev/null 2>&1; then
    break
  fi
  if [ \"$attempt\" -eq 60 ]; then
    echo 'trino did not become ready for Iceberg readback validation' >&2
    exit 7
  fi
  sleep 2
done
actual_rows=\"$({LAKEHOUSE_COMPOSE_COMMAND} --profile lakehouse-query exec -T trino trino --catalog r2 --schema silver --execute \"SELECT count(*) FROM {spec_iceberg_table}\" | tr -d '\"[:space:]')\"
if [ \"$actual_rows\" != \"$expected_rows\" ]; then
  echo \"trino row count mismatch table={spec_iceberg_table} expected=$expected_rows actual=$actual_rows\" >&2
  exit 8
fi
echo '{SUMMARY_BEGIN_MARKER}'
cat {summary_path}
echo '{SUMMARY_END_MARKER}'
",
        spec_input_path = spec_input_path,
        spec_input_format = spec.input_format,
        spec_contract = spec.contract,
        spec_summary_path = spec.summary_path,
        spec_iceberg_table = spec.iceberg_table,
        spec_spark_master = spec.spark_master,
        spec_spark_driver_memory = spec.spark_driver_memory,
        java_extra_options_args = java_extra_options_args
    )
}

fn extract_marked_summary_json(output: &str) -> anyhow::Result<String> {
    let Some((_, rest)) = output.split_once(SUMMARY_BEGIN_MARKER) else {
        bail!("remote lakehouse job output did not contain summary marker");
    };
    let Some((summary, _)) = rest.split_once(SUMMARY_END_MARKER) else {
        bail!("remote lakehouse job output did not contain summary marker end");
    };
    let trimmed = summary.trim();
    if trimmed.is_empty() {
        bail!("remote lakehouse job summary marker was empty");
    }
    Ok(trimmed.to_owned())
}

fn validate_spark_summary(raw: &str) -> anyhow::Result<SparkRunSummary> {
    let summary = SparkRunSummary::from_json_str(raw)
        .map_err(|error| anyhow::anyhow!("invalid Spark summary JSON: {error}"))?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(&summary.contract)
        .ok_or_else(|| anyhow::anyhow!("unknown lakehouse contract: {}", summary.contract))?;
    summary
        .validate_for_contract(contract)
        .map_err(|error| anyhow::anyhow!("Spark summary failed contract validation: {error}"))?;
    Ok(summary)
}

/// 층 교정표 summary 검증: 스키마 버전 + 해소/미해소 합이 충돌 총수와 일치.
/// 반환: `(conflict_rows, resolved_count)`.
fn validate_unit_floor_resolution_summary(raw: &str) -> anyhow::Result<(u64, u64)> {
    let value: JsonValue = serde_json::from_str(raw)
        .map_err(|error| anyhow::anyhow!("invalid floor resolution summary JSON: {error}"))?;
    let schema_version = value
        .get("schema_version")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if schema_version != "foundation-platform.building_register_unit_floor_resolution.v1" {
        bail!("invalid floor resolution summary schema_version: {schema_version}");
    }
    let read_count = |key: &str| -> anyhow::Result<u64> {
        value
            .get(key)
            .and_then(JsonValue::as_u64)
            .ok_or_else(|| anyhow::anyhow!("floor resolution summary missing {key}"))
    };
    let conflict_rows = read_count("conflict_rows")?;
    let resolved = read_count("resolved_count")?;
    let unresolved = read_count("unresolved_count")?;
    if resolved + unresolved != conflict_rows {
        bail!(
            "floor resolution summary counts inconsistent: {resolved} + {unresolved} != {conflict_rows}"
        );
    }
    Ok((conflict_rows, resolved))
}

fn validate_unit_proposal_context_summary(raw: &str) -> anyhow::Result<u64> {
    let value: JsonValue = serde_json::from_str(raw)
        .map_err(|error| anyhow::anyhow!("invalid proposal context summary JSON: {error}"))?;
    let schema_version = value
        .get("schema_version")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if schema_version
        != "foundation-platform.building_register_unit_proposal_context_export_summary.v1"
    {
        bail!("invalid proposal context summary schema_version: {schema_version}");
    }
    let target_kind = value
        .get("target")
        .and_then(|target| target.get("kind"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if target_kind != "ai_proposal_input_jsonl" {
        bail!("invalid proposal context summary target kind: {target_kind}");
    }
    value
        .get("proposal_count")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| anyhow::anyhow!("proposal context summary missing proposal_count"))
}

fn build_audit_record_input(
    summary_json: String,
    audit: &RemoteLakehouseAuditConfig,
) -> Option<RecordLakehouseBatchRunInput> {
    match audit {
        RemoteLakehouseAuditConfig::Disabled => None,
        RemoteLakehouseAuditConfig::Record {
            recorded_by_staff_id,
            request_id,
        } => Some(RecordLakehouseBatchRunInput {
            summary_json,
            recorded_by_staff_id: *recorded_by_staff_id,
            request_id: request_id.clone(),
        }),
    }
}

async fn record_lakehouse_batch_run_if_enabled(
    summary_json: String,
    audit: &RemoteLakehouseAuditConfig,
) -> anyhow::Result<bool> {
    let Some(input) = build_audit_record_input(summary_json, audit) else {
        return Ok(false);
    };

    let database_url = env::var("DATABASE_URL").context(
        "DATABASE_URL is required when FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT=1",
    )?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to Postgres for lakehouse batch audit")?;
    let use_case = RecordLakehouseBatchRun::new(Arc::new(PgLakehouseBatchRunAudit::new(pool)));
    use_case
        .execute(input)
        .await
        .map_err(|error| anyhow::anyhow!("failed to record lakehouse batch audit: {error}"))?;
    Ok(true)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn required_lookup(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<String> {
    optional_lookup(lookup, name).ok_or_else(|| anyhow::anyhow!("{name} is required"))
}

fn optional_lookup(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_bool_lookup(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<Option<bool>> {
    optional_lookup(lookup, name)
        .map(|value| match value.as_str() {
            "1" | "true" | "TRUE" => Ok(true),
            "0" | "false" | "FALSE" => Ok(false),
            _ => bail!("{name} must be one of 1, 0, true, false"),
        })
        .transpose()
}

fn optional_u32_lookup(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<Option<u32>> {
    optional_lookup(lookup, name)
        .map(|value| {
            let parsed = value
                .parse::<u32>()
                .with_context(|| format!("{name} must be a positive integer"))?;
            if parsed == 0 {
                bail!("{name} must be greater than zero");
            }
            Ok(parsed)
        })
        .transpose()
}

#[cfg(test)]
#[path = "remote_lakehouse_job/tests.rs"]
mod tests;
