//! The lakehouse jobs the publisher drives on the data server: the FLOOR cycle (root ADR-0128,
//! ADR-0338), the Silver refresh lanes (root ADR-0169) and the cleanup each unit's `ExecStopPost`
//! runs.
//!
//! The FLOOR cycle exports in the control container and judges and loads in this process: the
//! history gate (`building_register_floor_silver_export::history_gate_run`) and the `DuckDB` load
//! (`building_register_floor_silver_export::floor_load`) read the `DuckDB` extensions beside this
//! release's publisher, as the Silver refresh lanes' loads do (root ADR-0599).

use std::{env, sync::Arc};

use anyhow::{bail, Context};
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::{RecordLakehouseBatchRun, RecordLakehouseBatchRunInput};
use lakehouse_domain::{industrial_complex_lakehouse_contract_by_table_name, SparkRunSummary};
use lakehouse_infrastructure::PgLakehouseBatchRunAudit;
use sqlx::PgPool;
use uuid::Uuid;

mod floor_cycle;
mod floor_history;
mod floor_source;
mod invocation_cleanup;
/// The labels a job's Compose project cleanup lists by; a tool container that carries them is removed
/// with the job's Spark container (`lakehouse_tile_bake`).
pub use invocation_cleanup::{ONEOFF_LABEL, PROJECT_LABEL};
mod silver_refresh;

pub use silver_refresh::export_commands as silver_refresh_export_commands;

const LAKEHOUSE_COMPOSE_COMMAND: &str = "docker compose -f compose.lakehouse.yml";
/// The hub FLOOR source the export reads (root ADR-0128).
const FLOOR_SOURCE_SLUG: &str = "hubgokr__building_register_floor_overview";
/// Rows per handoff Parquet file the export writes.
const FLOOR_CHUNK_ROWS: u32 = 250_000;

/// One FLOOR cycle on this host: the release it runs from, the inputs it selected and where it
/// works.
#[derive(Clone, Debug, Eq, PartialEq)]
struct FloorPipelineConfig {
    release_root: String,
    floor_source: floor_source::FloorSource,
    local: floor_cycle::LocalExecution,
    audit: RemoteLakehouseAuditConfig,
    native_execution_profile: crate::lakehouse_engine_contract::ExecutionProfile,
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

/// The cycle's last step: a retained edition reports itself, a loaded one is validated and
/// audited as the Spark load's summary was.
async fn finish_job(config: &FloorPipelineConfig, summary_json: String) -> anyhow::Result<()> {
    if let Some(retained_rows) =
        floor_history::retained_outcome(&summary_json, &config.floor_source)?
    {
        println!(
            "remote-lakehouse-job-ok job=building_register_floors_pipeline_full action=already_retained retained_row_count={retained_rows} persisted_row_count=0 audit_recorded=false"
        );
        return Ok(());
    }
    let summary = validate_run_summary(&summary_json)?;
    let audit_recorded = record_lakehouse_batch_run_if_enabled(summary_json, &config.audit).await?;
    println!(
        "remote-lakehouse-job-ok job=building_register_floors_pipeline_full contract={} row_count={} persisted_row_count={}",
        summary.contract,
        summary.row_count,
        summary.persisted_row_count.unwrap_or(0)
    );
    if audit_recorded {
        println!(
            "remote-lakehouse-job-audit-recorded job=building_register_floors_pipeline_full contract={}",
            summary.contract
        );
    }
    Ok(())
}

pub fn stop_floor_cycle() -> anyhow::Result<()> {
    floor_cycle::stop()
}

/// `ExecStopPost` of foundation-gold-panel-rebuild.service: remove the Spark container a
/// timed-out or killed run left behind (root ADR-0139).
pub fn stop_gold_panel_rebuild() -> anyhow::Result<()> {
    invocation_cleanup::run(invocation_cleanup::Job::GoldPanelRebuild)
}

/// `ExecStopPost` of foundation-lakehouse-maintenance.service: remove the Spark container a
/// timed-out or killed run left behind (root ADR-0238).
pub fn stop_lakehouse_maintenance() -> anyhow::Result<()> {
    invocation_cleanup::run(invocation_cleanup::Job::LakehouseMaintenance)
}

/// `ExecStopPost` of foundation-parcel-tile-bake@.service: remove the Spark container a timed-out
/// or killed bake left behind (root ADR-0133 §7).
pub fn stop_parcel_tile_bake() -> anyhow::Result<()> {
    invocation_cleanup::run(invocation_cleanup::Job::ParcelTileBake)
}

/// `ExecStopPost` of foundation-unit-price-versions-migrate.service: remove the Spark container a
/// timed-out or killed run left behind (root ADR-0317 §5.2).
pub fn stop_unit_price_versions() -> anyhow::Result<()> {
    invocation_cleanup::run(invocation_cleanup::Job::UnitPriceVersions)
}

pub async fn run_floor_cycle() -> anyhow::Result<()> {
    floor_cycle::run().await
}

/// `foundation-silver-refresh@<lane>.service`: refresh one Silver lane from the newest complete
/// release in the Bronze ledger, or report it unchanged (root ADR-0169).
pub async fn run_silver_refresh() -> anyhow::Result<()> {
    silver_refresh::run().await
}

/// `ExecStopPost` of `foundation-silver-refresh@.service`: remove the export container a
/// timed-out or killed run left behind.
pub fn stop_silver_refresh() -> anyhow::Result<()> {
    silver_refresh::stop()
}

/// The shell lines every FLOOR container step starts with: the release, the Compose project and
/// state the cleanup knows, the native execution profile the control container is held to, and
/// the pinned image, which must already be on the host.
fn floor_prelude(config: &FloorPipelineConfig) -> String {
    let local = &config.local;
    let profile = &config.native_execution_profile;
    // Docker's memory-swap is the total memory + swap ceiling; equal means no swap.
    let memory_swap_mib = u64::from(profile.memory_mib) + u64::from(profile.swap_mib);
    let remote_state = shell_quote(&local.state_root.join("remote").to_string_lossy());
    format!(
        "\
set -euo pipefail
cd {root}
export COMPOSE_PROJECT_NAME={project}
export FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT={state_root}
export FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_STATE_ROOT={remote_state}
mkdir -p {remote_state}
export FOUNDATION_PLATFORM_NATIVE_MEMORY_MIB='{memory_mib}'
export FOUNDATION_PLATFORM_NATIVE_CPU_SLOTS='{cpu_slots}'
export FOUNDATION_PLATFORM_NATIVE_PIDS_LIMIT='{pids_limit}'
export FOUNDATION_PLATFORM_NATIVE_MEMORY_SWAP_MIB='{memory_swap_mib}'
docker image inspect {image} >/dev/null
",
        root = shell_quote(&config.release_root),
        project = shell_quote(&local.project),
        state_root = shell_quote(&local.state_root.to_string_lossy()),
        memory_mib = profile.memory_mib,
        cpu_slots = profile.cpu_slots,
        pids_limit = profile.pids_limit,
        image = shell_quote(&local.image),
    )
}

/// One run of the control container with the export's settings; `extra` adds `-e` lines.
fn floor_native_run(config: &FloorPipelineConfig, extra: &str, command: &str) -> String {
    let root = config.floor_source.root();
    format!(
        "\
{native_compose} --profile lakehouse-control run --rm --no-deps --pull never \\
  --user \"$(id -u):$(id -g)\" \\
{extra}  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_BRONZE_ROOT='target/lakehouse' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG='{FLOOR_SOURCE_SLUG}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_PATH='{root}/handoff' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT='parquet' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_CHUNK_ROWS='{FLOOR_CHUNK_ROWS}' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_NORMALIZATION_PROPOSAL_INPUT_PATH='{root}/proposals.jsonl' \\
  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SUMMARY_PATH='{root}/export-summary.json' \\
{exporter_env}  lakehouse-control {command}
",
        native_compose = floor_cycle::native_compose(config),
        exporter_env = config.floor_source.exporter_env(),
    )
}

/// Before the gate: stage the exact committed inputs, then inspect them into the receipt at
/// `receipt` (a path under `target/lakehouse`, as the container sees it).
fn build_floor_inspection_script(config: &FloorPipelineConfig, receipt: &str) -> String {
    // Credentials pass by environment name only and are needed only for missing staged files.
    let stage = floor_native_run(
        config,
        "\
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \\
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID \\
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET \\
  -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID \\
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY \\
",
        "stage-building-register-floor-inputs",
    );
    let inspection = floor_native_run(
        config,
        &format!(
            "  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH={} \\\n",
            shell_quote(receipt)
        ),
        "export-building-register-floor-silver-handoff",
    );
    format!("{}{stage}{inspection}", floor_prelude(config))
}

/// After the gate asked for it: export the edition the receipt named into the run's handoff.
fn build_floor_export_script(config: &FloorPipelineConfig) -> String {
    format!(
        "{}{}",
        floor_prelude(config),
        floor_native_run(config, "", "export-building-register-floor-silver-handoff")
    )
}

fn validate_run_summary(raw: &str) -> anyhow::Result<SparkRunSummary> {
    let summary = SparkRunSummary::from_json_str(raw)
        .map_err(|error| anyhow::anyhow!("invalid FLOOR run summary JSON: {error}"))?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(&summary.contract)
        .ok_or_else(|| anyhow::anyhow!("unknown lakehouse contract: {}", summary.contract))?;
    summary.validate_for_contract(contract).map_err(|error| {
        anyhow::anyhow!("FLOOR run summary failed contract validation: {error}")
    })?;
    Ok(summary)
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

#[cfg(test)]
#[path = "remote_lakehouse_job/tests.rs"]
mod tests;
