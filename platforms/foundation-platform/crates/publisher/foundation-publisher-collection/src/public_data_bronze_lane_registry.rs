use std::collections::BTreeMap;

use serde::Deserialize;

pub const LANE_REGISTRY_SCHEMA_VERSION: &str =
    "foundation-platform.public_data_bronze_lane_registry.v1";
pub const LANE_EXECUTION_GATE_CONFIRMATION: &str = "ConfirmPublicDataBronzeLaneExecution";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LaneRegistryDocument {
    pub schema_version: String,
    pub status: String,
    pub owner: String,
    pub non_executable_source_acquisition_lanes: Vec<NonExecutableSourceLane>,
    pub lanes: Vec<LaneDefinition>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct NonExecutableSourceLane {
    pub source_acquisition_lane: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LaneDefinition {
    pub lane_id: String,
    pub status: String,
    pub include_by_default: bool,
    pub provider: String,
    pub source_acquisition_lanes: Vec<String>,
    pub endpoint_groups: Vec<String>,
    pub command: String,
    pub command_args: Vec<String>,
    pub inner_parallel_env: String,
    pub execution_gate: String,
    pub full_download_gate_env: String,
    pub live_write_gate_env: String,
    pub planned_blocker: String,
    pub completion_claim_allowed: Option<bool>,
    pub national_rollout_allowed: Option<bool>,
    pub environment: BTreeMap<String, String>,
}

impl LaneDefinition {
    pub fn command_args_or_default(&self) -> Vec<String> {
        if !self.command_args.is_empty() {
            return self.command_args.clone();
        }
        if self.command.trim().is_empty() {
            return Vec::new();
        }
        vec![self.command.clone()]
    }

    pub fn command_args_are_subcommand_only(&self) -> bool {
        let Some(first_arg) = self.command_args.first() else {
            return true;
        };
        if first_arg != &self.command {
            return false;
        }
        !self.command_args.iter().any(|arg| {
            matches!(
                arg.as_str(),
                "cargo" | "run" | "-p" | "--package" | "foundation-outbox-publisher"
            )
        })
    }
}

pub fn validate_lane_control_contract(registry: &LaneRegistryDocument) -> Vec<String> {
    let mut blockers = Vec::new();
    for lane in &registry.lanes {
        if lane.completion_claim_allowed != Some(false) {
            blockers.push(format!(
                "lane completion_claim_allowed must be explicitly false: {}",
                lane.lane_id
            ));
        }
        if lane.national_rollout_allowed != Some(false) {
            blockers.push(format!(
                "lane national_rollout_allowed must be explicitly false: {}",
                lane.lane_id
            ));
        }
        if !lane.command.trim().is_empty()
            && !lane.command_args.is_empty()
            && !lane.command_args_are_subcommand_only()
        {
            blockers.push(format!(
                "lane command_args must be subcommand-only: {}",
                lane.lane_id
            ));
        }
        if lane.status == "enabled" {
            if lane.execution_gate != LANE_EXECUTION_GATE_CONFIRMATION {
                blockers.push(format!(
                    "enabled lane execution_gate is required: {}",
                    lane.lane_id
                ));
            }
            validate_gate_env(
                &lane.lane_id,
                "live_write_gate_env",
                &lane.live_write_gate_env,
                true,
                &mut blockers,
            );
            validate_gate_env(
                &lane.lane_id,
                "full_download_gate_env",
                &lane.full_download_gate_env,
                lane_requires_full_download_gate(lane),
                &mut blockers,
            );
        }
        if lane.status == "planned" && lane.planned_blocker.trim().is_empty() {
            blockers.push(format!(
                "planned lane planned_blocker is required: {}",
                lane.lane_id
            ));
        }
    }
    blockers
}

pub fn valid_platform_env(value: &str) -> bool {
    value.starts_with("FOUNDATION_PLATFORM_")
        && value.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
}

pub fn environment_contains_secret_material(name: &str, value: &str) -> bool {
    let upper_name = name.to_ascii_uppercase();
    let sensitive_name = upper_name.split('_').any(|token| {
        matches!(
            token,
            "KEY" | "SECRET" | "TOKEN" | "PASSWORD" | "CREDENTIAL"
        )
    }) || upper_name.contains("ACCESS_KEY")
        || upper_name.contains("SERVICE_KEY")
        || upper_name.contains("API_KEY")
        || upper_name.contains("PRIVATE_KEY");
    let lower_value = value.to_ascii_lowercase();
    sensitive_name
        || lower_value.contains("secret")
        || lower_value.contains("password")
        || lower_value.contains("token")
        || lower_value.contains("access_key")
        || lower_value.contains("service_key")
        || lower_value.contains("api_key")
        || lower_value.contains("cfut_")
}

fn validate_gate_env(
    lane_id: &str,
    field: &str,
    value: &str,
    required: bool,
    blockers: &mut Vec<String>,
) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        if required {
            blockers.push(format!("enabled lane {field} invalid: {lane_id}"));
        }
        return;
    }
    if !valid_platform_env(trimmed) {
        blockers.push(format!("enabled lane {field} invalid: {lane_id}"));
    }
    if environment_contains_secret_material(field, trimmed) {
        blockers.push(format!(
            "enabled lane {field} must not look like secret material: {lane_id}"
        ));
    }
}

fn lane_requires_full_download_gate(lane: &LaneDefinition) -> bool {
    lane.source_acquisition_lanes
        .iter()
        .any(|source_lane| matches!(source_lane.as_str(), "bulk_file" | "provider_dataset_file"))
}
