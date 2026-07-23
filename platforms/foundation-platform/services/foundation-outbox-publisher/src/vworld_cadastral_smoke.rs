//! `VWorld` cadastral 2D Data API smoke command.

use anyhow::{bail, Context};
use collection_infrastructure::{
    VWorldDataApiClient, VWorldDataApiConfig, VWorldDataFeatureRequest, VWorldRequestPolicy,
};
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    optional_bool_env, optional_duration_millis_env, optional_duration_seconds_env,
    optional_env_value, optional_positive_u32_env, optional_u32_env, required_env_value,
};

const DEFAULT_BASE_URI: &str = "https://api.vworld.kr";
const DEFAULT_USER_AGENT: &str = "foundation-outbox-publisher/0.1";
const CADASTRAL_DATASET: &str = "LP_PA_CBND_BUBUN";
const DEFAULT_PAGE: u32 = 1;
const DEFAULT_SIZE: u32 = 10;

/// Runs a read-only `VWorld` cadastral 2D Data API smoke request.
pub async fn run() -> anyhow::Result<()> {
    let config = VWorldCadastralSmokeConfig::from_env()?;
    let client = VWorldDataApiClient::new_with_policy(
        &VWorldDataApiConfig {
            base_uri: config.base_uri.clone(),
            api_key: config.api_key.clone(),
            domain: config.domain.clone(),
            user_agent: config.user_agent.clone(),
        },
        config.request_policy,
    )?;
    let page = client
        .fetch_feature_page(&config.request)
        .await
        .context("failed to fetch VWorld cadastral 2D Data API smoke page")?;
    let feature_count = feature_count(&page.payload);
    let total_records = json_pointer_string(&page.payload, "/response/record/total");
    let bbox = page
        .payload
        .pointer("/response/result/featureCollection/bbox")
        .cloned()
        .unwrap_or(JsonValue::Null);
    let first_pnu = json_pointer_string(
        &page.payload,
        "/response/result/featureCollection/features/0/properties/pnu",
    );

    tracing::info!(
        dataset = %config.request.dataset,
        attr_filter = ?config.request.attr_filter,
        page = config.request.page,
        size = config.request.size,
        total_records = ?total_records,
        feature_count,
        first_pnu = ?first_pnu,
        bbox = %bbox,
        "VWorld cadastral 2D Data API smoke succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldCadastralSmokeConfig {
    base_uri: String,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    request: VWorldDataFeatureRequest,
    request_policy: VWorldRequestPolicy,
}

impl VWorldCadastralSmokeConfig {
    fn from_env() -> anyhow::Result<Self> {
        let default_policy = VWorldRequestPolicy::default();
        let max_attempts = optional_positive_u32_env("FOUNDATION_PLATFORM_VWORLD_MAX_ATTEMPTS")?
            .unwrap_or_else(|| default_policy.max_attempts());
        let request_timeout =
            optional_duration_seconds_env("FOUNDATION_PLATFORM_VWORLD_REQUEST_TIMEOUT_SECONDS")?
                .unwrap_or_else(|| default_policy.request_timeout());
        let initial_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_INITIAL_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.initial_backoff());
        let max_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_MAX_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.max_backoff());
        let pnu = optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PNU")?.or(
            optional_env_value("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU")?,
        );
        if optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOM_FILTER")?.is_some() {
            bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOM_FILTER is not supported for VWorld cadastral smoke; use FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER"
            );
        }
        let attr_filter = optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER")?
            .or_else(|| pnu.map(|value| format!("pnu:=:{value}")));

        Ok(Self {
            base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATA_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            api_key: required_env_value("VWORLD_API_KEY")?,
            domain: optional_env_value("VWORLD_DOMAIN")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request: VWorldDataFeatureRequest {
                dataset: CADASTRAL_DATASET.to_owned(),
                attr_filter,
                columns: vec![
                    "pnu".to_owned(),
                    "jibun".to_owned(),
                    "bonbun".to_owned(),
                    "bubun".to_owned(),
                    "addr".to_owned(),
                    "ag_geom".to_owned(),
                ],
                geometry: optional_bool_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOMETRY")?
                    .unwrap_or(true),
                attribute: optional_bool_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTRIBUTE")?
                    .unwrap_or(true),
                crs: optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_CRS")?
                    .or_else(|| Some("EPSG:4326".to_owned())),
                page: optional_u32_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PAGE")?
                    .unwrap_or(DEFAULT_PAGE),
                size: optional_u32_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SIZE")?
                    .unwrap_or(DEFAULT_SIZE),
            },
            request_policy: VWorldRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
        })
    }
}

fn feature_count(payload: &JsonValue) -> usize {
    payload
        .pointer("/response/result/featureCollection/features")
        .and_then(JsonValue::as_array)
        .map_or(0, Vec::len)
}

fn json_pointer_string(payload: &JsonValue, pointer: &str) -> Option<String> {
    payload
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}
