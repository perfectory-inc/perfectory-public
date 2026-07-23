use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, ModelGateway,
    ModelReasoningEffort, NormalizationAuditPort, NormalizationOutboxPort,
    NormalizationProposalGenerator, NormalizationReconcileQueuePort, PermissionAction,
    RateLimitQuota, RateLimitRouteClass, RateLimiterPort,
};
use intelligence_normalization_infrastructure::{
    FoundationPlatformNormalizationClient, FoundationPlatformNormalizationConfig,
    InMemoryWorkflowState, MemoryRateLimitConfig, MemoryRateLimiter,
    ModelBackedNormalizationProposalGenerator, NormalizationGeneratorConfig,
    OllamaNativeModelGateway, OllamaNativeModelGatewayConfig, OpenAiCompatibleModelGateway,
    OpenAiCompatibleModelGatewayConfig, PostgresWorkflowState, PostgresWorkflowStateConfig,
    RedisRateLimitConfig, RedisRateLimiter, WorkloadTokenProvider,
};
use metrics_exporter_prometheus::PrometheusHandle;

use crate::auth::{inbound_auth_config_from_lookup, InboundAuthConfig, InboundPrincipalConfig};

#[derive(Clone)]
pub struct AppState {
    /// Durable outbox port for normalization submissions.
    ///
    /// Default: in-memory (local-dev / test fallback). Task 9 wires
    /// `DATABASE_URL` → Postgres adapter via `from_env`.
    pub normalization_outbox: Arc<dyn NormalizationOutboxPort>,
    /// Durable audit log port for normalization events.
    pub normalization_audit_log: Arc<dyn NormalizationAuditPort>,
    pub reconcile_queue: Arc<dyn NormalizationReconcileQueuePort>,
    pub foundation_submitter: Option<Arc<dyn FoundationNormalizationSubmitter>>,
    pub model_gateway: Option<Arc<dyn ModelGateway>>,
    pub chat_model_ids: Arc<Vec<String>>,
    pub proposal_generator: Option<Arc<dyn NormalizationProposalGenerator>>,
    pub inbound_auth: Option<InboundAuthConfig>,
    pub metrics: Option<PrometheusHandle>,
    pub rate_limiter: Option<Arc<dyn RateLimiterPort>>,
    pub rate_limit_policy: RateLimitRuntimePolicy,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RateLimitRoutePolicy {
    pub quota: RateLimitQuota,
    pub cost: u32,
}

impl RateLimitRoutePolicy {
    fn validate(self) -> Result<Self, FoundationSubmissionError> {
        self.quota
            .validate()
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })?;
        if self.cost == 0 || self.cost > self.quota.capacity {
            return Err(FoundationSubmissionError::InvalidResponse {
                message: "rate limit cost must be positive and not exceed capacity".to_string(),
            });
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RateLimitRuntimePolicy {
    default: RateLimitRoutePolicy,
    overrides: BTreeMap<RateLimitRouteClass, RateLimitRoutePolicy>,
}

impl Default for RateLimitRuntimePolicy {
    fn default() -> Self {
        Self {
            default: RateLimitRoutePolicy {
                quota: RateLimitQuota {
                    capacity: 60,
                    refill_per_second: 1.0,
                },
                cost: 1,
            },
            overrides: BTreeMap::new(),
        }
    }
}

impl RateLimitRuntimePolicy {
    pub fn for_route(&self, route_class: RateLimitRouteClass) -> RateLimitRoutePolicy {
        self.overrides
            .get(&route_class)
            .copied()
            .unwrap_or(self.default)
    }

    pub fn with_route_policy(
        mut self,
        route_class: RateLimitRouteClass,
        policy: RateLimitRoutePolicy,
    ) -> Self {
        self.overrides.insert(route_class, policy);
        self
    }
}

pub struct ConfiguredRateLimiter {
    pub limiter: Arc<dyn RateLimiterPort>,
    pub policy: RateLimitRuntimePolicy,
}

impl Default for AppState {
    /// Creates an `AppState` backed by a single shared [`InMemoryWorkflowState`]
    /// instance that implements all three workflow ports. This is the
    /// local-dev and test fallback; Task 9 replaces it with a Postgres adapter
    /// when `DATABASE_URL` is set.
    fn default() -> Self {
        let workflow = Arc::new(InMemoryWorkflowState::default());
        Self {
            normalization_outbox: workflow.clone(),
            normalization_audit_log: workflow.clone(),
            reconcile_queue: workflow,
            foundation_submitter: None,
            model_gateway: None,
            chat_model_ids: Arc::new(Vec::new()),
            proposal_generator: None,
            inbound_auth: None,
            metrics: None,
            rate_limiter: None,
            rate_limit_policy: RateLimitRuntimePolicy::default(),
        }
    }
}

impl AppState {
    /// Builds `AppState` from environment variables.
    ///
    /// When `DATABASE_URL` is set to a non-empty value, connects to Postgres
    /// and replaces all three workflow ports with a [`PostgresWorkflowState`]
    /// instance.  Falls back to the in-memory adapter when `DATABASE_URL` is
    /// absent or empty.
    pub async fn from_env() -> Result<Self, FoundationSubmissionError> {
        let mut state = Self::default();

        // Task 9: wire Postgres adapter when DATABASE_URL is present.
        if let Some(config) = postgres_workflow_config_from_lookup(|key| env::var(key).ok())? {
            let pg = PostgresWorkflowState::connect(config).await.map_err(|e| {
                FoundationSubmissionError::InvalidResponse {
                    message: format!(
                        "DATABASE_URL is set but postgres workflow state connect failed: {e}"
                    ),
                }
            })?;

            state = state.with_workflow_state(Arc::new(pg));
        }

        if let Some((gateway, config)) = model_gateway_from_env()? {
            let generator = ModelBackedNormalizationProposalGenerator::new_dyn(
                gateway.clone(),
                NormalizationGeneratorConfig {
                    profile_id: config.profile_id,
                    model_id: Some(config.default_model.clone()),
                    prompt_id: "normalization-proposal-v1".to_string(),
                    prompt_version: "v1".to_string(),
                    temperature: 0.1,
                    max_output_tokens: 1024,
                    reasoning_effort: config.reasoning_effort.clone(),
                },
            );
            state = state
                .with_model_gateway_dyn(gateway)
                .with_chat_model_id(config.default_model.clone())
                .with_proposal_generator(Arc::new(generator));
        }

        if let Some(rate_limiter) = rate_limiter_from_env().await? {
            state = state
                .with_rate_limiter_dyn(rate_limiter.limiter)
                .with_rate_limit_policy(rate_limiter.policy);
        }

        let Some(foundation_platform_config) =
            foundation_platform_config_from_lookup(|key| env::var(key).ok())?
        else {
            return Ok(state);
        };

        let submitter = FoundationPlatformNormalizationClient::new(foundation_platform_config)?;

        Ok(state.with_foundation_submitter(Arc::new(submitter)))
    }

    /// Replaces all three workflow ports with a single shared implementation.
    ///
    /// `T` must implement `NormalizationOutboxPort`,
    /// `NormalizationAuditPort`, and `NormalizationReconcileQueuePort` so that
    /// one `Arc<T>` can back every workflow field, guaranteeing that outbox
    /// writes, audit appends, and reconcile stats all land in the same store.
    pub fn with_workflow_state<T>(mut self, workflow: Arc<T>) -> Self
    where
        T: NormalizationOutboxPort
            + NormalizationAuditPort
            + NormalizationReconcileQueuePort
            + 'static,
    {
        self.normalization_outbox = workflow.clone();
        self.normalization_audit_log = workflow.clone();
        self.reconcile_queue = workflow;
        self
    }

    /// Replaces just the outbox and audit ports, preserving the existing
    /// reconcile queue unless the caller overrides it separately.
    ///
    /// This supports test decorators that intentionally wrap submission-state
    /// behavior without also acting as reconcile-queue implementations.
    pub fn with_outbox_and_audit_ports<T>(mut self, workflow: Arc<T>) -> Self
    where
        T: NormalizationOutboxPort + NormalizationAuditPort + 'static,
    {
        self.normalization_outbox = workflow.clone();
        self.normalization_audit_log = workflow;
        self
    }

    pub fn with_reconcile_queue<T>(mut self, reconcile_queue: Arc<T>) -> Self
    where
        T: NormalizationReconcileQueuePort + 'static,
    {
        self.reconcile_queue = reconcile_queue;
        self
    }

    pub fn with_inbound_auth(mut self, inbound_auth: InboundAuthConfig) -> Self {
        self.inbound_auth = Some(inbound_auth);
        self
    }

    pub fn with_metrics(mut self, metrics: Option<PrometheusHandle>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_rate_limiter_dyn(mut self, limiter: Arc<dyn RateLimiterPort>) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    pub fn with_rate_limit_policy(mut self, policy: RateLimitRuntimePolicy) -> Self {
        self.rate_limit_policy = policy;
        self
    }

    pub fn with_required_inbound_auth(self, token: impl Into<String>) -> Self {
        self.with_required_inbound_auth_actions(
            token,
            vec![
                PermissionAction::ChatCompletions,
                PermissionAction::ValidateNormalizationProposal,
                PermissionAction::GenerateNormalizationProposal,
                PermissionAction::SubmitNormalizationProposal,
                PermissionAction::IngestKnowledge,
                PermissionAction::RetrieveKnowledge,
                PermissionAction::ViewGraphContext,
            ],
        )
    }

    pub fn with_required_inbound_auth_actions(
        mut self,
        token: impl Into<String>,
        actions: Vec<PermissionAction>,
    ) -> Self {
        self.inbound_auth = Some(InboundAuthConfig {
            required: true,
            shared_token: Some(token.into()),
            principal: Some(InboundPrincipalConfig::new(
                "service:intelligence-api-test",
                "tenant:local",
                "intelligence-platform",
                actions,
            )),
            allowed_origins: vec![],
        });
        self
    }

    pub fn with_foundation_submitter<T>(mut self, submitter: Arc<T>) -> Self
    where
        T: FoundationNormalizationSubmitter + 'static,
    {
        self.foundation_submitter = Some(submitter);
        self
    }

    pub fn with_proposal_generator<T>(mut self, generator: Arc<T>) -> Self
    where
        T: NormalizationProposalGenerator + 'static,
    {
        self.proposal_generator = Some(generator);
        self
    }

    pub fn with_model_gateway<T>(mut self, gateway: Arc<T>) -> Self
    where
        T: ModelGateway + 'static,
    {
        self.model_gateway = Some(gateway);
        self
    }

    pub fn with_model_gateway_dyn(mut self, gateway: Arc<dyn ModelGateway>) -> Self {
        self.model_gateway = Some(gateway);
        self
    }

    pub fn with_chat_model_id(mut self, model_id: impl Into<String>) -> Self {
        let mut model_ids = (*self.chat_model_ids).clone();
        let model_id = model_id.into();
        if !model_id.trim().is_empty() && !model_ids.contains(&model_id) {
            model_ids.push(model_id);
        }
        self.chat_model_ids = Arc::new(model_ids);
        self
    }
}

/// Builds a [`PostgresWorkflowStateConfig`] from a lookup function.
///
/// Returns `None` when `DATABASE_URL` is absent or empty (fall back to the
/// in-memory adapter).  Returns an error when a present variable is
/// unparseable or produces an invalid config.
fn postgres_workflow_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<PostgresWorkflowStateConfig>, FoundationSubmissionError> {
    let Some(database_url) = lookup("DATABASE_URL").filter(|u| !u.trim().is_empty()) else {
        return Ok(None);
    };

    let timeout_seconds: u64 = match lookup("DATABASE_TIMEOUT_SECONDS") {
        None => 10,
        Some(v) => v
            .parse()
            .map_err(|e| FoundationSubmissionError::InvalidResponse {
                message: format!("DATABASE_TIMEOUT_SECONDS is invalid: {e}"),
            })?,
    };

    let mut config =
        PostgresWorkflowStateConfig::new(database_url, timeout_seconds).map_err(|e| {
            FoundationSubmissionError::InvalidResponse {
                message: format!(
                    "DATABASE_URL/DATABASE_TIMEOUT_SECONDS produced an invalid postgres workflow state config: {e}"
                ),
            }
        })?;

    if let Some(max_conn_str) = lookup("DATABASE_MAX_CONNECTIONS") {
        let n: u32 =
            max_conn_str
                .parse()
                .map_err(|e| FoundationSubmissionError::InvalidResponse {
                    message: format!("DATABASE_MAX_CONNECTIONS is invalid: {e}"),
                })?;
        config =
            config
                .with_max_connections(n)
                .map_err(|e| FoundationSubmissionError::InvalidResponse {
                    message: format!(
                        "DATABASE_MAX_CONNECTIONS produced an invalid postgres workflow state config: {e}"
                    ),
                })?;
    }

    Ok(Some(config))
}

async fn rate_limiter_from_env() -> Result<Option<ConfiguredRateLimiter>, FoundationSubmissionError>
{
    rate_limiter_from_lookup(|key| env::var(key).ok()).await
}

pub async fn rate_limiter_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<ConfiguredRateLimiter>, FoundationSubmissionError> {
    match lookup("INTELLIGENCE_RATE_LIMIT_MODE")
        .unwrap_or_else(|| "disabled".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "disabled" | "" => Ok(None),
        "memory" => {
            let policy = rate_limit_policy_from_lookup(&lookup)?;
            let key_prefix =
                lookup("INTELLIGENCE_RATE_LIMIT_PREFIX").unwrap_or_else(|| "ip".to_string());
            let limiter =
                MemoryRateLimiter::new(MemoryRateLimitConfig { key_prefix }).map_err(|error| {
                    FoundationSubmissionError::InvalidResponse {
                        message: error.to_string(),
                    }
                })?;
            Ok(Some(ConfiguredRateLimiter {
                limiter: Arc::new(limiter),
                policy,
            }))
        }
        "redis" => {
            let redis_url = lookup("INTELLIGENCE_RATE_LIMIT_REDIS_URL").ok_or_else(|| {
                FoundationSubmissionError::InvalidResponse {
                    message: "INTELLIGENCE_RATE_LIMIT_REDIS_URL is required".to_string(),
                }
            })?;
            let policy = rate_limit_policy_from_lookup(&lookup)?;
            let ttl_seconds = parse_env_u64(&lookup, "INTELLIGENCE_RATE_LIMIT_TTL_SECONDS", 600)?;
            let timeout_ms = parse_env_u64(&lookup, "INTELLIGENCE_RATE_LIMIT_TIMEOUT_MS", 50)?;
            let key_prefix =
                lookup("INTELLIGENCE_RATE_LIMIT_PREFIX").unwrap_or_else(|| "ip".to_string());
            let limiter = RedisRateLimiter::connect(RedisRateLimitConfig {
                redis_url,
                key_prefix,
                ttl_seconds,
                timeout_ms,
            })
            .await
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })?;
            Ok(Some(ConfiguredRateLimiter {
                limiter: Arc::new(limiter),
                policy,
            }))
        }
        other => Err(FoundationSubmissionError::InvalidResponse {
            message: format!("INTELLIGENCE_RATE_LIMIT_MODE is invalid: {other}"),
        }),
    }
}

fn rate_limit_policy_from_lookup(
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<RateLimitRuntimePolicy, FoundationSubmissionError> {
    let default_capacity =
        parse_env_positive_u32(lookup, "INTELLIGENCE_RATE_LIMIT_DEFAULT_CAPACITY", 60)?;
    let default_refill_per_second = parse_env_positive_f64(
        lookup,
        "INTELLIGENCE_RATE_LIMIT_DEFAULT_REFILL_PER_SECOND",
        1.0,
    )?;
    let default_cost = parse_env_positive_u32(lookup, "INTELLIGENCE_RATE_LIMIT_DEFAULT_COST", 1)?;
    let default = RateLimitRoutePolicy {
        quota: RateLimitQuota {
            capacity: default_capacity,
            refill_per_second: default_refill_per_second,
        },
        cost: default_cost,
    }
    .validate()?;

    let mut policy = RateLimitRuntimePolicy {
        default,
        overrides: BTreeMap::new(),
    };

    for route_class in [
        RateLimitRouteClass::Chat,
        RateLimitRouteClass::Retrieval,
        RateLimitRouteClass::GraphContext,
        RateLimitRouteClass::NormalizationSubmit,
        RateLimitRouteClass::BatchControl,
    ] {
        let suffix = rate_limit_env_suffix(route_class);
        let capacity = parse_env_positive_u32(
            lookup,
            &format!("INTELLIGENCE_RATE_LIMIT_{suffix}_CAPACITY"),
            default_capacity,
        )?;
        let refill_per_second = parse_env_positive_f64(
            lookup,
            &format!("INTELLIGENCE_RATE_LIMIT_{suffix}_REFILL_PER_SECOND"),
            default_refill_per_second,
        )?;
        let cost = parse_env_positive_u32(
            lookup,
            &format!("INTELLIGENCE_RATE_LIMIT_{suffix}_COST"),
            default_cost,
        )?;
        policy = policy.with_route_policy(
            route_class,
            RateLimitRoutePolicy {
                quota: RateLimitQuota {
                    capacity,
                    refill_per_second,
                },
                cost,
            }
            .validate()?,
        );
    }

    Ok(policy)
}

fn rate_limit_env_suffix(route_class: RateLimitRouteClass) -> &'static str {
    match route_class {
        RateLimitRouteClass::Chat => "CHAT",
        RateLimitRouteClass::Retrieval => "RETRIEVAL",
        RateLimitRouteClass::GraphContext => "GRAPH_CONTEXT",
        RateLimitRouteClass::NormalizationSubmit => "NORMALIZATION_SUBMIT",
        RateLimitRouteClass::BatchControl => "BATCH_CONTROL",
    }
}

fn parse_env_positive_u32(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u32,
) -> Result<u32, FoundationSubmissionError> {
    let value = parse_env_u32(lookup, key, default)?;
    if value == 0 {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: format!("{key} must be positive"),
        });
    }
    Ok(value)
}

fn parse_env_positive_f64(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: f64,
) -> Result<f64, FoundationSubmissionError> {
    let value = match lookup(key) {
        Some(value) => {
            value
                .parse()
                .map_err(|error| FoundationSubmissionError::InvalidResponse {
                    message: format!("{key} is invalid: {error}"),
                })?
        }
        None => default,
    };
    if !value.is_finite() || value <= 0.0 {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: format!("{key} must be a positive finite number"),
        });
    }
    Ok(value)
}

fn parse_env_u32(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u32,
) -> Result<u32, FoundationSubmissionError> {
    match lookup(key) {
        Some(value) => value
            .parse()
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: format!("{key} is invalid: {error}"),
            }),
        None => Ok(default),
    }
}

fn parse_env_u64(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u64,
) -> Result<u64, FoundationSubmissionError> {
    match lookup(key) {
        Some(value) => value
            .parse()
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: format!("{key} is invalid: {error}"),
            }),
        None => Ok(default),
    }
}

fn foundation_platform_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<FoundationPlatformNormalizationConfig>, FoundationSubmissionError> {
    let Some(base_url) = lookup("FOUNDATION_PLATFORM_BASE_URL") else {
        return Ok(None);
    };

    let submission_path = lookup("FOUNDATION_PLATFORM_NORMALIZATION_PATH")
        .unwrap_or_else(|| "/internal/normalization/proposals".to_string());
    let workload_token_provider = foundation_platform_workload_token_provider_from_lookup(&lookup)?;
    if workload_token_provider.is_none() {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE is required when the Foundation Platform base URL is set".to_string(),
        });
    }
    let timeout_seconds = lookup("FOUNDATION_PLATFORM_TIMEOUT_SECONDS")
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);

    Ok(Some(FoundationPlatformNormalizationConfig {
        base_url,
        submission_path,
        workload_token_provider,
        timeout_seconds,
    }))
}

fn foundation_platform_workload_token_provider_from_lookup(
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<Option<WorkloadTokenProvider>, FoundationSubmissionError> {
    if let Some(token_file) =
        lookup("FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE")
            .filter(|value| !value.trim().is_empty())
    {
        return WorkloadTokenProvider::from_file(token_file.trim()).map(Some);
    }
    Ok(None)
}

type ModelGatewayEnv = (Arc<dyn ModelGateway>, ModelRuntimeEnvConfig);

fn model_gateway_from_env() -> Result<Option<ModelGatewayEnv>, FoundationSubmissionError> {
    let Some(config) = model_runtime_config_from_lookup(|key| env::var(key).ok())? else {
        return Ok(None);
    };

    let gateway: Arc<dyn ModelGateway> = if config.chat_path.trim() == "/api/chat" {
        Arc::new(
            OllamaNativeModelGateway::new(OllamaNativeModelGatewayConfig {
                base_url: config.base_url.clone(),
                chat_path: config.chat_path.clone(),
                api_key: config.api_key.clone(),
                default_model: config.default_model.clone(),
                timeout_seconds: config.timeout_seconds,
            })
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })?,
        )
    } else {
        Arc::new(
            OpenAiCompatibleModelGateway::new(OpenAiCompatibleModelGatewayConfig {
                base_url: config.base_url.clone(),
                chat_path: config.chat_path.clone(),
                api_key: config.api_key.clone(),
                default_model: config.default_model.clone(),
                timeout_seconds: config.timeout_seconds,
            })
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })?,
        )
    };

    Ok(Some((gateway, config)))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelRuntimeEnvConfig {
    base_url: String,
    chat_path: String,
    api_key: Option<String>,
    default_model: String,
    profile_id: String,
    timeout_seconds: u64,
    reasoning_effort: Option<ModelReasoningEffort>,
}

fn model_runtime_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<ModelRuntimeEnvConfig>, FoundationSubmissionError> {
    let Some(base_url) = env_value(&lookup, "MODEL_RUNTIME_BASE_URL", "MODEL_GATEWAY_BASE_URL")
    else {
        return Ok(None);
    };
    let Some(default_model) = env_value(
        &lookup,
        "MODEL_RUNTIME_DEFAULT_MODEL",
        "MODEL_GATEWAY_DEFAULT_MODEL",
    ) else {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "MODEL_RUNTIME_DEFAULT_MODEL is required when MODEL_RUNTIME_BASE_URL is set"
                .to_string(),
        });
    };
    let chat_path = env_value(
        &lookup,
        "MODEL_RUNTIME_CHAT_PATH",
        "MODEL_GATEWAY_CHAT_PATH",
    )
    .unwrap_or_else(|| "/v1/chat/completions".to_string());
    let api_key = env_value(&lookup, "MODEL_RUNTIME_API_KEY", "MODEL_GATEWAY_API_KEY")
        .filter(|value| !value.is_empty());
    let timeout_seconds = env_value(
        &lookup,
        "MODEL_RUNTIME_TIMEOUT_SECONDS",
        "MODEL_GATEWAY_TIMEOUT_SECONDS",
    )
    .and_then(|value| value.parse().ok())
    .unwrap_or(30);
    let profile_id = env_value(
        &lookup,
        "MODEL_RUNTIME_PROFILE_ID",
        "MODEL_GATEWAY_PROFILE_ID",
    )
    .unwrap_or_else(|| "normalization-default".to_string());
    let reasoning_effort = env_value(
        &lookup,
        "MODEL_RUNTIME_REASONING_EFFORT",
        "MODEL_GATEWAY_REASONING_EFFORT",
    )
    .map(|value| parse_reasoning_effort(&value))
    .transpose()?;

    Ok(Some(ModelRuntimeEnvConfig {
        base_url,
        chat_path,
        api_key,
        default_model,
        profile_id,
        timeout_seconds,
        reasoning_effort,
    }))
}

fn parse_reasoning_effort(value: &str) -> Result<ModelReasoningEffort, FoundationSubmissionError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(ModelReasoningEffort::None),
        "low" => Ok(ModelReasoningEffort::Low),
        "medium" => Ok(ModelReasoningEffort::Medium),
        "high" => Ok(ModelReasoningEffort::High),
        _ => Err(FoundationSubmissionError::InvalidResponse {
            message: "MODEL_RUNTIME_REASONING_EFFORT must be one of none, low, medium, high"
                .to_string(),
        }),
    }
}

fn env_value(
    lookup: &impl Fn(&str) -> Option<String>,
    primary_key: &str,
    fallback_key: &str,
) -> Option<String> {
    lookup(primary_key).or_else(|| lookup(fallback_key))
}

/// Consolidated runtime configuration: bind address + inbound auth.
///
/// Produced by [`api_runtime_config_from_lookup`]; stored before any socket is opened so the
/// fail-closed guard runs before the OS allocates a port.
#[derive(Clone, Debug)]
pub struct ApiRuntimeConfig {
    pub bind_address: SocketAddr,
    pub inbound_auth: InboundAuthConfig,
}

pub fn api_runtime_config_from_env() -> Result<ApiRuntimeConfig, FoundationSubmissionError> {
    api_runtime_config_from_lookup(|key| env::var(key).ok())
}

pub fn api_runtime_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<ApiRuntimeConfig, FoundationSubmissionError> {
    let bind_address = api_bind_address_from_lookup(&lookup)?;

    let inbound_auth = inbound_auth_config_from_lookup(&lookup).map_err(|error| {
        FoundationSubmissionError::InvalidResponse {
            message: error.to_string(),
        }
    })?;

    if !bind_address.ip().is_loopback() && !inbound_auth.required {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "inbound authentication is required for non-loopback bind addresses: set INTELLIGENCE_INBOUND_AUTH_MODE=shared-token and INTELLIGENCE_INBOUND_SERVICE_TOKEN, or bind loopback via INTELLIGENCE_API_BIND=127.0.0.1:8010"
                .to_string(),
        });
    }

    Ok(ApiRuntimeConfig {
        bind_address,
        inbound_auth,
    })
}

fn api_bind_address_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<SocketAddr, FoundationSubmissionError> {
    let value = lookup("INTELLIGENCE_API_BIND").unwrap_or_else(|| "127.0.0.1:8010".to_string());
    value
        .parse()
        .map_err(|error| FoundationSubmissionError::InvalidResponse {
            message: format!("INTELLIGENCE_API_BIND is invalid: {error}"),
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use intelligence_normalization_application::{ModelReasoningEffort, RateLimitRouteClass};

    use super::{
        api_bind_address_from_lookup, foundation_platform_config_from_lookup,
        model_runtime_config_from_lookup, postgres_workflow_config_from_lookup,
        rate_limiter_from_lookup, FoundationSubmissionError, ModelRuntimeEnvConfig,
    };

    #[test]
    fn model_runtime_env_names_take_precedence_over_legacy_gateway_names() {
        let values = BTreeMap::from([
            (
                "MODEL_RUNTIME_BASE_URL",
                "http://model-runtime.internal:11434",
            ),
            ("MODEL_GATEWAY_BASE_URL", "http://open-webui.internal:8080"),
            ("MODEL_RUNTIME_CHAT_PATH", "/v1/chat/completions"),
            ("MODEL_GATEWAY_CHAT_PATH", "/api/chat/completions"),
            ("MODEL_RUNTIME_DEFAULT_MODEL", "gemma-ko:latest"),
            ("MODEL_GATEWAY_DEFAULT_MODEL", "legacy-model"),
            ("MODEL_RUNTIME_PROFILE_ID", "normalization-ko"),
            ("MODEL_RUNTIME_TIMEOUT_SECONDS", "45"),
            ("MODEL_RUNTIME_REASONING_EFFORT", "none"),
        ]);

        let config =
            model_runtime_config_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .unwrap()
                .unwrap();

        assert_eq!(
            config,
            ModelRuntimeEnvConfig {
                base_url: "http://model-runtime.internal:11434".to_string(),
                chat_path: "/v1/chat/completions".to_string(),
                api_key: None,
                default_model: "gemma-ko:latest".to_string(),
                profile_id: "normalization-ko".to_string(),
                timeout_seconds: 45,
                reasoning_effort: Some(ModelReasoningEffort::None),
            }
        );
    }

    #[test]
    fn legacy_model_gateway_env_names_remain_supported() {
        let values = BTreeMap::from([
            ("MODEL_GATEWAY_BASE_URL", "http://model-runtime.test:11434"),
            ("MODEL_GATEWAY_DEFAULT_MODEL", "gemma-ko:latest"),
        ]);

        let config =
            model_runtime_config_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .unwrap()
                .unwrap();

        assert_eq!(config.base_url, "http://model-runtime.test:11434");
        assert_eq!(config.default_model, "gemma-ko:latest");
        assert_eq!(config.chat_path, "/v1/chat/completions");
    }

    #[test]
    fn default_model_is_required_when_runtime_base_url_is_set() {
        let values = BTreeMap::from([("MODEL_RUNTIME_BASE_URL", "http://model-runtime")]);

        let error =
            model_runtime_config_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .unwrap_err();

        let FoundationSubmissionError::InvalidResponse { message } = error else {
            panic!("expected invalid response error");
        };
        assert!(message.contains("MODEL_RUNTIME_DEFAULT_MODEL"));
    }

    #[test]
    fn foundation_workload_token_file_configures_normalization_submitter() {
        let token_path = std::env::temp_dir().join(format!(
            "intelligence-platform-token-{}.txt",
            std::process::id()
        ));
        std::fs::write(&token_path, "zitadel-workload-token\n").unwrap();

        let values = BTreeMap::from([
            (
                "FOUNDATION_PLATFORM_BASE_URL",
                "https://foundation-api:3000",
            ),
            (
                "FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE",
                token_path.to_str().unwrap(),
            ),
        ]);

        let config = foundation_platform_config_from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap()
        .unwrap();

        assert_eq!(config.base_url, "https://foundation-api:3000");
        assert_eq!(config.submission_path, "/internal/normalization/proposals");
        assert!(config.workload_token_provider.is_some());

        std::fs::remove_file(token_path).unwrap();
    }

    #[test]
    fn foundation_platform_rejects_static_service_tokens() {
        let values = BTreeMap::from([
            (
                "FOUNDATION_PLATFORM_BASE_URL",
                "https://foundation-api:3000",
            ),
            (
                "FOUNDATION_PLATFORM_INTELLIGENCE_SERVICE_TOKEN",
                "foundation-static-token",
            ),
        ]);

        let error = foundation_platform_config_from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap_err();

        let FoundationSubmissionError::InvalidResponse { message } = error else {
            panic!("expected invalid response error");
        };
        assert!(message
            .contains("FOUNDATION_PLATFORM_INTELLIGENCE_WORKLOAD_IDENTITY_TOKEN_FILE is required"));
        assert!(!message.contains("SERVICE_TOKEN"));
    }

    #[test]
    fn api_bind_address_defaults_to_localhost_and_accepts_network_bind_override() {
        let default_address = api_bind_address_from_lookup(|_| None).unwrap();
        assert_eq!(default_address.to_string(), "127.0.0.1:8010");

        let values = BTreeMap::from([("INTELLIGENCE_API_BIND", "0.0.0.0:8010")]);
        let override_address =
            api_bind_address_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .unwrap();

        assert_eq!(override_address.to_string(), "0.0.0.0:8010");
    }

    #[test]
    fn postgres_workflow_config_absent_database_url_returns_none() {
        let result = postgres_workflow_config_from_lookup(|_| None).unwrap();
        assert!(result.is_none(), "absent DATABASE_URL must return None");
    }

    #[test]
    fn postgres_workflow_config_empty_database_url_returns_none() {
        let values = BTreeMap::from([("DATABASE_URL", "   ")]);
        let result =
            postgres_workflow_config_from_lookup(|key| values.get(key).map(|v| v.to_string()))
                .unwrap();
        assert!(result.is_none(), "whitespace DATABASE_URL must return None");
    }

    #[test]
    fn postgres_workflow_config_invalid_timeout_errors_with_var_name() {
        let values = BTreeMap::from([
            ("DATABASE_URL", "postgres://localhost/test"),
            ("DATABASE_TIMEOUT_SECONDS", "not-a-number"),
        ]);
        let err =
            postgres_workflow_config_from_lookup(|key| values.get(key).map(|v| v.to_string()))
                .unwrap_err();
        let FoundationSubmissionError::InvalidResponse { message } = err else {
            panic!("expected InvalidResponse; got {err:?}");
        };
        assert!(
            message.contains("DATABASE_TIMEOUT_SECONDS is invalid"),
            "error must name the env var; got: {message}"
        );
    }

    #[test]
    fn postgres_workflow_config_absent_timeout_defaults_to_ten() {
        let values = BTreeMap::from([("DATABASE_URL", "postgres://localhost/test")]);
        let config =
            postgres_workflow_config_from_lookup(|key| values.get(key).map(|v| v.to_string()))
                .unwrap()
                .unwrap();
        assert_eq!(config.timeout_seconds(), 10);
    }

    #[test]
    fn postgres_workflow_config_invalid_max_connections_errors_with_var_name() {
        let values = BTreeMap::from([
            ("DATABASE_URL", "postgres://localhost/test"),
            ("DATABASE_MAX_CONNECTIONS", "oops"),
        ]);
        let err =
            postgres_workflow_config_from_lookup(|key| values.get(key).map(|v| v.to_string()))
                .unwrap_err();
        let FoundationSubmissionError::InvalidResponse { message } = err else {
            panic!("expected InvalidResponse; got {err:?}");
        };
        assert!(
            message.contains("DATABASE_MAX_CONNECTIONS is invalid"),
            "error must name the env var; got: {message}"
        );
    }

    #[test]
    fn postgres_workflow_config_zero_max_connections_errors_with_var_name() {
        let values = BTreeMap::from([
            ("DATABASE_URL", "postgres://localhost/test"),
            ("DATABASE_MAX_CONNECTIONS", "0"),
        ]);
        let err =
            postgres_workflow_config_from_lookup(|key| values.get(key).map(|v| v.to_string()))
                .unwrap_err();
        let FoundationSubmissionError::InvalidResponse { message } = err else {
            panic!("expected InvalidResponse; got {err:?}");
        };
        assert!(
            message.contains("DATABASE_MAX_CONNECTIONS"),
            "error must mention DATABASE_MAX_CONNECTIONS; got: {message}"
        );
    }

    #[test]
    fn postgres_workflow_config_explicit_max_connections_is_stored() {
        let values = BTreeMap::from([
            ("DATABASE_URL", "postgres://localhost/test"),
            ("DATABASE_MAX_CONNECTIONS", "25"),
        ]);
        let config =
            postgres_workflow_config_from_lookup(|key| values.get(key).map(|v| v.to_string()))
                .unwrap()
                .unwrap();
        assert_eq!(config.max_connections(), 25);
    }

    #[tokio::test]
    async fn rate_limiter_lookup_disabled_returns_none() {
        let limiter = rate_limiter_from_lookup(|_| None).await.unwrap();
        assert!(limiter.is_none());
    }

    #[tokio::test]
    async fn rate_limiter_lookup_invalid_mode_names_env_var() {
        let values = BTreeMap::from([("INTELLIGENCE_RATE_LIMIT_MODE", "banana")]);
        let error =
            match rate_limiter_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .await
            {
                Ok(_) => panic!("expected error"),
                Err(error) => error,
            };

        let FoundationSubmissionError::InvalidResponse { message } = error else {
            panic!("expected invalid response error");
        };
        assert!(message.contains("INTELLIGENCE_RATE_LIMIT_MODE"));
    }

    #[tokio::test]
    async fn rate_limiter_lookup_requires_redis_url_in_redis_mode() {
        let values = BTreeMap::from([("INTELLIGENCE_RATE_LIMIT_MODE", "redis")]);
        let error =
            match rate_limiter_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .await
            {
                Ok(_) => panic!("expected error"),
                Err(error) => error,
            };

        let FoundationSubmissionError::InvalidResponse { message } = error else {
            panic!("expected invalid response error");
        };
        assert!(message.contains("INTELLIGENCE_RATE_LIMIT_REDIS_URL"));
    }

    #[tokio::test]
    async fn rate_limiter_lookup_accepts_memory_mode_and_route_policy_overrides() {
        let values = BTreeMap::from([
            ("INTELLIGENCE_RATE_LIMIT_MODE", "memory"),
            ("INTELLIGENCE_RATE_LIMIT_CHAT_CAPACITY", "7"),
            ("INTELLIGENCE_RATE_LIMIT_CHAT_REFILL_PER_SECOND", "2.5"),
            ("INTELLIGENCE_RATE_LIMIT_CHAT_COST", "3"),
        ]);

        let configured =
            rate_limiter_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .await
                .unwrap()
                .unwrap();

        let chat = configured.policy.for_route(RateLimitRouteClass::Chat);
        assert_eq!(chat.quota.capacity, 7);
        assert_eq!(chat.quota.refill_per_second, 2.5);
        assert_eq!(chat.cost, 3);
    }

    #[tokio::test]
    async fn rate_limiter_lookup_invalid_route_refill_names_env_var() {
        let values = BTreeMap::from([
            ("INTELLIGENCE_RATE_LIMIT_MODE", "memory"),
            ("INTELLIGENCE_RATE_LIMIT_CHAT_REFILL_PER_SECOND", "oops"),
        ]);
        let error =
            match rate_limiter_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .await
            {
                Ok(_) => panic!("expected error"),
                Err(error) => error,
            };

        let FoundationSubmissionError::InvalidResponse { message } = error else {
            panic!("expected invalid response error");
        };
        assert!(message.contains("INTELLIGENCE_RATE_LIMIT_CHAT_REFILL_PER_SECOND"));
    }

    #[tokio::test]
    async fn rate_limiter_lookup_invalid_capacity_names_env_var() {
        let values = BTreeMap::from([
            ("INTELLIGENCE_RATE_LIMIT_MODE", "redis"),
            ("INTELLIGENCE_RATE_LIMIT_REDIS_URL", "redis://127.0.0.1:1/"),
            ("INTELLIGENCE_RATE_LIMIT_DEFAULT_CAPACITY", "oops"),
        ]);
        let error =
            match rate_limiter_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .await
            {
                Ok(_) => panic!("expected error"),
                Err(error) => error,
            };

        let FoundationSubmissionError::InvalidResponse { message } = error else {
            panic!("expected invalid response error");
        };
        assert!(message.contains("INTELLIGENCE_RATE_LIMIT_DEFAULT_CAPACITY"));
    }

    #[tokio::test]
    async fn rate_limiter_lookup_redis_mode_does_not_require_live_connection() {
        let values = BTreeMap::from([
            ("INTELLIGENCE_RATE_LIMIT_MODE", "redis"),
            ("INTELLIGENCE_RATE_LIMIT_REDIS_URL", "redis://127.0.0.1:1/"),
        ]);

        let limiter =
            rate_limiter_from_lookup(|key| values.get(key).map(|value| value.to_string()))
                .await
                .unwrap();

        assert!(limiter.is_some());
    }
}
