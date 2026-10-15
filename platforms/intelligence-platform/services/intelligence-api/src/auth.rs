use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::Response,
    Json,
};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use subtle::ConstantTimeEq;

use intelligence_normalization_application::{
    IdentityPolicyDecision, IdentityPolicyDecisionPort, IdentityPolicyError, PermissionAction,
    PermissionCheck, PrincipalKind, PrincipalScope, RuntimeSecurityError, VerifiedPrincipal,
};
use intelligence_normalization_infrastructure::HttpIdentityPolicyClient;

use crate::state::AppState;

/// Runtime configuration for inbound authentication.
#[derive(Clone, Default)]
pub struct InboundAuthConfig {
    pub required: bool,
    pub shared_token: Option<String>,
    pub principal: Option<InboundPrincipalConfig>,
    /// `identity` mode: every protected call is decided by Identity (root ADR-0216).
    pub identity_policy: Option<Arc<dyn IdentityPolicyDecisionPort>>,
    pub allowed_origins: Vec<String>,
}

const IDENTITY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IDENTITY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Identity and permissions bound to the configured inbound service token.
///
/// The request cannot override any of these values with headers. A shared token
/// authenticates exactly one configured workload identity; it is not a carrier
/// for caller-supplied tenant or user claims.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundPrincipalConfig {
    pub subject_id: String,
    pub tenant_id: String,
    pub product_id: String,
    pub actions: Vec<PermissionAction>,
}

impl InboundPrincipalConfig {
    #[must_use]
    pub fn new(
        subject_id: impl Into<String>,
        tenant_id: impl Into<String>,
        product_id: impl Into<String>,
        actions: Vec<PermissionAction>,
    ) -> Self {
        Self {
            subject_id: subject_id.into(),
            tenant_id: tenant_id.into(),
            product_id: product_id.into(),
            actions,
        }
    }

    #[must_use]
    pub fn local_test() -> Self {
        Self::new(
            "service:intelligence-api-test",
            "tenant:local",
            "intelligence-platform",
            all_actions(),
        )
    }
}

impl std::fmt::Debug for InboundAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundAuthConfig")
            .field("required", &self.required)
            .field(
                "shared_token",
                &self.shared_token.as_ref().map(|_| "<redacted>"),
            )
            .field("principal", &self.principal)
            .field("identity_policy", &self.identity_policy.is_some())
            .field("allowed_origins", &self.allowed_origins)
            .finish()
    }
}

/// Parses inbound auth configuration from an environment-variable lookup function.
///
/// Supported modes:
/// - `"disabled"` (default): authentication is not enforced.
/// - `"identity"` (production): the caller presents its Identity workload token; each protected
///   action is decided by Identity's `POST /identity/v1/policy/decisions` at
///   `INTELLIGENCE_IDENTITY_BASE_URL`. Who the caller is and what it may do come only from
///   Identity's workload policy (root ADR-0216).
/// - `"shared-token"`: loopback-only local development. A bearer token and an explicitly bound
///   principal read from `INTELLIGENCE_INBOUND_SERVICE_*` variables. Request headers are never
///   used as identity or authorization input.
///
/// `INTELLIGENCE_CORS_ALLOWED_ORIGINS` is a comma-separated list of allowed origins.
pub fn inbound_auth_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<InboundAuthConfig, RuntimeSecurityError> {
    let mode = lookup("INTELLIGENCE_INBOUND_AUTH_MODE").unwrap_or_else(|| "disabled".to_string());

    let allowed_origins = lookup("INTELLIGENCE_CORS_ALLOWED_ORIGINS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let required = match mode.trim() {
        "disabled" => false,
        "shared-token" => true,
        "identity" => {
            let base_url = lookup("INTELLIGENCE_IDENTITY_BASE_URL").unwrap_or_default();
            let client = HttpIdentityPolicyClient::new(
                base_url.trim(),
                IDENTITY_CONNECT_TIMEOUT,
                IDENTITY_REQUEST_TIMEOUT,
            )
            .map_err(|message| RuntimeSecurityError::AuthenticationFailed {
                message: format!("INTELLIGENCE_IDENTITY_BASE_URL: {message}"),
            })?;
            return Ok(InboundAuthConfig {
                required: true,
                shared_token: None,
                principal: None,
                identity_policy: Some(Arc::new(client)),
                allowed_origins,
            });
        }
        _ => {
            return Err(RuntimeSecurityError::AuthenticationFailed {
                message:
                    "INTELLIGENCE_INBOUND_AUTH_MODE must be disabled, shared-token or identity"
                        .to_string(),
            })
        }
    };

    let shared_token = lookup("INTELLIGENCE_INBOUND_SERVICE_TOKEN")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    if required && shared_token.is_none() {
        return Err(RuntimeSecurityError::AuthenticationFailed {
            message: "INTELLIGENCE_INBOUND_SERVICE_TOKEN is required when \
                      INTELLIGENCE_INBOUND_AUTH_MODE is shared-token"
                .to_string(),
        });
    }

    let principal = if required {
        Some(InboundPrincipalConfig::new(
            required_env(&lookup, "INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID")?,
            required_env(&lookup, "INTELLIGENCE_INBOUND_SERVICE_TENANT_ID")?,
            required_env(&lookup, "INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID")?,
            parse_actions(&required_env(
                &lookup,
                "INTELLIGENCE_INBOUND_SERVICE_ACTIONS",
            )?)?,
        ))
    } else {
        None
    };

    Ok(InboundAuthConfig {
        required,
        shared_token,
        principal,
        identity_policy: None,
        allowed_origins,
    })
}

fn required_env(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
) -> Result<String, RuntimeSecurityError> {
    let value = lookup(key).unwrap_or_default();
    let value = value.trim();
    if value.is_empty() {
        return Err(RuntimeSecurityError::AuthenticationFailed {
            message: format!(
                "{key} is required when INTELLIGENCE_INBOUND_AUTH_MODE is shared-token"
            ),
        });
    }
    Ok(value.to_owned())
}

fn parse_actions(value: &str) -> Result<Vec<PermissionAction>, RuntimeSecurityError> {
    let actions = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(parse_action)
        .collect::<Result<Vec<_>, _>>()?;
    if actions.is_empty() {
        return Err(RuntimeSecurityError::AuthenticationFailed {
            message: "INTELLIGENCE_INBOUND_SERVICE_ACTIONS must contain at least one action"
                .to_owned(),
        });
    }
    Ok(actions)
}

fn parse_action(value: &str) -> Result<PermissionAction, RuntimeSecurityError> {
    match value {
        "chat_completions" => Ok(PermissionAction::ChatCompletions),
        "validate_normalization_proposal" => Ok(PermissionAction::ValidateNormalizationProposal),
        "generate_normalization_proposal" => Ok(PermissionAction::GenerateNormalizationProposal),
        "submit_normalization_proposal" => Ok(PermissionAction::SubmitNormalizationProposal),
        "ingest_knowledge" => Ok(PermissionAction::IngestKnowledge),
        "retrieve_knowledge" => Ok(PermissionAction::RetrieveKnowledge),
        "view_graph_context" => Ok(PermissionAction::ViewGraphContext),
        _ => Err(RuntimeSecurityError::AuthenticationFailed {
            message: format!("unknown inbound service action: {value}"),
        }),
    }
}

fn all_actions() -> Vec<PermissionAction> {
    PermissionAction::ALL.to_vec()
}

/// Request extension carrying a verified principal for downstream handlers.
#[derive(Clone, Debug)]
pub struct AuthenticatedRequestPrincipal(pub VerifiedPrincipal);

/// JSON error body returned on authentication or principal validation failure.
#[derive(Debug, Serialize)]
pub struct AuthErrorResponse {
    pub code: &'static str,
    pub message: &'static str,
}

/// Constant-time comparison of `Authorization: Bearer <token>` against the
/// shared token stored in `config`.
///
/// Returns `false` when `config` carries no shared token, when the header is
/// absent, or when it does not match.  Used by both the auth middleware and the
/// `/metrics` handler so the comparison logic lives in exactly one place.
pub(crate) fn bearer_token_matches(headers: &HeaderMap, config: &InboundAuthConfig) -> bool {
    let Some(expected_token) = config.shared_token.as_deref() else {
        return false;
    };
    let auth_header = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let expected = format!("Bearer {expected_token}");
    auth_header.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Axum middleware that enforces inbound shared-token authentication on protected routes.
///
/// When `state.inbound_auth` is `None` or `required` is `false` the request passes
/// through without a principal (local-dev mode). When required, the `Authorization`
/// header is verified with a constant-time comparison; on success the verified
/// principal is inserted as a request extension.
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<AuthErrorResponse>)> {
    let Some(config) = state.inbound_auth.as_ref() else {
        return Ok(next.run(request).await);
    };

    if !config.required {
        return Ok(next.run(request).await);
    }

    if let Some(policy) = config.identity_policy.as_ref() {
        let Some(actions) = required_actions(request.uri().path()) else {
            return Err(auth_error(StatusCode::FORBIDDEN));
        };
        let capabilities: Vec<_> = actions
            .iter()
            .map(PermissionAction::identity_capability)
            .collect();
        let principal_id =
            authorize_with_identity(policy.as_ref(), request.headers(), &capabilities).await?;
        // Identity workload principals carry no tenant: the principal is its own scope.
        let principal = VerifiedPrincipal::new_for_kind(
            principal_id.clone(),
            PrincipalKind::Service,
            vec![PrincipalScope {
                tenant_id: principal_id.clone(),
                product_id: principal_id,
                actions,
            }],
        )
        .map_err(|_| auth_error(StatusCode::UNAUTHORIZED))?;
        request
            .extensions_mut()
            .insert(AuthenticatedRequestPrincipal(principal));
        return Ok(next.run(request).await);
    }

    if !bearer_token_matches(request.headers(), config) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(AuthErrorResponse {
                code: "authentication_failed",
                message: "authentication failed",
            }),
        ));
    }

    let Some(bound_principal) = config.principal.as_ref() else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(AuthErrorResponse {
                code: "principal_invalid",
                message: "principal is invalid",
            }),
        ));
    };

    let scopes = vec![PrincipalScope {
        tenant_id: bound_principal.tenant_id.clone(),
        product_id: bound_principal.product_id.clone(),
        actions: bound_principal.actions.clone(),
    }];

    let principal = VerifiedPrincipal::new_for_kind(
        bound_principal.subject_id.clone(),
        PrincipalKind::Service,
        scopes,
    )
    .map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(AuthErrorResponse {
                code: "principal_invalid",
                message: "principal is invalid",
            }),
        )
    })?;

    let Some(actions) = required_actions(request.uri().path()) else {
        return Err((
            StatusCode::FORBIDDEN,
            Json(AuthErrorResponse {
                code: "authorization_failed",
                message: "authorization failed",
            }),
        ));
    };
    for action in actions {
        let decision = intelligence_normalization_application::PermissionDecision::from_principal(
            &principal,
            &PermissionCheck {
                tenant_id: bound_principal.tenant_id.clone(),
                product_id: bound_principal.product_id.clone(),
                action,
            },
        );
        if !decision.allowed {
            return Err((
                StatusCode::FORBIDDEN,
                Json(AuthErrorResponse {
                    code: "authorization_failed",
                    message: "authorization failed",
                }),
            ));
        }
    }

    request
        .extensions_mut()
        .insert(AuthenticatedRequestPrincipal(principal));

    Ok(next.run(request).await)
}

fn auth_error(status: StatusCode) -> (StatusCode, Json<AuthErrorResponse>) {
    let (code, message) = match status {
        StatusCode::UNAUTHORIZED => ("authentication_failed", "authentication failed"),
        StatusCode::FORBIDDEN => ("authorization_failed", "authorization failed"),
        _ => (
            "authorization_unavailable",
            "authorization is temporarily unavailable",
        ),
    };
    (status, Json(AuthErrorResponse { code, message }))
}

/// Asks Identity for every `(resource, action)` with the caller's bearer and returns the one
/// principal Identity evaluated. Any deny is 403, a rejected credential 401, and an unreachable
/// or off-contract Identity 503 — never an allow.
pub(crate) async fn authorize_with_identity(
    policy: &dyn IdentityPolicyDecisionPort,
    headers: &HeaderMap,
    capabilities: &[(&str, &str)],
) -> Result<String, (StatusCode, Json<AuthErrorResponse>)> {
    let bearer = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| auth_error(StatusCode::UNAUTHORIZED))?;
    let trace_id = uuid::Uuid::new_v4().to_string();
    let mut evaluated: Option<String> = None;
    for (resource, action) in capabilities {
        let decision = policy
            .decide(bearer, resource, action, &trace_id)
            .await
            .map_err(|error| match error {
                IdentityPolicyError::Unauthenticated => auth_error(StatusCode::UNAUTHORIZED),
                IdentityPolicyError::Unavailable => auth_error(StatusCode::SERVICE_UNAVAILABLE),
            })?;
        let IdentityPolicyDecision::Allow { principal_id } = decision else {
            return Err(auth_error(StatusCode::FORBIDDEN));
        };
        if evaluated
            .as_ref()
            .is_some_and(|previous| previous != &principal_id)
        {
            return Err(auth_error(StatusCode::UNAUTHORIZED));
        }
        evaluated = Some(principal_id);
    }
    // No capability asked means nothing was allowed.
    evaluated.ok_or_else(|| auth_error(StatusCode::FORBIDDEN))
}

/// Exact-path permission map covering both route namespaces per root
/// ADR-0001 §6: the OpenAI-compatible surface stays at `/v1/...` (recorded
/// exception) while platform-native routes live under `/intelligence/v1/...`.
/// Unknown paths deny by default (`None` → 403).
fn required_actions(path: &str) -> Option<Vec<PermissionAction>> {
    match path {
        "/v1/models" | "/v1/chat/completions" => Some(vec![PermissionAction::ChatCompletions]),
        "/intelligence/v1/normalization/validate-proposal" => {
            Some(vec![PermissionAction::ValidateNormalizationProposal])
        }
        "/intelligence/v1/normalization/generate-and-validate" => Some(vec![
            PermissionAction::GenerateNormalizationProposal,
            PermissionAction::ValidateNormalizationProposal,
        ]),
        "/intelligence/v1/normalization/generate-validate-submit" => Some(vec![
            PermissionAction::GenerateNormalizationProposal,
            PermissionAction::ValidateNormalizationProposal,
            PermissionAction::SubmitNormalizationProposal,
        ]),
        "/intelligence/v1/normalization/submit-proposal" => {
            Some(vec![PermissionAction::SubmitNormalizationProposal])
        }
        _ => None,
    }
}
