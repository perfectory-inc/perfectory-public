use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrincipalKind {
    User,
    Service,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PermissionAction {
    ChatCompletions,
    ValidateNormalizationProposal,
    GenerateNormalizationProposal,
    SubmitNormalizationProposal,
    IngestKnowledge,
    RetrieveKnowledge,
    ViewGraphContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalScope {
    pub tenant_id: String,
    pub product_id: String,
    pub actions: Vec<PermissionAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPrincipal {
    pub subject_id: String,
    pub kind: PrincipalKind,
    pub scopes: Vec<PrincipalScope>,
}

impl VerifiedPrincipal {
    pub fn service(
        subject_id: impl Into<String>,
        scopes: Vec<PrincipalScope>,
    ) -> Result<Self, RuntimeSecurityError> {
        Self::new(subject_id, PrincipalKind::Service, scopes)
    }

    pub fn user(
        subject_id: impl Into<String>,
        scopes: Vec<PrincipalScope>,
    ) -> Result<Self, RuntimeSecurityError> {
        Self::new(subject_id, PrincipalKind::User, scopes)
    }

    pub fn new_for_kind(
        subject_id: impl Into<String>,
        kind: PrincipalKind,
        scopes: Vec<PrincipalScope>,
    ) -> Result<Self, RuntimeSecurityError> {
        Self::new(subject_id, kind, scopes)
    }

    fn new(
        subject_id: impl Into<String>,
        kind: PrincipalKind,
        scopes: Vec<PrincipalScope>,
    ) -> Result<Self, RuntimeSecurityError> {
        let subject_id = subject_id.into().trim().to_string();
        if subject_id.is_empty() {
            return Err(RuntimeSecurityError::InvalidPrincipal {
                message: "principal subject is required".to_string(),
            });
        }
        if scopes.is_empty()
            || scopes.iter().any(|scope| {
                scope.tenant_id.trim().is_empty()
                    || scope.product_id.trim().is_empty()
                    || scope.actions.is_empty()
            })
        {
            return Err(RuntimeSecurityError::InvalidPrincipal {
                message: "principal tenant scope is invalid".to_string(),
            });
        }

        let scopes = scopes
            .into_iter()
            .map(|scope| PrincipalScope {
                tenant_id: scope.tenant_id.trim().to_string(),
                product_id: scope.product_id.trim().to_string(),
                actions: scope.actions,
            })
            .collect();

        Ok(Self {
            subject_id,
            kind,
            scopes,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionCheck {
    pub tenant_id: String,
    pub product_id: String,
    pub action: PermissionAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionDecision {
    pub allowed: bool,
    pub reason: String,
}

impl PermissionDecision {
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reason: "allowed".to_string(),
        }
    }

    pub fn deny(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: reason.into(),
        }
    }

    pub fn from_principal(principal: &VerifiedPrincipal, check: &PermissionCheck) -> Self {
        let allowed = principal.scopes.iter().any(|scope| {
            scope.tenant_id == check.tenant_id
                && scope.product_id == check.product_id
                && scope.actions.contains(&check.action)
        });
        if allowed {
            Self::allow()
        } else {
            Self::deny("principal is not authorized for tenant, product, and action")
        }
    }
}

#[async_trait]
pub trait PermissionResolverPort: Send + Sync {
    async fn resolve(
        &self,
        principal: &VerifiedPrincipal,
        check: &PermissionCheck,
    ) -> Result<PermissionDecision, RuntimeSecurityError>;
}

#[derive(Debug, Error)]
pub enum RuntimeSecurityError {
    #[error("{message}")]
    InvalidPrincipal { message: String },
    #[error("{message}")]
    AuthenticationFailed { message: String },
    #[error("{message}")]
    AuthorizationFailed { message: String },
}

impl RuntimeSecurityError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidPrincipal { .. } => "principal tenant scope is invalid",
            Self::AuthenticationFailed { .. } => "authentication failed",
            Self::AuthorizationFailed { .. } => "authorization failed",
        }
    }
}

pub fn trace_context_from_principal(
    trace_id: String,
    principal: &VerifiedPrincipal,
    tenant_id: &str,
    product_id: String,
) -> Result<TraceContext, RuntimeSecurityError> {
    if !principal
        .scopes
        .iter()
        .any(|scope| scope.tenant_id == tenant_id)
    {
        return Err(RuntimeSecurityError::AuthorizationFailed {
            message: format!("principal has no scope for tenant {tenant_id}"),
        });
    }
    Ok(TraceContext {
        trace_id,
        tenant_id: tenant_id.to_string(),
        human_user_id: principal.subject_id.clone(),
        product_id,
    })
}
