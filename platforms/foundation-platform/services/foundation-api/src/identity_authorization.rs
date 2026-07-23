//! Foundation authorization boundary backed by Identity Platform.

use async_trait::async_trait;
use std::time::Duration;
use uuid::Uuid;

use crate::identity_http_client::HttpIdentityClient;
use crate::identity_token_verifier::{
    IdentityTokenVerificationError, IdentityTokenVerifier, VerifiedPrincipalKind,
};

/// Principal data Foundation may retain after authorization succeeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedPrincipal {
    pub principal_id: Uuid,
    pub trace_id: String,
}

/// Principal kind a Foundation route accepts before consulting Identity policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredPrincipalKind {
    Staff,
    Service,
}

impl RequiredPrincipalKind {
    const fn accepts(self, verified: VerifiedPrincipalKind) -> bool {
        matches!(
            (self, verified),
            (Self::Staff, VerifiedPrincipalKind::Staff)
                | (Self::Service, VerifiedPrincipalKind::Service)
        )
    }
}

/// Fail-closed authorization outcomes exposed to Foundation routes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityAuthorizationError {
    Unauthorized,
    Forbidden,
    Unavailable,
}

impl std::fmt::Display for IdentityAuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unauthorized => "identity credential is unauthorized",
            Self::Forbidden => "identity policy denied the action",
            Self::Unavailable => "identity authorization is unavailable",
        })
    }
}

impl std::error::Error for IdentityAuthorizationError {}

/// Sole Foundation port for staff and service authorization.
#[async_trait]
pub trait IdentityAuthorization: Send + Sync {
    async fn authorize(
        &self,
        bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError>;
}

/// Local token verification followed by one Identity policy decision.
pub struct HttpIdentityAuthorization {
    token_verifier: IdentityTokenVerifier,
    identity_client: HttpIdentityClient,
    authorization_timeout: Duration,
}

impl HttpIdentityAuthorization {
    #[must_use]
    pub const fn new(
        token_verifier: IdentityTokenVerifier,
        identity_client: HttpIdentityClient,
        authorization_timeout: Duration,
    ) -> Self {
        Self {
            token_verifier,
            identity_client,
            authorization_timeout,
        }
    }
}

#[async_trait]
impl IdentityAuthorization for HttpIdentityAuthorization {
    async fn authorize(
        &self,
        bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        tokio::time::timeout(self.authorization_timeout, async {
            let verified_principal_kind =
                self.token_verifier
                    .verify(bearer)
                    .await
                    .map_err(|error| match error {
                        IdentityTokenVerificationError::Unauthorized => {
                            IdentityAuthorizationError::Unauthorized
                        }
                        IdentityTokenVerificationError::Infrastructure => {
                            IdentityAuthorizationError::Unavailable
                        }
                    })?;
            if !required_principal_kind.accepts(verified_principal_kind) {
                return Err(IdentityAuthorizationError::Forbidden);
            }

            let principal_id = self
                .identity_client
                .authorize(bearer, resource, action, resource_id, trace_id)
                .await?;
            Ok(AuthorizedPrincipal {
                principal_id,
                trace_id: trace_id.to_owned(),
            })
        })
        .await
        .unwrap_or(Err(IdentityAuthorizationError::Unavailable))
    }
}
