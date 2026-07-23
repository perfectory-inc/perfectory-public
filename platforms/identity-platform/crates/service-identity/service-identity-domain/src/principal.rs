//! Validated service principal model.

use authorization_domain::Permission;
use identity_contracts::PrincipalId;

/// Service principal accepted only after credential verification succeeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedServicePrincipal {
    /// Stable public identifier for the service principal.
    pub principal_id: PrincipalId,
    /// Identity-owned capabilities granted to the service principal.
    pub capabilities: Vec<Permission>,
}
