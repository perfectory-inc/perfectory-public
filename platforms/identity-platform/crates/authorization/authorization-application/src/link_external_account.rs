//! Linking a source's account to a staff member from already verified actor context
//! (root ADR-0569).

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use authorization_domain::{evaluate_policy, PolicyInput, RoleCode};
use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;
use staff_identity_domain::{ExternalAccount, ExternalAccountLink};

use crate::ports::{ExternalAccountLinkPersistenceError, ExternalAccountLinkUnitOfWork};

/// Whether the command ties the account to the staff member or ends the tie.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalAccountLinkChange {
    /// Append a link that holds from `valid_from`.
    Link {
        /// When the link starts to hold; at most now.
        valid_from: DateTime<Utc>,
    },
    /// Append the end, now, of the link that holds now.
    Unlink,
}

/// Input required to link or unlink an account using trusted actor context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkStaffExternalAccountInput {
    /// Trusted internal staff identifier recorded as `linked_by`.
    pub actor_staff_id: StaffId,
    /// Trusted effective roles used to authorize the change.
    pub actor_roles: Vec<RoleCode>,
    /// Staff member the account belongs to.
    pub target_staff_id: StaffId,
    /// Validated source account.
    pub account: ExternalAccount,
    /// Link or unlink.
    pub change: ExternalAccountLinkChange,
    /// The time the command is judged at.
    pub now: DateTime<Utc>,
}

/// Error returned while linking or unlinking an account.
#[derive(Debug)]
pub enum LinkStaffExternalAccountError {
    /// The verified actor lacks `MASTER_ADMIN`.
    PermissionDenied(&'static str),
    /// A link may not start in the future: an event is read with the link valid at its time.
    ValidFromInFuture,
    /// The link record or its outbox event could not be persisted.
    Persistence(ExternalAccountLinkPersistenceError),
}

impl Display for LinkStaffExternalAccountError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PermissionDenied(reason_code) => {
                write!(formatter, "permission denied: {reason_code}")
            }
            Self::ValidFromInFuture => formatter.write_str("valid_from is in the future"),
            Self::Persistence(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for LinkStaffExternalAccountError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PermissionDenied(_) | Self::ValidFromInFuture => None,
            Self::Persistence(error) => Some(error),
        }
    }
}

/// Authorizes and appends an external-account link record.
pub struct LinkStaffExternalAccount {
    unit_of_work: Arc<dyn ExternalAccountLinkUnitOfWork>,
}

impl LinkStaffExternalAccount {
    /// Creates the use case backed only by its mutation port.
    #[must_use]
    pub fn new(unit_of_work: Arc<dyn ExternalAccountLinkUnitOfWork>) -> Self {
        Self { unit_of_work }
    }

    /// Applies the link policy to trusted roles and appends the authorized record.
    ///
    /// # Errors
    /// Returns [`LinkStaffExternalAccountError::PermissionDenied`] when the actor lacks
    /// `MASTER_ADMIN`, [`LinkStaffExternalAccountError::ValidFromInFuture`] for a link that starts
    /// after `now`, or [`LinkStaffExternalAccountError::Persistence`] when the write fails.
    pub async fn execute(
        &self,
        input: LinkStaffExternalAccountInput,
    ) -> Result<ExternalAccountLink, LinkStaffExternalAccountError> {
        let decision =
            evaluate_policy(&PolicyInput::staff_external_account_link(input.actor_roles));
        if !decision.is_allowed() {
            return Err(LinkStaffExternalAccountError::PermissionDenied(
                decision.reason_code(),
            ));
        }
        let record = match input.change {
            ExternalAccountLinkChange::Link { valid_from } => {
                if valid_from > input.now {
                    return Err(LinkStaffExternalAccountError::ValidFromInFuture);
                }
                self.unit_of_work
                    .link(
                        input.target_staff_id,
                        &input.account,
                        valid_from,
                        input.actor_staff_id,
                    )
                    .await
            }
            ExternalAccountLinkChange::Unlink => {
                self.unit_of_work
                    .unlink(input.target_staff_id, &input.account, input.actor_staff_id)
                    .await
            }
        };
        record.map_err(LinkStaffExternalAccountError::Persistence)
    }
}
