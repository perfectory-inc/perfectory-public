//! Staff account aggregate.

use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;
use serde::{Deserialize, Serialize};

/// Staff account authenticated by the configured identity provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Staff {
    /// Stable internal staff identifier.
    pub id: StaffId,
    /// Staff organization subject claim from the identity provider.
    pub zitadel_subject: String,
    /// Staff email address.
    pub email: String,
    /// Staff display name.
    pub display_name: String,
    /// Primary role code used for default authorization.
    pub primary_role_code: String,
    /// UTC timestamp when the staff account was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the staff account was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
