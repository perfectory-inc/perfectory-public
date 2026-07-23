//! Staff session model.

use chrono::{DateTime, Utc};
use identity_shared_kernel::{SessionId, StaffId};
use serde::{Deserialize, Serialize};

/// Verified staff session tracked for revocation checks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StaffSession {
    /// Stable internal session identifier.
    pub session_id: SessionId,
    /// Staff account that owns this session.
    pub staff_id: StaffId,
    /// JWT ID used for revoked-token matching.
    pub jti: String,
    /// UTC timestamp when the session was issued.
    pub issued_at: DateTime<Utc>,
    /// UTC timestamp when the session expires.
    pub expires_at: DateTime<Utc>,
}
