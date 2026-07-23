//! `BusinessVerificationStatus` — 사업자 인증 큐 상태 + 상태 전이 머신.
//!
//! Spec § 5.5 `business_verification_queue.status` `CHECK` enum 4값:
//! `pending`, `approved`, `rejected`, `needs_more_info`.
//!
//! 상태 전이 규칙:
//!
//! - `Pending` → `Approved` (`approve`)
//! - `Pending` → `Rejected` (`reject`, note required)
//! - `Pending` → `NeedsMoreInfo` (`request_more_info`, note required)
//! - `NeedsMoreInfo` → `Pending` (`resubmit`, 사용자 재제출)
//! - `Approved` / `Rejected` 는 terminal.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 사업자 인증 큐 상태 (4값).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusinessVerificationStatus {
    /// 어드민 검토 대기.
    Pending,
    /// 승인 (terminal).
    Approved,
    /// 거부 (terminal).
    Rejected,
    /// 추가 자료 요청. 사용자 재제출 시 `Pending` 으로 복귀.
    NeedsMoreInfo,
}

/// `BusinessVerificationStatus` 파싱 에러.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BusinessVerificationStatusError {
    /// 미지원 값.
    #[error("unknown business_verification_status: '{0}' (expected: pending, approved, rejected, needs_more_info)")]
    Unknown(String),
}

impl BusinessVerificationStatus {
    /// 정규화된 `snake_case` 문자열 반환 (`DB varchar(20)` 매핑).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::NeedsMoreInfo => "needs_more_info",
        }
    }

    /// 상태 전이가 허용되는지 검사 (도메인 머신).
    ///
    /// 허용 전이 4개:
    ///
    /// - `Pending` → `Approved`
    /// - `Pending` → `Rejected`
    /// - `Pending` → `NeedsMoreInfo`
    /// - `NeedsMoreInfo` → `Pending`
    ///
    /// `Approved` / `Rejected` 는 terminal — 어떤 전이도 허용 안 해요.
    /// 같은 상태로의 전이는 항상 false.
    #[must_use]
    pub const fn can_transition_to(self, target: Self) -> bool {
        use BusinessVerificationStatus::{Approved, NeedsMoreInfo, Pending, Rejected};
        matches!(
            (self, target),
            (Pending, Approved | Rejected | NeedsMoreInfo) | (NeedsMoreInfo, Pending)
        )
    }
}

impl fmt::Display for BusinessVerificationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BusinessVerificationStatus {
    type Err = BusinessVerificationStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            "needs_more_info" => Ok(Self::NeedsMoreInfo),
            other => Err(BusinessVerificationStatusError::Unknown(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn as_str_matches_spec_for_each_variant() {
        assert_eq!(BusinessVerificationStatus::Pending.as_str(), "pending");
        assert_eq!(BusinessVerificationStatus::Approved.as_str(), "approved");
        assert_eq!(BusinessVerificationStatus::Rejected.as_str(), "rejected");
        assert_eq!(
            BusinessVerificationStatus::NeedsMoreInfo.as_str(),
            "needs_more_info"
        );
    }

    #[test]
    fn from_str_parses_each_variant() {
        assert_eq!(
            BusinessVerificationStatus::from_str("pending"),
            Ok(BusinessVerificationStatus::Pending)
        );
        assert_eq!(
            BusinessVerificationStatus::from_str("approved"),
            Ok(BusinessVerificationStatus::Approved)
        );
        assert_eq!(
            BusinessVerificationStatus::from_str("rejected"),
            Ok(BusinessVerificationStatus::Rejected)
        );
        assert_eq!(
            BusinessVerificationStatus::from_str("needs_more_info"),
            Ok(BusinessVerificationStatus::NeedsMoreInfo)
        );
    }

    #[test]
    fn from_str_rejects_unknown() {
        let err = BusinessVerificationStatus::from_str("archived").unwrap_err();
        assert!(matches!(err, BusinessVerificationStatusError::Unknown(s) if s == "archived"));
    }

    #[test]
    fn from_str_rejects_empty() {
        let err = BusinessVerificationStatus::from_str("").unwrap_err();
        assert!(matches!(err, BusinessVerificationStatusError::Unknown(_)));
    }

    #[test]
    fn display_matches_as_str() {
        assert_eq!(
            format!("{}", BusinessVerificationStatus::NeedsMoreInfo),
            "needs_more_info"
        );
    }

    #[test]
    fn round_trip_each_variant() {
        for v in [
            BusinessVerificationStatus::Pending,
            BusinessVerificationStatus::Approved,
            BusinessVerificationStatus::Rejected,
            BusinessVerificationStatus::NeedsMoreInfo,
        ] {
            assert_eq!(BusinessVerificationStatus::from_str(v.as_str()).unwrap(), v);
        }
    }

    #[test]
    fn serde_roundtrip_via_json() {
        let v = BusinessVerificationStatus::NeedsMoreInfo;
        let json = serde_json::to_string(&v).expect("serialize");
        assert_eq!(json, r#""needs_more_info""#);
        let back: BusinessVerificationStatus = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, v);
    }

    #[test]
    fn serde_roundtrip_all_4_variants() {
        for v in [
            BusinessVerificationStatus::Pending,
            BusinessVerificationStatus::Approved,
            BusinessVerificationStatus::Rejected,
            BusinessVerificationStatus::NeedsMoreInfo,
        ] {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: BusinessVerificationStatus =
                serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    #[test]
    fn copy_and_hash() {
        use std::collections::HashSet;
        let a = BusinessVerificationStatus::Pending;
        let b = a; // Copy
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(BusinessVerificationStatus::Pending);
        set.insert(BusinessVerificationStatus::Approved);
        assert_eq!(set.len(), 2);
    }

    // ── State machine: ALLOWED transitions ─────────────────────────

    #[test]
    fn allowed_pending_to_approved() {
        assert!(BusinessVerificationStatus::Pending
            .can_transition_to(BusinessVerificationStatus::Approved));
    }

    #[test]
    fn allowed_pending_to_rejected() {
        assert!(BusinessVerificationStatus::Pending
            .can_transition_to(BusinessVerificationStatus::Rejected));
    }

    #[test]
    fn allowed_pending_to_needs_more_info() {
        assert!(BusinessVerificationStatus::Pending
            .can_transition_to(BusinessVerificationStatus::NeedsMoreInfo));
    }

    #[test]
    fn allowed_needs_more_info_to_pending() {
        assert!(BusinessVerificationStatus::NeedsMoreInfo
            .can_transition_to(BusinessVerificationStatus::Pending));
    }

    // ── State machine: DISALLOWED transitions ──────────────────────

    #[test]
    fn disallowed_approved_terminal() {
        for target in [
            BusinessVerificationStatus::Pending,
            BusinessVerificationStatus::Approved,
            BusinessVerificationStatus::Rejected,
            BusinessVerificationStatus::NeedsMoreInfo,
        ] {
            assert!(
                !BusinessVerificationStatus::Approved.can_transition_to(target),
                "Approved should not transition to {target:?}"
            );
        }
    }

    #[test]
    fn disallowed_rejected_terminal() {
        for target in [
            BusinessVerificationStatus::Pending,
            BusinessVerificationStatus::Approved,
            BusinessVerificationStatus::Rejected,
            BusinessVerificationStatus::NeedsMoreInfo,
        ] {
            assert!(
                !BusinessVerificationStatus::Rejected.can_transition_to(target),
                "Rejected should not transition to {target:?}"
            );
        }
    }

    #[test]
    fn disallowed_needs_more_info_skip_to_terminal() {
        assert!(!BusinessVerificationStatus::NeedsMoreInfo
            .can_transition_to(BusinessVerificationStatus::Approved));
        assert!(!BusinessVerificationStatus::NeedsMoreInfo
            .can_transition_to(BusinessVerificationStatus::Rejected));
    }

    #[test]
    fn disallowed_self_transition_for_each() {
        for v in [
            BusinessVerificationStatus::Pending,
            BusinessVerificationStatus::Approved,
            BusinessVerificationStatus::Rejected,
            BusinessVerificationStatus::NeedsMoreInfo,
        ] {
            assert!(
                !v.can_transition_to(v),
                "{v:?} → {v:?} should not be allowed"
            );
        }
    }
}
