//! `ListingReviewDecision` — 매물 검토 결정 (3값).
//!
//! Spec § 5.5 `listing_review_queue.decision` `CHECK` enum 3값:
//! `approve`, `reject`, `request_changes`.
//!
//! `decision` 컬럼은 `NULL` 가능 — `NULL` = pending (검토 전).
//! `Some(ListingReviewDecision)` 으로 채워지면 terminal (이후 변경 불가).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 매물 검토 결정 (3값).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingReviewDecision {
    /// 승인 — 매물 게시 허용.
    Approve,
    /// 거부 — 매물 게시 거부 (메모 필수).
    Reject,
    /// 변경 요청 — 매물 정보 수정 필요 (메모 필수).
    RequestChanges,
}

/// `ListingReviewDecision` 파싱 에러.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ListingReviewDecisionError {
    /// 미지원 값.
    #[error("unknown listing_review_decision: '{0}' (expected: approve, reject, request_changes)")]
    Unknown(String),
}

impl ListingReviewDecision {
    /// 정규화된 `snake_case` 문자열 반환 (`DB varchar(20)` 매핑).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Reject => "reject",
            Self::RequestChanges => "request_changes",
        }
    }
}

impl fmt::Display for ListingReviewDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ListingReviewDecision {
    type Err = ListingReviewDecisionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "approve" => Ok(Self::Approve),
            "reject" => Ok(Self::Reject),
            "request_changes" => Ok(Self::RequestChanges),
            other => Err(ListingReviewDecisionError::Unknown(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn as_str_matches_spec_for_each_variant() {
        assert_eq!(ListingReviewDecision::Approve.as_str(), "approve");
        assert_eq!(ListingReviewDecision::Reject.as_str(), "reject");
        assert_eq!(
            ListingReviewDecision::RequestChanges.as_str(),
            "request_changes"
        );
    }

    #[test]
    fn from_str_parses_each_variant() {
        assert_eq!(
            ListingReviewDecision::from_str("approve"),
            Ok(ListingReviewDecision::Approve)
        );
        assert_eq!(
            ListingReviewDecision::from_str("reject"),
            Ok(ListingReviewDecision::Reject)
        );
        assert_eq!(
            ListingReviewDecision::from_str("request_changes"),
            Ok(ListingReviewDecision::RequestChanges)
        );
    }

    #[test]
    fn from_str_rejects_unknown() {
        let err = ListingReviewDecision::from_str("approved").unwrap_err();
        assert!(matches!(err, ListingReviewDecisionError::Unknown(s) if s == "approved"));
    }

    #[test]
    fn from_str_rejects_empty() {
        let err = ListingReviewDecision::from_str("").unwrap_err();
        assert!(matches!(err, ListingReviewDecisionError::Unknown(_)));
    }

    #[test]
    fn display_matches_as_str() {
        assert_eq!(
            format!("{}", ListingReviewDecision::RequestChanges),
            "request_changes"
        );
    }

    #[test]
    fn round_trip_each_variant() {
        for v in [
            ListingReviewDecision::Approve,
            ListingReviewDecision::Reject,
            ListingReviewDecision::RequestChanges,
        ] {
            assert_eq!(ListingReviewDecision::from_str(v.as_str()).unwrap(), v);
        }
    }

    #[test]
    fn serde_roundtrip_via_json() {
        let v = ListingReviewDecision::RequestChanges;
        let json = serde_json::to_string(&v).expect("serialize");
        assert_eq!(json, r#""request_changes""#);
        let back: ListingReviewDecision = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, v);
    }

    #[test]
    fn serde_roundtrip_all_3_variants() {
        for v in [
            ListingReviewDecision::Approve,
            ListingReviewDecision::Reject,
            ListingReviewDecision::RequestChanges,
        ] {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: ListingReviewDecision = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    #[test]
    fn copy_and_hash() {
        use std::collections::HashSet;
        let a = ListingReviewDecision::Approve;
        let b = a; // Copy
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(ListingReviewDecision::Approve);
        set.insert(ListingReviewDecision::Reject);
        assert_eq!(set.len(), 2);
    }
}
