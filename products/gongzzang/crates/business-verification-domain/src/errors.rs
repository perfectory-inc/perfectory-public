//! `BusinessVerification` 도메인 에러.

use thiserror::Error;

use crate::status::BusinessVerificationStatus;

/// `BusinessVerification` Aggregate 검증/전이 에러.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BusinessVerificationError {
    /// 상태 전이 불허 (도메인 머신 위반).
    #[error("invalid business_verification status transition: {from} → {to}")]
    InvalidTransition {
        /// 현재 상태.
        from: BusinessVerificationStatus,
        /// 목표 상태.
        to: BusinessVerificationStatus,
    },
    /// `reviewer_note` 가 비었음 (`reject` / `request_more_info` 는 메모 필수).
    #[error("reviewer_note cannot be empty for {action}")]
    EmptyReviewerNote {
        /// 메모가 필수인 액션 이름 (예: `"reject"`, `"request_more_info"`).
        action: &'static str,
    },
    /// `reviewer_note` 가 2000자 초과.
    #[error("reviewer_note exceeds 2000 chars (got {actual})")]
    ReviewerNoteTooLong {
        /// 실제 길이.
        actual: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_transition_message() {
        let err = BusinessVerificationError::InvalidTransition {
            from: BusinessVerificationStatus::Approved,
            to: BusinessVerificationStatus::Pending,
        };
        assert_eq!(
            err.to_string(),
            "invalid business_verification status transition: approved → pending"
        );
    }

    #[test]
    fn empty_reviewer_note_message_for_reject() {
        let err = BusinessVerificationError::EmptyReviewerNote { action: "reject" };
        assert_eq!(err.to_string(), "reviewer_note cannot be empty for reject");
    }

    #[test]
    fn empty_reviewer_note_message_for_request_more_info() {
        let err = BusinessVerificationError::EmptyReviewerNote {
            action: "request_more_info",
        };
        assert_eq!(
            err.to_string(),
            "reviewer_note cannot be empty for request_more_info"
        );
    }

    #[test]
    fn reviewer_note_too_long_message() {
        let err = BusinessVerificationError::ReviewerNoteTooLong { actual: 2001 };
        assert_eq!(
            err.to_string(),
            "reviewer_note exceeds 2000 chars (got 2001)"
        );
    }
}
