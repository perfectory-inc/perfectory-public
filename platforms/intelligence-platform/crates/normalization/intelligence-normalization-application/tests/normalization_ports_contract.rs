// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_application::{
    FoundationSubmissionError, FoundationSubmissionFailureClass,
};

#[test]
fn foundation_submission_errors_are_safe_to_return() {
    let error = FoundationSubmissionError::Rejected {
        status: 409,
        body: "duplicate proposal".to_string(),
        retryable: false,
    };

    assert_eq!(
        error.to_string(),
        "foundation-platform rejected normalization submission with status 409"
    );
    assert_eq!(
        error.safe_message(),
        "foundation-platform rejected submission"
    );
}

#[test]
fn submission_errors_classify_outbox_transitions() {
    assert_eq!(
        FoundationSubmissionError::PreSendFailure {
            message: "connection refused".to_string()
        }
        .failure_class(),
        FoundationSubmissionFailureClass::Retryable
    );

    assert_eq!(
        FoundationSubmissionError::Rejected {
            status: 503,
            body: "try later".to_string(),
            retryable: true,
        }
        .failure_class(),
        FoundationSubmissionFailureClass::Retryable
    );

    assert_eq!(
        FoundationSubmissionError::Rejected {
            status: 422,
            body: "invalid payload".to_string(),
            retryable: false,
        }
        .failure_class(),
        FoundationSubmissionFailureClass::Terminal
    );

    assert_eq!(
        FoundationSubmissionError::AmbiguousOutcome {
            message: "deadline after request body was sent".to_string()
        }
        .failure_class(),
        FoundationSubmissionFailureClass::ReconcileRequired
    );

    assert_eq!(
        FoundationSubmissionError::InvalidResponse {
            message: "missing submission_id".to_string()
        }
        .failure_class(),
        FoundationSubmissionFailureClass::ReconcileRequired
    );
}
