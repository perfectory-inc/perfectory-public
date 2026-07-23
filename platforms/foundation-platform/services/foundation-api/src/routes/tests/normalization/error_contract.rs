use super::*;

const PROPOSAL_ID: &str = "018f7c6a-0000-7000-8000-000000000101";
const APPLICATION_ID: &str = "018f7c6a-0000-7000-8000-000000000102";
const TARGET_ID: &str = "018f7c6a-0000-7000-8000-000000000103";
const REVIEW_REASON: &str = "reviewed against raw evidence";
const ROLLBACK_REASON: &str = "restore the reviewed prior value";

#[derive(Clone, Copy)]
enum ExpectedAdminInvocation {
    Review {
        decision: NormalizationReviewDecision,
        reason: &'static str,
    },
    Apply {
        expected_version: i64,
    },
    Rollback {
        expected_current_version: i64,
        reason: &'static str,
    },
}

impl ExpectedAdminInvocation {
    const fn operation(self) -> NormalizationOperation {
        match self {
            Self::Review { .. } => NormalizationOperation::Review,
            Self::Apply { .. } => NormalizationOperation::Apply,
            Self::Rollback { .. } => NormalizationOperation::Rollback,
        }
    }

    async fn assert(self, uow: &RecordingNormalizationUnitOfWork) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Review { decision, reason } => {
                uow.assert_only_review_invocation(
                    uuid::Uuid::parse_str(PROPOSAL_ID)?,
                    decision,
                    reason,
                )
                .await;
            }
            Self::Apply { expected_version } => {
                uow.assert_only_apply_invocation(
                    uuid::Uuid::parse_str(PROPOSAL_ID)?,
                    expected_version,
                )
                .await;
            }
            Self::Rollback {
                expected_current_version,
                reason,
            } => {
                uow.assert_only_rollback_invocation(
                    uuid::Uuid::parse_str(APPLICATION_ID)?,
                    expected_current_version,
                    reason,
                )
                .await;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn authorized_submit_invalid_catalog_input_preserves_exact_422_contract(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(
            AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
                Arc::new(RecordingNormalizationUnitOfWork::default()),
                service_identity_authorization(),
            )?,
        );
        let mut body = valid_normalization_proposal_body(
            false,
            true,
            true,
            "r2://foundation-platform/raw/company-1.json",
        );
        body["request"]["source_system"] = serde_json::json!(" ");
        let response = router(state)
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_exact_json_response(
            response,
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "code": "invalid_normalization_proposal",
                "message": "source_system must not be empty"
            }),
        )
        .await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn authorized_submit_persistence_failure_redacts_internal_detail(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let sensitive_detail =
            "postgres://normalization:secret@db.internal/catalog.normalization_proposal";
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::failing(
            NormalizationOperation::Submit,
            NormalizationError::Persistence(sensitive_detail.to_owned()),
        ));
        let state = Arc::new(
            AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
                normalization_uow.clone(),
                service_identity_authorization(),
            )?,
        );
        let body = valid_normalization_proposal_body(
            false,
            true,
            true,
            "r2://foundation-platform/raw/company-1.json",
        );
        let response = router(state)
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        let response_body = assert_exact_json_response(
            response,
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({
                "code": "normalization_proposal_persistence_failed",
                "message": "normalization proposal persistence failed"
            }),
        )
        .await?;
        assert!(!response_body.contains(sensitive_detail));
        assert_eq!(normalization_uow.commands.lock().await.len(), 1);
        assert!(normalization_uow.error.lock().await.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn authorized_blank_review_reason_preserves_exact_400_contract() -> Result<(), Box<dyn Error>>
{
    assert_application_validation_error(
        format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/approve"),
        normalization_review_request_body("   "),
        "reason must not be empty",
    )
    .await
}

#[tokio::test]
async fn authorized_nonpositive_apply_version_preserves_exact_400_contract(
) -> Result<(), Box<dyn Error>> {
    for expected_version in [0, -1] {
        assert_application_validation_error(
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/apply"),
            normalization_apply_request_body(expected_version),
            "expected_version must be positive",
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn authorized_blank_rollback_reason_preserves_exact_400_contract(
) -> Result<(), Box<dyn Error>> {
    assert_application_validation_error(
        format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback"),
        normalization_rollback_request_body(8, "   "),
        "reason must not be empty",
    )
    .await
}

#[tokio::test]
async fn authorized_nonpositive_rollback_version_preserves_exact_400_contract(
) -> Result<(), Box<dyn Error>> {
    for expected_current_version in [0, -1] {
        assert_application_validation_error(
            format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback"),
            normalization_rollback_request_body(expected_current_version, ROLLBACK_REASON),
            "expected_version must be positive",
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn authorized_review_apply_and_rollback_failures_preserve_exact_400_contract(
) -> Result<(), Box<dyn Error>> {
    let cases = [
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/approve"),
            normalization_review_request_body(REVIEW_REASON),
            "normalization proposal not found",
            ExpectedAdminInvocation::Review {
                decision: NormalizationReviewDecision::Approved,
                reason: REVIEW_REASON,
            },
        ),
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/reject"),
            normalization_review_request_body("raw evidence does not support the proposal"),
            "normalization proposal is not pending_review",
            ExpectedAdminInvocation::Review {
                decision: NormalizationReviewDecision::Rejected,
                reason: "raw evidence does not support the proposal",
            },
        ),
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/apply"),
            normalization_apply_request_body(7),
            "normalization proposal not found",
            ExpectedAdminInvocation::Apply {
                expected_version: 7,
            },
        ),
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/apply"),
            normalization_apply_request_body(7),
            "proposal must be approved before apply",
            ExpectedAdminInvocation::Apply {
                expected_version: 7,
            },
        ),
        (
            format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback"),
            normalization_rollback_request_body(8, ROLLBACK_REASON),
            "normalization application not found",
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
        (
            format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback"),
            normalization_rollback_request_body(8, ROLLBACK_REASON),
            "normalization application is already rolled back",
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
    ];

    for (uri, body, message, expected_invocation) in cases {
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::failing(
            expected_invocation.operation(),
            normalization_application_error(message),
        ));
        let response = authorized_admin_response(&uri, &body, normalization_uow.clone()).await?;
        assert_exact_json_response(
            response,
            StatusCode::BAD_REQUEST,
            serde_json::json!({"error": message}),
        )
        .await?;
        expected_invocation.assert(&normalization_uow).await?;
    }
    Ok(())
}

#[tokio::test]
async fn authorized_apply_and_rollback_missing_targets_preserve_exact_404_contract(
) -> Result<(), Box<dyn Error>> {
    let cases = [
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/apply"),
            normalization_apply_request_body(7),
            ExpectedAdminInvocation::Apply {
                expected_version: 7,
            },
        ),
        (
            format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback"),
            normalization_rollback_request_body(8, ROLLBACK_REASON),
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
    ];

    for (uri, body, expected_invocation) in cases {
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::failing(
            expected_invocation.operation(),
            NormalizationError::TargetNotFound(TARGET_ID.to_owned()),
        ));
        let response = authorized_admin_response(&uri, &body, normalization_uow.clone()).await?;
        assert_exact_json_response(
            response,
            StatusCode::NOT_FOUND,
            serde_json::json!({"error": TARGET_ID}),
        )
        .await?;
        expected_invocation.assert(&normalization_uow).await?;
    }
    Ok(())
}

#[tokio::test]
async fn authorized_apply_and_rollback_conflicts_preserve_exact_409_contract(
) -> Result<(), Box<dyn Error>> {
    let apply_uri = format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/apply");
    let rollback_uri = format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback");
    let apply_body = normalization_apply_request_body(7);
    let rollback_body = normalization_rollback_request_body(8, ROLLBACK_REASON);
    let cases = [
        (
            apply_uri.as_str(),
            &apply_body,
            NormalizationError::TargetVersionConflict {
                expected: 7,
                current: 8,
            },
            "version mismatch",
            ExpectedAdminInvocation::Apply {
                expected_version: 7,
            },
        ),
        (
            rollback_uri.as_str(),
            &rollback_body,
            NormalizationError::TargetVersionConflict {
                expected: 8,
                current: 9,
            },
            "version mismatch",
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
        (
            rollback_uri.as_str(),
            &rollback_body,
            NormalizationError::TargetStateConflict(TARGET_ID.to_owned()),
            TARGET_ID,
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
        (
            apply_uri.as_str(),
            &apply_body,
            NormalizationError::TargetArchived(TARGET_ID.to_owned()),
            TARGET_ID,
            ExpectedAdminInvocation::Apply {
                expected_version: 7,
            },
        ),
        (
            rollback_uri.as_str(),
            &rollback_body,
            NormalizationError::TargetArchived(TARGET_ID.to_owned()),
            TARGET_ID,
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
    ];

    for (uri, body, error, message, expected_invocation) in cases {
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::failing(
            expected_invocation.operation(),
            error,
        ));
        let response = authorized_admin_response(uri, body, normalization_uow.clone()).await?;
        assert_exact_json_response(
            response,
            StatusCode::CONFLICT,
            serde_json::json!({"error": message}),
        )
        .await?;
        expected_invocation.assert(&normalization_uow).await?;
    }
    Ok(())
}

#[tokio::test]
async fn authorized_review_apply_and_rollback_infrastructure_failures_are_opaque(
) -> Result<(), Box<dyn Error>> {
    let cases = [
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/approve"),
            normalization_review_request_body(REVIEW_REASON),
            ExpectedAdminInvocation::Review {
                decision: NormalizationReviewDecision::Approved,
                reason: REVIEW_REASON,
            },
        ),
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/reject"),
            normalization_review_request_body("raw evidence does not support the proposal"),
            ExpectedAdminInvocation::Review {
                decision: NormalizationReviewDecision::Rejected,
                reason: "raw evidence does not support the proposal",
            },
        ),
        (
            format!("/catalog/v1/normalization/proposals/{PROPOSAL_ID}/apply"),
            normalization_apply_request_body(7),
            ExpectedAdminInvocation::Apply {
                expected_version: 7,
            },
        ),
        (
            format!("/catalog/v1/normalization/applications/{APPLICATION_ID}/rollback"),
            normalization_rollback_request_body(8, ROLLBACK_REASON),
            ExpectedAdminInvocation::Rollback {
                expected_current_version: 8,
                reason: ROLLBACK_REASON,
            },
        ),
    ];

    for (uri, body, expected_invocation) in cases {
        let sensitive_detail =
            "postgres://normalization:secret@db.internal/catalog.normalization_application";
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::failing(
            expected_invocation.operation(),
            NormalizationError::Persistence(sensitive_detail.to_owned()),
        ));
        let response = authorized_admin_response(&uri, &body, normalization_uow.clone()).await?;
        assert_opaque_internal_error_response(response, sensitive_detail).await?;
        expected_invocation.assert(&normalization_uow).await?;
    }
    Ok(())
}

async fn assert_application_validation_error(
    uri: String,
    body: serde_json::Value,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::default());
    let response = authorized_admin_response(&uri, &body, normalization_uow.clone()).await?;
    assert_exact_json_response(
        response,
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error": message}),
    )
    .await?;
    normalization_uow.assert_no_admin_invocation().await;
    Ok(())
}

fn normalization_application_error(message: &str) -> NormalizationError {
    match message {
        "normalization proposal not found" => NormalizationError::ProposalNotFound,
        "normalization application not found" => NormalizationError::ApplicationNotFound,
        other => NormalizationError::InvalidState(other.to_owned()),
    }
}
