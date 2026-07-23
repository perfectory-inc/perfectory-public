use super::*;

#[tokio::test]
async fn router_rejects_normalization_proposal_without_service_identity(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);
    let body = valid_normalization_proposal_body(
        false,
        true,
        true,
        "r2://foundation-platform/raw/company-1.json",
    );

    let response = app
        .oneshot(normalization_service_request(&body, None)?)
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_normalization_submission_with_staff_bearer() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(
        AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
            Arc::new(RecordingNormalizationUnitOfWork::default()),
            staff_identity_authorization(),
        )?,
    );
    let app = router(state);
    let body = valid_normalization_proposal_body(
        false,
        true,
        true,
        "r2://foundation-platform/raw/company-1.json",
    );

    let response = app
        .oneshot(normalization_service_request(
            &body,
            Some("foundation-platform-staff-token"),
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn router_rejects_normalization_proposal_approval_without_staff_bearer(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let proposal_id = uuid::Uuid::now_v7();
    let body = normalization_review_request_body(
        "operator approved normalized value after reviewing raw evidence",
    );
    let response = router(state)
        .oneshot(normalization_service_request_to(
            &format!("/catalog/v1/normalization/proposals/{proposal_id}/approve"),
            &body,
            None,
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_normalization_proposal_rejection_without_staff_bearer(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let proposal_id = uuid::Uuid::now_v7();
    let body = normalization_review_request_body(
        "raw evidence did not support the proposed normalized value",
    );
    let response = router(state)
        .oneshot(normalization_service_request_to(
            &format!("/catalog/v1/normalization/proposals/{proposal_id}/reject"),
            &body,
            None,
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_normalization_proposal_apply_without_staff_bearer(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let proposal_id = uuid::Uuid::now_v7();
    let body = normalization_apply_request_body(7);
    let response = router(state)
        .oneshot(normalization_service_request_to(
            &format!("/catalog/v1/normalization/proposals/{proposal_id}/apply"),
            &body,
            None,
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_normalization_application_rollback_without_staff_bearer(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let application_id = uuid::Uuid::now_v7();
    let body = normalization_rollback_request_body(8, "operator rollback after review");
    let response = router(state)
        .oneshot(normalization_service_request_to(
            &format!("/catalog/v1/normalization/applications/{application_id}/rollback"),
            &body,
            None,
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_normalization_admin_routes_with_intelligence_service_token(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            service_identity_authorization(),
        )?);
        let app = router(state);
        let proposal_id = uuid::Uuid::now_v7();
        let application_id = uuid::Uuid::now_v7();
        let requests = [
            (
                format!("/catalog/v1/normalization/proposals/{proposal_id}/approve"),
                normalization_review_request_body(
                    "operator approved normalized value after reviewing raw evidence",
                ),
            ),
            (
                format!("/catalog/v1/normalization/proposals/{proposal_id}/reject"),
                normalization_review_request_body(
                    "raw evidence did not support the proposed normalized value",
                ),
            ),
            (
                format!("/catalog/v1/normalization/proposals/{proposal_id}/apply"),
                normalization_apply_request_body(7),
            ),
            (
                format!("/catalog/v1/normalization/applications/{application_id}/rollback"),
                normalization_rollback_request_body(8, "operator rollback after review"),
            ),
        ];

        for (uri, body) in requests {
            let response = app
                .clone()
                .oneshot(normalization_admin_request_with_intelligence_service_token(
                    &uri, &body,
                )?)
                .await?;

            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }

        Ok(())
    })
    .await
}
