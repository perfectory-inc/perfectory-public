use super::*;

#[tokio::test]
async fn router_accepts_review_only_normalization_proposal_after_service_identity(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::default());
        let state = Arc::new(
            AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
                normalization_uow.clone(),
                service_identity_authorization(),
            )?,
        );
        let app = router(state);
        let mut body = valid_normalization_proposal_body(
            false,
            true,
            true,
            "r2://foundation-platform/raw/company-1.json",
        );
        body["submission_metadata"]["producer"] = serde_json::json!("spoofed-service");

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["status"], "queued");
        assert_eq!(payload["review_required"], true);
        assert_eq!(payload["platform"], "foundation-platform");
        assert_eq!(payload["metadata"]["storage"], "proposal_inbox");
        assert_eq!(payload["metadata"]["mode"], "durable_review_gate");
        assert!(payload["metadata"]["proposal_key"]
            .as_str()
            .is_some_and(|key| key.starts_with("normprop:v1:")));
        assert!(payload["submission_id"]
            .as_str()
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok()));
        let recorded_command = {
            let commands = normalization_uow.commands.lock().await;
            assert_eq!(commands.len(), 1);
            commands[0].clone()
        };
        assert_eq!(
            recorded_command.submitted_by_service,
            "intelligence-platform"
        );
        assert_ne!(
            recorded_command.submitted_by_principal_id.as_uuid(),
            uuid::Uuid::nil()
        );
        assert_eq!(recorded_command.source_system, "foundation-platform-r2");
        assert_eq!(
            recorded_command.raw_record_id,
            "r2://foundation-platform/raw/company-1.json"
        );
        assert_eq!(
            recorded_command.status,
            NormalizationProposalStatus::PendingReview
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_accepts_building_register_floor_normalization_proposal_after_service_identity(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::default());
        let state = Arc::new(
            AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
                normalization_uow.clone(),
                service_identity_authorization(),
            )?,
        );
        let app = router(state);
        let body = valid_building_register_floor_normalization_proposal_body();

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let recorded_command = {
            let commands = normalization_uow.commands.lock().await;
            assert_eq!(commands.len(), 1);
            commands[0].clone()
        };
        assert_eq!(
            recorded_command.target_kind,
            NormalizationTargetKind::BuildingRegisterFloor
        );
        assert_eq!(
            recorded_command.target_schema_version,
            "building_register_floor.normalized.v1"
        );
        assert_eq!(
            recorded_command.proposal_schema_version,
            "building_register_floor.normalized.v1"
        );
        assert_eq!(
            recorded_command.submitted_by_service,
            "intelligence-platform"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_accepts_building_register_unit_normalization_proposal_after_service_identity(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let normalization_uow = Arc::new(RecordingNormalizationUnitOfWork::default());
        let state = Arc::new(
            AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
                normalization_uow.clone(),
                service_identity_authorization(),
            )?,
        );
        let app = router(state);
        let body = valid_building_register_unit_normalization_proposal_body();

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let recorded_command = {
            let commands = normalization_uow.commands.lock().await;
            assert_eq!(commands.len(), 1);
            commands[0].clone()
        };
        assert_eq!(
            recorded_command.target_kind,
            NormalizationTargetKind::BuildingRegisterUnit
        );
        assert_eq!(
            recorded_command.target_schema_version,
            "building_register_unit.normalized.v1"
        );
        assert_eq!(
            recorded_command.proposal_schema_version,
            "building_register_unit.normalized.v1"
        );
        assert_eq!(
            recorded_command.submitted_by_service,
            "intelligence-platform"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_rejects_normalization_proposal_with_direct_commit_allowed(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            service_identity_authorization(),
        )?);
        let app = router(state);
        let body = valid_normalization_proposal_body(
            true,
            true,
            true,
            "r2://foundation-platform/raw/company-1.json",
        );

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_rejects_normalization_proposal_without_human_review_gate(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            service_identity_authorization(),
        )?);
        let app = router(state);
        let body = valid_normalization_proposal_body(
            false,
            false,
            true,
            "r2://foundation-platform/raw/company-1.json",
        );

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_rejects_normalization_proposal_when_validation_is_not_accepted(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            service_identity_authorization(),
        )?);
        let app = router(state);
        let body = valid_normalization_proposal_body(
            false,
            true,
            false,
            "r2://foundation-platform/raw/company-1.json",
        );

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_rejects_normalization_proposal_for_mismatched_raw_record(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            service_identity_authorization(),
        )?);
        let app = router(state);
        let body = valid_normalization_proposal_body(
            false,
            true,
            true,
            "r2://foundation-platform/raw/different-company.json",
        );

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn router_rejects_normalization_proposal_for_mismatched_schema_version(
) -> Result<(), Box<dyn Error>> {
    with_intelligence_service_token(|| async {
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            service_identity_authorization(),
        )?);
        let app = router(state);
        let mut body = valid_normalization_proposal_body(
            false,
            true,
            true,
            "r2://foundation-platform/raw/company-1.json",
        );
        body["proposal"]["schema_version"] = serde_json::json!("company.normalized.v2");

        let response = app
            .oneshot(normalization_service_request(
                &body,
                Some("foundation-platform-intelligence-token-32-valid"),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    })
    .await
}
