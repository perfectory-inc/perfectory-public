use std::sync::Arc;

use catalog_application::ports::CatalogUnitOfWork;
use catalog_domain::{IndustrialComplex, IndustrialComplexKind};
use catalog_infrastructure::PgCatalogUnitOfWork;
use chrono::Utc;
use foundation_normalization_application::{
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationUnitOfWork, SubmitNormalizationProposal,
};
use foundation_normalization_domain::{
    NormalizationProposalStatus, NormalizationReviewDecision, NormalizationTargetKind,
};
use foundation_normalization_infrastructure::PgNormalizationUnitOfWork;
use foundation_shared_kernel::ids::{ComplexId, PrincipalId};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::support::TestResult;

pub struct NormalizationFixture {
    pub complex: IndustrialComplex,
    pub proposal_id: Uuid,
    pub principal_id: PrincipalId,
    pub normalization_uow: PgNormalizationUnitOfWork,
}

pub async fn create_pending_fixture(pool: &PgPool) -> TestResult<NormalizationFixture> {
    let complex = sample_complex();
    PgCatalogUnitOfWork::new(pool.clone())
        .create_complex(&complex)
        .await?;

    let normalization_uow = PgNormalizationUnitOfWork::new(pool.clone());
    let proposal =
        SubmitNormalizationProposal::new(Arc::new(PgNormalizationUnitOfWork::new(pool.clone())))
            .execute(valid_command(&complex))
            .await?;

    Ok(NormalizationFixture {
        complex,
        proposal_id: proposal.id,
        principal_id: PrincipalId::new(Uuid::now_v7()),
        normalization_uow,
    })
}

pub async fn approve_fixture(fixture: &NormalizationFixture) -> TestResult {
    let reviewed = fixture
        .normalization_uow
        .review_normalization_proposal(NormalizationProposalReviewCommand {
            id: Uuid::now_v7(),
            proposal_id: fixture.proposal_id,
            reviewer_principal_id: fixture.principal_id,
            decision: NormalizationReviewDecision::Approved,
            reason: "normalization fixture approval".to_owned(),
        })
        .await?;
    assert_eq!(reviewed.status, NormalizationProposalStatus::Approved);
    Ok(())
}

#[allow(dead_code)] // Shared integration-test support is compiled once per test binary.
pub async fn submit_and_approve(
    pool: &PgPool,
    fixture: &NormalizationFixture,
    proposed_record: JsonValue,
) -> TestResult<Uuid> {
    let mut command = valid_command(&fixture.complex);
    command.proposed_record = proposed_record;
    let proposal =
        SubmitNormalizationProposal::new(Arc::new(PgNormalizationUnitOfWork::new(pool.clone())))
            .execute(command)
            .await?;
    fixture
        .normalization_uow
        .review_normalization_proposal(NormalizationProposalReviewCommand {
            id: Uuid::now_v7(),
            proposal_id: proposal.id,
            reviewer_principal_id: fixture.principal_id,
            decision: NormalizationReviewDecision::Approved,
            reason: "normalization fixture approval".to_owned(),
        })
        .await?;
    Ok(proposal.id)
}

pub fn valid_command(complex: &IndustrialComplex) -> NormalizationProposalSubmissionCommand {
    NormalizationProposalSubmissionCommand {
        id: Uuid::now_v7(),
        proposal_key: String::new(),
        submitted_by_service: "intelligence-platform".to_owned(),
        submitted_by_principal_id: PrincipalId::new(Uuid::now_v7()),
        source_system: "foundation-platform-r2".to_owned(),
        raw_record_id: format!("normalization-fixture-{}", Uuid::new_v4()),
        raw_object_key: None,
        raw_checksum_sha256: Some("d".repeat(64)),
        bronze_object_id: None,
        target_kind: NormalizationTargetKind::IndustrialComplex,
        target_identity: json!({"complex_id": complex.id.as_uuid()}),
        target_schema_version: "industrial_complex.normalized.v1".to_owned(),
        proposal_schema_version: "industrial_complex.normalized.v1".to_owned(),
        proposed_record: json!({
            "name": "normalized industrial complex",
            "area_m2": 987_654
        }),
        proposed_record_sha256: String::new(),
        proposed_patch: None,
        confidence: 0.99,
        evidence: json!({"source":"normalization-infrastructure-test"}),
        validation: json!({"accepted":true}),
        model_profile_id: Some("normalization-ko".to_owned()),
        model_id: Some("local-model".to_owned()),
        prompt_id: Some("industrial-complex-normalizer".to_owned()),
        prompt_version: Some("v1".to_owned()),
        policy_id: "normalization-policy".to_owned(),
        policy_version: "v1".to_owned(),
        trace_id: format!("normalization-fixture-trace-{}", Uuid::new_v4()),
        status: NormalizationProposalStatus::PendingReview,
    }
}

pub fn sample_complex() -> IndustrialComplex {
    let now = Utc::now();
    IndustrialComplex {
        id: ComplexId::new(Uuid::now_v7()),
        official_complex_code: format!("IC-{}", Uuid::new_v4().simple()),
        name: format!("Normalization fixture {}", Uuid::new_v4()),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: random_primary_bjdong_code(),
        area_m2: 123_456,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 1,
    }
}

fn random_primary_bjdong_code() -> String {
    let digits = Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .filter(char::is_ascii_digit)
        .take(8)
        .collect::<String>();
    format!("11{digits:0<8}")
}

#[derive(Debug, Eq, PartialEq)]
pub struct TransactionState {
    pub proposal_row: JsonValue,
    pub review_rows: JsonValue,
    pub application_rows: JsonValue,
    pub canonical_row: JsonValue,
    pub outbox_rows: JsonValue,
}

pub async fn load_transaction_state(
    pool: &PgPool,
    fixture: &NormalizationFixture,
) -> Result<TransactionState, sqlx::Error> {
    let proposal_row = sqlx::query_scalar(
        "SELECT to_jsonb(proposal)
         FROM catalog.normalization_proposal proposal
         WHERE proposal.id = $1",
    )
    .bind(fixture.proposal_id)
    .fetch_one(pool)
    .await?;
    let review_rows = sqlx::query_scalar(
        "SELECT COALESCE(
             jsonb_agg(to_jsonb(review_row) ORDER BY review_row.id),
             '[]'::jsonb
         )
         FROM catalog.normalization_proposal_review review_row
         WHERE review_row.proposal_id = $1",
    )
    .bind(fixture.proposal_id)
    .fetch_one(pool)
    .await?;
    let application_rows = sqlx::query_scalar(
        "SELECT COALESCE(
             jsonb_agg(to_jsonb(application_row) ORDER BY application_row.id),
             '[]'::jsonb
         )
         FROM catalog.normalization_application application_row
         WHERE application_row.proposal_id = $1",
    )
    .bind(fixture.proposal_id)
    .fetch_one(pool)
    .await?;
    let canonical_row = sqlx::query_scalar(
        "SELECT to_jsonb(complex_row)
         FROM catalog.industrial_complex complex_row
         WHERE complex_row.id = $1",
    )
    .bind(fixture.complex.id.as_uuid())
    .fetch_one(pool)
    .await?;
    let outbox_rows = sqlx::query_scalar(
        "SELECT COALESCE(
             jsonb_agg(to_jsonb(outbox_row) ORDER BY outbox_row.event_id),
             '[]'::jsonb
         )
         FROM catalog.outbox_event outbox_row
         WHERE outbox_row.payload->>'proposal_id' = $1
            OR outbox_row.payload->>'complex_id' = $2",
    )
    .bind(fixture.proposal_id.to_string())
    .bind(fixture.complex.id.to_string())
    .fetch_one(pool)
    .await?;

    Ok(TransactionState {
        proposal_row,
        review_rows,
        application_rows,
        canonical_row,
        outbox_rows,
    })
}

#[allow(dead_code)] // Shared integration-test support is compiled once per test binary.
pub async fn load_complex_scope_state(pool: &PgPool, complex_id: String) -> TestResult<JsonValue> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'canonical', (
                 SELECT to_jsonb(complex_row)
                 FROM catalog.industrial_complex complex_row
                 WHERE complex_row.id = $1::uuid
             ),
             'proposal_statuses', (
                 SELECT COALESCE(
                     jsonb_object_agg(proposal.id::text, proposal.status ORDER BY proposal.id),
                     '{}'::jsonb
                 )
                 FROM catalog.normalization_proposal proposal
                 WHERE proposal.target_identity->>'complex_id' = $1
             ),
             'applications', (
                 SELECT COALESCE(
                     jsonb_agg(to_jsonb(application) ORDER BY application.id),
                     '[]'::jsonb
                 )
                 FROM catalog.normalization_application application
                 JOIN catalog.normalization_proposal proposal ON proposal.id = application.proposal_id
                 WHERE proposal.target_identity->>'complex_id' = $1
             ),
             'outbox', (
                 SELECT COALESCE(
                     jsonb_agg(to_jsonb(outbox) ORDER BY outbox.event_id),
                     '[]'::jsonb
                 )
                 FROM catalog.outbox_event outbox
                 WHERE outbox.payload->>'complex_id' = $1
             )
         )",
    )
    .bind(complex_id)
    .fetch_one(pool)
    .await?)
}
