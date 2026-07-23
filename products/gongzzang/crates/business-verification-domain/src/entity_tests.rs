//! `BusinessVerification` Aggregate 테스트 (entity 가 500 줄 임계 근접 — `#[path]` 분리).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;

fn sample_business_number() -> BusinessNumber {
    // 표본: 첫 3자리 ≥ 101, NTS 체크섬 OK (shared-kernel 테스트와 동일한 값).
    BusinessNumber::try_new("1234567891").expect("valid sample BN")
}

fn sample_documents() -> serde_json::Value {
    serde_json::json!([
        "business_verification/abc/biz_reg.pdf",
        "business_verification/abc/cert.png"
    ])
}

fn sample_documents_v2() -> serde_json::Value {
    serde_json::json!(["business_verification/abc/biz_reg_v2.pdf"])
}

fn make_pending(at: DateTime<Utc>) -> BusinessVerification {
    BusinessVerification::try_new_pending(
        Id::new(),
        Id::new(),
        sample_business_number(),
        sample_documents(),
        at,
    )
}

// ── try_new_pending ───────────────────────────────────────────

#[test]
fn try_new_pending_initial_status_is_pending() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
}

#[test]
fn try_new_pending_version_is_1() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    assert_eq!(business_verification.version, 1);
}

#[test]
fn try_new_pending_sla_is_submitted_plus_24h() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    assert_eq!(
        business_verification.sla_due_at,
        Some(now + Duration::hours(24))
    );
}

#[test]
fn try_new_pending_reviewer_fields_are_none() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    assert!(business_verification.reviewer_id.is_none());
    assert!(business_verification.reviewer_note.is_none());
    assert!(business_verification.reviewed_at.is_none());
}

#[test]
fn try_new_pending_updated_at_equals_submitted_at() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    assert_eq!(business_verification.updated_at, now);
    assert_eq!(business_verification.submitted_at, now);
}

#[test]
fn try_new_pending_documents_preserved() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    assert_eq!(
        business_verification.submitted_documents,
        sample_documents()
    );
}

// ── approve ───────────────────────────────────────────────────

#[test]
fn approve_happy_path_transitions_and_records_reviewer() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let reviewer = Id::<UserMarker>::new();
    let later = now + Duration::hours(1);
    business_verification
        .approve(reviewer.clone(), Some("OK".to_owned()), later)
        .expect("approve ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Approved
    );
    assert_eq!(business_verification.reviewer_id, Some(reviewer));
    assert_eq!(business_verification.reviewer_note.as_deref(), Some("OK"));
    assert_eq!(business_verification.reviewed_at, Some(later));
    assert_eq!(business_verification.updated_at, later);
}

#[test]
fn approve_bumps_version() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let v0 = business_verification.version;
    business_verification
        .approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    assert_eq!(business_verification.version, v0 + 1);
}

#[test]
fn approve_accepts_none_note() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve with none note ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Approved
    );
    assert!(business_verification.reviewer_note.is_none());
}

// ── reject ────────────────────────────────────────────────────

#[test]
fn reject_happy_path_transitions_and_records_note() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let reviewer = Id::<UserMarker>::new();
    let later = now + Duration::hours(2);
    business_verification
        .reject(
            reviewer.clone(),
            "사업자등록증 위조 의심돼요".to_owned(),
            later,
        )
        .expect("reject ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Rejected
    );
    assert_eq!(business_verification.reviewer_id, Some(reviewer));
    assert_eq!(
        business_verification.reviewer_note.as_deref(),
        Some("사업자등록증 위조 의심돼요")
    );
    assert_eq!(business_verification.reviewed_at, Some(later));
    assert_eq!(business_verification.version, 2);
}

#[test]
fn reject_without_note_errors() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let err = business_verification
        .reject(Id::new(), String::new(), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::EmptyReviewerNote { action: "reject" }
    ));
    // 상태가 그대로여야 해요 (전이 실패 시 mutation 0).
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
    assert_eq!(business_verification.version, 1);
}

#[test]
fn reject_with_whitespace_only_note_errors() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let err = business_verification
        .reject(Id::new(), "   ".to_owned(), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::EmptyReviewerNote { action: "reject" }
    ));
}

// ── request_more_info ─────────────────────────────────────────

#[test]
fn request_more_info_happy_path() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let reviewer = Id::<UserMarker>::new();
    let later = now + Duration::hours(3);
    business_verification
        .request_more_info(reviewer.clone(), "추가 서류 필요해요".to_owned(), later)
        .expect("request_more_info ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::NeedsMoreInfo
    );
    assert_eq!(business_verification.reviewer_id, Some(reviewer));
    assert_eq!(
        business_verification.reviewer_note.as_deref(),
        Some("추가 서류 필요해요")
    );
    assert_eq!(business_verification.reviewed_at, Some(later));
    assert_eq!(business_verification.version, 2);
}

#[test]
fn request_more_info_without_note_errors() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let err = business_verification
        .request_more_info(Id::new(), String::new(), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::EmptyReviewerNote {
            action: "request_more_info"
        }
    ));
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
}

// ── resubmit ──────────────────────────────────────────────────

#[test]
fn resubmit_clears_reviewer_and_replaces_documents() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    // 먼저 NeedsMoreInfo 로 보낸 뒤 resubmit.
    business_verification
        .request_more_info(
            Id::new(),
            "더 필요해요".to_owned(),
            now + Duration::hours(1),
        )
        .expect("rmi ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::NeedsMoreInfo
    );
    assert!(business_verification.reviewer_id.is_some());
    assert!(business_verification.reviewer_note.is_some());
    assert!(business_verification.reviewed_at.is_some());

    let resubmit_at = now + Duration::hours(5);
    business_verification
        .resubmit(sample_documents_v2(), resubmit_at)
        .expect("resubmit ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
    assert_eq!(
        business_verification.submitted_documents,
        sample_documents_v2()
    );
    assert!(business_verification.reviewer_id.is_none());
    assert!(business_verification.reviewer_note.is_none());
    assert!(business_verification.reviewed_at.is_none());
    assert_eq!(business_verification.updated_at, resubmit_at);
}

#[test]
fn resubmit_bumps_version() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .request_more_info(
            Id::new(),
            "더 필요해요".to_owned(),
            now + Duration::hours(1),
        )
        .expect("rmi ok");
    let v_before_resubmit = business_verification.version;
    business_verification
        .resubmit(sample_documents_v2(), now + Duration::hours(2))
        .expect("resubmit ok");
    assert_eq!(business_verification.version, v_before_resubmit + 1);
}

// ── 4 disallowed transitions ──────────────────────────────────

#[test]
fn approved_terminal_cannot_be_rejected() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    let err = business_verification
        .reject(Id::new(), "too late".to_owned(), now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::InvalidTransition {
            from: BusinessVerificationStatus::Approved,
            to: BusinessVerificationStatus::Rejected
        }
    ));
}

#[test]
fn approved_terminal_cannot_request_more_info() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    let err = business_verification
        .request_more_info(Id::new(), "more".to_owned(), now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::InvalidTransition {
            from: BusinessVerificationStatus::Approved,
            to: BusinessVerificationStatus::NeedsMoreInfo
        }
    ));
}

#[test]
fn rejected_terminal_cannot_be_approved() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .reject(Id::new(), "no good".to_owned(), now + Duration::hours(1))
        .expect("reject ok");
    let err = business_verification
        .approve(Id::new(), None, now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::InvalidTransition {
            from: BusinessVerificationStatus::Rejected,
            to: BusinessVerificationStatus::Approved
        }
    ));
}

#[test]
fn rejected_terminal_cannot_be_resubmitted() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .reject(Id::new(), "no good".to_owned(), now + Duration::hours(1))
        .expect("reject ok");
    let err = business_verification
        .resubmit(sample_documents_v2(), now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::InvalidTransition {
            from: BusinessVerificationStatus::Rejected,
            to: BusinessVerificationStatus::Pending
        }
    ));
}

// ── reviewer_note 길이 ────────────────────────────────────────

#[test]
fn reject_with_2000_char_note_accepted() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let exactly = "X".repeat(2000);
    business_verification
        .reject(Id::new(), exactly.clone(), now + Duration::hours(1))
        .expect("2000 ok");
    assert_eq!(
        business_verification.reviewer_note.as_deref(),
        Some(exactly.as_str())
    );
}

#[test]
fn reject_with_2001_char_note_errors() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let too_long = "X".repeat(2001);
    let err = business_verification
        .reject(Id::new(), too_long, now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::ReviewerNoteTooLong { actual: 2001 }
    ));
    // 전이 전에 검증 실패 — 상태 유지.
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
}

#[test]
fn approve_with_2001_char_note_errors() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    let too_long = "X".repeat(2001);
    let err = business_verification
        .approve(Id::new(), Some(too_long), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        BusinessVerificationError::ReviewerNoteTooLong { actual: 2001 }
    ));
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
}

// ── 상태 머신 happy → terminal cycle ──────────────────────────

#[test]
fn full_cycle_rmi_then_resubmit_then_approve() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .request_more_info(Id::new(), "더 필요".to_owned(), now + Duration::hours(1))
        .expect("rmi ok");
    business_verification
        .resubmit(sample_documents_v2(), now + Duration::hours(2))
        .expect("resubmit ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Pending
    );
    business_verification
        .approve(Id::new(), None, now + Duration::hours(3))
        .expect("approve ok");
    assert_eq!(
        business_verification.status,
        BusinessVerificationStatus::Approved
    );
    assert_eq!(business_verification.version, 4); // 1 + 3 transitions
}

// ── serde ──────────────────────────────────────────────────────

#[test]
fn serde_roundtrip_pending() {
    let now = Utc::now();
    let business_verification = make_pending(now);
    let json = serde_json::to_string(&business_verification).expect("serialize");
    let back: BusinessVerification = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(business_verification, back);
}

#[test]
fn serde_roundtrip_approved() {
    let now = Utc::now();
    let mut business_verification = make_pending(now);
    business_verification
        .approve(Id::new(), Some("OK".to_owned()), now + Duration::hours(1))
        .expect("approve ok");
    let json = serde_json::to_string(&business_verification).expect("serialize");
    let back: BusinessVerification = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(business_verification, back);
}
