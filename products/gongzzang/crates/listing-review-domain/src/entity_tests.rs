//! `ListingReview` Aggregate 테스트 (entity 가 500 줄 임계 근접 — `#[path]` 분리).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;

fn sample_flags() -> serde_json::Value {
    serde_json::json!(["suspected_duplicate", "price_anomaly"])
}

fn make_pending(at: DateTime<Utc>) -> ListingReview {
    ListingReview::try_new_pending(Id::new(), Id::new(), Some(80), Some(sample_flags()), at)
        .expect("valid pending listing_review")
}

// ── try_new_pending ───────────────────────────────────────────

#[test]
fn try_new_pending_decision_is_none() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert!(listing_review.decision.is_none());
    assert!(listing_review.is_pending());
}

#[test]
fn try_new_pending_version_is_1() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert_eq!(listing_review.version, 1);
}

#[test]
fn try_new_pending_sla_is_submitted_plus_12h() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert_eq!(listing_review.sla_due_at, Some(now + Duration::hours(12)));
}

#[test]
fn try_new_pending_reviewer_fields_are_none() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert!(listing_review.reviewer_id.is_none());
    assert!(listing_review.reviewer_note.is_none());
    assert!(listing_review.decided_at.is_none());
}

#[test]
fn try_new_pending_updated_at_equals_submitted_at() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert_eq!(listing_review.updated_at, now);
    assert_eq!(listing_review.submitted_at, now);
}

#[test]
fn try_new_pending_auto_check_preserved() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert_eq!(listing_review.auto_check_score, Some(80));
    assert_eq!(
        listing_review.auto_check_flags.as_ref(),
        Some(&sample_flags())
    );
}

#[test]
fn try_new_pending_accepts_none_auto_check() {
    let now = Utc::now();
    let listing_review =
        ListingReview::try_new_pending(Id::new(), Id::new(), None, None, now).expect("none ok");
    assert!(listing_review.auto_check_score.is_none());
    assert!(listing_review.auto_check_flags.is_none());
    assert!(listing_review.is_pending());
}

// ── auto_check_score boundary ─────────────────────────────────

#[test]
fn try_new_pending_score_0_accepted() {
    let now = Utc::now();
    let listing_review = ListingReview::try_new_pending(Id::new(), Id::new(), Some(0), None, now)
        .expect("score 0 ok");
    assert_eq!(listing_review.auto_check_score, Some(0));
}

#[test]
fn try_new_pending_score_100_accepted() {
    let now = Utc::now();
    let listing_review = ListingReview::try_new_pending(Id::new(), Id::new(), Some(100), None, now)
        .expect("score 100 ok");
    assert_eq!(listing_review.auto_check_score, Some(100));
}

#[test]
fn try_new_pending_score_101_errors() {
    let now = Utc::now();
    let err =
        ListingReview::try_new_pending(Id::new(), Id::new(), Some(101), None, now).unwrap_err();
    assert!(matches!(
        err,
        ListingReviewError::AutoCheckScoreOutOfRange { actual: 101 }
    ));
}

// ── is_pending ────────────────────────────────────────────────

#[test]
fn is_pending_true_before_decision() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    assert!(listing_review.is_pending());
}

#[test]
fn is_pending_false_after_approve() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    assert!(!listing_review.is_pending());
}

// ── decide_approve ────────────────────────────────────────────

#[test]
fn approve_happy_path_records_reviewer_and_decision() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let reviewer = Id::<UserMarker>::new();
    let later = now + Duration::hours(1);
    listing_review
        .decide_approve(reviewer.clone(), Some("OK".to_owned()), later)
        .expect("approve ok");
    assert_eq!(
        listing_review.decision,
        Some(ListingReviewDecision::Approve)
    );
    assert_eq!(listing_review.reviewer_id, Some(reviewer));
    assert_eq!(listing_review.reviewer_note.as_deref(), Some("OK"));
    assert_eq!(listing_review.decided_at, Some(later));
    assert_eq!(listing_review.updated_at, later);
}

#[test]
fn approve_bumps_version() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let v0 = listing_review.version;
    listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    assert_eq!(listing_review.version, v0 + 1);
}

#[test]
fn approve_accepts_none_note() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve with none note ok");
    assert_eq!(
        listing_review.decision,
        Some(ListingReviewDecision::Approve)
    );
    assert!(listing_review.reviewer_note.is_none());
}

// ── decide_reject ─────────────────────────────────────────────

#[test]
fn reject_happy_path_records_note() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let reviewer = Id::<UserMarker>::new();
    let later = now + Duration::hours(2);
    listing_review
        .decide_reject(reviewer.clone(), "허위 매물 의심돼요".to_owned(), later)
        .expect("reject ok");
    assert_eq!(listing_review.decision, Some(ListingReviewDecision::Reject));
    assert_eq!(listing_review.reviewer_id, Some(reviewer));
    assert_eq!(
        listing_review.reviewer_note.as_deref(),
        Some("허위 매물 의심돼요")
    );
    assert_eq!(listing_review.decided_at, Some(later));
    assert_eq!(listing_review.version, 2);
}

#[test]
fn reject_without_note_errors() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let err = listing_review
        .decide_reject(Id::new(), String::new(), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        ListingReviewError::EmptyReviewerNote { action: "reject" }
    ));
    // 결정 전 검증 실패 — pending 그대로.
    assert!(listing_review.is_pending());
    assert_eq!(listing_review.version, 1);
}

#[test]
fn reject_with_whitespace_only_note_errors() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let err = listing_review
        .decide_reject(Id::new(), "   ".to_owned(), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        ListingReviewError::EmptyReviewerNote { action: "reject" }
    ));
}

// ── decide_request_changes ────────────────────────────────────

#[test]
fn request_changes_happy_path() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let reviewer = Id::<UserMarker>::new();
    let later = now + Duration::hours(3);
    listing_review
        .decide_request_changes(reviewer.clone(), "사진 다시 올려 주세요".to_owned(), later)
        .expect("request_changes ok");
    assert_eq!(
        listing_review.decision,
        Some(ListingReviewDecision::RequestChanges)
    );
    assert_eq!(listing_review.reviewer_id, Some(reviewer));
    assert_eq!(
        listing_review.reviewer_note.as_deref(),
        Some("사진 다시 올려 주세요")
    );
    assert_eq!(listing_review.decided_at, Some(later));
    assert_eq!(listing_review.version, 2);
}

#[test]
fn request_changes_without_note_errors() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let err = listing_review
        .decide_request_changes(Id::new(), String::new(), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        ListingReviewError::EmptyReviewerNote {
            action: "request_changes"
        }
    ));
    assert!(listing_review.is_pending());
}

// ── once-only (AlreadyDecided) ────────────────────────────────

#[test]
fn approved_cannot_be_decided_again_to_reject() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    let err = listing_review
        .decide_reject(Id::new(), "too late".to_owned(), now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(err, ListingReviewError::AlreadyDecided));
    assert_eq!(
        listing_review.decision,
        Some(ListingReviewDecision::Approve)
    );
}

#[test]
fn approved_cannot_be_decided_again_to_request_changes() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    let err = listing_review
        .decide_request_changes(Id::new(), "more".to_owned(), now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(err, ListingReviewError::AlreadyDecided));
}

#[test]
fn rejected_cannot_be_decided_again_to_approve() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_reject(Id::new(), "no good".to_owned(), now + Duration::hours(1))
        .expect("reject ok");
    let err = listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(err, ListingReviewError::AlreadyDecided));
}

#[test]
fn approved_cannot_be_approved_again() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(1))
        .expect("approve ok");
    let err = listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(err, ListingReviewError::AlreadyDecided));
}

#[test]
fn request_changes_cannot_be_decided_again() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_request_changes(Id::new(), "fix it".to_owned(), now + Duration::hours(1))
        .expect("rc ok");
    let err = listing_review
        .decide_approve(Id::new(), None, now + Duration::hours(2))
        .unwrap_err();
    assert!(matches!(err, ListingReviewError::AlreadyDecided));
}

// ── reviewer_note 길이 ────────────────────────────────────────

#[test]
fn reject_with_2000_char_note_accepted() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let exactly = "X".repeat(2000);
    listing_review
        .decide_reject(Id::new(), exactly.clone(), now + Duration::hours(1))
        .expect("2000 ok");
    assert_eq!(
        listing_review.reviewer_note.as_deref(),
        Some(exactly.as_str())
    );
}

#[test]
fn reject_with_2001_char_note_errors() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let too_long = "X".repeat(2001);
    let err = listing_review
        .decide_reject(Id::new(), too_long, now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        ListingReviewError::ReviewerNoteTooLong { actual: 2001 }
    ));
    // 결정 전 검증 실패 — pending 유지.
    assert!(listing_review.is_pending());
}

#[test]
fn approve_with_2001_char_note_errors() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    let too_long = "X".repeat(2001);
    let err = listing_review
        .decide_approve(Id::new(), Some(too_long), now + Duration::hours(1))
        .unwrap_err();
    assert!(matches!(
        err,
        ListingReviewError::ReviewerNoteTooLong { actual: 2001 }
    ));
    assert!(listing_review.is_pending());
}

// ── serde ──────────────────────────────────────────────────────

#[test]
fn serde_roundtrip_pending() {
    let now = Utc::now();
    let listing_review = make_pending(now);
    let json = serde_json::to_string(&listing_review).expect("serialize");
    let back: ListingReview = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(listing_review, back);
}

#[test]
fn serde_roundtrip_approved() {
    let now = Utc::now();
    let mut listing_review = make_pending(now);
    listing_review
        .decide_approve(Id::new(), Some("OK".to_owned()), now + Duration::hours(1))
        .expect("approve ok");
    let json = serde_json::to_string(&listing_review).expect("serialize");
    let back: ListingReview = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(listing_review, back);
}
