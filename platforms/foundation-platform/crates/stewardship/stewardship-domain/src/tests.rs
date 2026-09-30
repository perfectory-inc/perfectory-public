use chrono::{DateTime, Duration, TimeZone, Utc};
use uuid::Uuid;

use super::{
    check_approval, check_decision, check_idempotency_key, claim_expiry, evidence_etag, Claim,
    DecisionDraft, Outcome, ReasonCode, ReviewItem, ReviewStatus, StewardshipError,
};

// Synthetic parcels in the repository-reserved 99999 namespace (public-fixture-safety).
const NEW: &str = "9999930100100010000";
const OLD: &str = "9999910100100010000";
const OTHER: &str = "9999910100100020000";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2099, 9, 30, 12, 0, 0)
        .single()
        .unwrap_or_default()
}

fn item(status: ReviewStatus, in_effect: bool) -> ReviewItem {
    ReviewItem {
        item_id: Uuid::nil(),
        subject_code: NEW.to_owned(),
        status,
        candidates_json: format!(
            "[{{\"evidence_kind\": \"area+category\", \"evidence_ref\": \"\", \"grade\": \"needs_review\", \
             \"in_effect\": {in_effect}, \"predecessor_pnu\": \"{OLD}\", \"relation\": \"jurisdiction_transfer\"}}]"
        ),
    }
}

fn draft(item: &ReviewItem, outcome: Outcome, predecessor: Option<&str>) -> DecisionDraft {
    DecisionDraft {
        outcome,
        predecessor_pnu: predecessor.map(ToOwned::to_owned),
        reason_code: outcome
            .writes_lineage()
            .then_some(ReasonCode::BuildingRegister),
        note: String::new(),
        evidence_etag: item.evidence_etag(),
        supersedes_decision_id: None,
    }
}

fn steward(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

#[test]
fn the_etag_matches_the_python_twin() {
    // sha256("pending\n[]"), computed by lineage_review_queue.evidence_etag.
    assert_eq!(
        evidence_etag(ReviewStatus::Pending, "[]"),
        "012053e5d2845d033fa0c2799a9646e3367fc51fa516d45bb0e2f6b9a5a61a8f"
    );
}

#[test]
fn a_link_to_a_candidate_is_accepted_by_one_person() -> Result<(), StewardshipError> {
    let item = item(ReviewStatus::NeedsReview, false);
    let needs_second = check_decision(
        &item,
        &draft(&item, Outcome::Link, Some(OLD)),
        steward(1),
        None,
        now(),
    )?;
    assert!(!needs_second);
    Ok(())
}

#[test]
fn a_decision_on_evidence_that_changed_is_refused() {
    let item = item(ReviewStatus::NeedsReview, false);
    let mut stale = draft(&item, Outcome::Link, Some(OLD));
    stale.evidence_etag = "0".repeat(64);
    assert!(matches!(
        check_decision(&item, &stale, steward(1), None, now()),
        Err(StewardshipError::StaleEvidence { .. })
    ));
}

#[test]
fn only_a_listed_candidate_can_be_linked() {
    let item = item(ReviewStatus::NeedsReview, false);
    assert_eq!(
        check_decision(
            &item,
            &draft(&item, Outcome::Link, Some(OTHER)),
            steward(1),
            None,
            now()
        ),
        Err(StewardshipError::NotACandidate(OTHER.to_owned()))
    );
    assert!(matches!(
        check_decision(
            &item,
            &draft(&item, Outcome::Link, None),
            steward(1),
            None,
            now()
        ),
        Err(StewardshipError::InvalidInput(_))
    ));
    assert!(matches!(
        check_decision(
            &item,
            &draft(&item, Outcome::NotALink, Some(OLD)),
            steward(1),
            None,
            now()
        ),
        Err(StewardshipError::InvalidInput(_))
    ));
}

#[test]
fn a_lineage_decision_cites_a_reason_and_other_needs_a_note() {
    let item = item(ReviewStatus::Pending, false);
    let mut no_reason = draft(&item, Outcome::NotALink, None);
    no_reason.reason_code = None;
    assert!(check_decision(&item, &no_reason, steward(1), None, now()).is_err());
    let mut other = draft(&item, Outcome::NotALink, None);
    other.reason_code = Some(ReasonCode::Other);
    assert!(check_decision(&item, &other, steward(1), None, now()).is_err());
    other.note = "new forest parcel, no predecessor".to_owned();
    assert_eq!(
        check_decision(&item, &other, steward(1), None, now()),
        Ok(false)
    );
    let unsure = draft(&item, Outcome::Unsure, None);
    assert_eq!(
        check_decision(&item, &unsure, steward(1), None, now()),
        Ok(false)
    );
}

#[test]
fn another_stewards_live_claim_blocks_but_an_expired_one_does_not() {
    let item = item(ReviewStatus::NeedsReview, false);
    let decision = draft(&item, Outcome::Link, Some(OLD));
    let live = Claim {
        claimed_by: steward(2),
        expires_at: claim_expiry(now()),
    };
    assert!(matches!(
        check_decision(&item, &decision, steward(1), Some(live), now()),
        Err(StewardshipError::ClaimedByAnother { .. })
    ));
    assert!(check_decision(&item, &decision, steward(2), Some(live), now()).is_ok());
    let later = now() + Duration::minutes(31);
    assert!(check_decision(&item, &decision, steward(1), Some(live), later).is_ok());
}

#[test]
fn overturning_a_link_in_effect_needs_a_second_person() -> Result<(), StewardshipError> {
    let sample = item(ReviewStatus::Sample, true);
    assert!(check_decision(
        &sample,
        &draft(&sample, Outcome::NotALink, None),
        steward(1),
        None,
        now()
    )?);
    assert!(!check_decision(
        &sample,
        &draft(&sample, Outcome::Link, Some(OLD)),
        steward(1),
        None,
        now()
    )?);
    assert!(!check_decision(
        &sample,
        &draft(&sample, Outcome::Unsure, None),
        steward(1),
        None,
        now()
    )?);
    Ok(())
}

#[test]
fn the_decider_cannot_approve_their_own_decision() {
    assert_eq!(
        check_approval(steward(1), true, steward(1)),
        Err(StewardshipError::SelfApproval)
    );
    assert!(check_approval(steward(1), true, steward(2)).is_ok());
    assert!(matches!(
        check_approval(steward(1), false, steward(2)),
        Err(StewardshipError::InvalidState(_))
    ));
}

#[test]
fn an_idempotency_key_binds_one_body() -> Result<(), StewardshipError> {
    let item = item(ReviewStatus::NeedsReview, false);
    let first = draft(&item, Outcome::Link, Some(OLD));
    let mut second = first.clone();
    assert_eq!(first.request_sha256()?, second.request_sha256()?);
    second.note = "changed".to_owned();
    assert_ne!(first.request_sha256()?, second.request_sha256()?);
    assert!(check_idempotency_key("decide-2099-0001").is_ok());
    assert!(check_idempotency_key("short").is_err());
    assert!(check_idempotency_key("has space in it").is_err());
    Ok(())
}

#[test]
fn a_whole_consistent_handoff_parses_and_anything_else_is_refused() {
    use super::handoff::{parse_handoff, HANDOFF_SCHEMA_VERSION};

    let good_etag = evidence_etag(ReviewStatus::Pending, "[]");
    let document = |count: usize, etag: &str, schema: &str| {
        format!(
            "{{\"schema_version\": \"{schema}\", \"unit\": \"parcel\", \"published_at_utc\": \"2099-09-30T00:00:00Z\",              \"item_count\": {count}, \"items\": [{{\"item_id\": \"{}\", \"subject_code\": \"{NEW}\",              \"status\": \"pending\", \"candidates_json\": \"[]\", \"evidence_etag\": \"{etag}\",              \"from_snapshot_id\": null, \"to_snapshot_id\": null}}]}}",
            Uuid::nil()
        )
    };
    assert!(parse_handoff(&document(1, &good_etag, HANDOFF_SCHEMA_VERSION)).is_ok());
    assert!(
        parse_handoff(&document(2, &good_etag, HANDOFF_SCHEMA_VERSION)).is_err(),
        "count mismatch"
    );
    assert!(
        parse_handoff(&document(1, &"0".repeat(64), HANDOFF_SCHEMA_VERSION)).is_err(),
        "etag mismatch"
    );
    assert!(
        parse_handoff(&document(1, &good_etag, "v0")).is_err(),
        "unknown schema"
    );
    let whole = document(1, &good_etag, HANDOFF_SCHEMA_VERSION);
    assert!(
        parse_handoff(&whole[..whole.len() - 3]).is_err(),
        "a file cut short"
    );
}

fn foldable(outcome: Outcome, predecessor: Option<&str>) -> super::fold::FoldableDecision {
    let item = item(ReviewStatus::NeedsReview, false);
    super::fold::FoldableDecision {
        decision_id: Uuid::from_u128(7),
        subject_code: NEW.to_owned(),
        outcome,
        predecessor_code: predecessor.map(ToOwned::to_owned),
        reason_code: Some(ReasonCode::BuildingRegister),
        decided_by: steward(1),
        decided_at: now(),
        evidence_etag: item.evidence_etag(),
        idempotency_key: "decide-key-0001".to_owned(),
        candidates_json: item.candidates_json,
        from_snapshot_id: "s1".to_owned(),
        to_snapshot_id: "s2".to_owned(),
    }
}

#[test]
fn a_link_folds_as_an_official_row_with_the_candidates_relation() -> Result<(), StewardshipError> {
    let row = super::fold::lineage_row(&foldable(Outcome::Link, Some(OLD)))?;
    assert_eq!(
        (
            row.predecessor_pnu.as_deref(),
            row.relation.as_str(),
            row.grade.as_str(),
            row.evidence_kind.as_str()
        ),
        (Some(OLD), "jurisdiction_transfer", "official", "steward")
    );
    let evidence: super::fold::StewardEvidence = serde_json::from_str(&row.evidence_ref)
        .map_err(|e| StewardshipError::InvalidInput(e.to_string()))?;
    assert_eq!(
        evidence.evidence_etag,
        item(ReviewStatus::NeedsReview, false).evidence_etag()
    );
    assert_eq!(row.effective_date, "2099-09-30");
    Ok(())
}

#[test]
fn not_a_link_folds_as_a_pending_row_that_links_nothing() -> Result<(), StewardshipError> {
    let row = super::fold::lineage_row(&foldable(Outcome::NotALink, None))?;
    assert_eq!((row.predecessor_pnu, row.grade.as_str()), (None, "pending"));
    Ok(())
}

#[test]
fn only_lineage_decisions_on_a_listed_candidate_fold() {
    assert!(super::fold::lineage_row(&foldable(Outcome::Unsure, None)).is_err());
    assert!(super::fold::lineage_row(&foldable(Outcome::Link, Some(OTHER))).is_err());
}
