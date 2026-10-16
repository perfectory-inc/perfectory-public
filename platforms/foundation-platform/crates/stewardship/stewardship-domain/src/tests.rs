use chrono::{DateTime, Duration, TimeZone, Utc};
use uuid::Uuid;

use super::{
    check_approval, check_decision, check_idempotency_key, claim_expiry, evidence_etag, Claim,
    DecisionDraft, ItemKind, Outcome, ReasonCode, ReviewItem, ReviewStatus, StewardshipError,
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
        kind: ItemKind::Parcel,
        subject_code: NEW.to_owned(),
        status,
        candidates_json: format!(
            "[{{\"evidence_kind\": \"area+category\", \"evidence_ref\": \"\", \"grade\": \"needs_review\", \
             \"in_effect\": {in_effect}, \"predecessor_pnu\": \"{OLD}\", \"relation\": \"jurisdiction_transfer\"}}]"
        ),
        evidence_json: None,
    }
}

fn draft(item: &ReviewItem, outcome: Outcome, predecessor: Option<&str>) -> DecisionDraft {
    DecisionDraft {
        outcome,
        candidate_code: predecessor.map(ToOwned::to_owned),
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

// Synthetic 법정동 codes in the repository-reserved 99999 namespace (public-fixture-safety).
const OLD_DONG: &str = "9999910100";
const NEW_DONG: &str = "9999930100";
const OTHER_DONG: &str = "9999930200";

fn dong_item(status: ReviewStatus, offered: &[&str]) -> ReviewItem {
    let candidates = offered
        .iter()
        .map(|code| {
            format!(
                "{{\"code\": \"{code}\", \"name\": \"합성동\", \"created_date\": \"2099-07-01\"}}"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    ReviewItem {
        item_id: Uuid::nil(),
        kind: ItemKind::LegalDong,
        subject_code: OLD_DONG.to_owned(),
        status,
        candidates_json: format!("[{candidates}]"),
        evidence_json: Some("{}".to_owned()),
    }
}

fn dong_draft(item: &ReviewItem, outcome: Outcome, code: Option<&str>) -> DecisionDraft {
    DecisionDraft {
        outcome,
        candidate_code: code.map(ToOwned::to_owned),
        reason_code: outcome
            .writes_lineage()
            .then_some(ReasonCode::OfficialDocument),
        note: "합성 사유".to_owned(),
        evidence_etag: item.evidence_etag(),
        supersedes_decision_id: None,
    }
}

#[test]
fn a_legal_dong_pair_names_an_offered_code_and_says_why() -> Result<(), StewardshipError> {
    let item = dong_item(ReviewStatus::Steward, &[NEW_DONG]);
    let approve = dong_draft(&item, Outcome::Link, Some(NEW_DONG));
    assert!(!check_decision(&item, &approve, steward(1), None, now())?);
    let reject = dong_draft(&item, Outcome::NotALink, None);
    assert!(!check_decision(&item, &reject, steward(1), None, now())?);

    assert_eq!(
        check_decision(
            &item,
            &dong_draft(&item, Outcome::Link, Some(OTHER_DONG)),
            steward(1),
            None,
            now()
        ),
        Err(StewardshipError::NotACandidate(OTHER_DONG.to_owned())),
        "a code the run did not offer is refused"
    );
    let silent = DecisionDraft {
        note: " ".to_owned(),
        ..approve
    };
    assert!(
        matches!(
            check_decision(&item, &silent, steward(1), None, now()),
            Err(StewardshipError::InvalidInput(_))
        ),
        "an approval says why"
    );
    Ok(())
}

#[test]
fn a_split_or_a_change_awaiting_data_can_never_be_approved() {
    for status in [ReviewStatus::Split, ReviewStatus::AwaitingData] {
        let item = dong_item(status, &[NEW_DONG, OTHER_DONG]);
        for (outcome, code) in [(Outcome::Link, Some(NEW_DONG)), (Outcome::NotALink, None)] {
            assert_eq!(
                check_decision(
                    &item,
                    &dong_draft(&item, outcome, code),
                    steward(1),
                    None,
                    now()
                ),
                Err(StewardshipError::DataDecides {
                    subject: OLD_DONG.to_owned(),
                    status
                }),
                "{status} {outcome:?}"
            );
        }
    }
}

#[test]
fn a_legal_dong_item_without_candidates_takes_another_well_formed_code() {
    let item = dong_item(ReviewStatus::Steward, &[]);
    assert!(check_decision(
        &item,
        &dong_draft(&item, Outcome::Link, Some(NEW_DONG)),
        steward(1),
        None,
        now()
    )
    .is_ok());
    for bad in [OLD_DONG, "99999", "9999930100100010000"] {
        assert_eq!(
            check_decision(
                &item,
                &dong_draft(&item, Outcome::Link, Some(bad)),
                steward(1),
                None,
                now()
            ),
            Err(StewardshipError::NotACandidate(bad.to_owned())),
            "{bad}"
        );
    }
}

#[test]
fn a_legal_dong_handoff_carries_its_evidence_and_only_its_statuses() {
    use super::handoff::{parse_handoff, HANDOFF_SCHEMA_VERSION};

    let document = |status: &str, candidates: &str, evidence: &str| {
        let etag = super::status_from_wire(status)
            .map(|s| evidence_etag(s, candidates))
            .unwrap_or_default();
        serde_json::json!({
            "schema_version": HANDOFF_SCHEMA_VERSION, "unit": "legal_dong",
            "published_at_utc": "2099-09-30T00:00:00Z", "item_count": 1,
            "items": [{"item_id": Uuid::nil(), "subject_code": OLD_DONG, "status": status,
                       "candidates_json": candidates, "evidence_etag": etag,
                       "from_snapshot_id": null, "to_snapshot_id": null,
                       "evidence_json": serde_json::from_str::<serde_json::Value>(evidence).ok()
                           .map(|_| evidence.to_owned())}]
        })
        .to_string()
    };
    let recorded = serde_json::to_string(&super::LegalDongEvidence::default()).unwrap_or_default();
    assert!(
        parse_handoff(&document("steward", "[]", "{}")).is_err(),
        "evidence of another shape"
    );
    let offered =
        format!("[{{\"code\": \"{NEW_DONG}\", \"name\": \"합성동\", \"created_date\": \"\"}}]");
    assert!(parse_handoff(&document("steward", &offered, &recorded)).is_ok());
    assert!(
        parse_handoff(&document("pending", &offered, &recorded)).is_err(),
        "a parcel status"
    );
    assert!(
        parse_handoff(&document("steward", &offered, "null")).is_err(),
        "no evidence"
    );
    let itself = format!("[{{\"code\": \"{OLD_DONG}\", \"name\": \"\", \"created_date\": \"\"}}]");
    assert!(
        parse_handoff(&document("steward", &itself, &recorded)).is_err(),
        "the old code as its own candidate"
    );
}
