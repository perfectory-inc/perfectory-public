//! `PostgreSQL` behaviour of the steward store (root ADR-0115), against the real migrations.

use chrono::{DateTime, Duration, TimeZone, Utc};
use foundation_disposable_database::{run_in_disposable_database, TestResult};
use sqlx::PgPool;
use stewardship_application::{
    DecideCommand, DecideOutcome, LineageStewardshipStore, ReviewItemFilter,
};
use stewardship_domain::{
    evidence_etag, DecisionDraft, Outcome, ReasonCode, ReviewStatus, StewardshipError,
};
use stewardship_infrastructure::PgLineageStewardshipStore;
use uuid::Uuid;

/// The platform's migrations, read at run time: cargo runs a test from its package directory, so
/// the relative path resolves without a compile-time read (scripts/guard/build-coupling-baseline.sh).
async fn migrate(pool: &PgPool) -> TestResult {
    sqlx::migrate::Migrator::new(std::path::Path::new("../../../migrations"))
        .await?
        .run(pool)
        .await?;
    Ok(())
}

// Synthetic parcels in the repository-reserved 99999 namespace (public-fixture-safety).
const NEW_A: &str = "9999930100100010000";
const NEW_B: &str = "9999930100100020000";
const OLD: &str = "9999910100100010000";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2099, 9, 30, 12, 0, 0)
        .single()
        .unwrap_or_default()
}

fn candidates(in_effect: bool) -> String {
    format!(
        "[{{\"evidence_kind\": \"area+category\", \"evidence_ref\": \"\", \"grade\": \"needs_review\", \
         \"in_effect\": {in_effect}, \"predecessor_pnu\": \"{OLD}\", \"relation\": \"jurisdiction_transfer\"}}]"
    )
}

async fn item(
    pool: &PgPool,
    subject: &str,
    status: ReviewStatus,
    in_effect: bool,
) -> TestResult<(Uuid, String)> {
    let id = Uuid::new_v4();
    let json = candidates(in_effect);
    let etag = evidence_etag(status, &json);
    let wire = match status {
        ReviewStatus::NeedsReview => "needs_review",
        ReviewStatus::Pending => "pending",
        ReviewStatus::Sample => "sample",
    };
    sqlx::query(
        "INSERT INTO catalog.lineage_review_item
         (item_id, unit, subject_code, status, candidates_json, evidence_etag, queue_published_at)
         VALUES ($1, 'parcel', $2, $3, $4, $5, $6)",
    )
    .bind(id)
    .bind(subject)
    .bind(wire)
    .bind(&json)
    .bind(&etag)
    .bind(now())
    .execute(pool)
    .await?;
    Ok((id, etag))
}

fn decide(item_id: Uuid, etag: &str, decider: Uuid, key: &str, outcome: Outcome) -> DecideCommand {
    DecideCommand {
        item_id,
        draft: DecisionDraft {
            outcome,
            predecessor_pnu: (outcome == Outcome::Link).then(|| OLD.to_owned()),
            reason_code: outcome
                .writes_lineage()
                .then_some(ReasonCode::BuildingRegister),
            note: String::new(),
            evidence_etag: etag.to_owned(),
            supersedes_decision_id: None,
        },
        decider,
        idempotency_key: key.to_owned(),
        dry_run: false,
        now: now(),
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_claimed_item_is_decided_once_and_a_retry_replays() -> TestResult {
    run_in_disposable_database("steward_decide", |pool| async move {
        migrate(&pool).await?;
        let store = PgLineageStewardshipStore::new(pool.clone());
        let (id, etag) = item(&pool, NEW_A, ReviewStatus::NeedsReview, false).await?;
        let (alice, bob) = (Uuid::from_u128(1), Uuid::from_u128(2));

        store.claim(id, alice, now()).await?;
        let blocked = store
            .decide(decide(id, &etag, bob, "bob-decides-0001", Outcome::Link))
            .await;
        assert!(
            matches!(blocked, Err(StewardshipError::ClaimedByAnother { .. })),
            "{blocked:?}"
        );

        let mut dry = decide(id, &etag, alice, "", Outcome::Link);
        dry.dry_run = true;
        assert_eq!(
            store.decide(dry).await?,
            DecideOutcome::WouldRecord {
                requires_approval: false
            }
        );
        assert!(
            store.get_item(id, now()).await?.decisions.is_empty(),
            "a dry run stores nothing"
        );

        let first = store
            .decide(decide(
                id,
                &etag,
                alice,
                "alice-decides-0001",
                Outcome::Link,
            ))
            .await?;
        let DecideOutcome::Recorded(record) = first else {
            return Err("expected a recorded decision".into());
        };
        let again = store
            .decide(decide(
                id,
                &etag,
                alice,
                "alice-decides-0001",
                Outcome::Link,
            ))
            .await?;
        assert_eq!(again, DecideOutcome::Replayed(record.clone()));
        let other_body = store
            .decide(decide(
                id,
                &etag,
                alice,
                "alice-decides-0001",
                Outcome::Unsure,
            ))
            .await;
        assert_eq!(other_body, Err(StewardshipError::IdempotencyConflict));

        let view = store.get_item(id, now()).await?;
        assert_eq!(view.decisions.len(), 1);
        assert!(
            view.claim.is_none(),
            "deciding releases the decider's claim"
        );
        let open = store
            .list_items(
                ReviewItemFilter {
                    undecided_only: true,
                    limit: 10,
                    ..ReviewItemFilter::default()
                },
                now(),
            )
            .await?;
        assert!(open.is_empty(), "a linked item is no longer open");
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn stale_evidence_and_a_taken_predecessor_are_refused() -> TestResult {
    run_in_disposable_database("steward_refusals", |pool| async move {
        migrate(&pool).await?;
        let store = PgLineageStewardshipStore::new(pool.clone());
        let (a, etag_a) = item(&pool, NEW_A, ReviewStatus::NeedsReview, false).await?;
        let (b, etag_b) = item(&pool, NEW_B, ReviewStatus::NeedsReview, false).await?;
        let alice = Uuid::from_u128(1);

        let stale = store
            .decide(decide(
                a,
                &"0".repeat(64),
                alice,
                "stale-evidence-01",
                Outcome::Link,
            ))
            .await;
        assert!(
            matches!(stale, Err(StewardshipError::StaleEvidence { .. })),
            "{stale:?}"
        );

        store
            .decide(decide(a, &etag_a, alice, "link-a-to-old-01", Outcome::Link))
            .await?;
        let twice = store
            .decide(decide(b, &etag_b, alice, "link-b-to-old-01", Outcome::Link))
            .await;
        assert!(
            matches!(twice, Err(StewardshipError::PredecessorTaken { .. })),
            "{twice:?}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn overturning_a_link_in_effect_waits_for_a_second_person() -> TestResult {
    run_in_disposable_database("steward_four_eyes", |pool| async move {
        migrate(&pool).await?;
        let store = PgLineageStewardshipStore::new(pool.clone());
        let (id, etag) = item(&pool, NEW_A, ReviewStatus::Sample, true).await?;
        let (alice, bob) = (Uuid::from_u128(1), Uuid::from_u128(2));

        let DecideOutcome::Recorded(decision) =
            store.decide(decide(id, &etag, alice, "overturn-sample-01", Outcome::NotALink)).await?
        else {
            return Err("expected a recorded decision".into());
        };
        assert!(decision.requires_approval);
        let own = store.approve(decision.decision_id, alice, true, String::new()).await;
        assert_eq!(own, Err(StewardshipError::SelfApproval));

        // The table refuses it too, whoever writes to it.
        let direct = sqlx::query(
            "INSERT INTO catalog.lineage_steward_approval (approval_id, decision_id, verdict, approved_by)
             VALUES ($1, $2, 'approved', $3)",
        )
        .bind(Uuid::new_v4())
        .bind(decision.decision_id)
        .bind(alice)
        .execute(&pool)
        .await;
        assert!(direct.is_err(), "the four-eyes trigger must refuse a self-approval");

        let approved = store.approve(decision.decision_id, bob, true, String::new()).await?;
        assert_eq!(approved.approval.as_deref(), Some("approved"));
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn decisions_cannot_be_rewritten_and_claims_expire() -> TestResult {
    run_in_disposable_database("steward_append_only", |pool| async move {
        migrate(&pool).await?;
        let store = PgLineageStewardshipStore::new(pool.clone());
        let (id, etag) = item(&pool, NEW_A, ReviewStatus::Pending, false).await?;
        let (alice, bob) = (Uuid::from_u128(1), Uuid::from_u128(2));

        store.claim(id, alice, now()).await?;
        let later = now() + Duration::minutes(31);
        assert_eq!(
            store.claim(id, bob, later).await?.claimed_by,
            bob,
            "an expired claim returns to the queue"
        );

        let mut command = decide(id, &etag, bob, "no-predecessor-01", Outcome::NotALink);
        command.now = later;
        store.decide(command).await?;
        for statement in [
            "UPDATE catalog.lineage_steward_decision SET note = 'rewritten'",
            "DELETE FROM catalog.lineage_steward_decision",
        ] {
            assert!(
                sqlx::query(statement).execute(&pool).await.is_err(),
                "{statement} must be refused"
            );
        }
        Ok(())
    })
    .await
}

fn handoff_text(at: &str, subjects: &[(u128, &str)]) -> String {
    let items = subjects
        .iter()
        .map(|(id, code)| {
            format!(
                "{{\"item_id\": \"{}\", \"subject_code\": \"{code}\", \"status\": \"pending\", \
                 \"candidates_json\": \"[]\", \"evidence_etag\": \"{}\", \"from_snapshot_id\": null, \
                 \"to_snapshot_id\": null}}",
                Uuid::from_u128(*id),
                evidence_etag(ReviewStatus::Pending, "[]")
            )
        })
        .collect::<Vec<_>>();
    format!(
        "{{\"schema_version\": \"foundation-platform.lineage_review_handoff.v1\", \"unit\": \"parcel\", \
         \"published_at_utc\": \"{at}\", \"item_count\": {}, \"items\": [{}]}}",
        items.len(),
        items.join(",")
    )
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_handoff_replaces_the_queue_whole_and_never_goes_back() -> TestResult {
    use stewardship_domain::handoff::parse_handoff;

    run_in_disposable_database("steward_handoff", |pool| async move {
        migrate(&pool).await?;
        let store = PgLineageStewardshipStore::new(pool.clone());
        let both = [(101, NEW_A), (102, NEW_B)];
        let first = store
            .replace_review_items(&parse_handoff(&handoff_text(
                "2099-09-01T00:00:00Z",
                &both,
            ))?)
            .await?;
        assert_eq!(
            (first.previous, first.loaded, first.added, first.removed),
            (0, 2, 2, 0)
        );
        store
            .claim(Uuid::from_u128(102), Uuid::from_u128(1), now())
            .await?;
        let second = store
            .replace_review_items(&parse_handoff(&handoff_text(
                "2099-09-02T00:00:00Z",
                &both[..1],
            ))?)
            .await?;
        assert_eq!(
            (second.previous, second.loaded, second.added, second.removed),
            (2, 1, 0, 1)
        );
        let older = store
            .replace_review_items(&parse_handoff(&handoff_text(
                "2099-08-01T00:00:00Z",
                &both,
            ))?)
            .await;
        assert!(
            matches!(older, Err(StewardshipError::InvalidState(_))),
            "{older:?}"
        );
        let listed = store
            .list_items(
                ReviewItemFilter {
                    limit: 10,
                    ..ReviewItemFilter::default()
                },
                now(),
            )
            .await?;
        let codes: Vec<&str> = listed
            .iter()
            .map(|v| v.item.subject_code.as_str())
            .collect();
        assert_eq!(codes, [NEW_A]);
        let claims: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog.lineage_review_claim")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            claims, 0,
            "a claim on an item that left the queue goes with it"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn only_standing_decisions_in_effect_fold_and_each_folds_once() -> TestResult {
    run_in_disposable_database("steward_fold", |pool| async move {
        migrate(&pool).await?;
        let store = PgLineageStewardshipStore::new(pool.clone());
        for (subject, status) in [(NEW_A, ReviewStatus::NeedsReview), (NEW_B, ReviewStatus::Sample)] {
            let (id, _) = item(&pool, subject, status, status == ReviewStatus::Sample).await?;
            sqlx::query("UPDATE catalog.lineage_review_item SET from_snapshot_id = 's1', to_snapshot_id = 's2' WHERE item_id = $1")
                .bind(id)
                .execute(&pool)
                .await?;
        }
        let items = store.list_items(ReviewItemFilter { limit: 10, ..ReviewItemFilter::default() }, now()).await?;
        let (a, b) = (&items[0].item, &items[1].item);
        let alice = Uuid::from_u128(1);
        store.decide(decide(a.item_id, &a.evidence_etag(), alice, "fold-link-a-0001", Outcome::Link)).await?;
        // Overturning the sample's link waits for a second person, so it does not fold yet.
        store.decide(decide(b.item_id, &b.evidence_etag(), alice, "fold-overturn-b-01", Outcome::NotALink)).await?;
        store.decide(decide(a.item_id, &a.evidence_etag(), alice, "fold-unsure-a-0001", Outcome::Unsure)).await?;

        let foldable = store.foldable_decisions().await?;
        assert_eq!(foldable.iter().map(|d| d.subject_code.as_str()).collect::<Vec<_>>(), [NEW_A]);
        let row = stewardship_domain::fold::lineage_row(&foldable[0])?;
        assert_eq!((row.grade.as_str(), row.from_snapshot_id.as_str()), ("official", "s1"));

        let ids = [foldable[0].decision_id];
        assert_eq!(store.record_folds(&ids, "steward-fold-test").await?, 1);
        assert_eq!(store.record_folds(&ids, "steward-fold-test").await?, 0, "recording twice changes nothing");
        assert!(store.foldable_decisions().await?.is_empty());
        Ok(())
    })
    .await
}
