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

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

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
        MIGRATOR.run(&pool).await?;
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
        MIGRATOR.run(&pool).await?;
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
        MIGRATOR.run(&pool).await?;
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
        MIGRATOR.run(&pool).await?;
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
