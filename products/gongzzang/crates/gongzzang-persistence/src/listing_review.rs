//! `PgListingReviewRepository` — `Postgres` 구현체. OCC + transactional `audit_log`/`outbox_event`.
//!
//! `save` 는 `INSERT … ON CONFLICT (id) DO UPDATE … WHERE version = $N` 로 OCC 를
//! 강제하고, 같은 트랜잭션 안에서 `audit_log` row 와 `MutationContext::events` 의
//! 각 도메인 이벤트마다 `outbox_event` row 를 함께 `INSERT` 해 transactional
//! 추적성/이벤트 발행을 보장해요.
//!
//! 흐름은 SP5-iii T6 [`crates/gongzzang-persistence/src/business_verification.rs`] 와 동일하지만 Listing Review Queue 특성만 달라요:
//!
//! 1. `pool.begin()` 으로 트랜잭션 시작
//! 2. `INSERT … ON CONFLICT … WHERE version = $version` 로 Listing Review Queue 저장 (OCC)
//! 3. `rows_affected() == 0` → 버전 불일치 → `RepoError::Conflict` (tx 자동 rollback)
//! 4. `audit_log` row `INSERT` (`resource_kind = 'listing_review'`)
//! 5. `ctx.events` 의 각 이벤트마다 `outbox_event` `INSERT` (`aggregate_kind = 'listing_review'`)
//! 6. `tx.commit()`
//!
//! ## Listing Review Queue vs Business Verification Queue 차이
//!
//! - `decision: Option<ListingReviewDecision>` (Business Verification Queue 는 `status: BusinessVerificationStatus`) — `None` = pending,
//!   `Some(_)` = terminal.
//! - `listing_id: Id<ListingMarker>` FK (Business Verification Queue 는 `user_id`).
//! - `auto_check_score: Option<u8>` (0-100) ↔ DB `int` 변환.
//! - `auto_check_flags: Option<serde_json::Value>` (`JSONB`).
//! - `decided_at` (Business Verification Queue 는 `reviewed_at`).
//! - SLA 12h (Business Verification Queue 는 24h).
//!
//! ## Entity-DB asymmetry
//!
//! `ListingReview` 엔티티에 `updated_at` 필드가 있지만 DB
//! `listing_review_queue` 테이블에는 컬럼이 없어요. `INSERT`/`UPDATE` 시
//! 바인딩하지 않고, `SELECT` 시 `decided_at.unwrap_or(submitted_at)` 으로 합성해요.
//! (Business Verification Queue T6 와 동일 패턴 — spec FU 후보.)

#![allow(clippy::module_name_repetitions)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use listing_review_domain::decision::ListingReviewDecision;
use listing_review_domain::entity::ListingReview;
use listing_review_domain::repository::{ListingReviewRepository, RepoError};
use shared_kernel::id::{
    AuditLogMarker, Id, ListingMarker, ListingReviewMarker, OutboxEventMarker, UserMarker,
};
use shared_kernel::mutation::MutationContext;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::instrument;

use crate::error_map::map_sqlx_err;

/// `ListingReview` Aggregate 의 `Postgres` 저장소.
///
/// `save` 는 OCC + transactional `audit_log`/`outbox_event` 패턴을 사용해요.
#[derive(Debug, Clone)]
pub struct PgListingReviewRepository {
    pool: PgPool,
}

impl PgListingReviewRepository {
    /// 새 저장소를 만들어요.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `select` 절에서 모든 `listing_review_queue` 컬럼을 일관되게 가져오기 위한 상수.
///
/// `updated_at` 은 DB 에 없어서 `SELECT` 에 포함되지 않아요 — `row_to_listing_review` 가 합성.
const LISTING_REVIEW_COLUMNS: &str =
    "id, listing_id, submitted_at, auto_check_score, auto_check_flags, \
    reviewer_id, reviewer_note, decision, decided_at, sla_due_at, version";

fn parse_decision(s: Option<&str>) -> Result<Option<ListingReviewDecision>, RepoError> {
    match s {
        None => Ok(None),
        Some("approve") => Ok(Some(ListingReviewDecision::Approve)),
        Some("reject") => Ok(Some(ListingReviewDecision::Reject)),
        Some("request_changes") => Ok(Some(ListingReviewDecision::RequestChanges)),
        Some(other) => Err(RepoError::Database(format!(
            "unexpected listing_review decision: {other}"
        ))),
    }
}

/// `PgRow` → [`ListingReview`] 변환.
///
/// `updated_at` 은 DB 미존재 — `decided_at.unwrap_or(submitted_at)` 으로 합성.
/// `auto_check_score` 는 DB `int` → Rust `u8` 변환 (`0-100` 도메인 invariant).
fn row_to_listing_review(row: &PgRow) -> Result<ListingReview, RepoError> {
    let id_str: String = row
        .try_get("id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let listing_id_str: String = row
        .try_get("listing_id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let submitted_at: DateTime<Utc> = row
        .try_get("submitted_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let auto_check_score_i32: Option<i32> = row
        .try_get("auto_check_score")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let auto_check_flags: Option<serde_json::Value> = row
        .try_get("auto_check_flags")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let reviewer_id_str: Option<String> = row
        .try_get("reviewer_id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let reviewer_note: Option<String> = row
        .try_get("reviewer_note")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let decision_str: Option<String> = row
        .try_get("decision")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let decided_at: Option<DateTime<Utc>> = row
        .try_get("decided_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let sla_due_at: Option<DateTime<Utc>> = row
        .try_get("sla_due_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let version: i64 = row
        .try_get("version")
        .map_err(|e| RepoError::Database(e.to_string()))?;

    let id = Id::<ListingReviewMarker>::try_from_str(id_str.trim())
        .map_err(|e| RepoError::Database(format!("malformed listing_review id: {e}")))?;
    let listing_id = Id::<ListingMarker>::try_from_str(listing_id_str.trim())
        .map_err(|e| RepoError::Database(format!("malformed listing_id: {e}")))?;
    let auto_check_score = auto_check_score_i32
        .map(|v| {
            u8::try_from(v).map_err(|e| {
                RepoError::Database(format!("invalid auto_check_score in DB ({v}): {e}"))
            })
        })
        .transpose()?;
    let reviewer_id = reviewer_id_str
        .map(|s| {
            Id::<UserMarker>::try_from_str(s.trim())
                .map_err(|e| RepoError::Database(format!("malformed reviewer_id: {e}")))
        })
        .transpose()?;
    let decision = parse_decision(decision_str.as_deref())?;

    // Entity-DB asymmetry — DB 미존재 컬럼 합성:
    //   decided_at 있으면 그것, 없으면 submitted_at.
    let updated_at = decided_at.unwrap_or(submitted_at);

    Ok(ListingReview {
        id,
        listing_id,
        submitted_at,
        auto_check_score,
        auto_check_flags,
        reviewer_id,
        reviewer_note,
        decision,
        decided_at,
        sla_due_at,
        updated_at,
        version,
    })
}

#[async_trait]
impl ListingReviewRepository for PgListingReviewRepository {
    /// 트랜잭션 안에서 Listing Review Queue + `audit_log` + `outbox_event` 를 함께 저장.
    ///
    /// OCC 는 `ON CONFLICT (id) DO UPDATE … WHERE version = $version` 로 강제해요.
    /// `rows_affected() == 0` 이면 INSERT 도 UPDATE 도 적용 안 된 거라 [`RepoError::Conflict`].
    /// tx Drop 시 자동 rollback 이므로 `audit_log`/`outbox_event` 도 들어가지 않아요.
    ///
    /// 새 row 의 경우 `version` 은 도메인이 정한 값 (보통 1) 그대로 들어가고,
    /// 업데이트의 경우 DB 가 `version + 1` 로 bump 해요.
    ///
    /// `MutationContext` 매핑:
    /// - `ctx.actor_id` → `audit_log.actor_id` (`None` → `NULL`)
    /// - `ctx.action` → `audit_log.action`
    /// - `ctx.metadata` → `audit_log.after_state`
    /// - `ctx.client_ip` → `audit_log.ip_address` (`$N::inet` 캐스팅)
    /// - `ctx.user_agent` → `audit_log.user_agent`
    /// - `ctx.correlation_id` → `audit_log.correlation_id`
    /// - `ctx.occurred_at` → `audit_log.created_at` (`None` → `Utc::now()`)
    /// - `ctx.events` → 각 이벤트마다 `outbox_event` row 1개 (`aggregate_kind = 'listing_review'`)
    #[allow(clippy::needless_pass_by_value)]
    #[instrument(skip(self, listing_review, ctx), fields(
        listing_review_id = %listing_review.id.as_str(),
        version = listing_review.version,
        ctx_action = %ctx.action,
        correlation_id = %ctx.correlation_id,
        events_count = ctx.events.len(),
    ))]
    async fn save(
        &self,
        listing_review: &ListingReview,
        ctx: MutationContext,
    ) -> Result<(), RepoError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        // 1. UPSERT Listing Review Queue — OCC via WHERE version = $version (도메인이 들고 있는 버전).
        //
        //    INSERT 분기: 신규 row — `listing_review_queue.version` 컬럼은
        //      바인딩한 $11 값 (보통 1) 으로 그대로 들어감.
        //    UPDATE 분기: 기존 row — DB version 이 호출자 version 과 같을 때만
        //      적용되고, 컬럼은 `+1` 로 bump.
        //    버전 불일치 → `rows_affected() == 0` → `Conflict`.
        let auto_check_score_i32: Option<i32> = listing_review.auto_check_score.map(i32::from);
        let result = sqlx::query(
            r"
            insert into listing_review_queue (
                id, listing_id, submitted_at, auto_check_score, auto_check_flags,
                reviewer_id, reviewer_note,
                decision, decided_at, sla_due_at, version
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            on conflict (id) do update set
                auto_check_score = excluded.auto_check_score,
                auto_check_flags = excluded.auto_check_flags,
                reviewer_id = excluded.reviewer_id,
                reviewer_note = excluded.reviewer_note,
                decision = excluded.decision,
                decided_at = excluded.decided_at,
                sla_due_at = excluded.sla_due_at,
                version = listing_review_queue.version + 1
            where listing_review_queue.version = $11
            ",
        )
        .bind(listing_review.id.as_str())
        .bind(listing_review.listing_id.as_str())
        .bind(listing_review.submitted_at)
        .bind(auto_check_score_i32)
        .bind(listing_review.auto_check_flags.as_ref())
        .bind(listing_review.reviewer_id.as_ref().map(Id::as_str))
        .bind(listing_review.reviewer_note.as_deref())
        .bind(listing_review.decision.map(ListingReviewDecision::as_str))
        .bind(listing_review.decided_at)
        .bind(listing_review.sla_due_at)
        .bind(listing_review.version)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        if result.rows_affected() == 0 {
            // INSERT 도 UPDATE 도 적용 안 됨 = OCC 버전 불일치.
            // tx Drop 시 자동 rollback — audit_log / outbox_event 도 안 들어감.
            return Err(RepoError::Conflict);
        }

        // 2. INSERT audit_log — 같은 tx, resource_kind = 'listing_review'
        let audit_id = Id::<AuditLogMarker>::new();
        let occurred_at = ctx.occurred_at.unwrap_or_else(Utc::now);
        sqlx::query(
            r"
            insert into audit_log (
                id, actor_id, action, resource_kind, resource_id,
                before_state, after_state,
                ip_address, user_agent,
                correlation_id, created_at
            )
            values ($1, $2, $3, 'listing_review', $4, NULL, $5, $6::inet, $7, $8, $9)
            ",
        )
        .bind(audit_id.as_str())
        .bind(ctx.actor_id.as_ref().map(Id::as_str))
        .bind(&ctx.action)
        .bind(listing_review.id.as_str())
        .bind(&ctx.metadata)
        .bind(ctx.client_ip.as_deref())
        .bind(ctx.user_agent.as_deref())
        .bind(&ctx.correlation_id)
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        // 3. INSERT outbox_event for each ctx.events — 같은 tx, aggregate_kind = 'listing_review'
        for event in &ctx.events {
            let outbox_id = Id::<OutboxEventMarker>::new();
            sqlx::query(
                r"
                insert into outbox_event (
                    id, aggregate_kind, aggregate_id, event_type, payload,
                    correlation_id, created_at, published_at
                )
                values ($1, 'listing_review', $2, $3, $4, $5, $6, NULL)
                ",
            )
            .bind(outbox_id.as_str())
            .bind(listing_review.id.as_str())
            .bind(event.event_type())
            .bind(event.payload())
            .bind(&ctx.correlation_id)
            .bind(event.occurred_at())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        }

        // 4. commit — 실패 시 자동 rollback (tx Drop)
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(())
    }

    #[instrument(skip(self), fields(listing_review_id = %id.as_str()))]
    async fn find_by_id(
        &self,
        id: &Id<ListingReviewMarker>,
    ) -> Result<Option<ListingReview>, RepoError> {
        let sql =
            format!("select {LISTING_REVIEW_COLUMNS} from listing_review_queue where id = $1");
        let row = sqlx::query(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_listing_review).transpose()
    }

    #[instrument(skip(self), fields(limit))]
    async fn find_pending(&self, limit: u32) -> Result<Vec<ListingReview>, RepoError> {
        // SLA 임박 순: sla_due_at ASC, NULL 은 마지막. Pending partial index가 지원한다.
        // (where decision is null) 가 submitted_at 기준이라 pending 필터는
        // 인덱스로 가속됨.
        let sql = format!(
            "select {LISTING_REVIEW_COLUMNS} from listing_review_queue \
             where decision is null \
             order by sla_due_at asc nulls last, submitted_at asc \
             limit $1"
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_listing_review).collect()
    }

    #[instrument(skip(self), fields(listing_id = %listing_id.as_str()))]
    async fn find_by_listing(
        &self,
        listing_id: &Id<ListingMarker>,
    ) -> Result<Option<ListingReview>, RepoError> {
        // 매물당 활성 큐는 1건이라는 가정 (trait doc). 안전을 위해 가장 최근
        // submitted_at 한 건 반환.
        let sql = format!(
            "select {LISTING_REVIEW_COLUMNS} from listing_review_queue \
             where listing_id = $1 \
             order by submitted_at desc \
             limit 1"
        );
        let row = sqlx::query(&sql)
            .bind(listing_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_listing_review).transpose()
    }
}
