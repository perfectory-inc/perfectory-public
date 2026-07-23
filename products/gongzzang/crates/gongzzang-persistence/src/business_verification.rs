//! `PgBusinessVerificationRepository` — `Postgres` 구현체. OCC + transactional `audit_log`/`outbox_event`.
//!
//! `save` 는 `INSERT … ON CONFLICT (id) DO UPDATE … WHERE version = $N` 로 OCC 를
//! 강제하고, 같은 트랜잭션 안에서 `audit_log` row 와 `MutationContext::events` 의
//! 각 도메인 이벤트마다 `outbox_event` row 를 함께 `INSERT` 해 transactional
//! 추적성/이벤트 발행을 보장해요.
//!
//! 흐름은 SP5-iii T5 [`crates/gongzzang-persistence/src/admin_action.rs`] 와 동일하지만 *INSERT-only*
//! 가 아니라 *UPSERT + OCC* 라는 점만 달라요:
//!
//! 1. `pool.begin()` 으로 트랜잭션 시작
//! 2. `INSERT … ON CONFLICT … WHERE version = $version` 로 Business Verification Queue 저장 (OCC)
//! 3. `rows_affected() == 0` → 버전 불일치 → `RepoError::Conflict` (tx 자동 rollback)
//! 4. `audit_log` row `INSERT`
//! 5. `ctx.events` 의 각 이벤트마다 `outbox_event` `INSERT`
//! 6. `tx.commit()`
//!
//! ## Entity-DB asymmetry
//!
//! `BusinessVerification` 엔티티에 `updated_at` 필드가 있지만 DB
//! `business_verification_queue` 테이블에는 컬럼이 없어요. INSERT/UPDATE 시
//! 바인딩하지 않고, SELECT 시 `reviewed_at.unwrap_or(submitted_at)` 으로 합성해요.
//! (spec FU 후보 — DB 에 컬럼 추가 OR 엔티티에서 제거.)

#![allow(clippy::module_name_repetitions)]

use async_trait::async_trait;
use business_verification_domain::entity::BusinessVerification;
use business_verification_domain::repository::{BusinessVerificationRepository, RepoError};
use business_verification_domain::status::BusinessVerificationStatus;
use chrono::{DateTime, Utc};
use shared_kernel::business_number::BusinessNumber;
use shared_kernel::id::{
    AuditLogMarker, BusinessVerificationMarker, Id, OutboxEventMarker, UserMarker,
};
use shared_kernel::mutation::MutationContext;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::instrument;

use crate::error_map::map_sqlx_err;

/// `BusinessVerification` Aggregate 의 `Postgres` 저장소.
///
/// `save` 는 OCC + transactional `audit_log`/`outbox_event` 패턴을 사용해요.
#[derive(Debug, Clone)]
pub struct PgBusinessVerificationRepository {
    pool: PgPool,
}

impl PgBusinessVerificationRepository {
    /// 새 저장소를 만들어요.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `select` 절에서 모든 `business_verification_queue` 컬럼을 일관되게 가져오기 위한 상수.
///
/// `updated_at` 은 DB 에 없어서 SELECT 에 포함되지 않아요 — `row_to_business_verification` 가 합성.
const BUSINESS_VERIFICATION_COLUMNS: &str =
    "id, user_id, business_number, submitted_documents, status, \
    reviewer_id, reviewer_note, submitted_at, reviewed_at, sla_due_at, version";

fn parse_status(s: &str) -> Result<BusinessVerificationStatus, RepoError> {
    match s {
        "pending" => Ok(BusinessVerificationStatus::Pending),
        "approved" => Ok(BusinessVerificationStatus::Approved),
        "rejected" => Ok(BusinessVerificationStatus::Rejected),
        "needs_more_info" => Ok(BusinessVerificationStatus::NeedsMoreInfo),
        other => Err(RepoError::Database(format!(
            "unexpected business_verification status: {other}"
        ))),
    }
}

/// `PgRow` → [`BusinessVerification`] 변환.
///
/// `updated_at` 은 DB 미존재 — `reviewed_at.unwrap_or(submitted_at)` 으로 합성.
fn row_to_business_verification(row: &PgRow) -> Result<BusinessVerification, RepoError> {
    let id_str: String = row
        .try_get("id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let user_id_str: String = row
        .try_get("user_id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let business_number_str: String = row
        .try_get("business_number")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let submitted_documents: serde_json::Value = row
        .try_get("submitted_documents")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let reviewer_id_str: Option<String> = row
        .try_get("reviewer_id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let reviewer_note: Option<String> = row
        .try_get("reviewer_note")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let submitted_at: DateTime<Utc> = row
        .try_get("submitted_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let reviewed_at: Option<DateTime<Utc>> = row
        .try_get("reviewed_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let sla_due_at: Option<DateTime<Utc>> = row
        .try_get("sla_due_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let version: i64 = row
        .try_get("version")
        .map_err(|e| RepoError::Database(e.to_string()))?;

    let id = Id::<BusinessVerificationMarker>::try_from_str(id_str.trim())
        .map_err(|e| RepoError::Database(format!("malformed business_verification id: {e}")))?;
    let user_id = Id::<UserMarker>::try_from_str(user_id_str.trim())
        .map_err(|e| RepoError::Database(format!("malformed user_id: {e}")))?;
    let business_number = BusinessNumber::try_new(&business_number_str)
        .map_err(|e| RepoError::Database(format!("malformed business_number in DB: {e}")))?;
    let status = parse_status(&status_str)?;
    let reviewer_id = reviewer_id_str
        .map(|s| {
            Id::<UserMarker>::try_from_str(s.trim())
                .map_err(|e| RepoError::Database(format!("malformed reviewer_id: {e}")))
        })
        .transpose()?;

    // Entity-DB asymmetry — DB 미존재 컬럼 합성:
    //   reviewed_at 있으면 그것, 없으면 submitted_at.
    let updated_at = reviewed_at.unwrap_or(submitted_at);

    Ok(BusinessVerification {
        id,
        user_id,
        business_number,
        submitted_documents,
        status,
        reviewer_id,
        reviewer_note,
        submitted_at,
        reviewed_at,
        sla_due_at,
        updated_at,
        version,
    })
}

#[async_trait]
impl BusinessVerificationRepository for PgBusinessVerificationRepository {
    /// 트랜잭션 안에서 Business Verification Queue + `audit_log` + `outbox_event` 를 함께 저장.
    ///
    /// OCC 는 `ON CONFLICT (id) DO UPDATE … WHERE version = $version` 로 강제해요.
    /// `rows_affected() == 0` 이면 INSERT 도 UPDATE 도 적용 안 된 거라 [`RepoError::Conflict`].
    /// tx Drop 시 자동 rollback 이므로 audit/outbox 도 들어가지 않아요.
    ///
    /// 새 row 의 경우 `version` 은 도메인이 정한 값 (보통 1) 그대로 들어가고,
    /// 업데이트의 경우 DB 가 `version + 1` 로 bump 해요. 호출자는 *충돌이 없으면*
    /// `business_verification.version` 을 `+1` 해도 되지만, OCC WHERE 가 *호출자가 읽었던* 버전을
    /// 비교하므로 도메인 메서드의 `version += 1` 결과를 그대로 넣어도 동작해요
    /// (DB UPDATE 의 `version + 1` 이 동일 값으로 수렴).
    ///
    /// `MutationContext` 매핑:
    /// - `ctx.actor_id` → `audit_log.actor_id` (`None` → `NULL`)
    /// - `ctx.action` → `audit_log.action`
    /// - `ctx.metadata` → `audit_log.after_state`
    /// - `ctx.client_ip` → `audit_log.ip_address` (`$N::inet` 캐스팅)
    /// - `ctx.user_agent` → `audit_log.user_agent`
    /// - `ctx.correlation_id` → `audit_log.correlation_id`
    /// - `ctx.occurred_at` → `audit_log.created_at` (`None` → `Utc::now()`)
    /// - `ctx.events` → 각 이벤트마다 `outbox_event` row 1개
    #[allow(clippy::needless_pass_by_value)]
    #[instrument(skip(self, business_verification, ctx), fields(
        business_verification_id = %business_verification.id.as_str(),
        version = business_verification.version,
        ctx_action = %ctx.action,
        correlation_id = %ctx.correlation_id,
        events_count = ctx.events.len(),
    ))]
    async fn save(
        &self,
        business_verification: &BusinessVerification,
        ctx: MutationContext,
    ) -> Result<(), RepoError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        // 1. UPSERT Business Verification Queue — OCC via WHERE version = $version (도메인이 들고 있는 버전).
        //
        //    INSERT 분기: 신규 row — `business_verification_queue.version` 컬럼은
        //      바인딩한 $11 값 (보통 1) 으로 그대로 들어감.
        //    UPDATE 분기: 기존 row — DB version 이 호출자 version 과 같을 때만
        //      적용되고, 컬럼은 `+1` 로 bump.
        //    버전 불일치 → `rows_affected() == 0` → `Conflict`.
        let result = sqlx::query(
            r"
            insert into business_verification_queue (
                id, user_id, business_number, submitted_documents, status,
                reviewer_id, reviewer_note,
                submitted_at, reviewed_at, sla_due_at, version
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            on conflict (id) do update set
                business_number = excluded.business_number,
                submitted_documents = excluded.submitted_documents,
                status = excluded.status,
                reviewer_id = excluded.reviewer_id,
                reviewer_note = excluded.reviewer_note,
                reviewed_at = excluded.reviewed_at,
                sla_due_at = excluded.sla_due_at,
                version = business_verification_queue.version + 1
            where business_verification_queue.version = $11
            ",
        )
        .bind(business_verification.id.as_str())
        .bind(business_verification.user_id.as_str())
        .bind(business_verification.business_number.as_str())
        .bind(&business_verification.submitted_documents)
        .bind(business_verification.status.as_str())
        .bind(business_verification.reviewer_id.as_ref().map(Id::as_str))
        .bind(business_verification.reviewer_note.as_deref())
        .bind(business_verification.submitted_at)
        .bind(business_verification.reviewed_at)
        .bind(business_verification.sla_due_at)
        .bind(business_verification.version)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        if result.rows_affected() == 0 {
            // INSERT 도 UPDATE 도 적용 안 됨 = OCC 버전 불일치.
            // tx Drop 시 자동 rollback — audit_log / outbox_event 도 안 들어감.
            return Err(RepoError::Conflict);
        }

        // 2. INSERT audit_log — 같은 tx
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
            values ($1, $2, $3, 'business_verification', $4, NULL, $5, $6::inet, $7, $8, $9)
            ",
        )
        .bind(audit_id.as_str())
        .bind(ctx.actor_id.as_ref().map(Id::as_str))
        .bind(&ctx.action)
        .bind(business_verification.id.as_str())
        .bind(&ctx.metadata)
        .bind(ctx.client_ip.as_deref())
        .bind(ctx.user_agent.as_deref())
        .bind(&ctx.correlation_id)
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        // 3. INSERT outbox_event for each ctx.events — 같은 tx
        for event in &ctx.events {
            let outbox_id = Id::<OutboxEventMarker>::new();
            sqlx::query(
                r"
                insert into outbox_event (
                    id, aggregate_kind, aggregate_id, event_type, payload,
                    correlation_id, created_at, published_at
                )
                values ($1, 'business_verification', $2, $3, $4, $5, $6, NULL)
                ",
            )
            .bind(outbox_id.as_str())
            .bind(business_verification.id.as_str())
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

    #[instrument(skip(self), fields(business_verification_id = %id.as_str()))]
    async fn find_by_id(
        &self,
        id: &Id<BusinessVerificationMarker>,
    ) -> Result<Option<BusinessVerification>, RepoError> {
        let sql = format!(
            "select {BUSINESS_VERIFICATION_COLUMNS} from business_verification_queue where id = $1"
        );
        let row = sqlx::query(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_business_verification).transpose()
    }

    #[instrument(skip(self), fields(limit))]
    async fn find_pending(&self, limit: u32) -> Result<Vec<BusinessVerification>, RepoError> {
        // SLA 임박 순: sla_due_at ASC, NULL 은 마지막. Pending partial index가 지원한다.
        // (where status = 'pending') 가 submitted_at 기준이라 상태 필터는
        // 인덱스로 가속됨.
        let sql = format!(
            "select {BUSINESS_VERIFICATION_COLUMNS} from business_verification_queue \
             where status = 'pending' \
             order by sla_due_at asc nulls last, submitted_at asc \
             limit $1"
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_business_verification).collect()
    }

    #[instrument(skip(self), fields(user_id = %user_id.as_str()))]
    async fn find_by_user(
        &self,
        user_id: &Id<UserMarker>,
    ) -> Result<Vec<BusinessVerification>, RepoError> {
        // 최신 제출 순. user_id 인덱스가 조회를 지원한다.
        let sql = format!(
            "select {BUSINESS_VERIFICATION_COLUMNS} from business_verification_queue \
             where user_id = $1 \
             order by submitted_at desc"
        );
        let rows = sqlx::query(&sql)
            .bind(user_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_business_verification).collect()
    }
}
