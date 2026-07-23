//! Catalog vertical slice 의 end-to-end 검증.
//!
//! 두 시나리오:
//! 1. **Happy path** — `PgCatalogUnitOfWork::create_complex` → repo 로 find → outbox row 존재 → cleanup
//! 2. **Rollback path** — 중복 `official_complex_code` 로 conflict 유도 → complex / outbox 양쪽 모두 row 0 (atomicity)
//!
//! 로컬 Docker 스택이 떠 있을 때만 실행 — `DATABASE_URL` 미설정 시 자동 skip.
//! 수동 실행:
//!
//! ```bash
//! docker compose up -d
//! $env:DATABASE_URL = "postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
//! cargo test -p catalog-infrastructure --test catalog_round_trip -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr)]

use catalog_application::ports::{
    CatalogRepository, CatalogUnitOfWork, UpsertIndustrialComplexCommand,
};
use catalog_domain::{IndustrialComplex, IndustrialComplexKind};
use catalog_infrastructure::{PgCatalogRepository, PgCatalogUnitOfWork};
use chrono::Utc;
use foundation_shared_kernel::ids::ComplexId;
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    match PgPool::connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skipping — could not connect to DATABASE_URL: {e}");
            None
        }
    }
}

#[tokio::test]
#[ignore = "requires local docker stack — `cargo test -- --ignored` to run"]
async fn happy_path_uow_creates_complex_and_outbox_atomically() {
    let Some(pool) = pool().await else {
        return;
    };

    let repo = PgCatalogRepository::new(pool.clone());
    let uow = PgCatalogUnitOfWork::new(pool.clone());

    let complex = sample_complex();

    // 1) UoW 실행 — 한 tx 안에서 complex INSERT + outbox INSERT
    uow.create_complex(&complex).await.expect("create_complex");

    // 2) read repo 가 같은 complex 를 본다
    let found = repo
        .find_complex(complex.id)
        .await
        .expect("find")
        .expect("must exist after create");
    assert_eq!(found.id, complex.id);
    assert_eq!(found.name, complex.name);
    assert_eq!(found.kind, complex.kind);
    assert_eq!(found.area_m2, complex.area_m2);
    assert_eq!(found.version, 1);

    // 3) outbox 에 IndustrialComplexCreated.v1 row 존재 + payload 가 도메인 entity 와 일치.
    // serde(tag = "type") 는 internally-tagged — 페이로드 필드가 같은 JSON object 에 flat.
    let row: (String, serde_json::Value) = sqlx::query_as(
        "SELECT type, payload FROM catalog.outbox_event
         WHERE payload->>'type' = $1 AND payload->>'complex_id' = $2
         ORDER BY occurred_at DESC LIMIT 1",
    )
    .bind("catalog.industrial_complex.created.v2")
    .bind(complex.id.to_string())
    .fetch_one(&pool)
    .await
    .expect("outbox row");
    assert_eq!(row.0, "catalog.industrial_complex.created.v2");
    assert_eq!(
        row.1["official_complex_code"].as_str(),
        Some(complex.official_complex_code.as_str())
    );
    assert_eq!(
        row.1["primary_bjdong_code"].as_str(),
        Some(complex.primary_bjdong_code.as_str())
    );

    cleanup(&pool, complex.id, &complex.primary_bjdong_code).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn rollback_path_official_code_conflict_leaves_no_partial_state() {
    let Some(pool) = pool().await else {
        return;
    };

    let uow = PgCatalogUnitOfWork::new(pool.clone());

    // 1) 첫 산단을 정상 INSERT (selecting 시 outbox row 1개 발생).
    let first = sample_complex();
    uow.create_complex(&first).await.expect("first create");

    // 2) 같은 official_complex_code 로 두 번째 시도 — ComplexOfficialCodeConflict 로 실패해야 함.
    let mut conflict = sample_complex();
    conflict
        .official_complex_code
        .clone_from(&first.official_complex_code);
    let err = uow
        .create_complex(&conflict)
        .await
        .expect_err("must fail with conflict");
    assert!(
        matches!(
            err,
            catalog_domain::CatalogError::ComplexOfficialCodeConflict(_)
        ),
        "expected ComplexOfficialCodeConflict, got {err:?}"
    );

    // 3) **rollback 검증**: 두 번째 시도의 complex row 가 DB 에 없고, outbox 에도
    //    그 complex_id 로 IndustrialComplexCreated 이벤트가 *생기지 않았어야* 한다.
    let conflict_row: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM catalog.industrial_complex WHERE id = $1")
            .bind(conflict.id.as_uuid())
            .fetch_optional(&pool)
            .await
            .expect("conflict row check");
    assert!(
        conflict_row.is_none(),
        "atomicity violated — conflict complex row leaked into DB"
    );

    let conflict_outbox: Option<(Uuid,)> = sqlx::query_as(
        "SELECT event_id FROM catalog.outbox_event
         WHERE payload->>'type' = $1 AND payload->>'complex_id' = $2",
    )
    .bind("catalog.industrial_complex.created.v2")
    .bind(conflict.id.to_string())
    .fetch_optional(&pool)
    .await
    .expect("conflict outbox check");
    assert!(
        conflict_outbox.is_none(),
        "atomicity violated — outbox row leaked for failed insert"
    );

    cleanup(&pool, first.id, &first.primary_bjdong_code).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn upsert_by_official_code_creates_then_updates_existing_complex() {
    let Some(pool) = pool().await else {
        return;
    };

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let official_complex_code = format!("IC-{}", Uuid::new_v4().simple());
    let first_primary_bjdong_code = random_primary_bjdong_code();
    let second_primary_bjdong_code = random_primary_bjdong_code();

    let created = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            official_complex_code: official_complex_code.clone(),
            name: "E2E imported complex".to_owned(),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: first_primary_bjdong_code.clone(),
            area_m2: 1_000,
        }])
        .await
        .expect("create via upsert")
        .pop()
        .expect("one created complex");

    assert_eq!(created.official_complex_code, official_complex_code);
    assert_eq!(created.version, 1);

    let updated = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            official_complex_code: official_complex_code.clone(),
            name: "E2E imported complex updated".to_owned(),
            kind: IndustrialComplexKind::National,
            primary_bjdong_code: second_primary_bjdong_code.clone(),
            area_m2: 2_000,
        }])
        .await
        .expect("update via upsert")
        .pop()
        .expect("one updated complex");

    assert_eq!(updated.id, created.id);
    assert_eq!(updated.official_complex_code, official_complex_code);
    assert_eq!(updated.primary_bjdong_code, second_primary_bjdong_code);
    assert_eq!(updated.area_m2, 2_000);
    assert_eq!(updated.version, 2);

    let update_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM catalog.outbox_event
         WHERE payload->>'type' = 'catalog.industrial_complex.updated.v1'
           AND payload->>'complex_id' = $1
         ORDER BY occurred_at DESC
         LIMIT 1",
    )
    .bind(updated.id.to_string())
    .fetch_one(&pool)
    .await
    .expect("updated outbox row");
    let changed_fields: Vec<String> =
        serde_json::from_value(update_payload["changed_fields"].clone())
            .expect("changed_fields json array");
    assert_eq!(
        changed_fields,
        vec![
            "name".to_owned(),
            "kind".to_owned(),
            "primary_bjdong_code".to_owned(),
            "area_m2".to_owned(),
        ]
    );

    cleanup_by_complex_id(&pool, updated.id).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn upsert_by_official_code_allows_multiple_complexes_in_same_bjdong() {
    let Some(pool) = pool().await else {
        return;
    };

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let shared_bjdong_code = random_primary_bjdong_code();
    let first_official_code = format!("IC-{}", Uuid::new_v4().simple());
    let second_official_code = format!("IC-{}", Uuid::new_v4().simple());

    let complexes = uow
        .upsert_complexes_by_official_code(&[
            UpsertIndustrialComplexCommand {
                official_complex_code: first_official_code,
                name: "E2E shared bjdong complex A".to_owned(),
                kind: IndustrialComplexKind::General,
                primary_bjdong_code: shared_bjdong_code.clone(),
                area_m2: 1_000,
            },
            UpsertIndustrialComplexCommand {
                official_complex_code: second_official_code,
                name: "E2E shared bjdong complex B".to_owned(),
                kind: IndustrialComplexKind::National,
                primary_bjdong_code: shared_bjdong_code,
                area_m2: 2_000,
            },
        ])
        .await
        .expect("distinct official codes may share one legal-dong locator");

    assert_eq!(complexes.len(), 2);
    assert_ne!(complexes[0].id, complexes[1].id);
    assert_eq!(
        complexes[0].primary_bjdong_code,
        complexes[1].primary_bjdong_code
    );

    for complex in complexes {
        cleanup_by_complex_id(&pool, complex.id).await;
    }
}

fn sample_complex() -> IndustrialComplex {
    let now = Utc::now();
    IndustrialComplex {
        id: ComplexId::new(Uuid::now_v7()),
        official_complex_code: format!("IC-{}", Uuid::new_v4().simple()),
        name: format!("E2E 테스트 산단 {}", Uuid::new_v4()),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: random_primary_bjdong_code(),
        area_m2: 1_234_567,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 1,
    }
}

async fn cleanup(pool: &PgPool, complex_id: ComplexId, primary_bjdong_code: &str) {
    sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = $1 OR primary_bjdong_code = $2")
        .bind(complex_id.as_uuid())
        .bind(primary_bjdong_code)
        .execute(pool)
        .await
        .expect("cleanup complex");
    sqlx::query(
        "DELETE FROM catalog.outbox_event
         WHERE payload->>'type' LIKE 'catalog.industrial_complex.%'
           AND payload->>'primary_bjdong_code' = $1",
    )
    .bind(primary_bjdong_code)
    .execute(pool)
    .await
    .expect("cleanup outbox");
}

async fn cleanup_by_complex_id(pool: &PgPool, complex_id: ComplexId) {
    sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = $1")
        .bind(complex_id.as_uuid())
        .execute(pool)
        .await
        .expect("cleanup complex");
    sqlx::query(
        "DELETE FROM catalog.outbox_event
         WHERE payload->>'type' LIKE 'catalog.industrial_complex.%'
           AND payload->>'complex_id' = $1",
    )
    .bind(complex_id.to_string())
    .execute(pool)
    .await
    .expect("cleanup outbox");
}

fn random_primary_bjdong_code() -> String {
    let uid = Uuid::new_v4().simple().to_string();
    let digits: String = uid.chars().filter(char::is_ascii_digit).take(10).collect();
    format!("{digits:0<10}")[..10].to_owned()
}
