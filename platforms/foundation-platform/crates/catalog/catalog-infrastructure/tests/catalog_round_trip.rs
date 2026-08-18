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
    UpsertIndustrialComplexEffect,
};
use catalog_domain::{
    IndustrialComplex, IndustrialComplexKind, IndustrialComplexLotSalesStatus,
    IndustrialComplexStatus,
};
use catalog_infrastructure::{PgCatalogRepository, PgCatalogUnitOfWork};
use chrono::{NaiveDate, Utc};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId};
use sqlx::PgPool;
use uuid::Uuid;

/// Connection for this `#[ignore]`d live suite.
///
/// Both failure modes abort the test. A database that is configured but
/// unreachable used to return `None`, and the caller returned early — so a
/// connectivity regression inside the integration job would silently convert
/// these contract tests into no-ops behind a green check.
async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation`");
    PgPool::connect(&url)
        .await
        .expect("connect to the database in DATABASE_URL")
}

#[tokio::test]
#[ignore = "requires local docker stack — `cargo test -- --ignored` to run"]
async fn happy_path_uow_creates_complex_and_outbox_atomically() {
    let pool = pool().await;

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
    .bind("catalog.industrial_complex.created.v3")
    .bind(complex.id.to_string())
    .fetch_one(&pool)
    .await
    .expect("outbox row");
    assert_eq!(row.0, "catalog.industrial_complex.created.v3");
    assert_eq!(
        row.1["official_complex_code"].as_str(),
        Some(complex.official_complex_code.as_str())
    );
    assert_eq!(
        row.1["primary_bjdong_code"].as_str(),
        complex.primary_bjdong_code.as_deref()
    );

    cleanup(&pool, complex.id, complex.primary_bjdong_code.as_deref()).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn rollback_path_official_code_conflict_leaves_no_partial_state() {
    let pool = pool().await;

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
    .bind("catalog.industrial_complex.created.v3")
    .bind(conflict.id.to_string())
    .fetch_optional(&pool)
    .await
    .expect("conflict outbox check");
    assert!(
        conflict_outbox.is_none(),
        "atomicity violated — outbox row leaked for failed insert"
    );

    cleanup(&pool, first.id, first.primary_bjdong_code.as_deref()).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn upsert_by_official_code_creates_then_updates_existing_complex() {
    let pool = pool().await;

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let official_complex_code = format!("IC-{}", Uuid::new_v4().simple());
    let first_primary_bjdong_code = random_primary_bjdong_code();
    let second_primary_bjdong_code = random_primary_bjdong_code();

    let created = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            official_complex_code: official_complex_code.clone(),
            name: "E2E imported complex".to_owned(),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: Some(first_primary_bjdong_code.clone()),
            area_m2: 1_000,
            ..identity_only_command()
        }])
        .await
        .expect("create via upsert")
        .pop()
        .expect("one created complex");

    assert_eq!(created.effect, UpsertIndustrialComplexEffect::Inserted);
    let created = created.complex;
    assert_eq!(created.official_complex_code, official_complex_code);
    assert_eq!(created.version, 1);

    let updated = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            official_complex_code: official_complex_code.clone(),
            name: "E2E imported complex updated".to_owned(),
            kind: IndustrialComplexKind::National,
            primary_bjdong_code: Some(second_primary_bjdong_code.clone()),
            area_m2: 2_000,
            ..identity_only_command()
        }])
        .await
        .expect("update via upsert")
        .pop()
        .expect("one updated complex");

    assert_eq!(updated.effect, UpsertIndustrialComplexEffect::Updated);
    let updated = updated.complex;
    assert_eq!(updated.id, created.id);
    assert_eq!(updated.official_complex_code, official_complex_code);
    assert_eq!(
        updated.primary_bjdong_code.as_deref(),
        Some(second_primary_bjdong_code.as_str())
    );
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
    let pool = pool().await;

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
                primary_bjdong_code: Some(shared_bjdong_code.clone()),
                area_m2: 1_000,
                lakehouse_complex_id: None,
                status: None,
                sido_code: None,
                sigungu_code: None,
                address_text: None,
                management_agency_name: None,
                developer_name: None,
                designated_date: None,
                construction_start_date: None,
                completion_date: None,
                development_progress_percent: None,
                lot_sales_status: None,
                business_period_raw: None,
                business_period_start_month: None,
                business_period_end_month: None,
                designation_basis_law_raw: None,
                development_method_raw: None,
                development_purpose_raw: None,
                invited_industries_raw: None,
            },
            UpsertIndustrialComplexCommand {
                official_complex_code: second_official_code,
                name: "E2E shared bjdong complex B".to_owned(),
                kind: IndustrialComplexKind::National,
                primary_bjdong_code: Some(shared_bjdong_code),
                area_m2: 2_000,
                lakehouse_complex_id: None,
                status: None,
                sido_code: None,
                sigungu_code: None,
                address_text: None,
                management_agency_name: None,
                developer_name: None,
                designated_date: None,
                construction_start_date: None,
                completion_date: None,
                development_progress_percent: None,
                lot_sales_status: None,
                business_period_raw: None,
                business_period_start_month: None,
                business_period_end_month: None,
                designation_basis_law_raw: None,
                development_method_raw: None,
                development_purpose_raw: None,
                invited_industries_raw: None,
            },
        ])
        .await
        .expect("distinct official codes may share one legal-dong locator");

    assert_eq!(complexes.len(), 2);
    assert_ne!(complexes[0].complex.id, complexes[1].complex.id);
    assert_eq!(
        complexes[0].complex.primary_bjdong_code,
        complexes[1].complex.primary_bjdong_code
    );

    for outcome in complexes {
        cleanup_by_complex_id(&pool, outcome.complex.id).await;
    }
}

/// A complex whose source resolved no legal-dong code must survive a round trip as `None` rather
/// than being rejected by the write path or read back as an invented value (root ADR-0040).
#[tokio::test]
#[ignore = "requires local docker stack"]
async fn upsert_by_official_code_stores_a_complex_without_a_legal_dong_code() {
    let pool = pool().await;

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let official_complex_code = format!("IC-{}", Uuid::new_v4().simple());

    let created = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            official_complex_code: official_complex_code.clone(),
            name: "E2E complex without a legal-dong code".to_owned(),
            kind: IndustrialComplexKind::Agricultural,
            primary_bjdong_code: None,
            area_m2: 11_081,
            ..identity_only_command()
        }])
        .await
        .expect("create via upsert without a legal-dong code")
        .pop()
        .expect("one created complex");

    assert_eq!(created.effect, UpsertIndustrialComplexEffect::Inserted);
    let created = created.complex;
    assert_eq!(created.primary_bjdong_code, None);

    let stored: Option<String> = sqlx::query_scalar(
        "SELECT primary_bjdong_code FROM catalog.industrial_complex WHERE id = $1",
    )
    .bind(created.id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("stored complex");
    assert_eq!(stored, None);

    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM catalog.outbox_event
         WHERE payload->>'type' = 'catalog.industrial_complex.created.v3'
           AND payload->>'complex_id' = $1
         ORDER BY occurred_at DESC
         LIMIT 1",
    )
    .bind(created.id.to_string())
    .fetch_one(&pool)
    .await
    .expect("created outbox row");
    assert!(payload["primary_bjdong_code"].is_null());

    // A second identical command must change nothing rather than bump the version.
    let repeated = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            official_complex_code,
            name: "E2E complex without a legal-dong code".to_owned(),
            kind: IndustrialComplexKind::Agricultural,
            primary_bjdong_code: None,
            area_m2: 11_081,
            ..identity_only_command()
        }])
        .await
        .expect("repeat upsert")
        .pop()
        .expect("one repeated complex");
    assert_eq!(repeated.effect, UpsertIndustrialComplexEffect::Unchanged);
    assert_eq!(repeated.complex.version, 1);

    cleanup_by_complex_id(&pool, created.id).await;
}

/// Everything the Gold loader now writes must survive Postgres and come back unchanged.
///
/// The write path spells eight description columns plus the lakehouse identity into two SQL
/// statements and reads them back through a shared projection list. A column present in the
/// migration and missed by one of those is a runtime failure, not a compile error, so the round
/// trip is what pins it. The second half checks the other direction: a later snapshot that stops
/// carrying a value must clear the canonical column rather than leave the old one standing.
#[tokio::test]
#[ignore = "requires local docker stack"]
async fn upsert_by_official_code_round_trips_every_sourced_column() {
    let pool = pool().await;

    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let repo = PgCatalogRepository::new(pool.clone());
    let official_complex_code = format!("IC-{}", Uuid::new_v4().simple());
    let lakehouse_complex_id = synthetic_lakehouse_complex_id();

    let sourced = fully_sourced_command(official_complex_code.clone(), lakehouse_complex_id);

    let created = uow
        .upsert_complexes_by_official_code(std::slice::from_ref(&sourced))
        .await
        .expect("create a fully sourced complex")
        .pop()
        .expect("one created complex");
    assert_eq!(created.effect, UpsertIndustrialComplexEffect::Inserted);
    let created = created.complex;

    // Read through the repository rather than the write path's own return value: the two use the
    // same projection, and a column missing from it would otherwise be invisible here.
    let stored = repo
        .find_complex(created.id)
        .await
        .expect("find the stored complex")
        .expect("the complex exists");
    assert_every_sourced_column_round_tripped(&stored, lakehouse_complex_id);

    // Re-applying the same snapshot must be a no-op, not a version bump.
    let repeated = uow
        .upsert_complexes_by_official_code(std::slice::from_ref(&sourced))
        .await
        .expect("repeat the same upsert")
        .pop()
        .expect("one repeated complex");
    assert_eq!(repeated.effect, UpsertIndustrialComplexEffect::Unchanged);
    assert_eq!(repeated.complex.version, 1);

    // A snapshot that stopped carrying the values clears them.
    let cleared = uow
        .upsert_complexes_by_official_code(&[UpsertIndustrialComplexCommand {
            lakehouse_complex_id: Some(lakehouse_complex_id),
            status: None,
            sido_code: None,
            sigungu_code: None,
            address_text: None,
            management_agency_name: None,
            developer_name: None,
            designated_date: None,
            construction_start_date: None,
            completion_date: None,
            development_progress_percent: None,
            lot_sales_status: None,
            business_period_raw: None,
            business_period_start_month: None,
            business_period_end_month: None,
            designation_basis_law_raw: None,
            development_method_raw: None,
            development_purpose_raw: None,
            invited_industries_raw: None,
            ..sourced
        }])
        .await
        .expect("clear the sourced columns")
        .pop()
        .expect("one cleared complex");
    assert_eq!(cleared.effect, UpsertIndustrialComplexEffect::Updated);
    let cleared = cleared.complex;
    assert_eq!(cleared.id, created.id, "the natural key must keep its id");
    assert_eq!(cleared.status, None);
    assert_eq!(cleared.address_text, None);
    assert_eq!(cleared.designated_date, None);
    assert_eq!(cleared.construction_start_date, None);
    assert_eq!(cleared.completion_date, None);
    assert_eq!(cleared.development_progress_percent, None);
    assert_eq!(cleared.lot_sales_status, None);
    assert_eq!(cleared.business_period_raw, None);
    assert_eq!(cleared.business_period_start_month, None);
    assert_eq!(cleared.business_period_end_month, None);
    assert_eq!(cleared.designation_basis_law_raw, None);
    assert_eq!(cleared.development_method_raw, None);
    assert_eq!(cleared.development_purpose_raw, None);
    assert_eq!(cleared.invited_industries_raw, None);

    cleanup_by_complex_id(&pool, created.id).await;
}

/// Every column `fully_sourced_command` states, read back off the stored aggregate.
fn assert_every_sourced_column_round_tripped(
    stored: &IndustrialComplex,
    lakehouse_complex_id: LakehouseComplexId,
) {
    assert_eq!(stored.lakehouse_complex_id, Some(lakehouse_complex_id));
    assert_eq!(stored.status, Some(IndustrialComplexStatus::Operating));
    assert_eq!(stored.sido_code.as_deref(), Some("46"));
    assert_eq!(stored.sigungu_code.as_deref(), Some("46840"));
    assert_eq!(
        stored.address_text.as_deref(),
        Some("E2E 시 E2E 군 E2E 읍 일원")
    );
    assert_eq!(
        stored.management_agency_name.as_deref(),
        Some("E2E 관리기관")
    );
    assert_eq!(stored.developer_name.as_deref(), Some("E2E 시행자"));
    assert_eq!(stored.designated_date, NaiveDate::from_ymd_opt(1964, 4, 15));
    assert_eq!(
        stored.construction_start_date,
        NaiveDate::from_ymd_opt(1965, 3, 12)
    );
    assert_eq!(stored.completion_date, NaiveDate::from_ymd_opt(1974, 11, 5));
    // Exactly the digits that went in. `59.9` has no exact binary representation, so anything
    // that had passed through an `f64` would come back as a number nobody stated.
    assert_eq!(
        stored.development_progress_percent.as_deref(),
        Some("59.90")
    );
    assert_eq!(
        stored.lot_sales_status,
        Some(IndustrialComplexLotSalesStatus::InProgress)
    );
    assert_eq!(
        stored.business_period_raw.as_deref(),
        Some("1964-04~1974-11")
    );
    assert_eq!(
        stored.business_period_start_month.as_deref(),
        Some("1964-04")
    );
    assert_eq!(stored.business_period_end_month.as_deref(), Some("1974-11"));
    assert_eq!(
        stored.designation_basis_law_raw.as_deref(),
        Some("산업입지 및 개발에 관한 법률")
    );
    // The spelling the source used, suffix and space intact: this column is not an enumeration.
    assert_eq!(
        stored.development_method_raw.as_deref(),
        Some("공영개발 방식")
    );
    assert_eq!(
        stored.development_purpose_raw.as_deref(),
        Some("E2E 조성목적")
    );
    assert_eq!(
        stored.invited_industries_raw.as_deref(),
        Some("E2E 유치업종")
    );
}

/// The command a Gold snapshot produces when every sourced column carries a value.
fn fully_sourced_command(
    official_complex_code: String,
    lakehouse_complex_id: LakehouseComplexId,
) -> UpsertIndustrialComplexCommand {
    UpsertIndustrialComplexCommand {
        official_complex_code,
        name: "E2E fully sourced complex".to_owned(),
        kind: IndustrialComplexKind::Agricultural,
        primary_bjdong_code: None,
        area_m2: 272_089,
        lakehouse_complex_id: Some(lakehouse_complex_id),
        status: Some(IndustrialComplexStatus::Operating),
        sido_code: Some("46".to_owned()),
        sigungu_code: Some("46840".to_owned()),
        address_text: Some("E2E 시 E2E 군 E2E 읍 일원".to_owned()),
        management_agency_name: Some("E2E 관리기관".to_owned()),
        developer_name: Some("E2E 시행자".to_owned()),
        designated_date: NaiveDate::from_ymd_opt(1964, 4, 15),
        construction_start_date: NaiveDate::from_ymd_opt(1965, 3, 12),
        completion_date: NaiveDate::from_ymd_opt(1974, 11, 5),
        // `59.90` rather than a round number on purpose: it is the value an `f64` cannot hold
        // exactly, so a round trip that returns it unchanged is the evidence that no float is on
        // this path. The column is `numeric(5,2)`, the bind is `$n::numeric`, and the projection
        // reads it back as `::text`.
        development_progress_percent: Some("59.90".to_owned()),
        lot_sales_status: Some(IndustrialComplexLotSalesStatus::InProgress),
        business_period_raw: Some("1964-04~1974-11".to_owned()),
        business_period_start_month: Some("1964-04".to_owned()),
        business_period_end_month: Some("1974-11".to_owned()),
        designation_basis_law_raw: Some("산업입지 및 개발에 관한 법률".to_owned()),
        development_method_raw: Some("공영개발 방식".to_owned()),
        development_purpose_raw: Some("E2E 조성목적".to_owned()),
        invited_industries_raw: Some("E2E 유치업종".to_owned()),
    }
}

/// An upsert command that establishes identity and states nothing else.
///
/// Most of these tests are about the natural key, the version, and the outbox — not about the
/// nineteen sourced columns a Gold snapshot fills. Spelling `None` nineteen times in each of them
/// buries the field that the test is actually about.
const fn identity_only_command() -> UpsertIndustrialComplexCommand {
    UpsertIndustrialComplexCommand {
        official_complex_code: String::new(),
        name: String::new(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: None,
        area_m2: 0,
        lakehouse_complex_id: None,
        status: None,
        sido_code: None,
        sigungu_code: None,
        address_text: None,
        management_agency_name: None,
        developer_name: None,
        designated_date: None,
        construction_start_date: None,
        completion_date: None,
        development_progress_percent: None,
        lot_sales_status: None,
        business_period_raw: None,
        business_period_start_month: None,
        business_period_end_month: None,
        designation_basis_law_raw: None,
        development_method_raw: None,
        development_purpose_raw: None,
        invited_industries_raw: None,
    }
}

fn sample_complex() -> IndustrialComplex {
    let now = Utc::now();
    IndustrialComplex {
        id: ComplexId::new(Uuid::now_v7()),
        official_complex_code: format!("IC-{}", Uuid::new_v4().simple()),
        name: format!("E2E 테스트 산단 {}", Uuid::new_v4()),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: Some(random_primary_bjdong_code()),
        area_m2: 1_234_567,
        lakehouse_complex_id: None,
        status: None,
        sido_code: None,
        sigungu_code: None,
        address_text: None,
        management_agency_name: None,
        developer_name: None,
        designated_date: None,
        construction_start_date: None,
        completion_date: None,
        development_progress_percent: None,
        lot_sales_status: None,
        business_period_raw: None,
        business_period_start_month: None,
        business_period_end_month: None,
        designation_basis_law_raw: None,
        development_method_raw: None,
        development_purpose_raw: None,
        invited_industries_raw: None,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 1,
    }
}

async fn cleanup(pool: &PgPool, complex_id: ComplexId, primary_bjdong_code: Option<&str>) {
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

/// A unique id in the shape a derived lakehouse `complex_id` has: UUID version 5, RFC 4122 variant.
///
/// Shaped rather than actually derived. The stored column's CHECK is what this exercises — it
/// rejects a locally minted v7, which is the confusion the column exists to prevent — and this
/// crate does not carry the `v5` feature because nothing in it derives one. Uniqueness matters
/// because the column is unique where present, so two runs must not collide.
fn synthetic_lakehouse_complex_id() -> LakehouseComplexId {
    let mut bytes = *Uuid::new_v4().as_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    LakehouseComplexId::new(Uuid::from_bytes(bytes))
}

fn random_primary_bjdong_code() -> String {
    let uid = Uuid::new_v4().simple().to_string();
    let digits: String = uid.chars().filter(char::is_ascii_digit).take(10).collect();
    format!("{digits:0<10}")[..10].to_owned()
}
