---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Lakehouse 백필·스키마 재빌드

## 목적

스키마 변경·Spark 작업 수정·원천 정정 뒤 Foundation Platform이 Bronze 데이터를 Silver로
재생하거나 Gold projection을 다시 만들어야 할 때 이 런북을 사용한다. Iceberg가 lakehouse
정본이며 Postgres·PostGIS·검색·벡터 타일·서비스 캐시는 파생 산출물이다.

## 사전 조건

- `crates/lakehouse/lakehouse-domain/src/lakehouse.rs`의 대상 테이블 계약을 확인한다.
- Spark 작업과 workspace 계약이 통과하는지 확인한다.
  - `python -m py_compile infra/lakehouse/spark/jobs/*.py`
  - `cargo test --workspace --all-features`
- 쓰기 전에 source snapshot ID, 대상 테이블, 파티션 범위, 예상 행 수, 롤백 스냅샷을 정한다.
- 이 런북에서 쿼터에 영향을 주는 공공 API 수집을 실행하지 않는다. 백필은 이미 보관된
  Bronze 또는 승인된 handoff 입력에서 시작한다.

## 백필 계획

1. 계획 입력·대상 테이블·예상 행 수·운영자가 들어간 실행 기록을 만든다.
2. 먼저 staging 또는 smoke 테이블에서 Spark 작업을 실행한다.
3. 발행된 `foundation-platform.spark_run_summary.v1`을 검증한다.
   - `contract`가 정적 Rust 테이블 계약과 일치한다.
   - `row_count`와 `persisted_row_count`가 예상과 일치한다.
   - 차단 품질 지표가 0이다.
   - 계보에 source snapshot ID가 들어 있다.
4. staging 테이블에 읽기 전용 스모크를 실행한다.
5. 감사 행을 기록하고 검토한 뒤에만 승격한다.

## 스키마 재빌드

1. 먼저 Rust lakehouse 계약을 추가하거나 갱신한다.
2. Rust 계약 산출물에 맞게 Spark projection을 갱신한다.
3. `python -m py_compile infra/lakehouse/spark/jobs/*.py`를 실행한다.
4. 새 테이블 버전을 staging 테이블에 쓴다.
5. 이전 스냅샷과 행 수·필수 컬럼·대표 ID를 비교한다.
6. 재빌드를 새 batch 감사 행으로 기록한다.

## 실시간 DB 스키마 드리프트 확인

마이그레이션 기반 계약 fixture는 `docs/db/catalog-schema-contract.v1.example.json`이다. CI의
`postgres-integration` job은 핵심 extension·table·column이 마이그레이션 SQL에 계속 존재하는지
확인하고, `sqlx migrate run` 뒤 계약을 실시간 `pg_extension`·`information_schema.columns`와
비교한다. 마이그레이션 상태는 다음으로 로컬에서 확인할 수 있다.

```bash
sqlx migrate info
```

장애 기록이나 staging 데이터베이스와의 수동 드리프트 비교가 필요하면
`information_schema.columns`의 `columns` 행과 `pg_extension`의 `extensions` 배열을 가진 JSON
객체로 실시간 스키마를 내보내고 계약 fixture와 diff한다.

## 검증

승격 전 필수 검증은 다음과 같다.

- `cargo test --workspace --all-features`
- `python -m py_compile infra/lakehouse/spark/jobs/*.py`
- the CI `postgres-integration` DB schema drift gate when a DB stack is part of the change
- target-specific ignored integration tests when the local database stack is part of the change

## 롤백

백필 뒤 검증이 실패하면:

1. 즉시 승격을 중지한다.
2. 민감한 데이터가 아니라면 감사용으로 실패 산출물을 보존한다.
3. 소비자를 이전의 검증된 포인터 또는 Iceberg 스냅샷으로 되돌린다.
4. 실패한 source snapshot ID·대상 snapshot ID·행 수·실패 품질 지표를 기록한다.
5. 정본 Iceberg 스냅샷이 이미 승격됐다면 `iceberg-snapshot-rollback.md`를 사용한다.
