---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-28
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

## 운영 스키마와의 드리프트 확인 (루트 ADR-0259)

CI 는 `docs/db/catalog-schema-contract.v1.example.json` 을 실시간 스키마와 비교하지 않는다(그 fixture 를 읽는
것은 `postgis_mirror_dlq_cutover_evidence.rs` 의 컷오버 증거뿐이다). 드리프트는 운영 표 모양 원장 두 개로 막는다.

| 무엇 | 원장 | PR 에서 거는 곳 |
| --- | --- | --- |
| Iceberg 표의 열·타입·행 유무·옛 데이터 파일 열 집합 | `infra/lakehouse/contracts/lakehouse-production-shape.json` | `infra/lakehouse/spark/tests/test_lakehouse_production_shape.py`(배포의 `lakehouse_schema_migrate.py` `plan_table` 을 원장 위에서 돌린다), `foundation-publisher-core` 의 `lakehouse_snapshot_scan/production_shape_tests.rs`(옛 모양 Parquet 을 현재 계약으로 읽는다) |
| Postgres 에서 행이 있는 표 | `config/postgres-production-shape.json` | `.github/workflows/foundation-ci.yml` `postgres-integration` 의 `cargo xtask ci postgres-production-shape` — 새 마이그레이션을 main 스키마+합성 행 위에 적용 |

원장은 손으로 고치지 않는다. 운영에서 캡처 스크립트로만 다시 만든다(`scripts/ops/lakehouse-production-shape.py`,
`scripts/ops/postgres-production-shape.sh`). 배포 직후와 매일 `scripts/ops/shape-ledger-check.py` 작업이 둘을 다시
캡처해 커밋된 원장과 비교하고, 다르면 원장 갱신 PR 이 준비된다. 운영 DB 가 배포된 마이그레이션까지 왔는지는
서버에서 `sudo /opt/foundation-platform/current/scripts/deploy/foundation-release.sh verify`
(`assert-runtime-migrations.sh`)로 본다. 로컬 마이그레이션 상태는 `platforms/foundation-platform` 에서
`sqlx migrate info` 로 볼 수 있다(기본 `migrations/`).

## 검증

승격 전 필수 검증은 다음과 같다.

- `cargo test --workspace --all-features`
- `python -m py_compile infra/lakehouse/spark/jobs/*.py`
- 운영 표 모양 원장 관문(위 표: 레이크하우스 테스트 두 개, Postgres 는 CI `postgres-integration` 의 production-shape 단계)
- target-specific ignored integration tests when the local database stack is part of the change

## 롤백

백필 뒤 검증이 실패하면:

1. 즉시 승격을 중지한다.
2. 민감한 데이터가 아니라면 감사용으로 실패 산출물을 보존한다.
3. 소비자를 이전의 검증된 포인터 또는 Iceberg 스냅샷으로 되돌린다.
4. 실패한 source snapshot ID·대상 snapshot ID·행 수·실패 품질 지표를 기록한다.
5. 정본 Iceberg 스냅샷이 이미 승격됐다면 `iceberg-snapshot-rollback.md`를 사용한다.
