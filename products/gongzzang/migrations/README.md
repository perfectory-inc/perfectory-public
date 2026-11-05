---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# migrations/

## 개요

이 디렉토리는 SQLx 마이그레이션의 단일 출처(SSOT)예요. PostgreSQL 스키마의 모든
변경은 여기에 *forward-only* SQL 파일로 누적돼요.

## 명명 규칙

`YYYYMMDDHHMMSS_<snake_case>.sql` 형식이에요 — 14자리 UTC 타임스탬프 버전에
snake_case 의도 이름을 붙여요. 모노레포 공통 규칙은
[ADR-0001 §7](../../../docs/adr/0001-monorepo-governance-and-conventions.md)이
정의하고, `scripts/guard/migration-naming.sh`와 repo-guard
`migration-version-prefixes`가 강제해요. 추가 절차 정본은 `add-migration` skill
(`.claude/skills/add-migration/SKILL.md`)이에요.

새 마이그레이션은 `sqlx migrate add --timestamp <snake_case_이름>`으로 만들어요. `--timestamp`를
빼면 sqlx가 마지막 두 버전 차이가 1인 지금 상태를 보고 순차 번호(`last+1`)를 낼 수 있어요.

예시:

- `20260719000101_enable_postgis.sql`
- `20260719000102_core_tables.sql`
- `20260719000106_db_roles.sql`

규칙:

- **타임스탬프(UTC 14자리)** — sqlx-cli가 첫 번째 `_` 앞을 i64로 파싱해
  적용 순서를 정해요. 정수만 허용돼요
- **파일 크기** — repo-guard `file-line-limit`(500줄 경고, 1500줄 실패) 안에서 작게 유지해요
- **snake_case 이름** — *변경 의도*를 표현해요 (`add_listing_index`,
  `drop_legacy_column` 등). 테이블 이름이 아니라 "이 PR이 무엇을 하는가"를 적어요

## 적용 순서

SQLx는 정수 버전 오름차순으로 적용해요
(`20260719000101 < 20260719000102 < 20260719000106`).
새 마이그레이션은 항상 *마지막* 버전 다음에 추가하세요 — `sqlx migrate add --timestamp`가
현재 UTC 시각으로 이를 보장해요.

## 롤백 정책 — Forward-only

운영에서는 **절대** 과거 마이그레이션 SQL을 수정하지 않아요. 한 번 머지된
파일은 immutable이에요.

실수를 정정하려면 *새* 마이그레이션을 추가해 되돌려요
(예: `sqlx migrate add --timestamp revert_x` → `20260801120000_revert_x.sql`).

로컬 개발에서는 다음 한 줄로 DB를 처음부터 재구성할 수 있어요:

```bash
sqlx database drop -y && sqlx database create && sqlx migrate run --source migrations
```

## 로컬 검증

`products/gongzzang/`에서 한 줄이면 끝이에요(`DATABASE_URL`은 환경변수나 `.env`에서 읽어요):

```bash
bash scripts/sqlx-migrate.sh
```

사전 조건:

- Docker Compose 기동 (`infrastructure/docker/`)
- sqlx-cli 설치 — CI와 같은 버전·feature로 설치해요. 정본은 루트
  `.github/workflows/gongzzang-db-migrations.yml`의 "Install sqlx-cli + postgres client" 단계예요.

## CI 검증

루트 `.github/workflows/gongzzang-db-migrations.yml`(`required/gongzzang-migrations`)이 PR마다 돌아요.
PostGIS 컨테이너에 전체 마이그레이션을 적용하고 `tests/migrations/test_v001_full.sh`(테이블·인덱스 수 등
스키마 불변식), `tests/migrations/test_v002_audit_immutable.sh`, `cargo xtask integration gongzzang postgres`
(persistence 통합 시험)를 실행해요. 실패하면 머지가 차단돼요.

## 블루-그린 호환 변경 패턴 (DDL 안전성)

DDL은 **별도 PR**로 분리하세요. 코드 변경과 같이 묶으면 롤백 단위가 깨져요.

- **새 컬럼 추가**: NULL 허용으로 추가 → 백필 → NOT NULL 변환 (3-step)
- **컬럼 제거**: 코드에서 미참조 확인 → 1주 대기 → `DROP COLUMN`
- **인덱스 추가**: 운영에서는 `CREATE INDEX CONCURRENTLY`로 lock을 회피해요.
  단, sqlx는 마이그레이션을 트랜잭션으로 감싸기 때문에 `CONCURRENTLY`는
  *별도 파일*에 넣고 첫 줄에 `-- no-transaction` 마커를 붙여 트랜잭션을 꺼요

이 패턴을 지키면 두 버전의 앱이 동시에 같은 DB를 바라봐도 깨지지 않아요.

## 마이그레이션 실패 복구

마이그레이션이 중간에 실패하면 `_sqlx_migrations` 테이블이 부분 적용 상태를 기록해요.

```bash
sqlx migrate info --source migrations    # 적용 상태 확인
```

복구 절차:

- **로컬**: `sqlx database drop -y && sqlx database create && sqlx migrate run --source migrations`
  (위 *로컬 개발 재구성* 한 줄과 동일)
- **운영**: 절대 손으로 `_sqlx_migrations`을 건드리지 마세요. 새 *fix-forward*
  마이그레이션(`YYYYMMDDHHMMSS_fix_<원인>.sql`)을 PR로 올려서 진행하세요

## 참고 링크

- SQLx migrations:
  <https://docs.rs/sqlx/latest/sqlx/migrate/struct.Migrator.html>
- SQLx CLI: <https://github.com/launchbadge/sqlx/tree/main/sqlx-cli>
