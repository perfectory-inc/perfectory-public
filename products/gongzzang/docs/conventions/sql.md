---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# SQL 컨벤션 (PostgreSQL + PostGIS)

## 1. 도구

- **DB**: PostgreSQL + PostGIS (이미지 핀 정본: `tools/technology-versions.contract.json`). pgvector 는 미도입.
- **마이그레이션**: sqlx migrate (`migrations/`, 추가 절차는 루트 `add-migration` 스킬)
- **lint·format**: 자동 SQL lint 없음. CI 는 전체 마이그레이션 적용 + 스키마 불변식 시험
  (`tests/migrations/test_v001_full.sh`, `.github/workflows/gongzzang-db-migrations.yml`)
- **타입 검증**: SQLx compile-time (Rust, `gongzzang-sqlx-prepare.yml`)

## 2. 네이밍

- 테이블: `snake_case` 단수 (`listing`, NOT `listings`)
- 컬럼: `snake_case` (`created_at`, `owner_id`)
- 인덱스: `<table>_<column 또는 목적>_idx`, B-Tree 외 종류는 종류를 넣는다
  (`listing_pnu_idx`, `listing_owner_idx`, `search_history_user_time_brin_idx`)
- 제약·FK·시퀀스: 이름 있는 제약은 `<table>_<column>_<rule>` (`parcel_marker_anchor_srid_chk`), 그 밖에는
  PostgreSQL 기본 이름(`<table>_<column>_check`, `<table>_<column>_fkey`, `<table>_<column>_seq`)

## 3. 키워드 케이스

- 키워드: lowercase (`select`, `from`, `where`, `join`)
- 함수: lowercase (`now()`, `coalesce()`, `st_distance()`)
- 식별자: 따옴표 없이 (예약어 충돌 시만 `"order"`)

## 4. 좌표계 (SRID) 강제

- 모든 geometry 컬럼에 SRID 명시: `geometry(Polygon, 4326)`
- 저장: 항상 4326 (WGS84)
- 연산: 5179 (UTM-K) 변환 후
- 타일: 3857 (Web Mercator)

```sql
-- migrations/20260719000115_parcel_marker_anchor_projection.sql 형태
-- ✅ SRID 명시
anchor_point geometry(Point, 4326) not null,
-- ❌ SRID 없음
-- anchor_point geometry not null,
```

CI 마이그레이션 시험이 공간 표별 SRID 제약 존재를 확인한다(`tests/migrations/test_v001_full.sh`). 필지
geometry 정본은 Foundation Platform 이 소유하므로 Gongzzang 에 `parcel` 표를 만들지 않는다.

## 5. 인덱스 정책

| 종류 | 용도 |
|------|------|
| **B-Tree** | 일반 쿼리, FK, ORDER BY |
| **GIST** | PostGIS 공간 데이터 |
| **GIN** | JSONB, 배열, 전문 검색 |
| **BRIN** | 시계열 (audit_log, real_transaction) |
| **HNSW** | pgvector 임베딩 (미도입) |

## 6. 마이그레이션 안전성

- NOT NULL 추가 → DEFAULT 동반 + 별도 단계로 분리:
  1. NULL 허용 컬럼 추가
  2. 백필 (`update ... set ... where ... is null`)
  3. NOT NULL 제약 추가
- 큰 테이블 ALTER → 잠금 시간 검토 후 단계 분리
- 인덱스 생성 → `concurrently`
- 컬럼 삭제 → 사용 코드 제거 후 별도 PR (3단계)

## 7. 트랜잭션 정책

- Repository 메서드 = 한 트랜잭션
- Cross-aggregate 쓰기 = 도메인 이벤트 + Outbox 패턴 (직접 X)
- 격리 수준: 기본 READ COMMITTED. 동시성 핫스팟은 SERIALIZABLE + 재시도

## 8. Optimistic Locking

```sql
create table listing (
    id char(30) primary key,
    -- 동시 수정 차단
    version bigint not null default 1,
    -- ...
);

-- update 시 version 검사 + 증가
update listing
set price_krw = $1, version = version + 1
where id = $2 and version = $3;
-- 영향 받은 row 0 = conflict
```

## 9. Soft Delete

- soft delete 대상 표: `deleted_at TIMESTAMPTZ NULL` 컬럼
- 쿼리: 항상 `where deleted_at is null` (또는 별도 view `_active`)
- audit_log: hard delete 불가, 무한 보존

## 10. 감사 컬럼

현행 도메인 표(예: `listing`)의 공통 컬럼은 다음이다(`migrations/20260719000102_core_tables.sql`).

```sql
created_at timestamptz not null default now(),
updated_at timestamptz not null default now(),
version bigint not null default 1
```

- `deleted_at`은 soft delete 를 쓰는 표에만 둔다(현재 `"user"`).
- 행위자 기록은 `created_by`/`updated_by` 컬럼이 아니라 `audit_log`가 맡는다(소유자는 `owner_id`).

## 11. Gongzzang-owned external response archive

Catalog source raw lineage는 Foundation Platform 소유다. Gongzzang은 parcel·building·industrial
complex·manufacturer·public/reference spatial layer·Catalog API drift monitoring을 위한 raw
table을 추가하지 않는다.

ADR로 승인한 미래 Gongzzang 소유 external adapter archive table은 명시적인 owner-specific
name을 사용한다.

```sql
create table listing_external_data (
    listing_id char(30) primary key references listing(id),
    source varchar(40) not null,  -- e.g. 'korean_law', 'nice_identity'
    raw_response jsonb not null,
    fetched_at timestamptz not null,
    expires_at timestamptz not null
);

create index on listing_external_data using gin (raw_response);
```

Raw retention = audit, replay, and dispute evidence for the owning Gongzzang
adapter only.

## 12. 금지

- ❌ SRID 없는 geometry 컬럼
- ❌ `select *` (필요한 컬럼만 명시)
- ❌ `varchar` 길이 무제한 (`text`로 명확)
- ❌ Cross-aggregate transaction (도메인 이벤트로 우회)
- ❌ Hard delete (audit log 외)
- ❌ `st_distance` 단독 사용 (성능 — `st_dwithin` + GIST)
