---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# SSOT 매트릭스

> 정보별 진짜 SSOT(Single Source of Truth)와 그것의 사본, 그리고 그것을 지키는 검사.
> 검사가 없는 칸은 **"자동 검사 없음"**으로 적는다 — 없는 검사를 있다고 적지 않는다.

---

## 1. SSOT 매트릭스 표

| 정보 종류 | 진짜 SSOT | 사본 (재구성 가능) | 지키는 검사 |
|---------|---------|---------|---------|
| **사용자 데이터** | PostgreSQL `"user"` 표 | Valkey 세션 | 자동 검사 없음 |
| **Catalog 공공 API raw 응답** | Foundation Platform object lake / lineage store | Gongzzang legacy migration ledger only | `gongzzang-catalog-boundary.yml` (Catalog crate 재도입·Catalog 표 직접 쓰기 차단) |
| **Gongzzang-owned 외부 API raw 응답** | 소유 서비스별 archive/lineage contract | Valkey 캐시, 분석 마트 | 소유권 ADR (자동 검사 없음) |
| **비즈니스 규칙** | Gongzzang-owned `crates/*-domain` Rust 코드 | 문서, 테스트 (둘 다 코드 따라옴) | repo-guard `capability-layout` (도메인 crate 배치·소유 선언). 도메인 밖 비즈니스 로직 검사는 없음 |
| **API 계약** | Rust 코드 + utoipa (`services/gongzzang-api/src/bin/export-gongzzang-openapi.rs`) | `services/gongzzang-api/openapi.json`, TS 타입 (`packages/api-types/generated/`) | 재생성-diff CI 미도입 |
| **DB 스키마** | `migrations/*.sql` (`YYYYMMDDHHMMSS_<snake_case>.sql`) | Rust 타입 | `gongzzang-sqlx-prepare.yml` (`cargo sqlx prepare --workspace --check`) + repo-guard `migration-version-prefixes` |
| **Gongzzang DB migration smoke** | 루트 `.github/workflows/gongzzang-db-migrations.yml` + `tests/migrations/test_v001_full.sh` | disposable PostGIS verification output | 그 워크플로 자체 |
| **인프라 설정** | Pulumi TypeScript 코드 ([infrastructure/README.md](../infrastructure/README.md)) | AWS 콘솔 (수동 변경 금지) | 자동 drift 검사 없음 (`pulumi refresh` CI 없음) |
| **시크릿** | 운영: AWS 관리 secret·Pulumi state (`infrastructure-v2/index.ts`) | `.env.example`은 placeholder만 | gitleaks (pre-commit + 루트 `secret-scan.yml`) |
| **도메인 용어** | [glossary.md](./glossary.md) | 모든 코드/UI/문서 사용 | 자동 검사 없음 |
| **도구 버전** | 루트 `tools/technology-versions.contract.json` (`rust-toolchain.toml`·`package.json#packageManager`가 그 값을 따른다) | CI/Docker가 *읽기*만 | 루트 `technology-version-consistency` 가드 |
| **의존성 버전** | `Cargo.lock` + `pnpm-lock.yaml` | 보조 환경이 *그대로* 사용 | CI `pnpm install --frozen-lockfile` |
| **시간** | DB는 UTC `timestamptz` | 응답/UI에서만 KST 변환 | 컬럼 타입 |
| **좌표** | DB는 EPSG:4326 | — | `geometry(Point, 4326)` 컬럼 타입 + `ST_SRID` CHECK (마이그레이션) |
| **사용자 권한** | Zitadel + DB `"user".roles` | 클라이언트 캐시 | JWT 검증 (JWKS, `services/gongzzang-api/src/startup.rs`) |
| **에러 형식** | `services/gongzzang-api/src/http/problem.rs` ([에러 형식](./conventions/error-format.md)) | `apps/web/lib/http/problem.ts` (손 사본) | 자동 대조 없음 |
| **컨벤션** | `docs/conventions/*.md` | 도구 설정 (biome.json, clippy.toml) | 도구가 자동 강제 |
| **Foundation Platform/Gongzzang 경계** | `docs/architecture/foundation-platform-boundary.v1.json` | ADR 0034, AGENTS.md 요약 | repo-guard `capability-layout` (도메인 crate 소유 선언만 읽음) + `gongzzang-catalog-boundary.yml` |
| **Foundation Platform canonical Catalog tables** | Foundation Platform database | Gongzzang product references only | `gongzzang-catalog-boundary.yml` 의 Catalog 표 정규식. 계약의 `forbidden_canonical_catalog_tables` 목록은 읽는 코드가 없다 |
| **Foundation Platform database connection** | Foundation Platform service runtime only | Gongzzang has no DB connection copy | 자동 검사 없음 |
| **Foundation Platform HTTP env contract** | `.env.example` + `docs/architecture/foundation-platform-boundary.v1.json` | runtime env values | 루트 `environment-variable-naming` 가드 (env 이름) |
| **Foundation Platform service auth contract** | `docs/architecture/foundation-platform-boundary.v1.json` + `FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE` / `FOUNDATION_PLATFORM_WEBHOOK_SECRET` runtime secrets | `.env.example` placeholders only | webhook secret: `apps/web/lib/env.ts` 스키마 + `apps/web/tests/integration/foundation-platform-events-handler.test.ts`; workload token 파일: 읽는 쪽은 `crates/foundation-platform-client`·서비스 `startup`/`main` (전용 검사 없음) |
| **Platform Integration Policy** | `docs/architecture/platform-integration/index.v1.json` + folder policies | traffic-auth registry, Foundation Platform boundary, runtime code | 자동 검사 없음 (읽는 코드 없음) |
| **Gongzzang local Postgres port** | `infrastructure/docker/docker-compose.yml` + `infrastructure/docker/.env.example` | local `.env` `DATABASE_URL` | 자동 검사 없음 |
| **Foundation Platform Catalog API consumer contract** | `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json` | `../../platforms/foundation-platform/docs/openapi/catalog.v1.json` | `crates/foundation-platform-client/tests/catalog_contract_pin.rs` + `apps/web/tests/unit/map/foundation-catalog-contract.test.ts` |
| **Foundation Platform Catalog HTTP adapter placement** | `crates/foundation-platform-client` + service-owned adapters | `parcel-lookup` exposes port/projection only | 자동 검사 없음 (리뷰) |
| **Foundation Platform public/reference vector tile lifecycle** | Foundation Platform Catalog | Gongzzang has no ETL/tooling copy | 경계 계약 `path_ownership` 에 기록만 (읽는 검사 없음) |
| **Foundation Platform Catalog public API drift observability** | Foundation Platform observability pipeline | Gongzzang legacy migration ledger only | 경계 계약 `path_ownership` 에 기록만 (읽는 검사 없음) |
| **Foundation Platform Catalog workflow ownership** | Foundation Platform GitHub workflows / scheduler | Gongzzang has no Catalog source refresh workflow copy | 자동 검사 없음 |
| **결정 이력** | `docs/adr/NNNN-*.md` | (다른 곳 인용은 링크) | 새 결정은 코드 작성 *전* ADR 필수 |
| **운영 증빙·작업 메모** | 비공개 운영 저장소 또는 외부 증빙 저장소 | 공개 코드 트리에는 두지 않음 | 루트 `public-repository-safety` 가드 |
| **SSS 헌법** | [sss-charter.md](./sss-charter.md) | (다른 곳 인용은 링크) | (헌법 자체) |

---

## 2. 문서 SSOT (도메인 폴더 단위)

폴더 목록과 각 폴더의 책임은 [docs/README.md](./README.md)가 소유한다. 각 폴더 = 한 도메인의 SSOT.
다른 폴더에서 같은 정보 작성 = SSOT 위반. 폴더 없는 도메인의 실질 SSOT도 그 문서가 적는다.

---

## 3. 코드 SSOT (모노레포 워크스페이스)

```
crates/
├── <capability>-domain/     ← Gongzzang-owned 비즈니스 규칙 SSOT (listing-domain, user-domain 등 capability-first)
├── shared-kernel/           ← 공유 값 객체 SSOT
├── gongzzang-persistence/   ← Repository 구현 (도메인 trait 위임)
└── (foundation-platform-client, circuit-breaker, parcel-lookup, gongzzang-outbox, repo-guard 등 기술 crate)

services/                    ← 실행 프로세스 (gongzzang-api, gongzzang-outbox-publisher)
apps/                        ← UI (Next.js — 실물은 apps/web)
packages/                    ← TS 라이브러리 (ui, api-types 생성물)
```

> API 계약 + 에러 형식 SSOT 는 별도 crate 가 아니라 `services/gongzzang-api` 안에 있다
> (utoipa + `src/http/problem.rs`). `crates/data-clients` 는
> README-only 자리표시자로, 워크스페이스 멤버가 아니다 (capability-layout 가드가 강제).

---

## 4. 위반 차단 룰

각 룰은 *어디서* 강제되는지 명시한다. 로컬 훅은 빠른 사전 거름이고 판정은 CI다(루트 `lefthook.yml`).

| # | 위반 | 차단 도구 | 단계 |
|---|------|---------|------|
| 1 | 생성 아티팩트 수동 편집 (traffic/auth policy 산출물) | gongzzang-ci `Traffic/auth policy drift` job — 재생성 후 `git diff --exit-code` (OpenAPI 재생성-diff 는 미도입) | CI |
| 2 | 수동 작성 TS 타입 (백엔드 응답용) | 생성물만 커밋 (`packages/api-types`); 검사 도구는 **미도입** — 수동 준수 | (수동) |
| 3 | AWS 콘솔 수동 변경 | **미도입** — Pulumi drift 검사 CI 없음 | (수동) |
| 4 | DB 스키마 수동 변경 | sqlx migrate + prepare 검사 (루트 `gongzzang-db-migrations.yml`·`gongzzang-sqlx-prepare.yml`) | CI |
| 5 | 시크릿 git 커밋 | gitleaks (루트 `lefthook.yml` + `secret-scan.yml`) | pre-commit + CI |
| 6 | 코드 스타일 위반 | rustfmt + Biome (루트 lefthook), CI `cargo xtask verify gongzzang`·`gongzzang-frontend.yml` | pre-commit + CI |
| 7 | 의존성 방향 위반 | (Rust) repo-guard `capability-layout` + `gongzzang-catalog-boundary.yml`; (TS) 도구 **미도입** — 수동 준수 | CI |
| 8 | 파일 1500줄 초과 | `cargo run -p repo-guard -- file-line-limit` (gongzzang-ci Repo guardrails job), 루트 `file-line-limit.sh` | pre-push + CI |
| 9 | 글로서리 외 도메인 용어 | **미도입** — 자동 grep 룰 없음, 리뷰에서 수동 준수 | (수동) |
| 10 | TODO/HACK/XXX/TEMP 코드 | `scripts/lefthook/check-forbidden-implementation-markers.sh` + clippy `todo`·`unimplemented` deny | pre-push + CI |
| 11 | Foundation Platform 소유 Catalog crate·Catalog 표 쓰기·Catalog 변경 route 재도입 | `gongzzang-catalog-boundary.yml` | CI |
| 12 | Foundation Platform Catalog API consumer drift | catalog API consumer pin 테스트 (§1) | CI |

Foundation Platform vector tile ETL·Catalog API drift observability·Catalog workflow 재도입과 legacy
schema token(`allowed_legacy_schema_tokens`)은 경계 계약에 기록만 되어 있고 자동 차단이 없다.

---

## 5. 새 정보 추가 시 (워크플로우)

새 종류의 정보가 생기면:

1. **이 문서(§ 1 표)에 추가** — SSOT 위치, 사본, 지키는 검사
2. **검사 부재 시 검사 추가** — CI 먼저, 필요하면 lefthook 에 사본
3. **ADR 작성** (큰 결정의 경우)
4. **PR로 검토 + 머지**

→ "정보가 두 곳에 있는데 어디가 진짜?"라는 질문이 발생하기 *전에* 표에 박힘.

---

## 6. 자체 검증

분기별로 다음 질문을 점검한다. 괄호는 지금 답을 주는 장치다.

1. □ 같은 정보가 두 곳에 있으면 즉시 어느 게 SSOT인지 답 가능? (이 표)
2. □ DB와 도메인 코드가 충돌하면? (`cargo sqlx prepare --check` CI)
3. □ Rust 응답과 TS 타입이 충돌하면? (자동 검사 없음 — 생성물 재생성으로 확인)
4. □ AWS 콘솔에 직접 만든 리소스가 있는가? (자동 검사 없음 — 0개여야 함)
5. □ 같은 도메인 용어를 다르게 부르는 곳이 있는가? (자동 검사 없음 — glossary 대조)

자동 검사가 없는 질문은 검사를 추가할 후보다.

---

## 7. 안티패턴 (피해야 할 SSOT 위반)

| 안티패턴 | 사례 | 해결 |
|---------|------|------|
| **거대 단일 SSOT 파일** | 한 파일에 스키마 전체 | 폴더로 분해 |
| **TS 타입 수동 + Rust 변경 따라가기** | 손으로 쓴 응답 타입 | OpenAPI 자동 생성 |
| **AWS 콘솔에서 *살짝* 수정** | "한 번만 빠르게" | Pulumi 코드만 |
| **두 곳에 같은 도메인 용어** | "매물" vs "물건", "Property" vs "Listing" | glossary |
| **README에 정보 vs 코드 주석에 정보** | 코드 변경 후 README 까먹음 | 코드가 SSOT, README는 *링크만* |
| **시크릿을 두 곳에** | `.env`와 비밀 관리자에 둘 다 | 운영 비밀 관리자 한 곳만 |
