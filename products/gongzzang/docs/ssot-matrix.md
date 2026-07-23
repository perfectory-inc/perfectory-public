# SSOT 매트릭스

> 정보별 진짜 SSOT(Single Source of Truth)와 그것의 사본, 그리고 SSOT 위반을 자동 차단하는 룰.

---

## 1. SSOT 매트릭스 표

| 정보 종류 | 진짜 SSOT | 사본 (재구성 가능) | 위반 자동 차단 |
|---------|---------|---------|---------|
| **사용자 데이터** | PostgreSQL `user` 테이블 | Redis 세션, 검색 인덱스 | DB 외 직접 변경 금지 (linter) |
| **Catalog 공공 API raw 응답** | Foundation Platform object lake / lineage store | Gongzzang legacy migration ledger only | `foundation-ownership-boundary.sh` + boundary contract |
| **Gongzzang-owned 외부 API raw 응답** | 소유 서비스별 archive/lineage contract | Redis 캐시, 분석 마트 | 소유권 ADR + boundary gate |
| **비즈니스 규칙** | Gongzzang-owned `crates/*-domain` Rust 코드 | 문서, 테스트 (둘 다 코드 따라옴) | 도메인 외부 비즈니스 로직 = clippy lint |
| **API 계약** | Rust 코드 + utoipa 매크로 | `openapi.json` (자동), TS 타입 (자동, `packages/api-types`) | 생성물 재생성-diff (gongzzang-ci `Traffic/auth policy drift` job; dependency-cruiser는 미도입) |
| **DB 스키마** | `migrations/*.sql` (`YYYYMMDDHHMMSS_<snake_case>.sql`) | Rust 타입 + `.sqlx/` prepare metadata | 수동 ALTER TABLE 금지 + Foundation Platform legacy schema ledger |
| **Gongzzang DB migration smoke** | 루트 `.github/workflows/gongzzang-db-migrations.yml` + `tests/migrations/test_v001_full.sh` | disposable PostGIS verification output | `foundation-ownership-boundary.sh` + boundary contract |
| **인프라 설정** | Pulumi TypeScript 코드 | AWS 콘솔 (절대 수동 변경 금지) | Pulumi `refresh` drift 감지 → 알림 |
| **시크릿** | AWS Secrets Manager / Vault | `.env.example`은 placeholder만 | gitleaks |
| **도메인 용어** | `docs/glossary.md` | 모든 코드/UI/문서 사용 | grep CI 룰 |
| **도구 버전** | `rust-toolchain.toml` + `package.json#packageManager` | CI/Docker가 *읽기*만 | 직접 install 명령 차단 |
| **의존성 버전** | `Cargo.lock` + `pnpm-lock.yaml` | 보조 환경이 *그대로* 사용 | 수동 install 금지 |
| **시간** | DB는 UTC TIMESTAMPTZ | 응답/UI에서만 KST 변환 | 타입 시스템 (timezone-aware) |
| **좌표** | DB는 EPSG:4326 | 5179(연산), 3857(타일) | SRID 미지정 차단 |
| **사용자 권한** | Zitadel + DB `user_role` | 클라이언트 캐시 | JWT scope 검증 |
| **에러 코드** | `services/gongzzang-api/src/http/problem.rs` (RFC 9457) | OpenAPI spec, TS 타입 (`packages/api-types` 생성물) | enum exhaustive match |
| **컨벤션** | `docs/conventions/*.md` | 도구 설정 (biome.json, clippy.toml) | 도구가 자동 강제 |
| **Foundation Platform/Gongzzang 경계** | `docs/architecture/foundation-platform-boundary.v1.json` | ADR 0034, AGENTS.md 요약 | boundary CI gates |
| **Foundation Platform canonical Catalog tables** | Foundation Platform database | Gongzzang product references only | `forbidden_canonical_catalog_tables` + boundary CI gates |
| **Foundation Platform database connection** | Foundation Platform service runtime only | Gongzzang has no DB connection copy | direct DB reference regex + boundary CI gates |
| **Foundation Platform HTTP env contract** | `.env.example` + `docs/architecture/foundation-platform-boundary.v1.json` | runtime env values | root env example guard |
| **Foundation Platform service auth contract** | `docs/architecture/foundation-platform-boundary.v1.json` + `FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE` / `FOUNDATION_PLATFORM_WEBHOOK_SECRET` runtime secrets | `.env.example` placeholders only | boundary guard + env schema + focused auth tests |
| **Platform Integration Policy** | `docs/architecture/platform-integration/index.v1.json` + folder policies | traffic-auth registry, Foundation Platform boundary, runtime code, CI gates | platform-integration policy contract |
| **Gongzzang local Postgres port** | `infrastructure/docker/docker-compose.yml` + `infrastructure/docker/.env.example` | local `.env` `DATABASE_URL` | local Postgres port contract in boundary contract |
| **Foundation Platform Catalog API consumer contract** | `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json` | `../../platforms/foundation-platform/docs/openapi/catalog.v1.json` | generated provider contract + immutable consumer snapshot + SHA-256 pin |
| **Foundation Platform Catalog HTTP adapter placement** | `crates/foundation-platform-client + service-owned adapters` | `parcel-lookup` exposes port/projection only | `foundation-ownership-boundary.sh` + boundary contract |
| **Foundation Platform public/reference vector tile lifecycle** | Foundation Platform Catalog | Gongzzang has no ETL/tooling copy | `foundation-ownership-boundary.sh` + boundary contract |
| **Foundation Platform Catalog public API drift observability** | Foundation Platform observability pipeline | Gongzzang legacy migration ledger only | `foundation-ownership-boundary.sh` + boundary contract |
| **Foundation Platform Catalog workflow ownership** | Foundation Platform GitHub workflows / scheduler | Gongzzang has no Catalog source refresh workflow copy | workflow token scan in `foundation-ownership-boundary.sh` |
| **결정 이력** | `docs/adr/NNNN-*.md` | (다른 곳 인용은 링크) | 새 결정은 코드 작성 *전* ADR 필수 |
| **운영 증빙·작업 메모** | 비공개 운영 저장소 또는 외부 증빙 저장소 | 공개 코드 트리에는 두지 않음 | 루트 public-repository-safety guard |
| **SSS 헌법** | `docs/sss-charter.md` | (다른 곳 인용은 링크) | (헌법 자체) |
| **글로서리** | `docs/glossary.md` | (모든 코드/UI/문서) | grep CI |

---

## 2. 문서 SSOT (도메인 폴더 단위)

```
docs/
├── sss-charter.md          ← SSS 정의 SSOT
├── glossary.md             ← 도메인 용어 SSOT
├── ssot-matrix.md          ← 이 문서 (메타 SSOT)
│
├── adr/                    ← 모든 결정 이력 SSOT
├── conventions/            ← 코드 스타일 SSOT
├── data-sources/           ← 외부 API 카탈로그 SSOT
│
├── architecture/           ← 시스템 구조/관측성/캐싱 SSOT
├── auth/                   ← 인증/권한 SSOT
├── database/               ← DB/PostGIS/마이그레이션 SSOT
├── backend/                ← Rust 백엔드 SSOT
├── testing/                ← 테스트 전략 SSOT
├── frontend/               ← Next.js/UI SSOT
├── runbooks/               ← 운영 절차 SSOT
├── governance/             ← 거버넌스/문서 SSOT
├── compliance/             ← 컴플라이언스 SSOT
└── cost/                   ← 비용 추정 SSOT
```

각 폴더 = 한 도메인의 SSOT. 다른 폴더에서 같은 정보 작성 = SSOT 위반.
폴더 없는 도메인의 실질 SSOT: 인프라(IaC) = `infrastructure/README.md`(영역 루트),
보안·프라이버시 = `docs/sss-charter.md` §B-3, 캐시/메시징·API 방향 = ADR-0006/0007/0046/0047.

---

## 3. 코드 SSOT (모노레포 워크스페이스)

```
crates/
├── <capability>-domain/     ← Gongzzang-owned 비즈니스 규칙 SSOT (listing-domain, user-domain 등 capability-first)
├── shared-kernel/           ← 공유 값 객체 SSOT (Pnu, Money, Geometry 등)
├── gongzzang-persistence/   ← Repository 구현 (도메인 trait 위임)
└── (foundation-platform-client, circuit-breaker, parcel-lookup, gongzzang-outbox, repo-guard 등 기술 crate)

services/                    ← 실행 프로세스 (gongzzang-api, gongzzang-outbox-publisher)
apps/                        ← UI (Next.js — 실물은 apps/web)
packages/                    ← TS 라이브러리 (ui, api-types 생성물 등)
```

> API 계약 + 에러 코드 SSOT 는 별도 crate 가 아니라 `services/gongzzang-api` 안에 있다
> (utoipa 매크로 + `src/http/problem.rs`). `crates/api-types`·`crates/data-clients` 는
> README-only 자리표시자로, 워크스페이스 멤버가 아니다 (capability-layout 가드가 강제).

---

## 4. 위반 자동 차단 룰 (10개)

각 룰은 *어디서* 강제되는지 명시. 사람이 지키는 게 아님.

| # | 위반 | 차단 도구 | 단계 |
|---|------|---------|------|
| 1 | 생성 아티팩트 수동 편집 (traffic/auth policy 6종) | gongzzang-ci `Traffic/auth policy drift` job — 재생성 후 `git diff --exit-code` (OpenAPI 전체 재생성-diff 는 미도입) | CI |
| 2 | 수동 작성 TS 타입 (백엔드 응답용) | 생성물만 커밋 (`packages/api-types`); dependency-cruiser 는 **미도입** — 수동 준수 | CI(부분) |
| 3 | AWS 콘솔 수동 변경 | Pulumi `refresh` drift 감지 → CI 알림 | CI 정기 |
| 4 | DB 스키마 수동 변경 | sqlx migrate + `.sqlx` prepare 검증 (루트 `gongzzang-db-migrations.yml`·`gongzzang-sqlx-prepare.yml`; flyway 는 미사용) | CI |
| 5 | 시크릿 git 커밋 | gitleaks pre-commit + CI | pre-commit + CI |
| 6 | 코드 스타일 위반 | rustfmt + Biome (루트 lefthook) | pre-commit |
| 7 | 의존성 방향 위반 | (Rust) repo-guard `capability-layout` + `foundation-ownership-boundary.sh`; (TS) 도구 **미도입** — 수동 준수 | pre-push + CI |
| 8 | 파일 ≤500 / 1500 위반 | `scripts/lefthook/file-line-limit.sh` (gongzzang-ci Repo guardrails job) | CI |
| 9 | 글로서리 외 도메인 용어 | **미도입** — 자동 grep 룰 없음, 리뷰에서 수동 준수 | (수동) |
| 10 | TODO/HACK/XXX/`_TEMP` 코드 | clippy `todo` deny + Biome 자체 룰 | pre-commit + CI |
| 11 | Foundation Platform 소유 Catalog/ETL/raw crate 재도입 | `foundation-ownership-boundary.sh` + boundary contract | pre-push + CI |
| 12 | Foundation Platform legacy schema token 신규 사용 | `allowed_legacy_schema_tokens` ledger | pre-push + CI |
| 13 | Foundation Platform Catalog API consumer drift | catalog API consumer pin contract | pre-push + CI |
| 14 | Foundation Platform vector tile ETL/tooling/workflow 재도입 | `foundation-ownership-boundary.sh` + boundary contract | pre-push + CI |
| 15 | Foundation Platform Catalog API drift observability 재도입 | `foundation-ownership-boundary.sh` + boundary contract | pre-push + CI |

---

## 5. 새 정보 추가 시 (워크플로우)

새 종류의 정보가 생기면:

1. **이 문서(§ 1 표)에 추가** — SSOT 위치, 사본, 차단 룰
2. **차단 룰 부재 시 룰 추가** — lefthook / CI / linter
3. **ADR 작성** (큰 결정의 경우)
4. **PR로 검토 + 승인 후 머지**

→ "정보가 두 곳에 있는데 어디가 진짜?"라는 질문이 발생하기 *전에* 표에 박힘.

---

## 6. 자체 검증

분기별로 다음 5 질문 자체 점검:

1. □ 같은 정보가 두 곳에 있으면 즉시 어느 게 SSOT인지 답 가능?
2. □ DB와 도메인 코드가 충돌하면? → **컴파일 실패 (sqlx)**
3. □ Rust 응답과 TS 타입이 충돌하면? → **TS 컴파일 실패 (자동 생성)**
4. □ AWS 콘솔에 직접 만든 리소스가 있는가? → **0개여야 함 (Pulumi)**
5. □ 같은 도메인 용어를 다르게 부르는 곳이 있는가? → **0개 (glossary 자동 검증)**

→ 5/5 = SSOT 합격. 그 외는 즉시 차단 룰 추가.

---

## 7. 안티패턴 (피해야 할 SSOT 위반)

| 안티패턴 | 사례 | 해결 |
|---------|------|------|
| **거대 단일 SSOT 파일** | docs/schema.md 1349줄, docs/site-builder.md 1447줄 | 폴더로 분해 (`docs/schema/auth.md`, `docs/schema/parcel.md`...) |
| **TS 타입 수동 + Rust 변경 따라가기** | v2의 `ALLOWED_FOR_FRONTEND_TEMP` | OpenAPI 자동 생성 |
| **AWS 콘솔에서 *살짝* 수정** | "한 번만 빠르게" | Pulumi 코드만 |
| **두 곳에 같은 도메인 용어** | "매물" vs "물건", "Property" vs "Listing" | glossary 강제 |
| **README에 정보 vs 코드 주석에 정보** | 코드 변경 후 README 까먹음 | 코드가 SSOT, README는 *링크만* |
| **시크릿 .env에 + 1Password에 둘 다** | 동기화 실패 | AWS Secrets Manager / Vault만 |
