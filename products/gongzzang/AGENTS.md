# AGENTS.md

AI 에이전트(Claude Code / Cursor / Codex / Gemini / Cline / Aider 등) 공용 라우터.

## ✱ 제품 우선 원칙 — 이 문서의 최상위 규칙 (아래 SSS/7기둥/엔터프라이즈 규정에 우선)

> 2026-06-21 추가. 이 프로젝트는 **아직 런칭 전(유저 0)**이다. 아래 SSS/7기둥/엔터프라이즈
> 규정은 "그렇게 *될 수 있게* 설계하라"는 방향이지, "유저도 없는데 process·검사·거버넌스를
> *미리 다 지으라*"는 뜻이 아니다. **충돌하면 이 절이 이긴다.**
> 배경: [ADR 0044](./docs/adr/0044-bazel-transition-reconciliation.md)

1. **제품 우선.** 모든 작업은 "이게 끝나면 *유저가 뭘 할 수 있게 되나?*"에 답해야 한다.
   답이 없으면(순수 process·검사·문서) 기본적으로 하지 않는다.
2. **YAGNI.** 미래에 필요할까봐 미리 만들지 않는다. 실제 문제가 생긴 *뒤에* 추가한다.
3. **검사는 수요가 당길 때만.** 새 guard·CI 검사·레지스트리는 *실제 사고가 났거나 임박했을 때만*
   추가한다. 모든 검사는 **"실패하면 어떤 진짜 버그/사고를 막는가?"**를 한 문장으로 답할 수
   있어야 한다. 못 하면 = ceremony → 만들지 말고, 이미 있으면 삭제한다.
4. **메타 머신 금지.** 자기 자신을 검증하는 자동화(레지스트리/투영/래칫/증거번들/준비게이트)는
   만들지 않는다. **진척은 유저 가시 기능으로 측정한다 — 문서·검사 수가 아니라.**
5. **신규 PowerShell 금지.** 검사 로직은 Rust 또는 표준 도구(gitleaks·cargo-deny 등).
6. **삭제 우선.** 목적을 한 문장으로 설명 못 하는 기존 process·검사는 포팅·유지보다 **삭제**한다.

## 0. SSS 7 기둥 헌법 (요약)

*하이엔드 엔터프라이즈 SSS급 산업용 부동산 정보 플랫폼*. 모든 작업은 7 기둥을 *시스템적으로* 만족:

1. **일관성** — 같은 일은 같은 방식으로. 예외 0
2. **자동 강제** — 규칙은 사람이 아니라 시스템이 차단
3. **추적성** — 모든 변경·요청·결정 재구성 가능
4. **안전성** — 런타임 에러를 컴파일 시점에 차단
5. **가시성** — 서비스 상태 실시간 인지
6. **단일 출처(SSOT)** — 한 정보 = 한 곳에만
7. **명확성** — 컨벤션·네이밍으로 추측 제거

상세: [docs/sss-charter.md](./docs/sss-charter.md) — v2(2계층). 위 7 기둥은 *안쪽(어떻게 짓는가)*,
헌장은 *바깥쪽(유저가 무엇을 받는가)* 5 기둥(데이터 정확성·신뢰성·보안·성능·접근성)을 추가.
단, **✱ 제품 우선 원칙이 SSS보다 우선** — SSS는 미리 짓는 게 아니라 기능마다 벌어들입니다.

## 0.5. Cross-Area 아키텍처 (구 Cross-Repo) (Horizontal Platforms + Product Services)

[ADR 0048](./docs/adr/0048-horizontal-platform-redefinition.md)이 기존 core 중심 수직 구조 표현을 대체.
산단·필지·건물·제조사 마스터 데이터와 lakehouse/collection은 `foundation-platform` 책임,
내부 직원(Staff)·서비스 신원·권한은 `identity-platform` 책임. 물리 repo/path 전환 완료.

| 제품/service slug | 도메인 | 위치 (목표=현재) |
|---|---|---|
| `gongzzang` | B2C 부동산 플랫폼 (`gongzzang.com`) | `products/gongzzang` |
| `foundation-platform` | Catalog + lakehouse + collection + canonical data foundation | `platforms/foundation-platform` |
| `identity-platform` | Staff/service identity + Authz + policy | `platforms/identity-platform` |
| `intelligence-platform` | AI runtime + proposal generation + vector/RAG | `platforms/intelligence-platform` |
| `dawneer` (`Dawneer`/`더니어`) | B2B 산단 관리·사이트 제작 workbench + 단일 staff-facing admin composition surface | 별도 repo — 모노레포 미통합 |

> 2026-07-19부터 위 영역들은 perfectory 모노레포로 통합되었다 (루트 [ADR-0001](../../docs/adr/0001-monorepo-governance-and-conventions.md)). dawneer만 별도 repo로 남아 있다.

- Dawneer는 presentation/workbench state만 소유하고 각 플랫폼과 Gongzzang의 published API를 조합한다.
  Staff 인증·권한은 Identity Platform이 소유하며, Gongzzang B2C 사용자는 계속 Gongzzang이 소유한다.
- 문서·코드의 신규 플랫폼 식별자는 lowercase slug(`foundation-platform`, `identity-platform`, `intelligence-platform`).
  Brand display는 `Gongzzang`, `Dawneer`, `Foundation Platform`, `Identity Platform`, `Intelligence Platform`.
  legacy core name과 `gongzzang3`, `seanal-sms`, `Seanal Site Management System`은 legacy 물리 경로 또는 historical reference 전용.

**의사결정 SSOT (이 repo의 ADR)**: [0030](./docs/adr/0030-three-service-architecture.md) γ' 채택 · [0031](./docs/adr/0031-foundation-platform-bounded-contexts.md) Foundation/Identity 분리의 역사적 근거 · [0032](./docs/adr/0032-eventual-consistency-strategy.md) 일관성 전략 · [0033](./docs/adr/0033-seven-guardrails-enforcement.md) 7 Guardrails 강제 ·
[0034](./docs/adr/0034-catalog-ownership-handover-to-foundation-platform.md) catalog 자산 이양 시점·방식 · [0048](./docs/adr/0048-horizontal-platform-redefinition.md) 최종 수평 플랫폼 구조 · [0050](./docs/adr/0050-dawneer-workbench-and-internal-admin-surface.md) Dawneer workbench·admin 경계.
**Cross-area SSOT**: [Foundation ADR 0021](../../platforms/foundation-platform/docs/adr/0021-adopt-horizontal-platform-redefinition.md) +
[Gongzzang ADR 0034](./docs/adr/0034-catalog-ownership-handover-to-foundation-platform.md) +
[Gongzzang ADR 0048](./docs/adr/0048-horizontal-platform-redefinition.md)

### 이 repo 작업자가 알아야 할 영향 (경계 규칙)

- `crates/{industrial-complex-domain, parcel-domain, building-domain, manufacturer-domain}`,
  `crates/data-clients/{vworld, data-go-kr, raw-capture}`, `crates/data-pipeline-control`은
  **gongzzang workspace에 존재하면 안 된다**. Catalog/ETL 변경은 `foundation-platform`에서 진행.
  해당 crate 또는 직접 의존성을 재도입하면 boundary CI가 차단해야 한다.
- gongzzang은 (현재 구현 repo/path가 legacy일 수는 있어도) Foundation Platform published
  contract(API, event, immutable artifact)만 소비한다.
- 그 외 이 repo에 존재하면 안 되는 Foundation Platform 소유 자산 — ETL service scaffold
  (`services/data-pipeline`, `services/scraper-py`), public/reference vector tile ETL 자산(sp9),
  Catalog public API drift observability(api-health) — 의 파일 단위 전체 목록 SSOT:
  [docs/architecture/foundation-platform-boundary.v1.json](./docs/architecture/foundation-platform-boundary.v1.json)
- Gongzzang의 pinned Catalog API consumer contract는
  [foundation-platform-catalog-api-contract.v1.pin.json](./docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json).
  parcel/building Foundation Platform 소비 변경은 이 pin을 갱신하고
  `../../platforms/foundation-platform/docs/openapi/catalog.v1.json`과의 일치를 유지해야 한다.
- 신규 Gongzzang 스키마에는 Foundation 소유 collection/raw-capture/API-health 테이블이 없다.
  런칭 전 호환 정리 마이그레이션·legacy schema-token 예외는 의도적으로 부재.
- B2C 도메인(`crates/{listing-domain,listing-photo-domain,user-domain,court-auction-domain,real-transaction-domain}`,
  `crates/{bookmark-domain,search-history-domain,analysis-report-domain,notification-domain}`)과 Gongzzang 운영 도메인
  (`crates/{admin-action-domain,audit-log-domain,business-verification-domain,featured-content-domain,listing-report-domain,listing-review-domain,outbox-event-domain,system-alert-domain}`)은
  이 repo가 영구 owner이며 영향 없음.

### 지도/매물 마커 SSOT 및 현재 게이트

먼저 확인: [ADR 0018](./docs/adr/0018-pnu-first-identity-no-coordinates.md) Listing 위치 identity는 PNU-first ·
[ADR 0037](./docs/adr/0037-pnu-anchor-pbf-marker-tiles.md) PNU-anchor PBF marker tile contract ·
[ADR 0038](./docs/adr/0038-listing-marker-serving-index-filter-mask.md) serving index/filter mask contract.
현행 구현 SSOT는 `crates/gongzzang-persistence/src/listing/marker_tile.rs`와
`apps/web/lib/map/{marker-tile-contract.ts,listing-map-runtime.ts}`다.

- `foundation-platform` owns parcel geometry, PNU marker anchors, and public/reference spatial layers.
- `gongzzang` owns listing semantics and Gongzzang-owned listing PBF marker tiles.
- listing rows must not own canonical marker coordinates such as `geom_point`, latitude, or longitude.
- launch marker requests must not use public `bbox`/`bounds` marker request shapes.
- implementation gate은 verification-first: listing PBF endpoint, anchor read model migration,
  frontend listing PBF switch는 tests + migration smoke + guardrails 뒷받침 후에만 완료를 주장한다.

## 1. 절대 규칙

- ❌ 모든 파일 **1500줄 초과 금지** (≤500 권장). 초과 시 폴더로 분해
- ❌ [docs/glossary.md](./docs/glossary.md) 외 도메인 용어 사용 금지
- ❌ 사용자에게 노출되는 텍스트를 LLM이 생성하지 말 것 (옵션 A 위반)
- ❌ 임시방편 코드 (`TEMP`, `HACK`, `XXX`, `ALLOWED_FOR_FRONTEND_TEMP` 류) 금지
- ❌ 메인 시스템(`apps/`, `services/`, `crates/`, `packages/`)에 MCP/LLM SDK 의존성 금지
- ❌ Pulumi 외 AWS 콘솔 직접 변경 금지 (인프라는 코드로만)
- ❌ API 키 하드코딩 / `.env` 커밋 — gitleaks가 차단
- ❌ SRID 미지정 공간 쿼리 (PostGIS 호출 시 항상 EPSG 명시)

## 2. 작업별 진입점 (라우팅)

| 작업 유형 | 우선 참조 |
|---------|----------|
| 새 기능 추가 | [docs/backend/](./docs/backend/README.md) + [docs/conventions/](./docs/conventions/README.md) |
| 새 외부 API 통합 | [docs/data-sources/](./docs/data-sources/README.md) + [docs/backend/circuit-breaker.md](./docs/backend/) |
| DB 스키마 변경 | [docs/database/migrations.md](./docs/database/migrations.md) + [docs/database/er-diagram-v001.md](./docs/database/er-diagram-v001.md) |
| 인증/권한 작업 | [docs/auth/](./docs/auth/README.md) + [docs/conventions/error-format.md](./docs/conventions/error-format.md) |
| UI 컴포넌트 | [docs/frontend/](./docs/frontend/README.md) + [docs/conventions/ui-writing-korean.md](./docs/conventions/ui-writing-korean.md) |
| 패널 시스템 | [docs/frontend/panel-sss-axes.md](./docs/frontend/panel-sss-axes.md) (§10 축) |
| 인프라 변경 | [infrastructure/README.md](./infrastructure/README.md) (Pulumi 코드로만) |
| 새 결정 필요 | [docs/adr/README.md](./docs/adr/README.md) (ADR 작성 후 코드) |
| 관측성/로깅 | [docs/architecture/observability.md](./docs/architecture/observability.md) |
| 보안/PII | [docs/sss-charter.md](./docs/sss-charter.md) §B-3 보안·프라이버시 |
| 컴플라이언스 | [docs/compliance/](./docs/compliance/README.md) |

## 3. 데이터 접근 규칙 (SSS 핵심)

### 메인 시스템 (사용자 트래픽 경로)

- **Catalog 공식 API 직접 호출 금지**: V-World, data.go.kr 는 Foundation Platform 이 소유합니다.
- gongzzang 은 Catalog 데이터를 Foundation Platform published contract 로만 소비합니다.
- 법제처(open.law.go.kr) 등 Gongzzang 소유 사용자 기능에 필요한 외부 API만 직접 통합 가능합니다.
- LLM/MCP 의존성 0
- 모든 외부 호출에 Circuit Breaker + Retry + Timeout + Audit log
- Gongzzang-owned direct external calls must preserve raw lineage through an
  ADR-approved archive/lineage contract. Catalog raw lineage belongs to Foundation Platform.

### AI 에이전트 경로 (개발자 Claude 세션 한정)

- MCP 사용 가능 (개발/탐색용)
- 메인 코드에 import 금지

### 향후 옵션 C (AI 어시스턴트, 별도 모듈)

- `apps/ai-assistant/` 자리만 비워둠
- 도입 시 verify_citations 등 환각 방지 의무

## 4. 자동 강제 흐름

에디터 → pre-commit → pre-push → CI(PR) → CI(merge) 5단계 상세: [docs/conventions/enforcement-flow.md](./docs/conventions/enforcement-flow.md)

## 5. 한국어 규칙

- 사용자 노출 문자열: **해요체** (예: "조회했어요", "잠시 후 다시 시도해 주세요")
- 에러 메시지: **원인 + 대응 안내**
- 법령 인용: 정식 명칭 + 조·항·호 (예: "국토의 계획 및 이용에 관한 법률 제76조제5항")
- 도메인 용어: [docs/glossary.md](./docs/glossary.md) 의 영문 식별자 사용 (코드)
- 로그/커밋: 영어 (Conventional Commits)

## 6. 사용자 확인 필요한 작업

- 새 npm/cargo 패키지 추가
- DB 스키마 변경 (마이그레이션 생성 전 승인)
- 인증/권한/개인정보 로직 수정
- V-World 쿼터에 영향을 줄 배치 작업
- 공공데이터 재배포/오픈소스 공개
- `git push --force`, `git reset --hard`, 브랜치 삭제

## 7. 도메인 어휘

SSOT는 [docs/glossary.md](./docs/glossary.md) — 한→영 대응표 포함. 코드 식별자는 glossary의 영문만 사용.

## 8. SSOT 원칙

각 정보는 **한 곳에만** 존재. 사본이 있으면 그것이 사본임을 명시.

- 사용자 데이터 → PostgreSQL `user` (Redis 세션은 사본)
- Catalog public API raw → Foundation Platform object lake / lineage store
- Gongzzang-owned external API raw → owning module's approved archive / lineage contract
- 비즈니스 규칙 → `crates/*-domain` Rust 코드
- API 계약 → Rust 코드 + utoipa (OpenAPI 자동, TS 타입 자동)
- DB 스키마 → `migrations/*.sql` (`YYYYMMDDHHMMSS_<snake_case>.sql`, sqlx migrate/prepare가 자동 검증)
- 인프라 → Pulumi TypeScript (AWS 콘솔 수동 변경 금지)
- 도메인 용어 → [docs/glossary.md](./docs/glossary.md)

상세 매트릭스: → [docs/ssot-matrix.md](./docs/ssot-matrix.md)

## 9. 1500줄 안티패턴 경보

규칙 자체는 §1(1500줄 금지, ≤500 권장). 임계값·배경·사례: [docs/conventions/enforcement-flow.md](./docs/conventions/enforcement-flow.md)

## 10. SSS-grade Panel System Axes

패널 작업 시 [docs/frontend/panel-sss-axes.md](./docs/frontend/panel-sss-axes.md) 필독 — 15개 축(§10.1–10.5) 전문.
BLOCKER 중 *Type Safety / SSOT / Security & Privacy / Migration* 은 도메인 무관 일반 룰로 다른 영역에도 동일 적용.
