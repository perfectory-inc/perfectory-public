# docs/

도메인별 SSOT 문서 트리. **한 폴더 = 한 도메인**, 각 폴더의 README가 인덱스.

## 학습 순서 (새로 합류한 분 기준)

| # | 문서 | 내용 |
|---|------|------|
| 1 | [sss-charter.md](./sss-charter.md) | 7 기둥 SSS 헌법 — *모든 작업의 측정 자* |
| 2 | [glossary.md](./glossary.md) | 한·영 도메인 용어 사전 |
| 3 | [ssot-matrix.md](./ssot-matrix.md) | 정보별 SSOT + 위반 자동 차단 룰 |
| 4 | [conventions/](./conventions/README.md) | 코드 스타일 + 네이밍 + 에러 형식 |
| 5 | [data-sources/](./data-sources/README.md) | 외부 공공 API 카탈로그 |
| 6 | [adr/](./adr/README.md) | 모든 기술·아키텍처 결정 이력 |

## 도메인 카테고리

| 카테고리 | 책임 | 작성 시점 |
|---------|------|---------|
| [auth/](./auth/README.md) | Zitadel, OIDC/OAuth2, RBAC, NICE 본인인증, WebAuthn | sub-project 3 |
| [architecture/](./architecture/README.md) | 시스템 구조, 데이터 흐름, 캐싱, 관측성([observability.md](./architecture/observability.md)), geo 파이프라인 | 전반 |
| [database/](./database/migrations.md) | Postgres + PostGIS 마이그레이션 규칙, ER 다이어그램 | sub-project 2 |
| [backend/](./backend/README.md) | Axum, SQLx, DDD, Circuit Breaker, Idempotency | sub-project 5 |
| [runbooks/](./runbooks/) | Foundation Platform 연동·공급망 운영 절차 | 전반 |
| [testing/](./testing/README.md) | 단위/통합/E2E/property/mutation/load/chaos/contract | 전반 |
| [frontend/](./frontend/README.md) | Next.js, shadcn/Radix, TanStack Query, Naver Maps, PWA, a11y | sub-project 6 |
| [governance/](./governance/README.md) | ADR, CODEOWNERS, Changesets, Renovate, DORA, C4 | 전반 |
| [compliance/](./compliance/README.md) | PIPA, ISMS-P, SOC 2, audit log retention, 공공데이터 라이선스 | Phase 3+ |
| [cost/](./cost/README.md) | Phase별 AWS 비용 추정, RI/Spot 전략 | 전반 |

폴더 없는 도메인의 실질 SSOT: 인프라(IaC) = [../infrastructure/README.md](../infrastructure/README.md),
보안·프라이버시 = [sss-charter.md](./sss-charter.md) §B-3, 캐시/메시징·API 방향 = [adr/](./adr/README.md) (ADR-0006/0007/0046/0047).

## SSOT 원칙

- 한 정보는 *한 폴더*에만 작성
- 다른 곳에서 필요하면 `→ @docs/<domain>/<topic>.md` 링크
- 중복 검출 = CI 차단 (lefthook + markdownlint + 자체 lint)

## 역사 기록 경계

dated plan/spec/handoff/research와 운영 증거는 공개 코드 트리에 두지 않는다. 필요한 현행
불변식은 `adr/`, `architecture/`, `runbooks/` 또는 코드로 승격하고, 역사 기록은
[루트 ADR-0007](../../../docs/adr/0007-public-code-private-operations-boundary.md)에 따른
비공개 전환 archive에서만 보존한다.

## 작성 규칙

1. 모든 .md ≤500줄. 초과 시 폴더로 분해.
2. 모든 도메인 폴더에 `README.md` 필수.
3. 다른 문서 참조는 명시적 Markdown 링크 (`[text](path.md)` 또는 `@AGENTS.md` 자동 import)
4. 한국어 본문 + 영어 코드 식별자 (glossary 매핑 강제)
