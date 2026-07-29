---
status: current
owner: gongzzang
doc_type: README
last_reviewed: 2026-07-28
---

# docs/

도메인별 SSOT 문서 트리. **한 폴더 = 한 도메인**, 각 폴더의 README가 인덱스.

## 전체 문서 트리

```text
docs/
├── adr/          제품 설계 결정
├── architecture/시스템 구조·플랫폼 경계
├── auth/         인증·인가
├── backend/      서버 구현 규칙
├── conventions/  코드·DB·문서 규칙
├── data-sources/외부 데이터 사용 계약
├── frontend/     웹·지도·접근성
├── governance/   운영 거버넌스
├── runbooks/     운영 절차
└── testing/      테스트 전략
```

전체 모노레포 문서는 [자동 색인](../../../docs/document-catalog.md)에서 찾습니다.

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

| 카테고리 | 책임 |
|---------|------|
| [auth/](./auth/README.md) | Zitadel, OIDC/OAuth2, RBAC, NICE 본인인증, WebAuthn |
| [architecture/](./architecture/README.md) | 시스템 구조, 데이터 흐름, 캐싱, 관측성, geo 파이프라인 |
| [database/](./database/README.md) | Postgres + PostGIS 마이그레이션 규칙, ER 다이어그램 |
| [backend/](./backend/README.md) | Axum, SQLx, DDD, Circuit Breaker, Idempotency |
| [runbooks/](./runbooks/README.md) | Foundation Platform 연동·공급망 운영 절차 |
| [testing/](./testing/README.md) | 단위·통합·E2E·property·mutation·load·chaos·contract |
| [frontend/](./frontend/README.md) | Next.js, shadcn/Radix, TanStack Query, Naver Maps, PWA, 접근성 |
| [governance/](./governance/README.md) | ADR, CODEOWNERS, 변경·검토 절차, 사고 대응 |
| [compliance/](./compliance/README.md) | PIPA, ISMS-P, SOC 2, 보존·라이선스 정책 |
| [cost/](./cost/README.md) | 비용 기준과 추정 |

아직 작성하지 않은 문서는 이 지도에 작업을 쌓지 않는다. 필요한 문서 작업은 루트
[운영 준비 작업 목록](../../../docs/roadmap/production-readiness.md)에서 관리한다.

폴더 없는 도메인의 실질 SSOT: 인프라(IaC) = [../infrastructure/README.md](../infrastructure/README.md),
보안·프라이버시 = [sss-charter.md](./sss-charter.md) §B-3, 캐시/메시징·API 방향 = [adr/](./adr/README.md) (ADR-0006/0007/0046/0047).

## SSOT 원칙

- 한 정보는 *한 폴더*에만 작성
- 다른 곳에서 필요하면 해당 문서에서 실제 상대 경로를 가리키는 Markdown 링크를 사용한다.
- 중복 검출 = CI 차단 (lefthook + markdownlint + 자체 lint)

## 역사 기록 경계

dated plan/spec/handoff/research와 운영 증거는 공개 코드 트리에 두지 않는다. 필요한 현행
불변식은 `adr/`, `architecture/`, `runbooks/` 또는 코드로 승격하고, 역사 기록은
[루트 ADR-0007](../../../docs/adr/0007-public-code-private-operations-boundary.md)에 따른
비공개 전환 archive에서만 보존한다.

## 작성 규칙

1. 모든 .md ≤500줄. 초과 시 폴더로 분해.
2. 모든 도메인 폴더에 `README.md` 필수.
3. 다른 문서 참조는 명시적 상대 Markdown 링크를 사용하고, 영역 규칙은 [영역 AGENTS.md](../AGENTS.md)를 따른다.
4. 한국어 본문 + 영어 코드 식별자 (glossary 매핑 강제)
