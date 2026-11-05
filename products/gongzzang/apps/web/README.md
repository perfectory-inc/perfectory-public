---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# apps/web

Gongzzang 웹 클라이언트다.

## 책임 범위

- Framework: Next.js App Router (버전은 `package.json`)
- 역할: 브라우저 UI와 same-origin BFF proxy 표면
- 사용자 문구: typed i18n(`lib/i18n/`)만 사용
- LLM/MCP 의존성: runtime 경로에서 금지

## 경로

화면 경로의 정본은 `app/` 트리다(`(public)`·`(authenticated)` 그룹). 서버 표면은 셋이다.

- `/api/proxy/*`: same-origin proxy to the Rust API
- `/api/auth/*`: login·callback·refresh·logout
- `/foundation-platform/events`: Foundation webhook 수신(POST)

## 경계

- business rule은 Rust domain crate와 API service에 둔다.
- 매물 marker 렌더링은 Gongzzang 매물 marker PBF tile(`lib/map/marker-tile-contract.ts`)을 사용한다.
- PNU/필지 데이터 소유권은 `foundation-platform`에 둔다.
