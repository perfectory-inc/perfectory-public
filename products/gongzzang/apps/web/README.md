---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# apps/web

Gongzzang 웹 클라이언트다.

## 책임 범위

- Framework: Next.js App Router
- 역할: 브라우저 UI와 same-origin BFF proxy 표면
- 사용자 문구: typed i18n만 사용
- LLM/MCP 의존성: runtime 경로에서 금지

## 현재 경로

- `/listings`: listing search/map surface
- `/api/proxy/*`: same-origin proxy to the Rust API
- `/api/auth/*`: authentication callback/session endpoints

## 경계

- business rule은 Rust domain crate와 API service에 둔다.
- 매물 marker 렌더링은 Gongzzang 매물 PBF source를 사용한다.
- PNU/필지 anchor 소유권은 `foundation-platform`에 둔다.
