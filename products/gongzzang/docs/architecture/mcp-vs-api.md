---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# MCP와 API

이 문서는 개발자·에이전트 탐색 경로와 운영 런타임 경로를 분리한다.

## 1. 규칙

MCP와 에이전트 도구는 개발 탐색에서만 허용한다.

운영 애플리케이션·서비스·crate·패키지는 별도 AI 보조 경계를 만드는 ADR이 승인되기 전까지 MCP
서버·LLM SDK·에이전트 전용 connector에 의존하지 않는다.

## 2. 운영 API 경로

Production runtime uses explicit APIs and typed contracts:

```text
Browser
  -> Next.js routes/proxy
  -> Rust API
  -> Postgres / Redis / R2 / Foundation Platform published APIs
```

Important files:

- `services/gongzzang-api/src/app.rs`
- `apps/web/app/api/proxy/[...path]/route.ts`
- `docs/architecture/platform-integration/index.v1.json`
- `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

## 3. Agent Exploration Path

에이전트 세션은 MCP나 브라우저 자동화로 외부 시스템과 로컬 코드를 확인할 수 있다.

Examples:

- repository audit
- source documentation lookup
- local browser inspection
- one-off research before creating an ADR or implementation plan

에이전트가 찾은 사실은 운영 동작에 영향을 주기 전에 코드·ADR·레지스트리·명시적 문서로 남겨야 한다.

## 4. Forbidden Production Coupling

다음 영역에는 MCP/LLM SDK 의존성을 추가하지 않는다.

- `apps/web`
- `services/gongzzang-api`
- `services/gongzzang-outbox-publisher`
- `crates/*-domain`
- `crates/gongzzang-persistence`
- `packages/ui`

런타임 정확성을 에이전트 기억·대화 기록·로컬 브라우저 상태·MCP 전용 원천에 의존시키지 않는다.

## 5. Future AI Assistant Boundary

Gongzzang에 AI 기능을 추가할 때는 별도 승인 경계를 통해 도입한다.

Expected shape:

```text
Gongzzang / Foundation Platform source records
  -> approved ingestion/indexing job
  -> vector/search/knowledge index
  -> AI assistant service
  -> product API
```

AI service는 LLM SDK, embedding, vector search, retrieval 도구를 사용할 수 있다. 주 제품
도메인은 여전히 정본 record와 삭제/lifecycle 규칙을 소유한다.

## 6. Guardrails

AI 또는 agent 대상 코드를 도입할 때는 다음을 따른다.

- 먼저 ADR을 작성한다.
- canonical business record는 owner service에 둔다.
- 생성 summary와 embedding은 derived artifact로 둔다.
- add boundary checks before runtime dependency is introduced.

Foundation Platform 경계와 platform-integration 정책은 CI에서 강제한다.
and pre-commit. The Foundation Platform catalog boundary is guarded by
`scripts/lefthook/foundation-ownership-boundary.sh` and the boundary contract
`docs/architecture/foundation-platform-boundary.v1.json`.
