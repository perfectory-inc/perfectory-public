---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-11-05
---

# MCP와 API

이 문서는 개발자·에이전트 탐색 경로와 운영 런타임 경로를 분리한다.

## 1. 규칙

MCP와 에이전트 도구는 개발 탐색에서만 허용한다.

운영 앱·서비스·크레이트·패키지는 별도 AI 보조 경계를 만드는 ADR 이 승인되기 전까지 MCP
서버·LLM SDK·에이전트 전용 연결기에 의존하지 않는다.

## 2. 운영 API 경로

운영 실행은 명시적인 API 와 타입이 있는 계약을 쓴다:

```text
Browser
  -> Next.js routes/proxy
  -> Rust API
  -> Postgres / Valkey(Redis protocol) / R2 / Foundation Platform published APIs
```

주요 파일:

- `services/gongzzang-api/src/app.rs`
- `apps/web/app/api/proxy/[...path]/route.ts`
- `docs/architecture/platform-integration/index.v1.json`
- `docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

## 3. 에이전트 탐색 경로

에이전트 세션은 MCP나 브라우저 자동화로 외부 시스템과 로컬 코드를 확인할 수 있다.

예:

- 저장소 감사
- 원천 문서 찾아보기
- 로컬 브라우저 확인
- ADR 이나 구현 계획을 쓰기 전의 일회성 조사

에이전트가 찾은 사실은 운영 동작에 영향을 주기 전에 코드·ADR·등록부·명시적 문서로 남겨야 한다.

## 4. 금지된 운영 결합

다음 영역에는 MCP/LLM SDK 의존성을 추가하지 않는다.

- `apps/web`
- `services/gongzzang-api`
- `services/gongzzang-outbox-publisher`
- `crates/*-domain`
- `crates/gongzzang-persistence`
- `packages/ui`

런타임 정확성을 에이전트 기억·대화 기록·로컬 브라우저 상태·MCP 전용 원천에 의존시키지 않는다.

## 5. 앞으로의 AI 보조 경계

공짱에 AI 기능을 더할 때는 따로 승인된 경계를 거쳐 들인다.

예상 모양:

```text
Gongzzang / Foundation Platform source records
  -> approved ingestion/indexing job
  -> vector/search/knowledge index
  -> AI assistant service
  -> product API
```

AI 서비스는 LLM SDK, 임베딩, 벡터 검색, 검색 증강 도구를 쓸 수 있다. 주 제품
도메인은 여전히 정본 기록과 삭제·수명 주기 규칙을 소유한다.

## 6. 안전장치

AI 나 에이전트를 위한 코드를 들일 때는 다음을 따른다.

- 먼저 ADR 을 쓴다.
- 정본 업무 기록은 소유 서비스에 둔다.
- 생성된 요약과 임베딩은 파생 산출물로 둔다.
- 실행 의존성을 들이기 전에 경계 검사를 먼저 더한다.

파운데이션 경계와 플랫폼 연동 정책은 CI 에서 강제한다. 파운데이션 카탈로그
경계는 `tools/semgrep/rules/gongzzang-catalog-boundary.yml`(루트 Semgrep 규칙)와 경계 계약
`docs/architecture/foundation-platform-boundary.v1.json`이 지킨다(ADR-0149 이후 CI 전용).
