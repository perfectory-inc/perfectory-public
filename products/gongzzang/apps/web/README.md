---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# 공짱 웹 (`apps/web`)

Gongzzang 웹 클라이언트다.

## 책임 범위

- 프레임워크: Next.js App Router(버전은 `package.json`)
- 역할: 브라우저 화면과 같은 출처의 BFF 프록시
- 사용자 문구: 타입이 있는 다국어 사전(`lib/i18n/`)만 쓴다
- 언어 모델·MCP 의존: 실행 경로에서 금지

## 경로

화면 경로의 정본은 `app/` 트리다(`(public)`·`(authenticated)` 그룹). 서버 표면은 셋이다.

- `/api/proxy/*`: Rust API 로 가는 같은 출처 프록시
- `/api/auth/*`: 로그인·콜백·갱신·로그아웃
- `/foundation-platform/events`: Foundation 웹훅 수신(POST)

## 경계

- 업무 규칙은 Rust 도메인 크레이트와 API 서비스에 둔다.
- 매물 표지는 공짱 매물 표지 PBF 타일(`lib/map/marker-tile-contract.ts`)로 그린다.
- PNU/필지 데이터 소유권은 `foundation-platform`에 둔다.
