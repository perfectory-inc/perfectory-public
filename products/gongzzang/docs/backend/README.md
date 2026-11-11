---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# backend/

Rust 백엔드 아키텍처와 패턴의 정본.

## 책임 영역
구현됨:

- Axum HTTP 서버 (`services/gongzzang-api`)
- 공짱이 소유한 비동기 이벤트를 내보내는 Tokio 아웃박스 발행기 (`services/gongzzang-outbox-publisher`)
- 도메인 주도 설계의 도메인 크레이트 (`crates/*-domain` — 목록은 디렉터리가 정본)
- 클린 아키텍처 (포트와 어댑터, 구현체는 `crates/gongzzang-persistence`)
- 회로 차단기 (보호된 바깥 호출) — [circuit-breaker.md](./circuit-breaker.md)
- 아웃박스 패턴 (`outbox_event` 표, `crates/gongzzang-outbox`)

미구현 (코드 없음):

- 명령·조회 분리(CQRS)용 별도 읽기 저장소
- 이벤트 소싱
- 사가 패턴

세부 구현 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## 관련 ADR
- [ADR 0001 — Rust·TypeScript](../adr/0001-language-rust-ts.md)
- [ADR 0002 — Cargo·pnpm·Turborepo](../adr/0002-monorepo-cargo-pnpm-turbo.md)

## 관련 컨벤션
- [Rust 컨벤션](../conventions/rust.md)
- [에러 형식 컨벤션](../conventions/error-format.md)
