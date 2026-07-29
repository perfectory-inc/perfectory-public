---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# backend/

Rust 백엔드 아키텍처·패턴 SSOT.

## 책임 영역
- Axum HTTP 서버 (services/gongzzang-api)
- Tokio outbox publisher for Gongzzang-owned asynchronous events (`services/gongzzang-outbox-publisher`)
- DDD Aggregate 17개 (4 Bounded Context)
- Clean Architecture (Port + Adapter)
- CQRS (Read/Write 분리, Phase 3+)
- Event Sourcing (audit-critical 도메인)
- Saga 패턴 (분산 트랜잭션, Phase 3+)
- Circuit Breaker (모든 외부 호출)
- Idempotency (모든 쓰기 요청)
- Outbox 패턴

세부 구현 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## 관련 ADR
- [ADR 0001 — Rust·TypeScript](../adr/0001-language-rust-ts.md)
- [ADR 0002 — Cargo·pnpm·Turborepo](../adr/0002-monorepo-cargo-pnpm-turbo.md)

## 관련 컨벤션
- [Rust 컨벤션](../conventions/rust.md)
- [에러 형식 컨벤션](../conventions/error-format.md)
