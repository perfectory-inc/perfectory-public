---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# shared-kernel

공짱 도메인 공유 값 객체 (Pnu, Money, Area, BusinessNumber 등) crate예요. 모듈 목록 정본은 `src/lib.rs`.

## 책임

- BC 간 공통 어휘 (Pnu, Money, Id prefix marker, `DomainEvent`, `MutationContext`, ...)
- *Aggregate*는 각 capability crate에 속해요 (예: `crates/listing-domain`)
- DB·HTTP·외부 API 의존 없음 — 순수 값 객체만

## 의존

- `ulid` — domain ID 생성
- `chrono` — 시각 (UTC 저장 + KST 표시)
- `geo-types` — Geometry 좌표
- `serde` / `serde_json` — 직렬화 (DB·HTTP 양쪽)
- `thiserror` — 에러 enum
- `regex` + `std::sync::LazyLock` — `Email` 검증
- `utoipa` — API 스키마 derive

## 추가/변경 흐름

1. 계약 변경이면 현행 ADR 또는 도메인 문서에 결정 기록
2. 이 crate에 *실패 테스트* 추가
3. 최소 구현 + clippy 통과
4. 다른 crate가 import

## 테스트 커버리지

`tarpaulin.toml`(제품 루트)에 `cargo-tarpaulin` 설정이 있지만 CI 커버리지 게이트는 없어요
(`gongzzang-ci.yml` 머리 주석).
