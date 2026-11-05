---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# crates/gongzzang-persistence

`SQLx` `Postgres` `Repository` 구현체에요.

## 책임

- 도메인 crate가 정의한 `*Repository` trait의 `Postgres` 구현(`Pg*Repository`)이에요 — user, listing,
  listing photo/report/review, admin action, audit log, business verification, featured content,
  system alert, outbox, bookmark, search history, analysis report, notification. 모듈 목록 정본은 `src/lib.rs`.
- listing marker tile·mask·delta·tombstone 조회 모델과 Foundation Platform anchor import(`foundation_anchor`)도 여기 있어요.
- `Optimistic Locking` (`version` 컬럼)을 쓰는 Aggregate는 버전 불일치를 `RepoError::Conflict`로 돌려줘요.
- `sqlx::Error`는 `error_map`의 `map_sqlx_err`로 각 도메인 `RepoError`에 매핑해요 (메시지만 — 정보 누설 방지).

## 의존

- 각 도메인 crate (`Cargo.toml` `[dependencies]`가 정본)와 `shared-kernel`.
- `sqlx` (`postgres` + `chrono`), `geo-types`, `bigdecimal`.

## 정책

- 지금 모든 쿼리는 runtime `sqlx::query` / `query_as` / `query_scalar` + `.bind()`로 작성해요. 컴파일 시점
  매크로(`query!` 계열)와 커밋된 `.sqlx/` 메타데이터는 없어요(마지막 캐시는
  [루트 ADR-0232](../../../../docs/adr/0232-gongzzang-zitadel-subject-lives-only-in-external-account.md) 때 지워졌어요).
- 스키마 정합성은 `tests/*_integration.rs`가 마이그레이션된 DB에 대해 검증해요(`integration` feature,
  CI는 `cargo xtask integration gongzzang postgres`).
- `unwrap`/`expect` 금지(워크스페이스 lint `deny`) — 모든 에러는 `RepoError`로 매핑해요.
