---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# crates/gongzzang-persistence

`SQLx` 로 짠 `Postgres` 저장소(`Repository`) 구현체예요.

## 책임

- 도메인 크레이트가 정의한 `*Repository` 트레이트의 `Postgres` 구현(`Pg*Repository`)이에요. 사용자, 매물,
  매물 사진·신고·후기, 관리자 조치, 감사 기록, 사업자 확인, 추천 콘텐츠, 시스템 경보, 아웃박스,
  북마크, 검색 기록, 분석 보고서, 알림. 모듈 목록 정본은 `src/lib.rs`.
- 매물 표지 타일·마스크·변경분·삭제 표시 조회 모델과 Foundation 앵커 가져오기(`foundation_anchor`)도 여기 있어요.
- 낙관적 잠금(`version` 컬럼)을 쓰는 애그리거트는 버전 불일치를 `RepoError::Conflict` 로 돌려줘요.
- `sqlx::Error`는 `error_map`의 `map_sqlx_err`로 각 도메인 `RepoError`에 매핑해요 (메시지만, 정보 누설 방지).

## 의존

- 각 도메인 crate (`Cargo.toml` `[dependencies]`가 정본)와 `shared-kernel`.
- `sqlx` (`postgres` + `chrono`), `geo-types`, `bigdecimal`.

## 정책

- 지금 모든 쿼리는 실행 시점 `sqlx::query` / `query_as` / `query_scalar` + `.bind()`로 작성해요. 컴파일 시점
  매크로(`query!` 계열)와 커밋된 `.sqlx/` 메타데이터는 없어요(마지막 캐시는
  [루트 ADR-0232](../../../../docs/adr/0232-gongzzang-zitadel-subject-lives-only-in-external-account.md) 때 지워졌어요).
- 스키마 정합성은 `tests/*_integration.rs`가 마이그레이션된 DB에 대해 검증해요(`integration` feature,
  CI는 `cargo xtask integration gongzzang postgres`).
- `unwrap`/`expect` 금지(워크스페이스 린트 `deny`): 모든 오류는 `RepoError`로 매핑해요.
