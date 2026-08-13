---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# 폐기된 마이그레이션 경로

`db/migration/`은 마이그레이션 정본이 아니다.

모든 SQLx 마이그레이션은 `migrations/`에 둔다. 현재 이름 규칙은
`YYYYMMDDHHMMSS_<snake_case>.sql`, documented in [`../../migrations/README.md`](../../migrations/README.md).

이곳에 새 SQL 파일을 추가하지 않는다.
