---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# 코드 컨벤션

> 모든 컨벤션은 *자동 강제*되어야 함. 사람이 지키는 룰은 SSS 미달.

## 학습 순서

1. [naming-and-ids.md](./naming-and-ids.md) — 네이밍 + ULID prefix
2. [rust.md](./rust.md) — Rust (rustfmt + clippy pedantic)
3. [typescript.md](./typescript.md) — TypeScript (Biome)
4. [sql.md](./sql.md) — PostgreSQL + PostGIS
5. [error-format.md](./error-format.md) — RFC 9457 Problem Details
6. [ui-writing-korean.md](./ui-writing-korean.md) — 사용자 노출 한국어 (해요체)
7. [testing.md](./testing.md) — 테스트 네이밍 + 분류
8. [git-and-pr.md](./git-and-pr.md) — Conventional Commits + PR 룰
9. [comments.md](./comments.md) — Why over What
10. [enforcement-flow.md](./enforcement-flow.md) — 자동 강제 단계 + 1500줄 안티패턴 (AGENTS.md §4·§9 이관)

## 자동 강제 도구 매핑

훅 정의 정본은 루트 `lefthook.yml`, CI 정본은 `.github/workflows/gongzzang-*.yml`이다. 도구 버전은
`tools/technology-versions.contract.json`이 소유한다. 로컬 훅은 보조이며 판정은 CI다(루트 ADR-0005·ADR-0149).

| 컨벤션 | 도구 | 단계 |
|--------|------|------|
| Rust 포맷 | rustfmt | pre-commit + CI |
| Rust lint | clippy (`cargo xtask` verify) | CI |
| TS 포맷/lint | Biome | pre-commit + CI (`gongzzang-frontend.yml`) |
| SQL | 마이그레이션 적용 + 스키마 불변식 시험 (`gongzzang-db-migrations.yml`), SQLx prepare | CI |
| Markdown lint | markdownlint-cli2 | pre-commit + CI |
| 커밋 메시지 | 없음 (commit-msg 훅 미설치) | — |
| 시크릿 | gitleaks | pre-commit + CI (`secret-scan.yml`) |
| 의존성 방향 | clippy `disallowed-types` + `repo-guard capability-layout` + `gongzzang-catalog-boundary.yml` | CI |
| 파일 크기 (1500줄) | `scripts/guard/file-line-limit.sh` / `repo-guard file-line-limit` | pre-push + CI |
| 금지 마커 (TODO/HACK/XXX 등) | `scripts/lefthook/check-forbidden-implementation-markers.sh` | pre-push + CI |

## 위반 처리

- pre-commit 단계 차단 → 즉시 수정 후 재시도
- CI 단계 차단 → PR 머지 불가
- *예외 적용* 필요 시 ADR 작성
