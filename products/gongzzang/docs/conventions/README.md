---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# 코드 컨벤션

> 모든 규약은 *도구가 자동으로 강제*해야 한다. 사람이 지켜야 하는 규칙은 기준 미달이다.

## 학습 순서

1. [naming-and-ids.md](./naming-and-ids.md) — 이름 짓기 + ULID 접두어
2. [rust.md](./rust.md) — Rust (rustfmt + clippy 엄격 규칙)
3. [typescript.md](./typescript.md) — TypeScript (Biome)
4. [sql.md](./sql.md) — PostgreSQL + PostGIS
5. [error-format.md](./error-format.md) — RFC 9457 문제 상세 형식
6. [ui-writing-korean.md](./ui-writing-korean.md) — 사용자 노출 한국어 (해요체)
7. [testing.md](./testing.md) — 시험 이름 + 분류
8. [git-and-pr.md](./git-and-pr.md) — 커밋 메시지 규약 + PR 규칙
9. [comments.md](./comments.md) — 무엇보다 왜를 적는다
10. [enforcement-flow.md](./enforcement-flow.md) — 자동 강제 단계 + 1500줄 반패턴 (AGENTS.md §4·§9 에서 옮김)

## 자동 강제 도구 매핑

훅 정의 정본은 루트 `lefthook.yml`, CI 정본은 `.github/workflows/gongzzang-*.yml`이다. 도구 버전은
`tools/technology-versions.contract.json`이 소유한다. 로컬 훅은 보조이며 판정은 CI다(루트 ADR-0005·ADR-0149).

| 규약 | 도구 | 단계 |
|--------|------|------|
| Rust 형식 | rustfmt | 커밋 훅 + CI |
| Rust 정적 검사 | clippy (`cargo xtask` verify) | CI |
| TS 형식·정적 검사 | Biome | 커밋 훅 + CI (`gongzzang-frontend.yml`) |
| SQL | 마이그레이션 적용 + 스키마 불변식 시험 (`gongzzang-db-migrations.yml`), SQLx 준비 파일 | CI |
| 마크다운 정적 검사 | markdownlint-cli2 | 커밋 훅 + CI |
| 커밋 메시지 | 없음 (커밋 메시지 훅 미설치) | — |
| 비밀값 | gitleaks | 커밋 훅 + CI (`secret-scan.yml`) |
| 의존성 방향 | clippy `disallowed-types` + `repo-guard capability-layout` + `gongzzang-catalog-boundary.yml` | CI |
| 파일 크기 (1500줄) | `scripts/guard/file-line-limit.sh` / `repo-guard file-line-limit` | 푸시 훅 + CI |
| 금지 표식 (`TODO`·`HACK`·`XXX` 등) | 루트 Semgrep 규칙 `tools/semgrep/rules/gongzzang-implementation-markers.yml` | CI |

## 위반 처리

- 커밋 훅에서 막히면 → 바로 고치고 다시 커밋
- CI 에서 막히면 → PR 을 병합할 수 없다
- *예외*가 필요하면 ADR 을 쓴다
