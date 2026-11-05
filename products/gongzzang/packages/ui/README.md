---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# @gongzzang/ui

Gongzzang 웹에서 공유하는 UI 컴포넌트와 디자인 토큰 패키지입니다. 제품별 화면 조합은
`apps/web`이 소유합니다. 공개 진입점은 `package.json` `exports`가 정본입니다
(`.`, `./primitives`, `./tokens`(CSS), "./tokens.js", `./lib/utils`).

- 제품 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `pnpm --filter @gongzzang/ui typecheck`
