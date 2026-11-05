---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# Frontend (apps/web)

> **목적**: 공짱 frontend 인프라와 화면.
> **Stack**: Next.js App Router + React + TypeScript + Tailwind CSS + shadcn/ui (코드 복사). 버전 핀은
> [`tools/technology-versions.contract.json`](../../../../tools/technology-versions.contract.json)이 정본이다.
> **Decision**: [ADR-0003](../adr/0003-frontend-nextjs-react19.md)

## 시작

```bash
# 의존성 설치 (root 에서)
pnpm install

# dev server (apps/web)
pnpm --filter=@gongzzang/web dev
# 또는
pnpm dev

# 브라우저: http://localhost:3000
```

## 디렉토리 구조

- `apps/web/` — Next.js App Router
  - `app/` — 라우트 (`(authenticated)/`·`(public)/` 그룹, `error.tsx` / `not-found.tsx` / `loading.tsx`)
  - `app/api/proxy/[...path]/route.ts` — backend proxy
  - `app/api/auth/` — login / callback / logout / refresh
  - `proxy.ts` — 인증 관문 (Next.js 16 의 middleware 자리)
  - `lib/` — utilities (api / map / listings / panel / i18n 등)
  - `stores/` — Zustand (`listings.ts`, `map.ts`)
  - `instrumentation.ts` — OpenTelemetry SDK 초기화 (frontend Sentry 는 아직 없음)
  - `tests/unit/`·`tests/integration/` — Vitest, `tests/e2e/` — Playwright + axe
- `packages/ui/` — shadcn primitives + tokens (swap point)
  - `primitives/` — Button / Input / Card / Dialog / Form / Select / Sonner 등
  - `tokens/` — CSS vars (color / spacing / typography + Pretendard webfont)
- `packages/api-types/` — utoipa OpenAPI → TypeScript types

## 주요 명령어

| 명령어 | 설명 |
|---|---|
| `pnpm dev` | 개발 서버 (turbo) |
| `pnpm --filter=@gongzzang/web build` | production 빌드 (next build) |
| `pnpm --filter=@gongzzang/web typecheck` | TypeScript 검증 (tsc --noEmit) |
| `pnpm --filter=@gongzzang/web test` | Vitest unit |
| `pnpm --filter=@gongzzang/web test:integration` | Vitest integration (Redis 필요) |
| `pnpm --filter=@gongzzang/web test:e2e` | Playwright e2e + a11y |
| `pnpm --filter=@gongzzang/web lint` | Biome lint |
| `pnpm format` | Biome format |
| `pnpm --filter=@gongzzang/web test:bundle` | size-limit bundle budget |
| `pnpm --filter=@gongzzang/api-types generate` | utoipa OpenAPI → TS types |

## 한국어 UI 컨벤션 (해요체)

규칙은 [UI 라이팅](../conventions/ui-writing-korean.md)이 정본이다.

- 시간/숫자/면적 포맷: `apps/web/lib/i18n/haeyo.ts` utils 사용
- 다국어 자원: `apps/web/lib/i18n/ko.json` (next-intl)

## 디자인 시스템 swap path

```text
[현재] packages/ui/tokens/ — CSS vars (color/spacing/typography + Pretendard)
       ↓ Tailwind 4 @theme 가 var(--color-brand-500) 등 참조
       ↓ packages/ui/primitives/ 가 className 으로 사용

[미래] 별도 design-system 패키지를 도입하면 packages/ui/tokens/ 만 그 토큰을 re-export
       ↓ primitives 코드 변경 0 — 시각적 swap
```

## 검증 게이트

CI 정본은 루트 `.github/workflows/gongzzang-frontend.yml` 이다. 단계: Biome check · Markdown lint ·
typecheck · Vitest 레인 완전성 · unit · Redis integration · build · bundle size · Playwright e2e + a11y.

## 비목표

- PWA / offline — YAGNI
- Storybook — over-engineered

## 참고

- [ADR-0003](../adr/0003-frontend-nextjs-react19.md): frontend stack decision
- [AGENTS.md](../../AGENTS.md): 영역 규칙과 강제 장치
- 패널 시스템 규범: [panel-sss-axes.md](./panel-sss-axes.md)
- 매물 검색 런타임: [listings-search.md](./listings-search.md)
