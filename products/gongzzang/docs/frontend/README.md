---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# 프런트엔드 (`apps/web`)

> **목적**: 공짱 프런트엔드 인프라와 화면.
> **기술 구성**: Next.js 앱 라우터 + React + TypeScript + Tailwind CSS + shadcn/ui (코드 복사). 버전 핀은
> [`tools/technology-versions.contract.json`](../../../../tools/technology-versions.contract.json)이 정본이다.
> **결정**: [ADR-0003](../adr/0003-frontend-nextjs-react19.md)

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

## 디렉터리 구조

- `apps/web/` — Next.js 앱 라우터
  - `app/` — 라우트 (`(authenticated)/`·`(public)/`  묶음, `error.tsx` / `not-found.tsx` / `loading.tsx`)
  - `app/api/proxy/[...path]/route.ts` — 백엔드 프록시
  - `app/api/auth/` — 로그인·콜백·로그아웃·갱신
  - `proxy.ts` — 인증 관문 (Next.js 16 의 미들웨어 자리)
  - `lib/` — 공용 함수 (`api`·`map`·`listings`·`panel`·`i18n` 등)
  - `stores/` — Zustand 저장소 (`listings.ts`, `map.ts`)
  - `instrumentation.ts` — OpenTelemetry SDK 초기화 (프런트엔드 Sentry 는 아직 없음)
  - `tests/unit/`·`tests/integration/` — Vitest 시험, `tests/e2e/` — Playwright + axe 접근성 시험
- `packages/ui/` — shadcn 기본 부품 + 디자인 토큰 (교체 지점)
  - `primitives/` — 버튼·입력·카드·대화상자·폼·선택·알림 등
  - `tokens/` — CSS 변수 (색·간격·글꼴 + Pretendard 웹 글꼴)
- `packages/api-types/` — utoipa OpenAPI → TypeScript 타입

## 주요 명령

| 명령 | 설명 |
|---|---|
| `pnpm dev` | 개발 서버 (turbo) |
| `pnpm --filter=@gongzzang/web build` | 운영 빌드 (`next build`) |
| `pnpm --filter=@gongzzang/web typecheck` | TypeScript 검증 (`tsc --noEmit`) |
| `pnpm --filter=@gongzzang/web test` | Vitest 단위 시험 |
| `pnpm --filter=@gongzzang/web test:integration` | Vitest 통합 시험 (Redis 필요) |
| `pnpm --filter=@gongzzang/web test:e2e` | Playwright 종단 + 접근성 시험 |
| `pnpm --filter=@gongzzang/web lint` | Biome 정적 검사 |
| `pnpm format` | Biome 형식 맞춤 |
| `pnpm --filter=@gongzzang/web test:bundle` | size-limit 번들 크기 예산 |
| `pnpm --filter=@gongzzang/api-types generate` | utoipa OpenAPI → TS 타입 |

## 한국어 화면 문구 규약 (해요체)

규칙은 [UI 라이팅](../conventions/ui-writing-korean.md)이 정본이다.

- 시간·숫자·면적 형식: `apps/web/lib/i18n/haeyo.ts` 의 함수를 쓴다
- 문구 자원: `apps/web/lib/i18n/ko.json` (next-intl)

## 디자인 시스템 교체 경로

```text
[현재] packages/ui/tokens/ — CSS vars (color/spacing/typography + Pretendard)
       ↓ Tailwind 4 @theme 가 var(--color-brand-500) 등 참조
       ↓ packages/ui/primitives/ 가 className 으로 사용

[미래] 별도 design-system 패키지를 도입하면 packages/ui/tokens/ 만 그 토큰을 re-export
       ↓ primitives 코드 변경 0 — 시각적 swap
```

## 검증 관문

CI 정본은 루트 `.github/workflows/gongzzang-frontend.yml` 이다. 단계: Biome 검사 · 마크다운 정적 검사 ·
타입 검사 · Vitest 레인 완전성 · 단위 시험 · Redis 통합 시험 · 빌드 · 번들 크기 · Playwright 종단 + 접근성 시험.

## 하지 않는 것

- PWA·오프라인 — 지금 필요 없다
- Storybook — 지금 규모에 과하다

## 참고

- [ADR-0003](../adr/0003-frontend-nextjs-react19.md): 프런트엔드 기술 구성 결정
- [AGENTS.md](../../AGENTS.md): 영역 규칙과 강제 장치
- 패널 시스템 규범: [panel-sss-axes.md](./panel-sss-axes.md)
- 매물 검색 런타임: [listings-search.md](./listings-search.md)
