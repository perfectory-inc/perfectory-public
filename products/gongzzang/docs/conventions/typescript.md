---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# TypeScript 컨벤션

## 1. 도구

- **TypeScript**: strict (`tsconfig.base.json`)
- **포맷·lint·import sort**: Biome (단독, `biome.json`)
- **테스트**: Vitest (unit·integration 레인), Playwright — [테스트 컨벤션](./testing.md)
- **공급망**: OSV-Scanner 가 `pnpm-lock.yaml` 을 포함한 모든 lockfile 을 검사 (`cargo xtask ci osv-gate`, 허용 목록은 루트 `osv-scanner.toml`, 루트 ADR-0414)

버전 핀(TypeScript·Biome·Vitest·Next.js 등)은 [`tools/technology-versions.contract.json`](../../../../tools/technology-versions.contract.json)이 정본이다.

## 2. tsconfig 핵심 (`tsconfig.base.json`)

- `strict: true`
- `noUncheckedIndexedAccess: true`
- `exactOptionalPropertyTypes: true`
- `verbatimModuleSyntax: true`
- `noImplicitOverride: true`
- `isolatedModules: true`

## 3. Biome 핵심 (`biome.json`)

- 포맷: 2 space, 100 width, `lineEnding: lf`
- import sort: `assist.actions.source.organizeImports: on`
- `correctness.noUnusedImports: error`
- `style.useConst: error`, `useImportType: error`
- `suspicious.noExplicitAny: error`, `noConsole: warn` (`console.warn/error/info` 허용)

## 4. Next.js 패턴 (옵션 A)

### Server Component 기본

```tsx
// apps/web/app/(authenticated)/listings/page.tsx
import { fetchListings } from "@/lib/listings/api";

export default async function ListingsPage({ searchParams }: Props) {
  const params = await searchParams;
  const data = await fetchListings({ filters: parseListingFilters(params) });
  return <ListingsList data={data.listings} />;
}
```

백엔드 호출은 생성된 proxy client(`@/lib/api/api-proxy-client.generated`)로만 —
직접 `services/gongzzang-api` 호출 금지, 항상 Next.js proxy route 경유 (`apps/web/lib/api.ts`).

### Client Component는 명시적

```tsx
"use client";
import { useState } from "react";
// ...
```

### Server Action = 얇은 프록시

지금 `apps/web` 에는 Server Action 이 없다. 만들게 되면 생성된 proxy client 만 부르는 얇은 프록시로 둔다
(세션 확인은 `proxy.ts` 인증 관문과 `/api/proxy` route 가 한다).

```tsx
"use server";
import { apiProxyClient } from "@/lib/api/api-proxy-client.generated";

export async function bookmarkListing(listingId: string) {
  return apiProxyClient.listingBookmark.post({ id: listingId });
}
```

→ 비즈니스 로직 0줄. 검증·계산·저장은 Rust.

## 5. import 패턴

```tsx
// 1. 외부
import { useState } from "react";
import { z } from "zod";

// 2. 절대 경로 (@/)
import { fetchListings } from "@/lib/listings/api";

// 3. 워크스페이스
import { Button } from "@gongzzang/ui";
import type { paths } from "@gongzzang/api-types";

// 4. 상대 (같은 폴더만)
import { ListingCard } from "./listing-card";
```

## 6. 컴포넌트 구조

```tsx
// 1. import
// 2. type/interface
// 3. constants
// 4. component
// 5. helper (같은 파일 안에서만 쓰는 것)
```

## 7. 폼

`react-hook-form` + `zod`:

```tsx
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";

const schema = z.object({
  pnu: z.string().length(19),
  priceKrw: z.number().positive(),
});

const { register, handleSubmit } = useForm({ resolver: zodResolver(schema) });
```

## 8. 상태 관리

- 서버 상태: TanStack Query 또는 Server Component fetch
- 클라이언트 상태: Zustand (필요 시만)
- 폼 상태: react-hook-form

전역 상태는 *마지막 수단*. 우선 props drilling → useContext → Zustand 순서.

## 9. 금지 패턴

- ❌ `any` 타입 (`noExplicitAny: error`)
- ❌ `console.log` (대신 `@/lib/observability/logger` 의 pino 로거 또는 `console.warn/error/info`)
- ❌ Server Action 안에 비즈니스 로직 (Rust로 위임)
- ❌ 백엔드 응답 타입 수동 작성 (`packages/api-types/generated/schema.ts`는 OpenAPI 자동 생성만)
- ❌ TODO/HACK/XXX 코멘트
- ❌ `// @ts-ignore`, `// @ts-expect-error` (없이 해결)

## 10. 자동 강제

- pre-commit: `pnpm biome check --write` (루트 `lefthook.yml` 의 `gz-biome`, 바뀐 파일만)
- CI (정본): 루트 `.github/workflows/gongzzang-frontend.yml` — `pnpm biome check .`, `pnpm turbo typecheck`, `pnpm turbo test` 등
