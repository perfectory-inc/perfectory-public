# ADR-0003: 프론트엔드 — Next.js 16 + React 19 + Naver Maps

| | |
|---|---|
| 작성일 | 2026-05-01 |
| 상태 | Accepted |
| 결정자 | 운영자 |

## 컨텍스트

산업용 부동산 정보 플랫폼 사용자 UI. 한국 시장 + 지도 중심 + 반응형 웹 + PWA(Phase 1) + 추후 네이티브 앱(Phase 2+).

## 결정

- **Framework**: Next.js 16.2.6 (App Router)
- **React**: 19.2.5
- **TypeScript**: 5.9.3 strict
- **Style**: Tailwind CSS 4.2.4 + shadcn/ui + Radix UI
- **상태**: TanStack Query (서버) + Zustand (클라이언트)
- **폼**: react-hook-form + zod
- **지도**: Naver Maps SDK (한국 사용자 친숙도)
- **타일/마커**: PMTiles + Canvas 2D 마커 (수만 마커 고속 렌더)
- **i18n**: 미사용 (한국어만 고정)

## 대안

- **Remix**: 풀스택 기능 강함, 그러나 ecosystem Next.js 우위
- **Nuxt + Vue**: Vue 채용 풀 작음
- **SvelteKit**: 빠름, 그러나 한국 ecosystem 작음
- **Mapbox GL JS / MapLibre**: Naver보다 글로벌, 그러나 한국 사용자 익숙도 낮음
- **Kakao Map**: 사용 가능, Naver 대비 차이 미미. 대안 자리

## 결과

- 긍정: Server Components → Rust API 호출 단순(얇은 프록시 패턴), Tailwind+shadcn 검증된 디자인 시스템, Naver Maps 한국 사용자 익숙
- 부정: Server Component 학습 곡선, 서드파티 라이브러리 일부 RSC 미지원, Naver Maps 글로벌 진출 시 재작업
- 영향 영역: `apps/platform-web/`, `apps/admin-web/`, `packages/ui-web/`, `packages/map/`

## 재검토 트리거

- Next.js Server Action 패턴이 보안/성능 이슈 일으킬 때
- 한국 외 시장 진출 결정 시 Naver Maps 재고
- React 20 또는 후속 메이저 출시 시

## 참조

- [Frontend 문서](../frontend/README.md)
- [Naver Maps 데이터 소스](../data-sources/naver-maps.md)
- [TypeScript 컨벤션](../conventions/typescript.md)

## 개정 각주

- **2026-09-02** — Next.js 를 `16.2.6` 에서 `16.2.11` 로 올렸다. 이 ADR 의 결정은 "Next.js 16
  App Router" 이고 패치 버전은 그 결정을 바꾸지 않으므로 위 결정문은 그대로 둔다. 사유는
  16.2.11 이 닫는 권고 9건(high 4 — SSRF 2, 미들웨어 우회, 서비스 거부; medium 5)이고,
  전부 `>= 16.0.0, < 16.2.11` 대상이라 이 판을 쓰는 한 피할 수 없다.
  핀의 정본은 `products/gongzzang/apps/web/package.json` 과
  `scripts/guard/technology-version-consistency.sh` 이며, 이 각주는 그 값을 다시 적지 않는다.
