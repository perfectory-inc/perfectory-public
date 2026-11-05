---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# Naver Maps API

## 개요

- 운영 기관: 네이버 클라우드 플랫폼
- 공식 사이트: https://www.ncloud.com/product/applicationService/maps
- 우리 사용: **브라우저 지도 렌더링만** — Maps JavaScript API v3 (`gl`·`clustering` 서브모듈).
  지오코딩·역지오코딩·경로·정적 지도 API는 쓰지 않는다. 도입하면 이 문서에 계약을 먼저 적는다.
- 결정: 지도 vendor는 [ADR-0003](../adr/0003-frontend-nextjs-react19.md)·[ADR-0013](../adr/0013-listing-search-naver-maps.md),
  vector 상호작용 모델은 [ADR-0020](../adr/0020-naver-vector-interaction-model.md)이 소유한다.

## 인증

- 네이버 클라우드 플랫폼 가입 → 애플리케이션 등록 → Client ID 발급
- 환경변수: `NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID` 하나 (`apps/web/lib/env.ts`가 검증, 예시는
  `apps/web/.env.local.example`). 서버 쪽 secret은 쓰지 않는다.
- `NEXT_PUBLIC_*` = 브라우저 노출. 콘솔의 *서비스 URL*(도메인 제한)로 보호한다.

## SDK 로드

```tsx
// apps/web/app/layout.tsx (요지)
<script
  type="text/javascript"
  src={`https://oapi.map.naver.com/openapi/v3/maps.js?ncpKeyId=${env.NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID}&submodules=gl,clustering`}
/>
```

- `<head>` 에서 **동기 로드**한다. `gl` 서브모듈이 첫 `Map` 생성 시점에 WebGL 백엔드를 등록해야
  하므로 동적 주입으로는 `gl: true` 가 켜지지 않는다.
- CSP `script-src` 허용 호스트는 `apps/web/proxy.ts` 가 소유한다.

## Quota·비용 경계

Quota와 단가는 공급자가 변경할 수 있는 외부 값이므로 이 문서에 숫자로 고정하지 않는다.
배포 전 공식 Naver Cloud 문서에서 현재 값을 확인하고, 환경별 승인 호출 예산과 경고
임계값은 비공개 운영 시스템에 기록한다. 예상 호출량은 활성 지도 세션과 실측 호출 수로 계산한다.

## 좌표계

- 입출력은 WGS84 위경도(EPSG:4326).

## 마커 렌더링

매물 마커는 Naver `gl` 지도 안의 GL 브리지(`apps/web/lib/map/listing-map-runtime.ts`)를 통해 그린다.
렌더러 전환은 공개 문서의 임의 사용자 수나 phase가 아니라 지원 기기 벤치마크와
frame-time·memory 예산으로 결정한다.

## 대안

- Mapbox GL JS / MapLibre / Kakao Map — 비교와 기각 이유는 [ADR-0003](../adr/0003-frontend-nextjs-react19.md)이 소유한다.

## 라이선스

- 이용 조건(로고 표기, 재배포 제한 등)은 네이버 클라우드 이용약관이 정본이다. 새 사용 방식을
  넣기 전에 약관을 확인한다.
