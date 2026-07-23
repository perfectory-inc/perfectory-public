# Naver Maps API

## 개요

- 운영 기관: 네이버 클라우드 플랫폼
- 공식 사이트: https://www.ncloud.com/product/applicationService/maps
- 우리 사용: 지도 렌더링, 마커 표시, 지오코딩, 경로 계산

## 인증

- 네이버 클라우드 플랫폼 가입 → 애플리케이션 등록 → Client ID/Secret 발급
- 환경변수: `NAVER_MAPS_CLIENT_ID`, `NAVER_MAPS_CLIENT_SECRET`
- *서비스 URL* 등록 (도메인 제한)

## Quota·비용 경계

Quota와 단가는 공급자가 변경할 수 있는 외부 값이므로 이 문서에 숫자로 고정하지 않는다.
배포 전 공식 Naver Cloud 문서에서 현재 값을 확인하고, 환경별 승인 호출 예산과 경고
임계값은 비공개 운영 시스템에 기록한다. 예상 호출량은 활성 지도 세션과 기능별 실측
호출 수로 계산한다.

## 핵심 SDK

| 종류 | 용도 |
|------|------|
| Maps JavaScript API | 브라우저 지도 렌더링 |
| Maps API for Web Dynamic | 동적 지도 + 마커 |
| Maps API for Web Static | 정적 이미지 |
| Geocoding API | 주소 → 좌표 |
| Reverse Geocoding API | 좌표 → 주소 |
| Directions API | 경로 (자동차/대중교통) |

## 좌표계

- **EPSG:4326 (WGS84)** — 입출력 표준
- 국내 주소·POI 검색은 자동으로 한국 좌표 처리

## 요청 예시 (지오코딩)

```
GET https://maps.apigw.ntruss.com/map-geocode/v2/geocode?
  query=서울특별시 강남구 테헤란로 123

Headers:
  X-NCP-APIGW-API-KEY-ID: {NAVER_MAPS_CLIENT_ID}
  X-NCP-APIGW-API-KEY: {NAVER_MAPS_CLIENT_SECRET}
```

## 클라이언트 SDK 사용 (Next.js)

```tsx
// packages/map/src/naver-map.tsx
import Script from "next/script";

export function NaverMap({ children }: Props) {
  return (
    <>
      <Script
        strategy="beforeInteractive"
        src={`https://oapi.map.naver.com/openapi/v3/maps.js?ncpClientId=${process.env.NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID}`}
      />
      {/* map container */}
    </>
  );
}
```

`NEXT_PUBLIC_*` = 브라우저 노출. 도메인 제한으로 보호.

## 마커 렌더링

기본 마커의 feature 수가 성능 예산을 넘으면 기존 vector-tile/WebGL 경로를 사용한다.
렌더러 전환은 공개 문서의 임의 사용자 수나 phase가 아니라 지원 기기 벤치마크와
frame-time·memory 예산으로 결정한다.

## Circuit Breaker 정책

- timeout: 5초 (지도는 사용자 응답성 중요)
- retry: 0회 (지도 깨짐 방지)
- fallback: "지도를 불러올 수 없어요" 메시지

## 캐시 정책

| 종류 | TTL |
|------|-----|
| 지오코딩 결과 | 30일 (주소→좌표는 거의 안 바뀜) |
| 정적 지도 이미지 | CDN 7일 |
| 경로 계산 | 1시간 |

## 비용 산정

```text
monthly_calls = map_sessions * measured_calls_per_session
              + geocoding_calls + search_calls + directions_calls
monthly_cost = max(0, monthly_calls - current_included_quota) * current_unit_price
```

입력값, 공식 가격 출처, 승인 예산, 실제 사용량은 비공개 운영 기록에서 관리한다.
캐시 변경은 provider 약관과 정확성 요구사항을 지키면서 실측 호출 분포로 판단한다.

## 대안

- Mapbox GL JS: 글로벌, 한국 사용자 익숙도 낮음
- MapLibre (OSS): 무료, 그러나 한국 데이터 약함
- Kakao Map: 거의 동등, 교체 요구가 생기면 별도 ADR에서 비교

→ ADR-0003 참조.

## 라이선스

- 네이버 클라우드 이용약관
- 정적 지도 다운로드 후 재배포 금지 (실시간 호출만)
- "Naver Maps" 로고 표기 의무 (지도 위)

## 한국 사용자 친화 기능

- POI 풍부 (식당, 카페, 시설)
- 한국 주소 자동완성
- 도로명·지번 둘 다 검색 가능
- 한국어 음성 안내 (경로)
