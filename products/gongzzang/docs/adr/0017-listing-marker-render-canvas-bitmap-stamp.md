# ADR-0017: 매물 마커 렌더링 — Naver Marker + Canvas content + BitmapStampCache (단일 렌더 박자)

| | |
|---|---|
| 작성일 | 2026-05-06 |
| 상태 | Accepted |
| 결정자 | 사용자 |
| 컨텍스트 | SP9 폴리곤 base layer 채택 ([ADR 0016](./0016-medallion-base-layer-postgis-silver-pmtiles-gold.md)) 직후 — 같은 지도 위에 표시될 매물/실거래/산단/광고 마커의 렌더 방식을 박제 |

## 컨텍스트

ADR 0016 이 폴리곤을 PMTiles 로 mapbox-gl 의 같은 WebGL 캔버스 안에 그리기로 확정했음. 이 캔버스 위에 동시에 살아있을 마커 종류:

- 매물 핀 (Listing — SP6-ii 진행 중)
- 실거래가 (RealTransaction — Phase 2+)
- 산업단지 라벨 (IndustrialComplex — Phase 2+)
- 매물 광고 (MapAdvertisement — Phase 2+)

검토한 렌더링 구조는 두 종류입니다. 하나는 Canvas bitmap을 cache하고 Naver Marker
인스턴스를 재사용하는 단일 경로이고, 다른 하나는 GL Symbol, Canvas overlay, DOM marker를
각각 관리하는 혼합 경로입니다. 혼합 경로는 표현력은 높지만 lifecycle, state, animation
clock을 세 경로에서 동기화해야 합니다.

따라서 선택 기준은 다른 checkout의 관찰 결과가 아니라 이 저장소에서 검증 가능한 불변식입니다.
같은 의미의 marker는 하나의 renderer와 store가 소유하고, frame budget과 marker churn은
재현 가능한 성능 테스트로 검증해야 합니다.

## 결정

매물 및 같은 지도 위 모든 마커를 다음 단일 패턴으로 렌더한다:

> **Naver Marker (1개 인스턴스/마커)**
> └ icon content = `<div>` 컨테이너 안 `<canvas>`
> └ canvas 그림은 `BitmapStampCache` 에서 미리 구운 비트맵을 `drawImage` 로 stamp

핵심 원칙:

1. **렌더 박자는 한 갈래** — mapbox-gl WebGL 캔버스 (베이스 + 폴리곤) + Naver Marker (Canvas content 비트맵 stamp) 둘 뿐. GL Symbol Layer 안 씀, DOM-only 마커 안 씀
2. **마커 비트맵은 캐시** — 같은 type/state 의 마커는 한 번만 그리고 N번 stamp
3. **마커 인스턴스는 풀링** — `MarkerManager` 가 type 별 풀 보유, 위치/내용 hash 비교 후 변경분만 갱신
4. **마커 도메인 분리는 "데이터 차원" 에서만** — 매물/실거래/산단/광고가 *컴포넌트로 분리되지 않음*. 단일 `MarkersLayer` 가 모든 도메인의 마커 데이터를 받아 한 번에 그림
5. **상태는 단일 store** — 마커 상태/선택/호버는 모두 한 Zustand store
6. **개별 마커 애니메이션은 CSS transform 으로** — GL pulse ring / GL hover 같은 GL-side 애니 안 씀. CSS `transform: scale()` + `opacity` 만

## 대안

| 안 | 평가 |
|---|---|
| **A. 본 결정 — Naver Marker + Canvas + BitmapStampCache** | ✅ 한 lifecycle, bitmap reuse, 명시적 pooling. 성능 예산은 저장소 benchmark로 증명 |
| B. GL Symbol Layer (mapbox-gl 안에 마커도 직접 추가) | 🟡 높은 밀도에 유리할 수 있으나 SDK 호환성과 텍스트 품질을 별도 검증해야 함 |
| C. DOM-only 마커 (`<div>` 위치 absolute) | ❌ marker 수에 따라 layout/reflow 비용이 증가하고 bitmap cache 이점을 쓰기 어려움 |
| D. 혼합 (GL + Canvas + DOM) | ❌ 여러 lifecycle과 animation clock을 동기화하는 운영 부채가 생김 |
| E. Naver SDK 의 `clustering` 서브모듈에 의존 | 🟡 단순 클러스터링은 가능하나 마커 디자인 자유도 낮음 — 보조 도구로만 |

## 결과

### 긍정
- 시각: 폴리곤(GL) 과 마커(Canvas stamp) 가 같은 mapbox-gl frame timing 안에서 동기화 → 드래그/줌 부드러움 보장
- 성능: 동일 bitmap의 중복 rasterization을 피하고 성능 예산을 자동 측정할 수 있음
- 단순성: 마커 컴포넌트 1개 (`MarkersLayer`) — 도메인 추가는 데이터 차원 추가일 뿐, 새 컴포넌트 마운트 아님
- 호환: SP9 폴리곤 PMTiles 경로와 marker lifecycle이 분리됨

### 부정
- 거대 마커 (광고 카드 등) 가 cache key 폭증을 유발할 수 있음 → `BitmapStampCache.maxEntries` 에 측정 기반 LRU 제한 필요
- GL Symbol 의 회전 동기화 같은 효과는 포기 (3D 회전 시 마커가 캔버스 평면에 고정 — 의도된 trade-off)
- 마커 hit-test 는 Naver Marker 의 click 이벤트에 의존 — 더 정교한 hit area 는 별도 구현 필요

### 영향 영역
- `apps/web/components/map/` (신규 폴더 — 현재 `components/listings/listing-map.tsx` 의 단일 파일은 SP9 프론트 T 에서 흡수 후 삭제)
  - `MarkersLayer.tsx` — 단일 마커 레이어 컴포넌트
  - `BitmapStampCache.ts` — bitmap cache
  - `MarkerManager.ts` — 풀링/생명주기
  - `marker-renderers/` — type 별 canvas 그리기 함수 (`drawListingPin`, `drawRealTransactionDot`, …)
- `apps/web/lib/naver-maps.ts` — 변경 없음 (SDK loader 그대로)
- `apps/web/stores/listings.ts` 외 단일 map store 통합 검토

### 도입하지 않을 혼합-renderer machinery
- GL Symbol Layer (`useGLMarkerLayer`, `useGLSymbolLayer`)
- GL 애니메이션 오케스트레이터 (`glFadeAnimation`, `glHoverAnimation`, `glPulseRingSharedLayer`, `glMarkerAnimationOrchestrator`)
- 도메인별 마커 컴포넌트 분리 (`features/sale-property/`, `features/court-auction/` 식 마커 폭증)
- store 다중화 (`useMapStore` + `useNewMapStore` + `useUnifiedMapStore`)
- `EventBus` (충돌 실제로 발생할 때 별도 ADR 로 도입)

## 재검토 트리거

- 실제 marker density에서 frame budget을 지속적으로 넘김 → GL Symbol Layer (대안 B) 부분 도입 ADR
- 광고 마커가 측정된 cache miss budget을 지속적으로 넘김 → 광고만 별도 렌더 경로 ADR
- 마커 클릭과 폴리곤 클릭이 같은 좌표에서 충돌 → 우선순위 event coordination ADR
- 3D 모드 도입 (tilt/pitch) → 마커 GL Symbol 전환 재평가

## 참조

- → [ADR 0013](./0013-listing-search-naver-maps.md) (Naver Maps SDK 채택)
- → [ADR 0016](./0016-medallion-base-layer-postgis-silver-pmtiles-gold.md) (PMTiles 폴리곤 base layer)
- 구현 근거는 이 저장소의 지도 런타임과 성능 검증으로만 유지한다. 외부 로컬 체크아웃은
  계약 근거가 아니다.
- AGENTS.md § 0 SSS 7 기둥 — 일관성(같은 도메인 같은 패턴), 명확성(렌더 박자 단일화)
