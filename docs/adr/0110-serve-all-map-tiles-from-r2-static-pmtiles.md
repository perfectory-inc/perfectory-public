# ADR 0110: 지도 타일 전량을 R2 정적 PMTiles 로 서빙하고 PostGIS 는 warm delta 로 축소한다

- Status: Accepted
- Date: 2026-09-27

## Context

[ADR-0109](./0109-gongzzang-reads-parcel-and-building-detail-from-the-r2-edge.md) 로
필지 **상세**는 R2 엣지 서빙으로 넘어갔다. 그러나 지도 **타일**은 아직 R2 가 아니다.

**실측(2026-09-27):**
- 필지 폴리곤·행정경계·(전국)산업단지 타일 = **Dynamic** — Martin 이 `serving_postgis`
  (112GB, `parcel_boundary_mirror` 39,861,511행)를 실시간 렌더한다. 정본 geometry 는 R2
  Iceberg(silver)지만 **서빙 바이트는 PostGIS 에서 나온다.**
- **마커(PBF)만 R2 static** 파이프라인을 가진다.
- 필지·행정경계는 **정적 굽기 명령 자체가 없다**(전수조사: `*_static_release_publish` 는
  산업단지 하나뿐). 산업단지는 명령은 있으나([ADR-0053]) 전국 승격은 3필지 정확성
  슬라이스만 증명됐다(runbook `tiles-object-storage-first-slice`).
- 운영 실태: ai-server `martin-complex` 컨테이너가 **3주째 unhealthy**,
  `tiles/martin.perfectory.io` = **521**. 지도가 3주 죽어도 아무도 모른 것 자체가 dynamic
  PostGIS→Martin 경로의 취약성이다.
- Martin R2 정적 설정(`scripts/tiles/martin-static-production.yaml`)과 runtime manifest V2
  는 코드에 존재하나 **미가동**(`FOUNDATION_TILE_RUNTIME_MANIFEST_V2_ENABLED=false`, 엣지
  정책 `status=planned`).

[ADR-0006](./0006-object-storage-first-serving.md) 이 이미 "static basemap(필지·산단·
행정경계·건물) = R2 PMTiles + Martin + CDN; 새 승인 편집만 Dynamic PostGIS"를 목표로
정했고, `docs/architecture/single-source-spatial-publication.md` 는 이를 "승인된 방향,
구현 대기"로 명시한다. 이 ADR 은 그 실행이며, 사장 지시(2026-09-27) "지도까지 전부 R2 에서
하고, DB 에는 나중에 R2 로 확정될 수 있는 수정분만 취급한다"를 프로그램으로 고정한다.

## Decision

1. **지도 타일의 서빙 정본은 R2 정적 PMTiles 다.** 필지·행정경계·산업단지 폴리곤을 전국
   규모로 R2 에 굽고, Martin 은 R2(static)에서 서빙한다. Dynamic PostGIS 는 지우지 않고
   **"아직 R2 에 안 구워진 최근 승인 수정분(warm delta)"만** 남기는 층으로 축소한다.

2. **기존 기계장치를 확장한다(재발명 금지).** 산업단지 정적 굽기(ADR-0053: 활성 dynamic
   소스에서 빌드 → 두 아카이브 형식 검증 → R2 파생객체 1회 생성 → 전바이트 재해시 → Martin 이
   그 release-addressed 소스를 디코드함을 증명 → 기록·승격)의 `static_release_toolchain`·
   `VectorTileBuildLifecycle`·`PromoteTileLayerStatic`·R2 저장 계층은 unit-agnostic 이다.
   필지·행정경계 static publish 명령은 **unit key 와 소스 뷰만 다른 복제**로 만든다.

3. **레이어별 dynamic→static 전환은 ADR-0006 의 XOR 규칙과 ADR-0053 의 검증 순서를
   따른다.** 각 `(publication_unit, serving_generation)`은 Dynamic XOR Static 중 하나다.
   static 이 실제로 Martin 으로 디코드·서빙됨을 증명한 **뒤에만** runtime manifest 를 static
   으로 승격한다. **전환 전까지 dynamic 을 유지**해 지도가 깨지지 않게 한다(= 게이트).

4. **단계(작은 것부터, 위험 최소화):**
   - **1단계 — 산업단지 전국 static 승격.** 명령이 이미 있으니(ADR-0053) 전국 규모로
     굽고 검증·승격해 파이프라인 전체를 실증한다(가장 작음, 1,442). `martin-complex`
     unhealthy(521)를 static 전환으로 해소.
   - **2단계 — 행정경계 static.** publish 명령 신설 + 전국(약 5,067 유닛) 굽기·승격.
   - **3단계 — 필지 static.** publish 명령 신설 + 전국(3,986만 폴리곤) 굽기·승격. 최대
     규모이므로 [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md)(일
     변경분만 재굽기)와 결합해 증분 굽기로 간다.
   - **4단계 — Martin R2 배포 전환 + runtime manifest V2 가동.**
     `martin-static-production.yaml`(R2 pmtiles) 실배포, `..._MANIFEST_V2_ENABLED=true`,
     엣지 정책 활성, 521 origin 해소.
   - **5단계 — PostGIS warm delta 축소.** 전국 static 완성이 전제. 미러를 전량(39.86M)에서
     "아직 R2 에 안 구운 최근 수정분 + tombstone"만 남기고 축소한다.

5. **DB 최종 상태는 ADR-0108/0109 와 같다:** 서빙되는 대량 데이터는 R2, DB 는 (가) R2 확정
   전 warm 수정분, (나) 유저·매물(공짱), (다) 계속 쓰는 운영 상태(수집·발행 큐·원장·정규화
   승인)만.

## Consequences

- 지도 서빙이 R2 CDN 으로 단일화 → PostGIS 112GB 대폭 축소, Martin 실시간 렌더 취약성
  (3주 unhealthy) 제거, 지도 가용성이 CDN 수준으로 상승.
- 각 단계는 개별 ADR/PR. 특히 3단계(전국 필지 굽기)는 대규모 배치라 국가 규모 재굽기의
  알려진 함정(무음 미완성·덮어쓰기 충돌·spark 마운트·힙 부족)이 그대로 적용된다.
- 리스크: static 굽기 배치의 안정성 = 지도 신선도. Dynamic 을 게이트 통과까지 유지하므로
  전환 자체는 무중단. 일 재굽기 스케줄 상시화가 전제 안전망이다.
- 이 ADR 은 방향·단계·게이트를 고정한다. 각 단계의 실제 코드는 게이트 통과 후 별도 변경이다.
