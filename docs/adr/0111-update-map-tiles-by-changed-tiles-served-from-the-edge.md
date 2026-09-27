# ADR 0111: 지도 타일은 전국 기본판 위에 바뀐 타일만 갈아 끼우고, 엣지 Worker 가 서빙한다

- Status: Accepted
- Date: 2026-09-27
- Supersedes: [ADR-0110](./0110-serve-all-map-tiles-from-r2-static-pmtiles.md) 4·5단계,
  [ADR-0006](./0006-object-storage-first-serving.md) 의 지도 타일 두 행과 "타일 제공 엔진" 절,
  FP-ADR-0004([`0004-static-vector-tile-runtime-contract.md`](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md))
  의 "완전 source 교체만 허용·tombstone/overlay 금지" 조항

## Context

사장 지시(2026-09-27): "지도는 전부 R2 에서, DB 에는 R2 로 확정되기 전 수정분만. 방식은
Mapbox·OSM 처럼." ADR-0110 1~3단계로 필지·행정경계·산업단지 세 레이어의 전국 PMTiles 가
R2 에 올라가 있다(릴리스 d72f5ca3·9694ab53·62271990). 남은 문제는 **바뀐 데이터를 어떻게
지도에 반영하느냐**다.

**실측(2026-09-27, 저장소 전수조사 + 원본 대조):**

- 지도 경로의 갱신 방식은 **전량 교체 하나뿐**이다. 새 리비전을 통째로 적재(필지 3,986만 행)
  → 새 projection load → 포인터 CAS. 정적 PMTiles 도 통째로 다시 굽는다(필지 약 2.5시간,
  타일 약 267만 장). 삭제는 "새 리비전에 없음"으로만 표현되고, 옛 load 는 지워지지 않고 쌓여
  `serving_postgis` 가 112GB 다.
- foundation 코드·마이그레이션에 tombstone 은 **0건**이다. FP-ADR-0004 가 feature
  tombstone·overlay·subtraction 을 금지했다. ADR-0108 과 ADR-0110 의 "tombstone 포함" 서술은
  존재하지 않는 기능을 전제한 오기다.
- [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md) 의 변경분 도출 잡
  (`parcel_panel_delta.py`)은 있으나 **아무도 결과를 읽지 않는다.** 삭제 PNU 목록의 소비자가
  없어 삭제된 필지의 상세 JSON 이 다음 전량 굽기 전까지 200 으로 나간다.
- 마커 앵커 재구축은 원천에서 사라진 PNU 의 옛 앵커를 끄지 않는다.
- 공개 타일 서빙 경로가 없다. `martin.perfectory.io` 는 521, runtime manifest V2 는 꺼져 있고,
  세 정적 릴리스의 `tiles_url_template` 은 `http://127.0.0.1:3111` 로 기록됐다(PR #235 가
  이 주소를 발행 단계에서 거부하도록 고쳤다).

**벤치마크(공개 자료):**

- **Mapbox Tiling Service** — 바뀐 데이터만 다시 타일링하는 incremental update. feature id 가
  필수이고, 변경이 15% 를 넘으면 전체 재타일링을 권한다.
  ([문서](https://docs.mapbox.com/mapbox-tiling-service/guides/incremental-updates/))
- **OpenStreetMap** — 분 단위 편집을 반영하려고 PostGIS 에서 바뀐 타일만 하나씩 다시 만드는
  Tilekiln 을 만들었다. "타일은 하나씩 바뀌므로 하나씩 만들어야 한다."
  ([pnorman](https://www.openstreetmap.org/user/pnorman/diary/403600))
- **Felt** — 단일 타일 서버(mbtileserver)가 먼 지역에서 느려 PMTiles + CDN 으로 옮겼다.
  ([Felt](https://felt.com/blog/upload-anything))
- **Protomaps / Washington Post** — PMTiles 를 비공개 버킷에 두고 Cloudflare Worker 또는
  Lambda+CloudFront 가 Z/X/Y 를 범위 읽기로 바꿔 서빙한다. 상시 타일 서버가 없다.
  ([Protomaps](https://protomaps.com/blog/serverless-self-hosted-maps/))

공통 구조: **대량은 미리 구운 파일을 CDN 으로, 변경은 바뀐 타일만 다시 만들어 저장, 요청
시점 DB 렌더는 대량 서빙에 쓰지 않는다.**

## Decision

1. **한 레이어의 서빙 상태 = 기본판 1개 + 패치 세대 1개.** 기본판(base)은 지금의 전국
   PMTiles 정적 릴리스다(ADR-0053 경로 그대로, 불변). 패치 세대(patch generation)는 그
   기본판 이후 바뀐 타일 낱장의 집합이다. 한 타일 자리에 대한 응답은 셋 중 하나다: 패치가
   있으면 패치, "빈 타일" 표시가 있으면 빈 응답, 둘 다 없으면 기본판. FP-ADR-0004 의
   `DynamicPostgis XOR StaticPmtiles` 는 `(base release, patch generation)` 쌍으로 대체된다.

2. **조합은 타일 단위로만, 서버에서만 한다.** 브라우저·Martin 에서 두 원천을 겹치거나
   feature 를 숨기는 일은 계속 금지다(FP-ADR-0004 의 이 부분은 유지). 브라우저는 언제나 완결된
   타일 한 장을 받는다.

3. **패치 타일은 빌드 시점 feature 병합으로 만든다.** 대상 타일의 현재 바이트(최신 패치 또는
   기본판)를 디코드해, 바뀐 feature 를 그 레이어의 `feature_id_property`(`pnu`,
   `administrative_unit_id`, `complex_id`)로 빼고, 새 도형을 렌더해 넣는다. 새 도형 렌더는
   기본판과 같은 파라미터(extent·buffer·clip·속성 목록)여야 하며 그 동일성은 시험으로
   고정한다. 영향 타일은 **옛 도형과 새 도형의 bbox 합집합**이 걸치는 그 레이어 줌 범위의
   모든 타일이다(옛 도형을 빼야 지워진다). 병합 결과 feature 가 0개면 객체를 만들지 않고
   색인에 "빈 타일"로 적는다 — 이것이 **타일 단위 툼스톤**이며, 없으면 Worker 가 기본판의 옛
   타일로 떨어진다.

4. **변경 목록은 feature 단위 툼스톤을 포함하되 서버 내부 전용이다.** 한 행 =
   `(unit, feature_id, op ∈ {upsert, delete}, 새 도형, 옛 bbox, 출처, change_seq)`. 출처는
   원천 리비전 또는 (후속 ADR 의) 승인 편집이다. 정본은 레이크하우스의 append-only 표이고,
   PostGIS 에는 **아직 기본판에 합쳐지지 않은 행만** warm 으로 둔다. 원천 리비전의 변경은
   연속 스냅숏의 도형 체크섬 비교로 도출한다. ADR-0099 의 `row_digest` 델타와 **한 잡, 한
   목록**으로 합쳐 상세 JSON·마커 앵커·타일이 같은 변경 목록을 소비한다(삭제 PNU 의 상세
   JSON 404 전환과 앵커 비활성화가 이 목록의 소비자다).

5. **R2 배치는 불변·append-only 다.** 패치 객체는
   `gold/vector-tiles/patches/{unit}/{base_release_id}/{generation}/{z}/{x}/{y}.mvt`
   create-only. 세대 색인 `.../{generation}/index.json` 은 **누적형**이다(이전 세대의 항목을
   포함해 한 번 읽으면 그 세대가 완결된다). 현재 세대는 Catalog 의 runtime manifest 에
   unit 별 `patch_generation` 으로 싣고 기존 CAS 로 옮긴다. 덮어쓰기·삭제 경로는 없다.

6. **공개 서빙은 Cloudflare Worker 한 개다(`foundation-tile-gateway`).** 기존 parcel·building
   gateway 와 같은 틀이고, R2 는 바인딩으로 읽는다(공개 버킷 금지, 서버에 키 상주 없음). 공개
   호스트는 `config/r2-connections.contract.json` 의 `tile_derivatives` 에 한 곳으로 선언한다.
   타일 URL 은 `https://{host}/{unit}-{base_release_id}/{z}/{x}/{y}` 다 — 이미 release 행의
   CHECK 와 웹 검증이 요구하는 `/{martin_source_id}/{z}/{x}/{y}` 형태 그대로다. 패치 세대는 URL
   에 넣지 않는다 — 기본판 주소는 불변 계약이다(ADR-0037). Worker 는 DB 를 읽지 않는다.

7. **캐시와 반영 지연.** 타일 응답은 브라우저 `max-age=60` 과 엣지 캐시를 쓰고, 패치 세대를
   승격할 때 영향 타일 URL 을 엣지에서 purge 한다. 목표 반영 지연은 **패치 빌드 시간 + 60초
   이내**다.

8. **패치 세대 승격 게이트(ADR-0053 의 정신).** 승격 전에 (가) 모든 패치 객체가 create-only 로
   존재하고 재해시가 일치하고, (나) 영향 타일 표본이 **처음부터 렌더한 결과와 디코드 비교로
   같고**(feature id 집합·속성), (다) 색인이 이전 세대를 빠짐없이 포함하고, (라) CAS 가
   통과해야 한다. 공개 URL 은 https 이고 내부 주소가 아니어야 한다(PR #235 에서 구현).

9. **압축(compaction)은 전국 재굽기다.** 패치 타일이 기준을 넘으면(초기값: 기본판 타일의 5%
   또는 7일, 측정 후 조정) 전국 기본판을 다시 굽고 새 base release 로 승격한다. 그 base 에
   포함된 `change_seq` 까지의 warm 행은 PostGIS 에서 은퇴한다(정본은 레이크하우스에 남는다).
   재굽기 검증은 "직전 base + 패치 = 새 base"를 영향 타일에서 디코드 비교로 확인한다.

10. **Martin 은 공개 서빙에서 빠진다.** 전국 기본판을 굽는 `martin-cp` 의 내부 도구로만 남는다.
    패치 렌더는 warm 행에 대한 PostGIS `ST_AsMVT` 로 한다. 요청 시점 DB 렌더 경로(dynamic
    공개 서빙)는 이 ADR 의 단계가 끝나면 없어진다.

11. **단계.** 각 단계는 별도 PR 이고, 앞 단계가 운영에서 확인된 뒤 다음으로 간다.
    - **A. 공개.** tile gateway Worker, 공개 호스트 계약, 세 레이어를 공개 URL 로 다시 발행
      (루프백으로 기록된 릴리스의 교정은 새 발행이다 — ADR-0037), runtime manifest V2 가동,
      공짱 웹 CSP.
    - **B. 변경 목록.** 원천 리비전 → 변경 목록(레이크하우스) 도출, ADR-0099 잡 연결, 삭제
      PNU 상세 JSON 404, 앵커 삭제 누락 수정.
    - **C. 패치.** 패치 빌더·세대 색인·Worker 패치 조회·purge·승격 게이트.
    - **D. 압축과 축소.** 압축 잡, `serving_postgis` 의 누적 load·전량 미러를 warm 행만 남기고
      축소.
    - **E. 사람 편집.** 필지 도형 편집·삭제 API 는 별도 ADR 로 정한다.

## Consequences

- 수정이 수 분 안에 지도에 보이면서도, 브라우저는 항상 미리 만든 타일만 받는다. DB 가 느리거나
  멈춰도 지도는 나온다. 상시 타일 서버가 없어져 521 같은 단일 장애점이 사라진다.
- `serving_postgis` 112GB 의 대부분(누적 load·전량 미러)을 D 단계에서 걷어낸다. DB 에 남는 것은
  사장 지시대로 R2 확정 전 수정분뿐이다.
- 새로 짜는 것: MVT 디코드·병합·인코드(공식 `vector_tile.proto` 기반), 렌더 파라미터 동일성
  시험, 세대 색인, Worker 패치 조회, purge 호출. 가장 큰 위험은 **패치 렌더와 기본판 렌더의
  미세한 차이**이며, 게이트 (나)와 압축 검증이 이를 잡는다.
- 비용: Workers 유료 요금제와 요청량 과금, purge API 호출. 상시 서버 비용은 없어진다. 정확한
  요율과 purge 한도는 A 단계에서 계약 문서에 적는다.
- Cloudflare 종속이 늘지만 R2·gateway 3개가 이미 같은 곳에 있어 새 종속은 아니다. Worker
  코드는 작고, 같은 구조의 AWS(Lambda+CloudFront) 구현이 공개돼 있다.
- ADR-0108·ADR-0110 의 "tombstone" 서술은 이 ADR 의 4번(서버 내부 변경 목록)과 3번(타일 단위
  빈 타일 표시)으로 대체된다. 두 ADR 에 정정 각주를 단다.
