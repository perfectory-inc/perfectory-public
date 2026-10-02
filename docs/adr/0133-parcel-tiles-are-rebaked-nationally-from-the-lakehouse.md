# ADR 0133: 필지 타일은 레이크하우스에서 전국을 다시 굽는다

- Status: Accepted
- Date: 2026-10-02
- Amends: [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md) §8·§9·§12 3단계(필지),
  [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md) 9번의 필지 적용
- Builds on: [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §7

## Context

ADR-0112는 세 폴리곤 unit을 R2 기본판 하나로 서비스하고, 오븐이 PostGIS가 아니라 R2 원천을 읽게 했다.
산업단지·행정경계는 그렇게 운영 중이다. 필지는 아직 아니다. 2026-10-02 운영에서 잰 값:

- 필지의 활성 release는 `static_pmtiles`(5.1GB, z14–16)다. fallback은 `dynamic_postgis` release를 가리킨다.
  `serving_postgis.parcel_boundary_mirror`(62GB)와 `parcel_boundary_publication`(50GB)은 서비스에 쓰이지 않는다.
  필지 view는 0행이고, Martin의 필지 요청은 72시간 동안 0건이다. 그래도 새 필지판을 만드는 유일한 길은 이
  PostGIS 왕복이다. 그래서 두 표를 지우면 필지 지도가 굳는다.
- ADR-0112 §8은 필지를 ADR-0111의 바뀐 타일 패치로 접게 했다. 그런데 패치를 만드는 코드도, 패치를 서비스하는
  코드도 없다. 게이트웨이는 타일을 `immutable`로 캐시하는데, 같은 주소에서 타일이 바뀌는 패치와 함께 쓸 수 없다.
- 오븐 `bake-lakehouse-tiles`는 서빙본 전체를 메모리에 올리고, maxzoom 타일 전부를 도형까지 메모리에 해독한다.
  4천만 건에서는 동작하지 않는다.
- **서울 시험 굽기**(silver `vworldkr__parcel-202606`의 서울 898,741건, 운영과 같은 tippecanoe v2.79.0 옵션):
  - Trino 내보내기 128초, 355MB.
  - tippecanoe **16초**, 최대 메모리 **0.96GB**, 결과 86.6MB.
  - 단순 비례로 전국은 내보내기 약 94분(병렬로 단축 가능), 굽기 약 12분, 결과 약 3.8GB다.
- **운영판과 비교**: 서울 다섯 지점의 z14·15·16 타일 15장을 해독해 비교했다.
  - 운영판의 필지는 새 굽기에 **하나도 빠지지 않았다**.
  - 새 굽기에만 있는 필지(4–16%)는 표본 17건 모두 타일 경계 **바깥**의 버퍼 안에 있었다. 차이는 버퍼 폭 하나다.

## Decision

1. **필지는 패치 없이 매번 전국을 다시 굽는다.** 굽기는 위 측정 규모라서 하루 한 번 전국 재굽기로 충분하다.
   ADR-0111의 바뀐 타일 패치는 필지에 쓰지 않는다. 게이트웨이의 `immutable` 캐시는 그대로 둔다. 새 판은 새
   release 주소이므로 캐시가 엇갈리지 않는다. 전국 굽기 시간이 예산(아래 7번)을 넘는다는 측정이 나오면 그때
   패치를 다시 검토한다. 이 결정은 ADR-0112 §8의 필지 문장과 §12 3단계의 "패치 접기"를 대체한다.
2. **필지 서빙본은 Spark executor에서 만든다.** `parcel_boundary_served_gold.py`가 Silver의 `source_snapshot_id`
   하나를 읽는다. 지도 편집 원장을 적용하고 Gold 표를 쓴 뒤, 굽기 입력을 여러 조각(part)으로 create-only로
   내보낸다. driver로 행을 모으지 않는다. 요약은 `polygon_served_gold.v2`이고 조각마다 경로·행 수·SHA-256을
   적는다. 산업단지·행정경계의 v1 요약은 그대로 받는다.
3. **오븐은 unit을 가리지 않고 4천만 건 규모를 견딘다.**
   - 조각을 흘려 읽고, 작업 폴더와 tippecanoe 임시 폴더(`-t`)는 `/data`에 둔다.
   - 컨테이너마다 메모리 상한을 걸고, 그 값은 `tools/host-memory-budget.contract.json`에 등록한다.
   - 시작 전에 작업 디스크의 여유를 확인한다.
   - maxzoom 검사는 도형을 해독하지 않고 feature id만 흘려 센다(id당 고정 크기 hash).
4. **새 오븐의 첫 승격은 동등성 검사를 통과해야 한다**(ADR-0112 §9의 디코드 비교를 구체화한다).
   - 대상: 활성 release가 `lakehouse_bake`가 아닐 때 굽는 첫 판. 즉 지금의 필지.
   - 표본: 모든 시도 범위, 밀집 타일, 고정 무작위 집합의 z14·15·16 타일.
   - 통과 조건: (가) 운영판 타일의 feature id가 새 판의 같은 타일에 모두 있고, (나) 새 판에만 있는 id의 bbox는
     모두 타일 경계 밖이다. 어느 하나라도 어기면 승격을 거부하고 건수를 남긴다.
5. **새 Silver 판으로 굽는 필지 release는 그 판의 원천 기록을 가진다.** 지금 `lakehouse_bake`는 입력 release의
   `source_record_id`를 물려받게 묶여 있다. 그래서 새 Silver 판으로 굽는 판이 옛 원천을 가리키게 된다.
   필지 bake는 실제로 읽은 Silver 판의 원천 기록에 묶는다. 이때 같은 판에 대한 ADR-0113 §7 대조 검사
   (`parcel_matching_gate.py`)의 통과 판정을 증거로 함께 기록해야 한다. 통과 판정이 없으면 build를 시작하지
   않는다. 산업단지·행정경계의 기존 규칙은 바꾸지 않는다.
6. **필지 편집 오버레이는 이 ADR의 범위가 아니다.** 지금 필지를 편집하는 화면도 원천도 없다. 필지 굽기는 새
   Silver 판이 생기거나 강제할 때 돈다. 편집 저장소의 `parcels` unit은 편집 기능을 만드는 변경에서 정한다.
7. **예약과 예산.** Airflow 작업 `parcel_tile_bake`가 하루 한 번 돈다. 새 Silver 판이 없고 강제도 아니면 굽지
   않고 "할 일 없음"을 남긴다. 예산은 360분이다. 처음 전국을 실제로 구운 시간·메모리·디스크를 런북에 남긴다.
8. **마커 앵커는 R2에서 다시 만든다.** 필지를 구운 뒤 `rebuild-parcel-marker-anchors-streaming`(Silver 조각을
   읽음)을 돌린다. 미러를 읽는 `rebuild-parcel-marker-anchors` 명령과 HTTP 경로는 지운다.
9. **그다음에 ADR-0112 §10을 실행한다.** 필지 첫 lakehouse 판이 승격되어 fallback이 manifest에서 빠지고, 위
   1–8번이 운영에서 확인되면 그때 실행한다.
   - 지우는 것: `serving_postgis` 경계 표·view, PostGIS 게시·정적판 명령, Martin 둘.
   - 남기는 것: 이력 원장 `spatial_projection_load`·`parcel_boundary_mirror_rebuild_run`과, 옛 행이 쓰는 값
     `dynamic_postgis`. 새 행은 이 값을 쓰지 않는다.
   - 표 삭제와 Martin 중지는 실행 시점에 사장 확인을 받는다.

## Consequences

- PostGIS 필지 사본 112GB를 지울 수 있게 된다. 지우면 DB 백업이 130GB에서 약 18GB로 준다.
- 필지 지도가 레이크하우스 새 판을 하루 안에 따라간다. 이전에는 PostGIS 왕복 약 2.5시간을 손으로 돌려야 했다.
- 매일 전국 재굽기는 R2에 약 4GB 판을 하나씩 쌓는다. 옛 판 정리(보존 기간)는 release 보존 정책에서 정한다.
- 새로 만드는 것: 필지 서빙본 Spark 작업과 계약, 오븐의 흘려 읽기·`/data` 작업 폴더·메모리 상한·id 검사,
  동등성 검사, bake의 원천 기록 규칙 변경(마이그레이션), 예약 작업, 런북.
- 버퍼 폭 차이로 새 판의 타일은 경계 밖 이웃 필지를 조금 더 담는다. 화면에서는 타일 경계에서 잘리므로
  보이는 결과는 같다.
