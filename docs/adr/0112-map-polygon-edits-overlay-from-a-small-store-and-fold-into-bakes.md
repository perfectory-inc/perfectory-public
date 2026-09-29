# ADR 0112: 지도 폴리곤 편집은 작은 저장소의 오버레이로 즉시 보이고, 주기 굽기가 타일에 접어 넣는다 — PostGIS 예비판은 없앤다

- Status: Accepted
- Date: 2026-09-28
- Supersedes: [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md) 의
  2번(브라우저 조합 금지), 4번 중 "PostGIS warm" 부분, 7번(반영 지연 목표), 10번(Martin·PostGIS
  렌더), 12번 D·E 단계; [ADR-0110](./0110-serve-all-map-tiles-from-r2-static-pmtiles.md) 의
  "PostGIS 는 warm delta 로 축소" 방향; FP-ADR-0004
  ([`0004-static-vector-tile-runtime-contract.md`](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md))
  의 dynamic fallback 과 "feature 숨김 전송 없음" 조항

## Context

ADR-0111 A 단계로 필지·행정경계·산업단지 세 레이어가 R2 정적 PMTiles 에서 Cloudflare Worker
(`foundation-tile-gateway`)로 서빙되기 시작했다(2026-09-28, PR #231–#238). 그런데 서빙과 무관한
것들이 그대로 남아 있다.

**실측(2026-09-28):**

- 운영 manifest 의 세 unit 은 `active_kind = static_pmtiles` 이지만 `fallback_kind =
  dynamic_postgis` 를 달고 있다. 그 예비판을 위해 `serving_postgis` 에 전국 필지 도형 전량 사본과
  누적 load 가 남아 DB 가 약 130GB 이고, 이를 읽는 Martin 인스턴스 둘이 상시 떠 있다.
- 정적 기본판 굽기 자체가 PostGIS 를 오븐으로 쓴다: 세 발행 명령
  (`*_boundary_static_release_publish.rs`)이 `serving_postgis.*_current` 뷰를 `martin-cp` 로 굽는다.
  마커 앵커 재구축도 `serving_postgis.parcel_boundary_mirror` 를 읽는다. 즉 **R2 원천 →
  서버 PostGIS 적재 → 굽기 → R2** 의 왕복이다.

**사장 결정(2026-09-28):**

1. 서빙은 R2 + Cloudflare Worker 다. **R2 가 동작하지 않으면 그것이 장애이고, 예비판으로 돈을
   쓰지 않는다.** dynamic fallback 과 그것을 위한 전량 사본은 필요 없다.
2. 관리자가 폴리곤을 고치거나 지우면 **손님도 즉시** 봐야 한다. 매 수정마다 타일을 굽지 않는다.
   R2 로 확정 반영되기 전의 수정분만 **작은 DB** 가 들고, 구워진 타일에서는 해당 폴리곤을
   툼스톤으로 가리고 작은 DB 의 새 폴리곤을 겹쳐 보여 준다.
3. 대상은 필지만이 아니라 폴리곤 레이어 전부(필지·산업단지·행정경계)다. 구현은 산업단지부터.
4. Martin 은 쓰지 않는다. 나중에 실시간 대량 레이어가 생기면 그 레이어만 별도 unit 으로 쓴다.

이 결정은 ADR-0111 이 유지한 FP-ADR-0004 의 전제("편집은 드물고, 브라우저에서 두 원천을 겹치지
않는다")를 바꾼다. 그 금지는 **같은 unit 을 static 과 dynamic 두 완결 원천으로 동시에 보여 줄 때**
어느 쪽이 참인지 흐려지는 것을 막으려던 것이다. 여기서 겹치는 것은 완결 원천 둘이 아니라
**불변 기본판 + 아직 접히지 않은 소수의 편집 목록**이고, 편집 목록은 기본판 release 에 묶인
순번(`change_seq`)으로 어디까지 접혔는지 정확히 말할 수 있다. 이것이 Mapbox·Felt 류가 편집 즉시
반영에 쓰는 "타일 + 편집 레이어" 구조다.

## Decision

1. **세 폴리곤 unit 의 공개 서빙은 R2 기본판(+패치) 하나다.** unit 과 feature id 는 ADR-0111 3번
   그대로다: 필지 `pnu`, 산업단지 `complex_id`, 행정경계 `administrative_unit_id`. 타일은
   `foundation-tile-gateway` Worker 가 R2 바인딩으로만 읽는다(ADR-0111 6번 유지). **예비판이
   없다**: manifest 의 `fallback_kind`·`fallback_release_id` 를 세 unit 에서 없애고, Worker·R2 가
   응답하지 못하면 그것은 장애로 경보한다(대체 경로로 떨어지지 않는다).

2. **확정 전 편집은 작은 편집 저장소가 든다.** 저장소는 **Cloudflare D1** 이다(서빙과 같은 곳,
   상시 서버 없음). 한 행 = `(unit, base_release_id, change_seq, feature_id, op ∈ {upsert, delete},
   geometry(GeoJSON, EPSG:4326, upsert 만), properties, editor, edited_at)`. 행은 append-only 다 —
   같은 feature 를 다시 고치면 새 `change_seq` 행이 쌓이고, 오버레이는 feature 별 최신 행만 쓴다.
   저장소가 들고 있는 것은 **아직 기본판에 접히지 않은 행뿐**이다.

3. **저장 시점에 검증한다.** 편집 쓰기 API 는 저장 전에 거부한다: 도형 유효성(자기교차·빈 도형),
   좌표계와 국토 범위, 꼭짓점 상한, `delete`·`upsert`(수정) 대상 feature 의 존재, 그 unit 의 속성
   목록. 거부된 편집은 저장소에 들어가지 않으므로 오버레이도, 굽기도 잘못된 도형을 보지 않는다.

4. **손님 화면 = 기본판 − 툼스톤 + 새 폴리곤.** 오버레이 API(`GET /overlay/{unit}`)는 현재
   기본판에 아직 접히지 않은 편집의 feature 별 최신 상태를 돌려준다: 가릴 id 목록(upsert·delete
   모두)과 새 폴리곤(upsert 만, GeoJSON). 웹 지도는
   - 기본판 레이어에 `feature_id` 가 가릴 목록에 있으면 그리지 않는 필터를 걸고(= **feature
     툼스톤**; 타일 바이트는 건드리지 않는다),
   - 같은 스타일의 GeoJSON 레이어로 새 폴리곤을 위에 그린다.

   새로 생긴 feature 는 가릴 대상 없이 그려지기만 하고, 삭제는 가리기만 한다. 오버레이 API 는
   타일 Worker 와 별개 경로이며, 타일 경로는 여전히 DB 를 읽지 않는다.

5. **반영 지연.** 관리자 본인 화면은 저장 응답 직후 오버레이를 다시 받는다. 손님 화면은 지도
   로드 시와 주기 조회(초기값 30초, 측정 후 조정)로 받는다. 오버레이 응답은 엣지 캐시하지 않거나
   편집 순번을 키로 캐시한다. 목표: 손님 반영 ≤ 조회 주기.

6. **오버레이 장애는 기본판으로 내려앉는다.** 오버레이 API 가 실패하면 웹은 기본판만 그린다
   (편집 직전 상태가 보일 뿐 지도는 나온다). 이것은 예비판이 아니라 **보조 레이어의 부재**다 —
   추가 인프라가 없다. 반대로 타일 Worker·R2 장애는 1번대로 장애다.

7. **주기 굽기가 편집을 타일에 접어 넣는다 = 툼스톤 GC.** 굽기 잡은
   (가) 편집 저장소의 미접힘 행을 레이크하우스의 append-only 편집 원장에 먼저 기록·재확인하고,
   (나) 원천 + 편집을 반영한 타일을 R2 에서 만들어 새 기본판 release 또는 ADR-0111 의 패치 세대로
   발행·승격하고(ADR-0111 5·8번 게이트 유지),
   (다) manifest 에 그 발행이 포함한 `folded_through_change_seq` 를 기록한 뒤,
   (라) 그 순번 이하의 행을 편집 저장소에서 은퇴시킨다.
   오버레이 API 는 현재 manifest 의 `folded_through_change_seq` 보다 큰 행만 돌려주므로, 승격과
   은퇴 사이 어느 순간에도 편집이 두 번 그려지거나 사라지지 않는다. 편집의 이력 정본은
   레이크하우스 원장이고, 편집 저장소는 미접힘 구간의 작업 사본이다.

8. **굽기 방식은 unit 규모로 고른다.** 작은 unit(산업단지 약 1,400·행정경계 약 5,000 feature)은
   매 굽기에서 전국 기본판을 새로 굽는다 — 수 분이고 패치 병합의 위험이 없다. 필지는 ADR-0111
   3번의 바뀐 타일 패치로 접고, 패치가 기준을 넘으면 전국 재굽기로 압축한다(ADR-0111 9번 유지).
   굽기 주기 초기값은 하루 1회이며, 편집 저장소 행 수가 상한(초기값 unit 당 5,000)을 넘으면
   주기와 무관하게 굽는다 — 오버레이가 클라이언트에 무거워지기 전에 접는다.

9. **오븐은 PostGIS 가 아니라 R2 원천을 읽는 타일러다.** 기본판·패치는 R2 Silver 원천(+레이크하우스
   편집 원장)을 읽어 **tippecanoe** 로 굽는다. `martin-cp` 와 `ST_AsMVT` 는 굽기에서 빠진다.
   새 오븐의 첫 발행은 현재 기본판과 대표 타일을 디코드 비교(feature id 집합·속성)해 같음을 보인
   뒤에만 승격한다. 마커 앵커 재구축도 `parcel_boundary_mirror` 대신 R2 원천을 읽는다.

10. **PostGIS 전량 사본과 Martin 을 걷어낸다.** 9번이 세 unit 모두 운영에서 확인되고 1번의
    fallback 이 manifest 에서 빠진 뒤, `serving_postgis` 의 경계 도형 표·누적 load·미러와 Martin
    인스턴스 둘을 내린다. 표 삭제와 서비스 중지는 되돌리기 어려우므로 실행 시점에 사장 확인을
    받는다. 옛 기본판 release 들은 R2 에 불변으로 남으므로 되돌리기는 manifest 포인터 이동이다.

11. **Martin 은 실시간 대량 레이어 전용 별도 unit 으로만 허용한다.** 편집 즉시 반영은 2–7번으로
    해결하므로 Martin 이 필요 없다. 초당 갱신되는 대량 데이터처럼 굽기로 따라갈 수 없는 레이어가
    생기면 그 레이어만 자기 unit 의 `dynamic` 원천으로 두며, 이 ADR 의 세 폴리곤 unit 의 예비판이
    되지 않는다.

12. **단계.** 각 단계는 별도 PR 이고 앞 단계가 운영에서 확인된 뒤 다음으로 간다.
    - **1. 산업단지.** 편집 저장소·쓰기 API(검증)·오버레이 API, 웹 필터+GeoJSON 레이어,
      tippecanoe 전량 굽기와 접기(7번), fallback 제거.
    - **2. 행정경계.** 1 과 같은 틀, unit 만 바꾼다.
    - **3. 필지.** 패치 접기(ADR-0111 C 단계를 tippecanoe 오븐으로), 앵커 재구축 이전, fallback
      제거.
    - **4. 정리.** 10번(PostGIS 경계 표·Martin 제거, 사장 확인 후).

## Consequences

- 손님은 관리자 저장 후 조회 주기 안에 새 폴리곤을 본다. 타일은 편집마다 굽지 않고 주기적으로만
  굽는다. ADR-0111 의 "수정마다 패치 빌드" 경로는 필지 접기 수단으로만 남는다.
- 상시 떠 있던 예비 DB 사본(약 130GB)과 Martin 둘이 사라진다. 대신 R2 가 단일 장애 지점이며,
  이는 사장이 선택한 조건이다. 옛 release 가 R2 에 남아 되돌리기는 무료다.
- 브라우저가 두 원천(기본판 + 오버레이)을 조합한다. 위험은 둘이다: (가) 오버레이 도형과 기본판
  도형의 렌더 차이(단순화·클리핑) — 편집된 feature 만 해당하고 다음 굽기에서 사라진다,
  (나) 오버레이가 커지는 것 — 8번의 상한 굽기로 막는다.
- 새로 짜는 것: D1 스키마·쓰기/오버레이 API, 저장 시 도형 검증, 웹 필터·GeoJSON 레이어,
  tippecanoe 오븐, 레이크하우스 편집 원장, `folded_through_change_seq` manifest 칸.
- 편집 쓰기 API 의 인증·권한(관리자만)은 신원 플랫폼의 기존 보호 API 틀을 쓴다. 세부는 1단계 PR
  에서 정한다.
- ADR-0111 과 FP-ADR-0004 에 대체 각주를 단다.

---

> **개정 각주(2026-09-28, 1단계 구현 중 정한 세부):**
> ① 7번 (가)의 레이크하우스 편집 원장은 `silver.map_edit_ledger`(append-only, 같은 `change_seq` 는
> 한 번만)이고, 굽기의 입력은 원천을 직접 읽지 않고 **`gold.industrial_complex_boundary_served`** 스냅숏이다 —
> 현재 공식 Silver 경계에 원장의 **모든** 편집을 순서대로 적용한 결과. 접기가 편집 저장소를 비우므로
> 원장만이 옛 편집을 기억한다. 이 Gold 스냅숏이 release 의 canonical snapshot 이 된다.
> ② Spark 이미지에 투영 라이브러리가 없으므로(ADR-0042) 편집 도형은 `export-map-edit-handoff` 가
> EPSG:4326 → 5186 WKB 로 바꿔 넘긴다. 좌표계는 원천과 같은 5186 하나다.
> ③ 카탈로그는 새 build kind `lakehouse_bake` 로 이 굽기를 받는다: 입력은 active 검증 정적 release,
> 출력은 Gold 스냅숏 위의 새 data revision(입력과 같은 수집 원천에 묶임), 승격 후 fallback 없음.
> ④ 3번의 "delete 대상 존재 확인"은 저장 경로가 아니라 Gold 빌드가 한다(`deletes_of_absent_features`
> 로 집계) — 존재 판정 원천이 곧 걷어낼 `serving_postgis` 뿐이기 때문이다.

> **Revision (2026-09-29):** 행정경계 유닛의 id 규칙 `uuid5("scope:legal-dong:" + 현재 코드)` 는 코드가 바뀌면
> id 가 바뀌어 ADR-0103 1항을 어긴다. [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md)
> 9항이 "승계 사슬의 가장 이른 코드" 로 바꾼다. 개편 전 7월 스냅숏은 두 규칙의 결과가 같다.
