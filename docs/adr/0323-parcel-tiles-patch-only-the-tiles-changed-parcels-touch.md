# ADR 0323: 필지 타일은 바뀐 필지가 닿는 타일만 패치로 굽고, 엣지가 기본판 위에 얹어 서빙한다

- Status: Accepted
- Date: 2026-10-29
- Implements: [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md) 12번 C 단계(패치)
- Amends: ADR-0111 3번(패치 타일 만드는 법), 4번(변경 목록의 출처), 5번(R2 배치·세대 색인), 7번(캐시와 purge)
- Related: [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md)(by-PNU 패치 세대),
  [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md)(전국 재굽기),
  [ADR-0251](./0251-a-first-parcel-release-on-a-new-edition-counts-parcels-that-left-it.md)(판 사이 PNU 이동 실측),
  [ADR-0262](./0262-parcel-tiles-are-baked-at-maxzoom-detail-14-and-only-subpixel-parcels-may-be-missing.md)(서브픽셀),
  ADR-0316 Part C(PR #897, 시도별 굽기 — 이 ADR 의 바로 앞 단계)

## Context

사장 원칙(2026-10-28): 모든 처리는 바뀐 것만 한다. 전량은 이유와 측정 비용이 있는 예외다.

필지 타일은 판마다 전국을 다시 굽는다. ADR-0133 §7 실측(2026-10-13): GDAL 60분, tippecanoe 109분, 판 5.55GB,
`/data` 최대 43.7GB. PR #897 은 시도별 archive 를 만들어 행이 그대로인 시도를 다시 쓰고 `tile-join` 으로 합친다.
그래도 남는 전량 작업은 다음과 같다.

| 남은 전량 | 크기 |
|---|---|
| 바뀐 필지가 하나라도 있는 시도는 통째로 다시 굽는다 | 판 사이 변경은 전국에 흩어진다. ADR-0251: 202606→202609 에 PNU 0.26% 가 들고 났고, 그 법정동은 7,819+4,973 개로 전국에 흩어져 있다 |
| `tile-join` 이 전국 판을 다시 쓴다 | 5.55GB 읽기+쓰기 |
| 새 release 를 통째로 올린다 | 5.55GB R2 업로드 |
| 새 release 는 새 URL 이다 | 엣지 캐시의 전국 타일이 한꺼번에 식는다 |

ADR-0111 은 이것을 "기본판 + 패치 세대"로 풀기로 했지만(Accepted) 만들지 않았다. 그 사이 같은 원리가 by-PNU
서빙에서 ADR-0141(manifest 하나만 가변, 패치는 create-only, CAS)과 ADR-0147(낱개 객체 대신 묶음)로 운영에
들어갔다.

변경 목록은 어디서 오는가(과제가 짚은 두 곳):

- **ADR-0312(PR #902) 의 `gold.<t>__changes`** 는 `gold_incremental` 레인의 커밋이 적는다.
  `gold.parcel_boundary_served` 는 판 전체를 `INSERT OVERWRITE` 하는 표라 그 레인이 아니다.
- **ADR-0311(PR #899) 의 Silver 변경 기록** 은 비교(compare) 레인의 커밋이다. `silver.parcel_boundaries` 는 판마다
  전국 한 벌을 `source_snapshot_id` 로 쌓는 표라 그 레인이 아니다.
- 그래서 둘 다 지금은 필지 경계의 변경을 적지 않는다. 판과 판의 차이는 비교로만 나온다. 비교는 ADR-0141 §3 의
  지문(fingerprint) 방식이 이미 정본이다. 오븐은 핸드오프를 어차피 처음부터 끝까지 읽으므로, 그 읽기에서 지문을
  만들면 추가 Spark 작업이 없다.

벤치마크:

- **OpenStreetMap Tilekiln**: 바뀐 타일을 DB 에서 **통째로** 다시 렌더한다. feature 를 타일 안에서 고치지 않는다.
  ([pnorman](https://www.openstreetmap.org/user/pnorman/diary/403600))
- **Mapbox Tiling Service incremental update**: feature id 로 바뀐 feature 를 찾고, 그것이 닿는 타일만 다시 만든다.
  변경이 15% 를 넘으면 전체 재타일링을 권한다.
  ([문서](https://docs.mapbox.com/mapbox-tiling-service/guides/incremental-updates/))
- **Protomaps PMTiles + Cloudflare Worker**: 한 archive 를 범위 읽기로 서빙한다. 디렉터리는 isolate 메모리에
  캐시한다. ([Protomaps](https://docs.protomaps.com/deploy/cloudflare))
- **Delta Lake / Iceberg 의 기본+델타 읽기**(merge-on-read): 불변 기본 파일 위에 작은 델타 파일을 얹는다. 델타가
  쌓이면 압축(compaction)한다. ADR-0111 §9 의 압축과 같은 구조다.

## Decision

1. **지문 색인.** 패치 대상 unit(계약 `vector_tile_gateway.patches.units`, 지금은 `parcels`)의 굽기는 핸드오프를
   한 번 흘리며 feature 마다 다음 셋을 적는다.
   - id 해시(16B);
   - 타일 내용 해시(id·속성·WKB 의 SHA-256 앞 8B);
   - 경위도 상자(f32 네 개, 바깥으로 올림).

   이 색인은 id 순으로 정렬해 파일로 둔다. 4천만 건에 1.6GB 다.

   - 파일 이름은 `<work_root>/<unit>-feature-index/<gold snapshot>.<bake inputs 해시>.idx` 다.
   - **그 스냅숏이 서빙된 뒤에만** create-only 로 남긴다. 전량 굽기는 승격 뒤에, 패치는 manifest 교체 뒤에 남긴다.
   - bake inputs 는 GDAL 이미지, tippecanoe 버전, CRS, tippecanoe 인자다. PR #897 의 시도 archive 이름과 같은
     문자열이다. 그래서 굽는 도구가 바뀌면 옛 색인을 찾지 못하고 전량(새 기본판)으로 간다.

2. **변경과 더러운 타일.** 서빙 중인 스냅숏의 색인과 새 색인을 병합 조인해 바뀐 feature 를 낸다.
   - 추가: id 가 새 색인에만 있다.
   - 변경: 내용 해시나 상자가 다르다.
   - 삭제: id 가 옛 색인에만 있다.

   더러운 타일은 레이어 줌 범위(필지 14–16)에서 **옛 상자와 새 상자** 어느 쪽이든 닿는 모든 타일이다
   (ADR-0111 §3 그대로). 상자는 tippecanoe 의 기본 버퍼 5px 에 1px 를 더해 넓힌다.

3. **패치 타일은 통째로 다시 렌더한다(ADR-0111 §3 개정).**
   - 더러운 타일의 minzoom 조상에 닿는 모든 서빙 feature(이웃)를 전량 굽기와 **같은** `ogr2ogr`·tippecanoe
     실행으로 굽는다. 같은 함수, 같은 인자, 같은 컨테이너 상한이다.
   - 이 결과에서 더러운 타일만 그대로 가져온다. 한 타일에 나올 수 있는 feature 는 모두 그 타일의 버퍼 안에 닿고,
     그것들은 모두 이웃에 들어 있다. 그래서 패치 타일은 "처음부터 렌더한 결과"다.
   - ADR-0111 §3 의 feature 단위 병합(디코드 → 빼기 → 새 도형 렌더 → 인코드)은 하지 않는다. tippecanoe 의
     단순화·공유 경계 처리를 Rust 로 다시 만들어야 하고, ADR-0111 이 스스로 꼽은 가장 큰 위험("패치 렌더와
     기본판 렌더의 미세한 차이")을 만든다. Tilekiln 과 MTS 도 타일을 통째로 만든다.
   - 이웃이 없는 더러운 타일은 **빈 MVT(gzip)** 로 쓴다. 이것이 타일 툼스톤이다(ADR-0111 §3).

4. **패치는 누적형 PMTiles 하나다(ADR-0111 §5 개정).**
   - 패치 세대는 `{root}/{unit}-{release}.patch-{n}.pmtiles` 에 create-only 로 올린다. 기본판과 같은 접두어다.
   - 내용은 이번 더러운 타일, 그리고 이전 세대의 타일 중 이번에 더럽지 않은 것을 바이트 그대로 옮긴 것이다.
   - 그래서 요청 하나는 많아야 **패치 하나 + 기본판** 을 읽는다. ADR-0141 의 K 개 패치 목록을 차례로 보는 방식보다
     읽기 상한이 작다.
   - ADR-0111 §5 의 타일 낱장 객체와 세대 색인 JSON 은 쓰지 않는다. 같은 정보가 PMTiles 디렉터리에 있고, Worker 는
     그 디렉터리를 이미 캐시한다(ADR-0147 이 by-PNU 에서 낱개를 묶음으로 바꾼 것과 같은 이유).
   - PMTiles 쓰기는 spec v3 의 참조 구현과 같은 배치를 따른다: 헤더, 루트(16KiB 안), 메타데이터, 잎 디렉터리,
     데이터. 모듈은 `pmtiles_write.rs` 다.

5. **manifest 하나만 가변이다(ADR-0141 §4·Revision 3 와 같다).**
   - 위치는 `{root}/{unit}-{release}.patches.json` 이고 내용은 다음과 같다.
     - `schema_version`, `base_source_id`;
     - `generation`(발행마다 증가);
     - `gold_snapshot`, `folded_through_change_seq`;
     - `patch`(세대·sha256·바이트·타일 수·툼스톤 수, 또는 null).
   - 쓰기는 처음엔 `If-None-Match: *`, 그 뒤엔 읽은 판의 `If-Match` 다. 진 쪽은 거부된다.
   - 되돌리기는 manifest 를 이전 세대로 돌리는 것이다. 다음 발행은 이미 객체가 있는 세대 번호를 건너뛴다.
   - 서빙 타일이 바뀌지 않은 새 스냅숏은 manifest 의 스냅숏만 옮기고 같은 패치를 가리킨다.
   - ADR-0111 §5 의 "Catalog runtime manifest 의 `patch_generation`" 은 쓰지 않는다. Worker 는 DB 를 읽지 않는다
     (ADR-0111 §6). by-PNU 와 같은 R2 manifest 가 정본이다. 기본판 release 와 그 승격은 Catalog 에 그대로 남는다.

6. **Worker 는 패치 → 툼스톤 → 기본판 순으로 답한다.**
   - URL 은 그대로 `/{unit}-{base_release_id}/{z}/{x}/{y}` 다(ADR-0037·ADR-0111 §6).
   - 패치 대상 unit 이면 manifest 를 읽는다. isolate 마다 `manifest_cache_seconds`(60초) 동안 다시 읽지 않는다.
   - 패치 archive 의 **디렉터리로만** 그 타일이 있는지 본다. 타일 바이트는 엣지 캐시를 놓쳤을 때만 읽는다.
   - 패치에 있는 타일의 엣지 캐시 키에는 패치 세대를 붙인다. 기본판 타일의 캐시 키는 그대로다. 그래서 새 패치는
     **바뀐 타일만** 엣지에서 식힌다. purge API 를 부르지 않는다(ADR-0111 §7 개정).
   - 패치 대상 unit 의 브라우저 캐시는 `public, max-age=3600` 이다. 같은 URL 의 내용이 바뀌기 때문이다.
     ADR-0141 Revision 1 과 같은 근거다: 굽기는 판마다(많아야 하루 한 번) 돈다. 패치 대상이 아닌 unit 은 지금처럼
     1년 immutable 이다.
   - manifest 가 이 기본판의 것이 아니거나, 패치 세대가 manifest 세대보다 크거나, 가리킨 패치 객체가 없으면 500 이다.
     캐시하지 않는다. 기본판으로 몰래 떨어지지 않는다.

7. **승격 게이트(ADR-0111 §8 의 패치판).** 다음 중 하나라도 어기면 manifest 를 바꾸지 않는다.
   - (가) 패치 헤더가 레이어의 MVT·줌 범위와 맞는다.
   - (나) 패치 maxzoom 타일의 모든 feature 가 새 스냅숏이 서빙하는 id 다.
   - (다) 삭제된 feature 는 패치 어디에도 없다.
   - (라) 추가·변경된 feature 는 모두 패치 maxzoom 타일에 있다. 없어도 되는 것은 ADR-0262 서브픽셀뿐이다.
   - (마) create-only 업로드 뒤 전 바이트를 다시 해시해 일치한다.
   - (바) manifest CAS 가 통과한다.

8. **전량(새 기본판)으로 가는 때(ADR-0111 §9).** 다음 중 하나면 지금의 전량 굽기(PR #897 시도별)가 새 기본판을
   만든다. 이유는 `lakehouse-tile-bake-patch-skipped reason=…` 로 저널에 남는다.
   - 서빙 중인 스냅숏의 색인이 같은 bake inputs 로 없다. 첫 굽기, 도구 교체, 패치를 모르는 코드가 만든 기본판이
     여기에 든다.
   - 누적 패치가 기본판 타일의 `max_patched_tile_ratio`(5%) 를 넘는다. ADR-0111 §9 초깃값이고, MTS 의 15% 권고보다
     낮다. 지역 개편(ADR-0251: PNU 16.2% 재번호)이 있는 판은 여기서 전량이 된다.
   - 기본판 헤더가 타일 수를 적지 않았거나 gzip 이 아니다.

9. **켜는 순서.** 패치를 아는 코드와 Worker 는 같은 병합으로 배포된다. 색인은 그 코드가 서빙한 스냅숏에만
   생긴다. 그래서 첫 패치는 항상 "패치를 아는 코드가 만든 기본판" 위에 얹힌다. 그 기본판의 타일은 처음부터
   `max-age=3600` 으로 나갔으므로, 1년 immutable 로 브라우저에 박힌 옛 타일이 패치를 가리는 일은 없다.
   `parcel_served_gold`·`parcel_tile_bake` 는 여전히 `enabled: false` 이고, 켜는 것은 사장 결정이다.

## Consequences

판 하나가 시도 하나만 조금 바꿀 때의 전후다. "지금"은 실측, "뒤"는 계산이다(운영 측정 아님).

| 단계 | 지금(PR #897, 실측 바탕) | 뒤(이 ADR, 계산) |
|---|---|---|
| 핸드오프 읽기 | 1회 | 2회(색인 1, 이웃 1). 둘 다 순차 스트림 |
| GDAL·tippecanoe 입력 | 바뀐 시도 전체(평균 235만 행, 흩어지면 17 시도 전부) | 이웃 = 더러운 z14 타일 수 × z14 타일당 필지(전국 평균 약 1,900) |
| tile-join | 전국 5.55GB 다시 쓰기 | 없음 |
| R2 업로드 | 5.55GB | 누적 패치 타일 × 평균 약 2.1KB(5.55GB / 약 267만 타일). 타일 1만 장이면 약 21MB |
| 엣지 캐시 | 새 URL 이라 전국이 식는다 | 패치된 타일만 식는다 |
| 메모리 | 서빙 id 640MB | + 색인 두 벌 3.2GB(호스트 62GB) |
| `/data` | 판마다 시도 archive 증가 | + 판마다 색인 1.6GB(append-only, 보존 기한은 사장 결정) |

- 첫 리허설의 `lakehouse-tile-bake-patch-plan` 줄(바뀐 수·더러운 타일 수·한도)이 실제 판 변경의 첫 측정값이다.
  판 사이에서 도형만 바뀐 같은 PNU 의 수는 아직 재지 않았다(긴 bake 가 도는 동안 4천만 행 조인을 서버에 돌리지
  않았다).
- 이웃 굽기는 이웃 안에서만 공유 경계를 찾는다. 이웃 가장자리 밖 필지와 공유하는 꼭짓점의 단순화가 전국 굽기와
  미세하게 다를 수 있다. PR #897 의 시도 경계도 같은 성질이다. 게이트는 id 집합으로 판정하고, 압축(새 기본판)이
  차이를 없앤다.
- 남은 것:
  - 색인 파일의 보존 기한;
  - `gold.parcel_boundary_served` 를 `gold_incremental` 레인으로 옮겨 ADR-0312 의 `__changes` 로 변경을 받는 일.
    그러면 색인 읽기가 바뀐 행만으로 준다. 그때도 이웃 선택에는 상자가 있어야 하므로 색인은 남는다.
- 비용: R2 쓰기는 패치 1 + manifest 1 회다. 패치 객체가 쌓이는 저장은 세대마다 기본판의 5% 이하다.
  Worker 는 isolate 마다 60초에 manifest GET 1 회를 더 쓴다.
