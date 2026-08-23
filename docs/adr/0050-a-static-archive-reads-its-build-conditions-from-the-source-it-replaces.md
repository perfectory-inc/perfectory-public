# ADR 0050: 정적 아카이브는 자기가 대체하는 소스에서 굽는 조건을 읽는다

- Status: Accepted
- Date: 2026-08-23
- 관련: [ADR-0006 기준 데이터는 객체 저장소 우선](./0006-object-storage-first-serving.md),
  [ADR-0012 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md),
  [ADR-0013 release 유니크 키는 두 source kind 를 함께 받는다](./0013-release-uniqueness-admits-both-source-kinds.md),
  [ADR-0014 serving_generation 은 한 단위의 소스 선택을 센다](./0014-serving-generation-tracks-one-unit-source-selection.md),
  [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md),
  [ADR-0043 정본 id 는 다시 계산하지 않고 읽는다](./0043-a-canonical-id-is-read-not-recomputed.md),
  [FP-ADR-0004 정적 벡터타일 런타임 계약](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)

## Context

ADR-0006 이 한 발행 단위의 서빙 상태를 둘로 못 박았다. 새로 승인된 내용이 있는 단위는 PostGIS 를
Martin 이 읽고, 예약 발행이 끝난 단위는 R2 의 불변 PMTiles 하나가 된다. 한 단위는 둘 중 하나만
가진다. 산업단지(`complex`)는 폴리곤 적재·경계 렌더·패널·검색이 #94~#97 로 이어지며 생산자와
소비자가 짝이 맞는 것이 확인돼, 두 번째 상태로 옮길 수 있는 첫 단위가 됐다.

### 정적 사슬은 있었지만 필지 fixture 전용이었다

굽는 사슬 자체는 `scripts/tiles/tiles-slice-proof.sh` 안에 한 벌 있다. 그러나 그 안에서 굽는
조건이 전부 리터럴이다. bbox 하나, zoom 밴드 셋(0–11 / 12–13 / 14–16), 레이어 이름 셋, PNU 셋,
그리고 아카이브의 논리 타일 수와 payload 바이트까지. 다른 단위를 그 파일로 구울 수는 없다.

`foundation-outbox-publisher` 의 커맨드 표에는 항목이 116개 있고 그중 아카이브를 굽는 것은
없다. `catalog.vector_tile_build_job` — 정적 빌드 한 번의 상태 원장 — 은 표와 CHECK 제약만 있고
쓰는 코드가 없다. ADR-0040 이 이름 붙인 형태 그대로다.

### 그래서 `complex` 를 구우려면 굽는 조건을 어딘가 세 번째로 적어야 했다

zoom 6–16 은 이미 두 곳에 있다.

| 어디 | 무엇 |
|---|---|
| `scripts/tiles/martin-dynamic.yaml` | `complex` source 의 `minzoom: 6` / `maxzoom: 16` |
| `industrial_complex_boundary_runtime_promote.rs` | `const TILE_ZOOM: (i16, i16) = (6, 16)` |

두 값이 어긋나도 아무것도 실패하지 않는다. 서로를 주석으로만 가리킨다. 여기에 정적 빌드의
세 번째 사본이 생기면, 어긋났을 때 증상은 **"어떤 줌에서 지도가 빈다"** 하나뿐이고 그 줌을
아무도 열어 보지 않으면 증상조차 없다. 굽는 쪽이 조건을 스스로 적는 한 이 결함은 계속 만들 수
있다.

### 디코더는 `complex` 타일을 아예 읽지 못했다

`scripts/tiles/mvt_assert.rs` 의 `dump` 는 모든 feature 에 `pnu` 를 요구했다. `complex` feature 는
`complex_id` 와 `official_complex_code` 만 싣고 `pnu` 를 싣지 않는다. 그래서 이 저장소가 #95 부터
서빙해 온 레이어의 모든 feature 가 디코더 자신의 identity 규칙에 걸렸고, ADR-0006 이 승격 전에
요구하는 **동적/정적 identity 대조가 이 단위에 대해서는 표현조차 불가능**했다. 검사가 없던 게
아니라 쓸 수 없었다.

## Decision

1. **굽는 조건은 그 단위의 동적 소스 TileJSON 에서 읽는다.** zoom 범위, bbox, vector-layer
   메타데이터가 그것이다. 빌드 경로에 zoom 리터럴과 bbox 리터럴을 두지 않는다.
2. **굽기 전에 TileJSON 의 zoom 범위가 그 단위가 현재 고른 release 의
   `catalog.vector_tile_release_layer.tile_min_zoom`/`tile_max_zoom` 과 같은지 확인한다.** 다르면
   아무것도 굽지 않는다. Martin 이 실제로 자르는 범위와 발행된 release 가 광고하는 범위는 같은
   사실이고, 다를 때 잘못되는 쪽은 클라이언트가 읽는 값이다.
3. **타일은 그 레이어가 선언한 identity 속성만 싣는다.** 빌드는 TileJSON 의 `fields` 개수를 세어
   정확히 그 수인지 확인한다. 상태·진행률·분양 같은 움직이는 값은 캐시 뒤에서 조용히 낡고
   기하는 맞은 채로 남기 때문에, 개수를 세는 것이 "하나 더 늘었다"를 잡는 자리다.
4. **feature 의 identity 는 그 레이어가 선언한 속성들이다.** `mvt_assert` 는
   `--identity-property` 로 키를 받도록 하고, 키를 identity 안에 실어 서로 다른 키로 뜬 두 dump 가
   같다고 비교되지 않게 한다. 기본값을 두지 않는다 — 키 없는 dump 는 레이어 이름 목록이고,
   레이어만 같고 나머지가 전부 다른 두 타일이 같다고 나온다.
5. **아카이브의 이름은 파생한다.** Martin source id 와 object key 는
   `catalog_domain::static_release_martin_source_id` / `static_release_pmtiles_object_key` 의 정의를
   따르고, 정적 Martin 은 `pmtiles.paths` 로 그 파일명에서 source id 를 얻는다. 이름 있는
   `pmtiles.sources` 는 운영자가 고른 경로에 아무 파일이나 답하게 하므로 release 주소 계약을
   증명하지 못한다.

이 다섯을 `scripts/tiles/boundary-slice-proof.sh` 가 `complex` 단위에 대해 실행하고,
`scripts/tiles/martin-static-local-paths.yaml` 이 5항의 발견 방식을 고정한다. 이 증명이 막는 실제
사고는 하나다: **동적 소스가 자르는 범위와 다른 범위로 구운 아카이브를 올려, 어떤 줌에서 빈
지도를 서빙하는 것.**

## Consequences

- 정적 아카이브를 굽는 데 새 설정 파일이 필요 없다. 단위가 동적으로 한 번 발행돼 있으면 그
  TileJSON 이 굽는 조건 전부다. `admin` 도 같은 방식으로 구울 수 있다.
- `mvt_assert dump` 는 호출자에게 `--identity-property` 를 요구한다. 필지 증명의 호출부에
  `--identity-property pnu` 가 붙었고 출력 형식은 그대로다 — 키가 하나일 때 canonical line 은
  이전과 바이트 단위로 같다.
- `dump` 가 타일 여러 장을 한 번에 받는다. 압축을 푼 아카이브는 폴리곤 두 개짜리 fixture 에서도
  수백 장이고, 한 장에 프로세스 하나씩이면 "전 줌 디코드"가 제일 먼저 빠지는 검사가 된다.
- 남는 것: 아카이브를 R2 에 올리고 포인터를 정적으로 바꾸는 커맨드는 아직 없다. 이 결정은 굽고
  검증하는 데까지를 정의하고, 업로드와 CAS 전환은 별도 승인으로 남는다.
  `catalog.vector_tile_build_job` 도 여전히 생산자가 없다.
