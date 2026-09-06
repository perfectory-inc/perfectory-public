# ADR 0087: 필지는 토지특성 원장에서 면적과 특성을 배운다

- Status: Accepted
- Date: 2026-09-06

## Context

ADR-0070의 필지 경계 원천은 공부 면적을 주지 않는다. 도형에서 계산한 면적은 공부 면적을
대신할 수 없다. 브이월드는 같은 토지특성 데이터셋(dsId=4)을 AL_D194 Shapefile과
AL_D195 CSV 두 표현으로 제공한다. 이 레인은 제공자의 한글 헤더 CSV인 **AL_D195**를 선택한다.
제공자 정본은 [브이월드 토지특성 데이터셋](https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?svcCde=NA&dsId=4)의 CSV 형식이다.

2026-09-06 실측에 따르면 `bronze/source=vworldkr__land_characteristic/`에는 두 표현이 이미
함께 보존되어 있다. AL_D195는 17개 시도 × 3개 빈티지(20250813·20260402·20260519),
총 51개 ZIP이며 최신 20260519가 전국을 완전히 덮는다. 재수집은 필요 없다.
객체 이름만으로 시도와 빈티지를 알 수 없으므로 내부 `AL_D195_<sido>_<yyyymmdd>.csv`와
객체 크기를 실측한 [원천 객체 계약](../../platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-characteristic-source-objects.json)이 입력 선택의 정본이다.

## Decision

1. 필지 도형의 Shapefile 원천은 `vworldkr__parcel` 하나다. 필지 속성 데이터는 이름 있는
   CSV를 PNU로 연결한다. `silver.land_characteristic`는 AL_D195의 EUC-KR CSV를 읽으며
   도형을 저장하지 않는다. ADR-0083·0085의 ZIP 선택·CSV 해석·전송·압축·계보 경로를
   `LAND_CHARACTERISTIC_LANE` 선언으로 재사용한다. 별도 형식 해석기나 전송기는 만들지 않는다.
2. 제공자의 26개 한글 헤더를 순서까지 정확히 단정하고, 바뀐 헤더는 적재 전에 거부한다.
   이름 있는 열을 Silver 계약에 대응시키고 PNU와 양수 유한 숫자 면적을 검사한다.
   무효 행은 이유별 건수를 남기고 제외한다. `공시지가`는 입력 위치만 소비하고 Silver에
   복제하지 않는다. 가격 정본은 `catalog.parcel_price`다.
3. 실측 객체 계약의 `dataset_series=AL_D195`, `load_granularity=sido`와 내부 CSV 이름을
   검사한다. 가장 최신의 완전한 17개 시도 빈티지만 선택하며 부분 전국·중복 시도·중복 객체를
   거부한다. 투영은 행의 객체 계보·스냅샷·PNU 시도를 검사하고 `source_vintage`를 선택한
   객체 계약에서 얻는다. `data_reference_date`와 객체 수확 빈티지는 별개의 사실이다.
4. `catalog.parcel_characteristic`는 `pnu character(19)` 기본키로 필지당 한 행을 가진다.
   `land_category`, `area_m2 numeric`, `land_use_situation`, `terrain_height`,
   `terrain_shape`, `road_contact`, `source_snapshot_id`와 원천 빈티지를 보존한다.
   COPY 임시 표에서 PNU별 최신 빈티지를 골라 병합하고 오래된 재실행은 최신 값을 덮지
   않는다. 모든 객체를 한 트랜잭션으로 처리한다. 경계가 늦게 오는 필지의 사실도 보존하므로
   `catalog.parcel` 외래키는 두지 않는다. PNU 조회의 `$1::character(19)` 캐스트를 유지한다.
5. by-pnu API의 nullable `characteristics{land_category, area_m2, land_use_situation,
   terrain_height, terrain_shape, road_contact}`와 Gongzzang 소비 계약은 유지한다.
   `토지면적`에서 온 `area_m2`가 ADR-0070에서 비워 둔 공부 면적을 채운다.

## Consequences

- 문자열 특성은 원천 표기를 유지하고 숫자 면적은 소수부를 보존한다.
- 기존 가격 CSV 레인의 형식·전송 처리를 재사용해 속성 원천마다 별도 DBF 해석기를 만들지 않는다.
- Rust 계약·Spark 계약 산출물·CSV 위치 대응 테스트가 출력 열 집합의 차이를 차단한다.
- 전국 Spark 실행과 운영 승격은 조정자의 병합 후 작업이며 이 변경의 검증 범위에 포함하지 않는다.

## 실행 경로

- `export-land-characteristic-silver-handoff`: `FOUNDATION_PLATFORM_LAND_CHARACTERISTIC_`
  접두사의 `INPUT_PATH`/`INPUT_OBJECT_KEY`, `OUTPUT_PATH`/`OUTPUT_OBJECT_KEY`,
  `SOURCE_SNAPSHOT_ID`, 선택 `SUMMARY_PATH`를 기존 수출기와 같은 방식으로 받는다.
- `silver_scalar_handoff_to_lakehouse.py --contract silver.land_characteristic`가 JSONL을
  기존 적재 SSOT로 처리한다. 객체별 적재 식별자는 `source_record_id`다.
- `load-parcel-characteristic-catalog-projection`: `DATABASE_URL`, 기존 R2 환경 설정과
  `FOUNDATION_PLATFORM_PARCEL_CHARACTERISTIC_PROJECTION_LOAD_CONFIRM=true`가 필요하다.
  `LAND_CHARACTERISTIC_SOURCE_CONTRACT`는 선택 계약 경로다.
