# ADR 0087: 필지는 토지특성 원장에서 면적과 특성을 배운다

- Status: Accepted
- Date: 2026-09-06

## Context

ADR-0070의 경계 원천은 필지 면적을 주지 않는다. 면적을 도형에서 계산하면 공부 면적이라는
다른 사실을 만들어 낸다. Bronze의 `vworldkr__land_characteristic`는 AL_D194 Shapefile이며,
필요한 사실은 도형이 아니라 EUC-KR DBF 속성에 있다. 문제는 열 이름이 A0~A26이라는 점이다.
그럴듯한 문자열이나 지도에서 맞아 보이는 도형은 열 의미의 증거가 되지 않는다.

열 의미의 정본은 제공자 자신이다: 브이월드는 같은 데이터셋(`dtmk_ntads_s002.do?svcCde=NA&dsId=4`,
수집 설정 `{svc_cde: NA, ds_id: 4}`)을 Shapefile 과 별도로 **한글 헤더 CSV** 로도 배포하며,
그 CSV 헤더 순서가 DBF 의 A-열 순서와 같다(A0 은 CSV 에 없는 내부 일련번호라 A1 부터 대응).
2026-09-06 이 CSV 헤더로 매핑을 확정: A1=고유번호(PNU), A2=법정동코드, A3=법정동명,
A4=대장구분코드, A5=대장구분명, A6=지번, A7=토지일련번호, A8=기준연도, A9=기준월,
A10=지목코드, A11=지목명, A12=토지면적, A13/14=용도지역코드/명1, A15/16=용도지역코드/명2,
A17=토지이용상황코드, A18=토지이용상황, A19=지형높이코드, A20=지형높이, A21=지형형상코드,
A22=지형형상, A23=도로접면코드, A24=도로접면, A25=개별공시지가, A26=데이터기준일자.
독립 실측 표본(강원 태백 51190)도 이 매핑과 일치했다. A8·A9 는 개별공시지가의 기준연도·월이며,
표본 A8=2025 이나 현재 가격 투영은 2026년 1월(`vworldkr-land-individual-price:20260526`)이라
객체 수확일·평가 빈티지를 혼동하면 정상 자료를 틀린 매핑으로 오판한다. 그러나 매핑 자체는
제공자 CSV 로 확정된 사실이지 추정이 아니다 — 검증 게이트는 이후 형식 변경을 잡는 파수꾼으로
남긴다. 807객체의 실측 목록은 조정자가 교체하는 명시적 보류 사항이며,
`coordinator_fills_real_inventory=true`인 계약은 전국 적재를 거부한다.

## Decision

1. `silver.land_characteristic`는 AL_D194의 DBF 속성을 PNU로 식별한다. 지목·토지이용상황·
   지형높이·지형형상·도로접면은 원천 문자열로, 면적은 양수 숫자로 나른다. 도형은 저장하지
   않는다. 경계는 `vworldkr__parcel`이 소유한다. ZIP·DBF 해석은 기존 `zip`·`shapefile`의
   `dbase`와 Foundation의 순차 읽기 어댑터를, 전송·압축·요약은 ADR-0083 공용 수출 경로를
   재사용한다. 별도 Shapefile 전송기를 만들지 않는다.
2. 무명 열 매핑 검증은 두 단계다. 객체마다 기본 1,000행(환경변수
   `FOUNDATION_PLATFORM_PARCEL_CHARACTERISTIC_SAMPLE_SIZE`)을 표본으로 삼아 PNU 파싱 성공
   99% 이상, 양수 유한 면적 99% 이상, 지목 28종 폐집합 포함 95% 이상을 모두 요구한다.
   표본은 무효 행을 포함한 객체 전체에서 안정적인 해시로 고른다. 정렬된 원천의 앞부분만
   검사해 뒤쪽의 오류를 놓치는 것을 피한다.
   폐집합의 단일 정의는 적재기의 `LAND_CATEGORIES`다. 실패는
   `land_characteristic_mapping_pnu_failed`, `land_characteristic_mapping_area_failed`,
   `land_characteristic_mapping_land_category_failed`로 거부한다. 개별 행의 무효 PNU·면적은
   서빙하지 않고 건수를 기록한다. 수출기는 원천의 필수 식별자·수치 타입 오류를 거부한다.
   부 교차검증은 동일 PNU와 동일 `(A8,A9)=(base_year,base_month)`인 행에 한해 A25를
   `catalog.parcel_price.price_per_m2`와 대조한다. 1% 초과 불일치는
   `land_characteristic_price_mapping_mismatch`로 거부한다. 교집합이 없으면
   `land_characteristic_price_check_skipped_no_overlapping_vintage`에 가격 행 부재와 빈티지
   차이 건수를 남기고 주 검증 결과를 사용한다. 이는 2025·2026 실측에 따른 조정자의
   명시적 변경이다. 모든 객체를 한 트랜잭션으로 처리해 뒤 객체의 실패도 앞 병합을 취소한다.
3. `catalog.parcel_characteristic`는 `pnu character(19)` 기본키로 필지당 한 행을 가진다.
   `land_category`, `area_m2 numeric`, `land_use_situation`, `terrain_height`,
   `terrain_shape`, `road_contact`, `source_snapshot_id`와 원천 빈티지를 보존한다.
   COPY 임시 표에서 PNU별 최신 빈티지를 골라 병합하고, 오래된 재실행은 최신 값을 덮지
   않는다. 경계가 늦게 오는 필지의 사실도 보존하므로 `catalog.parcel` 외래키는 두지 않는다.
4. by-pnu API는 nullable `characteristics{land_category, area_m2, land_use_situation,
   terrain_height, terrain_shape, road_contact}`를 제공한다. 표기·형식화는 소비자 몫이다.
   이 원천의 `area_m2`가 ADR-0070에서 비워 둔 공부 면적을 채운다.
5. A25는 검증에만 쓴다. Silver 원천 검증값을 메모리 표본에서 비교하지만 서빙 특성 표나
   특성 API에는 가격을 복제하지 않는다. 공시지가의 서빙 정본은 `catalog.parcel_price`다.
6. 객체 계약은 실측한 시군구 집합과 빈티지를 보존하고 가장 최신의 완전한 전국 빈티지를
   선택한다. 부분 전국·중복 시군구·계약과 다른 객체 계보는 거부한다. 전국 Spark 실행과
   운영 승격은 이 코드 변경에 포함하지 않는다.

## Consequences

- 수치형 면적은 소수 면적을 버리지 않는다. 문자열 특성은 임의 코드표나 표시 문구로 바꾸지
  않아 원천에 없는 의미를 만들지 않는다.
- DBF 속성만 읽으므로 기존 경계 도형의 투영·메모리·저장 비용을 반복하지 않는다.
- 현재처럼 평가 빈티지가 겹치지 않으면 가격이 매핑을 증명했다고 보고하지 않는다. 세 가지
  주 검증을 통과했음을 보고하고 부 검증을 건너뛴 이유·건수를 별도로 기록한다.
- 면적·PNU·빈티지 제약과 검증 게이트는 성공처럼 보이는 잘못된 적재를 막는다.

## 실행 경로

- `export-land-characteristic-silver-handoff`: `FOUNDATION_PLATFORM_LAND_CHARACTERISTIC_`
  접두사의 `INPUT_PATH`/`INPUT_OBJECT_KEY`, `OUTPUT_PATH`/`OUTPUT_OBJECT_KEY`,
  `SOURCE_SNAPSHOT_ID`, 선택 `SUMMARY_PATH`를 기존 수출기와 같은 방식으로 받는다.
- `silver_scalar_handoff_to_lakehouse.py --contract silver.land_characteristic`가 JSONL을
  기존 적재 SSOT로 처리한다. 객체별 적재 식별자는 `source_record_id`다.
- `load-parcel-characteristic-catalog-projection`: `DATABASE_URL`, 기존 R2 환경 설정과
  `FOUNDATION_PLATFORM_PARCEL_CHARACTERISTIC_PROJECTION_LOAD_CONFIRM=true`가 필요하다.
  `LAND_CHARACTERISTIC_SOURCE_CONTRACT`는 선택 계약 경로다. 운영 실행은 실측 목록 교체 후다.
