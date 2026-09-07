---
status: current
owner: foundation-platform
doc_type: catalog
last_reviewed: 2026-09-06
---

<!-- GENERATED FILE. Do not edit by hand. -->
<!-- Render with: python3 scripts/catalog/render-pipeline-map.py -->

# 데이터가 화면에 도착하기까지

원천은 제공기관의 데이터이고, Bronze는 받은 원본, Silver는 이름·형식을 맞춘 표,
Gold는 제공 목적에 맞춘 표입니다. 서빙은 조회·지도 제공용 저장소이며 화면은 이를 읽습니다.
아래의 수집규모는 **카탈로그에 등록된 endpoint 수**입니다. 실제 수집 객체 수·행 수·용량은
이 카탈로그에 없으므로 추정하지 않습니다. 실행 경로가 있다는 표시는 운영 배포·전국 적재 완료를 뜻하지 않습니다.

현재 범위: 원천 **8그룹 / 133 endpoint**,
Silver·Gold **16표**,
서빙·운영 원장 **73표**.

정본: [파이프라인 그래프](../platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json) · [원천 카탈로그](../platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json) ·
[결정 ADR-0086](./adr/0086-the-pipeline-graph-names-every-dataset-once.md).

## 원천 가족별 전체 범위

| 원천 | 수집규모 | 연결 상태 |
|---|---:|---|
| 건축HUB 파일 | 59 endpoint | 일부 연결 — 표제부·층·호·면적만 Silver 레인이 있고 나머지는 수집 단계에 남는다. |
| 건축물대장 API | 10 endpoint | 수집 비활성 — 파일 수집과 중복되어 비활성화된 API이다. |
| 산업입지정보 산업단지 | 3 endpoint | 일부 연결 — 목록·고시·상세는 브이월드 산업단지의 주소 해석 근거로 연결된다. |
| 주소정보 전자지도 | 11 endpoint | 승인 후 수집 가능 — 수동 승인 수집 경로이며 건물 도형 Silver 계약은 아직 없다. |
| 기타 공공 일괄 파일 | 13 endpoint | 승인 후 수집 가능 — 수동 승인 수집 경로이며 Silver 연결은 없다. |
| 실거래가 API | 12 endpoint | 수집 비활성 — 카탈로그에서 중복 API로 비활성화되어 있다. |
| 브이월드 공간·토지 파일 | 24 endpoint | 일부 연결 — 필지·용도지역·공시지가·행정경계·산업단지 프로필과 경계의 실행 경로가 있다. 나머지는 수집 단계에 남는다. |
| 브이월드 토지대장 API | 1 endpoint | 예정 — API 수집 예정이며 Silver 계약은 없다. |

## 데이터 가족별 연결 경로

수집규모는 각 경로가 선택한 원천의 endpoint 수입니다. 같은 원천이 두 경로에 쓰이면 중복 표시되므로 합산하지 않습니다.

| 원천 | 수집규모 | Silver → Gold | 서빙 | 화면 |
|---|---:|---|---|---|
| 브이월드 공간·토지 파일: VWorld 필지 | 1 endpoint | 필지 경계 (`silver.parcel_boundaries`) | 필지 기본·식별자<br>필지 경계 서빙 | 필지 지도 타일<br>카탈로그 조회 API<br>공짱 지도·상세 패널 |
| 브이월드 공간·토지 파일: VWorld 토지이용계획 | 1 endpoint | 필지별 토지이용계획 (`silver.land_use_plan`) | 필지 용도지역 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| ↳ D155 CSV만 연결한다. 같은 원천의 D154 도형은 Bronze에 남는다. | | | | |
| 브이월드 공간·토지 파일: VWorld 토지이용구역 코드 | 1 endpoint | 용도지역 코드 사전 (`silver.land_use_zone_code`) | 필지 용도지역 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| ↳ 코드 사전의 계층을 그대로 읽는다. | | | | |
| 브이월드 공간·토지 파일: VWorld 개별공시지가 | 1 endpoint | 필지별 공시지가 (`silver.land_individual_price`) | 필지 공시지가 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| ↳ D151 CSV만 연결한다. D150 DBF는 Bronze에 남는다. | | | | |
| 브이월드 공간·토지 파일: VWorld 토지특성 | 1 endpoint | 필지별 토지특성 (`silver.land_characteristic`) | 필지 토지특성 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| ↳ AL_D195 CSV 속성을 PNU로 연결한다. 도형은 기존 필지 경계가 소유한다. | | | | |
| 브이월드 공간·토지 파일: VWorld 임야 | 1 endpoint | 필지별 임야대장 (`silver.land_forest_ledger`) | 필지 임야대장 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| ↳ AL_D003 CSV 속성을 PNU로 연결한다. 도형은 기존 필지 경계가 소유한다. | | | | |
| 브이월드 공간·토지 파일: VWorld 토지이동연혁 | 1 endpoint | 필지별 토지이동이력 (`silver.land_transfer_history`) | 필지 토지이동 사건 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| ↳ AL_D157 전체 사건을 PNU·이력순번으로 보존한다. 폐쇄·말소 사건도 연혁에 남는다. | | | | |
| 건축HUB 파일: 건축물대장 표제부 파일 | 1 endpoint | 건물 표제부 (`silver.building_register_titles`) | 건물 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| 건축HUB 파일: 건축물대장 층별개요 파일 | 1 endpoint | 건물 층별 정보 (`silver.building_register_floors`) | — | — |
| 건축HUB 파일: 건축물대장 전유부 파일 | 1 endpoint | 건물 호별 정보 (`silver.building_register_units`) | 건물의 호 | 카탈로그 조회 API<br>공짱 지도·상세 패널 |
| 건축HUB 파일: 건축물대장 전유공용면적 파일 | 1 endpoint | 호별 전유·공용 면적 (`silver.building_register_unit_areas`) | — | — |
| 산업입지정보 산업단지: ILIS 산업단지 목록<br>ILIS 산업단지 고시 목록<br>ILIS 산업단지 상세 | 3 endpoint | 산업단지 기본 정보 (`silver.industrial_complexes`)<br>산업단지 경계 (`silver.industrial_complex_boundaries`)<br>산업단지 제공용 프로필 (`gold.complex_catalog`) | 산업단지<br>산업단지 프로필 포인터<br>산업단지 경계 서빙 | 산업단지 지도 타일<br>카탈로그 조회 API<br>산업단지 프로필 게이트웨이<br>공짱 지도·상세 패널 |
| ↳ ILIS 목록·고시·상세는 산업단지 주소 해석의 근거이다. 프로필 본문은 브이월드에서 온다. | | | | |
| 브이월드 공간·토지 파일: VWorld 산업단지 경계 | 1 endpoint | 산업단지 경계 (`silver.industrial_complex_boundaries`) | 산업단지 경계 서빙 | 산업단지 지도 타일<br>공짱 지도·상세 패널 |
| 브이월드 공간·토지 파일: VWorld 읍면동 경계 | 1 endpoint | — | 행정경계 서빙 | 행정경계 지도 타일<br>공짱 지도·상세 패널 |
| ↳ Silver 표를 거치지 않는 행정경계 직접 서빙 레인이다. | | | | |
| 브이월드 공간·토지 파일: VWorld 시군구 경계<br>VWorld 산업단지 개요 | 2 endpoint | 산업단지 기본 정보 (`silver.industrial_complexes`)<br>산업단지 경계 (`silver.industrial_complex_boundaries`)<br>산업단지 제공용 프로필 (`gold.complex_catalog`) | 산업단지<br>산업단지 프로필 포인터<br>산업단지 경계 서빙 | 산업단지 지도 타일<br>카탈로그 조회 API<br>산업단지 프로필 게이트웨이<br>공짱 지도·상세 패널 |
| ↳ 산업단지 프로필 원본을 변환하며 시군구 경계의 DBF를 주소 행정구역 판정에 사용한다. | | | | |

## 수집 이후 아직 연결되지 않은 데이터

등록 원천 중 위 실행 경로가 선택하지 않은 항목을 자동으로 모았습니다. 파일 수집이 가능한 원천도
실제 객체 보유 여부는 수집 원장을 확인해야 합니다. 비활성·승인 대기·예정 원천을 수집 완료로 세지 않습니다.
같은 source slug 안의 제외 형식(D154 도형·D150 DBF)은 위 경로의 설명에 따로 명명했습니다.

| 원천 가족 | 이후 연결 없는 endpoint | 수집 설정 |
|---|---|---|
| 건축HUB 파일 | 건축물대장 기본 정보 파일 (`hubgokr__building_register_basis_outline`) | `bulk_file` |
| 건축HUB 파일 | 건축물대장 총괄표제부 파일 (`hubgokr__building_register_master`) | `bulk_file` |
| 건축HUB 파일 | 건축물대장 공동주택 가격 파일 (`hubgokr__building_register_apartment_price`) | `bulk_file` |
| 건축HUB 파일 | 건축물대장 오수정화시설 파일 (`hubgokr__building_register_sewage_facility`) | `bulk_file` |
| 건축HUB 파일 | 건축물대장 부속지번 파일 (`hubgokr__building_register_sub_parcel`) | `bulk_file` |
| 건축HUB 파일 | 건축물대장 지역지구구역 파일 (`hubgokr__building_register_district_zone`) | `bulk_file` |
| 건축HUB 파일 | 건물 가스 사용량 파일 (`hubgokr__building_energy_monthly_gas`) | `bulk_file` |
| 건축HUB 파일 | 건물 전기 사용량 파일 (`hubgokr__building_energy_monthly_electricity`) | `bulk_file` |
| 건축HUB 파일 | 기본개요 (`hubgokr__building_permit_basis_outline`) | `bulk_file` |
| 건축HUB 파일 | 동별개요 (`hubgokr__building_permit_building_outline`) | `bulk_file` |
| 건축HUB 파일 | 층별개요 (`hubgokr__building_permit_floor_overview`) | `bulk_file` |
| 건축HUB 파일 | 호별개요 (`hubgokr__building_permit_unit_outline`) | `bulk_file` |
| 건축HUB 파일 | 대수선 (`hubgokr__building_permit_major_repair`) | `bulk_file` |
| 건축HUB 파일 | 공작물관리대장 (`hubgokr__building_permit_structure_ledger`) | `bulk_file` |
| 건축HUB 파일 | 철거멸실관리대장 (`hubgokr__building_permit_demolition_loss`) | `bulk_file` |
| 건축HUB 파일 | 가설건축물 (`hubgokr__building_permit_temporary_building`) | `bulk_file` |
| 건축HUB 파일 | 오수정화시설 (`hubgokr__building_permit_sewage_facility`) | `bulk_file` |
| 건축HUB 파일 | 주차장 (`hubgokr__building_permit_parking`) | `bulk_file` |
| 건축HUB 파일 | 부설주차장 (`hubgokr__building_permit_annex_parking`) | `bulk_file` |
| 건축HUB 파일 | 전유공용면적 (`hubgokr__building_permit_exclusive_common_area`) | `bulk_file` |
| 건축HUB 파일 | 호별전유공용면적 (`hubgokr__building_permit_unit_exclusive_common_area`) | `bulk_file` |
| 건축HUB 파일 | 지역지구구역 (`hubgokr__building_permit_district_zone`) | `bulk_file` |
| 건축HUB 파일 | 도로대장 (`hubgokr__building_permit_road_ledger`) | `bulk_file` |
| 건축HUB 파일 | 대지위치 (`hubgokr__building_permit_site_location`) | `bulk_file` |
| 건축HUB 파일 | 주택유형 (`hubgokr__building_permit_housing_type`) | `bulk_file` |
| 건축HUB 파일 | 기본개요 (`hubgokr__housing_permit_basis_outline`) | `bulk_file` |
| 건축HUB 파일 | 동별개요 (`hubgokr__housing_permit_building_outline`) | `bulk_file` |
| 건축HUB 파일 | 층별개요 (`hubgokr__housing_permit_floor_overview`) | `bulk_file` |
| 건축HUB 파일 | 호별개요 (`hubgokr__housing_permit_unit_outline`) | `bulk_file` |
| 건축HUB 파일 | 부대시설 (`hubgokr__housing_permit_ancillary_facility`) | `bulk_file` |
| 건축HUB 파일 | 오수정화시설 (`hubgokr__housing_permit_sewage_facility`) | `bulk_file` |
| 건축HUB 파일 | 주차장 (`hubgokr__housing_permit_parking`) | `bulk_file` |
| 건축HUB 파일 | 부설주차장 (`hubgokr__housing_permit_annex_parking`) | `bulk_file` |
| 건축HUB 파일 | 전유공용면적 (`hubgokr__housing_permit_exclusive_common_area`) | `bulk_file` |
| 건축HUB 파일 | 행위호전유공용면적 (`hubgokr__housing_permit_act_unit_exclusive_common_area`) | `bulk_file` |
| 건축HUB 파일 | 행위개요 (`hubgokr__housing_permit_act_outline`) | `bulk_file` |
| 건축HUB 파일 | 관리공동형별개요 (`hubgokr__housing_permit_managed_communal_type_outline`) | `bulk_file` |
| 건축HUB 파일 | 관리공동부대복리시설 (`hubgokr__housing_permit_managed_communal_ancillary_welfare_facility`) | `bulk_file` |
| 건축HUB 파일 | 지역지구구역 (`hubgokr__housing_permit_district_zone`) | `bulk_file` |
| 건축HUB 파일 | 대지위치 (`hubgokr__housing_permit_site_location`) | `bulk_file` |
| 건축HUB 파일 | 복리분양시설 (`hubgokr__housing_permit_welfare_sale_facility`) | `bulk_file` |
| 건축HUB 파일 | 기본개요 (`hubgokr__building_register_closed_basis_outline`) | `bulk_file` |
| 건축HUB 파일 | 총괄표제부 (`hubgokr__building_register_closed_master`) | `bulk_file` |
| 건축HUB 파일 | 표제부 (`hubgokr__building_register_closed_main`) | `bulk_file` |
| 건축HUB 파일 | 층별개요 (`hubgokr__building_register_closed_floor_overview`) | `bulk_file` |
| 건축HUB 파일 | 부속지번 (`hubgokr__building_register_closed_sub_parcel`) | `bulk_file` |
| 건축HUB 파일 | 전유공용면적 (`hubgokr__building_register_closed_exclusive_common_area`) | `bulk_file` |
| 건축HUB 파일 | 오수정화시설 (`hubgokr__building_register_closed_sewage_facility`) | `bulk_file` |
| 건축HUB 파일 | 전유부 (`hubgokr__building_register_closed_exclusive_unit`) | `bulk_file` |
| 건축HUB 파일 | 공동주택가격 (`hubgokr__building_register_closed_apartment_price`) | `bulk_file` |
| 건축HUB 파일 | 지역지구구역 (`hubgokr__building_register_closed_district_zone`) | `bulk_file` |
| 건축HUB 파일 | 지번별에너지(전기,연도별) (`hubgokr__building_energy_yearly_electricity`) | `provider_inventory_missing` |
| 건축HUB 파일 | 지번별에너지(가스,연도별) (`hubgokr__building_energy_yearly_gas`) | `provider_inventory_missing` |
| 건축HUB 파일 | 점검기관 (`hubgokr__building_maintenance_inspection_agency`) | `bulk_file` |
| 건축HUB 파일 | 정기점검이력 (`hubgokr__building_maintenance_regular_inspection_history`) | `bulk_file` |
| 건축물대장 API | 건축물대장 표제부 (`datagokr__building_register_main`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 기본개요 (`datagokr__building_register_basis_outline`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 층별개요 (`datagokr__building_register_floor_overview`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 전유공용면적 (`datagokr__building_register_exclusive_common_area`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 주택가격 (`datagokr__building_register_house_price`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 전유부 (`datagokr__building_register_exclusive_unit`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 오수정화시설 (`datagokr__building_register_sewage_facility`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 총괄표제부 (`datagokr__building_register_master`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 부속지번 (`datagokr__building_register_sub_parcel`) | `disabled_api_duplicate` |
| 건축물대장 API | 건축물대장 지구지역구역 (`datagokr__building_register_district_zone`) | `disabled_api_duplicate` |
| 주소정보 전자지도 | JUSO 건물 도형 (`jusogokr__building`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 기초구간 (`jusogokr__base_interval`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 국가기초구역 (`jusogokr__basic_area`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 건물군 (`jusogokr__building_group`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 건물 출입구 (`jusogokr__building_entrance`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 법정구역 읍면동 (`jusogokr__legal_emd`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 법정구역 리 (`jusogokr__legal_ri`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 법정구역 시도 (`jusogokr__legal_sido`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 법정구역 시군구 (`jusogokr__legal_sigungu`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 도로구간 (`jusogokr__road_section`) | `manual_approval_bulk` |
| 주소정보 전자지도 | JUSO 실폭도로 (`jusogokr__road_width`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 고속도로 접근점 (`public-bulk-highway-access-point`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 학교 위치 (`public-bulk-school-location`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 공장등록현황 (`factoryongokr__factory_registration`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 대학교 (`public-bulk-university`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 세대수 (`moisgokr__household_count`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 동 인구 (`moisgokr__dong_population`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 철도역 (`public-bulk-railway-station`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 철도역 노선 매핑 (`public-bulk-railway-station-line`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 도시철도역 (`public-bulk-subway-station`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 항만 (`public-bulk-port`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 공항 (`public-bulk-airport`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 상권 (`public-bulk-commercial-district`) | `manual_approval_bulk` |
| 기타 공공 일괄 파일 | 행정동 법정동 연계 (`public-bulk-adm-dong-code-linkage`) | `manual_approval_bulk` |
| 실거래가 API | 연립다세대 매매 실거래가 (`datagokr__real_transaction_row_house_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 연립다세대 전월세 실거래가 (`datagokr__real_transaction_row_house_rent`) | `disabled_api_duplicate` |
| 실거래가 API | 공장창고 실거래가 (`datagokr__real_transaction_industrial_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 토지 실거래가 (`datagokr__real_transaction_land_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 상업업무용 실거래가 (`datagokr__real_transaction_commercial_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 오피스텔 매매 실거래가 (`datagokr__real_transaction_officetel_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 오피스텔 전월세 실거래가 (`datagokr__real_transaction_officetel_rent`) | `disabled_api_duplicate` |
| 실거래가 API | 아파트 매매 상세 실거래가 (`datagokr__real_transaction_apartment_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 아파트 전월세 실거래가 (`datagokr__real_transaction_apartment_rent`) | `disabled_api_duplicate` |
| 실거래가 API | 아파트 분양권전매 실거래가 (`datagokr__real_transaction_apartment_presale`) | `disabled_api_duplicate` |
| 실거래가 API | 단독다가구 매매 실거래가 (`datagokr__real_transaction_detached_house_trade`) | `disabled_api_duplicate` |
| 실거래가 API | 단독다가구 전월세 실거래가 (`datagokr__real_transaction_detached_house_rent`) | `disabled_api_duplicate` |
| 브이월드 공간·토지 파일 | VWorld 시도 경계 (`vworldkr__boundary_sido`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 통계 읍면동 경계 (`vworldkr__boundary_census_emd`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 통계 시도 경계 (`vworldkr__boundary_census_sido`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 통계 시군구 경계 (`vworldkr__boundary_census_sigungu`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 토지소유 (`vworldkr__land_ownership`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 토지권리등록 (`vworldkr__land_right_registration`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 토지이용구역 (`vworldkr__land_use_zone`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 공인중개사 (`vworldkr__real_estate_broker`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 산업단지 위치 (`vworldkr__sandan_location`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 산업단지 시설용지 (`vworldkr__sandan_facility_land_use`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 산업단지 용도지역 (`vworldkr__sandan_land_use_zone`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 산업단지 유치업종 (`vworldkr__sandan_permitted_industry`) | `provider_dataset_file` |
| 브이월드 공간·토지 파일 | VWorld 산업단지 필지 (`vworldkr__sandan_parcel`) | `provider_dataset_file` |
| 브이월드 토지대장 API | VWorld 토지대장 (`vworldkr__land_register`) | `open_api_only` |

## 정제 표의 미연결 구간

| 표 | 상태 | 이유 |
|---|---|---|
| `silver.building_register_floors` | 실행 경로 있음 | Silver 변환은 있으나 서빙으로 가는 실행 경로는 아직 없다. |
| `silver.building_register_unit_areas` | 실행 경로 있음 | Silver 변환은 있으나 서빙으로 가는 실행 경로는 아직 없다. |
| `silver.complex_parcel_memberships` | 계약만 있음 | 계약은 있으나 현재 Rust·Spark 코드에서 생산 레인을 찾지 못했다. |
| `gold.complex_spatial_locator` | 계약만 있음 | 계약은 있으나 현재 Rust·Spark 코드에서 생산 레인을 찾지 못했다. |

## 서빙·운영 원장 전체 목록

사용자 데이터와 품질·수집·발행 원장을 모두 포함합니다. 각 물리 표는 정확히 한 그룹에만 속합니다.

| 책임 | 물리 표 |
|---|---|
| 필지 기본·식별자 | `catalog.parcel`<br>`catalog.parcel_identifier` |
| 필지 용도지역 | `catalog.parcel_zoning` |
| 필지 공시지가 | `catalog.parcel_price` |
| 필지 토지특성 | `catalog.parcel_characteristic` |
| 필지 임야대장 | `catalog.parcel_forest_ledger` |
| 필지 토지이동 사건 | `catalog.parcel_transfer_event` |
| 건물 | `catalog.building` |
| 건물의 호 | `catalog.building_unit` |
| 산업단지 | `catalog.industrial_complex` |
| 산업단지 프로필 포인터 | `catalog.industrial_complex_gold_pointer` |
| 필지의 산업단지 소속 | `catalog.parcel_complex_membership` |
| 업종 분류·허용 | `catalog.industry_group`<br>`catalog.industry_group_member`<br>`catalog.allowed_industry`<br>`catalog.parcel_industry_assignment` |
| 공장·제조업체 | `catalog.manufacturer` |
| 산업단지 고시·첨부 | `catalog.complex_notice`<br>`catalog.complex_attachment`<br>`catalog.notice_attachment` |
| 설계·디지털 트윈 | `catalog.blueprint`<br>`catalog.digital_twin_asset` |
| 행정구역 원장 | `catalog.administrative_unit`<br>`catalog.administrative_unit_identifier`<br>`catalog.administrative_unit_parent`<br>`catalog.administrative_unit_transition`<br>`catalog.parcel_administrative_unit` |
| 행정경계 리비전 | `catalog.administrative_boundary_revision` |
| 원천 등록부 | `catalog.source_catalog` |
| 원천 기록 | `catalog.source_record` |
| 파일 자산 | `catalog.file_asset` |
| 수집 객체 원장 | `catalog.bronze_object` |
| 수집 작업 | `catalog.collection_job` |
| 수집 실행 | `catalog.ingestion_run` |
| 레이크하우스 자산·버전 | `catalog.lakehouse_data_asset`<br>`catalog.lakehouse_dataset_version`<br>`catalog.lakehouse_object_artifact` |
| 레이크하우스 저장·접근 정책 | `catalog.lakehouse_storage_namespace`<br>`catalog.lakehouse_access_policy` |
| 레이크하우스 실행·품질·계보 | `catalog.lakehouse_batch_run`<br>`catalog.lakehouse_quality_check`<br>`catalog.lakehouse_lineage_edge` |
| 카탈로그 변경 원장 | `catalog.catalog_edit`<br>`catalog.catalog_mutation_idempotency` |
| 정규화 제안·검토 | `catalog.normalization_application`<br>`catalog.normalization_proposal`<br>`catalog.normalization_proposal_review`<br>`catalog.normalization_proposal_submission_audit` |
| 이벤트 발행·격리 | `catalog.outbox_event`<br>`catalog.outbox_quarantine` |
| 필지 마커 | `catalog.parcel_marker_anchor`<br>`catalog.parcel_marker_anchor_generation_run` |
| 발행 리비전·증거 | `catalog.publication_revision`<br>`catalog.parcel_publication_source_evidence` |
| 공간 레이어·스키마 | `catalog.spatial_layer`<br>`catalog.schema_profile` |
| 타일 산출물 | `catalog.vector_tile_artifact`<br>`catalog.vector_tile_artifact_source_file_asset` |
| 타일 빌드·갱신 | `catalog.vector_tile_build_job`<br>`catalog.vector_tile_refresh_observation` |
| 타일 릴리스 | `catalog.vector_tile_release`<br>`catalog.vector_tile_release_layer`<br>`catalog.vector_tile_publication_unit` |
| 타일 매니페스트 | `catalog.vector_tile_manifest`<br>`catalog.vector_tile_runtime_manifest`<br>`catalog.vector_tile_runtime_manifest_pointer`<br>`catalog.vector_tile_runtime_manifest_unit` |
| 필지 경계 서빙 | `serving_postgis.parcel_boundary_mirror`<br>`serving_postgis.parcel_boundary_mirror_rebuild_run`<br>`serving_postgis.parcel_boundary_publication` |
| 행정경계 서빙 | `serving_postgis.administrative_unit_boundary_publication` |
| 산업단지 경계 서빙 | `serving_postgis.industrial_complex_boundary_publication` |
| 공간 적재 원장 | `serving_postgis.spatial_projection_load` |

## 운영·제공 접점

| 접점 | 상태 | 역할 |
|---|---|---|
| 산업단지 Gold 포인터 발행 (`industrial-complex-gold-pointer-publish`) | 실행 증거 대기 | 원천·파일·Gold 포인터·outbox 이벤트를 함께 발행한다. |
| 지도 타일 승격 (`vector-tile-manifest-promote`) | 실행 증거 대기 | 검증된 타일 매니페스트 버전을 현재 제공 버전으로 승격한다. |
| 지도 타일 되돌림 (`vector-tile-manifest-rollback`) | 실행 증거 대기 | 이전 타일 매니페스트 버전으로 되돌린다. |
| 레이크하우스 품질 검사 (`lakehouse-quality-gate`) | 실행 증거 대기 | 변환 결과가 품질 기준을 어기면 발행을 막는다. |
| 레이크하우스 계보 이벤트 (`lakehouse-lineage-event`) | 실행 증거 대기 | 원천·정제·품질·산출물의 연결 근거를 발행한다. |
| 객체 저장소 재고 감사 (`r2-inventory-audit`) | 실행 증거 대기 | 저장 공간의 객체 재고와 삭제 후보를 확인한다. |
| 신선도·실행시간 기준 (`lakehouse-slo-policy`) | 실행 증거 대기 | 수집과 변환의 지연·실패 신호를 관측한다. |
| 운영 관측 화면 (`grafana-lakehouse-dashboard`) | 실행 증거 대기 | 레이크하우스와 이벤트 대기열 지표를 표시한다. |
| 카탈로그 변경 전달 (`outbox-catalog-event-fanout`) | 실행 증거 대기 | 카탈로그 변경 이벤트를 제품 수신기로 전달한다. |
| 이벤트 계약 등록부 (`event-fabric-registry`) | 예정 | 현재 webhook 및 향후 브로커 연결의 이벤트 계약을 기록한다. |
| 향후 이벤트 소비자 (`future-event-fabric-consumers`) | 예정 | 검색·AI·알림 소비자는 초기 출시 범위 밖의 계획이다. |
| 공짱 이벤트 수신기 (`gongzzang-catalog-consumer-receiver`) | 상태 미확인 | 공짱에서 카탈로그 변경을 수신한다. 배포된 수신기 검증은 별도 증거가 필요하다. |
| 다우니어 이벤트 수신기 (`dawneer-catalog-consumer-receiver`) | 상태 미확인 | 다우니어 수신 계약이며 실제 배포 상태는 확인되지 않았다. |
| 운영 계보 수신기 (`openlineage-production-receiver`) | 운영 증거 없음 | 운영 수신·보존 증거가 아직 첨부되지 않았다. |
| 전체 파이프라인 운영 조율 (`production-orchestrator`) | 운영 증거 없음 | 매일 수집 타이머와 별개로 전체 변환의 재시도·배압 운영 증거가 아직 없다. |
| 배포된 수신기 종단 검증 (`consumer-deployed-receiver-e2e`) | 증거 미충족 | 카탈로그 변경이 배포된 제품 수신기에서 처리된 증거가 필요하다. |
| 운영 관측 화면 배포 (`production-dashboard-deployment`) | 운영 증거 없음 | 실제 배포된 대시보드와 알림 경로 증거가 필요하다. |
| 릴리스 공급망 검사 (`supply-chain-release-gates`) | 승인 증거 필요 | 릴리스 차단·서명·소프트웨어 구성 명세의 승인 증거를 요구한다. |
| 운영 알림 전달 (`production-alert-routing`) | 운영 증거 없음 | 실제 운영 알림 경로와 수신 증거가 필요하다. |
| 필지 지도 타일 (`parcel-tiles`) | 실행 경로 있음 | 필지 경계 발행 및 runtime 매니페스트로 제공한다. 첫 통합 매니페스트 세대는 2 이상이다. |
| 행정경계 지도 타일 (`admin-tiles`) | 실행 경로 있음 | 읍면동 경계를 제공한다. 최초 통합 매니페스트 세대는 3이다. |
| 산업단지 지도 타일 (`complex-tiles`) | 실행 경로 있음 | 산업단지 경계를 동적 또는 정적 타일로 제공한다. |
| 카탈로그 조회 API (`catalog-read-api-smoke`) | 실행 경로 있음 | 필지·건물·호·산업단지를 조회한다. 빈 속성은 빈 상태로 반환한다. |
| 산업단지 프로필 게이트웨이 (`profile-gateway`) | 실행 경로 있음 | 비공개 Gold 프로필 객체를 HTTP로 제공한다. 현재 공짱 패널의 직접 읽기 경로는 확인되지 않았다. |
| 공짱 지도·상세 패널 (`gongzzang-panel`) | 실행 경로 있음 | 공개 HTTP 계약을 소비해 지도와 필지·건물·산업단지 정보를 표시한다. |
| 매일 원천 확인 (`daily-source-sweep`) | 실행 경로 있음 | 건축HUB 목록을 매일 살펴 새 파일을 수집한다. |
| 수집 객체 등록부 (`lakehouse-object-registry`) | 실행 경로 있음 | 수집 원장 전체에서 객체 재고를 등록·대조한다. |

## 실행 근거

| 연결 | 실행 명령·스크립트 |
|---|---|
| 브이월드 공간·토지 파일 → 필지 경계 | `export-vworld-cadastral-shapefile-silver-handoff`<br>`export-vworld-cadastral-silver-handoff-shard`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/vworld_parcel_boundaries_handoff_to_silver.py` |
| 브이월드 공간·토지 파일 → 필지별 토지이용계획 | `export-land-use-plan-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 브이월드 공간·토지 파일 → 용도지역 코드 사전 | `export-land-use-zone-code-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 브이월드 공간·토지 파일 → 필지별 공시지가 | `export-land-individual-price-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 브이월드 공간·토지 파일 → 필지별 토지특성 | `export-land-characteristic-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 브이월드 공간·토지 파일 → 필지별 임야대장 | `export-land-forest-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 브이월드 공간·토지 파일 → 필지별 토지이동이력 | `export-land-transfer-history-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 건축HUB 파일 → 건물 표제부 | `export-building-register-title-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 건축HUB 파일 → 건물 층별 정보 | `export-building-register-floor-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 건축HUB 파일 → 건물 호별 정보 | `export-building-register-unit-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 건축HUB 파일 → 호별 전유·공용 면적 | `export-building-register-unit-area-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py` |
| 산업입지정보 산업단지 → 산업단지 기본 정보 | `build-industrial-complex-address-resolution`<br>`export-industrial-complex-bronze-raw-jsonl`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py` |
| 브이월드 공간·토지 파일 → 산업단지 경계 | `export-industrial-complex-boundary-silver-handoff`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_boundaries_handoff_to_silver.py` |
| 산업단지 기본 정보 → 산업단지 경계 | `platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_boundaries_handoff_to_silver.py` |
| 산업단지 기본 정보 → 산업단지 제공용 프로필 | `platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_silver_to_gold.py` |
| 필지 경계 → 필지 기본·식별자 | `load-parcel-catalog-projection` |
| 필지 경계 → 필지 경계 서빙 | `rebuild-postgis-parcel-boundary-mirror-national-from-contract`<br>`publish-parcel-boundary-postgis` |
| 필지별 토지이용계획 → 필지 용도지역 | `load-parcel-zoning-catalog-projection` |
| 용도지역 코드 사전 → 필지 용도지역 | `load-parcel-zoning-catalog-projection` |
| 필지별 공시지가 → 필지 공시지가 | `load-parcel-price-catalog-projection` |
| 필지별 토지특성 → 필지 토지특성 | `load-parcel-characteristic-catalog-projection` |
| 필지별 임야대장 → 필지 임야대장 | `load-parcel-forest-ledger-catalog-projection` |
| 필지별 토지이동이력 → 필지 토지이동 사건 | `load-parcel-transfer-event-catalog-projection` |
| 건물 표제부 → 건물 | `platforms/foundation-platform/infra/lakehouse/spark/jobs/building_titles_catalog_handoff.py`<br>`load-building-catalog-projection` |
| 건물 호별 정보 → 건물의 호 | `platforms/foundation-platform/infra/lakehouse/spark/jobs/building_register_units_parcel_handoff.py`<br>`load-building-unit-catalog-projection`<br>`load-building-unit-building-link` |
| 산업단지 기본 정보 → 산업단지 | `load-industrial-complex-canonical` |
| 산업단지 경계 → 산업단지 경계 서빙 | `platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_boundaries_silver_to_postgis_handoff.py`<br>`publish-industrial-complex-boundary-postgis` |
| 산업단지 제공용 프로필 → 산업단지 Gold 포인터 발행 | `export-industrial-complex-gold-profiles`<br>`publish-industrial-complex-gold-pointer` |
| 산업단지 Gold 포인터 발행 → 산업단지 프로필 포인터 | `publish-industrial-complex-gold-pointer` |
| 브이월드 공간·토지 파일 → 행정경계 서빙 | `platforms/foundation-platform/scripts/tiles/admin-boundary/convert.sh`<br>`platforms/foundation-platform/scripts/tiles/admin-boundary/merge.py`<br>`write-official-administrative-boundary-source-snapshot`<br>`register-serving-source-lineage`<br>`publish-administrative-boundary-postgis` |
| 필지 경계 서빙 → 필지 지도 타일 | `promote-parcel-boundary-runtime` |
| 행정경계 서빙 → 행정경계 지도 타일 | `promote-administrative-boundary-runtime` |
| 산업단지 경계 서빙 → 산업단지 지도 타일 | `publish-industrial-complex-boundary-static-release` |
| 필지 기본·식별자 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 필지 용도지역 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 필지 공시지가 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 필지 토지특성 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 필지 임야대장 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 필지 토지이동 사건 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 건물 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 건물의 호 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 산업단지 → 카탈로그 조회 API | `platforms/foundation-platform/services/foundation-api/src/routes/mod.rs` |
| 산업단지 프로필 포인터 → 산업단지 프로필 게이트웨이 | `platforms/foundation-platform/services/foundation-profile-gateway/src/index.ts` |
| 필지 지도 타일 → 공짱 지도·상세 패널 | `products/gongzzang/apps/web/lib/map/foundation-vector-fill-layers.ts` |
| 행정경계 지도 타일 → 공짱 지도·상세 패널 | `products/gongzzang/apps/web/lib/map/foundation-vector-fill-layers.ts` |
| 산업단지 지도 타일 → 공짱 지도·상세 패널 | `products/gongzzang/apps/web/lib/map/foundation-vector-fill-layers.ts` |
| 카탈로그 조회 API → 공짱 지도·상세 패널 | `products/gongzzang/services/gongzzang-api/src/foundation_parcel_lookup.rs`<br>`products/gongzzang/services/gongzzang-api/src/complex_reader.rs`<br>`products/gongzzang/apps/web/components/panels/parcel/summary.tsx`<br>`products/gongzzang/apps/web/components/panels/complex/summary.tsx` |
| 매일 원천 확인 → 건축HUB 파일 | `platforms/foundation-platform/scripts/ops/daily-source-sweep.sh` |
| 수집 객체 원장 → 수집 객체 등록부 | `seed-lakehouse-registry`<br>`verify-lakehouse-registry` |
| 수집 객체 등록부 → 레이크하우스 자산·버전 | `seed-lakehouse-registry` |
| 산업단지 제공용 프로필 → 레이크하우스 품질 검사 | `platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_silver_to_gold.py` |
| 레이크하우스 품질 검사 → 산업단지 Gold 포인터 발행 | `publish-industrial-complex-gold-pointer` |
| 산업단지 제공용 프로필 → 레이크하우스 계보 이벤트 | `publish-lakehouse-lineage-event` |
| 산업단지 Gold 포인터 발행 → 카탈로그 변경 전달 | `publish-industrial-complex-gold-pointer` |
| 카탈로그 변경 전달 → 이벤트 계약 등록부 | `publish-outbox-once` |
| 카탈로그 변경 전달 → 공짱 이벤트 수신기 | `publish-outbox-once` |
| 카탈로그 변경 전달 → 다우니어 이벤트 수신기 | `publish-outbox-once` |
| 브이월드 공간·토지 파일 → 산업단지 기본 정보 | `build-industrial-complex-address-resolution`<br>`export-industrial-complex-bronze-raw-jsonl`<br>`platforms/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py` |

## 이전 지도에서 바뀐 점

- v1의 원천별 수집·변환 작업 노드는 원천 그룹과 via로 합쳤다. 운영 조회에 쓰이는 노드 ID와 runtime_bindings는 보존했다.
- silver.building_footprints와 JUSO 변환은 계약·생산자가 없어 제거했다.
- canonical-silver-gold-live-write 및 postgis-mirror-and-dlq-schema의 과거 정적 차단 상태는 최신 계약·마이그레이션·runtime 증거로 대체한다.
- silver.complex_parcel_memberships 및 gold.complex_spatial_locator는 생산자 없이 계약만 있어 연결선을 만들지 않는다.
- vworldkr__sandan_profile·sandan_boundary 및 ILIS 주소 근거는 실제 생산 코드에서 확인한 연결이다. 다른 sandan 원천까지 연결되었다고 추정하지 않는다.

## 갱신 방법

원천 카탈로그·레이크하우스 계약·마이그레이션을 변경하면 그래프도 함께 갱신합니다.
`pipeline-graph-covers-every-dataset.sh`가 세 집합과 중복·엣지 끝점을 대조하고,
`render-pipeline-map.py --check`가 이 문서와 API 예제가 정본에서 생성된 바이트인지 확인합니다.
