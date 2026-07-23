# Bronze source slug 통일 (v1, owner-approved SSOT)

> 규칙: `source_slug = {providerid}__{dataset_slug}` (소문자, 이중 언더스코어).
> **`operation`(API 호출용 식별자) != `dataset_slug`(의미 식별자)** - 생성기는 `dataset_slug`로 만든다(ADR 0014 D3).
> providerid: `vworldkr / datagokr / hubgokr / jusogokr / moisgokr / factoryongokr`.
> 근거: dbt `source__entity` + BigQuery(`-` 금지)/Databricks(소문자) + AWS(소스별). ADR 0014.
> 이 표는 마이그레이션 old->new 매핑이자 **operation->dataset_slug 맵**이다.

## 1. data.go.kr (22) - operation != dataset_slug (핵심)

### 건축물대장 API (10) - `getBr*` operation -> 의미 dataset_slug
| operation (API) | dataset_slug | 새 source_slug | 현재(old) slug | 한국어 |
|---|---|---|---|---|
| getBrTitleInfo | building_register_main | `datagokr__building_register_main` | data-go-kr-building-register-getbrtitleinfo | 표제부 |
| getBrRecapTitleInfo | building_register_master | `datagokr__building_register_master` | ...-getbrrecaptitleinfo | 총괄표제부 |
| getBrExposInfo | building_register_exclusive_unit | `datagokr__building_register_exclusive_unit` | ...-getbrexposinfo | 전유부 |
| getBrExposPubuseAreaInfo | building_register_exclusive_common_area | `datagokr__building_register_exclusive_common_area` | ...-getbrexpospubuseareainfo | 전유공용면적 |
| getBrFlrOulnInfo | building_register_floor_overview | `datagokr__building_register_floor_overview` | ...-getbrflroulninfo | 층별개요 |
| getBrHsprcInfo | building_register_house_price | `datagokr__building_register_house_price` | ...-getbrhsprcinfo | 주택가격 |
| getBrJijiguInfo | building_register_district_zone | `datagokr__building_register_district_zone` | ...-getbrjijiguinfo | 지구지역구역 |
| getBrWclfInfo | building_register_sewage_facility | `datagokr__building_register_sewage_facility` | ...-getbrwclfinfo | 오수정화시설 |
| getBrAtchJibunInfo | building_register_sub_parcel | `datagokr__building_register_sub_parcel` | ...-getbratchjibuninfo | 부속지번 |
| getBrBasisOulnInfo | building_register_basis_outline | `datagokr__building_register_basis_outline` | ...-getbrbasisoulninfo | 기본개요 |

### 실거래가 API (12) - `getRTMSDataSvc*` operation -> dataset_slug
| operation (API) | dataset_slug | 새 source_slug | 한국어 |
|---|---|---|---|
| getRTMSDataSvcAptTradeDev | real_transaction_apartment_trade | `datagokr__real_transaction_apartment_trade` | 아파트 매매 |
| getRTMSDataSvcAptRent | real_transaction_apartment_rent | `datagokr__real_transaction_apartment_rent` | 아파트 전월세 |
| getRTMSDataSvcOffiTrade | real_transaction_officetel_trade | `datagokr__real_transaction_officetel_trade` | 오피스텔 매매 |
| getRTMSDataSvcOffiRent | real_transaction_officetel_rent | `datagokr__real_transaction_officetel_rent` | 오피스텔 전월세 |
| getRTMSDataSvcRHTrade | real_transaction_row_house_trade | `datagokr__real_transaction_row_house_trade` | 연립다세대 매매 |
| getRTMSDataSvcRHRent | real_transaction_row_house_rent | `datagokr__real_transaction_row_house_rent` | 연립다세대 전월세 |
| getRTMSDataSvcSHTrade | real_transaction_detached_house_trade | `datagokr__real_transaction_detached_house_trade` | 단독다가구 매매 |
| getRTMSDataSvcSHRent | real_transaction_detached_house_rent | `datagokr__real_transaction_detached_house_rent` | 단독다가구 전월세 |
| getRTMSDataSvcNrgTrade | real_transaction_commercial_trade | `datagokr__real_transaction_commercial_trade` | 상업업무용 매매 |
| getRTMSDataSvcInduTrade | real_transaction_industrial_trade | `datagokr__real_transaction_industrial_trade` | 공장창고 매매 |
| getRTMSDataSvcLandTrade | real_transaction_land_trade | `datagokr__real_transaction_land_trade` | 토지 매매 |
| getRTMSDataSvcSilvTrade | real_transaction_apartment_presale | `datagokr__real_transaction_apartment_presale` | 아파트 분양권전매 |

## 2. hub.go.kr (59) - operation == dataset_slug

### 2.1 건축물대장 벌크 (group 03, 12) - 기존 등록분
> Phase 3b 일관성 rename 적용: group 03/0301 `building_register` -> `building_register_basis_outline`
> (data.go.kr `getBrBasisOulnInfo`와 동일한 dataset_slug = 기본개요. provider가 달라 source_slug는 분리됨).

| dataset_slug (= operation) | 새 source_slug | 현재(old) slug | 한국어 |
|---|---|---|---|
| building_register_basis_outline | `hubgokr__building_register_basis_outline` | hub-building-building-register | 건축물대장 기본 정보 (기본개요) |
| building_register_apartment_price | `hubgokr__building_register_apartment_price` | ...-apartment-price | 공동주택 가격 |
| building_register_district_zone | `hubgokr__building_register_district_zone` | ...-district-zone | 지역지구구역 |
| building_register_exclusive_common_area | `hubgokr__building_register_exclusive_common_area` | ...-exclusive-common-area | 전유공용면적 |
| building_register_exclusive_unit | `hubgokr__building_register_exclusive_unit` | ...-exclusive-unit | 전유부 |
| building_register_floor_overview | `hubgokr__building_register_floor_overview` | ...-floor-overview | 층별개요 |
| building_register_main | `hubgokr__building_register_main` | ...-main | 표제부 |
| building_register_master | `hubgokr__building_register_master` | ...-master | 총괄표제부 |
| building_register_sewage_facility | `hubgokr__building_register_sewage_facility` | ...-sewage-facility | 오수정화시설 |
| building_register_sub_parcel | `hubgokr__building_register_sub_parcel` | ...-sub-parcel | 부속지번 |

### 2.2 건축인허가 벌크 (group 01, 17 신규) - Phase 3b
| code | 한국어 | dataset_slug | 새 source_slug |
|---|---|---|---|
| 0101 | 기본개요 | building_permit_basis_outline | `hubgokr__building_permit_basis_outline` |
| 0102 | 동별개요 | building_permit_building_outline | `hubgokr__building_permit_building_outline` |
| 0103 | 층별개요 | building_permit_floor_overview | `hubgokr__building_permit_floor_overview` |
| 0104 | 호별개요 | building_permit_unit_outline | `hubgokr__building_permit_unit_outline` |
| 0105 | 대수선 | building_permit_major_repair | `hubgokr__building_permit_major_repair` |
| 0106 | 공작물관리대장 | building_permit_structure_ledger | `hubgokr__building_permit_structure_ledger` |
| 0107 | 철거멸실관리대장 | building_permit_demolition_loss | `hubgokr__building_permit_demolition_loss` |
| 0108 | 가설건축물 | building_permit_temporary_building | `hubgokr__building_permit_temporary_building` |
| 0109 | 오수정화시설 | building_permit_sewage_facility | `hubgokr__building_permit_sewage_facility` |
| 0110 | 주차장 | building_permit_parking | `hubgokr__building_permit_parking` |
| 0111 | 부설주차장 | building_permit_annex_parking | `hubgokr__building_permit_annex_parking` |
| 0112 | 전유공용면적 | building_permit_exclusive_common_area | `hubgokr__building_permit_exclusive_common_area` |
| 0113 | 호별전유공용면적 | building_permit_unit_exclusive_common_area | `hubgokr__building_permit_unit_exclusive_common_area` |
| 0114 | 지역지구구역 | building_permit_district_zone | `hubgokr__building_permit_district_zone` |
| 0115 | 도로대장 | building_permit_road_ledger | `hubgokr__building_permit_road_ledger` |
| 0116 | 대지위치 | building_permit_site_location | `hubgokr__building_permit_site_location` |
| 0117 | 주택유형 | building_permit_housing_type | `hubgokr__building_permit_housing_type` |

### 2.3 주택인허가 벌크 (group 02, 16 신규) - Phase 3b
| code | 한국어 | dataset_slug | 새 source_slug |
|---|---|---|---|
| 0201 | 기본개요 | housing_permit_basis_outline | `hubgokr__housing_permit_basis_outline` |
| 0202 | 동별개요 | housing_permit_building_outline | `hubgokr__housing_permit_building_outline` |
| 0203 | 층별개요 | housing_permit_floor_overview | `hubgokr__housing_permit_floor_overview` |
| 0204 | 호별개요 | housing_permit_unit_outline | `hubgokr__housing_permit_unit_outline` |
| 0205 | 부대시설 | housing_permit_ancillary_facility | `hubgokr__housing_permit_ancillary_facility` |
| 0206 | 오수정화시설 | housing_permit_sewage_facility | `hubgokr__housing_permit_sewage_facility` |
| 0207 | 주차장 | housing_permit_parking | `hubgokr__housing_permit_parking` |
| 0208 | 부설주차장 | housing_permit_annex_parking | `hubgokr__housing_permit_annex_parking` |
| 0209 | 전유공용면적 | housing_permit_exclusive_common_area | `hubgokr__housing_permit_exclusive_common_area` |
| 0210 | 행위호전유공용면적 | housing_permit_act_unit_exclusive_common_area | `hubgokr__housing_permit_act_unit_exclusive_common_area` |
| 0211 | 행위개요 | housing_permit_act_outline | `hubgokr__housing_permit_act_outline` |
| 0212 | 관리공동형별개요 | housing_permit_managed_communal_type_outline | `hubgokr__housing_permit_managed_communal_type_outline` |
| 0213 | 관리공동부대복리시설 | housing_permit_managed_communal_ancillary_welfare_facility | `hubgokr__housing_permit_managed_communal_ancillary_welfare_facility` |
| 0214 | 지역지구구역 | housing_permit_district_zone | `hubgokr__housing_permit_district_zone` |
| 0215 | 대지위치 | housing_permit_site_location | `hubgokr__housing_permit_site_location` |
| 0216 | 복리분양시설 | housing_permit_welfare_sale_facility | `hubgokr__housing_permit_welfare_sale_facility` |

### 2.4 폐쇄말소대장 벌크 (group 04, 10 신규) - Phase 3b
| code | 한국어 | dataset_slug | 새 source_slug |
|---|---|---|---|
| 0401 | 기본개요 | building_register_closed_basis_outline | `hubgokr__building_register_closed_basis_outline` |
| 0402 | 총괄표제부 | building_register_closed_master | `hubgokr__building_register_closed_master` |
| 0403 | 표제부 | building_register_closed_main | `hubgokr__building_register_closed_main` |
| 0404 | 층별개요 | building_register_closed_floor_overview | `hubgokr__building_register_closed_floor_overview` |
| 0405 | 부속지번 | building_register_closed_sub_parcel | `hubgokr__building_register_closed_sub_parcel` |
| 0406 | 전유공용면적 | building_register_closed_exclusive_common_area | `hubgokr__building_register_closed_exclusive_common_area` |
| 0407 | 오수정화시설 | building_register_closed_sewage_facility | `hubgokr__building_register_closed_sewage_facility` |
| 0408 | 전유부 | building_register_closed_exclusive_unit | `hubgokr__building_register_closed_exclusive_unit` |
| 0409 | 공동주택가격 | building_register_closed_apartment_price | `hubgokr__building_register_closed_apartment_price` |
| 0410 | 지역지구구역 | building_register_closed_district_zone | `hubgokr__building_register_closed_district_zone` |

### 2.5 건물에너지 연도별 벌크 (group 05, 2 신규) - Phase 3b
| code | 한국어 | dataset_slug | 새 source_slug |
|---|---|---|---|
| 0501 | 지번별에너지(전기,연도별) | building_energy_yearly_electricity | `hubgokr__building_energy_yearly_electricity` |
| 0502 | 지번별에너지(가스,연도별) | building_energy_yearly_gas | `hubgokr__building_energy_yearly_gas` |

### 2.6 건축물유지관리 벌크 (group 06, 2 신규) - Phase 3b
| code | 한국어 | dataset_slug | 새 source_slug |
|---|---|---|---|
| 0606 | 점검기관 | building_maintenance_inspection_agency | `hubgokr__building_maintenance_inspection_agency` |
| 0607 | 정기점검이력 | building_maintenance_regular_inspection_history | `hubgokr__building_maintenance_regular_inspection_history` |

### 2.7 건물에너지 월별 벌크 (group 08, 2) - 기존 등록분 + Phase 3b 일관성 rename
> Phase 3b rename: 에너지 계열 네이밍을 `building_energy_{cadence}_{utility}`로 통일.
> 08/0501·0502 연도별과 08/0801·0802 월별이 같은 `building_energy_*` 패밀리를 이룸.

| code | dataset_slug (= operation) | 새 source_slug | 현재(old) slug | 한국어 |
|---|---|---|---|---|
| 0801 | building_energy_monthly_electricity | `hubgokr__building_energy_monthly_electricity` | hub-building-building-electricity-usage (구 `building_electricity_usage`) | 건물 전기 사용량 (월별) |
| 0802 | building_energy_monthly_gas | `hubgokr__building_energy_monthly_gas` | hub-building-building-gas-usage (구 `building_gas_usage`) | 건물 가스 사용량 (월별) |

> **Phase 3b rename 요약 (3건):** group 03/0301 `building_register` -> `building_register_basis_outline`,
> 08/0801 `building_electricity_usage` -> `building_energy_monthly_electricity`,
> 08/0802 `building_gas_usage` -> `building_energy_monthly_gas`.
> 각 건 dataset_slug + bronze.source_slug 동시 변경 (operation == dataset_slug 규칙 유지).

## 3. VWorld (25) - operation == dataset_slug
모두 `vworldkr__{operation}`. boundary_census_emd/sido/sigungu, boundary_emd/sido/sigungu,
land_characteristic, land_forest, land_individual_price, land_ownership, land_register,
land_right_registration, land_transfer_history, land_use_plan, land_use_zone, land_use_zone_code,
parcel, real_estate_broker, sandan_boundary, sandan_facility_land_use, sandan_land_use_zone,
sandan_location, sandan_parcel, sandan_permitted_industry, sandan_profile.
(old `vworld-dataset-{kebab}` -> new `vworldkr__{snake}`.)

## 4. juso (11) - 중복 `juso_` prefix 제거가 dataset_slug
| dataset_slug | 새 source_slug | old slug | 한국어 |
|---|---|---|---|
| base_interval | `jusogokr__base_interval` | juso-electronic-map-juso-base-interval | 기초구간 |
| basic_area | `jusogokr__basic_area` | ...-juso-basic-area | 국가기초구역 |
| building | `jusogokr__building` | ...-juso-building | 건물 도형 |
| building_entrance | `jusogokr__building_entrance` | ...-juso-building-entrance | 건물 출입구 |
| building_group | `jusogokr__building_group` | ...-juso-building-group | 건물군 |
| legal_emd | `jusogokr__legal_emd` | ...-juso-legal-emd | 법정구역 읍면동 |
| legal_ri | `jusogokr__legal_ri` | ...-juso-legal-ri | 법정구역 리 |
| legal_sido | `jusogokr__legal_sido` | ...-juso-legal-sido | 법정구역 시도 |
| legal_sigungu | `jusogokr__legal_sigungu` | ...-juso-legal-sigungu | 법정구역 시군구 |
| road_section | `jusogokr__road_section` | ...-juso-road-section | 도로구간 |
| road_width | `jusogokr__road_width` | ...-juso-road-width | 실폭도로 |

## 5. mois.go.kr (2) + factoryon.go.kr (1) - operation == dataset_slug
| dataset_slug | 새 source_slug | old slug | 한국어 |
|---|---|---|---|
| dong_population | `moisgokr__dong_population` | public-bulk-dong-population | 동 인구 |
| household_count | `moisgokr__household_count` | public-bulk-household-count | 세대수 |
| factory_registration | `factoryongokr__factory_registration` | public-bulk-factory-registration | 공장등록현황 |

## 6. 카탈로그 밖 divergent 기본값 (감사로 발견 - 코드 상수)
| 현재(old) 상수/기본값 | 처리 |
|---|---|
| `molit-building-register` (data.go.kr 건축물대장 API 기본값) | **bare 슬러그 금지.** 실행 operation에 따라 `datagokr__building_register_main` 등 **구체** dataset_slug로 (ADR 0014 Consequences). bare 상수 제거. |
| `vworld-cadastral` (vworld_cadastral_ingest) | dataset_slug `cadastral` -> `vworldkr__cadastral` (vworldkr__parcel과 별개 데이터) |
| `vworld-land-register` (vworld_land_register_ingest) | dataset_slug `land_register` -> `vworldkr__land_register` |
| `*-national-pilot`, `*-local-proof` | **RESOLVED (Phase 3, owner-confirmed): canonical로 fold.** `-national-pilot`/`-local-proof` 접미사는 데이터 정체성이 아니라 RUN 범위 구분(run_id/manifest/local-FS prefix가 담당)이므로 source slug는 접미사를 떼고 plain canonical 생성기 출력으로 간다 (예: building-register는 실행 operation별 `datagokr__building_register_main`, cadastral은 `vworldkr__cadastral`). |

## 7. deferred (지금 안 함)
- `mixed_public_source` 10개 (POI: 고속도로IC/학교/대학/철도역/지하철/항만/공항/상권/행정동코드) - 나중에 `poi__*` 또는 실제 출처. 어드민 수기등록도 그때.
- 미등록 `hub-go-kr-public-bulk-task-*` - **RESOLVED (Phase 3, owner-confirmed): 등록 전까지 fail-closed.** 카탈로그에 없는 building_hub_bulk 인벤토리 작업은 canonical dataset_slug가 없으므로 기존 opaque `hub-go-kr-public-bulk-task-*` 슬러그를 만들지 않고 계획 단계에서 bail한다. 등록된(catalog) 작업은 이 경로에 도달하지 않고 카탈로그의 생성기 파생 `bronze.source_slug`로 처리된다. 실제 쓸 때 카탈로그에 dataset_slug를 등록하면 자동으로 풀린다.
