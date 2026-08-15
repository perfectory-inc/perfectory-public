---
status: current
owner: foundation-platform
doc_type: catalog
last_reviewed: 2026-07-30
---

<!-- GENERATED FILE. Do not edit by hand. -->
<!-- Render with: python3 scripts/catalog/render-public-data-catalog.py -->

# 공공데이터 수집 카탈로그

> 이 문서는 사람이 읽는 색인입니다. 정확한 endpoint, source slug, 수집 레인, 허용 여부의 기준은 아래 JSON SSOT입니다.
> 숫자와 표는 JSON에서 자동 생성되므로 이 문서에 직접 목록을 추가하지 않습니다.

## 기준 문서

- [공공 소스 엔드포인트 카탈로그](./public-source-endpoint-catalog.v1.json) — 무엇을 수집할 수 있는지
- [Bronze 수집 레인 레지스트리](./public-data-bronze-lane-registry.v1.json) — 어떤 방식으로 실행하는지
- [Bronze source slug 매핑](./bronze-source-slug-rename.v1.md) — 제공기관·데이터명·슬러그 참고표
- [Bronze 레인 실행 런북](../runbooks/public-data-bronze-lane-orchestration.md) — 실행 게이트와 운영 절차

## 현재 카탈로그 규모

- 엔드포인트 정의: **130개**
- 고유 dataset slug: **111개**
- 고유 Bronze source slug: **130개**
- 국가 수집 허용 endpoint: **82개**
- 기본 실행 레인에 포함되는 endpoint: **81개**

## 제공기관별 정리

| 제공기관 | endpoint 수 | 국가 수집 허용 | 기본 실행 | 주요 상태 |
|---|---:|---:|---:|---|
| data.go.kr | 22 | 0 | 0 | 중복 API 비활성 22 |
| factoryon.go.kr | 1 | 0 | 0 | 수동 승인 1 |
| hub.go.kr | 59 | 57 | 57 | 기본 실행 57, 제공기관 목록 없음 2 |
| juso.go.kr | 11 | 0 | 0 | 수동 승인 11 |
| mixed_public_source | 10 | 0 | 0 | 수동 승인 10 |
| mois.go.kr | 2 | 0 | 0 | 수동 승인 2 |
| vworld.kr | 25 | 25 | 24 | API 예정 1, 기본 실행 24 |

## 수집 레인

| 레인 | 상태 | 기본 포함 | 제공기관 | endpoint 그룹 |
|---|---|---:|---|---|
| `data-go-kr-api` | planned | false | data.go.kr | real_transaction_open_api |
| `building-hub-bulk` | enabled | true | hub.go.kr | building_hub_bulk |
| `vworld-dataset-file` | enabled | true | vworld.kr | vworld_dataset |
| `vworld-ned-open-api` | planned | false | vworld.kr | vworld_ned_open_api |

## 취급 데이터 묶음

| endpoint 그룹 | 수 | 설명 |
|---|---:|---|
| `building_hub_bulk` | 59 | hub.go.kr 건축물·허가·에너지·점검 벌크 파일 |
| `building_register_open_api` | 10 | data.go.kr 건축물대장 API 중복 경로 |
| `juso_electronic_map_bulk` | 11 | juso.go.kr 주소정보 전자지도 벌크 (수동 승인) |
| `other_bulk` | 13 | 학교·공장·인구·교통 등 추가 벌크 (수동 승인) |
| `real_transaction_open_api` | 12 | data.go.kr 실거래 API 보조·검증 경로 |
| `vworld_dataset` | 24 | vworld.kr 제공기관 데이터 파일 |
| `vworld_ned_open_api` | 1 | vworld.kr NED API (현재 기본 실행 제외) |

## Foundation 건축물 데이터 세부 분류

| Hub 작업 그룹 | endpoint 수 | 의미 |
|---|---:|---|
| `01` | 17 | 건축허가 |
| `02` | 16 | 주택허가 |
| `03` | 10 | 건축물대장 |
| `04` | 10 | 폐쇄말소대장 |
| `05` | 2 | 연간 에너지 |
| `06` | 2 | 건축물 유지관리 |
| `08` | 2 | 월별 에너지 |

### 폐쇄말소대장

폐쇄말소대장은 Hub group `04`의 10개 벌크 endpoint로 Bronze 수집 대상입니다. 현재 catalog의 `product_semantics`는 `forbidden`, Silver 상태는 `planned`이므로, 원시 보관과 상품 제공을 구분합니다.

### 연간 에너지

연간 전기·가스 2개 endpoint는 카탈로그에는 남아 있지만 현재 제공기관 inventory 행이 없어 `provider_inventory_missing`으로 비활성화되어 있습니다.

## 수집 데이터 한글 목록

> 사람이 보는 목록입니다. endpoint·dataset slug·Bronze source slug 같은 기술 식별자는 [공공 소스 엔드포인트 카탈로그](./public-source-endpoint-catalog.v1.json)에서 확인합니다.

| 제공기관 | 데이터 종류 | 수집 데이터(한글명) | 수집 방식 | 국가 수집 허용 | 현재 상태 |
|---|---|---|---|---:|---|
| data.go.kr | 건축물대장 API | 건축물대장 부속지번 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 기본개요 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 전유부 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 전유공용면적 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 층별개요 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 주택가격 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 지구지역구역 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 총괄표제부 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 표제부 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 건축물대장 API | 건축물대장 오수정화시설 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 아파트 전월세 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 아파트 매매 상세 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 공장창고 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 토지 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 상업업무용 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 오피스텔 전월세 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 오피스텔 매매 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 연립다세대 전월세 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 연립다세대 매매 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 단독다가구 전월세 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 단독다가구 매매 실거래가 | 중복 API | false | 중복 API 비활성 |
| data.go.kr | 부동산 실거래 | 아파트 분양권전매 실거래가 | 중복 API | false | 중복 API 비활성 |
| factoryon.go.kr | 기타 공공데이터 | 공장등록현황 | 벌크(수동 승인) | false | 수동 승인 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건물 전기 사용량 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 지번별에너지(전기,연도별) | 제공기관 목록 없음 | false | 제공기관 목록 없음 |
| hub.go.kr | 건축물·허가·에너지·점검 | 지번별에너지(가스,연도별) | 제공기관 목록 없음 | false | 제공기관 목록 없음 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건물 가스 사용량 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 점검기관 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 정기점검이력 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 부설주차장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 기본개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 동별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 철거멸실관리대장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 지역지구구역 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 전유공용면적 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 층별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 주택유형 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 대수선 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 주차장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 도로대장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 오수정화시설 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 대지위치 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 공작물관리대장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 가설건축물 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 호별전유공용면적 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 호별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 기본 정보 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 공동주택 가격 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 공동주택가격 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 기본개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 지역지구구역 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 전유공용면적 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 전유부 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 층별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 표제부 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 총괄표제부 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 오수정화시설 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 부속지번 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 지역지구구역 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 전유공용면적 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 전유부 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 층별개요 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 표제부 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 총괄표제부 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 오수정화시설 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 건축물대장 부속지번 파일 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 행위개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 행위호전유공용면적 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 부대시설 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 부설주차장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 기본개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 동별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 지역지구구역 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 전유공용면적 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 층별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 관리공동부대복리시설 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 관리공동형별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 주차장 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 오수정화시설 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 대지위치 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 호별개요 | bulk_file | true | 기본 실행 |
| hub.go.kr | 건축물·허가·에너지·점검 | 복리분양시설 | bulk_file | true | 기본 실행 |
| juso.go.kr | 주소정보 전자지도 | JUSO 기초구간 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 국가기초구역 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 건물 도형 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 건물 출입구 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 건물군 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 법정구역 읍면동 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 법정구역 리 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 법정구역 시도 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 법정구역 시군구 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 도로구간 | 벌크(수동 승인) | false | 수동 승인 |
| juso.go.kr | 주소정보 전자지도 | JUSO 실폭도로 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 행정동 법정동 연계 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 공항 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 상권 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 고속도로 접근점 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 항만 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 철도역 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 철도역 노선 매핑 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 학교 위치 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 도시철도역 | 벌크(수동 승인) | false | 수동 승인 |
| mixed_public_source | 기타 공공데이터 | 대학교 | 벌크(수동 승인) | false | 수동 승인 |
| mois.go.kr | 기타 공공데이터 | 동 인구 | 벌크(수동 승인) | false | 수동 승인 |
| mois.go.kr | 기타 공공데이터 | 세대수 | 벌크(수동 승인) | false | 수동 승인 |
| vworld.kr | 공간·토지·산업단지 | VWorld 통계 읍면동 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 통계 시도 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 통계 시군구 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 읍면동 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 시도 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 시군구 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지특성 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 임야 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 개별공시지가 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지소유 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지권리등록 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지이동연혁 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지이용계획 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지이용구역 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 토지이용구역 코드 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 필지 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 공인중개사 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 경계 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 시설용지 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 용도지역 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 위치 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 필지 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 유치업종 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 공간·토지·산업단지 | VWorld 산업단지 개요 | 제공기관 파일 | true | 기본 실행 |
| vworld.kr | 수치표고(NED) | VWorld 토지대장 | 공개 API | true | API 예정 |

## 문서 유지 규칙

1. 정확한 데이터 목록과 상태는 JSON SSOT만 수정합니다.
2. 변경 후 `python3 scripts/catalog/render-public-data-catalog.py`를 실행합니다.
3. CI는 `--check`로 생성 결과가 커밋된 문서와 같은지 검사합니다.
4. Bronze에 수집된다는 사실만으로 Silver/Gold 또는 제품 공개가 완료된 것으로 보지 않습니다.
