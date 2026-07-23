# 건축물대장·토지대장 필드 매핑 v1 DRAFT

- **상태:** DRAFT — provider schema mapping candidate. 공식 원천 스키마와 고정 fixture로 검증하기
  전에는 정규화 계약으로 사용하지 않는다.
- **Owner:** foundation-platform
- **목적:** 원천별 위치 필드를 캐노니컬 개념에 연결하기 위한 검토용 매핑을 정의한다.
- **관련 결정:** [ADR 0023 — 표준 PNU 체계](../adr/0023-standard-pnu-canonical-dialect.md),
  [ADR 0027 — normalization capability ownership](../adr/0027-normalization-capability-ownership.md)
- **운영 증거 경계:** 실제 파일 ID, 파일 크기, 표본 값, 행 수, 수집 상태, R2 인벤토리, 실행
  결과는 [루트 ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md)에 따라
  비공개로 관리한다.

이 문서의 숫자는 원천 레코드의 **0-based 필드 위치**다. 파일별 행 수나 수집 완료 수가 아니다.
`검증 필요` 표시는 공식 스키마 또는 재현 가능한 fixture가 확보되기 전까지 소비자 계약에
노출하면 안 된다.

## 공통 불변식

1. 여러 대장에 PNU 구성요소가 있지만 위치는 레코드 종류마다 다르다.
2. 건축물대장 허브의 대지구분은 표준 PNU와 코드 의미가 다르므로 ADR 0023의 단일 변환을
   거쳐야 한다. 원천 칸을 이어 붙인 값을 canonical `pnu`로 취급하지 않는다.
3. 관리 PK의 범위와 공유 여부는 레코드 종류마다 다르다. 공식 조인 계약이 없는 레코드끼리
   관리 PK가 같다고 추론하지 않는다.
4. 건축물대장 내부 필지 연결은 `register_parcel_key`, 외부 필지 연결은 표준 `pnu`를 사용한다.
   블록처럼 표준 PNU가 없는 행은 주소·lineage 기반 별도 해결 대상으로 남긴다.
5. 동명과 호명은 자유 텍스트이므로 원문을 보존한 뒤 버전된 정규화 규칙을 적용한다.

## 건축물대장 위치 매핑 후보

### 표제부 (`building_register_main`, `mart_djy_03`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | `building_registry_key` | 동 레벨 원천 관리 PK |
| 5 | `jibun_address` | 원문 보존 |
| 6 | `road_address` | 원문 보존 |
| 8/9/10/11/12 | 시군구/법정동/대지구분/번/지 | ADR 0023 변환 필요 |
| 17/18/20 | 도로명코드/새주소 법정동/본번 | 원천 값 |
| 22 | `dong_name` | 텍스트 정규화 대상 |
| 28 | `gross_floor_area` | 단위 검증 필요 |
| 29 | `far_floor_area` | 단위 검증 필요 |
| 34/35 | `main_use_code`/`main_use_name` | 코드표 버전 필요 |
| 43/44 | `above_ground_floor_count`/`below_ground_floor_count` | 옥탑 포함 의미 검증 필요 |

### 층별개요 (`building_register_floor_overview`, `mart_djy_04`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | `building_registry_key` | 표제부와의 조인 가설을 fixture로 검증해야 함 |
| 18/19/20/21 | 층구분코드/층구분명/층번호/층라벨 | 층라벨은 자유 텍스트 |

원천 층구분 코드 후보는 `10` 지하, `20` 지상, `30` 옥탑, `40` 각층, `21` 복수층하,
`22` 복수층상이다. 코드표 버전을 확인한 뒤 캐노니컬 enum에 연결한다.

### 전유부 (`building_register_exclusive_unit`, `mart_djy_09`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 전유부 관리 PK | 표제부 PK와 같은 체계라고 가정하지 않음 |
| 8~12 | PNU 구성요소 | ADR 0023 변환 필요 |
| 16/17/19 | 도로명코드/새주소 법정동/본번 | 원천 값 |
| 21 | `dong_name` | 텍스트 정규화 대상 |
| 22 | `ho_name` | 텍스트 정규화 대상 |
| 23/24/25 | 층구분코드/층구분명/층번호 | 층 계약으로 정규화 |

### 전유공용면적 (`building_register_exclusive_common_area`, `mart_djy_06`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 전유공용면적 관리 PK | 레코드 종류 전용 |
| 8~12 | PNU 구성요소 | ADR 0023 변환 필요 |
| 22 | 호 식별 후보 | 검증 필요 |
| 23/25 | 층구분코드/층번호 | 검증 필요 |
| 26 | 전유/공용 구분 후보 | 공식 코드표 필요 |
| 37 | 면적 | 단위와 null 의미 검증 필요 |

전유부와의 연결 후보는 표준 PNU 또는 `register_parcel_key`에 정규화된 동명·호명을 더한
복합 키다. 고유성 검증 없이 확정 조인으로 승격하지 않는다.

### 총괄표제부 (`building_register_master`, `mart_djy_02`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 총괄 관리 PK | 레코드 종류 전용 |
| 7/8 | 지번주소/도로명주소 | 원문 보존 |
| 10~14 | PNU 구성요소 | 표제부와 위치가 다름 |
| 24~29 | 대지면적/건축면적/건폐율/연면적/용적률산정연면적/용적률 후보 | 순서·단위 검증 필요 |

동별 표제부 연결은 관리 PK 공유를 가정하지 않고 표준 PNU 또는 `register_parcel_key`와
주소·동 식별 증거를 사용한다.

### 기본개요 (`building_register_basis_outline`, `mart_djy_01`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 기본개요 관리 PK | 범위 검증 필요 |
| 6/7 | 지번주소/도로명주소 | 원문 보존 |
| 9~13 | PNU 구성요소 | 다른 대장과 위치가 다름 |

### 부속지번 (`building_register_sub_parcel`, `mart_djy_05`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 부속지번 관리 PK | 레코드 종류 전용 |
| 8~12 | 주 PNU 구성요소 | 대표 필지 후보 |
| 23/24/25/26 | 관련 PNU 구성요소 후보 | 위치와 구성 규칙 검증 필요 |

이 레코드는 한 건물과 여러 필지의 관계를 제공할 수 있는 우선 후보다. N:M 관계를 별도
추론하기 전에 공식 부속지번 의미와 고유성을 검증한다.

### 지역지구구역 (`building_register_district_zone`, `mart_djy_10`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 지역지구 관리 PK | 공유 범위 검증 필요 |
| 3~7 | PNU 구성요소 | ADR 0023 변환 필요 |
| 13/14 | 용도지역 코드/명 | 코드표 버전 필요 |

### 공동주택가격 (`building_register_apartment_price`, `mart_djy_08`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 공동주택가격 관리 PK | 레코드 종류 전용 |
| 8~12 | PNU 구성요소 | ADR 0023 변환 필요 |
| 22 | 기준일 | 날짜 형식 검증 필요 |
| 23 | 공시가격 | 통화·단위 검증 필요 |

### 오수정화 (`building_register_sewage_facility`, `mart_djy_07`)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 오수정화 관리 PK | 공유 범위 검증 필요 |
| 8~12 | PNU 구성요소 | ADR 0023 변환 필요 |
| 21/22/23 | 정화조 용량/방법/구조 후보 | 순서·단위·코드표 검증 필요 |

## 조인 계약 후보

다음은 구현 전 증명해야 하는 가설이며 현재 완료 주장이 아니다.

- 층별개요와 표제부의 관리 PK가 같은 동을 유일하게 식별하는지 확인한다.
- 지역지구구역과 오수정화의 관리 PK 공유 범위를 확인한다.
- 다른 대장 간에는 관리 PK 공유를 가정하지 않는다.
- 표준 PNU가 존재하면 외부 필지 앵커로 사용하고, 건축물대장 내부 블록 행에는
  `register_parcel_key`를 사용한다.
- 자유 텍스트 동·호 조인은 정규화 버전과 신뢰도, 충돌 상태를 함께 저장한다.

## 토지 계열 매핑 후보

### 대지권등록 (`land_right_registration`, CSV)

| 칸 | 후보 개념 | 비고 |
|---|---|---|
| 0 | 고유번호/PNU 후보 | 표준 PNU 파서로 검증 |
| 7/8/9/10/11 | 건축물명/동명/층명/호명/실명 | 원문 보존 후 정규화 |
| 12 | 대지권 비율 | 분자·분모 형식과 단위 검증 필요 |

전유부 연결 후보는 표준 PNU와 정규화된 동명·호명이다. 조인 고유성과 누락률을 고정
fixture에서 검증한 뒤 승격한다.

### 토지특성·지적 (`land_characteristic`, `cadastral`, SHP)

SHP는 geometry와 DBF 속성을 함께 제공한다. 익명형 필드 이름이 사용되는 배포본은 공식
필드 사양 또는 버전 고정 fixture 없이는 지목·면적 같은 캐노니컬 필드에 연결하지 않는다.

### 토지 CSV 후보

- `land_ownership`: PNU, 지번, 동·층·호, 지목, 면적, 가격, 소유 구분과 변동 정보 후보
- `land_use_plan`: PNU, 지번, 용도지역·지구 코드/명, 저촉 여부 후보
- `land_individual_price`: PNU, 지번, 기준연도, 공시지가, 표준지 여부 후보

각 목록은 후보 의미다. 공식 헤더·코드표·단위 검증 전에는 공개 API 필드로 고정하지 않는다.

## Geometry source candidates

- 필지 경계 후보: V-World 지적 또는 토지특성 SHP
- 건물 경계 후보: JUSO 건물 도형

이 문서는 해당 소스의 수집·계정·승인·R2 적재 상태를 기록하지 않는다. geometry source를
선택할 때는 라이선스, 갱신 주기, 좌표계, 필드 사양, 전체/증분 전달 방식, 품질 게이트를 별도
결정하고 운영 상태는 비공개 배포 인벤토리에서 확인한다.

## UI projection candidates

| UI 영역 | 원천 후보 | 캐노니컬 후보 |
|---|---|---|
| 복합 전체 | `building_register_master` | 단지명, 동·세대 집계, 면적, 건폐율, 용적률, 주용도 |
| 동 | `main` + `floor_overview` + `district_zone` | 동명, 층수, 면적, 구조, 용도, 승인일, 층별개요 |
| 호 | `exclusive_unit` + `exclusive_common_area` + `apartment_price` + `land_right_registration` | 호명, 층, 전유·공용면적, 대지권비율, 공시가격 |
| 필지 | `sub_parcel` + 토지 SHP | 관련지번, 지목, 면적, 경계 geometry |

이 표는 제품 응답 계약이 아니라 소스 후보 지도다. 제품 API는 검증·정규화된 Foundation
계약만 소비한다.

## 캐노니컬 개념 시드

```text
building_registry_key
register_parcel_key
pnu
jibun_address / road_address
dong_name / ho_name
floor_kind_code / floor_number / floor_label
gross_floor_area / far_floor_area
above_ground_floor_count / below_ground_floor_count
main_use_code / main_use_name
```

## 검증 및 승격 게이트

1. provider 공식 스키마와 배포 버전을 기록한다.
2. 비밀이나 실제 운영 식별자를 포함하지 않는 최소 fixture를 고정한다.
3. 각 위치의 타입, null 의미, 단위, 코드표를 fixture 기반 테스트로 검증한다.
4. PNU 변환은 ADR 0023의 shared-kernel SSOT만 호출한다.
5. 조인 후보마다 고유성, 누락, 충돌, 블록 행 동작을 검증한다.
6. 검증된 항목만 버전된 정규화 계약으로 승격하고 이 초안에서 `검증 필요` 표시를 제거한다.
7. 실행별 수치와 실제 표본은 비공개 운영 증거에 연결한다.
