# ADR 0003 - Industrial Complex Catalog SSOT

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-12 |
| 상태 | Accepted |
| 범위 | `foundation-platform` Catalog, `gongzzang`, `dawneer` 산업단지 관련 데이터 경계 |

## 결정

산업단지에 대한 모든 canonical fact 와 산업단지에 종속되는 운영 subobject 는
`foundation-platform` Catalog 가 단일 원장으로 소유한다.

여기에는 산업단지, 필지, 건물, 제조사뿐 아니라 공지, 고시/첨부 파일, 도면,
공간 레이어, 3D/digital twin asset, 유치업종/허용업종 규칙, 필지별 업종 배정,
공식 출처와 원본 snapshot 이 포함된다.

`dawneer` 는 사이트 제작과 게시, 페이지 구성, 테마, 사이트별 표시 설정만 소유한다.
`gongzzang` 은 매물, 경매, 거래, 일반 사용자 데이터를 소유한다. 두 서비스는
산업단지 데이터를 복제해서 write owner 가 되지 않고, `foundation-platform` 의 ID 를
참조하거나 event 기반 read model 로 소비한다.

## 소유권 규칙

| 질문 | Owner |
|---|---|
| 다른 제품에서 보여도 같은 값이어야 하는가? | `foundation-platform` |
| 산업단지, 필지, 도면, 3D, 업종 규칙에 붙는 사실인가? | `foundation-platform` |
| 특정 사이트에서만 다르게 보이는 제목, 설명, 색상, 노출 순서인가? | `dawneer` |
| 매물 상품, 경매, 거래, 일반 사용자 행동 데이터인가? | `gongzzang` |
| 공짱 admin 과 Dawneer 사용자가 공유하는 직원 신원인가? | `foundation-platform` Staff Identity |
| 공짱 일반 사용자 신원인가? | `gongzzang` |

## 데이터 계약

- `foundation-platform` 내부 Catalog API 는 자기 리소스의 `id` 를 사용한다.
- Consumer DB 에서 foundation-platform 리소스를 참조할 때는
  `foundation_platform_complex_id`, `foundation_platform_parcel_id`,
  `foundation_platform_building_id`, `foundation_platform_layer_id` 처럼 출처를 드러낸다.
- object storage 참조는 ADR 0002 를 따른다. DB/API/DTO/port 표준은
  `objectKey`, `object_key`, `ObjectStorageService` 이다.
- imported data 는 source lineage 를 가져야 한다. 최소한 `source`, `source_url`,
  `source_record_id` 또는 `source_snapshot_id`, `version`, `updated_at` 을 추적한다.
- Consumer 는 foundation-platform DB 에 직접 접근하지 않는다. HTTP API 또는 event/read model 로만 접근한다.

## Dawneer 경계

`dawneer` 에 남는 산업단지 관련 row 는 presentation extension 이어야 한다.

예시는 다음과 같다.

```text
dawneer.site_catalog_presentation
  site_id
  foundation_platform_complex_id
  foundation_platform_parcel_id nullable
  foundation_platform_layer_id nullable
  visible
  display_order
  title_override nullable
  description_override nullable
  image_override nullable
  contact_channel_override nullable
  display_color_override nullable
```

이 row 는 산단 데이터를 정의하지 않는다. 특정 사이트가 foundation-platform 데이터를
어떤 순서와 스타일로 보여줄지만 정의한다.

## Gongzzang 경계

`gongzzang` 은 산업단지/필지/건물 ID 를 참조해서 매물, 경매, 거래, 검색, 사용자
행동 데이터를 구성한다. 매물 사진과 매물 설명은 `gongzzang` 소유다. 반대로 공식
산단 도면, 공식 첨부 파일, 공식 3D asset 은 `foundation-platform` 소유다.

## 영향

- `dawneer.industrial_complex`, `parcel_info`, `interactive_blueprint`, `polygon`,
  `industry_group`, `parcel_industry_group` 는 최종적으로 write owner 가 아니다.
- `dawneer` 에 필요한 cache 는 event 로 갱신하고, schema 이름에서 cache 또는
  presentation 목적을 분명히 드러낸다.
- `gongzzang` 의 산단/필지/건물/제조사 도메인은 foundation-platform Catalog 의 consumer 로 전환한다.
- Foundation Platform Catalog API 는 현재 초기 스키마보다 넓어져야 한다. 상세 모델은
  [Industrial Complex SSOT Model](../catalog/industrial-complex-ssot-model.md) 을 따른다.

## 비목표

- `gongzzang` 일반 사용자와 Staff identity 를 합치지 않는다.
- `gongzzang` 매물, 경매, 거래 데이터를 foundation-platform 로 옮기지 않는다.
- `dawneer` 사이트 빌더, 페이지, 테마, 배포, 마케팅 운영 데이터를 foundation-platform 로 옮기지 않는다.
- Consumer 가 foundation-platform DB 를 직접 SELECT 하는 경로를 만들지 않는다.

## 완료 정의

- 산업단지에 붙는 canonical write path 는 foundation-platform API 하나로 수렴한다.
- `gongzzang` 과 `dawneer` 는 같은 `IndustrialComplexId`, `ParcelId`, `BuildingId` 를 참조한다.
- `dawneer` 의 사이트별 override 는 canonical fact 를 덮어쓰지 않고 presentation 에만 적용된다.
- 산업단지 관련 파일/도면/3D asset 은 `object_key` 로 관리되고 R2 primary policy 를 따른다.
