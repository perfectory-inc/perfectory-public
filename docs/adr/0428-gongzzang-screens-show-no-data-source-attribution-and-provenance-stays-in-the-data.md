# ADR 0428: 공짱 화면은 데이터 출처를 보이지 않고, 출처는 데이터에 남는다

- Status: Accepted
- Date: 2026-11-06
- Supersedes: [ADR-0427](./0427-a-photo-is-shown-from-a-rendition-ladder-made-once-from-its-stored-original.md) §6(출처 표시)만,
  [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md) §6 의 "화면은 출처 한 줄을 보인다"만,
  [ADR-0281](./0281-court-auction-is-collected-openly-politely-and-stops-when-refused.md) §6 의 "화면에 내보낼 때 출처 표시는
  소비자(Gongzzang)의 몫"만. 세 ADR 의 나머지는 그대로다.

## Context

2026-11-06 사장 결정: 어느 화면에도 데이터 출처를 표시하지 않는다. 그날 main 의 공짱 웹에는 출처 표시가 네 곳 있었다.

| 자리 | 보이던 글 |
| --- | --- |
| 지도 오른쪽 아래(`components/listings/listing-map.tsx` `MapAttribution`, i18n `map.attribution`) | "필지 polygon: V-World (공공누리 제1유형: 출처표시)" |
| 필지 패널 바닥(`summary.tsx` `ParcelFooter`, `panels.parcel.summary.sources`) | "출처: 국토교통부 토지특성·개별공시지가·…·건축물대장" |
| 필지 패널 규제 구역 머리(`mapZonesSource`·`mapZonesSourceAsOf`) | "용도지역지구 · 기준일 …" |
| 공매 패널 아래(`public-auction.tsx`, `panels.parcel.publicAuction.attribution`, ADR-0427 §6) | "출처: 온비드(한국자산관리공사)" |
| 건물 층 미리보기(`/preview-floors`, 개발 전용) | "데이터 출처: 표제부·전유부·전유공용면적 (hub.go.kr, …)" |

지도 SDK(NAVER 지도 v3, `gl: true`)는 자기 지도 데이터 저작권 컨트롤(`mapDataControl`)을 기본으로 켠다. 이 저장소에
MapLibre·Mapbox 의 `AttributionControl` 은 없다(지도는 NAVER SDK 하나다).

## Decision

1. **화면에는 데이터 출처가 없다.** 위 네 자리와 미리보기의 글·컴포넌트·i18n 키·그 글을 확인하던 시험을 지운다.
   규제 구역 머리에는 기준일만 남는다("기준일 {date}", 기준일이 없으면 아무것도 없다) — 신선도는 출처가 아니다.
2. **지도 SDK 의 데이터 저작권 컨트롤은 명시적으로 끈다**(`mapDataControl: false`, 기본값에 기대지 않는다). NAVER 로고
   컨트롤은 데이터 출처가 아니라 지도 제공자의 상표이므로 이 결정 밖이다(건드리지 않는다).
3. **출처는 데이터에 남는다(append-only, 지우지 않는다).** 문서·계약·정의의 출처 칸은 이 결정으로 바뀌지 않는다.

   | 데이터셋 계열 | 출처가 남는 곳 |
   | --- | --- |
   | 필지(by-PNU 프로필·특성·가격·임야·토지권리·이동이력·용도지역) | 문서 `source.table`·`source.iceberg_snapshot_id`(프로필은 `source.metadata_location`·`manifest_list_location`·`lineage[]` 까지) — `contracts/by-pnu-documents/*.json` |
   | 건물(by-PNU 프로필·페이지) | 문서 `source.table`·`source.iceberg_snapshot_id` |
   | 공매·공공매각(온비드) | 문서 `source.*` 와 `source_datasets`; 원천 계약 `infra/lakehouse/contracts/data-go-kr-onbid.contract.json` |
   | 소유·멸실·에너지·실거래·규제 구역·부속 필지 | 스키마 레지스트리 `contracts/schema-registry/<문서>/1/*.schema.json` 의 `source.table`·`source.iceberg_snapshot_id` |
   | 법원경매 | Silver 행의 원천·Bronze 객체 키(ADR-0281 §6), ODCS `silver.court_auction_*.odcs.yaml` 목적 칸 |
   | 모든 공공 원천 | 엔드포인트 157 개의 `provider`(hub.go.kr 59·data.go.kr 31·vworld.kr 26·rt.molit.go.kr 12·juso.go.kr 11 외) — `docs/catalog/public-source-endpoint-catalog.v1.json`; 원천별 계약 `infra/lakehouse/contracts/<제공자>-*.json` |

## 제공자 약관이 출처 표시를 말하는 곳(사실만)

- V-World 연속지적도(dsId 30563): 공공누리 제1유형(출처표시) — 지운 지도 글에 적혀 있던 조건.
- 온비드: 사이트이용정책상 공사가 저작재산권 전부를 가진 저작물은 공공누리 제1유형(출처표시)(ADR-0424 Context 표).
- 법원경매정보: 저작권 보호정책의 출처 표시 의무(ADR-0281 §6, ODCS 목적 칸).
- 온비드 물건목록·입찰결과(data.go.kr 15157207·15157254): "이용허락범위 제한 없음", 출처 표시 의무 없음(ADR-0287 §6).
- NAVER 지도 SDK: 지도 데이터 저작권 컨트롤을 기본으로 보인다.

## Consequences

- 공짱 웹: 출처 컴포넌트 1 개·i18n 키 9 개·시험 1 개가 사라지고(키 `mapZonesAsOf` 1 개가 새로 생긴다), 지도 생성 옵션에 `mapDataControl: false` 가 더해진다.
- 출처를 다시 보여야 하면 새 ADR 로 이 결정을 바꾼다; 데이터의 출처 칸이 그대로라 화면만 다시 만들면 된다.
- 가드는 더하지 않는다(ADR-0001: 막을 실제 사고가 아직 없다).
