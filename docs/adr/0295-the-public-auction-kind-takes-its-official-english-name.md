# ADR 0295: 공매 종류의 영어 식별자를 공식 용어 public-auction 으로 바꾼다

- Status: Accepted
- Date: 2026-10-23
- Relates to: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md) §9 (종류 장치), [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md) (공매·경매·공장은 Foundation 의 필지 원천), [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md) (공매는 자기 Silver 에서 만드는 종류), [ADR-0292](./0292-public-sale-servable-gold-carries-only-host-panel-pnus.md)

## Context

공매(온비드, 한국자산관리공사)의 공식 영어 용어는 "Public Auction" 이다 — KAMCO·온비드 영문 표기와 법제처 영역 법령이 그렇게 쓴다. 우리 내부 영어 식별자는 이를 `public_sale`/`public-sale` 로 잘못 영어화해 두었다. "sale" 은 수의계약·매각 일반을 뜻해 공개 경쟁 입찰인 공매와 어긋난다.

이 종류는 아직 서빙되지도 소비되지도 않는다. ADR-0287 의 첫 발행·굽기는 비용 승인 뒤로 미뤄져 있고 아직 하지 않았다. 이름을 바꾸기 가장 싼 때는 지금, 발행 전이다. 늦으면 서빙 경로·소비자·구운 문서까지 끌고 가야 한다.

한국어 낱말 "공매" 는 그대로 둔다. 바뀌는 것은 영어 식별자뿐이다. 온비드/data.go.kr 원천 필드 이름도 우리 것이 아니라 그대로 둔다.

## Decision

공매 종류의 영어 식별자를 `public_sale` → `public_auction`, `public-sale` → `public-auction` 로 바꾼다. 바뀌는 대상:

1. **Gold 표·열.** `gold.parcel_public_sale` → `gold.parcel_public_auction`, 열 `public_sale_json` → `public_auction_json`. 표 이름은 종류 장치가 레인 이름에서 만들어 내므로(루트 ADR-0193, `by_pnu_kind`), 레인 이름 한 줄을 바꾸면 Rust 계약·내보낸 계약 JSON·ODCS 계약이 함께 바뀐다.
2. **레인·종류·경로.** by-PNU 레인 `parcel-public-sale` → `parcel-public-auction`, 종류 `public-sale` → `public-auction`, 게이트웨이 경로 `/parcels/public-sale/by-pnu/` → `/parcels/public-auction/by-pnu/`.
3. **생산자·작업·유닛.** Spark 생산자 작업 `onbid_public_sale_silver_to_gold` → `onbid_public_auction_silver_to_gold`, 자기 재구축 작업 `gold_public_sale_rebuild` → `gold_public_auction_rebuild`, systemd 유닛 `foundation-gold-public-sale-rebuild.service` → `foundation-gold-public-auction-rebuild.service`.
4. **그 밖.** 수집 슬러그, 계약 파일 이름, 파이프라인 그래프 노드·엣지, 작업 의존성, 묶음 레인 식별자의 같은 식별자를 모두 함께 바꾼다.

바뀌지 않는 것:

- Silver 표 `silver.onbid_real_estate_item` 과 `silver.onbid_item_bid_result` 는 이름에 이 식별자가 없고, 바뀌지 않는다. 공매 Silver 는 이 표들이다.
- Gold 표는 새것이고 서빙되지 않으므로 Iceberg 제자리 이름 변경은 없다. 바뀐 계약이 다음 Gold 재구축에서 `gold.parcel_public_auction` 를 처음부터 새로 만든다. 파괴적 마이그레이션은 없다.
- 첫 발행·굽기는 여전히 비용 승인 뒤에만 한다(ADR-0287 §8, 출시 전 규칙). 이 결정은 코드·계약·문서만 바꾸고 운영은 건드리지 않는다.

## Consequences

- 옛 ADR(0193·0280·0281·0287·0288·0289·0292)은 그때의 기록이므로 본문의 옛 이름 `public_sale`/`public-sale` 을 그대로 둔다 — 받아들여진 ADR 의 글은 고치지 않는다. 그 이름이 가리키던 것의 현재 이름이 public-auction 임을 이 ADR 이 남긴다.
- `config/r2-connections.contract.json` 의 `by_pnu_lanes.parcel-public-auction.decision` 은 이 ADR 을 가리킨다. 종류의 최초 결정 근거는 ADR-0287 이지만, 그 파일 이름이 옛 식별자를 담고 있어 config 에 옛 이름을 다시 들이지 않으려 이 ADR 로 건다. 이 ADR 이 ADR-0287 로 잇는다.
- 앞으로 영어 산문·화면·소비자는 "public auction" 을 쓴다.
