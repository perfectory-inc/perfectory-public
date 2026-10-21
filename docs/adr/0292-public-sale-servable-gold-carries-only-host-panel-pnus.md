# ADR 0292: 공매 서빙 Gold 는 주인 패널에 있는 PNU 만 싣는다

- Status: Accepted
- Date: 2026-10-22
- Amends: [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md) §7 (이력 없는 폐지 PNU 의 공매가 `lineage_missing` 으로 서빙 Gold 에 남는다)
- Relates to: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md) §9 (종류 = 주인 패널의 섹션), [ADR-0272](./0272-a-served-section-may-not-silently-lose-a-region.md) (한 시도의 섹션 커버리지를 조용히 잃지 않는다)

## Context

공매는 주인 패널(`gold.parcel_panel`)의 **섹션**으로 서빙된다. 게이트웨이는 `parcels/by-pnu/{pnu}` 의 필지 문서를 읽고 거기에 `public_sale` 섹션을 얹는다. 필지 문서가 없는 PNU 에는 얹을 곳이 없어 404 다. 계약(`config/r2-connections.contract.json` `by_pnu_lanes.parcel-public-sale`)도 공매에 자기 host 를 주지 않는다 — `host: parcel_by_pnu_gateway`, `section: public_sale`. (유닛가 같은 종류는 자기 `public_hostname` 을 갖지만 공매는 갖지 않는다.)

ADR-0287 §7 과 그 본문("이력이 없는 폐지 PNU 의 공매는 그 PNU 에 남고 `lineage_missing` 으로 표시된다. 공매는 버려지지 않는다")은 폐지 PNU 의 공매를 서빙 Gold 에 남긴다. 그런데 그 PNU 는 현행 지적도에 없어 필지 문서가 없다. 그래서 그 행들은 영원히 404 다 — 서빙되지 못한 채 서빙 Gold 에만 있다. ADR-0287 의 Consequences 도 공짱 웹 공매 탭이 "문서가 있을 때만 보인다" 고 적었으니, 이 행들은 애초에 화면에 닿을 수 없었다.

2026-10-22 운영 Gold 실측: `gold.parcel_public_sale` 5,617 행 중 593 행(10.6%)이 `lineage_missing` 이고, 그 중 530 행은 존재하지 않는 시도코드 `12` 등 레거시·오매핑 PNU 다. 나머지 5,024 행은 모두 패널에 있다. PNU별 서빙 게이트(`availability_min` 0.9995)는 그 593 행의 404 때문에 발행을 거부했다.

ADR-0193 §9 는 종류의 행이 주인 패널 행의 **부분집합**이라 전제한다 — 모든 섹션 종류가 패널 고정점에 묶이고 패널과 같은지 검사받는다. ADR-0287 은 자기 생산자 종류에서 그 고정점·검사를 뗐는데(그 Silver 가 패널과 다른 주기로 커밋되기 때문), 부분집합 **불변식**까지 함께 뗀 셈이 되어 패널에 없는 PNU 가 서빙 Gold 에 들어갔다.

## Decision

1. **서빙 Gold 는 주인 패널에 문서가 있는 PNU 만 싣는다.** 공매 생산자(`onbid_public_sale_silver_to_gold.servable`)는 `lineage_missing` 으로 놓인 회차를 서빙 Gold 에서 뺀다. 섹션은 필지 문서 위에만 얹히므로, 문서가 없는 PNU 의 행은 서빙될 수 없고 게이트를 404 로 떨어뜨릴 뿐이다. 이로써 자기 생산자 종류도 다른 모든 종류와 같은 불변식(행 ⊆ 주인 패널, ADR-0193 §9)을 지킨다. 고정점·같음-검사(ADR-0287 이 뗀 것)는 그대로 떼어 둔다 — 공매는 여전히 자기 Silver 에서 자기 주기로 만든다. 묶이는 것은 **어느 PNU 가 서빙되는가** 뿐이다.

2. **버려지는 것은 없다 — 데이터는 Silver 에 그대로다.** `silver.onbid_real_estate_item` 은 모든 물건·회차를 쌓는다(append-only). 서빙 Gold 에서 빠진 회차는 `lineage_missing_rounds` 카운터로 집계되어 실행 요약에 남는다. ADR-0287 §7 의 "공매는 버려지지 않는다" 는 이 데이터 계층에서 유지된다. 레거시 PNU 를 현행 PNU 로 매핑하거나(경매의 옛 시군구코드 매핑과 같은 부류) 공매에 자기 문서 레인을 주면 — 둘 다 이 ADR 밖 — 그 행들이 다시 서빙 대상이 된다.

3. **이 열린-경매 종류는 시도별 섹션 커버리지 게이트(ADR-0272)를 받지 않는다.** 그 게이트는 천천히 자라는 섹션(대장)이 한 시도의 커버리지를 조용히 잃는 것을 막는다. 공매의 행은 **열린** 온비드 경매라 날마다 열리고 닫혀 한 시도의 수가 크게 흔들리고 0 에 이르는 것이 정상이다 — 한 시도의 하락은 여기서 결함이 아니다. 이 종류의 수 게이트는 표 전체 행-손실 하한(계약 `kind_tables.overrides.parcel-public-sale.max_row_loss_fraction` 0.5, #836)이다. 생산자는 하네스가 넘기는 `--section-coverage-*` 를 받되(스케줄 재빌드가 "unrecognized arguments" 로 죽지 않도록) 강제하지 않는다. 시도별 게이트는 날마다의 변동성을 깨끗한 기준선에 대고 잰 뒤에 다시 본다.

## Consequences

- 공매 서빙 Gold 가 5,617 → 5,024(−10.6%)로 줄고, PNU별 서빙 게이트의 404 가 사라져 첫 발행이 통과한다.
- `servable()` 은 시험(합성 PNU)으로 고정된다: `lineage_missing` 회차는 집계되나 서빙 Gold 에 없다.
- ADR-0287 §7 과 본문 "그 PNU 에 남고" 는 서빙 Gold 가 아니라 Silver 를 뜻하도록 좁혀진다(ADR-0287 끝 각주).
- 시도별 섹션 커버리지의 강제가 공매에서 빠진다. catastrophic 손실은 0.5 행-손실 하한이 잡는다. 레거시 시도코드(예: `12`)를 현행 코드로 매핑하는 일은 별도 PR 로 남는다 — 그때까지 그 530 행은 Silver 에만 있다.
