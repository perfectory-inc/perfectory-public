# ADR 0183: 여러 배치로 싣는 Silver 판은 브랜치에 쌓고 한 번의 커밋으로 공개한다

- Status: Accepted
- Date: 2026-10-11
- Builds on: [ADR-0069](./0069-one-column-holds-five-kinds-of-thing.md)(적재 기록),
  [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)(Silver 레인),
  [ADR-0182](./0182-the-gold-rebuild-joins-the-unit-prices-and-the-panel-reads-the-newest-vintage.md)(가격 조합은 가장 새 판)

## Context

Silver 레인은 큰 판을 여러 배치로 싣는다. 공동주택가격은 2억 행을 40부분씩 6배치로, 토지 레인은 첫 배치를 덮어쓰고
나머지를 붙인다. 배치마다 Iceberg 커밋이 하나이므로, 배치 사이에 표의 `main` 은 새 판의 일부만 가진다.

2026-10-11 실제로 그 상태가 남았다. 가격 레인의 두 번째 배치가 실패하자(#411) `silver.building_register_apartment_price`
에는 202609 판 4천만 행만 남았다. Gold 재생성은 그 표를 읽어 세대 가격을 "두 표가 함께 가진 가장 새 판"으로
조합한다(ADR-0182). 그대로 돌았다면 반쪽 판을 최신으로 조합해 서빙했을 것이다. 그날은 손으로 Gold 실행을 막았다.
읽는 쪽마다 "이 판이 다 들어왔나"를 따로 판단하게 하는 것은 같은 사실을 여러 곳에 두는 것이다.

적재 기록(`foundation.ingest-batch-objects`, ADR-0069)을 읽는 두 곳(Spark `read_ingested_objects`, Rust
`load_ingested_batch_objects`)도 표의 모든 스냅숏을 읽었다. 그래서 `main` 밖에 쌓인 것까지 "적재됨"으로 셌다.

## Decision

1. **여러 배치 적재는 브랜치에 쌓는다.** Silver 레인의 배치 적재(허브 부분 레인, 토지 레인)는 배치마다
   `--iceberg-branch <이름>` 을 받아 그 표의 브랜치에 쓴다. 브랜치는 첫 배치가 `main` 에서 만든다
   (`append_batch_once(..., branch=)`). 이름은 레인이 정하는 한 곳(`execute::load_branch`)뿐이다.
   `load_<적재 identity 의 sha256 앞 16자>` 이고, 재시도는 같은 브랜치에서 이어 간다. 쓴 직후의 읽기 검증도 그
   브랜치에서 한다. 한 커밋으로 끝나는 적재(표제부·호·호 면적의 덮어쓰기 한 번)는 `main` 에 그대로 쓴다.
2. **공개는 카탈로그 커밋 하나다.** 모든 배치가 끝나면 레인이 `publish_branch` 로 `main` 을 브랜치 머리로 옮기고
   브랜치를 지운다. 한 번의 REST 커밋이며 `main`·브랜치 두 참조를 모두 단언한다. 빨리감기만 허용한다. 브랜치를 만든
   뒤 `main` 이 움직였으면(압축 등) 공개하지 않는다. 다음 시도가 낡은 브랜치를 지우고(`remove_stale_branch`) 현재
   `main` 에서 다시 쌓는다. 읽는 쪽은 판 전체를 보거나 아무것도 보지 않는다.
3. **"적재됨"은 `main` 의 계보다.** 두 기록 읽기는 해당 참조 머리에서 부모를 따라간 스냅숏의 기록만 센다. 판이
   실렸는지(레인 계획, 적재 확인)는 `main` 에 묻는다. 재시도가 어디까지 했는지는 브랜치에 묻는다.
4. 스냅숏이 하나도 없는 표는 브랜치를 만들 바탕이 없고 읽는 쪽도 없으므로 `main` 에 쓴다. 그러면 공개할 것이 없다.

## Consequences

- 배치 중간의 실패·중단·재시도에도 `main` 에는 앞 판 아니면 새 판 전체만 있다. Gold·가격 조합·Trino 는 바뀌지 않는다.
- 적재 중에 `main` 을 바꾸는 작업(같은 표의 압축)과 겹치면 공개가 거부되고 다음 실행이 다시 쌓는다. 레인은 한 번에
  하나만 돈다.
- 시험: `iceberg_rest_catalog_adapter`(main 계보만 셈, 빨리감기만 공개, 낡은 브랜치 제거),
  `test_lakehouse_ingest.LoadBranchTest`, `test_load_branch_iceberg`(실제 Iceberg 에서 공개 전 `main` 불변),
  레인 인자(`land_tests`, `tests`).
