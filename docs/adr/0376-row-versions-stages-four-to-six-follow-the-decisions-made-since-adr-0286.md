# ADR 0376: row_versions 4–6단계는 ADR-0286 뒤에 정한 결정을 따른다

- Status: Accepted
- Date: 2026-11-04
- Amends: [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md) §1 의 판 순서 동률 처리, §3 의
  `Editions/Pinned` 처리, 단계 4 의 범위
- Builds on: [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md)(공시가격
  버전 표), [ADR-0318](./0318-the-remaining-silver-lanes-append-their-changed-keys-compared-bucket-by-bucket.md)(비교 레인),
  [ADR-0328](./0328-a-compared-silver-table-orders-versions-by-edition-then-commit-instant.md)(판 다음 커밋 시각),
  [ADR-0291](./0291-the-served-cadastral-edition-is-operator-chosen-and-readiness-gated.md)(서빙 지적 판),
  [ADR-0347](./0347-unit-ids-and-link-evidence-are-named-by-content-and-every-gold-commit-records-its-changed-keys.md) §4(Gold 변경 기록)

## Context

ADR-0286(2026-10-20)의 1b 는 병합됐다(#815). 1c·2·3 단계(#818·#816·#817)는 draft 로 남았고, 그 사이 main 에
결정 여럿이 들어왔다. 2026-11-04 에 사슬을 main(d53c8770) 위로 다시 쌓으며 실물과 대조한 결과는 이렇다.

| 항목 | ADR-0286 이 가정한 것 | 2026-11-04 실물 | 근거 |
|---|---|---|---|
| Silver 표의 모양 | 허브 표제부·호·면적은 `Replaced`, VWorld 토지 6개는 키 없는 `Replaced` | Silver 에 `Replaced` 표가 하나도 없다. 비교 레인 12개는 `LatestPerKey`(`newest_first [판, ingested_at_utc]`) | ADR-0318, ADR-0328, 계약 산출물 64표 집계 |
| 공시가격 두 표의 독자 | 단계 4 가 `newest_batch`·`newest_shared_vintage` 를 `current_rows` 로 옮긴다 | `_versions` 표(ADR-0317)로 옮기는 PR #919 가 따로 있다 | #911 병합, #919 draft |
| `silver.parcel_boundaries` 의 독자 | `Editions/Pinned` 이라 `current_rows` 가 거부한다 | 서빙 독자는 `read_served_parcels`(served_edition + 술어)이고, `address_pnu`·실거래 적재는 `current_rows`(술어만, 모든 판)를 쓴다 | 코드 읽기, ADR-0148 |
| 판 순서의 동률 | 커밋 순서로 가른다 | 행마다 커밋 순번을 Spark 가 주지 않는다(Iceberg v2). 2026-11-04 운영 Editions 표 6개에 동률 0건 | Trino 읽기 실측 |
| Gold 변경 기록 `gold.<t>__changes` | 없음 | 계약 없이 쓰이는 Iceberg 표(ADR-0347 §4) | 코드 읽기 |

## Decision

1. **판 순서의 동률은 거부한다.** `newest_edition` 은 판마다 `newest_first` 의 최댓값을 차례로 비교하고, 그래도
   같으면 거부한다. ADR-0286 §1 의 "커밋 순서로 가른다"는 행에 커밋 순번이 있어야 하는데, Iceberg v2 에서 Spark 는
   그것을 주지 않는다. 고르지 못하는 것을 고르는 척하지 않는다. `current_rows` 가 `LatestPerKey` 의 동률을 거부하는
   것과 같다. 표가 v3 행 계보(`_last_updated_sequence_number`)를 가지면 그 열로 동률을 가른다. 2026-11-04 측정에서
   운영 Editions 표(산단 경계 1판, 행정경계 3판, 층 2세대, 공장 1판, 법정동 스냅숏, 지번변경 이력 시도별)는 동률 0건이다.
   공시가격 옛 표는 202609 판이 묶음 4개라 동률이다. 그래서 그 표의 독자는 #919 가 버전 표로 옮기기 전까지 지금의
   `newest_batch`(커밋 순서 포함)를 그대로 쓴다.
2. **`Editions/Pinned` 표에 술어가 있으면 `current_rows` 는 술어만 적용한다.** 판마다 자기 술어로 현재다(ADR-0148).
   술어가 없는 고정 판 표는 그대로 거부한다. 판 하나를 읽는 독자는 `pinned_edition(frame, contract, edition)` 을 쓴다.
   `address_pnu`·실거래 적재가 모든 판의 열린 필지를 합쳐 읽는 것은 ADR-0291 §5 가 금지한 모양이다. 그러나 그것을
   서빙 판으로 좁히면 PNU 결정 결과가 바뀐다(새 판에만 있는 필지가 빠진다). 이것은 이 ADR 이 정할 일이 아니고
   ADR-0291 §5 의 구현 과제다. 4단계는 이 두 독자의 동작을 바꾸지 않는다.
3. **4단계의 범위는 산단 경계·행정경계·층이다.** 독자 다섯 곳을 옮긴다:
   - 산단 경계: PostGIS 핸드오프와 served Gold. 둘 다 `current_official_boundaries` 를 거친다.
   - 행정경계: served Gold 의 `read_newest_silver` 와 핸드오프의 앞 판 읽기.
   - 층: 건물 패널의 `newest_floor_batch`.

   각 독자는 사적 규칙과 같은 판을 고르는지 합성 데이터로 시험한다
   (`test_row_versions_editions_spark.MovedReadersPickTheOldRuleRowsTest`: 판 이름 순서와 적재 순서가 어긋나는 네 판의
   모든 부분집합). 공시가격 두 표와 전유부는 ADR-0317·0318 이 옮긴다.

   남은 사적 판 고르기는 범위 밖이다. 2026-11-04 목록은 아래와 같고, 6단계의 정본 독자 가드가 막기 전에 각각 옮긴다.
   - 법정동 스냅숏 `max(snapshot_date)`: Spark 다섯 곳과 Rust `land.rs` 한 곳.
   - 공장 `latest_edition_rows`: 판 id 의 문자열 순서로 고른다.
4. **Gold 변경 기록은 `Events` 다.** `gold.<t>__changes` 는 자기 계약이 없다. 모양은 기록하는 Gold 가 정한다.
   그래서 `platform_contracts.change_record_versions` 한 곳에서 `Events` 로 선언한다. 쓰기는 덧붙이기만 허용되고,
   바꿔 쓰기는 거부된다. Gold 가 아닌 표에는 변경 기록이 없다.
5. **dbt 매크로는 Editions 를 같은 뜻으로 렌더한다.** 순서는 판별 최댓값이다. 매크로는 동률을 거부하지 못한다.
   그래서 키 유일성 시험(`tests/foundation_current_keys_are_unique.sql`)을 계약에서 렌더한다. 2026-11-04 의 dbt
   `foundation` 원천 넷 중 유일성 대상은 0개다. 셋은 `keep_ties` 이고 하나는 Editions 다.

## Consequences

- 사슬의 병합 순서는 #818 → #816 → #817 → 4단계 → 5단계 → 6단계다. 앞 단계에 기대는 PR 은 draft 다.
- 1c(#818)의 백필은 그대로 필요하다. 공시가격 옛 표는 ADR-0317 뒤에도 남는다(§5.4). 계약이 그 표를 판 순서
  `[vintage]` 로 선언하려면 열이 있어야 한다. 백필 규모는 2026-11-04 측정으로 12.07억 행·3.14 GB 이고, 배포 이행
  단계에서 8–15분이 걸린다(계산).
- 동률 거부는 실패할 수 있는 관문이다. 같은 판 열 값으로 판 둘이 실리면 독자가 멈춘다. 그때는 계약의
  `newest_first` 에 `ingested_at_utc` 를 더한다(ADR-0328 의 모양).

## Rejected

- **동률을 판 id 로 가르기**(옛 `newest_source_snapshot`·`newest_floor_batch`): 판 id 는 시간 순이 아니다.
  2026-09 층 묶음은 `selection-v2`, 그 뒤 묶음은 `content-v1` 이다. ADR-0286 이 없애려던 규칙이다.
- **Pinned 표의 `current_rows` 를 거부하고 `address_pnu`·실거래를 서빙 판으로 옮기기**: 독자마다 옛 규칙과 같은 행을
  골라야 한다는 ADR-0286 단계 4 의 안전망에 어긋난다. PNU 결정 결과를 바꾸는 일은 ADR-0291 §5 에서 따로 정한다.
