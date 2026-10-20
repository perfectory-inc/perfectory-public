# ADR 0286: 모든 표 계약은 행이 어떻게 쌓이는지 적고, 쓰는 쪽과 읽는 쪽이 그 선언을 따른다

- Status: Accepted
- Date: 2026-10-20
- Area: `platforms/foundation-platform`
- Builds on: [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md)
  (버전을 쌓는 표의 현재 행 규칙), [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md)
  (원천 판 레인은 바뀐 키만 쌓는다), [ADR-0240](./0240-the-complex-boundary-file-is-loaded-by-a-scheduled-job.md)
  (산단 경계는 판을 덧붙이고 독자는 가장 새 판을 읽는다)

## Context

2026-10-16 03:36Z `foundation-complex-boundary-release` 의 첫 감독 실행이 실패했다. 원인은
`silver.industrial_complexes` 의 쓰는 쪽이 적재마다 단지 1,442개를 한 벌씩 덧붙였는데, 계약에는 현재 행
규칙이 없었다(`current_row_predicate=None`)는 것이다. 표는 4,326행이 됐고, 독자는 어느 행이 현재인지 알 수 없었다.
ADR-0267(#646)이 그 표에 규칙을 적었다. #809 는 규칙을 적은 표의 독자가 모두 `current_rows` 를 거치는지 계약에서
뽑아 검사한다.

남은 구멍은 쓰는 쪽에 있다. 계약에 규칙을 적을지는 표를 만든 사람의 기억에 달려 있다. 규칙을 안 적은 채
덧붙이는 표는 어떤 가드에도 "버전을 쌓는 표"로 보이지 않는다. 10-16 이 바로 그 모양이었다. 2026-10-20 전수
조사에서 같은 모양이 다섯 표 더 나왔다. 모두 덧붙여 쓰고, 계약에는 규칙이 없고, 독자가 저마다 고른다.

| 표 | 쓰는 방식 | 독자가 고르는 방법(계약 밖) |
|---|---|---|
| `silver.unit_official_price` | 판(vintage)마다 한 묶음을 append | 패널: `unit_official_price.newest_batch`. dbt: 모든 묶음에서 가장 늦은 `base_date` 를 고른 뒤 `row_number` 로 하나 |
| `silver.building_register_apartment_price` | `silver_refresh` `iceberg_write_mode: append` | `unit_official_price.newest_shared_vintage` |
| `silver.building_register_exclusive_unit` | 같음 | 같음 |
| `silver.industrial_complex_boundaries` | 원천 파일 판마다 append(ADR-0240) | `current_official_boundaries` → `newest_source_snapshot` |
| `silver.administrative_boundaries` | 판마다 append | 독자마다 `source_snapshot_id` 를 고른다 |

dbt 의 `unit_official_price` 독자는 같은 판이 두 번 조인되면(2026-10-11 재적재) 같은 `base_date` 의 행을 두 묶음
모두에서 받는다. 지금은 뒤의 `row_number` 가 그것을 가린다.

### 실사례

| 사례 | 무엇을 선언하나 | 누가 강제하나 |
|---|---|---|
| Databricks Delta Live Tables `APPLY CHANGES INTO … KEYS (k) SEQUENCE BY c STORED AS SCD TYPE 1 \| 2` | 키, 순서 열, 이력 방식(덮어쓰기 / 이력) | 파이프라인 정의. 쓰기가 선언대로만 일어난다 |
| Apache Hudi `hoodie.datasource.write.recordkey.field` + `precombine.field`(ordering field), 표 유형 | 행의 키와 "더 새 행"을 고르는 열 | 쓰는 쪽. 키가 없는 upsert 표는 만들 수 없다 |
| Apache Iceberg `identifier-field-ids` | 스키마에 적힌 행의 정체성 | 엔진(Flink upsert·equality delete) |
| dbt snapshot `unique_key` + `strategy`, `dbt_valid_to IS NULL` | 키와 버전 방식, 현재 행 조건 | 스냅숏 설정. 현재 뷰를 하나만 만든다 |
| Kimball 사실 표 유형(transaction / periodic snapshot)과 SCD Type 1·2 | 표가 사건을 쌓는지, 시점 사본을 쌓는지, 차원을 고치는지 | 모델링 규약 |

공통점은 셋이다. 행이 어떻게 쌓이는지를 표 정의의 필수 항목으로 적는다. 쓰기가 그 선언을 따른다. 현재를 읽는
방법은 한 곳에서만 만든다. 이 ADR 은 DLT 의 `KEYS / SEQUENCE BY / SCD TYPE` 과 Kimball 의 periodic snapshot 을
합친 모양을 계약 필드로 옮긴다.

## Decision

1. **`LakehouseTableContract.row_versions` 는 필수다.** `Option` 이 아니고 기본값도 없다. 계약을 새로 쓰는
   사람은 넷 중 하나를 고르지 않으면 컴파일되지 않는다.

   | 값 | 뜻 | 사례의 이름 |
   |---|---|---|
   | `Replaced { key }` | 키마다 한 행. 적재가 표를 통째로 바꾼다 | SCD Type 1, DLT `SCD TYPE 1` |
   | `LatestPerKey { key, newest_first, removal_column, keep_ties }` | 키마다 버전을 쌓고 가장 새 행이 현재다. 지금의 `current_row_latest` 가 여기로 옮겨 온다 | SCD Type 2 현재 뷰, Hudi record key + ordering field, DLT `SCD TYPE 2` |
   | `Editions { edition_column, read: Newest \| Pinned }` | 적재 하나가 완전한 판 하나를 덧붙인다. 독자는 판 하나를 읽는다. 가장 새 판이거나, 부른 쪽이 고정한 판이다 | Kimball periodic snapshot |
   | `Events` | 행 하나가 사실 하나다. 모든 행이 현재다 | Kimball transaction fact, 원장·변경 이력 |

   `current_row_predicate` 는 그대로 둔다. 어느 값과도 함께 쓸 수 있다(예: 필지 경계는 `Editions` +
   `valid_to_utc IS NULL`). 계약 산출물(JSON)은 `row_versions` 를 싣는다. 이행 기간에는 `current_row_latest` 를
   `LatestPerKey` 에서 파생해 함께 싣는다. Python 과 dbt 는 이행 단계에서 `row_versions` 를 읽도록 바꾼다.

2. **쓰는 쪽이 선언을 따른다.**
   - `lakehouse_ingest.append_batch_once` 는 계약이 `Replaced` 인 표에 쓰기 전에 거부한다. Silver·reference 표에
     덧붙이는 길은 이 함수 하나다. 10-16 의 표는 여기서 멈췄을 것이다.
   - 레인 계약의 `silver_refresh.spark.iceberg_write_mode` 는 `Replaced` 일 때만 `overwrite` 다. 다른 값이면
     `append` 다. 시험이 레인 계약과 표 계약을 맞대어 본다.
   - `Editions` 표에 덧붙이는 묶음에는 `edition_column` 값이 하나뿐이다. `append_batch_once` 가 확인한다.

3. **읽는 쪽은 한 곳에서 읽는다.**
   - `platform_contracts.current_rows(frame, contract)` 는 네 값을 모두 다룬다.
     - `Replaced`·`Events`: 그대로 둔다.
     - `LatestPerKey`: 지금 규칙(ADR-0267·0275) 그대로다.
     - `Editions/Newest`: 가장 새 판의 행만 남긴다. 순서는 `edition_column` 이고, 같으면 커밋 순서다.
   - `Editions/Pinned` 은 `current_rows` 가 거부한다. 독자는 `pinned_edition(frame, contract, edition)` 으로 판을 받는다.
   - #809 의 가드는 "`current_row_latest` 가 있는 표"에서 "`Replaced`·`Events` 가 아닌 표"로 넓어진다.
     `newest_batch`·`newest_source_snapshot`·`newest_shared_vintage` 같은 사적 규칙은 이 두 함수로 대체되어
     사라진다.
   - dbt 는 렌더된 `foundation_current_rows` 매크로 하나로 읽는다(#809). 매크로는 네 값을 같은 뜻으로 렌더한다.

4. **현재 키가 유일한지 데이터로 확인한다.** dbt `unique` 시험의 모양을 따른다.
   - `Replaced` 와 `LatestPerKey(keep_ties = false)` 표는 현재 행에서 키가 유일해야 한다.
   - Spark 는 `current_rows` 가 이미 동률을 거부한다.
   - dbt 는 `foundation` 원천마다 `foundation_current_rows` 위의 키 유일성 시험을 렌더한다. 매크로의
     `rank() = 1` 이 동률을 조용히 남기는 구멍을 닫는다.

5. **가드는 실패할 수 있음을 보인다.** 각 단계의 시험은 위반을 심어 거부되는 것을 확인한다.
   - `Replaced` 표에 `append_batch_once`.
   - `row_versions` 를 빠뜨린 계약(컴파일 실패를 `trybuild` 없이 보일 수 없으므로, 계약 목록 전체를 도는 시험이
     산출물에 `row_versions` 가 없는 표를 거부한다).
   - 사적 판 선택.
   - 동률이 있는 dbt 원천.

## 단계

작은 PR 로 나눈다. 앞 단계에 기대는 PR 은 draft 로 둔다.

1. 이 ADR. #809(독자 가드) 병합 뒤에 병합한다.
2. Rust: `row_versions` 필드를 더하고 계약 48개를 분류한다. 각 분류의 근거는 그 표를 쓰는 코드다(쓰기 방식,
   묶음의 판 열). 산출물을 다시 렌더한다. 표 모양은 바뀌지 않으므로 운영 표 모양 원장(ADR-0259)은 그대로다.
3. `append_batch_once` 의 거부와 레인 계약 시험(결정 2).
4. `current_rows` 의 `Editions` 처리와 `pinned_edition`. 다섯 표의 독자를 옮긴다(결정 3). 독자마다 옛 사적 규칙과
   같은 행을 고르는지 합성 데이터로 시험한다.
5. dbt 매크로와 키 유일성 시험(결정 4).

## Consequences

- 표를 새로 만드는 사람은 행이 어떻게 쌓이는지 정해야 한다. 정하지 않으면 컴파일되지 않는다. 10-16 의 결함은
  계약 단계에서 막힌다.
- 독자가 판이나 버전을 고르는 사적 규칙이 사라진다. 다섯 표의 독자 일곱 곳이 옮겨 간다.
- `Editions/Newest` 의 "가장 새 판"은 `edition_column` 순서다. 지금 `newest_source_snapshot` 은 `valid_from_utc`
  를, `newest_batch` 는 커밋 순서를 쓴다. 단계 4 에서 표마다 같은 판을 고르는지 증명한 뒤에 옮긴다.
- `current_row_latest` 는 산출물에서 이행 기간 동안 파생값으로 남는다. 독자가 모두 `row_versions` 를 읽으면 지운다.

## Rejected

- **가드가 쓰기 코드를 정적으로 읽어 "덧붙이는 표"를 추론하기**: 쓰기 경로가 여러 모양이다(`append_batch_once`,
  레인 계약, `writeTo().append()`). 추론은 새 모양을 놓친다. 선언이 정본이고, 쓰기는 한 함수에서 선언을 확인한다.
- **`current_row_latest` 를 필수로 만들기만 하기**: 판을 통째로 덧붙이는 표(`Editions`)와 사건 표(`Events`)를
  표현하지 못한다. 그런 표에 키별 최신을 억지로 적으면 틀린 행을 고른다.
- **표마다 Trino·Iceberg 뷰로 현재를 내보내기**: 정의가 계약 밖 SQL 로 한 벌 더 생긴다. Spark 의 핀(스냅숏 id)
  읽기와도 맞지 않는다.
