# ADR 0332: 적재 기록은 그 스냅숏이 만료될 수 있기 전에 표 속성으로 옮긴다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0062](./0062-an-ingest-batch-records-itself-in-the-table-it-writes.md) (기록은 스냅숏 요약에만 둔다),
  [ADR-0238](./0238-every-lakehouse-table-is-maintained-on-a-schedule.md) (적재 기록 스냅숏의 만료 보호가 기록을 지킨다)
- Builds on: [ADR-0328](./0328-a-compared-silver-table-orders-versions-by-edition-then-commit-instant.md) §3 (판 기록의 표 속성),
  [ADR-0237](./0237-lakehouse-snapshots-are-kept-30-days-12-month-ends-and-every-year-end.md) (30일 보존)

## Context

Silver 적재기는 무엇을 이미 실었는지를 Iceberg 스냅숏 요약 `foundation.ingest-batch-objects` 에서만 읽는다(ADR-0062,
`lakehouse_ingest.read_ingested_objects`, Rust `load_ingested_batch_objects`). 이 요약은 스냅숏이 만료되면 함께 사라진다.
그러면 이미 붙인 객체가 "아직 안 실림" 으로 읽혀 다시 붙는다. ADR-0062 의 사고와 같은 모양이다: 2026-08-27
`silver.parcel_boundaries` 에 필지 1,865,891 개가 세 번 들어갔다. ADR-0328 은 같은 이유로 판 기록을 표 속성으로 옮겼고,
객체 기록은 남은 일로 적었다.

지금 그 사고를 막고 있는 것은 정비 작업의 만료 보호다(`lakehouse_maintenance.protected_snapshots`). main 계보에서
적재 기록을 가진 스냅숏은 만료하지 않고, 만료 하한을 가장 오래된 기록 스냅숏까지 끌어내린다. 그 결과 기록이 있는 표는
첫 적재 뒤로 스냅숏을 하나도 만료하지 않는다. ADR-0237 의 30일 보존이 그 표들에는 걸리지 않는다. 그리고 기록의
영속성이 보존 정책과 한 몸이 된다. 보호를 풀거나 다른 길로 만료하면 기록이 사라진다.

**잰 것(2026-10-29, 운영 Trino `$snapshots` 읽기 전용, 표 32개).**

| 항목 | 값 |
|---|---:|
| 적재 기록을 가진 표 | 27 |
| 30일(2026-09-29 이전)보다 오래된 스냅숏 | 180 (16개 표) |
| 그중 적재 기록을 가진 것 = 지금 요약에만 있고 보호에 기대는 기록 | **173** |
| 이미 사라진 기록 | 0. 첫 스냅숏이 남아 있거나, 기록 이전 적재가 ADR-0069 백필로 기록됨 |
| 가장 긴 기록 값 | 5,759자 (`building_register_apartment_price`) |

### 정본을 어디에 둘지 찾아본 것

- **`catalog.bronze_object`(Postgres Bronze 장부)**: Bronze 객체의 수집 사실을 갖는다. "어느 Silver 표가 그 객체를
  실었나" 는 갖지 않는다. 데이터와 같은 커밋에 쓰이지도 않는다. 그리고 레이크하우스가 정본이고 Postgres 는 다시
  만드는 투영이다(ADR-0046, 기억 "Lakehouse is the center"). 기록을 거기 두면 데이터와 기록이 갈라질 수 있다.
  ADR-0062 가 없애려던 틈이 그것이다.
- **표 속성(ADR-0328)**: 같은 카탈로그의 표 메타데이터다. 만료되지 않는다. 판 기록이 이미 이 모양이다
  (`foundation.edition.<판>`). 객체 기록도 여기 둔다.
- **같은 커밋에 쓰기**: Spark DataFrame 쓰기는 커밋에 요약만 실을 수 있고 표 속성은 실을 수 없다. 판 기록이 같은
  커밋에 들어가는 것은 Rust `publish_branch` 가 REST 커밋을 직접 만들기 때문이다. main 에 바로 붙이는 적재 작업 14개는
  그 길이 아니다. 그러나 같은 커밋이 꼭 필요하지는 않다. 요약을 지우는 것은 만료 하나뿐이다. 그래서 만료보다 먼저
  속성을 쓰면 "기록은 요약이나 속성, 또는 둘 다에 있다" 가 모든 커밋 경계에서 성립한다.

### 실사례

- **Delta Lake `txn`(SetTransaction)**: 스트리밍 쓰기의 멱등 기록(appId, version)이 로그에 있고, 체크포인트가 그 기록을
  앞으로 옮긴 다음에야 옛 로그 파일을 지운다(`delta.logRetentionDuration`). 이 결정의 순서가 그대로 그것이다.
- **Apache Iceberg Flink sink `flink.max-committed-checkpoint-id`**: 요약에 두고, Iceberg 문서가 "그 스냅숏을 만료하지
  말라" 고 경고한다. 지금 우리 보호가 그 경고를 따른 것이다. 영속 사본이 생기면 보존 정책이 이 기록에 묶이지 않는다.
- **Kafka `__consumer_offsets` 압축(compaction)**: 키마다 마지막 값을 남기고 나머지 로그는 지운다. 기록의 수명과
  로그의 수명이 따로다.

## Decision

1. **영속 사본은 표 속성이다.** `foundation.ingest-batch-objects.<스냅숏 id>` = 그 커밋 요약의 값(쉼표로 이은 적재
   식별자). 커밋 하나에 속성 하나를 둔다. 스냅숏 id 가 키라 같은 기록을 두 번 쓰지 않는다. 독자는 객체마다 그것을
   넣은 스냅숏을 그대로 안다.
2. **정비 작업이 만료보다 먼저 옮긴다.** `run_snapshot_expiry` 는 태그를 정리한 뒤 만료 전에 다음을 한 커밋
   (`ALTER TABLE … SET TBLPROPERTIES`)으로 쓴다: main 계보에서 보존 창(30일)보다 오래되었고 속성이 아직 없는 적재
   기록 전부(`lakehouse_ingest.ingest_record_properties`). 이 커밋이 실패하면 만료로 가지 않는다. 만료가 실패하면
   기록이 둘 다에 남을 뿐이다. 이 저장소에서 `expire_snapshots` 를 부르는 곳은 이 작업 하나다(전수 검색).
3. **모든 독자가 요약과 속성을 함께 읽는다.** Python `read_ingested_objects`(모든 적재기, `silver_release_compare`,
   `append_source_object`, 레지스트리 백필)와 Rust `load_ingested_batch_objects`·`load_staged_batch_objects`(Silver
   refresh 레인, `plan-silver-load-objects`)다. 속성은 main 의 기록뿐이고, 브랜치는 main 에서 만들므로 어떤 브랜치의
   계보에도 들어 있다(ADR-0328 `read_editions` 와 같은 이유).
4. **만료 보호는 이 결정에서 그대로 둔다.** 기록은 더 이상 보호에 기대지 않는다. 그러나 보호가 지금 함께 지키는 것이
   둘 있다:
   - Gold 증분 재구축이 고정 판(Silver 스냅숏)을 다시 읽는다(`gold_incremental`).
   - FLOOR 작업이 증인 스냅숏까지 append 사슬을 걷는다(`building_register_floor_history.verify_append_descendant`,
     정비 계약 `tables."silver.building_register_floors"`).

   보호를 풀어 30일 보존을 되살리는 것은 지난 판을 지우는 되돌릴 수 없는 일이다. 그래서 그 둘을 각자의 규칙으로
   지킨 다음, 따로 정한다.

## Consequences

| 항목 | 전 | 뒤 | 근거 |
|---|---|---|---|
| 30일 지난 적재 기록의 사본 | 요약 하나(보호에 기댐) 173개 | 요약 + 표 속성 173개(첫 정비 순회) | 실측 + 2번 |
| 요약이 만료된 객체를 다시 실을 때 | 다시 붙음(중복) | `already_appended`·건너뜀 | 시험 |
| 기록 영속성이 보존 정책에 묶임 | 예 | 아니오 | 4번 |
| 표 메타데이터 크기 | — | 적재 커밋당 속성 하나(대개 100자 안팎, 최대 5,759자) | 실측 |

- 자주 적재하는 표는 속성이 빨리 는다. 공매 물건·입찰결과는 10일에 41·45 커밋(해마다 약 1,600 개, 계산), 공매 사진은
  2일에 57 커밋(해마다 약 10,000 개, 키와 값 약 140바이트씩 약 1.4 MB, 계산)이다. 그 크기가 커밋마다 다시 쓰는
  `metadata.json` 에 더해진다. 커지면 기록을 묶을 수 있다(예: 달마다 속성 하나). 그때 바꿀 곳은 독자 둘과 이 작업뿐이다.
  `shortcut`: 지금은 커밋 하나에 속성 하나. 사진 표의 `metadata.json` 이 1 MB 를 넘으면 월별로 묶는다.
- 재시도 인증(`silver_scalar_handoff_to_lakehouse.recorded_input_times_match`)은 기록한 스냅숏의 행을 다시 읽는다.
  그 스냅숏이 만료된 뒤에 같은 객체를 다시 돌리면, 조용히 건너뛰지 않고 "recorded append snapshot is unavailable" 로
  거부한다. 중복도 아니고 낡은 재적재도 아니다.
- 이 결정 전에 이미 만료된 기록은 되살릴 수 없다. 실측으로 그런 표는 없다.
- 시험:
  - `test_lakehouse_ingest`: 요약이 없고 속성만 있는 기록을 읽으면 다시 붙이지 않는다.
  - `test_lakehouse_maintenance`: 창 밖의 기록만 옮기고, 이미 있는 속성은 다시 쓰지 않는다. 값은 Spark SQL 문자열로
    따옴표 처리한다.
  - `test_month_end_tags_iceberg`: 실제 Iceberg 에서 정비 실행 → 기록 스냅숏을 `expire_snapshots` 로 만료 →
    독자가 같은 객체·스냅숏을 돌려주고 붙일 것이 없다. 두 번째 실행은 속성을 다시 쓰지 않는다.
  - `iceberg_rest_catalog_adapter`: 만료된 기록을 Rust 독자가 속성에서 읽는다.
- 이 결정이 막는 사고(ADR-0001): 2026-08-27 의 세 번 적재와 같은 중복 적재. 이번에는 보존 정책이 기록을 지운 뒤에
  생기는 경우다.

## Rejected

- **`catalog.bronze_object` 에 "실린 표" 열을 더하기**: Postgres 는 투영이고 데이터와 같은 커밋이 아니다. 기록과 행이
  갈라지는 틈(ADR-0062 의 사고)을 다시 연다.
- **적재기가 붙인 직후 속성을 쓰기**: 쓰는 곳이 적재 작업 14개와 Rust 레인 2개로 흩어진다. 요약이 30일 동안 그 일을 이미 하므로
  더 지켜 주는 것도 없다. 요약을 지우는 단 한 곳(만료) 앞에서 옮기는 것이 쓰는 곳 하나다.
- **스냅숏 만료를 Rust REST 커밋(`remove-snapshots` + `set-properties` 한 커밋)으로 다시 짜기**: 같은 커밋이 되지만
  Iceberg `expire_snapshots` 가 하는 파일 정리를 잃는다. 2번의 순서로 같은 불변식이 선다.
- **만료 보호만 계속 믿기**: 지금 그 보호 때문에 기록이 있는 표 27개가 30일 보존을 받지 못한다. 보호를 풀거나 다른
  길로 만료하는 순간 중복이 생긴다.
