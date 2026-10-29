# ADR 0328: 비교로 쌓는 Silver 표는 키의 버전을 판(edition) 다음 커밋 시각으로 정렬한다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0318](./0318-the-remaining-silver-lanes-append-their-changed-keys-compared-bucket-by-bucket.md)
  §4(허브 대장의 최신 순서 `source_snapshot_id`)·§5(`rows_as_of` 가 버전 없는 표를 읽는다는 문장)·§7(전유부 다리의
  최신 순서 `vintage`), [ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)
  (토지 레인 다섯의 최신 순서), [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md)
  §2(판 기록은 스냅숏 요약에만 있다)
- Builds on: [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md),
  [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md),
  [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md),
  [ADR-0237](./0237-lakehouse-snapshots-are-kept-30-days-12-month-ends-and-every-year-end.md)

## Context

비교 레인 11개(허브 대장 셋·전유부 다리·토지 여섯·용도 코드표)는 판마다 바뀐 키의 행과 사라진 키의 삭제 표시만
main 에 붙인다(ADR-0275·0311·0318). 독자는 계약의 `current_row_latest` 로 키마다 가장 새 버전을 고른다
(`keep_ties: true`: 같은 순위의 행은 모두 현재 행). ADR-0275 §2 는 그 순서를 `(판, ingested_at_utc)` 로 정했지만
ADR-0311·0318 의 계약은 판 열 하나만 적었고, 그 순서가 "같은 판을 다시 실은" 경우를 가르지 못했다.

| 표 | 정렬(전) | 같은 판을 다시 실으면 |
|---|---|---|
| 토지 여섯 | `source_snapshot_id` = `<label>:<YYYYMMDD>` | 다시 실은 판의 id 가 같다: 바뀐 행이 옛 행과 비기고(둘 다 현재), 삭제 표시가 지울 행과 비겨 버려진다 |
| 허브 대장 셋 | `source_snapshot_id` = `silver-refresh-<lane>-YYYYMM-<sha16>` | 같은 달 안의 순서가 적재 시각이 아니라 바이트 해시다: 해시가 작은 재수집 판이 진다. main 은 객체를 `already_appended` 로 기록해 다시 시도하지도 않는다 |
| 전유부 다리 | `vintage` | 토지와 같다. 아래 `unit_official_price` 가 조인을 거부하거나(`CONFLICTING_SQL`) 행을 겹친다 |
| 용도 코드표 | `ingested_at_utc` | 문제없음 |

**운영에서 이미 일어났다.** 2026-10-29 운영 Trino(읽기 전용) 실측:

- `silver.land_transfer_history`: 10-15 에 판 `vworldkr-land-transfer-history:20260930` 이 실렸고, 10-21 에 같은 판이
  재수집 객체(4598–4613)로 다시 실려 비교가 203,061행을 붙였다(바뀐 키의 행 201,476 + 삭제 표시 1,585). 모두 같은
  `source_snapshot_id` 다. 그 결과 지금 현재 행에는 **삭제된 키 1,585개가 그대로 살아 있고, 바뀐 키 19개는 옛 행
  19개가 새 행 옆에 함께 현재**다. 영향 PNU 1,599개(바뀌거나 지워진 키가 있는 PNU).
- 같은 판을 두 번 가진 표는 이것 하나다. `building_register_exclusive_unit` 은 판 202608·202609·202610 이 각
  `source_snapshot_id` 하나·적재 시각 하나다(19,822,168 · 19,847,604 · 19,860,453행, 삭제 표시 0). 허브 대장 셋은
  판 하나(202610)만 있다: 표제부 8,064,354 · 전유부 19,860,453 · 전유공용면적 114,461,830행, 각 시각 하나. 토지
  나머지 다섯도 판 하나에 시도마다 적재 시각 하나다. 네 허브 표에는 changes branch 가 아직 없다(비교 첫 실행 전).
- 기존 행 중 한 키의 행이 같은 판 안에서 둘 이상의 적재 시각을 가진 경우: 위 표들은 시도(키의 첫 열 접두사)마다
  또는 판마다 시각이 하나이므로 없다. `land_transfer_history` 의 10-15 행도 시도마다 객체 하나·시각 하나다.

판 기록도 약했다. 비교의 봉인 커밋이 `foundation.edition` 을 스냅숏 요약에 적고 `refuse_older_edition` 이 그것을
읽는데, 요약은 30일이면 만료된다(ADR-0237). 바뀐 키가 없는 판은 행도 남기지 않으므로 30일 뒤에는 오래된 판을
거부할 근거가 사라진다. 지금 `building_register_exclusive_unit` main 에는 판 기록 요약이 하나도 없다(비교 전 적재).

## Decision

1. **비교로 쌓는 표의 순서는 `[판, ingested_at_utc]` 또는 `[ingested_at_utc]` 다.** 판 열은 토지 레인의
   `source_snapshot_id`, 전유부 다리의 `vintage`, 허브 대장의 `valid_from_utc`(제공자 월의 첫날, 이미 필수 열)다.
   허브 대장은 실행 이름의 해시가 아니라 제공자 월로 판을 가른다. 계약 10개의 `newest_first` 를 바꾼다(코드표는 그대로).
   `silver_release_compare.edition_column` 이 다른 모양을 거부하고, 시험이 모든 비교 레인을 대조한다.
2. **비교는 붙이는 모든 행에 판과 실행 시각 하나를 찍는다**(`commit_stamp`). 바뀐 행과 삭제 표시 모두, 열의 타입으로
   변환해서. 그래서 같은 판을 다시 실은 행은 앞 적재를 이기고, 삭제 표시는 지우는 행을 언제나 이긴다: 판은
   가진 판보다 작지 않고(3번), 시각은 main 의 어느 행보다 늦다. 늦지 않으면(시계가 거꾸로) 비교가 거부한다.
   이어 받은 실행은 자기 시각을 찍는다. 키의 행은 모두 한 묶음에 있으므로 키마다 한 버전이다.
3. **가진 판보다 오래된 판은 언제나 거부한다.** 가진 판 = 커밋 요약 ∪ 표 속성 `foundation.edition.<판>` ∪ 행이 가진
   가장 새 판. 표 속성은 `publish_branch` 가 main 을 옮기는 **같은 카탈로그 커밋**에서 발행하는 계보의 판마다
   적는다(Iceberg REST `set-properties`). 속성은 만료되지 않는다. 행의 최댓값은 속성이 생기기 전의 표를 덮는다.
   이 규칙은 토지 레인에도 적용된다(판 = 실린 판의 id).
4. **봉인의 판 값은 행에 찍은 값과 같다.** 판으로 정렬하는 표는 그 판, 시각으로만 정렬하는 코드표는 그 실행의
   시각이다(전에는 적재 단계 행의 최대 시각이라 바뀐 행의 시각과 달랐다).
5. **`--objects` 가 비면 거부한다.** `all([])` 이 빈 봉인을 봉인으로 읽어 비교 없이 `staged` 를 보고했다.
6. **`rows_as_of(frame, contract, release)` 의 `release` 는 판 값이다.** 같은 판을 두 번 실었으면 나중 적재가 이긴다.
   버전이 없거나 삭제 표시가 없는 표는 거부한다(ADR-0317 §3 과 같음). ADR-0318 §5 의 "버전이 없는 표는 그 판의
   행을 그대로 읽는다"는 문장은 코드와 달랐고 이 결정이 고친다. 허브 표제부를 실행 이름으로 읽는
   `parcel_lineage_to_silver.read_buildings` 는 그 실행 행의 판(월)을 찾아 읽는다.

### 실제 사례

- **Kimball SCD Type 2 / dbt snapshot**: 버전은 업무 시점과 적재 시점 두 축으로 정렬하고, 같은 업무 시점을 다시
  실으면 `dbt_updated_at`(적재 시각)이 가른다. 1·2번이 같다.
- **SQL:2011 bitemporal 표**: 업무 시간(여기서 판)과 시스템 시간(커밋 시각)을 함께 둔다. 시스템 시간은 쓰는 쪽이
  찍고 단조 증가한다. 2번의 "main 보다 늦은 시각만" 이 그 불변식이다.
- **Delta Lake CDF / Iceberg 스냅숏 계보**: 커밋 순서가 같은 키의 변경을 가른다. 해시는 순서가 아니다(허브 대장).
- **Apache Iceberg 표 속성 · Flink sink `flink.max-committed-checkpoint-id`**: 만료되지 않아야 할 기록은 스냅숏
  요약이 아니라 표 메타데이터에 두고, 데이터를 옮기는 같은 커밋에서 쓴다. 3번이 같다.

## Consequences

| 항목 | 전 | 뒤 | 근거 |
|---|---|---|---|
| `land_transfer_history` 현재 행의 죽은 키 | 1,585 | 0 | 실측(10-29); 배포 순간 순서만으로 바뀜, 새 행 없음 |
| 같은 표의 옛 행이 새 행과 함께 현재인 키 | 19 | 0 | 실측 |
| 전유부 다리·허브 대장 셋·토지 다섯의 현재 행 | — | 같음 | 키마다 판 안의 적재 시각이 하나(실측) |
| 같은 달 재수집(허브)·같은 판 재수집(토지·전유부) | 진다·비긴다 | 이긴다 | 시험 |
| 오래된 판 거부 | 요약 만료(30일) 뒤 사라짐 | 표 속성·행으로 유지 | 시험 |

- **바로잡기는 덧붙이지 않는다.** 10-21 비교가 붙인 행은 이미 실행 시각(10-21 03:48–08:19)을 가져 10-15 행
  (22:10–22:12)보다 늦다. 순서에 시각을 더하는 것만으로 현재 행이 10-21 판이 된다. 행을 지우거나 새로 쓰지 않는다
  (append-only 그대로). 다른 표는 현재 행이 그대로다.
- **Gold 는 저절로 따라오지 않는다.** 필지 패널의 변경 집합은 두 핀 사이 현재 행의 차이(ADR-0180)인데, 이번 변화는
  커밋이 아니라 계약에서 오므로 차이에 잡히지 않는다. 영향 PNU 1,599개의 by-PNU 문서는 그 PNU 를 다시 굽기 전까지
  옛 이전 이력(죽은 1,585키 포함)을 보인다. 그 PNU 만 다시 굽는 일은 운영 창과 사장 승인이 필요하다(아래 남은 일).
- 비교 첫 실행(허브 넷)은 지금과 같은 현재 행을 비교한다(판 하나, 시각 하나). 가진 판은 행의 최댓값으로 정한다
  (전유부 202610, 허브 2026-10-01). 비교는 main 의 `ingested_at_utc` 를 한 번 더 걸러 읽는다(최대 시각 확인,
  Iceberg 파일 통계로 거른다) 그리고 판 열의 최댓값을 한 번 읽는다.
- 허브 대장의 순서가 실행 이름이 아니게 되면서 건물 패널의 "가장 새 판 이름"도 계약의 순서로 고른다
  (`max_by`); 정확한 고정은 전과 같이 Iceberg 핀이다.
- 시험: `test_silver_release_compare_versions_iceberg`(전유부 같은 판 재적재·허브 같은 달 낮은 해시·이어 받은
  실행의 시각 둘·표 속성으로 오래된 판 거부, 모두 `compare_and_append` 전 과정), `test_silver_release_compare_lanes_spark`
  (모든 비교 레인에서 같은 판 재적재), `test_silver_release_compare`(순서 모양 거부·빈 `--objects` 거부),
  `iceberg_rest_catalog_adapter`(발행 커밋이 판 속성을 적는다).
- 남은 일: (1) `land_transfer_history` 영향 PNU 1,599개 부분 재굽기(사장 결정). (2) 판 기록을 표 속성으로
  옮긴 것과 같은 이유로 `foundation.ingest-batch-objects`(객체 기록)도 요약 만료에 기대고 있다. 같은 모양으로 옮길지
  따로 정한다.

## Rejected

- **허브 실행 이름에 적재 시각을 넣어 정렬**: 이미 실린 이름(`…-202610-d1e6…`)과 새 이름이 같은 달에서 잘못
  정렬된다(`d` > `2`). 이름은 식별이고 순서가 아니다.
- **삭제 표시만 더 큰 값으로 찍기**: 같은 판을 다시 실은 바뀐 행도 옛 행과 비긴다. 순서 자체가 모자랐다.
- **판 기록을 비교 작업이 봉인 때 표 속성으로 쓰기**: 발행 전에 main 이 갖지 않은 판을 가졌다고 말한다. 발행과
  같은 커밋에 쓴다.
- **잘못 현재인 행에 정정 삭제 표시를 덧붙이기**: 순서를 고치면 필요 없고, 고치지 않으면 다음 재적재에 다시 생긴다.
