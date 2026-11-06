# ADR 0432: 발행 데이터셋은 계약에서 읽은 규칙의 막는 Dagster 자산 검사를 통과해야 읽힌다 — 첫 둘은 11-04 의 두 사고

- Status: Accepted (2026-11-05, 소유자 지시 "근본 구조전환까지 전부 알아서 빠르게 진행해")
- Date: 2026-11-05
- Builds on: [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(엔진은 Dagster, 0단계 airlift observe #946),
  [ADR-0334](./0334-each-user-journey-has-an-slo-and-every-data-release-is-written-audited-and-canaried-before-it-is-served.md)(쓰기·감사·발행),
  ADR-0415(정의 하나, #1062),
  [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md)(삭제 표시), [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md)(현재 행)
- 관련 사건(2026-11-04): 공매 사진 Silver(`silver.onbid_real_estate_item_photo`)에 행이 있는데 화면의 회차 사진이 비었다.
  용도지역지구 구역(`silver.land_use_zone_area`)의 현재 행에서 `(layer_code, zone_feature_id)` 가 겹쳤다. 둘 다 작업은 초록이었고, 발행 전에 아무도 검사하지 않았다.

## Context

ADR-0333 시험 (d)("파티션 검사가 실패하면 그 자산의 하류를 막는다")는 Airflow 3.3.2 에서 되지 않았다(#940). Dagster 의 자산 검사는
`blocking=True` 이면 같은 실행에서 하류 자산을 물질화하지 않는다(dagster.io/docs/guides/test/asset-checks "Blocking downstream materialization",
1.6.4 에서 도입). #946 은 Dagster 를 Airflow 의 관찰자로 두고(airlift observe), 단가 검사 둘을 보고 전용(`blocking=False`, WARN)으로 붙였다.

2026-11-05 운영 Trino 에서 읽기 전용으로 잰 값(아래 검사의 SQL 그대로):

| 검사 | 결과 | 걸린 시간 |
|---|---|---:|
| 용도지역지구 현재 키 유일 | 현재 행 655,574, 키 655,266 → **겹친 행 308, 모두 층 D036, 한 스냅숏 안** | 약 2.5 s |
| 공매 회차의 사진 | 서빙 회차 28,802, 사진 있는 물건의 회차 28,705, 사진이 빈 회차 **0**(11-04 사고는 고쳐짐) | 약 4 s |

용도지역지구의 겹침은 지금도 있다. 계약의 `current_row_latest.keep_ties: true` 가 한 스냅숏 안의 같은 키 두 행을 둘 다 현재로 둔다.

## Decision

1. **발행 검사는 막는다.** 발행 검사는 자산을 가진 정의 파일의 `asset_checks` 절에 `blocking: true` 로 선언되고(심각도 ERROR),
   `orchestration/dagster/asset_checks.py` 가 그 선언을 `orchestration/dagster/publish_checks.py` 의 검사 종류에 붙인다. 레인의 작업이
   Dagster 로 옮겨진 날부터, 검사가 실패한 실행은 그 자산의 하류(서빙 묶음)를 물질화하지 않는다. 그 전(airlift observe)에는
   airlift 의 센서가 Airflow 의 물질화를 본 뒤 검사를 돌려 실패를 자산에 기록하고 무엇이 틀렸는지 표본을 남긴다.
2. **규칙은 다시 적지 않는다.** 표 이름은 파이프라인 그래프 노드의 `table_name`, 키·최신 순서·삭제 열은 레이크하우스 계약의
   `current_row_latest`(`platform_contracts.current_row_latest`), 매체 종류·물건 열은 생산자 모듈
   (`onbid_public_auction_document.ROUND_MEDIA_KINDS`·`ITEM`·`PHOTO_SOURCE`)에서 읽는다.
3. **현재 행은 한 규칙으로 고른다.** `current_rows_sql(table, columns)` 는 `platform_contracts.current_rows` 의 규칙을 Trino SQL 로 쓴다:
   키마다 `newest_first` 내림차순(nulls last) 1순위, 동률은 모두 남기고(검사가 센다), 삭제 열이 `removed` 인 행을 뺀다.
   술어(`current_row_predicate`)나 판(`editions`)으로 고르는 표는 번역이 없어서 거부한다. 추측하지 않는다.
4. **첫 검사 둘.**
   - `current_key_unique`(`iceberg/silver/land_use_zone_area`): 현재 행 수 = 현재 키 수. 실패하면 겹친 키 표본 5개를 남긴다.
   - `rounds_carry_their_media`(`iceberg/gold/parcel_public_auction`, 추가 입력 사진 Silver): 물건에 현재 매체 행이 있으면
     그 물건의 모든 서빙 회차(`public_auction_json.rounds[]`)의 `photos` 가 비지 않는다. 실패하면 물건 표본 5개를 남긴다.
5. **검사 하나 = 종류 하나 + 선언 + 시험.** 다음 검사는 같은 틀(`*_sql` + `*_result` + `KINDS` 의 항목)로 종류를 더하고, 자산의
   정의 파일에 `{"asset", "check", "blocking", "reads"}` 를 적는다. 노드 id·표 이름은 코드에 적지 않는다. 생성기
   (`scripts/catalog/declared_definitions.py`)는 남의 그룹 자산에 붙은 검사와 중복을, 코드 위치는 그래프에 없는 노드와
   구현되지 않은 종류를 거부한다. 계약 `quality` 에서 종류를 생성하고 품질 블록이 없는 발행 데이터셋을 거부하는 일은
   ADR-0410 §2.4 의 다음 조각이다.

## Consequences

- 운영 영향은 0단계에서 기록뿐이다: Airflow 가 작업을 돌리고, 검사는 Trino 에서 수 초 읽는다(위 측정). 메모리는 #946 의 데몬 상한(480m) 안이다.
- 용도지역지구 검사는 오늘 실패한다(308 행). 이것을 고치는 일(D036 층의 같은 키 두 행을 어떻게 하나로 할지 — 원천의 중복인지, 키가 모자란지)은
  그 레인의 결정이며 이 ADR 밖이다. 검사는 그 결정 전까지 실패를 기록하고, 레인이 Dagster 로 옮겨지면 발행을 막는다.
- 공매 검사는 오늘 통과한다. 11-04 사고와 같은 모양이 다시 생기면 기록되고, 이후에는 막힌다.
- 시험: `orchestration/tests/test_dagster_publish_checks.py`(CI `dagster-observe` 레인, 코드 위치 이미지 안). 선언된 발행 검사가 모두 막는지,
  그래프 노드에 붙는지, 계약마다 현재 행 SQL 이 계약의 키·순서·삭제 열을 따르는지, 번역 없는 규칙을 거부하는지.

## 기각한 대안

- **Airflow 작업 끝에 SQL 검사 단계를 더하기**: 실패를 자산에 기록하고 하류만 막는 수단이 Airflow 에 없다(ADR-0333 (d)).
- **`datacontract test` 로 계약 품질만 돌리기**: 키 유일성은 되지만 "Silver 에 매체가 있으면 Gold 회차가 그것을 싣는다" 같은 표 사이 규칙이 없다.
  계약 품질의 일반 규칙은 3단계에서 같은 틀로 생성한다.
- **검사마다 표·키를 문자열로 적기**: 계약과 갈라진다(ADR-0415 R4).
