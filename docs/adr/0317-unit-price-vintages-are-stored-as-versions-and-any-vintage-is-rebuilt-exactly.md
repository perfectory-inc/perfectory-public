# ADR 0317: 세대 가격의 판은 전체 사본이 아니라 바뀐 키의 버전으로 쌓고, 어느 판이든 그대로 다시 만든다

- Status: Accepted
- Date: 2026-10-28
- Amends: [ADR-0101](./0101-unit-prices-preserve-every-reference-date.md) §4(판마다 전체 배치를 append 하고 읽기는
  마지막 선택의 전체 배치), [ADR-0182](./0182-the-gold-rebuild-joins-the-unit-prices-and-the-panel-reads-the-newest-vintage.md)
  §2·§3(한 판에 한 묶음, 패널은 가장 새 판의 묶음), [ADR-0185](./0185-a-unit-price-batch-is-its-snapshots-and-the-panel-reads-the-last-one-of-a-vintage.md)
  §1(스냅숏이 바뀌면 같은 판을 다시 조합)
- Builds on: [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md)(바뀐 키만 쌓기와 삭제 표시),
  [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md)(현재 행은 계약에서),
  [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md)(브랜치 적재),
  [ADR-0189](./0189-an-appended-silver-input-is-its-own-change-set.md)(append 입력의 변경 집합)

## Context

공동주택가격 원천은 판(vintage, 월)마다 **전국 전체 이력**(기준일 2006–2026 전부)을 다시 낸다. 두 Silver 표는 그 판을
통째로 쌓는다. 2026-10-28 운영 실측(Trino `$partitions`·`$snapshots`·집계, 읽기 전용)은 다음과 같다.

| 표 | 쌓는 방식 | 행 | 크기 |
|---|---|---:|---:|
| `silver.building_register_apartment_price` | 판마다 원천 파일 전체 append | 202,102,050 / 202,097,817 / 202,058,239 (202608·09·10), 합 606,258,106 | 판마다 약 7.0 GB, 합 21.05 GB |
| `silver.unit_official_price` | 조합 한 번마다 전국 묶음 전체 append | 묶음마다 약 1.887억, 합 1,207,165,362 | 3.14 GB |

- 조합 표에는 30일 동안 묶음 다섯 개(10-12, 10-15, 10-20, 10-20, 10-21)가 붙었다. 10-15·10-20 07:01·10-21 은 압축
  (`replace`)이 스냅숏 id 를 바꾸자 ADR-0185 §1 에 따라 **같은 판을 다시 조합한 것**이다. 10월에 붙은 10.18억 행 중
  적어도 5.66억 행은 이미 있던 내용의 사본이다.
- 판 사이에 실제로 바뀌는 행(조합 표, 같은 판의 마지막 묶음끼리, 키+내용 다중집합 비교):

  | 시도 | 202608 → 202609 | 202609 → 202610 | 판 크기 |
  |---|---|---|---:|
  | 세종(36) | 0 | 0 | 1,325,948 |
  | 서울(11) | 추가 323 · 삭제 1,560 | 추가 0 · 삭제 6,578 (0.023%) | 29,068,310 |

  원천 표의 같은 비교는 재지 못했다. PNU 로 걸러도 판 전체를 읽어 by-PNU 굽기가 도는 동안 600초 상한을 넘었다
  (멈춤). 원천은 조합 앞 단계라 바뀌는 비율이 같은 크기일 것으로 본다(추정).
- 10-20 원천 적재는 CPU 10시간 25분이었다. 0.02% 를 위해 100% 를 쓰고, 그 100% 가 표에 영구히 남는다(append-only).

사장 결정(2026-10-28): (키, 판)마다 바뀐 행만 저장하고, 어느 기준 시점이든 정확히 다시 만든다. ADR-0101 의
"모든 기준일 보존"은 뜻 그대로 지킨다.

### 실사례

| 사례 | 저장 | 시점 읽기 |
|---|---|---|
| Kimball SCD Type 2 | 키마다 버전 행(유효 시작·끝) | 시점이 구간 안인 행 |
| dbt snapshot `strategy: check` | `check_cols` 의 지문이 바뀐 키만 새 버전 | `dbt_valid_from <= t < dbt_valid_to` |
| Delta Lake Change Data Feed | 커밋마다 `_change_type`(insert/delete/update)·`_commit_version` | `VERSION AS OF`, `table_changes(from, to)` |
| Apache Iceberg `create_changelog_view` | 스냅숏 사이의 순변경(식별 열 기준) | 스냅숏 시간 여행 |
| SQL:2011 system-versioned(bitemporal) 표 | 업무 시간 열 + 시스템 시간 구간 | `FOR SYSTEM_TIME AS OF t` |

공통점: 바뀐 것만 쓰고, 시점의 상태는 "그 시점 이하에서 키마다 가장 새 버전"으로 읽는다. 여기서 업무 시간은 이미
행에 있는 `base_date`(ADR-0101)이고, 시스템 시간은 판(`vintage`)과 커밋 시각이다. 판은 Iceberg 스냅숏이 아니라
업무상의 판이다: 스냅숏은 30일이면 만료된다(ADR-0237).

## Decision

1. **두 표 옆에 버전 표를 둔다.** `silver.building_register_apartment_price_versions`,
   `silver.unit_official_price_versions`. 열은 옛 표와 같고, `row_state`(삭제 표시), 조합 표에는 `vintage`·
   `ingested_at_utc` 를 더한다. 분할은 `vintage` 다. 계약은
   `current_row_latest { key, newest_first: [vintage, ingested_at_utc], removal_column: row_state }` 이다.
   - 원천: 키 `(mgmt_key, base_date)`, `keep_ties: true` — 정정공시가 한 판 안에서 키를 되풀이한다.
   - 조합: 키 `(mgm_bldrgst_pk, base_date)`, `keep_ties: false` — 정정공시는 조합 전에 하나로 확정된다(ADR-0101 §1).
2. **쓰기는 ADR-0275 의 비교 하나다.** 판 하나를 쓸 때 `silver_release_compare.release_changes` 로 현재 행과 키마다
   비교해, 새 키·내용이 바뀐 키의 행 전부와 사라진 키의 삭제 표시만 한 커밋으로 append 한다. 같은 판이면 아무 행도
   쓰지 않는다(빈 커밋). 그 함수에 셋을 더한다.
   - **키는 null-safe 로 맞춘다.** 관리번호가 없는 원천 줄도 한 키다. 이전에는 null 키가 판마다 "바뀜"과 "삭제"로
     동시에 셌다.
   - **커밋의 순서 값을 모든 행에 찍는다**(`commit_stamp`). `newest_first` 의 판 열에는 그 판을, `ingested_at_utc` 에는
     커밋 시각 하나를. 한 키의 행이 다른 배치에서 왔어도 한 버전이고, 같은 판을 다시 실으면(부분 적재 뒤 전체 적재,
     ADR-0185 의 10-11) 나중 커밋이 이긴다. 토지 레인(`newest_first: [source_snapshot_id]`)은 찍는 것이 없어 그대로다.
   - **커밋이 판을 기록한다**(`foundation.edition` 스냅숏 요약, `read_editions`). 바뀐 키가 없는 판은 행이 없으므로
     "실렸는가"는 행이 아니라 커밋이 말한다. 가진 판보다 **오래된 판은 거부**한다(`refuse_older_edition`): 뒤 판들이
     비교한 상태를 바꾸면 뒤 판들을 다시 만들 수 없다.
3. **시점 읽기는 한 함수다.** `platform_contracts.rows_as_of(frame, contract, vintage)` = 판이 `vintage` 이하인 버전에서
   `current_rows`. 가장 새 판은 `current_rows` 그대로다. 삭제 표시가 없는 표(옛 전체 사본 표)에는 거부한다: 판이
   떨어뜨린 키의 옛 행이 현재로 보이기 때문이다. dbt·Trino 는 같은 뜻을 `foundation_current_rows` 매크로 앞에
   `vintage <= ?` 조건으로 렌더한다(단계 3).
4. **"그대로"의 뜻.** 판 V 의 재구성은 그 판의 전체 사본과 **키와 내용 열의 다중집합이 같다**(반복 행 포함, null 과
   빈 문자열 구별). 원천 표의 내용 열에는 원천 줄 전체인 `raw_columns` 가 들어 있으므로 원천 내용은 바이트 단위로
   같다. 적재 열(`source_record_id`·`source_part_id`·`source_line_number`·`source_snapshot_id`·`ingested_at_utc`)은 그
   버전이 처음 나타난 판의 값이다. SCD2·dbt snapshot 이 행을 처음 본 그대로 두는 것과 같다. 줄 번호는 판마다
   움직이므로 판마다 다시 쓰면 아무것도 줄지 않는다. 내용 열은 `vintage_versions.APARTMENT_PRICE_CONTENT`·
   `UNIT_PRICE_CONTENT` 에 적는다(ADR-0275: 판마다 바뀌는 열은 넣지 않는다).
5. **이행은 덧붙이기만 한다.**
   1. 이 PR: 계약 두 개, 위 함수들, 일회성 이행 잡 `vintage_versions.py`, 동등성 시험. 운영에 쓰는 것은 없다.
   2. 운영자(창 안, 사람 손 한 번): `vintage_versions.py --target silver.building_register_apartment_price_versions`,
      그다음 `--target silver.unit_official_price_versions`. 잡은 옛 표의 판을 오래된 것부터 버전 표에 쓰고(조합 표는
      판마다 패널이 읽던 마지막 묶음, ADR-0185 §2), 판마다 `rows_as_of` 와 옛 사본을 `exceptAll` 로 양방향 비교한다.
      어느 판이든 차이가 0 이 아니면 실패한다. 다시 돌리면 기록된 판은 건너뛰고 비교만 다시 한다. 옛 표는 읽기만 한다.
   3. 후속 PR(draft, 이 PR 에 기댄다): 쓰는 쪽과 읽는 쪽을 함께 옮긴다.
      - 가격 레인: 판을 `_versions` 표의 브랜치에 쌓고(ADR-0183), 공개 대신 비교 추가(ADR-0275). 허브 부분 레인에
        `compare` 를 허용하고, 레인 계약의 `compare.content_columns` 가 내용 열의 정본이 된다(이 PR 의 상수는 지운다).
      - 조합: 결정 6 의 바뀐 키만 다시 조합해 `_versions` 에 비교 추가.
      - 독자: 건물 패널·세대 가격 패널의 가격 입력, dbt `stg_foundation__unit_official_price`, 운영자 투영 적재
        (`load-unit-official-price-projection`). 패널의 변경 집합은 두 핀 사이에 append 된 버전 행이다(ADR-0189 의
        일반화: 현재 행이 바뀐 키에는 반드시 append 된 행이 있다).
      - 단계 2 의 동등성 보고가 그 PR 의 병합 조건이다.
   4. 옛 두 표는 남긴다. 쓰기만 멈춘다. 지울지는 사장이 정한다.
6. **조합도 바뀐 것만 한다(후속 PR 의 설계).** 버전 표가 가진 마지막 조합 판을 P, 새 판을 V 라 하면 다시 조합할
   관리번호는 (a) 원천 버전 표에서 판이 [P, V] 인 행의 관리번호, (b) 전유부 사전(V)이 조합 표의 현재 행과 다른(동·호·
   PNU 가 바뀌거나 사라진) 관리번호, (c) 사전(V)에 새로 있고 V 시점 가격이 있는 관리번호다. 그 관리번호의 가격만
   `rows_as_of(원천, V)` 에서 읽어 조합하고, 조합 표의 그 관리번호 현재 행과만 비교한다. 다른 관리번호의 조합 결과는
   입력이 같으므로 같다. [P, V] 가 P 를 포함하는 것은 P 를 다시 실은 변경을 놓치지 않기 위해서다(상위집합). 버전 표가
   비어 있으면 전국 조합이다(첫 실행, 선언된 예외). 압축으로 스냅숏 id 만 바뀐 재실행은 (a)가 P 의 변경 행뿐이고 비교가
   아무것도 쓰지 않는다.

## Consequences

| 항목 | 지금 | 뒤 | 근거 |
|---|---:|---:|---|
| 조합 한 번에 쓰는 행 | 188,736,512 | 약 4.3만(0.023%); 같은 판 재조합은 0 | 서울 실측 비율 × 전국(계산) |
| 조합 표 행(10-28 의 내용) | 1,207,165,362 | 약 1.888억 + 판마다 수만 | 계산 |
| 원천 한 판에 쓰는 행 | 약 2.02억(7.0 GB) | 수만–수십만 | 추정(원천 비교 미측정) |
| 원천 표 행(3판) | 606,258,106(21.05 GB) | 약 2.02억 + 변경분(약 7 GB) | 추정 |

- 비교는 새 판 전체와 현재 행을 읽어 지문으로 셔플한다. ADR-0275 1단계처럼 원천 레인의 시간은 줄지 않을 수 있다
  (쓰기 바이트·저장량이 이득이다). 조합은 결정 6 으로 읽고 셔플하는 행 자체가 준다. 단계 2 실행이 판마다
  `appended_seconds` 를 남기므로 운영 규모의 시간을 그때 잰다.
- 이행 잡은 판마다 옛 사본과 재구성을 두 번 `exceptAll` 한다(원천 판당 2억 행). 무거운 일회성이므로 창 안에서 한
  표씩 돌린다(by-PNU 굽기와 겹치지 않게, 2026-10-18 `/data` 포화 교훈).
- 판 순서가 거꾸로인 적재는 거부된다. 옛 판을 고치려면 그 판 이후를 다시 만드는 별도 결정이 필요하다.
- 시험: `test_vintage_versions_spark`(세 판·재적재·정정공시 추가·사라졌다 돌아온 키·null 키·반복 줄에서 판마다
  재구성 = 전체 사본, 같은 판은 0 행, 오래된 판 거부, 삭제 표시 없는 표의 시점 읽기 거부),
  `test_silver_release_compare_iceberg`(토지 레인 동작 그대로).

## Rejected

- **옛 표를 그대로 두고 압축·중복 제거**: 같은 내용의 사본이 Iceberg 데이터 파일로 남는다. 압축은 파일을 합칠 뿐
  행을 줄이지 않는다.
- **옛 표 제자리 전환**(계약만 `current_row_latest` 로): 옛 전체 사본에는 삭제 표시가 없다. 판 V 에서 빠진 키의 앞 판
  행이 `rows_as_of(V)` 에 남는다. 그래서 새 표에 다시 쓴다.
- **SCD2 의 `valid_to` 닫기, `MERGE INTO`**: 쌓인 행을 고쳐 쓴다(ADR-0267·0275 와 같은 이유로 거절).
- **merge-on-read 삭제 파일**: 굽기의 스냅숏 스캔이 delete 파일을 거부한다(ADR-0180).
- **Iceberg 스냅숏을 판의 이력으로 쓰기**: 30일이면 만료된다(ADR-0237). 판은 업무 시간이다.
- **판 사이 차이만 쌓고 첫 판도 차이로**: 첫 판이 곧 전체다. 같은 것이다.

---

2026-10-29 각주: §2 의 판 기록(`foundation.edition` 스냅숏 요약)은 만료되므로 [ADR-0328](./0328-a-compared-silver-table-orders-versions-by-edition-then-commit-instant.md) 이 발행 커밋의 표 속성 `foundation.edition.<판>` 과 행의 최댓값을 더했다. `commit_stamp` 는 이제 모든 비교 레인에 판과 커밋 시각을 찍는다(토지 레인 포함).
