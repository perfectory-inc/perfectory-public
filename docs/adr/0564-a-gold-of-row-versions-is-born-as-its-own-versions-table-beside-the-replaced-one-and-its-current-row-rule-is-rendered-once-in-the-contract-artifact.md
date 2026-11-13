# ADR 0564: 판을 쌓는 Gold 는 옛 표 옆에 `_versions` 표로 태어나 판정받고 넘겨받으며, 현재 행 규칙은 계약 산출물이 한 번 렌더한다

- Status: Accepted
- Date: 2026-11-14
- Builds on: [ADR-0561](./0561-a-gold-that-merges-leaves-spark-by-becoming-a-table-of-appended-row-versions-the-publisher-commits.md)
  (수락된 길: 바뀐 행을 쌓는 Gold 와 발행기 커밋 — 이 문서는 그 3 번의 전제를 채우고 2 번을 "옮긴다"로 정한다),
  [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)
  (판정으로만 옮긴다), [ADR-0560](./0560-the-publisher-commits-duckdb-staged-parquet-with-iceberg-rust-fast-append-and-only-tables-whose-commit-is-an-append-move-off-spark.md)
  (추가 커밋 어댑터), [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)·[ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md)
  (`LatestPerKey`, 독자는 한 곳에서 읽는다), [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md)
  (`_versions` 표를 옛 표 옆에 두고 독자를 옮긴 선례), [ADR-0338](./0338-a-reader-of-an-old-snapshot-pins-it-with-its-own-tag-and-the-rest-expire-in-30-days.md)(독자의 핀),
  [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(부분 처리)
- Amends(옮겨진 표에 한해): [ADR-0344](./0344-gold-merges-write-only-the-changed-rows-as-merge-on-read-deletes-that-compaction-folds.md) 의 merge-on-read 커밋,
  [ADR-0347](./0347-unit-ids-and-link-evidence-are-named-by-content-and-every-gold-commit-records-its-changed-keys.md) §4 의 `__changes`
  (쌓는 표에는 없다, 5 번), ADR-0561 3 번의 "`lakehouse_snapshot_scan` 이 규칙을 적용한다"(규칙은 계약 타입에 두고 스캔은 행만 낸다, 4 번)와
  "되돌리기는 `main` 을 부모로"(앞으로 붙여 되돌린다, 6 번), 6 번의 접기 문턱(값을 정한다, 7 번), 7 번의 "한 배포"(새 표라 필요 없다, 1 번),
  ADR-0560 의 어댑터가 빈 추가를 거부하는 것(3 번).
- 묶는 범위: Decision 1~8 은 첫 표(실거래 Gold)에 대해 묶는다. 9 번의 뒤 차례는 사장이 수락한 방향의 순서이고, 각 표의 PR 이 앞 표가 잰
  값(죽은 판, 굽기 시간, 독자를 옮긴 비용)으로 이 문서의 숫자를 확인하거나 고친다.
- 결정한 사람: 사장(2026-11-14, ADR-0561 수락과 "대기업 수준으로"). 이 문서는 그 수락의 설계다.

## 사장에게: 세 줄

- `MERGE` 로 쓰던 Gold 는 **새 표 `gold.<이름>_versions`** 로 태어난다. 옛 표는 넘겨받기가 끝날 때까지 그대로 서빙하고, 새 표의 "현재 행"이
  옛 표와 한 행도 다르지 않다는 판정이 나온 뒤에 레인이 굽는 표를 계약의 한 줄(`gold_table`)로 옮긴다. 옮긴 직후 그 레인을 한 번 전량으로 다시 굽는다.
- "키마다 가장 새 행"이라는 규칙은 **계약에 한 번** 적고, 산출물이 SQL 한 벌을 렌더한다. Spark·DuckDB·Trino·dbt·검사·Rust 독자가 그것을 받아 쓴다.
  독자가 손으로 고르는 코드는 가드가 거부한다.
- 쌓는 표는 지워지는 자리가 없으므로 자란다. 실거래 Gold 로 계산하면 한 해 뒤 죽은 판이 많아야 27%(계산한 위쪽 한계, 재지 않음)이고,
  파일을 합치는 일은 지금의 유지보수 작업이 그대로 한다. 죽은 판을 지우는 다시 쓰기는 30% 문턱을 넘는 것을 잰 뒤에만 만든다.

## Context

**수락 뒤에 실물이 달라져 있다(2026-11-14 실측, ai-server 읽기 전용).**

| 무엇 | ADR-0561 이 적은 것 | 실물 |
|---|---|---|
| `gold.parcel_real_transaction` | 아직 없다 | 2026-11-12 14:00:40Z 에 태어났다: `overwrite` 12,094,440 행·14 파일·647.5 MB. 11-13 01:44Z 유지보수의 합치기(`replace`, 2 파일). `served-parcel-real-transaction-by-pnu-20261113T112023Z-…` 태그 — **서빙 중이다** |
| `gold.real_transaction_region_stats` | 같은 파일이 덮어쓴다 | 3,204,170 행, 스냅샷 1 개 |
| `gold.parcel_nearby` | 아직 없다 | 없다(시설 Silver 여섯에 스냅샷이 없다) |
| 실거래 Gold 의 바뀐 키 실행 | 재지 않음 | 여전히 없다. 표의 스냅샷은 첫 빌드와 합치기 둘뿐이다 |

그래서 "태어나지 않은 표라 옮길 데이터도 다시 구울 문서도 없다"는 전제는 주변 Gold 에만 남고, 실거래 Gold 는 **서빙 중인 `Replaced` 표를
넘겨받는 첫 사례**가 된다. 나머지 `MERGE` Gold 16 개와 덮어쓰는 Gold 여섯이 따라갈 길을 이 표가 연다.

**먼저 있는 것을 찾았다**(스캔: 저장소 전체의 `*.rs`·`*.py`·`*.sql`·`*.sh`·`*.json`·`*.yml` 에서 `current_rows`·`current_row_latest`·
`newest_first`·`removal_column`·`_versions`·`gold_table`·`fast_forward`·`commit.retry`; `iceberg-0.11.0` crate 소스의 `transaction/`).

| 필요 | 이미 있는 것 | 판단 |
|---|---|---|
| 요약 속성을 머리에서 읽기 | 머리는 자주 유지보수의 합치기(`replace`)다 — 위 표의 실거래 Gold 가 지금 그렇다. 그래서 지금 코드는 머리에서 조상으로 걸어 올라가 속성이 있는 가장 새 커밋을 찾는다(`gold_incremental.build_snapshot`, `foundation-gold-build` 태그의 `walks_main_back_to_it`) | 쌓는 표의 순서 값·핀·현재 행 수도 그렇게 읽는다(3 번). 머리만 읽으면 합치기 뒤 순서 값이 1 로 돌아간다 |
| 행 판 선언 | `LakehouseRowVersions::LatestPerKey { key, newest_first, removal_column, keep_ties }`, 삭제 표시 값 `removed`(`REMOVED_MARKER`, `platform_contracts.REMOVED`), 칸 이름 `row_state`(Silver 30 여 표) | 그대로 쓴다. 새 어휘 없음 |
| 현재 행 규칙의 렌더 | **넷이 따로 있다**: Spark `platform_contracts.current_rows`, dbt `render_current_rows.py`, Dagster `publish_checks.current_rows_sql`(Trino), Rust `LakehouseCurrentRowLatest::trino_current_rows_sql`. 그리고 손으로 고르는 Rust 독자 하나(`industrial_complex_gold_profile_export/document_facts.rs::newest_per_complex`) | 하나로 모은다(4 번). 이것이 ADR-0267 이 막으려던 모양이다 |
| 옛 표 옆의 새 표와 독자 이전 | ADR-0317 의 `silver.unit_official_price_versions`(이전 계약 `vintage-versions-migration.contract.json`) | 같은 모양을 Gold 에 쓴다 |
| 레인이 읽는 Gold 를 가리키는 한 줄 | `config/r2-connections.contract.json` `by_pnu_lanes.<lane>.gold_table` 은 **호스트 없는 레인만** 읽는다(`unit-price`). 종류 레인은 이름에서 표를 만든다: `by_pnu_kinds.Kind.gold_table`(`"gold." + lane`), Rust `by_pnu_kind::derive`(줄에서 계약을 만든다) → `sections::gold_table`. 패널은 상수다 | 그 필드를 종류 줄에서도 읽게 넓힌다(6 번). 새 스위치가 아니라 있는 필드의 독자를 늘린다. 이름에서 표를 만드는 곳을 전부 그 해석 하나로 모은다 |
| 표가 바뀐 레인의 재기준 | 검증 재기준(`verify-parcel-by-pnu-serving-rebase`)은 parcel 전용이고 객체 서빙만 읽으며 운영자 실행이다(`by_pnu_serving_rebase.rs`, `by-pnu-serving-bake.sh`: "the lane re-bases with a full bake"). 묶음 레인의 재기준은 **전량 굽기**(새 세대)다 | 넘겨받은 뒤 첫 굽기는 전량 굽기다(6 번). 묶음 레인용 검증 재기준을 새로 만들지 않는다 |
| 두 엔진 출력의 대조 | `engine_equality.py`(ADR-0559 §3) | 판정에 그대로 쓴다 |
| 추가 커밋 | `lakehouse_data_commit`(#1366), DuckDB 준비(`duckdb_parquet_stage`), 고정 스냅샷 읽기(#1378·#1380) | 그 위에 얹는다 |
| "머리가 아직 S 일 때만 붙인다" | 없다. iceberg-rust 0.11.0 의 `Transaction::commit` 은 **첫 시도에도** 표를 다시 읽어 그 위에 동작을 다시 적용한다(`transaction/mod.rs` `do_commit`). 그 다시 읽은 머리에 대한 `RefSnapshotIdMatch` 는 보내지만, 우리가 차분한 S 에 대한 것이 아니다 — 재시도 횟수를 0 으로 해도 S 뒤에 낀 커밋 위에 붙는다. 요구 조건을 더하는 동작은 만들 수 없다(`TransactionAction` 이 `pub(crate)`) | 3 번의 얇은 카탈로그 덧씌우개 |
| 빈 추가(행 없이 요약 속성만) | 라이브러리는 허용한다(`snapshot.rs`: 파일도 속성도 없을 때만 거부). 우리 어댑터가 거부한다(`lakehouse_data_commit.rs` 의 `DataAppend::files` 주석: 증명될 때까지 Spark 의 `record` 쓰기로) | 첫 구현 PR 이 CI 의 실제 Iceberg 에서 증명하고 연다 |
| 나쁜 커밋 되돌리기 | `main` 을 부모로 옮기는 동작은 없다(`iceberg_rest_catalog.rs`: `publish_branch`·`remove_stale_branch`·`create_tag`·`remove_tag`) | 만들지 않는다. 쌓는 표는 앞으로 붙여 되돌린다(6 번) |

**다른 회사와 프로젝트.** 바뀐 행만 덧붙이고 읽을 때 키별 최신을 고르는 표는 Hudi 의 merge-on-read(레코드 키 + ordering field, 합치기는
비동기 compaction), Delta Live Tables 의 `APPLY CHANGES … SEQUENCE BY`, Snowflake 의 스트림 위 `QUALIFY ROW_NUMBER()` 뷰, dbt snapshot 의 모양이다.
공통점은 셋이다: 순서 칸은 쓰는 쪽이 매기는 단조 값이다, 현재 뷰의 정의는 한 곳이다, 접기는 쓰기 길과 떨어진 유지보수다. 새 표를 옛 표 옆에
세우고 대조한 뒤 가리키는 줄을 옮기는 것은 Netflix 의 Iceberg WAP 와 Stripe·GitHub 의 온라인 표 이전(그림자 표 → 검증 → cut-over)의 모양이다.

## Decision

1. **쌓는 Gold 는 새 표로 태어난다: `gold.<t>_versions`.** 옛 `Replaced` 표의 계약·데이터·생산자는 넘겨받기가 끝날 때까지 손대지 않는다.
   - 이유 셋. ⓐ ADR-0561 7 번의 "계약 변경과 생산자 이동은 한 배포" 제약이 사라진다 — 옛 계약이 그대로라 옛 생산자가 멈추지 않는다.
     ⓑ 판정의 기준이 서빙 중인 운영 행 그 자체가 된다(같은 입력 핀에서 두 표를 나란히 본다). ⓒ 되돌리기가 데이터를 건드리지 않는다.
   - 이름의 `_versions` 는 그냥 조회하면 옛 판과 삭제 표시가 보인다는 것을 표 이름으로 알린다(ADR-0561 Consequences).
   - 아직 없는 표(`gold.parcel_nearby`)도 같은 이름 규칙으로 태어난다. 옛 표는 만들지 않는다.
   - 옛 표는 넘겨받은 뒤 쓰기를 멈추고 남긴다(쌓기만 한다). 지우는 것은 30 일 뒤 런북의 운영 절차이고 이 문서가 정하지 않는다.
2. **표 모양.**

   | 항목 | 값 |
   |---|---|
   | 칸 | 옛 표의 칸 전부(이름·형·필수 그대로) + `version_seq long`(필수) + `row_state string`(선택) |
   | `row_versions` | `LatestPerKey { key: 옛 표의 키, newest_first: [version_seq], removal_column: row_state, keep_ties: false }` |
   | `version_seq` | 그 표의 커밋 순번. 발행기가 매긴다: 머리 스냅샷 요약의 `foundation.version-seq` + 1, 첫 커밋은 1. 한 커밋의 모든 행이 같은 값을 갖는다. `published_at_utc`(문자열)·스냅샷 id(시간순이 아니다)에 기대지 않는다 |
   | 한 커밋 안 | 키마다 행은 하나다(준비 단계가 거부). 그래서 같은 순서 값의 두 행은 재실행이 두 번 쌓인 경우뿐이고 규칙이 거부한다 |
   | 삭제 표시 행 | 사라진 키마다 한 행: 그 키의 직전 현재 행의 내용 칸과 `row_digest` 를 **그대로** 싣고 `row_state = 'removed'`, 새 `version_seq`, 이 커밋의 `source_snapshot_id`·`published_at_utc`. 필수 칸을 선택으로 풀지 않는다(Silver 의 `silver_release_compare` 가 삭제 표시를 만드는 방식 그대로). 무엇이 지워졌는지가 표에 남는다 |
   | 파티션 | PNU 가 키의 첫 칸인 표는 `truncate(2, pnu)`(시도 17). 그 밖의 표는 지금의 identity 칸(`real_transaction_region_stats`: `dataset_slug`). `source_snapshot_id` 로 나누지 않는다 — 커밋마다 새 파티션이면 합치기가 판을 넘어 파일을 합치지 못한다(ADR-0561 이 남긴 물음) |
   | 정렬 | 키 칸(지금과 같다). 준비 파일은 시도마다 하나, 키로 정렬 |
   | 형식 | Iceberg v2. 삭제 파일은 없다(쓰는 길이 추가뿐) |

   - `truncate` 파티션 값은 어댑터가 계산한다(문자열의 앞 N 글자; 파일의 최소·최대가 같은 값으로 잘려야 한다 — 아니면 거부). ADR-0560 2 번이
     미뤄 둔 것이고, `rt_molit` Silver 의 `truncate(4, deal_ym)` 도 같은 코드로 풀린다(W75e 와 겹치지 않게: 먼저 병합되는 쪽의 것을 쓴다).
   - 시도로 나누면 by-PNU 샤드(PNU 접두)가 다른 시도의 추가 파일을 매니페스트 경계로 건너뛴다. 준비도 시도 단위다(ADR-0559 7 번, ADR-0440).
3. **쓰는 길 — 바뀐 키만 붙인다.** 표마다 발행기 명령 하나가 계획·계산·차분·검사·커밋을 한다. Spark 세션은 없다.
   1. *계획.* 머리 스냅샷 S 를 고정하고, `main` 을 S 에서 조상으로 걸어 **`foundation.version-seq` 를 실은 가장 새 커밋 B** 를 찾는다
      (사이에는 합치기만 있다 — 지금 `build_snapshot` 이 핀을 찾는 방식). B 의 요약이 직전 상태다: 입력 핀
      (`foundation.source-iceberg-snapshots`), 코드 판(`foundation.build-source-sha`), `foundation.version-seq`,
      `foundation.current-records`, 묶음 신원. 행이 있는 표에서 B 를 못 찾으면 **거부한다** — 순서 값을 1 로 되돌리지 않는다
      (만료가 B 를 지우지 못하게 `foundation-gold-build` 태그가 B 에 있다, 8).
      입력의 머리가 B 의 핀과 같고 코드 판이 같으면 태그를 맞춘 뒤(8) "할 일 없음"으로 끝난다.
   2. *계산.* 발행기 안의 DuckDB 가 입력을 고정한 스냅샷에서 읽는다. 처리 범위는 지금 생산자가 선언한 것 그대로다(`changed-keys`:
      입력 변경이 닿는 키만 다시 계산한다 — 실거래는 변경이 닿는 법정동, ADR-0350). 첫 빌드와 코드 판 변경은 시도 샤드로 전부 계산하고
      그것은 계약의 `full` 예외로 잰 비용과 함께 적는다.
   3. *차분.* 다시 계산한 키에 대해 S 의 현재 행(4 번의 규칙)과 `row_digest` 로 견준다. 다르거나 새로 생긴 키는 새 행으로, 다시 계산한
      범위 안에서 사라진 키는 삭제 표시 행으로 준비한다. 같은 키는 쓰지 않는다. **붙는 것은 바뀐 키뿐이다.**
   4. *검사(커밋 전, 전부 DuckDB).* 준비 파일만으로 되는 것: 생산자의 품질 관문(`lane.validate` 의 이식), 한 커밋에 키마다 한 행.
      S 의 현재 행이 있어야 하는 것: 변경 비율 상한(`max_changed_key_fraction` 을 넘으면 거부 — `MERGE` 때처럼 전체로 넘어가는 것이
      아니라 멈추고 알린다; 넘는 변경을 싣는 것은 사유를 적은 운영자 실행이다), 대조 표본(입력이 안 바뀐 키 표본을 다시 계산해
      현재 행과 같아야 한다), 행 하한, 섹션 포함(ADR-0272), 붙인 뒤의 현재 행 수.
   5. *커밋.* 준비 파일 전부를 **한 번의 추가 커밋**으로 붙인다. 요약 속성은 지금 Gold 커밋이 싣는 이름 그대로(`foundation.content-fingerprint`·
      `foundation.source-iceberg-snapshots`·`foundation.keys-changed`·`foundation.build-source-sha`)에 둘을 더한다: `foundation.version-seq`,
      `foundation.current-records`. 묶음 신원(`foundation.ingest-batch-objects`)은 `<입력 핀과 코드 판의 다이제스트>` 하나다 — 어댑터의
      재실행 가드가 응답을 잃은 커밋 뒤의 재시도를 막는다.
   6. *머리가 아직 S 일 때만.* 어댑터가 `expected_head` 를 받는다. 커밋에 넘기는 카탈로그를 얇게 덧씌워, 라이브러리가 커밋 직전에 표를 다시
      읽을 때 머리가 S 가 아니면 재시도하지 않는 오류로 끝낸다. 머리가 S 이면 라이브러리가 만드는 요구 조건(`RefSnapshotIdMatch(main, S)`)을
      카탈로그가 원자적으로 지킨다. 그 사이 머리가 옮겨지면(유지보수의 합치기) 그 run 은 실패하고 다음 run 이 다시 차분한다. 표마다
      쓰는 작업은 하나다(작업 계약). **응답을 잃은 커밋**: 머리가 S 가 아닌데 새 머리의 묶음 신원이 이 run 의 것이면 그 커밋은 들어간
      것이다 — 성공으로 읽고 8 로 간다.
   7. *행이 없는 커밋.* 입력 핀은 옮겨졌는데 바뀐 키가 없으면 요약 속성만 실은 빈 추가를 한다(지금의 빈 `MERGE` 와 같다: 다음 계획이
      새 핀에서 시작한다). 어댑터의 빈 추가 거부는 첫 구현 PR 이 CI 의 실제 Iceberg 에서 되읽어 증명한 뒤 푼다.
   8. *핀.* 커밋 뒤 입력 스냅샷마다 `foundation-gold-input-<표>` 태그를, 자기 커밋에 `foundation-gold-build` 태그를 옮긴다
      (`IcebergRestCatalog::create_tag`, 이름은 지금 것 그대로 — 만료 규칙 ADR-0338 이 그대로 듣는다). 태그는 커밋과 한 트랜잭션이
      아니므로 **모든 run 이 계획 뒤에 태그를 B 와 그 핀에 맞춘다**: 커밋과 태그 사이에 죽은 run 을 다음 run 이, "할 일 없음"이어도, 고친다.
   9. *순서 값은 다시 쓰이지 않는다.* 새 값은 언제나 B 의 값 + 1 이고 B 는 `main` 의 조상에서만 찾는다. 커밋을 걷어 내는 동작이 없으므로
      (6 번: 앞으로 붙여 되돌린다) 한 번 쓰인 값이 다른 행에 다시 붙는 길이 없다.
4. **현재 행은 한 번 정의하고 모든 독자에게 렌더한다.**
   - 정의는 계약(`LatestPerKey`)이다. Rust 계약 산출물(`industrial_complex_lakehouse_contracts.json`)이 `LatestPerKey` 표마다
     **`current_rows_sql` 한 벌**을 싣는다: `{source}`·`{columns}`·`{filter}` 자리만 빈 표준 SQL(`rank() OVER (PARTITION BY 키 ORDER BY
     newest_first DESC NULLS LAST)` = 1, 삭제 표시 제외). Spark SQL·DuckDB·Trino 가 같은 글자를 돈다. 렌더하는 코드는
     `LakehouseCurrentRowLatest::current_rows_sql` 하나이고 `trino_current_rows_sql` 은 그것의 이름이 된다.
   - 렌더는 그래서 셋이 남는다: SQL 글자(산출물), 스캔한 행에 대한 Rust 함수, Spark DataFrame 함수. 정의는 하나(계약)이고, 셋은 한
     고정물로 묶인다. **동률**: SQL 의 `rank() = 1` 은 동률을 모두 남긴다(SQL 은 거부하지 못한다). Rust 함수와 Spark 함수는 거부한다.
     그래서 SQL 로 읽는 쓰는 길(차분·판정)은 현재 행의 `count(*)` 와 `count(DISTINCT 키)` 가 같은지를 먼저 본다.
   - 받아 쓰는 쪽: 발행기의 DuckDB(차분·검사·판정), dbt 매크로 렌더(`render_current_rows.py`), Dagster 검사(`publish_checks.current_rows_sql`),
     운영자·에이전트의 조회(런북에 그 SQL 을 내는 명령 한 줄). 이 셋의 자체 렌더는 산출물의 글자를 읽는 것으로 바뀐다. Spark 의
     `platform_contracts.current_rows` 는 DataFrame 판으로 남되(유지보수와 아직 안 옮긴 생산자가 쓴다), 같은 합성 표에서 SQL 판과 같은 행을
     고르는지 시험이 묶는다.
   - 스캔한 행에 적용하는 Rust 판은 `LakehouseTableContract::current_rows(rows)` 하나다(`newest_edition_rows` 의 짝). 같은 순서 값의 두 행은
     거부, 가장 새 행이 삭제 표시면 그 키는 없다. `lakehouse_snapshot_scan` 은 행을 내기만 하고 규칙을 모른다 — 규칙이 스캔 안에 숨으면
     "모든 판을 이력으로 읽는" 독자가 막힌다. by-PNU 독자 셋의 "한 키에 한 행" 거부는 이 함수를 거친 뒤의 행에 남는다.
     `document_facts.rs::newest_per_complex` 는 이 함수로 바뀐다.
   - SQL 판과 Rust 판은 한 고정물(판 여럿·동률·삭제 표시·되살아난 키)을 함께 통과한다.
   - **가드.** #809 의 독자 가드(지금 `jobs/*.py` 모듈 단위)를 표 단위로 좁히고 Rust 를 덮는다: `LatestPerKey` 표를 스캔하는 Rust 코드가
     `current_rows` 를 거치지 않으면, 그 표를 이름 대는 SQL 이 산출물의 글자를 거치지 않으면 거부한다. 막는 사건: 2026-10-16
     `silver.industrial_complexes` 가 현재 행 규칙 없이 세 벌(4,326 행)로 읽혀 경계 발행이 멈췄다.
   - **메모리.** by-PNU 샤드는 접두 하나의 모든 판을 읽은 뒤 고른다. 샤드의 행 상한(`MAX_ROWS_PER_RUN`)은 현재 행이 아니라 **읽은 행**에
     건다 — 죽은 판이 늘면 샤드가 쪼개질 뿐 메모리 상한을 넘지 않는다.
5. **행 수·지문·변경 집합의 뜻.**
   - `foundation.content-fingerprint` 는 **현재 행**의 지문이다. 키 집합이 바뀌는 커밋에서도 더하고 빼서 갱신할 수 있는 형태
     (`gold_fingerprint` 의 `rows:N;digest_sum:…`)이므로 발행기가 붙인 행과 그것이 가린 행에서 계산하고, 넘겨받기 판정과 주 1 회 검사가
     현재 행 전체에서 다시 계산해 견준다.
   - 현재 행 수는 `foundation.current-records` 다. `total-records` 를 키 수로 읽던 곳(행 하한·변경 비율, 굽기의 `live_rows`, 샤드 계획의
     상한, 재고·형상 원장)은 `LatestPerKey` 표에서 이 속성을 읽는다. 옮기는 PR 이 그 독자를 스캔 범위와 함께 전부 적는다.
   - **`__changes` 는 쌓는 표에 없다.** 두 상태 사이에 바뀐 키는 `version_seq` 가 그 사이인 행의 키다. 굽기의 변경 집합은
     "서빙이 반영한 스냅샷의 `foundation.version-seq` = V" 에서 `version_seq > V` 인 행을 읽는다: 현재 행이 삭제 표시면 지울 문서, 아니면
     올릴 문서다. 스냅샷 계보(추가만 이어졌는지)에 기대지 않으므로 합치기가 사이에 끼어도 같다 — FLOOR 이력이 합치기를 못 받는 것과
     같은 함정을 피한다. V 는 서빙이 반영한 스냅샷에서 조상으로 걸어 찾는다(3-1 과 같은 걸음). 추가 파일은 판이 하나라 파일 통계로
     건너뛴다. 합친 파일은 키로 정렬돼 판이 섞이므로 건너뛰지 못하고 `version_seq` 칸 하나를 다 읽는다(실거래: 1,200 만 행의 정수 칸 —
     첫 표의 PR 이 잰다).
   - 그래서 `by_pnu_panel_delta.py`(Spark)는 쌓는 표의 레인에서 돌지 않는다. 발행기 명령이 변경 집합을 낸다(같은 산출물:
     `change-set.json`·`upserts.txt`·`deletes.txt`, 같은 거부 종류).
6. **넘겨받기와 되돌리기 — 표마다.**
   1. **스위치는 있는 필드다.** 종류 줄(`by_pnu_lanes.<lane>`)이 두 가지를 말한다: `versions_table`(쌓는 표가 있고 쓰인다 —
      `by_pnu_kind::derive` 가 옛 표의 계약 옆에 `_versions` 계약을 만든다)과 `gold_table`(레인이 굽는 표; 없으면 지금처럼
      `gold.<lane>`). 이름에서 표를 만들던 곳은 전부 한 해석(`by_pnu_kinds.Kind.gold_table`, Rust 는 `derive` 가 만든 `lane.gold`)을 거친다.
      첫 구현 PR 이 그 독자를 스캔 범위와 함께 전부 옮긴다 — 2026-11-14 에 찾은 것: `by_pnu_kinds.py`, `by_pnu_panel_delta.GOLD_TABLES`,
      `gold_rebuild.load_contract`(종류 표를 줄에서 만든다)와 `gold-panel-rebuild.sh`, `lane_follow_gate.py`, `edition_lockstep_gate.py`,
      `by-pnu-lane-cutover.sh`, `by-pnu-pack-operator.sh`, `by_pnu_serving_manifest_publish.rs::gold_table`. 이 해석이 놓이기 전에는
      어떤 표도 넘겨받지 않는다.
   2. **그림자 기간.** 새 표의 생산 명령과 작업을 싣는다. 옛 생산자와 옛 굽기는 그대로 돈다. 새 생산자는 **옛 표의 가장 새 빌드 커밋이
      기록한 입력 핀**을 자기 입력 핀으로 쓴다(입력의 머리가 아니라). 그래서 두 표는 언제나 같은 핀의 두 답이고, 새 표는 옛 표를 앞서지 않는다.
   3. **판정.** `engine_equality` 의 공통 SQL 을 발행기 명령이 돈다: 옛 표의 그 빌드 커밋의 행 ↔ 새 표의 같은 핀 커밋의 현재 행(4 번의 SQL).
      견주는 칸은 키와 내용 칸 전부와 `row_digest`, 그리고 지문이다. 판과 계보의 칸(`version_seq`·`row_state`·`source_snapshot_id`·
      `published_at_utc`)은 견주지 않는다 — 옛 표에서도 `row_digest` 밖의 칸이다. `equal` 이 아니면 넘겨받지 않는다. 예외는 ADR-0559 3 번의
      ⓐⓑ 그대로이고 사람이 정한다.
      - 첫 판정: 첫 빌드(전량)에서.
      - 둘째 판정(증분 길): 옛 생산자가 바뀐 키 실행을 커밋하면 그 핀에서 다시 견준다. 옛 생산자가 그림자 기간에 바뀐 키 실행을 한 번도
        커밋하지 못하면(실거래 Gold 는 2026-11-14 까지 한 번도 못 했다) 견줄 기준이 없다. 그때는 새 엔진이 스스로를 견준다: 같은 핀에서
        전량을 다시 계산한 결과와, 증분으로 쌓아 온 표의 현재 행이 `equal` 이어야 한다(시험 네임스페이스에 쓰고 지운다). 어느 판정으로
        넘겨받았는지를 PR 본문에 적는다.
   4. **넘겨받기.** 종류 줄의 `gold_table` 을 쌓는 표로 바꾸는 PR. 같은 PR 이 새 생산자의 핀을 입력의 머리로 풀고, 옛 생산 작업을 작업
      정의에서 빼고, 그 Spark 파일과 장부 줄을 `moved` 로 바꾸고(PR·대조한 행 수·판정의 지문), 계약 산출물·ODCS·파이프라인 그래프·레인
      색인을 다시 렌더한다. 배포가 곧 넘겨받기다: 사장의 단계도 시간창도 없다.
   5. **넘겨받은 뒤 첫 굽기는 전량 굽기다.** 굽기는 서빙 매니페스트가 반영한 표가 레인의 표와 다르면 변경 집합을 구하지 않고 새 세대를
      전량으로 굽는다(묶음 레인이 재기준하는 지금의 방식; 사유는 매니페스트에 남는다). 이것은 ADR-0314 의 `full` 예외이고 표마다 한 번이다:
      실거래는 문서 1,209 만 개, 소유 레인의 실측(3,940 만 문서 636 초)으로 계산해 약 200 초·상한 1,500 초(작업 정의의 첫 실행 추정).
      판정이 `equal` 이었으므로 새 세대의 문서는 옛 세대와 바이트가 같아야 한다 — 굽기가 세대의 내용 다이제스트를 옛 세대와 견주어
      요약에 적고, 다르면 발행하지 않는다. 발행하면 서빙 핀(`served-` 태그)이 새 표에 생기고 옛 표의 서빙 태그를 푼다. 그 뒤부터는
      5 번의 `version_seq` 변경 집합이다. 매니페스트 발행의 "표가 같아야 한다" 거부는 전량 세대의 표 바뀜을 받도록 고친다.
   6. **되돌리기 ⓐ — 넘겨받기 자체.** 4 의 PR 을 되돌린다. 옛 표는 지워지지 않았지만 그 사이 **멈춰 있었다.** 그래서 순서가 있다:
      옛 생산자가 먼저 밀린 핀을 따라잡아 커밋하고, 그 뒤의 굽기가(표가 다시 바뀌었으므로) 전량으로 굽는다. 굽기는 레인의 표가 기록한 입력
      핀이 서빙 세대가 구워진 핀보다 옛것이면 굽지 않는다 — 따라잡기 전의 옛 표로 서빙이 뒤로 가지 않는다. 한계를 숨기지 않는다:
      따라잡기는 Spark 의 `MERGE` 길이고, 실거래에서 그 길은 죽던 길이다. 되돌리기 ⓐ 는 옛 표의 쓰기를 멈춘 지 30 일(스냅샷 보존)
      안에서만, 그리고 옛 생산자가 돌 수 있는 동안만 성립한다. 그 밖의 고침은 ⓑ 다.
   7. **되돌리기 ⓑ — 나쁜 커밋은 앞으로 붙여 되돌린다.** `main` 을 부모로 옮기지 않는다(같은 순서 값이 다시 쓰여 굽기의 변경 집합이
      고친 행을 못 보고, 합치기가 위에 얹히면 옮길 수도 없다). 발행기 명령이 순서 값 N 의 커밋이 붙인 키마다 **N 직전의 현재 행**을 새
      순서 값으로 다시 붙인다(직전에 없던 키는 삭제 표시). 표는 N 이전의 현재 행으로 돌아가고, 굽기는 그 키들을 평소의 변경 집합으로
      본다. 입력 핀은 N 직전의 것을 싣는다 — 다음 생산 run 이 고쳐진 코드로 그 핀에서 다시 차분한다. 나쁜 판은 표에 남는다(쌓기만 한다).
   8. **가지(WAP)는 쓰지 않는다.** iceberg-rust 0.11.0 의 추가는 `main` 에만 간다(ADR-0560). 한 run 의 한 커밋이라 "전부 보이거나 하나도
      안 보인다"는 가지 없이 성립하고, 감사(3-4)는 커밋 전에 준비 파일과 S 위에서 끝난다. 새 표 전체가 옛 표 옆의 그림자이므로
      넘겨받기 전의 격리는 표 단위로 이미 있다.
   9. **레인이 아닌 표.** `gold.real_transaction_region_stats` 는 by-PNU 레인이 아니고 독자가 그 생산자 하나다(스캔: 저장소 전체의
      `*.rs`·`*.py`·`*.sh`·`*.sql`·`*.ts` — 계약 정의와 `real_transaction_silver_to_gold.py` 뿐). 쌓는 표가 생산자와 함께 태어나고,
      새 생산자가 그것을 읽는다. 스위치가 필요 없다. 판정은 같은 방식(옛 표의 행 ↔ 새 표의 현재 행)이다.
7. **자라는 양과 유지보수.**
   - *자람(계산, 재지 않음).* 실거래 Gold 는 12,094,440 행·647.5 MB(행당 약 54 바이트, 압축 뒤). 하루 실행이 행의 0.1% 를 바꾼다고
     **가정하면**(실측 없음: 바뀐 키 실행이 운영에서 커밋된 적이 없다) 하루 약 1.2 만 행·0.65 MB, 작은 파일은 덜 압축되므로 2 배로 잡아
     1.3 MB 다. 한 해 약 441 만 행·0.24~0.47 GB 가 붙는다. 붙은 행이 전부 있던 키의 새 판이면 한 해 뒤 죽은 판 비율은
     441 / (1,209 + 441) = 27% 가 위쪽 한계이고, 새 키가 섞인 만큼 낮다. 첫 달의 실측이 이 줄을 고친다. 옛 표를 남기는 30 일 동안은
     두 표의 바이트를 함께 낸다(실거래: 약 0.65 GB, R2 표준 저장 요금 GB·월 0.015 달러로 월 0.01 달러).
   - *합치기.* 지금의 `lakehouse_maintenance`(Spark 의 남은 역할, ADR-0559 1 번)가 그대로 한다: 작은 파일 10 개·목표의 1/4 문턱,
     파티션(시도) 안에서 판을 넘어 합친다. 합치기는 행을 지우지 않으므로 규칙과 `version_seq` 변경 집합이 그대로다. 삭제 파일이 없으므로
     `fold_deletes` 는 이 표들에서 할 일이 없다. 하루 17 파일(시도마다 하나)이면 약 열흘에 한 번 문턱을 넘는다.
   - *만료.* 스냅샷 보존(30 일, 월말·연말 태그)과 독자의 핀(`served-`·`baking-`·`foundation-gold-input-`·`foundation-gold-build`)은
     그대로 듣는다. 쌓는 표에서 만료는 합치기가 대체한 옛 파일만 돌려준다 — 옛 판은 현재 스냅샷의 행이라 남는다.
   - *접기(죽은 판을 지우는 다시 쓰기)는 만들지 않는다.* 지우는 쓰기는 허락된 쓰기 의도가 없다(ADR-0286). 다시 볼 문턱: 표의 죽은 판
     비율((매니페스트 행 − `foundation.current-records`) / 매니페스트 행)이 **30%** 를 넘거나, 샤드당 굽기 시간이 넘겨받기 전 기준의
     **1.5 배**를 넘는 것이 잰 값으로 보일 때. 기준은 넘겨받는 PR 이 적는 값이다(옛 표에서 잰 그 레인의 샤드당 굽기 시간). 30% 는
     위의 한 해 위쪽 한계에 맞춘 첫 값이고 첫 달의 실측으로 고친다. 유지보수 작업이 표마다 그 비율을 지금의 지표 파일
     (`lakehouse-maintenance.prom`)에 낸다.
   - *커밋과 유지보수가 겹칠 때.* 합치기가 생산 run 의 계획과 커밋 사이에 끼면 3-6 이 그 run 을 실패시키고 다음 run 이 다시 한다.
     유지보수가 진다는 보장은 Iceberg 의 낙관적 커밋 그대로다. 겹침은 첫 표의 PR 이 시험 표에서 잰다(ADR-0560 6 번이 남긴 것).
   - *운영 카탈로그의 관리형 만료·합치기는 켜지 않는다*(사장 지시; ADR-0560 의 시험 표 하나는 그 문서의 기한대로).
8. **지금 실패 중인 유지보수는 이 설계를 막지 않는다.** 2026-11-13 01:36Z run: 표 117 개 가운데 115 개 완료, 둘 거부
   (`gold.parcel_land_rights`·`gold.parcel_transfer_history`: 서빙 상태 파일이 이름 대는 스냅샷에 태그가 없어 만료를 거부, ADR-0338 의
   설계된 거부), 그리고 로그의 Spark `NullPointerException`(`BlockManagerId.executorId()`). 같은 run 이 `gold.parcel_real_transaction` 은
   합쳤다(01:44Z `replace`). 거부는 표 단위이고 다른 표의 합치기·만료를 막지 않는다. 다만 이 설계가 유지보수에 **기대는** 것이 둘 있다:
   ⓐ 쌓는 표의 합치기(안 돌면 샤드가 여는 파일이 는다 — 느려지지만 틀리지 않는다), ⓑ 서빙 핀이 태그라는 것(넘겨받기 6-5 가 새 표에
   `served-` 태그를 만든다; 위 두 표의 "상태 파일만 있고 태그 없음"이 새 표에서 되풀이되면 만료가 같은 식으로 멈춘다). 원인 분류는
   W74 의 것이다. 이 문서의 요구는 하나다: 넘겨받는 PR 이 새 표의 서빙 스냅샷에 태그가 있는지를 굽기 뒤 확인하고 본문에 적는다.
9. **순서.** 한 경로를 끝까지 먼저 뚫는다(AGENTS.md §계층을 넓게 쌓기 전에).

   | 차례 | 표 | 왜 이 차례인가 | 필요한 것 |
   |---|---|---|---|
   | 1 | `gold.parcel_real_transaction` (+ `gold.real_transaction_region_stats`) | 2026-11-11 에 26 번 죽은 작업. 서빙 중이라 넘겨받기 전 과정을 처음 증명한다 | 공용 조각(어댑터의 `expected_head`·빈 추가·`truncate`, 계약 타입의 `current_rows`·`current_rows_sql`, 종류 줄의 `gold_table` 해석, 앞으로 붙여 되돌리는 명령, `version_seq` 변경 집합 명령, 표가 바뀐 레인의 전량 굽기) + 생산자 이식(배치 `placed_deals` 는 판정 완료 10,027,082 행 equal; 문서 조립 `gold_rows`·지역 통계·영향 법정동은 이식과 전국 판정이 남았다). 지역 통계 표는 `double` 칸이 있다 — Rust 가 읽지 않고 DuckDB 가 읽는다 |
   | 2 | `gold.parcel_nearby` | 옛 표가 없어 넘겨받기가 없다. 태어나기만 하면 된다 | 시설 Silver 여섯의 첫 적재(이 문서 밖, 막는 것). DuckDB 의 정확한 최근접(승인된 예외 ⓑ), spatial·h3 확장을 릴리스에 싣기(#1378 은 httpfs·avro·iceberg) |
   | 3 | 자기 생산자를 가진 종류 Gold 열(`build_or_merge` 를 부르는 파일 하나씩): 공매·경매·소유·규제구역·부속지번·에너지·인허가·멸실·공장·총괄표제부 | 표가 작고 레인이 독립이다 | 표마다 생산자 이식과 판정. `regulation_zones` 는 교차 넓이가 GEOS 로 바뀌어 예외 ⓑ 절차 |
   | 4 | `by_pnu_kind_silver_to_gold`(패널에서 나오는 종류 여섯)와 `unit_price_panel` | 호스트 패널의 핀을 따른다 | 5 와 함께 설계: 패널이 쌓는 표가 되면 종류는 패널의 `version_seq` 변경 집합을 입력으로 받는다 |
   | 5 | `gold.parcel_panel`·`gold.building_panel` | 가장 크다(3,990 만 행·4.27 GB). 앞의 표들이 잰 죽은 판·굽기 시간이 근거가 된다 | 전국 판정, 16 GB 생산자의 시도 샤드 이식 |
   | 6 | 곁가지 `MERGE` 둘(`industrial_complex_silver_to_gold`, `place_reference_queue_to_gold`)과 덮어쓰는 Gold 여섯(서빙 경계 셋, `place_id_registry`·`place_id_bridge`, `lineage_review_queue`) | by-PNU 레인이 아니라 독자가 저마다 다르다(PostGIS 적재, 타일, 스튜어드 API) | 표마다 독자 목록. 서빙 경계 셋은 기하 인계(물결 5)와 함께. 여섯의 이름은 계약에서 뽑은 것이고 W75d 의 분류와 맞대어 확인한다 |

## Consequences

- 실거래 Gold 가 넘겨받아지면 `real_transaction_silver_to_gold.py` 가 Spark 세션 장부에서 빠지고(지역 통계 표까지 쌓는 표가 되어야
  한다), 그 레인의 굽기에서 Spark(`by_pnu_panel_delta`)가 빠진다. `gold_rebuild.py` 의 계획은 그 표에 대해 발행기 명령 안으로 들어간다.
- 넘겨받기 동안 표마다 두 생산자가 같이 돈다. Spark 풀의 슬롯은 넘겨받은 뒤에 빈다. 넘겨받을 때 레인마다 전량 굽기가 한 번 있다.
- 현재 행 규칙의 렌더가 넷에서 하나가 된다. 산출물의 글자가 바뀌면 dbt 매크로와 검사가 함께 바뀐다(이미 CI 가 산출물과 대조한다).
- 쌓는 표를 그냥 조회하면 옛 판과 삭제 표시가 보인다. 이름과 런북의 한 줄이 그것을 알린다.
- 변경 비율 상한을 넘는 변경이 자동으로 전체 다시 쓰기로 넘어가지 않는다. 쌓는 표에는 "전체 다시 쓰기"가 없고, 큰 변경은 큰 추가다.
  상한을 넘으면 멈추고 사람이 사유와 함께 싣는다 — 지문이 어긋난 채 표 전체가 한 번 더 쌓이는 것을 막는다.
- DataHub·ODCS·스키마 레지스트리: 계약 산출물에서 렌더되므로 새 표가 자동으로 실린다. ODCS 는 `row_versions` 와 `current_rows_sql` 을
  품질 절에 싣는다(소비자가 그 SQL 로 읽어야 한다는 것이 계약의 일부다). by-PNU 문서 스키마(레지스트리의 주제)는 문서가 같으므로
  바뀌지 않는다. 파이프라인 그래프는 옛 표 노드가 `retired` 로 남고 새 표 노드가 선을 받는다.

## 하지 않은 것, 확인하지 못한 것

- 아무것도 짓지 않았다. 이 문서는 설계이고, 표의 모양과 넘겨받기를 운영에서 잰 것이 아니다.
- 7 번의 자람은 계산이다. 실거래 Gold 의 바뀐 키 실행이 운영에서 한 번도 커밋되지 않아 하루 변경량의 실측이 없다.
- 합치기(`rewrite_data_files`)의 `replace` 커밋이 우리 요약 속성을 싣지 않는다는 것은 실거래 Gold 의 스냅샷 둘(둘째에 `foundation.keys-changed`
  가 없다)에서 본 것이고 유지보수 코드에서 확인한 것이 아니다. 3-1 은 어느 쪽이어도 맞게 걷는다.
- 카탈로그 덧씌우개로 머리를 고정하는 길(3-6)은 라이브러리 소스를 읽어 정했고 돌려 보지 않았다. 첫 구현 PR 이 CI 의 실제 Iceberg 에서
  "계획과 커밋 사이에 다른 커밋이 끼면 실패한다"를 시험한다.
- 빈 추가, `truncate` 파티션 값, 합치기와 겹치는 커밋, 앞으로 붙여 되돌리는 명령.
- 넘겨받은 뒤 전량 굽기가 옛 세대와 바이트가 같은지 견주는 것(세대의 내용 다이제스트가 지금 요약에 있는지 확인하지 않았다).
- Gold 를 읽는 dbt·Dagster 검사·PostGIS 적재를 표마다 전부 찾지 않았다. 넘겨받는 PR 이 표마다 스캔 범위와 함께 적는다.
- 실거래 생산자의 문서 조립·지역 통계·영향 법정동 계산의 DuckDB 이식과 전국 판정(ADR-0559 가 남긴 것).
- 덮어쓰는 Gold 여섯의 이름은 계약에서 뽑았고 W75d 의 분류 원문과 맞대지 않았다.
- 설계 검토: 이 세션에는 `/egm:egm-review` 명령이 없어, 그 플러그인의 검토자 묶음(`egm-synthesizer`: 설계·코드 대조·구현 준비·범위)을
  이 문서에 한 번 돌렸다. 차단 3·주요 7·경미 4. 고친 것: 순서 값을 머리가 아니라 조상에서 읽기(합치기 뒤 1 로 돌아가는 결함), 묶음 레인에
  없는 검증 재기준 대신 전량 굽기, `gold_table` 이 종류 레인에서 읽히지 않는다는 사실과 옮길 독자 목록, 그림자 기간의 핀과 판정의 칸,
  서빙 상태의 표 바뀜과 옛 태그, 순서 값을 다시 쓰는 `rollback_main` 을 버리고 앞으로 붙여 되돌리기, 되돌리기 ⓐ 의 한계, 커밋과 태그
  사이에 죽은 run, SQL 이 동률을 거부하지 못한다는 것, 묶는 범위, 지역 통계 표의 독자, 사실 오류 넷, Amends, 합친 파일의 읽기 비용.
  받아들이지 않은 것 하나: 4 번을 따로 ADR 로 떼라는 것 — 현재 행의 단일 렌더는 ADR-0561 3 번이 첫 표의 전제로 둔 것이라 여기 둔다.

## 기각한 대안

- **옛 표의 계약을 제자리에서 `LatestPerKey` 로 바꾼다.** 표가 하나라 이름이 깔끔하다. 그러나 계약과 생산자를 한 배포에 바꿔야 하고
  (`admit_write`), 독자 전부가 같은 배포에 규칙을 거쳐야 하며, 파티션 명세를 서빙 중인 표에서 바꿔야 하고, 되돌리려면 이미 쌓인 판을
  걷어 내야 한다. 판정의 기준(옛 표)이 판정과 함께 사라진다.
- **순서 칸으로 `published_at_utc` 나 Iceberg 순번(v3 행 계보)을 쓴다.** 앞의 것은 문자열이고 생산자가 시작할 때 찍는다(같은 값의 두 커밋이
  가능하다). 뒤의 것은 v3 이고 Rust 독자가 v3 의 삭제 벡터를 거부한다(ADR-0344).
- **삭제 표시 행의 내용 칸을 비운다.** 필수 칸을 선택으로 풀어야 하고(옛 표와 칸 계약이 달라져 판정의 "칸 하나하나"가 흐려진다), 무엇이
  지워졌는지가 표에서 사라진다.
- **`__changes` 를 발행기의 둘째 추가로 남긴다.** 한 사실(바뀐 키)이 두 표에 적히고 두 커밋이 된다. 쌓는 표에서는 붙은 행이 곧 기록이다.
- **변경 집합을 추가 스냅샷 범위 읽기로 낸다**(`read_appended` 의 짝). 합치기가 사이에 끼면 범위가 끊겨 전체 대조로 떨어진다.
  `version_seq` 는 합치기에 무관하다.
- **현재 행을 Iceberg 뷰로 낸다.** R2 카탈로그의 뷰를 Rust 스캔이 읽지 못하고, 정의가 계약 밖에 한 벌 더 생긴다(ADR-0286 Rejected).
- **가지에 붙이고 검사 뒤 승격한다(WAP).** 라이브러리가 가지로 커밋하지 못한다. 손으로 커밋 본문을 짜는 것은 ADR-0560 이 기각했다.
