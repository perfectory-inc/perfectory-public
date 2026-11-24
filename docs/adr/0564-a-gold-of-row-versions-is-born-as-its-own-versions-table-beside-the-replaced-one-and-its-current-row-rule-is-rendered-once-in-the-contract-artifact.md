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
      아니라 멈추고 알린다; 넘는 변경을 싣는 것은 사유를 적은 운영자 실행이다 — 2026-11-20 개정: 주변 Gold 의 상가만 커밋은 생산자가 확인해 기록하고 넘는다, [ADR-0596](./0596-the-nearby-gold-records-a-store-only-commit-and-the-bake-bakes-it-full-until-nearby-moves-to-the-platform-gis.md)), 대조 표본(입력이 안 바뀐 키 표본을 다시 계산해
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

## Revision 2026-11-20: 넘겨받기의 공통 길 하나 (결정 6 의 구현, W84)

서빙 중인 Gold 를 쌓는 표로 넘길 레인(등기·건축물대장 계열 일곱, 실거래, 경매)은 이 길 하나를 쓰게 된다. 레인마다 다른 것은 자기 단계(`kind_versions_stage::KindVersionsStage` 의 구현)와
그 한 줄(`kind_versions_build::KindBuild`: 표·레인·옛 표·명령 이름)뿐이다 — 계획·차분·관문·커밋은 한 벌(`kind_versions_build::run`)이다. 지금 이 길(`planned_inputs`)을 부르는 빌드는 `parcel_nearby_versions_build` 하나다 — 그 레인은 이미 쌓는 표를 서빙하므로 그림자 갈래를
타지 않는다. 각 레인의 빌드는 자기 넘겨받기 PR 과 함께 온다.

- **그림자인지는 설정이 아니라 사실이다.** 종류 줄이 `versions_table` 을 말하고 레인이 아직 옛 표(`gold.<x>`)를 굽는 동안 `gold.<x>_versions` 는
  그림자다. 공통 함수 `gold_versions_commit::planned_inputs` 가 그때 핀을 입력의 머리가 아니라 옛 표의 `foundation-gold-build` 태그 커밋이 적은
  `foundation.source-iceberg-snapshots`(두 엔진이 같은 속성에 쓴다)와 그 커밋의 서빙 에디션으로 돌려준다. 그 커밋이 적지 않은 입력은 그림자도 읽지
  않는다. 첫 그림자 빌드는 전량이다. 레인이 쌓는 표를 굽게 되면 같은 함수가 머리를 돌려준다.
- **판정(결정 6-3)은 있는 도구 그대로다.** `export-gold-takeover-sides` 가 옛 표를 그 빌드 커밋에서, 새 표의 현재 행을 같은 핀의 그림자 커밋에서
  Parquet 으로 내고, `engine_equality.py` 가 자기 SQL 을 `--runner-command`(발행기의 `query-takeover-sides`)로 발행기의 DuckDB 에서 돈다 — 릴리스
  이미지에는 Python DuckDB 가 없다. 견주는 칸은 두 계약이 함께 가진 칸에서 커밋마다 다른 `source_snapshot_id`·`published_at_utc` 를 뺀 것이다
  (`row_digest` 포함). `scripts/ops/gold-takeover-verdict.sh <표>` 가 그 둘을 잇고, `equal` 이어야 넘겨받기 PR 이 열린다. 정규화는 없다: 키 순서만
  달라도 `not equal` 이고, 그때는 ADR-0559 의 되돌리기 ⓐ(판정이 다르면 넘기지 않고 사람이 고친다)를 따른다.
- **전환(결정 6-5)은 판정한 빌드 쌍에서만 굽는다.** 레인 상태가 서빙 팩이 구워진 표(`section_packs.gold_table`; 객체 레인은 `published.gold_table`)를
  알리고, 그것이 레인의 표와 다르면 그 실행이 전환이다. 굽기 스크립트는 발행기의 `show-gold-takeover-switch` 로 쌍 — 옛 표의 빌드 태그 커밋과, 같은
  입력 핀을 적은 쌓는 표의 그림자 커밋 — 을 읽고, 팩이 반영한 스냅숏이 옛 표의 그 빌드가 아니면 거부한다(먼저 그 빌드까지 굽는다). 그다음
  변경 집합 없이 새 세대를 그림자 커밋에 고정해(ADR-0310) 전량으로 굽는다. 쌍 뒤에 바뀐 것은 다음 실행이 쌓는 표의 변경 집합으로 패치한다.
- **전환 내용 검사.** 판정은 두 표의 행이 같다고 말할 뿐, 레인이 서빙하는 문서는 말하지 않는다. 결정 6-5 의 "새 세대의 문서는 옛 세대와 같아야
  한다"를 그대로 지킨다. 다만 문서의 `source` 블록은 구운 표와 스냅숏을 적으므로 바이트가 아니라 내용으로 견준다: 동마다 새 문서를 그 PNU 에 지금
  서빙되는 답(팩에서 게이트웨이처럼 이어 읽은 것)과, 문서 스키마 판 표시(`schema_version`)와 `source` 를 빼고 null 을 없음으로 읽어 견준다(확장 탐침,
  ADR-0320 과 같은 비교). 다른 문서가 하나라도 있거나, 서빙되지만 새 표가 더는 답하지 않는 PNU 가 하나라도 있으면 그 동은 아무것도 쓰지 않고
  실행이 실패해 발행되지 않는다. 거부는 렌더러나 현재 행 규칙이 새 표를 옛 표와 달리 읽는다는 뜻이므로, 다시 시도해도 같은 쌍에서 같은 답이
  나온다: 고칠 곳은 코드이고, 고칠 때까지 레인은 옛 팩을 그대로 서빙한다(서빙이 틀어지지는 않는다). 로그는 실행 디렉터리의 샤드 로그에 처음 20 개
  PNU 를 적는다. 검사는 켜는 설정이 아니다: 다른 표의 팩 위 기초 굽기는 언제나 검사하고, 그런 팩 위 패치는 굽기와 발행 둘 다 거부한다. 비용은
  서빙 세대의 팩 수만큼의 읽기(동·구역마다 GET 한 번)와, 샤드마다 구역(section)마다 목록 한 번이다. 쪽으로 나뉜 문서(ADR-0325)는 첫 쪽만 견준다 —
  한계다.
- **전환 뒤.** 전량 세대의 발행은 팩 상태의 `gold_table` 을 레인의 표로 옮긴다(그 전에는 옛 표를 그대로 적어, 전환 다음 실행이 다시 전환으로 보였다).
  다른 표의 팩 위 패치·반영·한 구역만 다시 굽기는 발행이 거부한다. 그 전량 굽기가 유닛 한도에 걸리면 다음 실행이 같은 고정 스냅숏에서 이어 굽고
  동 단위로 재개한다(#1520). 넘겨받는 레인은 모두 팩 레인(ADR-0190 의 종류)이라, 객체 레인의 전환은 검사 없이 굽지 않고 거부한다.
- **구역 손실 막이(ADR-0272)는 쌓는 표의 공통 쓰기 길에 있다.** `gold_versions_commit::diff` 가 붙이기 전에, 다시 계산한 범위의 시도마다 내용 칸
  (`*_json`)이 빈 것이 아닌 PNU 수를 머리의 현재 행과 견준다. 경계는 `gold-panel-rebuild.contract.json` 의 `section_coverage` 하나다. 코드 변경 표의
  시도 짝(단계가 남긴 `pair_rows`)은 옛 시도가 머리에만 있고 새 시도가 빌드에 있을 때(또는 그 반대) 한 구역으로 견준다. Spark 와 다른 점 하나:
  전체는 다시 계산한 범위의 전체이므로 `min_region_pnus` 보다 작은 범위는 판정하지 않는다 — 그 범위가 통째로 비어도 붙는다. 한계다.

**한 레인의 순서와 끝.** ① 레인의 단계와 빌드 명령을 들인다(종류 줄 `versions_table`) → ② 그림자 빌드가 옛 표의 빌드 핀으로 전량을 쌓는다 →
③ 옛 생산 작업을 멈추고 마지막 그림자 빌드 뒤 `gold-takeover-verdict.sh` 가 `equal` → ④ 넘겨받기 PR(종류 줄 `gold_table` 을 쌓는 표로, 옛 작업을 작업
정의에서 뺌, `verdict.json` 을 본문에) → ⑤ 배포 뒤 첫 굽기가 전환(위) → ⑥ 다음 실행이 패치로 돌아오면 끝이다: 레인 팩의 `gold_table` 이 쌓는
표이고, 옛 작업이 없고, 패치 굽기 하나가 성공했다. ③ 뒤에 옛 표가 다시 빌드되면 같은 핀의 그림자 커밋이 없어 전환이 거부된다 — 그래서 ③ 에서
옛 작업을 먼저 멈춘다. `not equal` 이면 ④ 로 가지 않는다.

**쓰지 않은 Iceberg 기능과 그 까닭.** 가지(branch)와 `fast_forward`(WAP): iceberg-rust 0.11 은 가지로 커밋하지 못하고(ADR-0560), 가지는 표의 스키마
하나를 함께 쓰는데 옛 표(Replaced)와 쌓는 표는 모양이 다르다. `rename`·`register_table`: 두 표는 모양이 달라 이름을 바꿔 끼울 수 없고 독자는
`_versions` 이름을 읽는다. 태그는 이미 쓰던 `TableUpdate` 로 옮긴다.

**끝나는 조건.** 그림자 핀(`Shadow`·`shadow`·`shadow_of`·`planned_inputs` 의 그림자 갈래), 판정 길(`gold_takeover_sides.rs` 의
`export-gold-takeover-sides`·`query-takeover-sides`, `gold-takeover-verdict.sh`), 전환(`show-gold-takeover-switch`, `section_packs/takeover.rs`,
굽기 스크립트의 전환 갈래)은 마지막 서빙 Gold 가 쌓는 표로 넘어가면 지운다. `engine_equality.py` 의 `--runner-command` 다리는 마지막 Spark 작업이
사라지면 지운다. 다른 표의 팩 위 패치 거부와 전량 세대의 `gold_table` 옮기기, 구역 손실 막이는 오래 가는 규칙이다.

**아직 없는 것.** 결정 6-3 의 둘째 판정(증분 길)은 레인의 넘겨받기 PR 이 각자 싣는다. 전환에 걸리는 시간은 첫 레인에서 잰다.

## Revision 2026-11-22: 판정은 한 시도, 표 전체는 집계 (결정 6-3, 사장 결정)

결정 6-3 의 판정이 행을 견주는 범위는 한 시도다(ADR-0559 의 같은 날 개정 메모). "첫 판정: 첫 빌드(전량)에서"는 그림자의 첫 빌드가 전량이라는
말이고, 판정은 그 빌드에서 시도 하나의 행을 견준다. 둘째 판정(증분 길)도 같다. 결정 9 표의 1·5 줄과 "하지 않은 것"의 "전국 판정"은 이 범위로 읽는다.

- **한 시도의 바이트.** `gold-takeover-verdict.sh <표> [시도]`(두 자리, 기본 `41` 경기도 — 가장 크다. 레인을 맡은 사람이 앞선 판정이나 전환의
  내용 검사가 차이를 낸 시도를 줄 수 있다)가 `export-gold-takeover-sides` 에 시도를 넘긴다. 내보내기는 키 가운데 시도 코드를 가진 첫 칸(`pnu`,
  없으면 `region_code` — 지역 통계의 `legal_dong`·`sigungu` 둘 다 시도로 시작한다)의 범위(`>= '41' AND < '42'`, 파일 경계로 건너뛴다)로 두 쪽을
  거르고 그 시도의 행만 Parquet 으로 낸다. 현재 행 규칙보다 먼저 거르므로 키를 통째로 남기거나 뺀다. 옛 표에 그 시도의 행이 없으면 거부한다
  (빈 두 쪽이 `equal` 이 되지 않게). 시도 코드를 가진 키가 없는 표는 거부한다.
- **표 전체는 집계로만.** 같은 명령이 두 쪽 전체를 한 번씩 읽어 집계만 낸다: 행 수, 서로 다른 키의 수(행 수와 같아야 한다), 키 해시의 합,
  `row_digest` 의 합(Gold 지문과 같은 셈), 견주는 칸마다 null 아닌 값의 수. 두 쪽이 모두 같아야 하고, 하나라도 다르면 `checks.json` 에 적고 판정
  전에 멈춘다(exit 3). 그래서 시도 밖의 값 차이도 `row_digest` 의 합이 본다 — 합이므로 어느 키인지는 말하지 않는다.
- **기록.** `verdict.json` 에 `scope`(시도, 시도의 행 수, 전국 행 수, 집계 검사)가 붙는다. 넘겨받기 PR 은 그것을 싣는다.
- **전환.** 결정 6-5 의 전량 굽기는 그대로다: 판정한 빌드 쌍에서 구운 문서가 옛 세대와 다르면 발행하지 않는다(위 Revision 의 전환 내용 검사).
  전환이 없는 표(결정 6-9)는 위 집계가 전국의 검사다.
- **판정은 Dagster 의 자산 검사다(같은 날 덧붙임).** 레인의 정의가 그림자 `_versions` 자산에 검사 `takeover_equal` 을 시도와 함께 선언한다
  (`asset_checks`, `sido` 는 반드시 적는다; `blocking: true` 는 심각도 ERROR). 그림자 빌드의 유닛(`gold-versions-build.sh`)이 그림자 커밋 뒤에, 그리고
  그 머리의 판정이 끝까지 돌 때까지 다음 실행마다 같은 유닛의 키·풀로 판정을 돌린다(30 분·DuckDB 4 GB 상한, 유닛의 한 시간·`MemoryHigh` 안). 결과(시도의
  대조 수·집계 검사)를 journal 의 Pipes 줄로 알리면 유닛을 돌린 op 가 그 검사의 평가로 남긴다(`orchestration/dagster/takeover_checks.py`,
  `systemd_pipes.SystemdRun.check_evaluations`). 판정이 돌지 못하면 검사가 "끝까지 돌지 못했다"로 실패하고, 빌드의 커밋과 결과 줄은 그대로다.
  평가는 Dagster 안에서 아무것도 막지 않는다(물질화와 나란히 op 가 남긴다). 문은 넘겨받기다: 넘겨받기 PR 이 지금 그림자 머리(`candidate_snapshot`)에
  대해 통과한 평가의 run 을 인용하고, 전환은 판정한 빌드 쌍에서만 굽는다. 사람의 호출·호스트 슬롯은 없다. 위의 `gold-takeover-verdict.sh <표> [시도]`
  (이 Revision 의 첫 묶음, ADR-0559 의 같은 날 개정 메모)는 이 길로 바뀐다: 그 스크립트는 이제 유닛이 `<표> <시도> <작업 디렉터리>` 로 부른다.

## Revision 2026-11-22 (둘째): 판정은 값으로 견주고, 문서는 스키마로 본다 (결정 6-3 과 첫째 개정의 집계, 사장 규칙)

사장 규칙(같은 날 코디네이터 규칙 묶음의 "NO SPARK BYTE EMULATION"): 이식은 Spark·Java 의 글자 습관(`to_json` 의 실수 표기 `1.0E7`,
`Double.toString`, `.SSSZ` 시각, 키 순서, null 칸 생략)을 따라 쓰지 않는다. 판정은 **값**을 견주고, 후보가 서빙할 문서는 스키마 레지스트리의 계약으로
검증한다. 아래 "대체되는 문장"이 이것으로 읽힌다.

- **값의 뜻은 하나다.** 판정과 전환의 내용 검사(`section_packs::expansion::same_content`, #1608)는 같은 함수
  `foundation_publisher_core::json_values` 로 JSON 을 읽는다: null 칸은 없는 칸이다, 숫자는 값이다(`1.0E7`·`10000000.0`·`10000000` 이 하나,
  정수인 실수는 정수), RFC 3339 시각은 UTC 의 그 시각이다, 객체의 칸은 이름의 집합이다(키 순서를 보지 않는다), 목록의 순서와 글자는 쓴 그대로다.
  판정은 두 쪽의 `*_json` 칸을 이 함수로 읽어 칸 이름순의 한 글로 다시 쓴 뒤 견준다(JSON 이 아닌 글은 그 글 그대로). 그 밖의 칸은 글자로 견준다.
- **`row_digest` 는 견주지 않는다.** 쓴 바이트의 해시라 두 엔진이 같게 쓸 까닭이 없다. 그것이 대신하던 내용은 칸 하나하나로 견준다.
  **대가:** 바이트 판정은 DuckDB 단계의 다이제스트가 내용 칸을 하나도 빠뜨리지 않았음을 함께 증명했다(ADR-0559 결정 3 "칸을 덜 넣고 해시한 이식").
  쌓는 표는 다이제스트가 바뀐 키만 붙이므로(`gold_versions_commit::appended_sql`), 칸을 빠뜨린 다이제스트는 첫 빌드(모든 키를 붙인다)의 판정을
  통과하고 뒤 실행에서 그 칸의 변경을 조용히 놓친다. 그래서 **이식마다 단계 시험이 내용 칸 하나하나를 바꾸면 `row_digest` 가 바뀜을 보인다**(첫
  사례: 필지 패널의 단계 시험). 증분 길의 둘째 판정(결정 6-3)이 그 위의 둘째 그물이다. 결정 6-4 의 장부 줄은 "판정의 지문" 대신 집계의 내용 해시 합과
  시도의 대조 수를 싣는다.
- **표 전체의 집계.** 행 수·키의 유일성·키 해시의 합·칸마다 null 아닌 수는 SQL 한 번으로, **내용 해시의 합**은 발행기가 한 쪽을 시군구(시도 칸의
  앞 다섯 글자)마다 읽어 각 행의 키와 값으로 읽은 내용을 함께 해시해 더한다(`gold_takeover_sides::values`). 키를 함께 넣으므로 두 키의 내용이 서로
  바뀌어도 합이 다르다. 시군구마다 읽는 까닭은 DuckDB 가 발행기에 결과를 통째로 넘기기 때문이고(시군구 하나는 수십만 행), 두 표 다 그 칸으로 정렬돼
  있어 범위가 다른 파일을 건너뛴다. 같은 패스가 그 시도의 행을 값으로 읽은 채 Parquet 으로 낸다.
- **문서는 스키마로 본다.** 같은 명령이 후보의 그 시도 행을 시군구마다 레인의 렌더러(`section_packs::sections::Renderer`, 굽기가 돌리는 것)로 문서로
  만들고, 그 레인 주제의 현재 판 JSON Schema 로 검증한다. 현재 판은 릴리스에 컴파일된 것이다: `scripts/schema_registry.py generate` 가
  `host_document_schemas.rs` 의 `LANE_DOCUMENT_SCHEMAS` 로 넣고 `check` 가 레지스트리와 묶는다. 검증기는 `jsonschema` 크레이트(0.62.0, MIT, 기본 기능
  없이 — 원격·파일 참조를 읽지 않는다)다. 렌더러가 거부한 행이나 스키마가 거부한 문서가 하나라도 있으면 결과 검사가 실패한다(`checks.json`
  `documents`·`failed`, Dagster 검사의 `documents_rendered`·`documents_refused`). 기각: 레지스트리가 쓰는 부분집합만 보는 우리 검사기(`schema_registry.py`
  의 검사기는 두 판을 견주는 호환성 검사이고 문서를 검증하지 않는다 — 검증은 표준이 있는 일이다). `x-open-enum` 은 읽는 쪽의 호환성 표시라 검증기는
  모르는 키워드로 넘기고, 쓰는 쪽 문서는 `enum` 그대로 본다.
- **문서 검사가 보지 않는 것.** 그 시도 밖(전환의 전량 굽기가 모든 문서를 서빙 문서와 견준다); 레인 스키마가 열어 둔 것(`type`·`required`·`enum`·`pattern`
  만 있고 `additionalProperties` 는 없어, 덧붙은 칸은 통과한다); `source` 블록(후보의 표와 스냅숏, 자리표시 위치로 찍는다); 굽기의 Iceberg 독자가 읽는
  꼴(렌더러는 DuckDB 의 `to_json` 이 낸 행을 받는다 — 날짜·시각·decimal 칸이 같게 읽히는지 확인하지 않았다). by-PNU 레인이 굽지 않는 표(실거래 지역
  통계, 결정 6-9)와 레지스트리가 문서 주제를 두지 않은 레인(건물 레인: 페이지 부분 주제만 있다)은 검사하지 않고, `documents.skipped` 에 까닭을 적는다.
- **순서와 실패 뒤에 남는 것.** 두 쪽의 패스(집계와 시도의 행) → 문서 검사 → `checks.json` → 두 쪽의 Parquet 과 `sides.json` → 결과 검사가 하나라도
  실패했으면 명령이 실패한다. 판정 스크립트는 그때도 `sides.json` 이 있으면 시도 대조를 돌려 `verdict.json` 에 남기고 3 으로 끝난다(같다 0, 다르다 1,
  결과 검사 실패 3: 집계 또는 문서, 그 밖은 실행 오류). 시도에 옛 표의 행이 없으면 실행 오류다.
- **시간(계산, 재지 않음).** 패스는 두 표를 한 번씩 다 읽고 `*_json` 칸을 한 스레드로 해석한다. 필지 패널(3,986 만 행, 문서 칸 아홉)이면 두 쪽
  약 20 GB 의 JSON 글이고, 한 스레드 초당 100~200 MB 로 어림해 2~4 분, 시군구 약 250 번의 읽기를 더해 수 분이다. 문서 검사는 경기도(약 520 만 행)에서
  문서 하나 수십 마이크로초로 어림해 수 분이다. 판정의 30 분 상한 안으로 본다. 첫 레인의 판정이 잰다.
- **대체되는 문장**(넘겨받기 판정에 한해): 결정 5 의 "넘겨받기 판정과 주 1 회 검사가 현재 행 전체에서 다시 계산해 견준다"(지문 — 판정은 이제 값의 해시
  합을 견주고, 지문은 커밋의 것으로 남는다), 결정 6-3 의 "`row_digest`, 그리고 지문", 결정 6-4 의 "판정의 지문", Revision 2026-11-20 의 "(`row_digest`
  포함)"과 "정규화는 없다: 키 순서만 달라도 `not equal`", 첫째 Revision 2026-11-22 의 "`row_digest` 의 합(Gold 지문과 같은 셈)"과 "시도 밖의 값 차이도
  `row_digest` 의 합이 본다"(이제 내용 해시의 합이 본다). ADR-0559 의 대체는 그 문서의 같은 날 둘째 개정 메모에 있다.

## Revision 2026-11-22: 레인 밖 판 표의 공통 커밋과 스튜어드 큐의 그림자 (결정 3·6-9, W97)

- 고치는 곳: 결정 3-3 의 차분 키(한 칸 → 계약의 키), 위 Revision 2026-11-20 의 구역 손실 막이, 결정 9 의 차례(스튜어드 큐는 차례 6 의 표이지만
  공통 커밋을 넓히는 첫 표로 앞당긴다).

결정 6-9 의 표(by-PNU 레인이 아닌 Gold: 실거래 지역 통계, 서빙 필지 경계, 스튜어드 검토 큐, 그리고 뒤의 `place_id_*`)도 공통 커밋(`gold_versions_commit`)
하나로 쓴다. 공통 커밋은 새 설정을 받지 않고 판 표의 계약에서 읽는다. 키 첫 칸이 `pnu` 인 표에서는 지금과 같은 값이 나온다
(`only_changed_keys_are_appended_and_a_key_that_left_gets_a_marker` 가 `key() == ["pnu"]` 를 단언한다).

| 무엇 | 읽는 곳 | by-PNU 표 | 레인 밖 표(스튜어드 큐) |
|---|---|---|---|
| 키 | 판 표 계약의 현재 행 규칙 `key`(여러 칸) | `pnu` | `(unit, subject_code)` |
| 내용 digest | `row_digest` 칸 — 옛 표에 없으면 판 표가 더하고(파생 규칙, 서빙 필지 경계와 같은 날) 생산자가 스탬프를 뺀 내용으로 채운다 | `row_digest` | 생산자가 채운 `row_digest` |
| 준비 파일 | 판 표 계약의 파티션 명세의 값마다 하나 | 시도 | `unit` |

- **스탬프는 한 정의다.** 생산자의 digest 가 빼는 스탬프와 판정(결정 6-3)이 견주지 않는 스탬프는 같은 상수(`PER_COMMIT_STAMPS`)다. 스탬프만 다른 행은
  붙지 않는다. 지문(결정 5)은 그대로 `row_digest` 앞 15 자리의 합이다 — digest 가 표에 적히므로 식이 바뀌어도 한 합에 두 식이 섞이지 않는다.
- **지우는 표시의 스탬프는 칸의 형으로 쓴다**(스튜어드 큐의 `published_at_utc` 는 `timestamp`).
- **구역 손실 막이(위 Revision 2026-11-20)는 키 첫 칸이 `pnu` 인 표에 건다.** 다른 표의 `*_json` 칸(스튜어드 큐의 `candidates_json`)은 필지의 구역이 아니다.
  여러 칸 키의 by-PNU 표(결정 9 차례 4 의 `unit_price`)도 막이를 받는다.

**스튜어드 큐의 그림자.**

- **핀은 옛 표가 기록한 것을 빌린다.** 옛 표에는 빌드 태그도 `foundation.source-iceberg-snapshots` 도 없다. 대신 두 Spark 작업이 커밋마다 읽은 입력을
  적는다: parcel unit 은 `foundation.lineage-snapshot-id`, place_reference unit 은 원천마다 `foundation.place-reference-snapshot.<원천>`. 그림자 작업
  (`gold_lineage_review_queue_versions_build`, 입력이 움직이면 시작한다)은 옛 표의 `main` 을 새것부터 걸으며 입력마다 가장 새 기록을 핀으로 쓴다
  (`recorded_snapshots` 와 같은 걸음). 남은 이력이 이름 대지 않는 원천은 그 뒤 움직이지 않은 것이라(옛 작업은 매 실행 움직인 원천을 모두 읽는다) 머리를 쓴다.
  그래서 두 표는 언제나 같은 입력의 두 답이다.
- **첫 빌드는 옛 표의 스냅숏 짝을 잇는다.** place_reference 항목의 `from_snapshot_id`·`to_snapshot_id` 는 그 항목의 근거가 마지막으로 바뀐 원천 스냅숏
  짝이고, 옛 표에서는 `MERGE`(`WHEN MATCHED AND t.evidence_etag <> v.evidence_etag`)가 그 역사를 지킨다. 새 표는 같은 규칙을 머리의 현재 행으로
  지킨다(근거가 같은 항목은 머리의 짝을 그대로 둔다). 머리가 없는 첫 빌드는 옛 표의 행(빌린 핀을 적은 그 머리)을 머리로 삼는다. 내용(상태·후보·
  `evidence_etag`)은 새 엔진이 스스로 계산하고, 옛 표에서 오는 것은 근거가 같은 항목의 짝 둘뿐이다. 그 뒤는 빌린 핀이 같은 짝을 낸다.
- **Spark 의 부분 처리를 그대로 옮긴다.** parcel unit 은 계보가 움직이면 통째로, 원천의 항목은 그 원천이 움직이면 그것만, 코드(`code_version`: 큐 단계와
  계보 독자의 글자)가 움직이면 둘 다. 코드만 움직인 실행에서 움직이지 않은 원천의 새 항목은 `from_snapshot_id = to_snapshot_id` 다. 첫 빌드는 전량이다 —
  결정 3-2 의 `full` 예외, 계산한 900 s(작업 정의의 `first_run`).
- **결정 3-4 의 검사.** 키마다 한 행(차분)과 unit 마다의 상태값(parcel 은 `needs_review`·`pending`, place_reference 는 `steward`·`awaiting_data`)을 본다.
  잃은 행의 하한과 바뀐 키 비율의 상한은 두지 않는다: 항목은 스튜어드가 정하거나 더 센 근거가 닫으면 빠지므로 큐는 한 실행에 비어도 맞고, 옛 두
  작업에도 없었다.

**판정·전환·끝·되돌리기.**

- **판정.** 판정은 위의 자산 검사 `takeover_equal` 이다(값으로 견주는 둘째 개정 그대로). 지금의 판정 도구(`export-gold-takeover-sides`)는 옛 표의 빌드 태그와 시도를 가진
  키를 요구하므로 스튜어드 큐에 그대로 돌지 않는다. 그래서 큐의 정의는 아직 그 검사를 선언하지 않고, 그림자 유닛은 판정을 돌리지 않는다. 넘겨받기
  전에 도구를 넓히는 PR 이 더한다: 빌린 핀과 옛 표 머리의 짝(`shadow_of` 대신), 시도 키가 없는 표의 범위(표 전체 — 2026-11-19 22,068 행, 위 Revision 의
  시도 거부를 이 표에 한해 고친다), 그리고 큐 정의의 `asset_checks` 한 줄. 견주는 칸은 스탬프(`published_at_utc`)를 뺀 모든
  칸이다 — 첫 빌드가 짝을 이으므로 `from_snapshot_id`·`to_snapshot_id` 도 견준다. `not equal` 이면 넘기지 않는다.
- **전환(넘겨받기 PR).** 스튜어드 순환(`lineage-stewardship-cycle.sh`)이 Spark 두 단계 대신 이 표를 쓰는 명령을 부르고, 그 명령이 unit 마다 넘김
  파일(`lineage_review_queue.handoff_document` 의 모양, `stewardship_domain::handoff` 가 읽는 것)을 현재 행에서 쓴다. 같은 PR 이 그림자 핀과 첫
  빌드의 이어받기(`one-off` 표시)를 지우고 입력의 머리로 돌린다. 지우는 것: `lineage_review_queue_to_gold.py`·`place_reference_queue_to_gold.py` 와
  그 시험, `lineage-queue-check.py`(옛 표의 속성을 읽는다; 같은 일은 이 명령의 "할 일 없음"이 한다), 순환의 `--probe-only`(계보가 아직 없으면 이 명령이
  기다림으로 끝난다), `place_reference_queue.py` 와 그 시험. `lineage_review_queue.py` 는 다른 모듈(`legal_dong_code_change_pairs`·`parcel_lineage_section`·
  `parcel_panel_silver_to_gold`·`parcel_registry_frames`)이 `steward_resolved` 를 읽는 동안 남는다. 엔진 장부 두 줄은 `moved` 로, 그래프의 옛 표 노드는
  `retired` 로.
- **끝.** 순환 한 번이 두 unit 을 이 표의 현재 행에서 스튜어드 DB 로 실었고, 옛 두 작업과 그 정의가 없고, 장부 두 줄이 `moved` 다.
- **되돌리기.** 넘겨받기 PR 을 되돌린다. 옛 표는 지워지지 않았다: parcel unit 은 다음 실행이 통째로 다시 쓰고, place_reference 는 그 사이 움직인 원천을
  `MERGE` 가 다시 읽는다(옛 표가 기록한 스냅숏과 머리가 다르므로). 30 일(스냅숏 보존) 안이면 그대로 성립한다.

**기각한 대안.** 머리의 핀으로 그림자를 짓고 두 표의 핀이 우연히 같은 때 판정하기(같은 때가 오지 않을 수 있고, 첫 빌드의 짝이 영원히
다르다). 판정에서 place_reference 의 짝 둘을 빼기(정규화 금지, 위 Revision 2026-11-20). 공통 커밋이 표에 없는 digest 를 계산하기(식이 바뀌면 지문이
조용히 틀린다 — 그래서 digest 는 표에 적는다).

## Revision 2026-11-22 (넷째): 유닛의 한 시간을 넘는 빌드는 실행을 이어 짓는다 (결정 3, W81e)

판을 쌓는 표의 빌드는 `foundation-gold-versions@.service` 의 한 시간(`TimeoutStartSec`) 안에서 돈다. 실거래 레인의 첫 그림자 빌드는 전량이라 그보다
길다(계산: 2026-11-21 4 스레드 대조의 157만 거래 750 초를 전국 1,994만 거래로 늘리면 약 9,500 초). 한도를 레인 하나만 올리지 않고(인스턴스마다 손으로 쓴
drop-in 은 두지 않는다), 공통 빌드 길에 이어 짓기 하나를 둔다: `services/foundation-outbox-publisher/src/gold_versions_resume.rs`.

- **지문 하나에 상태 하나.** 상태는 레인의 상태 디렉터리 아래(`gold-versions-build.sh` 가 `FOUNDATION_PLATFORM_GOLD_VERSIONS_RESUME_DIR` 로 넘긴다)
  실행의 입력 핀·코드·판 지문(`gold_versions_commit::run_id`)마다 디렉터리 하나다. 다른 지문의 것은 열 때 지운다: 입력이 움직이면 처음부터다.
- **묶음.** 빌드는 일을 매 실행 같은 번호로 나눈 묶음(레인의 지역)으로 하고, 끝난 묶음의 행을 파일 하나로 둔다(쓰고 나서 이름을 바꿔 통째로만 선다).
  레인이 읽은 것(스풀)도 상태에 두어 이어 짓는 실행은 레이크하우스를 다시 읽지 않는다.
- **멈춤.** 25 분이 지나면 새 묶음을 시작하지 않는다(유닛의 한 시간에서 커밋한 실행이 이어 돌리는 판정 30 분과 마지막 묶음을 뺀 값). 그 실행은 커밋 없이
  `partial` 로 끝나고(넘겨받기 검사는 `committed`·`unchanged` 실행만 판정한다), 같은 지문의 다음 실행이 남은 묶음부터 잇는다. 마지막 묶음을 끝낸
  실행이 결정 3 대로 한 번 diff·커밋하고 상태를 지운다.
- **바뀐 것만.** 레인은 커밋 뒤 그 커밋이 무엇에서 지어졌는지(판·코드·판본과 지역마다의 행 요약)를 지문 밖 레인 기록으로 둔다. 다음 실행의 기준 커밋·
  코드·판본과 모든 문서가 읽는 것이 같으면 요약이 움직인 지역의 묶음만 짓고 diff 를 그 지역으로 좁힌다. 첫 사용자는 실거래 레인이다
  (`real_transaction_versions_stage`: 시군구 스풀, 코드 대응·서빙 형태·계보가 잇는 시군구를 한 묶음으로). 종류 레인(`kind_versions_build`)은 빌드가
  1 분 안이라 아직 쓰지 않는다. 지역 묶음은 뒤에 Dagster 파티션으로 옮길 수 있게 데이터에서 정해지는 결정적인 집합이다.

## Revision 2026-11-22 (다섯째): 호 공시가격의 그림자 (결정 9 차례 4, W98)

- 고치는 곳: 결정 3-4(바뀐 키 비율의 막이가 무엇으로 풀리는가), 결정 9 의 차례 4(`unit_price_panel`).

`gold.unit_price_versions`(키 `(pnu, building_id)`, 레인 밖 판 표 개정의 여러 칸 키)는 공통 커밋으로 쓰고, 계산은 발행기 안 DuckDB 의
`unit_price_stage` 가 한다. 종류 단계(`KindVersionsStage`)에는 넣지 않는다: 서빙 필지판으로 거르지도 재번호하지도 않고, 호는 자기 PNU 가 아니라
건물 문서가 그 호를 싣는 PNU 아래에 놓인다. 그 배치는 건물 패널과 같은 규칙 하나다(`building_unit_placement`: 인계 계약의 근거 규칙, 카탈로그
UUIDv8). 건물 패널이 옮겨 올 때 그것을 부른다.

- **핀은 호스트 패널을 따른다.** 그림자인 동안은 옛 표 빌드 태그의 핀 그대로다. 레인이 이 표를 굽게 되면 표제부·전유부는 건물 Gold 의 빌드 태그
  커밋이 읽은 스냅숏에 핀한다(`gold-panel-rebuild.contract.json` `pins_from`). 두 문서가 한 호를 같은 PNU 아래에 두어야 해서다.
- **가격 배치는 가장 새 vintage 의 것, 같은 vintage 의 여럿은 표가 붙인 순서의 마지막이다**(루트 ADR-0182). 계약의 판 순서(`row_versions` 의 `vintage`)만으로는
  그 동률을 거부하므로, 적재 원장(`foundation.ingest-batch-objects`)과 스냅숏 시각으로 순서를 세운다(`order_batches` 의 이식; 만료된 스냅숏의 배치는 남은
  것들보다 앞이다). 순서를 알 수 없으면 거부한다. 커밋은 읽은 배치를 `foundation.unit-price-batch` 에 적는다.
- **바뀐 가격 배치는 크기와 상관없이 바뀐 키를 설명한다**(결정 3-4 에 더함). 기준일이 한 해에 한 번 새로 붙으면 값이 있는 거의 모든 행이 바뀐다.
  코드 버전이 움직였을 때와 같은 자리에 기록한다(`foundation.recorded-explanation`). 배치가 그대로인데 절반 넘게 바뀌면 지금처럼 거부한다.
- **매 실행 모든 행을 짓는다.** 이 표의 비용은 읽기다(배치 하나가 1 억 8,873 만 행). 바뀐 PNU 만 지으려면 옛 핀과 새 핀을 둘 다 읽어야 해서 더 비싸다.
  붙는 것은 지금처럼 digest 가 바뀐 키뿐이다.
- **가격 이력이 줄어드는 빌드는 붙이기 전에 거부한다**(Spark 의 `assert_price_histories_kept`, `max_price_history_shrink_fraction`). 커밋이 쓰는 키의
  머리 쪽 행과 새 행에서 호마다 기록 수를 모아 전국에서 견준다(다른 시도로 옮긴 호도 짝을 찾는다). 분모는 머리의 현재 행이 가진 호 전체다.

## Revision 2026-11-23: 첫 넘겨받기 — 건물 에너지 (결정 6-4, W84b)

- 고치는 곳: 결정 6-4(넘겨받기 PR 이 바꾸는 것), 한 레인의 순서 ④.

`parcel-energy` 가 처음으로 넘겨받는다. 판정은 운영의 자산 검사다: `takeover_equal` 이 그림자 머리 `3875522570786167873`(옛 표 빌드
`4206959693242110730` 의 핀)에 대해 통과했다(Dagster run `364e6235-9fc1-473d-9dc1-85dd00e6d1de`, 유닛 run `20261123T005744Z`; 시도 41 의
286,663 행 모두 같음, 전국 1,449,777 행·키 해시 합·내용 해시 합·non-null 수가 같음, 시도의 문서 286,663 개 모두 스키마 통과). 첫 판정(전량)으로
넘겨받는다. 둘째 판정(증분 길)은 돌지 않았다: 그림자는 첫 전량 커밋(`version_seq` 1) 하나뿐이다. 이것이 이 넘겨받기의 한계이고, 넘겨받은 뒤의
첫 바뀐 키 실행은 다음 굽기의 패치가 처음 쓴다.

- **넘겨받기 PR 이 바꾸는 것.** 종류 줄의 `gold_table` 을 쌓는 표로, `spark_held_test` 를 발행기 단계의 시험으로, 옛 생산 작업
  (`gold_parcel_energy_rebuild`)과 그 `host_memory` 를 정의에서 빼고, Spark 파일과 시험을 지우고, 엔진 장부 줄을 `moved`(PR·전국 행 수·
  `rows:N;content_hash_sum:…`)로, 코드 버전 장부(`gold-panel-rebuild.contract.json` `plumbing_symbols`)의 그 생산자 줄을 지운다. 그림자의
  핀은 `gold_table` 이 바뀌는 것만으로 풀린다(`gold_versions_commit::shadow`: 레인이 쌓는 표를 구우면 `None`). 그림자 자산의 `takeover_equal`
  선언은 뺀다 — 그림자가 아니면 판정할 짝이 없다(`gold_takeover_check.declared` 도 그림자 아닌 실행을 판정하지 않는다).
- **옛 표의 노드는 남고 `unscheduled: replaced` 를 말한다.** 옛 표는 계약·데이터가 남으므로(결정 1, 지우는 것은 30 일 뒤 런북) 파이프라인 그래프의
  표로 남는다. 생산 작업도 읽는 이도 없으므로 그 까닭을 `replaced` 로 적는다(`job_specs.UNSCHEDULED`, `pipeline-graph.v1` 의 enum). 연결 규칙은
  `contract_only` 처럼 그 노드를 섬으로 받고(`pipeline_graph.reconcile`), 그 노드에 닿는 구현된 간선은 지어낸 길로 거부한다.

## Revision 2026-11-23: 둘째 넘겨받기 — 총괄표제부, 그리고 옛 생산 작업은 꺼진 줄로 한 릴리스 남는다 (결정 6-4, W84c)

- 고치는 곳: 결정 6-4(넘겨받기 PR 이 바꾸는 것). 위 Revision 의 "옛 생산 작업을 정의에서 뺀다" 를 바꾼다.

**옛 생산 작업은 넘겨받기 PR 에서 `enabled: false` 로 남는다.** 첫 넘겨받기(#1662)는 켜진 `gold_parcel_energy_rebuild` 를 지웠고, 릴리스 활성화
(`foundation-release.sh`)가 켜진 작업이 빠진 후보를 거부해 배포가 08:05Z 부터 13:14Z 까지 멈췄다(#1666 이 풀었다). 그래서 넘겨받기 PR 은 그 작업을 간선 없이
꺼 두고(`started_by: schedule`, `per_run_cap.unmeasured_reason`, `host_memory` 는 그대로), 그 릴리스가 운영에 오른 뒤의 PR 이 줄을 지운다.
CI 의 `a-release-keeps-the-jobs-it-switches-off` 가드가 켜진 작업을 지우는 PR 을 막는다.

`parcel-register-masters` 가 넘겨받는다. 판정은 운영의 자산 검사다: `takeover_equal` 이 그림자 머리 `9189577038282651751`(옛 표 빌드
`2986978892865942029` 의 핀)에 대해 통과했다(유닛 run `20261123T133232Z`, 13:44Z; 시도 41 의 127,113 행 모두 같음, 전국 581,357 행·키 해시 합·
내용 해시 합·non-null 수가 같음, 시도의 문서 127,113 개 모두 스키마 통과). 판정받은 머리는 `version_seq` 2 다: 코드 버전이 움직여 모든 PNU 를
다시 지었고 바뀐 키가 0 이었다(전량 길을 머리 위에서 한 번 더 돈 셈이다). 바뀐 키만 짓는 길은 돌지 않았다. 넘겨받은 뒤의 첫 실행이 처음 쓴다.

## Revision 2026-11-23: 셋째 넘겨받기 — 철거·멸실 (결정 6-4, W84c)

`parcel-demolitions` 가 넘겨받는다. 바꾸는 것은 둘째 넘겨받기와 같다(옛 생산 작업은 꺼진 줄로 한 릴리스 남는다). 판정은 운영의 자산 검사다:
`takeover_equal` 이 그림자 머리 `5346468805212475492`(옛 표 빌드 `2185671848266213744` 의 핀)에 대해 통과했다(유닛 run `20261123T141344Z`,
14:27Z; 시도 41 의 47,310 행 모두 같음, 전국 326,999 행·키 해시 합·내용 해시 합·non-null 수가 같음, 시도의 문서 47,310 개 모두 스키마 통과).
판정받은 머리는 `version_seq` 2 다: 코드 버전이 움직여 모든 PNU 를 다시 지었고 바뀐 키가 0 이었다. 바뀐 키만 짓는 길은 돌지 않았다.

## Revision 2026-11-23: 발행 ID 목록과 ID↔코드 연결표의 그림자 (결정 3·6-9, W97)

**무엇.** `gold.place_id_registry` 와 `gold.place_id_bridge`(루트 ADR-0113 §8, ADR-0481)도 레인 밖 판 표다:
`gold.place_id_registry_versions`·`gold.place_id_bridge_versions`, 옛 표의 키((unit, scope, place_id) 와
(unit, scope, place_id, code, valid_from))와 파티션 (unit, scope) 그대로에 파생이 `row_digest`·`version_seq`·`row_state` 를
더한다. 명령 하나(`build-place-id-versions`, 레인 `place-id`)가 두 표를 한 번의 읽기로 짓고 표마다 커밋 하나를 한다.

- **계산.** 필지 unit 은 서빙 에디션까지의 `silver.parcel_registry`(루트 ADR-0490 §4a)를 레이크하우스에서 한 번 읽어 로컬에
  내려 두고, 지역(scope)마다 그 지역의 행만 읽어 `parcel_place_id_release.registry_of`·`bridge_of` 와 같은 규칙으로 짓고 그
  지역의 diff 와 파일을 만든다. 지역은 id 의 첫 행(valid_from 이 이른 것, 없으면 뒤; 다음 recorded_at_utc, pnu)의 시도라 뒤의 행이
  다른 시도여도 바뀌지 않는다 — 그래서 원장은 통째로 읽는다(Spark 는 지역마다 통째로 읽었다). 읍면동 unit 은 경계 스냅숏을
  차례로 읽는 `place_id_release.admin_release` 의 Rust 판이다. 두 unit 의 옛 코드 짝은 모든 독자가 쓰는 하나의 crosswalk
  (`place_identity_stage` 의 `place_remap`·`place_ambiguous`)다.
- **부분.** unit 은 그 입력(필지: 원장·서빙 에디션, 읍면동: 경계)이나 코드 변경표나 코드가 움직였을 때만 다시 짓는다. 입력에 행이 없는
  지역은 diff 하지 않는다: Spark 발행도 읽지 않는 지역을 다시 쓰지 않는다. 빈 발행은 내지 않는다(옛 `release_changes` 의 규칙).
- **그림자의 핀.** Spark 발행은 핀을 적지 않고 지역마다 (unit, scope) 파티션을 덮어쓴다. 그래서 두 표는 `UNTAGGED_BUILDS` 에
  든다: 옛 registry 의 가장 새 발행 커밋 시각의 원장·경계·변경표다. 두 unit 의 발행은 저마다 Spark 칸을 모두 쥐어 같이 돌지
  않으므로, 그 커밋을 쓴 실행이 읽은 입력이 그 시각의 입력이다.
- **판정.** 한 시도(41)의 필지 unit — `scope` 칸이 시도다 — 에서 두 표가 모두 같아야 넘긴다(place_id 는 AI 조인 열쇠다: 그
  시도의 모든 place_id 가 그대로여야 한다). 거짓 `not equal` 은 날 수 있다: 순환이 예산(100분)을 넘겨 41 을 다음 실행으로 미루면
  옛 표의 41 은 그 핀보다 낡았다. 거짓 `equal` 은 날 수 없다: 값이 같다는 것은 값이 같다는 것이다. `release_id` 는 실행의
  스탬프라 digest 도 판정도 읽지 않는다(`BUILD_STAMPS`). 판정 도구가 `scope` 칸을 시도로 읽게 넓히는 PR 이 두 표의
  `asset_checks` 줄을 더한다.
- **풀.** 다른 판 빌드처럼 `duckdb` 풀에서 무게 20, 자기 유닛의 `memory_claim` 이다(root ADR-0605).
- **전환(넘겨받기 PR).** 하나뿐인 독자 `place_id_join` 의 `BRIDGE_SOURCE` 를 이 판 표의 현재 행으로 옮기고, 두 순환
  (`parcel-registry-cycle.sh` 3단계, `admin-boundary-release.sh` 7단계)이 이 명령을 부르며, 변경 내역(`gold.place_id_changelog`)은 판
  표의 diff 에서 낸다. 그때 Spark 작업과 그 시험을 지우고 엔진 장부 줄을 `moved` 로 옮긴다. 그때까지 변경 내역은 Spark 의 것이다:
  사건 표를 두 번 쌓지 않는다.

**기각한 대안.** 순환 안의 단계로 Spark 발행 바로 뒤에 짓기(같은 입력이 확실하지만 그림자가 Spark 유닛 안에 묶여 따로 서지 않고, 전환
때 다시 옮겨야 한다). 지역마다 원장을 시도 접두로 걸러 읽기(시도를 옮긴 id 의 행을 놓친다). 창 함수로 지역 구하기(4천만 행을
메모리에서 정렬한다 — 집계는 디스크로 흘린다).
## Revision 2026-11-24: 넷째 넘겨받기 — 등록공장 (결정 6-4, W96)

`parcel-factories` 가 넘겨받는다. 바꾸는 것은 둘째 넘겨받기와 같다(옛 생산 작업은 꺼진 줄로 한 릴리스 남는다). 판정은 운영의 자산 검사다:
`takeover_equal` 이 그림자 머리 `1873237728151999863`(옛 표 빌드 `7069980837781741781` 의 핀)에 대해 통과했다(유닛 run `20261123T135402Z`,
14:12Z; 시도 41 의 36,658 행 모두 같음, 전국 123,451 행·키 해시 합·내용 해시 합·non-null 수가 같음, 시도의 문서 36,658 개 모두 스키마 통과).
판정받은 머리는 `version_seq` 1 이다: 첫 빌드라 모든 PNU 를 지었다. 바뀐 키만 짓는 길은 돌지 않았다. 넘겨받은 뒤의 첫 실행이 처음 쓴다.
