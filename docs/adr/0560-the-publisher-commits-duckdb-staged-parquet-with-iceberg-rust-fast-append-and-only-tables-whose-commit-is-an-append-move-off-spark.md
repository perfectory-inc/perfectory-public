# ADR 0560: 발행기는 DuckDB 가 준비한 Parquet 을 iceberg-rust 의 추가 커밋으로 붙인다 — 커밋이 추가인 표만 Spark 를 떠난다

- Status: Accepted
- Date: 2026-11-12
- Builds on: [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)
  §4 의 "둘째 단계(커밋도 옮긴다)" — 거기서 "아직 증명되지 않았다"고 적은 탐침을 하고 그 결과로 길을 정한다.
  [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md)(적재 가지),
  [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)(행 판),
  [ADR-0338](./0338-a-reader-of-an-old-snapshot-pins-it-with-its-own-tag-and-the-rest-expire-in-30-days.md)(독자의 핀),
  [ADR-0344](./0344-gold-merges-write-only-the-changed-rows-as-merge-on-read-deletes-that-compaction-folds.md)(위치 삭제)
- 결정한 사람: 사장의 2026-11-12 역할 배정(DuckDB 가 주력, Spark 는 표 유지보수만)을 따르는 구현 결정이다.

## Context

ADR-0559 가 남긴 가장 큰 미지수는 커밋이었다. Rust 발행기는 Iceberg 를 읽고 태그와 가지를 옮기지만 데이터 스냅샷을 쓴 적이 없고,
DuckDB 는 우리 스냅샷 요약 속성과 태그를 쓰지 못한다. 그래서 계산을 DuckDB 로 옮겨도 커밋 때문에 Spark 세션이 남는다.

**먼저 있는 것을 찾았다**(스캔: 저장소 전체에서 `add-snapshot`·`ManifestWriter`·`fast_append`·`pyiceberg`, 그리고
`platforms/foundation-platform` 의 `*.rs`·`Cargo.toml`·`infra/lakehouse/spark/jobs/*.py` 에서 `iceberg`).
카탈로그 커밋은 `lakehouse-infrastructure/src/iceberg_rest_catalog.rs` 의 `commit_table` 하나이고 그 위에 `publish_branch`·
`create_tag`·`remove_tag`·`remove_stale_branch` 가 있다. 참조(ref)를 옮기고, `publish_branch` 는 판을 표 속성으로도 적는다. 데이터 스냅샷은 만들지 않는다. 매니페스트를 쓰는 코드는 없다. 읽는 쪽은
`foundation-publisher-core/src/lakehouse_snapshot_scan`(매니페스트 Avro 해독, 범위 읽기, 위치 삭제 적용)이다.
매니페스트 쓰기를 손으로 짜는 것은 성숙한 데이터 플레인을 다시 만드는 일이므로(AGENTS.md §해결 접근 순서 4·5) 라이브러리를 본다.

**후보: apache/iceberg-rust 0.11.0**(2026-11-03, Apache-2.0, rust 1.95 이상 — 우리 툴체인 1.99.1). 태그 `v0.11.0` 의 소스를 읽어 확인한 것:

| 무엇 | 0.11.0 에서 |
|---|---|
| 추가 커밋에 요약 속성 | `Transaction::fast_append().set_snapshot_properties(..)` — 있다 |
| 커밋이 가는 참조 | `main` 고정이다(`transaction/snapshot.rs` 가 `MAIN_BRANCH` 로 `SetSnapshotRef` 와 `RefSnapshotIdMatch` 를 만든다). 가지로 커밋하는 길이 없다 |
| 직접 커밋 본문 만들기 | 막혀 있다(`TableCommit` 의 builder 가 `pub(crate)`) |
| 남이 쓴 Parquet 을 등록 | 공개 함수가 없다(`ParquetWriter::parquet_to_data_file_builder` 가 `pub(crate)`). `DataFileBuilder` 는 공개다 |
| 충돌 | 커밋 전에 표를 다시 읽고, 기준이 바뀌었으면 그 위에 동작을 다시 적용한다. 표 속성 `commit.retry.*` 로 재시도 |
| 행 단위 삭제(`RowDelta`), 덮어쓰기 | 없다(추가·만료·속성·스키마·통계뿐) |
| 저장소 | `iceberg-storage-opendal`(`opendal-s3`) 을 카탈로그 builder 에 넘긴다 |
| 의존성 | arrow·parquet `58.4` — 워크스페이스는 `60` 이다. 두 판이 함께 지어진다 |

**탐침(2026-11-12, 운영 R2 카탈로그의 시험 네임스페이스 `w75b_commit_probe`; 운영 표는 읽지도 쓰지도 않았다).**
일회용 Rust 실행 파일(저장소에 넣지 않았다)이 iceberg-rust 로 커밋하고, **우리 코드 그대로**(`IcebergRestCatalog`,
`lakehouse_snapshot_scan::scan_snapshot_rows`·`scan_snapshot_rows_kept`·`scan_snapshot_inventory`)로 되읽었다. 시험 표 둘은 실제 계약의
칸과 파티션 칸을 그대로 가졌다: `gold.parcel_panel`(16 칸, `source_snapshot_id` identity 파티션)과
`gold.real_transaction_region_stats`(`dataset_slug` identity 파티션). 행은 합성(PNU·지역 코드 `99999…`)이다. 파일은 DuckDB 2.0.0 이
`COPY … (FORMAT parquet, FIELD_IDS {…})` 로 표의 `data/` 아래에 직접 썼다. ai-server 에서 메모리 2 GiB·CPU 2 개 컨테이너로 돌렸다.

| 무엇 | 결과 |
|---|---|
| DuckDB 가 쓴 Parquet 2 개(파티션 2 개, 1,500 행)를 한 번의 추가 커밋으로 | 됨. `operation=append`, 꼬리표 읽기 0.6 초·전체 2.2~3.3 초 |
| 요약 속성(`foundation.content-fingerprint`·`foundation.source-iceberg-snapshots`·`foundation.keys-changed`·`foundation.build-source-sha`·`foundation.ingest-batch-objects`) | 실림. `IcebergSnapshotSummary::source_snapshots`·`added_records`·`ingested_objects`, `load_ingested_batch_objects` 가 그대로 읽는다. Trino `$snapshots`, DuckDB `iceberg_snapshots` 도 같은 값을 본다 |
| 발행기 스캔으로 되읽기 | 1,500 행 전부, `ensure_complete` 통과(파일 2, 범위 요청 4) |
| 매니페스트 경계로 파일 건너뛰기(`KeptPrefix::pnu`) | 됨. 접두 하나에 파일 1 개(500 행)를 열지 않았다 — 꼬리표에서 매니페스트로 옮긴 경계가 맞다. 연 파일에서는 990 행을 해독하지 않았다(이것은 DuckDB 가 쓴 Parquet 통계와 접두 거르기의 몫이다) |
| 서빙 핀·월말 태그(`create_tag`) | 그 스냅샷에 태그 둘. 그 뒤 추가 커밋이 `main` 을 옮겨도 태그의 스냅샷을 고정해 읽으면 1,500 행 그대로다 |
| 같은 기준에서 출발한 추가 커밋 둘 + 발행기의 태그 커밋 하나를 동시에 | 셋 다 들어감(4.7 초). 진 쪽 추가는 이긴 쪽 위에 다시 적용됐다(1,850 → 2,050 행). 잃은 행 없음 |
| Spark 의 증분 추가 읽기(`gold_incremental.appended_span`·`read_appended`, 운영 jar) | `appended_span=True`, 발행기가 붙인 세 커밋의 550 행을 읽었다 |
| 지금의 Spark 커밋 길과 번갈아 | 발행기 추가 위에 Spark `MERGE`(merge-on-read, `with_commit_properties`) → 위치 삭제 5 건의 `overwrite` → 그 위에 다시 발행기 추가. 삭제 매니페스트가 이어지고 발행기 스캔은 2,110 행(매니페스트 2,115 − 삭제 5), Trino 도 2,110 행 |
| DuckDB·Trino 로 읽기 | 모든 스냅샷에서 같은 행 수(`AT (VERSION => …)`, `FOR VERSION AS OF`), 태그 넷 다 보인다 |
| 필드 id 없는 Parquet | 탐침이 거부했다(등록하지 않는다). id 가 없으면 다른 엔진이 칸을 이름 대응표 없이는 못 읽는다 |
| 파티션 값이 둘인 파일 | 탐침이 거부했다(파티션 칸의 최소와 최대가 다르다) |
| 이미 표가 가진 파일을 다시 붙이기 | iceberg-rust 가 거부했다(`Cannot add files that are already referenced`). 같은 파일은 두 번 붙지 않는다. 이름이 다른 같은 행은 이 검사가 막지 못한다 |

탐침에서 따로 드러난 것 둘:

- 발행기 스캔은 `double` 칸을 해독하지 못한다(`iceberg_scan::decode_value`: string·long·date·timestamp·decimal 만).
  `gold.real_transaction_region_stats` 는 Rust 가 읽은 적이 없는 표라 지금까지 드러나지 않았다. 재고(`scan_snapshot_inventory`)는 읽는다.
- 그 표에 대한 커밋·태그·DuckDB/Trino 읽기는 위와 같이 통과했다.

## Decision

1. **발행기의 Iceberg 데이터 커밋은 apache/iceberg-rust 의 추가 커밋(`fast_append`)이다.** 매니페스트·매니페스트 목록·커밋 본문·충돌 재적용은
   라이브러리가 한다. 우리 코드는 어댑터뿐이다: 준비된 Parquet 의 꼬리표에서 매니페스트 항목을 만들고 요약 속성을 싣는다.
   - 이것은 카탈로그 클라이언트·매니페스트 코덱·R2 클라이언트가 하나씩 더 생긴다는 뜻이다(`iceberg_rest_catalog.rs`,
     `lakehouse_snapshot_scan`, `foundation-outbox` 의 R2 옆에). 받아들이되 **쓰기에만** 쓴다: 읽기와 태그·가지 옮기기는 지금 코드 그대로이고,
     라이브러리의 스캔·카탈로그 읽기를 운영 코드가 부르지 않는다.
   - 데이터 커밋의 재시도는 라이브러리의 것이다(표 속성 `commit.retry.*`, 기준이 바뀌면 그 위에 다시 적용). 재시도가 바닥나면 그 run 은
     실패이고 아무것도 보이지 않는다. 응답을 잃은 커밋은 지금의 커밋들처럼 표를 다시 읽어 판정한다(3 번의 기록).
   - 라이브러리가 "남이 쓴 Parquet 등록"을 공개하면 어댑터의 그 부분을 지운다.
2. **준비 파일.** DuckDB 는 표 위치의 `data/` 아래에 Parquet 을 쓰고 커밋하지 않는다(이 위치는 이 ADR 이 정한다: 표의 고아 정리가 보는
   곳이어야 한다). 파일 이름은 묶음에서 결정적으로 나온다 — 같은 묶음을 다시 준비하면 같은 이름을 덮어쓴다. 그래서 죽은 run 이 남기는
   파일은 레인마다 묶음 하나치를 넘지 않는다. 어댑터는 다음이 아니면 붙이지 않는다(run 실패):
   - 칸의 집합·필드 id·이름·형·필수 여부가 표의 현재 스키마와 같다(`FIELD_IDS`).
   - identity 파티션 칸은 파일 하나에 값이 하나이고 null 이 아니다. 그 값이 파티션 값이고 파티션 명세 id 는 표의 현재 것이다.
     identity 가 아닌 변환(`truncate`·`bucket`)의 표는 어댑터가 그 변환을 계산하게 된 뒤에 옮긴다
     (`silver.real_transaction` 의 `["dataset_slug", "truncate(4, deal_ym)"]` 이 그 경우다).
   - 표 계약이 정렬 칸으로 든 칸(발행기 스캔이 파일을 건너뛰는 칸)은 꼬리표에 최소·최대·null 수가 있어야 한다. 없으면 붙이지 않는다.
   - 준비된 지 `orphan_cleanup.safety_days`(3 일)보다 하루 이상 짧은 파일만 붙인다. 더 묵은 파일은 다시 준비한다 — 고아 정리가 지울 수 있는
     파일을 스냅샷이 가리키게 하지 않는다.
3. **요약 속성은 호출한 쪽이 주는 그대로 싣고, 이름은 지금 쓰는 것뿐이다.** 정본은 지금 그 속성을 쓰는 코드다
   (`lakehouse_ingest.py` 의 `foundation.ingest-batch-objects`·`foundation.ingest-batch-token`·`foundation.edition`,
   `gold_incremental.py`·`gold_rebuild.py` 의 `foundation.content-fingerprint`·`foundation.source-iceberg-snapshots`·`foundation.keys-changed`).
   옮기는 PR 은 그 작업이 Spark 에서 싣던 속성 전부를 같은 값 형식으로 싣고, 대조(`engine_equality --no-digest`)에 요약 속성을 넣는다.
   - **재실행 가드는 파일 경로가 아니라 지금의 것이다.** 어댑터는 붙이기 전에 `main` 의 `foundation.ingest-batch-objects` 를 읽어
     (`load_ingested_batch_objects`) 이미 실린 묶음을 붙이지 않는다. 라이브러리의 "이미 가리키는 파일" 거부는 같은 파일에만 듣는다 —
     이름이 다른 같은 행, 합치기 뒤의 옛 경로는 막지 못한다.
   - 판 기록: `publish_branch` 는 `foundation.edition` 을 표 속성 `foundation.edition.<판>` 으로도 옮겨 적는다(요약은 30 일 뒤 만료된다,
     ADR-0328). 가지 없이 붙이는 길은 같은 트랜잭션에서 그 표 속성을 적어야 한다. 라이브러리에 속성 갱신 동작이 있으나 추가와 한
     트랜잭션에 묶이는지는 **재지 않았다**. 판을 싣는 표는 그것이 확인된 뒤에 옮긴다.
4. **한 run 이 한 번에 붙이는 적재는 가지가 필요 없다.** 준비 파일은 커밋 전에는 어떤 스냅샷도 가리키지 않으므로, 한 run 이 준비한 묶음
   전부를 한 번의 추가 커밋으로 붙이면 "전부 보이거나 하나도 안 보인다"(ADR-0183, ADR-0389)가 가지 없이 성립한다.
   **여러 run 에 걸쳐 이어 싣는 적재, 가지에서 현재 행과 비교한 뒤 싣는 적재(ADR-0318), 묶음마다 자기 커밋을 되읽는 적재(ADR-0391)는
   지금의 가지 그대로다** — 이 탐침은 그 모양을 재지 않았다. 가지로 커밋하는 길을 손으로 만들지 않는다.
5. **장부에서 빠지는 조건.** 줄이 `moved` 가 되려면 그 파일이 Spark 세션을 열지 않아야 하고(ADR-0559 §4), 그러려면 그 파일이 쓰는 표마다
   계약이 허락하는 쓰기가 **전부 추가**여야 한다(`platform_contracts.admit_write` 의 `append` 만; `replace` 를 허락받는 표,
   `overwritePartitions` 로 첫 묶음을 쓰는 적재, `MERGE` 는 아니다).
   - `MERGE` 로 갈아 끼우는 Gold(`gold_incremental.build_or_merge` 의 생산자 — 첫 물결의 실거래 Gold 와 주변 Gold 포함)는 계산만 옮길 수 있다
     (ADR-0559 §4 첫 단계). DuckDB 의 `MERGE` 로 대신하지 않는다(요약 속성을 못 쓴다).
   - `__changes` 표는 Gold 생산자의 세션 안에서 그 커밋의 머리와 부모로 쓰인다(`record_changes`). 따로 옮기지 않고 생산자와 함께 옮긴다.
   - 장부 57 줄 가운데 몇 줄이 이 조건에 드는지는 **세지 않았다**(파일 하나가 여러 레인의 여러 쓰기를 한다). 첫 표를 고르는 PR 이 센다.
   - 물결(`engines.waves`)은 바꾸지 않는다. 물결은 계산을 옮기는 순서이고, `moved` 는 그 표의 커밋까지 옮겨진 때다.
6. **동시성.** 커밋은 낙관적이다. 표마다 쓰는 작업은 지금처럼 하나다(작업 계약과 유닛). 태그 커밋, Spark 의 `MERGE` 커밋과 엇갈리는 것은
   탐침에서 들어갔다. 유지보수(`rewrite_data_files`·`expire_snapshots`)와 겹치는 것은 재지 않았고, 첫 표를 옮기기 전에 시험 표에서 잰다.
7. **커밋 안 된 준비 파일.** 지금 예약된 유지보수는 고아 정리를 돌리지 않는다(`lakehouse-maintenance.contract.json` 의
   `scheduled.skip: ["orphan_cleanup"]` — 서버에서 R2 를 상대로 돌려 본 적이 없어 운영자의 단계로 남아 있다). 그러므로 이 길은 고아 정리에
   기대지 않는다: 2 번의 결정적 이름이 남는 양을 묶음 하나치로 묶고, 다음 run 이 그것을 덮어쓰거나 붙인다. 고아 정리를 따로 만들지 않는다.
8. **순서.** ① DuckDB 를 2.0.0 으로(#1363 — `FIELD_IDS` 를 탐침과 같은 판으로). ② `iceberg`·`iceberg-catalog-rest`·
   `iceberg-storage-opendal`(`opendal-s3` 만) `=0.11.0` 과 어댑터, CI 의 실제 Iceberg 에서 도는 시험(준비 → 붙이기 → 발행기 스캔 되읽기 →
   거부 넷), `docs/technology-stack.md` 의 줄, arrow·parquet 58.4 가 60 옆에 지어지는 시간을 CI 시간 예산(ADR-0203) 안에서 잰 값.
   ③ 첫 표: 5 번에 드는, 파티션이 없거나 identity 이고, 판을 싣지 않고, 발행기 스캔이 모든 칸을 해독하는 Silver 추가 적재 하나.
   그 PR 이 전국·전체 작업의 판정, 되돌리는 절차(나쁜 추가 뒤 `main` 을 부모 스냅샷으로 — 지금 `iceberg_rest_catalog.rs` 에 그 동작이 없다),
   큰 표에서의 커밋 시간(중복 검사가 매니페스트를 전부 읽는다)을 싣는다. ②와 ③을 한 PR 로 하지 않는다.
   지금 의존성을 넣지 않는 것은 그것을 부르는 운영 코드가 아직 없기 때문이다(AGENTS.md §계층을 넓게 쌓기 전에).

## 확장의 판 (재 둔 것, 계약에는 그것을 읽는 PR 이 적는다)

DuckDB 2.0.0(`d54cbbe3df`) 이 `linux_amd64` 로 내려받은 확장 파일이다(2026-11-12, ai-server). 릴리스 산출물에 다이제스트로 싣고 경로로
`LOAD` 하는 것은 ADR-0559 §6 대로 그것을 읽는 첫 변환 PR 이 한다.

| 확장 | 판 | 바이트 | sha256 |
|---|---|---:|---|
| httpfs | `77392a7207` | 26,929,718 | `77e6c429aa04f25da7962572f12b19113e0ce46080fa1365ad66017aadcdb68d` |
| iceberg | `4c0c0874c4` | 57,088,302 | `b93676e544e5c6843915fb3392e3b3c35859c065fa6ace7a26cb46ca4394e56d` |
| avro | `345c3f4e82` | 28,342,942 | `3f37797ca9a4ef3b5a07e65278c653fe0a32f7edea1498c801022fed49afedc7` |
| spatial | `a3dbcd1781` | 108,189,118 | `8ec99d875355200316c743ede176e2cb50d2b712d2fa192ccbf19cc245a8d0bc` |
| h3 (커뮤니티) | `v1.5.6` | 520,766 | `abba3f5702e6917804a1e05be20013b51d2174e296b175f0a3fdfc2eeffd6352` |

## 첫 물결이 이 길로 가지 못하는 이유

| 작업 | 이유 | 풀리는 조건 |
|---|---|---|
| `real_transaction_silver_to_gold` | 커밋이 `MERGE` 다(5 번). 문서 조립과 지역 통계는 아직 옮겨 대조하지 않았다 | 계산 이식의 전국 판정(ADR-0559 §5); 장부에서 빠지려면 Gold 의 커밋 모양 결정 |
| `rt_molit_real_transaction` | 추가 적재지만 파티션이 `truncate` 다(2 번) | 어댑터의 `truncate` 계산, 전국 판정 |
| `parcel_nearby_silver_to_gold` | 시설 Silver 여섯에 스냅샷이 없어 어떤 엔진으로도 만들 수 없다. 커밋도 `MERGE` 다 | 시설 Silver 의 첫 적재. 그 뒤 DuckDB 의 정확한 최근접으로 첫 발행(ADR-0559 예외 ⓑ: 경기도 필지의 0.78%·40,467 행에서 Sedona `ST_KNN` 과 다르다 — 시설 종류마다 DuckDB 가 더 가까운 경우 6,955~8,527 행, 미터로 반올림하면 같은 거리의 다른 시설 40~1,565 행, Spark 가 더 가까운 경우 0, 최대 차이 214 m) |

HUB 계열 레인 작업(`silver_refresh_hub_lane_family`, ADR-0551)은 새 Spark 파일을 더하지 않았다. 그 22 개 레인은 이미 장부에 있는
`silver_scalar_handoff_to_lakehouse`·`silver_release_compare`(물결 4)의 세션으로 돌고, 그 두 줄과 함께 옮겨진다. 장부는 파일의 장부이므로
줄을 더하지 않는다(어느 작업이 Spark 풀을 쓰는지는 `orchestration/jobs.v1.json` 의 `pool` 이 말한다).

## Consequences

- 커밋 길은 **시험 크기에서, identity 파티션의 추가에 대해** 증명됐다. 운영 표로 가는 조건은 8 번의 ②③이다.
- Gold 생산자는 계산을 옮겨도 장부에서 빠지지 않는다. Spark 풀의 큰 슬롯은 비지만 세션은 남는다. "57 → 1" 로 가려면 Gold 의 커밋 모양
  (행 판을 쌓는 추가, 또는 iceberg-rust 의 행 단위 삭제)을 따로 결정해야 한다. 둘째 물결의 계산 이식이 끝나기 전에 사장과 조정자가 정한다.
- 탐침의 꼬리표 읽기는 파일을 통째로 받았다(수십 KB). 운영 어댑터는 꼬리표만 범위로 읽어야 한다. 발행기의 범위 읽기
  (`lakehouse_snapshot_scan/ranged_parquet`)는 비공개이고 파일 길이를 매니페스트에서 받으므로, 길이를 객체에서 얻는 길과 함께 열어야 한다.
- 발행기 스캔이 `double` 을 못 읽는다. 첫 표의 조건(8 번 ③)에 넣었고, 그 칸의 표를 Rust 로 읽게 되는 PR 이 고친다.

## 하지 않은 것, 확인하지 못한 것

- 운영 표에 대한 커밋. 탐침은 시험 표뿐이고 파일 1~2 개·수십 KB 다. 큰 파일·많은 파일의 커밋 시간은 모른다.
- 추가와 표 속성 갱신을 한 트랜잭션에 묶는 것(3 번), 파일이 0 개인 추가, `truncate`·`bucket` 파티션 값, 정렬 순서 id, 중첩 칸.
- 유지보수(합치기·만료)와 겹치는 커밋, 고아 정리와 준비 파일의 관계(고아 정리가 R2 에서 돈 적이 없다).
- 여러 run 에 걸친 적재·비교 적재·가지 발행과 겹치는 경우(4 번: 그 적재들은 옮기지 않는다).
- **R2 관리형 유지보수**(사장 승인 2026-11-12: 시험 표 하나에서만). 합치기는 표 단위로 켜졌다:
  `wrangler r2 bucket catalog compaction enable foundation-platform-lakehouse-prod w75b_commit_probe parcel_panel --target-size 64`.
  만료는 표 하나에만 켤 수 없었다(`… snapshot-expiration enable … --older-than-days 1 --retain-last 1` →
  `catalog-level snapshot expiration must be enabled first [code: 40013]`). 카탈로그 전체에 켜는 것은 승인 범위 밖이라 하지 않았다.
  그래서 태그와 핀이 관리형 만료에서 살아남는지는 이 카탈로그에서 재지 못했다 — 재려면 따로 만든 시험 버킷의 카탈로그가 필요하고
  그것은 사장의 결정이다. 합치기가 만든 `replace` 스냅샷을 독자들이 읽는지도 아직 모른다(켠 뒤 합치기가 돌지 않았다).
  시험 표와 그 합치기는 켜 둔 채 남긴다. `replace` 스냅샷이 생겨 독자들을 대조한 날이나 2026-11-19 가운데 이른 날에 끄고
  (`wrangler r2 bucket catalog compaction disable foundation-platform-lakehouse-prod w75b_commit_probe parcel_panel`) 표와 네임스페이스를
  지운다. 값: Cloudflare 문서에 관리형 유지보수의 값이 적혀 있지 않다(2026-11-12). 시험 표는 파일 7 개·80 KB 다.
  만료와 고아 정리는 Spark 의 `lakehouse_maintenance.py` 에 남는다(ADR-0559 §9).
- 설계 검토: EGM 검토자 넷(설계·코드 대조·구현 준비·범위)이 한 번 검토했다. 차단 3·주요 11·경미 9 가운데 이 문서에서 고친 것:
  고아 정리가 예약돼 있지 않다는 사실(7 번을 다시 썼다), 가지를 없앤다는 결정의 범위(4 번을 한 run 의 적재로 좁혔다), 판 기록(3 번),
  재실행 가드(3 번), 장부에서 빠지는 조건(5 번), `__changes`, 3 일 창(2 번), 겹치는 클라이언트(1 번), 물결과 계약의 어긋남(문장을 지웠다),
  준비 파일의 규칙(2 번), 순서·첫 표·되돌리기(8 번), 속성 목록(3 번), 대안 둘, 탐침 표의 잘못 읽은 줄. 남긴 것: 장부 57 줄의 분류와 첫 표의
  이름(세지 않았다 — 8 번 ③의 PR), iceberg-rust 소스 줄의 영구 링크.

## 기각한 대안

- **계산만 옮기고 커밋은 Spark 에 둔다(ADR-0559 §4 첫 단계에서 멈춘다).** 새 의존성이 없다. 그러나 추가 적재마다 Spark 세션이 남아 장부가
  줄지 않고, 사장이 정한 "Spark 는 표 유지보수만"에 닿지 못한다. 추가인 표는 이 ADR 의 길로, `MERGE` 인 표는 그 단계에 머문다.
- **매니페스트와 커밋 본문을 손으로 쓴다**(지금의 `commit_table` 위에). 가지로도 커밋할 수 있게 되지만 Avro 매니페스트·순번·요약 집계를
  다시 만드는 일이다.
- **DuckDB 가 커밋한다(추가든 `MERGE` 든).** 요약 속성과 태그를 못 쓴다(ADR-0559 의 표). 재실행 가드·입력 핀·내용 지문을 읽는 독자들이
  그 속성에 기대므로, Gold 의 `MERGE` 를 DuckDB 로 대신하면 그 독자들을 전부 다른 기록으로 옮겨야 한다.
- **DuckDB 가 쓴 파일을 iceberg-rust 의 Parquet writer 로 다시 쓴다.** 등록 함수가 공개가 아니라는 이유만으로 모든 행을 한 번 더 쓴다.
- **pyiceberg 로 커밋한다.** 발행기 밖에 Python 실행기를 하나 더 둔다. 독자와 카탈로그 클라이언트가 이미 Rust 다.
