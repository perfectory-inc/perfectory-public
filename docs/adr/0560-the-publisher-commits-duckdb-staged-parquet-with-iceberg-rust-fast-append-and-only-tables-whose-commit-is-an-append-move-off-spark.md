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

**먼저 있는 것을 찾았다**(스캔: `platforms/foundation-platform` 의 `*.rs`·`Cargo.toml`, `infra/lakehouse/spark/jobs/*.py`).
카탈로그 커밋은 `lakehouse-infrastructure/src/iceberg_rest_catalog.rs` 의 `commit_table` 하나이고 그 위에 `publish_branch`·
`create_tag`·`remove_tag`·`remove_stale_branch` 가 있다. 전부 참조(ref)만 옮긴다. 매니페스트를 쓰는 코드는 없다. 읽는 쪽은
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
| 매니페스트 경계로 파일 건너뛰기(`KeptPrefix::pnu`) | 됨. 접두 하나에 파일 1 개(500 행)를 열지 않고, 연 파일에서도 행 묶음 990 행을 건너뛰었다 — 꼬리표에서 옮긴 경계가 맞다 |
| 서빙 핀·월말 태그(`create_tag`) | 그 스냅샷에 태그 둘. 그 뒤 추가 커밋이 `main` 을 옮겨도 태그의 스냅샷을 고정해 읽으면 1,500 행 그대로다 |
| 같은 기준에서 출발한 추가 커밋 둘 + 발행기의 태그 커밋 하나를 동시에 | 셋 다 들어감(4.7 초). 진 쪽 추가는 이긴 쪽 위에 다시 적용됐다(1,850 → 2,050 행). 잃은 행 없음 |
| Spark 의 증분 추가 읽기(`gold_incremental.appended_span`·`read_appended`, 운영 jar) | `appended_span=True`, 발행기가 붙인 세 커밋의 550 행을 읽었다 |
| 지금의 Spark 커밋 길과 번갈아 | 발행기 추가 위에 Spark `MERGE`(merge-on-read, `with_commit_properties`) → 위치 삭제 5 건의 `overwrite` → 그 위에 다시 발행기 추가. 삭제 매니페스트가 이어지고 발행기 스캔은 2,110 행(매니페스트 2,115 − 삭제 5), Trino 도 2,110 행 |
| DuckDB·Trino 로 읽기 | 모든 스냅샷에서 같은 행 수(`AT (VERSION => …)`, `FOR VERSION AS OF`), 태그 넷 다 보인다 |
| 필드 id 없는 Parquet | 탐침이 거부했다(등록하지 않는다). id 가 없으면 다른 엔진이 칸을 이름 대응표 없이는 못 읽는다 |
| 파티션 값이 둘인 파일 | 탐침이 거부했다(파티션 칸의 최소와 최대가 다르다) |
| 이미 표가 가진 파일을 다시 붙이기 | iceberg-rust 가 거부했다(`Cannot add files that are already referenced`). 재실행이 행을 두 번 넣지 않는다 |

탐침에서 따로 드러난 것 둘:

- 발행기 스캔은 `double` 칸을 해독하지 못한다(`iceberg_scan::decode_value`: string·long·date·timestamp·decimal 만).
  `gold.real_transaction_region_stats` 는 Rust 가 읽은 적이 없는 표라 지금까지 드러나지 않았다. 재고(`scan_snapshot_inventory`)는 읽는다.
- 그 표에 대한 커밋·태그·DuckDB/Trino 읽기는 위와 같이 통과했다.

**R2 관리형 유지보수(사장 승인 2026-11-12: 시험 표 하나에서만).**

- 합치기는 표 단위로 켜진다: `wrangler r2 bucket catalog compaction enable foundation-platform-lakehouse-prod w75b_commit_probe parcel_panel --target-size 64` → 성공.
- 만료는 표 단위로만 켤 수 없다: `wrangler r2 bucket catalog snapshot-expiration enable foundation-platform-lakehouse-prod w75b_commit_probe parcel_panel --older-than-days 1 --retain-last 1`
  → `Cannot enable table snapshot expiration - catalog-level snapshot expiration must be enabled first [code: 40013]`.
  카탈로그 전체에 켜는 것은 승인 범위 밖이라 하지 않았다. 따라서 "태그와 핀이 관리형 만료에서 살아남는가"는 **이 카탈로그에서는 재지 못했다.**

## Decision

1. **발행기의 Iceberg 데이터 커밋은 apache/iceberg-rust 의 추가 커밋(`fast_append`)이다.** 매니페스트·매니페스트 목록·커밋 본문·충돌 재적용은
   라이브러리가 한다. 우리 코드는 얇은 어댑터뿐이다: 준비된 Parquet 의 꼬리표에서 매니페스트 항목(행 수, 크기, 칸별 값·null 수, 경계,
   파티션 값)을 만들고, 요약 속성을 싣는다. 태그·가지 옮기기는 지금의 `iceberg_rest_catalog.rs` 그대로다(라이브러리에 그 동작이 없다).
2. **준비 파일의 규칙.** DuckDB 는 표의 `data/` 아래에 Parquet 을 쓰고 커밋하지 않는다(ADR-0559 §4). 발행기는 다음을 지키지 않는 파일을
   붙이지 않는다:
   - 모든 칸이 표 스키마의 Iceberg 필드 id 를 갖고(`FIELD_IDS`), id 의 이름이 표와 같다.
   - identity 파티션 칸은 파일 하나에 값이 하나다. 그 값이 파티션 값이다. identity 가 아닌 변환(`truncate`·`bucket`)의 표는 그 변환을
     어댑터가 계산할 수 있게 된 뒤에 옮긴다(`silver.real_transaction` 의 `truncate(4, deal_ym)` 이 그 경우다).
   - 경계는 꼬리표의 통계가 정확할 때만 옮긴다(잘린 통계는 옮기지 않는다 — 발행기 스캔이 경계로 파일을 건너뛴다).
3. **요약 속성은 지금 독자들이 읽는 이름 그대로다**: `foundation.content-fingerprint`, `foundation.source-iceberg-snapshots`,
   `foundation.keys-changed`, `foundation.build-source-sha`, `foundation.ingest-batch-objects`, `foundation.edition`.
   새 이름을 만들지 않는다.
4. **적재 가지는 추가 커밋에 필요 없다.** ADR-0183 의 가지는 "여러 묶음이 한 번에 보이거나 하나도 안 보인다"를 위해 있다. 준비 파일은
   커밋 전에는 어떤 스냅샷도 가리키지 않으므로, 여러 묶음의 파일을 **한 번의 추가 커밋**으로 붙이면 같은 보장을 가지 없이 얻는다.
   가지로 커밋하는 길을 손으로 만들지 않는다. 이 길로 옮긴 표의 재개는 가지의 스냅샷이 아니라 준비된 파일 목록에서 한다
   (그 기록을 어디에 두는지는 첫 표를 옮기는 PR 이 기존 재개 기록 — `foundation.ingest-batch-objects`, Silver 새로 고침의 상태 디렉터리 —
   위에서 정한다).
5. **커밋이 추가인 표만 Spark 를 떠난다.** 장부(`engines.spark_sessions`)의 줄이 `moved` 가 되려면 그 파일이 Spark 세션을 열지 않아야 하고,
   그러려면 그 표의 커밋이 추가여야 한다.
   - 추가로 쌓는 Silver 적재와 `__changes` 표: 이 길로 옮긴다.
   - `MERGE` 로 갈아 끼우는 Gold(`gold_incremental.build_or_merge` 를 쓰는 생산자 전부 — 첫 물결의 실거래 Gold 와 주변 Gold 포함)는
     계산만 옮길 수 있다(ADR-0559 §4 첫 단계). 장부에서 빠지려면 그 표가 행 판을 쌓는 모양이 되거나(ADR-0286),
     iceberg-rust 가 행 단위 삭제 커밋을 갖게 되어야 한다. DuckDB 의 `MERGE` 로 대신하지 않는다(요약 속성을 못 쓴다).
   - 따라서 ADR-0559 §5 의 물결 순서는 "무엇이 먼저 죽는가"의 순서이고, **장부의 수가 실제로 줄어드는 순서는 Silver 추가가 먼저**다.
6. **동시성.** 커밋은 낙관적이다: 라이브러리가 기준(`main` 의 스냅샷)을 요구 조건으로 걸고, 지면 새 기준 위에 다시 적용한다.
   표마다 생산자는 지금처럼 하나다(재빌드 잠금, 작업 계약). 태그 커밋과 겹치는 것은 탐침에서 둘 다 들어갔다.
7. **커밋 안 된 준비 파일은 고아다.** 작업이 죽으면 준비 파일은 어떤 스냅샷에도 없는 채로 남고, 지금의 고아 정리
   (`lakehouse_maintenance.py`, `orphan_cleanup.safety_days` 3 일)가 지운다. 그래서 준비에서 커밋까지는 3 일 안이어야 한다 —
   가장 긴 작업의 시간제한(수 시간)보다 훨씬 길다. 고아 정리를 따로 만들지 않는다.
8. **의존성.** `iceberg`·`iceberg-catalog-rest`·`iceberg-storage-opendal`(`opendal-s3` 만) `=0.11.0` 을 워크스페이스에 넣는 것은
   첫 표를 옮기는 PR 이 한다. 그 PR 이 arrow·parquet 58.4 가 60 옆에 함께 지어지는 비용을 CI 시간 예산(ADR-0203) 안에서 재서 적는다.
   지금 넣지 않는 것은 그것을 부르는 운영 코드가 아직 없기 때문이다(AGENTS.md §계층을 넓게 쌓기 전에).
9. **R2 관리형 유지보수.** 합치기는 시험 표 하나에 켜 두고 본다(아래). 만료는 이 카탈로그에서 표 하나에만 켤 수 없으므로,
   시험하려면 카탈로그 전체에 켜거나(운영 표 전부가 대상이 된다 — 하지 않는다) 따로 만든 시험 버킷의 카탈로그에서 해야 한다.
   어느 쪽이든 사장의 결정이다. 그때까지 만료와 고아 정리는 Spark 의 `lakehouse_maintenance.py` 다(ADR-0559 §9 그대로).

## 첫 물결의 상태 (2026-11-12)

| 작업 | 판정 | 상태 | 이유 |
|---|---|---|---|
| `real_transaction_silver_to_gold` | 배치 단계만 equal(ADR-0559, 경기도) | 옮기지 않음 | 커밋이 `MERGE` 다(5 번). 문서 조립·지역 통계는 아직 옮겨 대조하지 않았다. 같은 날 W73 의 고침(#1346, #1361)으로 Spark 재빌드가 돌고 있어 전국 기준 출력이 아직 없다 |
| `rt_molit_real_transaction` (Silver) | 없음 | 옮기지 않음 | 추가 모양이라 이 길의 대상이지만 파티션이 `truncate(4, deal_ym)` 이다(2 번). 2026-11-12 에는 Spark 에서 끝나고 있다(04:04, 05:50, 08:06, 08:12 UTC 정상 종료) |
| `parcel_nearby_silver_to_gold` | 예외 ⓑ(ADR-0559: Sedona `ST_KNN` 이 경기도 필지의 0.78% 에서 더 먼 시설을 골랐다 — 40,467 행, DuckDB 가 더 가까운 경우만 있고 반대는 0, 최대 차이 214 m) | 막힘 | 시설 Silver 여섯(`silver.airport`·`railway_station`·`subway_station`·`school_location`·`university`·`commercial_district`)에 스냅샷이 없어 어떤 엔진으로도 만들 수 없다(유닛의 실패 사유: `required Silver inputs exist but were never loaded`). JVM 메모리 실패가 아니다. 커밋도 `MERGE` 다 |

HUB 계열 레인 작업(`silver_refresh_hub_lane_family`, ADR-0551, #1343)은 새 Spark 파일을 더하지 않았다. 그 22 개 레인은 이미 장부에 있는
`silver_scalar_handoff_to_lakehouse`·`silver_release_compare`(물결 4)의 세션으로 돈다. 그 두 줄이 옮겨질 때 함께 옮겨진다.
장부는 파일의 장부이므로 줄을 더하지 않는다(같은 작업을 `orchestration/jobs.v1.json` 의 `pool: "spark"` 가 이미 말한다).

## Consequences

- 커밋 길은 증명됐다. 남은 것은 운영 코드다: 어댑터(꼬리표 → 매니페스트 항목), DuckDB 준비 단계, 재개 기록. 첫 표는 identity 파티션이거나
  파티션이 없는 추가 표다.
- Gold 생산자는 계산을 옮겨도 장부에서 빠지지 않는다. Spark 풀의 큰 슬롯은 비지만(조인과 추론이 DuckDB 로 간다) 세션은 남는다.
  "57 → 1" 로 가려면 Gold 의 커밋 모양을 따로 결정해야 한다. 이 ADR 은 그것을 결정하지 않는다.
- 탐침의 꼬리표 읽기는 파일을 통째로 받았다(시험 파일이 수십 KB 다). 운영 어댑터는 꼬리표만 범위로 읽는다 — 발행기에 그 읽기가 이미 있다
  (`lakehouse_snapshot_scan/ranged_parquet`).
- `fast_append` 의 중복 검사는 현재 스냅샷의 매니페스트를 전부 읽는다. 매니페스트가 많은 표에서는 그 시간이 커밋마다 든다.
  첫 표를 옮기는 PR 이 그 표에서 잰다.
- 발행기 스캔이 `double` 을 못 읽는 것은 그 표를 Rust 로 읽게 되는 PR 이 고친다.
- 시험 표 `w75b_commit_probe.parcel_panel` 과 그 관리형 합치기는 켜 둔 채 남긴다 — 합치기가 `replace` 스냅샷을 만든 뒤 독자들이
  읽는지 보려면 하루 이상 걸린다. 끄는 명령:
  `wrangler r2 bucket catalog compaction disable foundation-platform-lakehouse-prod w75b_commit_probe parcel_panel`.
  표와 네임스페이스는 관찰이 끝나면 지운다. 비용: Cloudflare 문서에 관리형 유지보수의 값이 적혀 있지 않다(2026-11-12).
  시험 표는 파일 7 개·80 KB 다.

## 하지 않은 것, 확인하지 못한 것

- 운영 표에 대한 커밋. 탐침은 시험 표뿐이다.
- 관리형 합치기가 만든 `replace` 스냅샷을 독자들이 읽는지(아직 합치기가 돌지 않았다), 관리형 만료에서 태그와 핀이 살아남는지(켤 수 없었다).
- 큰 파일(수백 MB)·많은 파일(수천 개)의 커밋 시간. 탐침은 파일 1~2 개·수십 KB 다.
- `truncate`·`bucket` 파티션의 값 계산, 정렬 순서 id, 중첩 칸.
- 추가 커밋과 `publish_branch`(적재 가지 발행)가 같은 표에서 겹치는 경우. 4 번대로 옮긴 표는 가지를 쓰지 않는다.
- 첫 물결의 전국·전체 작업 대조(위 표).

## 기각한 대안

- **매니페스트와 커밋 본문을 손으로 쓴다**(지금의 `commit_table` 위에). 가지로도 커밋할 수 있게 되지만 Avro 매니페스트·순번·요약 집계를
  다시 만드는 일이다. 4 번으로 가지가 필요 없어졌다.
- **DuckDB 가 커밋한다.** 요약 속성과 태그를 못 쓴다(ADR-0559).
- **DuckDB 가 쓴 파일을 iceberg-rust 의 Parquet writer 로 다시 쓴다.** 등록 함수가 공개가 아니라는 이유만으로 모든 행을 한 번 더 쓰게 된다.
  꼬리표에서 항목을 만드는 쪽이 싸고, 탐침에서 경계까지 맞았다.
- **pyiceberg 로 커밋한다.** 발행기 밖에 Python 실행기를 하나 더 두는 길이다. 독자와 카탈로그 클라이언트가 이미 Rust 다.
