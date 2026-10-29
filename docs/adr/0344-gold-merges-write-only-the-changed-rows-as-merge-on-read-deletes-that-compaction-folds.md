# ADR 0344: Gold MERGE 는 바뀐 행만 쓴다 — merge-on-read 위치 삭제, 합치기가 접어 넣는다

- Status: Accepted
- Date: 2026-10-30
- Builds on: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) (바뀐 PNU 만 MERGE),
  [ADR-0307](./0307-a-range-ordered-gold-stays-ordered-through-merge-and-compaction.md) (범위 정렬 MERGE·합치기),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md) (바뀐 PNU 기록),
  [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md) (쓰기 증폭 측정),
  [ADR-0319](./0319-by-pnu-reads-fetch-only-the-footer-and-the-row-groups-they-decode.md) (by-PNU 범위 읽기)
- Tracking: PR #951

## Context — 0.12% 를 바꾸려고 100% 를 다시 쓴다

증분 Gold 재생성(ADR-0180)은 바뀐 PNU 만 다시 계산한다. 그런데 쓰기는 copy-on-write `MERGE` 라서
바뀐 행이 하나라도 든 데이터 파일을 통째로 다시 쓴다. 운영에서 잰 값(2026-10-29, journal 과 Trino
`$snapshots`, 읽기 전용):

| 커밋 | 바뀐 키 | 쓴 바이트 | 초 |
|---|---:|---:|---:|
| `gold.parcel_panel` 10-26 증분 | 48,477 (0.12%) | 4.266 GB / 4.266 GB (100%) | 3,096 |
| `gold.parcel_forest_ledger` 10-26 증분 | 48,477 | 1.855 GB (100%) | 892 |
| `gold.parcel_transfer_history` 10-23 증분 | 52,780 | 3.256 GB (100%) | 6,249 |

범위 정렬(ADR-0307)로는 풀리지 않는다(계산). 월 단위 토지 판(vintage)은 전국에 흩어진 약 4.8만 PNU 를
바꾼다. 범위 정렬된 `parcel_panel` 은 파일 약 34개에 각 약 117만 PNU 이므로 파일마다 바뀐 PNU 가 약
1,400개 든다. 한 파일에 하나도 안 들 확률은 약 e^-1400, 곧 0 이다. 파일 순서와 상관없이 전국 판 하나는
copy-on-write 로 100% 쓰기다.

## Decision

1. **증분 Gold MERGE 는 merge-on-read 로 쓴다.** `gold_incremental.merge_into_gold` 가 MERGE 하는 표는
   `write.merge.mode`·`write.update.mode`·`write.delete.mode` 를 `merge-on-read` 로 둔다. MERGE 는 이제
   바뀐 행만 새 데이터 파일로 쓰고, 건드린 데이터 파일마다 위치 삭제(position delete) 파일 하나를 쓴다.
   기존 데이터 파일은 그대로 둔다. 형식은 Iceberg format v2 위치 삭제다. v3 삭제 벡터(deletion
   vector, Puffin)는 Trino 와 아래 Rust 독자가 읽게 된 뒤의 별도 결정이다. 값은 MERGE 하는 쪽의 계약
   (`gold-panel-rebuild.contract.json`)이 정하고, MERGE 직전 한 곳에서 메타데이터 커밋으로 맞춘다
   (구현 PR 은 이 ADR 의 2번이 main 에 들어간 뒤).
2. **Gold 를 읽는 모든 독자가 삭제 파일을 적용한다 — 전환의 전제.**

   | 독자 | 상태 |
   |---|---|
   | Rust `lakehouse_snapshot_scan` (by-PNU 굽기·rebase·인벤토리·산단 적재) | 이 ADR 로 구현. 아래 3 |
   | Spark (Gold 생산자·`by_pnu_panel_delta`·served Gold) | Iceberg 1.11 이 기본 적용 |
   | Trino 481 (shape ledger·운영 조회) | Iceberg 커넥터가 v2 위치·동등 삭제를 기본 적용 |
   | dbt | Trino 위에서 돈다(`profiles.example.yml` `type: trino`). 소스는 silver 뿐, Gold 를 읽지 않는다 |
   | by-PNU 델타(ADR-0312 바뀐 PNU) | `gold.<t>__changes` 는 MERGE 가 기록한 PNU 라 삭제와 무관. 남은 행 수만 `total-records` 에서 `total-position-deletes` 를 뺀다(`by_pnu_panel_delta.live_rows`) |

3. **Rust 스캔의 삭제 적용.** 매니페스트 목록의 삭제 매니페스트(`content = 1`)를 읽고, 거기 든 살아 있는
   Parquet 위치 삭제 파일을 통째로 읽어 데이터 파일 경로별 삭제 위치 집합을 만든다. 데이터 파일은 지금처럼
   범위로 읽되(ADR-0319), 삭제된 위치는 Arrow `RowSelection` 으로 디코드하지 않는다(읽는 행 그룹 안에서만).
   데이터 파일은 경로로만 짝짓는다: 명세의 순번(sequence number) 조건은 한 경로에 두 파일이 있을 때만 갈리고,
   Iceberg 작성기는 경로를 다시 쓰지 않는다. **동등 삭제(`content = 2`)와 삭제 벡터(Puffin)는 거부한다**:
   건너뛰면 표에 없는 행을 서빙한다. 표의 행 수를 묻는 곳(굽기의 `gold_record_count`, rebase 의 `gold_rows`,
   인벤토리, 산단 적재)은 `ScannedRows::live_record_count()` = 매니페스트 행 수 − 삭제된 행 수를 쓴다.
   `ensure_complete` 의 셈(디코드 + 건너뜀 = 읽은 파일의 매니페스트 행 수)은 그대로다: 삭제된 행은 디코드하지
   않은 행에 든다. 파일 행 수를 넘는 삭제 위치는 거부한다.
4. **관문.** `foundation-publisher-core/tests/fixtures/merge_on_read` 는 고정 Spark 이미지와 엔진 계약의
   Iceberg 1.11 이 같은 쓰기(범위 정렬 append 세 번, `merge_sql` 모양 MERGE 두 번)를 받은 두 표다:
   copy-on-write 하나, merge-on-read 하나(위치 삭제 파일 4개, 앞 삭제를 다시 지우는 MERGE 포함).
   `expected.json` 은 Spark 가 읽은 행이다. `merge_on_read_tests.rs` 는 Rust 스캔이 두 표 모두에서 Spark 와
   **PNU 마다 같은 행**을 돌려주고, 조각 접두사마다도 같음을 확인한다. 픽스처는 `generate.py` 로 다시 만든다.
   이 관문이 main 에 들어가기 전에는 어떤 표도 1 로 바꾸지 않는다.
5. **합치기가 삭제를 접어 넣는다 — 쓸 때마다가 아니라 주기로.** `lakehouse_maintenance` 는 표의 가장 오래된
   살아 있는 삭제 파일이 계약의 날수(7일)를 넘으면 삭제를 가진 데이터 파일만 정렬 전략으로 다시 쓴다
   (`delete-file-threshold = 1`, `remove-dangling-deletes`). 정렬 순서를 가진 표의 파일 PNU 범위가 shape
   ledger 와 같은 잣대(한 시도 읽기가 파일의 50% 넘게 연다)로 흩어져 있으면 표 전체를 정렬해 다시 쓴다.
   잣대는 한 벌만 둔다(구현 PR).
6. **측정.** ADR-0314 §4 의 쓰기 증폭(커밋이 더한 바이트 / 표 바이트)은 바뀐 키 비율과 삭제 파일 몫 근처로
   떨어져야 한다. 운영에서 그렇게 잰 뒤에 `gold_panel_rebuild`·`gold_kind_rebuild` 가 `changed-keys` 가 되고
   `tools/ci/full-scope-jobs.baseline.json` 에서 빠진다.

## 다른 곳은 이렇게 한다

- Apache Iceberg 의 merge-on-read 행 수준 연산: v2 위치 삭제, v3 삭제 벡터. Netflix·Apple 이 잦은 upsert 에 쓰고
  주기적 합치기(`rewrite_data_files`)로 삭제를 접는다.
- Delta Lake 삭제 벡터: Databricks 는 `MERGE`/`UPDATE`/`DELETE` 에 기본으로 켜, 작은 변경이 파일을 다시 쓰지
  않게 하고 `OPTIMIZE`/`PURGE` 가 나중에 접는다.
- Apache Hudi Merge-On-Read(Uber): 변경 로그 + 주기적 compaction.
- 독자 쪽 적용은 iceberg-rust(`DeleteFileIndex`)·pyiceberg·Trino 와 같은 방식이다: 데이터 파일 경로로 위치
  삭제를 모아 그 행을 읽지 않는다.

## Consequences

- 계산(측정 아님), 커밋 하나에 쓰는 바이트. 바뀐 행 × 표의 행당 바이트 + 위치 하나에 8 B 이하의 삭제 파일:

  | 표 | 행당 B (측정) | 바뀐 키 | 지금 (CoW) | 뒤 (MoR) |
  |---|---:|---:|---:|---:|
  | gold.parcel_panel | 101.9 | 48,477 | 4.266 GB | 약 5.3 MB |
  | gold.parcel_forest_ledger | 46.7 | 48,477 | 1.855 GB | 약 2.7 MB |
  | gold.parcel_transfer_history | 81.8 | 52,780 | 3.256 GB | 약 4.7 MB |

  주기 합치기는 삭제를 가진 파일만 다시 쓴다. 전국 판이면 그것이 표 전체(parcel_panel 약 4.06 GB)지만 커밋마다가
  아니라 7일에 한 번이다.
- 굽기 조각은 이제 살아 있는 위치 삭제 파일을 모두 통째로 읽는다(작다: 위치 하나에 몇 바이트). 7일 주기가 그
  수와 크기를 묶는다. 늘면 삭제 파일의 `referenced_data_file` 로 읽을 파일만 고르는 것이 다음 단계다.
- 합치기 전까지 Iceberg 요약의 `total-records` 는 삭제된 행을 센다. 행 수가 판정에 쓰이는 곳은 살아 있는 행
  수로 바꿨다(3, 2 의 델타). `gold_rebuild` 의 비용 추정·shape ledger 의 키 비율 분모는 근삿값이라 그대로 둔다.
- MERGE 직후 데이터 파일은 변경분 파일이 전국 범위로 겹친다. 기준 파일은 범위를 지키고, 작은 파일 합치기와 5 가
  변경분을 정렬해 접는다. ADR-0307 §4 의 "MERGE 뒤 파일 경계가 겹치지 않는다"는 기준 파일에만 성립한다.
