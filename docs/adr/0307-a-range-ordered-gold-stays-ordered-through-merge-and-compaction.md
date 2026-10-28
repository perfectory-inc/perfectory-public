# ADR 0307: 범위 정렬 Gold 는 MERGE·합치기를 거쳐도 PNU 범위를 지킨다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md) (PNU 순서 쓰기·파일 건너뛰기),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) (바뀐 PNU 만 MERGE),
  [ADR-0066](./0066-a-table-that-fits-in-one-file-is-not-split.md) (합치기 목표 크기, `lakehouse-maintenance.contract.json`)

## Context — 사고: 전량 필지 by-PNU 굽기 20시간+ (2026-10-28)

전량 필지 묶음 굽기가 20시간을 넘겼다. 조각(약 252개)마다 로그가
`skipped the data files whose bounds exclude the prefix … data_files: 9, skipped_data_files: 0` 였고,
"section pack export" 에서 약 300초 동안 CPU 5%·내려받기 12.6 MB/s 였다. 조각마다 4 GB 표 전체를 받았다
(굽기 한 번에 약 1 TB).

ai-server Trino 로 잰 Gold 파일 경계(`<table>$files` 의 `readable_metrics.pnu`):

| 표 | 파일 | GB | 파일마다 PNU 범위 | 마지막 쓰기 |
|---|---:|---:|---|---|
| gold.parcel_panel | 9 | 4.06 | **전부 1111… ~ 5280… (전국)** | 10-26 `replace` (lakehouse-maintenance, 34→9) |
| gold.parcel_transfer_history | 20 | 3.26 | **전부 전국** | 10-23 `overwrite` (증분 MERGE) |
| gold.parcel_forest_ledger | 11 | 1.86 | **전부 전국** | `overwrite` (증분 MERGE) |
| gold.building_panel | 14 | 1.97 | 서로 안 겹침 | 10-20 `overwrite` (전체) |
| gold.parcel_characteristics | 11 | 1.68 | 서로 안 겹침 | 10-17 `overwrite` (전체) |
| gold.parcel_zoning | 10 | 1.50 | 서로 안 겹침 | `overwrite` (전체) |
| gold.parcel_price | 9 | 1.40 | 서로 안 겹침 | `overwrite` (전체) |
| gold.unit_price | 9 | 0.43 | 서로 안 겹침 | `overwrite` (전체) |
| gold.parcel_land_rights | 9 | 0.07 | 서로 안 겹침 | `overwrite` (전체) |
| gold.parcel_public_auction | 5 | 0.001 | 겹침(작아서 무관) | `overwrite` |

모든 표가 ADR-0164 대로 `sort-order = pnu`, `write.distribution-mode = range` 였다. 전체 쓰기
(`writeTo(...).overwrite`)는 그 순서를 지켰다. 순서를 깨는 쓰기는 둘이었다.

1. **증분 MERGE (ADR-0180).** Iceberg(1.10/1.11 `SparkWriteConf.copyOnWriteMergeDistributionMode`)는
   `write.merge.distribution-mode` 가 없으면 **분할된 표**에서 `write.distribution-mode` 로 돌아가지 않고
   `hash` 를 쓴다. Gold 는 `source_snapshot_id` 하나로 분할돼 있어 hash 는 행을 한 갈래로 모으고, 크기
   조정이 그것을 임의로 쪼개 파일마다 전국이 섞인다.
2. **유지보수 합치기 (`lakehouse_maintenance.py`).** `rewrite_data_files` 의 기본 전략 bin-pack 은 파일을 크기로만 묶어 이어
   붙인다. parcel_panel 의 범위 파일 34개(각 약 125 MB, 작은 파일 기준 128 MB 미만)가 9개로 합쳐지며 전부
   전국 범위가 됐다.

## Decision

1. **`range` 계약의 표는 MERGE 도 범위로 나눈다.** `platform_contracts.apply_write_order` 가
   `WRITE ORDERED BY` 와 함께 `write.merge.distribution-mode = range` 를 건다. `write_order_drift` 는 이
   속성이 `range` 가 아니면 어긋남으로 본다. 그래서 배포의 lakehouse 마이그레이션(ADR-0124)과 생산자의
   `ensure_contract_table` 이 살아 있는 표마다 한 번 맞춘다(메타데이터 커밋, 데이터 파일은 안 건드림).
   delete/update 는 Gold 쓰기에 쓰지 않으므로 걸지 않는다.
2. **정렬 순서가 있는 표는 정렬해서 합친다.** `lakehouse_maintenance` 는 살아 있는 표에 Iceberg
   `sort-order` 가 있으면 `rewrite_data_files(strategy => 'sort')` 로 합친다(표의 정렬 순서로 정렬).
   없는 표는 지금처럼 bin-pack 이다. 기준은 계약이 아니라 표의 실제 정렬 순서다: 정렬을 가진 표를
   bin-pack 하는 것은 어느 경우에도 그 정렬을 버리는 일이기 때문이다.
3. **파일 크기(N)는 새로 정하지 않는다.** 범위 쓰기의 파일 수는 Iceberg 의 쓰기 크기 조정이 정한다(측정:
   parcel_panel 34개 × 약 125 MB, building_panel 14개 × 약 140 MB). 128 MB 미만 파일이 10개 이상이면
   합치기가 계약의 512 MB 목표로 정렬해 다시 쓴다(parcel_panel 은 약 9개 × 450 MB). 둘 다 요청 범위
   256–512 MB 근처이고, 조각 하나가 대개 파일 1개(가끔 2개)만 연다.
4. **시험.** `test_gold_range_layout_iceberg.py` 가 실제 Iceberg 표에 공용 쓰기(`write_gold_snapshot`),
   증분 `merge_into_gold`, 유지보수 합치기를 차례로 돌리고 매번 데이터 파일의 `pnu` 경계가 겹치지 않음을
   확인한다. 1을 빼면 MERGE 뒤 15개 파일이 모두 전국 범위로, 2를 빼면 합치기 뒤 겹침으로 실패함을
   ai-server 의 고정 Spark 이미지에서 확인했다.

## Consequences

- **효과는 각 표가 다음에 다시 쓰일 때부터다.** 이 변경은 메타데이터만 바꾼다. 이미 섞인 파일은 그대로다.
  parcel_panel·parcel_transfer_history·parcel_forest_ledger 는 다음 Gold 재생성(전체 쓰기, 또는 이 뒤의
  MERGE 가 파일을 다시 쓸 때) 또는 다음 정렬 합치기에서 범위 파일이 된다. 그 뒤 첫 전량 굽기가 빠른 굽기다.
  행·값은 바뀌지 않는다(파일 배치만).
- **기대 효과(측정 아님).** parcel_panel 조각 하나의 내려받기: 4.06 GB → 합친 뒤 약 0.45 GB(파일 9개 중
  1개, 경계에 걸리면 2개), 합치기 전이면 약 0.13 GB(34개 중 1개). 굽기 한 번 약 1 TB → 약 120 GB(또는
  약 35 GB). 12.6 MB/s 기준 조각당 약 320초 → 약 36초(또는 약 10초).
- **정렬 합치기 비용.** sort 전략은 다시 쓰는 파일을 셔플·정렬한다(bin-pack 보다 무겁다). 대상은 정렬 순서가
  있는 Gold 표뿐이고 작은 파일 기준을 넘을 때만이다. 유지보수 140분 상한 안인지는 첫 실행에서 잰다.
- **읽기 쪽 남은 일(이 ADR 밖).** `lakehouse_snapshot_scan` 은 파일 경계로 파일을 건너뛰고(ADR-0164),
  읽은 파일 안에서도 Parquet row group 통계로 디코딩을 건너뛴다. 그러나 파일은 통째로 내려받는다
  (`LakehouseByteReader::read`). footer 를 먼저 읽고 필요한 row group 의 바이트 범위만 받으면 파일 크기와
  무관하게 조각당 읽기량이 row group 단위로 준다 — 후속 과제.
