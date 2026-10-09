# ADR 0164: 패널 Gold 는 PNU 순서로 쓰고, by-PNU 굽기 조각은 자기 앞자리를 담을 수 있는 파일만 읽는다

- Status: Accepted
- Date: 2026-10-09
- Builds on: [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md) (패널 Gold 재생성),
  [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md) (묶음 굽기),
  [ADR-0124](./0124-a-deploy-brings-every-lakehouse-table-to-its-contract.md) (배포의 lakehouse 마이그레이션)

## Context

2026-10-08 필지 묶음 굽기는 조각 126개로 돌았다. 조각마다 PNU 앞자리
(`FOUNDATION_PLATFORM_<LANE>_BY_PNU_SERVING_PNU_PREFIX`, 2–5자리)가 있다.

| 무엇 | 값 |
|---|---:|
| `gold.parcel_panel` 행 | 39,861,511 |
| 데이터 파일 | 약 33개 |
| 조각 하나가 연 파일 | 33개 전부 (앞자리 필터는 행을 읽은 뒤) |
| 조각 하나의 최소 시간 | 약 3.5분 (R2 가 느릴 때 20분) |
| 전체 굽기 | 약 7시간 |

원인은 두 겹이다.

1. **Gold 행이 PNU 순서로 저장되지 않는다.** 계약(`GOLD_PARCEL_PANEL`·`GOLD_BUILDING_PANEL`)의 `sort_order` 는
   이미 `["pnu"]` 였지만, 그것을 표에 옮기는 코드가 없었다. 생산자는 `'write.distribution-mode' = 'hash'` 로
   표를 만들었다. 분할 열(`source_snapshot_id`)이 값 하나뿐이라 hash 분배는 행을 사실상 임의로 나눈다.
   데이터프레임의 `sortWithinPartitions` 는 Iceberg 가 쓰기 전에 다시 섞으므로 남지 않는다. 그래서 파일마다
   PNU 범위가 전국에 걸치고, 파일 단위 최소·최대값으로는 어느 파일도 뺄 수 없다.
2. **스캔이 파일 통계를 읽지 않는다.** `lakehouse_snapshot_scan` 은 manifest 에서 경로·행 수·크기만 읽고
   `lower_bounds`·`upper_bounds`·`null_value_counts` 는 버렸다.

## Decision

1. **쓰기 배치는 계약이 정한다.** `LakehouseTableContract` 에 `write_distribution`
   (`LakehouseWriteDistribution::{Hash, Range}`)을 더한다. 계약 산출물
   (`infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`)에 `"write_distribution"` 으로
   나가고, 산출물 테스트가 Rust 상수와 같음을 강제한다. `gold.parcel_panel`·`gold.building_panel` 만 `range`,
   나머지 표는 `hash`(지금 그대로)다.
   - `sort_order` 에서 유추하지 않는다. 대부분의 표가 태스크 안 정렬용 `sort_order` 를 갖고 있어서, 유추하면
     재지 않은 Silver 적재 전부가 표 전체 범위 정렬로 바뀐다.
2. **`range` 의 뜻 = Iceberg 표 정렬 순서.** `range` 표는 계약의 `sort_order` 를 Iceberg 정렬 순서로 갖는다
   (`ALTER TABLE ... WRITE ORDERED BY pnu`, 같은 커밋에서 `write.distribution-mode=range`). 그러면 Spark
   쓰기가 정렬 키 범위로 나뉘어 파일 하나가 PNU 연속 구간 하나를 담는다. 정의는
   `platform_contracts.py` 의 `write_distribution_mode`·`write_order_drift`·`apply_write_order` 한 곳이다.
   - 생산자 둘(`parcel_panel_silver_to_gold.py`, `building_panel_silver_to_gold.py`)은 표를 만들고 맞추는 일을
     `ensure_contract_table` 하나로 한다(생성 시 분배 모드도 계약에서). 각자 갖던 `TBLPROPERTIES` 복사본은 없앴다.
   - 배포의 lakehouse 마이그레이션(`lakehouse_schema_migrate.py`, ADR-0124)은 계획에 `write_order` 를 더해
     살아 있는 표의 정렬 순서가 계약과 다르면 같은 함수로 맞춘다. 메타데이터 커밋이라 데이터 파일은 다시 쓰지 않는다.
3. **스캔은 증명된 파일만 건너뛴다.** `scan_snapshot_rows_kept` 가 `KeptPrefix { column, prefix }` 를 받는다.
   manifest 의 Avro 헤더 `schema` 로 열 이름 → field id 를 찾고, 파일마다 그 열의 하한·상한·null 수를 읽는다.
   파일을 건너뛰는 조건은 셋 다다.
   - null 수가 기록돼 있고 0 (null 행은 경계 밖이고, 읽었다면 거부됐을 행을 숨기면 안 된다),
   - 하한과 상한이 모두 기록돼 있음,
   - 앞자리의 전 구간 `[prefix, prefix 다음)` 이 `[하한, 상한]` 밖 (상한 < prefix, 또는 하한 ≥ prefix 다음).
   하나라도 없으면 읽는다. 앞자리는 ASCII 만 받는다: Iceberg 의 문자열 순서(UTF-16)와 경계 바이트(UTF-8)
   순서는 한쪽이 ASCII 이면 일치한다. Iceberg 가 16자로 자른 경계도 하한은 낮아지고 상한은 올림이라 그대로 성립한다.
   스캔은 읽은 파일에서도 앞자리 밖 행을 버린다. 그래서 결과는 파일을 읽었든 건너뛰었든 같다.
4. **완전성 증명은 같은 강도로 남는다.** 전에는 "디코딩 행 = manifest 행" 이었다. 이제
   `ScannedRows::ensure_complete` 가 (가) 읽은 파일의 디코딩 행 = 그 파일들의 manifest 행,
   (나) 읽은 행 + 건너뛴 파일의 manifest 행 = 스냅숏 전체 manifest 행을 확인한다. 건너뛴 파일 수·행 수는
   `ScannedRows` 와 로그(`skipped the data files whose bounds exclude the prefix`)에 남는다.
   조각 요약이 말하는 Gold 행 수(`scanned_row_count`, `gold_record_count`, 재기준의 `table_rows`)는
   전처럼 스냅숏 전체 행 수다. 굽기 스크립트가 조각마다 같은 값을 요구하므로 뜻을 바꾸지 않는다.
5. **호출부 넷 모두 앞자리를 넘긴다**: 묶음 굽기(`section_packs/bake.rs`), 객체 굽기 둘
   (`parcel_by_pnu_serving_export.rs`, `building_by_pnu_serving_export.rs`), 재기준(`by_pnu_serving_rebase.rs`).
   각 keep 필터는 이미 앞자리를 논리곱으로 갖고 있어서 결과 행은 바뀌지 않는다.

## 기대 효과 (측정 아님)

정렬된 Gold 에서 파일 하나는 약 120만 행의 연속 PNU 구간이다. 조각 126개의 평균은 약 32만 행이므로 조각
하나가 여는 파일은 대개 1–2개(앞자리 2자리 시도 조각은 그 비율만큼)다. 조각당 읽기량은 33파일에서 약 1/15–1/30
로 줄고, 전체 굽기에서 R2 데이터 파일 읽기는 약 4,158회(126 × 33)에서 수백 회로 준다. 실제 시간은 첫 정렬
Gold 를 구운 뒤 잰다.

## 운영 단계 (이 변경이 혼자 하지 않는 것)

- 배포의 `migrate` 가 두 표에 정렬 순서를 건다(메타데이터만). **이미 있는 파일은 그대로 무순서다.** 이후 쓰기부터
  정렬된다.
- 매일 Gold 재생성(ADR-0139)은 Silver 가 바뀔 때만 다시 쓴다. 바뀔 때까지 기다리지 않으려면 감독 실행으로
  `gold-panel-rebuild.sh all --unconditional "ADR-0164 PNU 순서로 다시 쓰기"` 를 한 번 돌린다(행 하한·검사는 그대로).
  다시 쓴 스냅숏은 새 Gold 세대이므로 다음 굽기가 전체를 굽는다 — 이 굽기가 효과를 처음 재는 자리다.
- 범위 분배는 쓰기 전에 표본 추출을 한 번 더 돈다(두 생산자 모두 Gold 프레임을 `persist` 하므로 입력을 다시
  계산하지 않는다). 정렬 쓰기의 시간·메모리는 재지 않았다. 감독 실행에서 ADR-0139 의 160분 상한 안인지 잰다.

## 검토한 대안

- **굽기 쪽에서 Gold 를 앞자리별로 다시 나눠 두기(조각별 사본)**: 같은 사실의 두 번째 사본이고, Gold 가 바뀔
  때마다 함께 맞춰야 한다.
- **`source_snapshot_id` 대신 PNU 앞자리로 분할**: 분할 명세 변경은 표 재작성과 모든 읽기 경로 점검이 필요하고,
  2–5자리로 길이가 다른 조각과 맞지도 않는다. 범위 정렬은 분할을 건드리지 않고 같은 효과를 낸다.
- **iceberg-rust 의 스캔 계획 사용**: ADR-0040 결정 8 이 manifest 해석을 이 모듈 하나에 둔다. 필요한 것은
  경계 비교 하나라 기존 해석기에 더하는 쪽이 작다.
- **모든 표에 `WRITE ORDERED BY sort_order`**: 재지 않은 Silver 적재 수십 개의 쓰기를 바꾼다. 필요한 표만
  계약에서 `range` 로 선언한다.

## Consequences

- 새 표가 by-PNU 로 굽히면 계약에 `range` 를 선언하는 것으로 충분하다. 생산자는 `ensure_contract_table` 을 쓴다.
- 건너뛰기는 증명 없이는 일어나지 않는다. 통계가 없는 옛 파일·무순서 파일은 지금처럼 읽힌다 — 느릴 뿐 틀리지 않는다.
- 테스트(`lakehouse_snapshot_scan/pruning_tests.rs`)는 합성 PNU(`99999…`)만 쓴다. 경계가 앞자리와 겹치는 파일을
  건너뛰게 바꾸면(하한을 앞자리 자체와 비교하도록 바꿈) 3개 테스트가, null 수 확인을 빼면 1개 테스트가 실패함을
  확인했다.
