# ADR 0316: 필지 경계 Silver 는 판(edition)으로 나누고 PNU 범위로 정렬한다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md) (판을 나란히 둔다),
  [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md) (PNU 순서 쓰기),
  [ADR-0307](./0307-a-range-ordered-gold-stays-ordered-through-merge-and-compaction.md) (MERGE·합치기도 순서를 지킨다)
- Revisits: [ADR-0063](./0063-a-partition-that-cannot-narrow-a-search-only-splits-files.md),
  [ADR-0066](./0066-a-table-that-fits-in-one-file-is-not-split.md) (필지 경계를 나누지 않던 이유)

## Context — 실측 (ai-server Trino, 2026-10-28)

`silver.parcel_boundaries` 는 ADR-0148 대로 판 두 개를 나란히 들고 있다. 두 판 모두 행 전부가
`valid_to_utc IS NULL`(현재 행)이고, 읽는 쪽은 모두 `source_snapshot_id = 제공 판` 으로 하나만 고른다
(`parcel_panel_silver_to_gold.read_served_parcels`, 공매·공매(공공)·kind 레인이 이것을 쓴다).

| 잰 것 | 값 |
|---|---|
| 판별 행 | `vworldkr__parcel-202609` 39,876,546 · `vworldkr__parcel-202606` 39,861,511 (합 79,738,057) |
| 파일 | 29개, 14.93 GB, 모두 spec 1(분할 없음). `$partitions` 는 옛 spec 의 `{sigungu_code=NULL}` 한 칸 |
| 파일마다 `pnu` 경계 | **29개 전부** `11110…`~`47770…/52800…` — 전국 |
| 파일마다 판 | 15개는 두 판이 섞였고 14개는 한 판. 경계는 16자에서 잘려 `vworldkr__parcel`~`vworldkr__parcem` 이라 **판을 가를 수 없다** |
| 제공 판 전체 읽기 (`count(geometry_wkb) WHERE source_snapshot_id = 제공 판`) | splits **137**(전 파일), 입력 44,518,090행(필요 39.9M), 물리 4.76 GB |
| 시군구 하나 범위 읽기 (`pnu` 범위 + 제공 판) | splits 9, 입력 74,309행(맞는 48,965), 물리 622 kB |

`sigungu_code` 열 값은 비어 있지 않다(NULL 0행). NULL 인 것은 옛 spec 의 분할 값이다: ADR-0066 이
sigungu 분할을 걷어 낸 뒤 쓴 파일은 분할이 없다. ADR-0066 의 근거는 "모든 소비자가 표를 통째로 읽는다"
였다. 판이 둘이 된 뒤(ADR-0148)로는 틀렸다: 소비자는 판 하나만 읽는데, 파일이 판을 가르지 않아 전부 연다.

## Decision

1. **분할 = 판.** 계약(`SILVER_PARCEL_BOUNDARIES`, Rust 정본 → `industrial_complex_lakehouse_contracts.json`)의
   `partition_spec` 은 `["source_snapshot_id"]` 이다. 판 하나가 약 7.5 GB, 512 MB 목표 파일 약 15개라
   ADR-0063·0066 이 막으려던 작은 파일은 생기지 않는다. 지역(sigungu·sido) 분할은 다시 두지 않는다:
   PNU 앞자리가 곧 지역이라 아래 정렬이 같은 일을 하고, 분할은 파일 크기만 깎는다.
2. **판 안에서는 PNU 범위.** `write_distribution` 은 `range`, `sort_order` 는 그대로 `pnu, valid_from_utc`.
   쓰기마다 파일이 PNU 의 이어진 한 구간을 갖고, 읽는 쪽은 `pnu` 경계로 파일을 건너뛴다(ADR-0164).
   MERGE·합치기에서도 순서가 지켜지는 것은 ADR-0307(`write.merge.distribution-mode`, 정렬 합치기) 몫이다.
3. **분할 진화는 더하기만.** `platform_contracts.apply_write_order` 가 계약의 분할 필드 중 살아 있는 spec 에
   없는 것을 `ALTER TABLE … ADD PARTITION FIELD` 로 더한다. 계약에 없는 살아 있는 필드는 지우지 않는다.
   이 단계는 이미 있던 쓰기 순서 단계(ADR-0164)를 넓힌 것이고, 같은 두 곳이 부른다: 배포의
   `lakehouse_schema_migrate`(4단계)와 쓰기 직전의 `spatial_silver_handoff.create_iceberg_table_if_missing`.
   range 계약에만 적용한다. 운영 range 표 10개(Gold)는 이미 `source_snapshot_id` 로 나뉘어 있어(10-28 확인)
   바뀌는 표는 `silver.parcel_boundaries` 하나다.
4. **이미 쓴 파일의 재배치는 한 번의 통제된 다시쓰기다.** 분할 진화는 메타데이터뿐이라 옛 29개 파일은
   옛 spec 그대로 남고, 그 파일들은 판 경계가 잘려 있어 새 판을 읽을 때도 계속 열린다. 그래서 운영자가
   한 번 다음을 돌린다(전량 처리의 선언된 예외: 옛 파일은 판이 섞여 있어 판 하나만 다시 쓸 수 없다):

   ```sql
   CALL r2.system.rewrite_data_files(table => 'silver.parcel_boundaries', strategy => 'sort',
     options => map('rewrite-all', 'true', 'target-file-size-bytes', '536870912',
                    'partial-progress.enabled', 'true', 'partial-progress.max-commits', '10'))
   ```

   행은 하나도 지우거나 바꾸지 않는다(append-only 유지: 다시쓰기는 같은 행을 새 파일로 옮기고, 옛 파일은
   스냅샷 만료(30일)까지 남는다). 긴 by-PNU 굽기가 도는 동안에는 돌리지 않는다.
5. **판은 둘 다 둔다.** 지우는 데이터는 없다. 읽는 쪽은 지금처럼 제공 판(`served_edition`)을 고르고,
   그 조건이 이제 분할 가지치기가 된다.

## Consequences

| | 전 (실측 10-28) | 후 (계산) |
|---|---|---|
| 제공 판 전체 읽기 | 29파일·137 splits, 44.5M행 입력 | 판 분할 하나 ≈15파일, 39.9M행 |
| 시군구 하나(PNU 범위) | 9 splits | 판 안 1~2파일 |
| 다음 판 적재 | 해시, 파일마다 전국 | 판 분할에 PNU 범위로 쌓임 |

- 다시쓰기 비용(계산, 실행 안 함): 14.93 GB 읽기 + 약 14.93 GB 쓰기. R2 내려받기 무료, 요청은 파일 약 30개
  분량(1센트 미만), 만료 전까지 겹쳐 남는 14.93 GB × $0.015/GB·월 ≈ $0.22. Spark 시간은 재지 않았다.
- 다음 판 적재는 range 셔플로 판 하나(≈7.5 GB)를 정렬한다. 해시 쓰기보다 셔플이 더 든다.
  적재는 판마다 한 번이다.
- 시험: `test_parcel_boundaries_layout_iceberg.py`(진짜 Iceberg: 판이 섞인 분할 없는 표 → 진화 → 새 판 append
  → 다시쓰기 뒤 파일마다 판 하나, 판 안 PNU 경계 안 겹침), `test_platform_contracts`·`test_lakehouse_schema_migrate`
  (빠진 필드만 더하고 지우지 않음), Rust 계약 시험.
- 공매 조인처럼 `pnu` 조건 없이 조인하는 읽기는 판 가지치기만 얻는다. 파일 건너뛰기까지 얻으려면 읽는 쪽이
  PNU 조건을 걸어야 하고, 그것은 이 ADR 범위 밖이다.
