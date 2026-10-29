# ADR 0319: by-PNU 읽기는 Parquet 파일을 통째로 받지 않는다 — 꼬리(footer)와 디코딩할 row group·페이지의 바이트 범위만 받는다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md)(Gold 는 PNU 순서, 샤드는 자기 파일만), [ADR-0102](./0102-lakehouse-jobs-survive-r2-connection-resets.md)(R2 읽기는 범위 단위로 재시도)
- 관련: 부분 처리 프로그램(2026-10-28 감사 — "전량 대신 부분" TOP 목록), wave 2 V3

## Context

`lakehouse_snapshot_scan`(foundation-publisher-core)은 by-PNU 베이크·리베이스와 Silver 갱신이 Gold/Silver 행을 읽는 유일한 경로다(ADR-0040 결정 8). 이미 두 단계로 **덜 디코딩**한다.

1. 매니페스트 경계로 접두어 밖 파일은 열지 않는다(`ScannedDataFile::excludes_prefix`, ADR-0164).
2. 연 파일 안에서는 row group 통계로 접두어 밖 row group 을 디코딩하지 않고(`row_group_excludes`), 남은 row group 은 `pnu` 열만 먼저 걸러 나머지 열은 접두어 안 행만 디코딩한다.

그러나 **받는 바이트는 줄지 않았다.** `LakehouseByteReader::read` 가 파일을 16 MiB 창으로 **처음부터 끝까지** 받아 `Vec<u8>` 로 만든 뒤에야 위 가지치기가 돈다. 디코딩하지 않을 row group·열도 전부 내려받는다.

운영 실측(2026-10-28, Trino `"$files"`, 읽기 전용):

| 표 | 파일 | 합계 | 행 | 파일당 row group(평균) | 최대 파일 |
|---|---:|---:|---:|---:|---:|
| `gold.parcel_panel` | 9 | 4.06 GB | 39.86M | 3.78 | 487 MB |
| `gold.building_panel` | 14 | 1.97 GB | 5.95M | 1.86 | 152 MB |
| `gold.parcel_price` | 9 | 1.40 GB | 35.78M | 1.89 | 174 MB |
| `gold.unit_price_panel` | 9 | 448 MB | 47K | 1.0 | 59 MB |

`parcel_panel` 의 row group 하나는 평균 약 119 MB·117만 행이다. 샤드 상한은 90만 행(`MAX_ROWS_PER_RUN`)이므로, 샤드 하나는 자기 범위에 걸친 파일 1~2개(파일당 평균 451 MB)를 **통째로** 받아 그중 한두 row group 만 디코딩한다. 2026-10-14 kind 베이크의 775 샤드 중 72% 는 행이 하나도 없는 접두어였는데, 그 샤드들도 파일 하나를 통째로 받았다. "모든 처리는 자기 몫만 읽는다"는 사장 원칙(2026-10-28)에 어긋나는 마지막 전량 구간이다.

Parquet 은 처음부터 이것을 위해 설계됐다. 파일 끝의 footer 에 row group·열 청크의 바이트 위치와 통계가, (있으면) offset index 에 페이지 위치가 있다. 그래서 객체 저장소 위의 모든 실전 Parquet 리더는 같은 순서로 읽는다.

- **Trino** `ParquetReader`/`TrinoParquetDataSource`: footer 를 먼저 읽고, 살아남은 row group 의 열 청크만 범위로 읽으며, 가까운 범위는 병합한다(`parquet.max-merge-distance`, `parquet.max-buffer-size`).
- **Spark** `ParquetFileReader`(parquet-mr): footer → row group 필터 → 필요한 열 청크만 `readNextFilteredRowGroup`. 열 인덱스로 페이지까지 거른다.
- **DuckDB** `httpfs`: footer 범위 GET 후 projection·row group 통계로 필요한 열 청크만 HTTP range 로 읽는다.
- **arrow-rs** `ParquetObjectReader`/`ParquetRecordBatchStream`: footer 를 `with_prefetch_hint` 만큼 한 번에 받고, row filter 열을 먼저 받아 거른 뒤 나머지 열은 **선택된 행이 있는 페이지만**(offset index) 받는다. 그 IO-독립 핵심이 `ParquetPushDecoder` 다. DataFusion 은 footer 선행 읽기 기본값을 512 KiB(`metadata_size_hint`)로 둔다.

우리 작업 공간에는 이미 `parquet` 60(arrow-rs)이 있고, `ParquetPushDecoder`·`ParquetMetaDataPushDecoder` 는 `async`/`object_store` 기능 없이 기본으로 들어 있다. R2 쪽도 이미 `R2ObjectStorage::get_object_range`(재시도 포함)가 있다. **새 의존성 없이** 업계 표준 읽기를 그대로 쓸 수 있다.

## Decision

1. **스냅샷 스캔은 데이터 파일을 범위로만 읽는다.** `LakehouseByteReader` 에 `read_range(location, start..end)` 를 더하고, `scan_snapshot_rows_kept` 는 데이터 파일마다 `ranged_parquet::decode_rows_under` 를 쓴다. 통째 `read` 는 매니페스트 목록·매니페스트(Avro, 작다)에만 남는다.
2. **읽기 순서는 arrow-rs 의 것을 그대로 쓴다 — 두 번째 구현을 만들지 않는다.**
   - footer: 파일 끝 512 KiB 를 한 번에 받아 `ParquetMetaDataPushDecoder` 에 넣는다(DataFusion `metadata_size_hint` 와 같은 값). footer 가 더 길면 디코더가 요구하는 범위를 한 번 더 받는다. offset index 는 있으면 읽고(`PageIndexPolicy::Optional`), column index 는 읽지 않는다(row group 통계로 충분).
   - 본문: `ParquetPushDecoder` 에 기존과 **같은** row group 선택·`pnu` row filter(`iceberg_scan::keep_prefix`, 메모리 경로와 공유)를 걸고, 디코더가 `NeedsData(ranges)` 로 요구하는 범위만 받아 넣는다. 그러므로 (a) 통계로 제외된 row group 은 한 바이트도 받지 않고, (b) 남은 row group 은 `pnu` 열 청크를 먼저 받아 거르고, (c) 나머지 열은 걸러진 행이 있을 때만, offset index 가 있으면 **그 행이 든 페이지만** 받는다.
   - 파일 길이는 매니페스트의 `file_size_in_bytes` 다(Iceberg 는 쓴 길이를 기록한다). HEAD 요청이 필요 없고, 길이가 틀리면 footer 디코딩이 **실패**한다 — 엉뚱한 바이트를 읽지 않는다.
3. **요청 병합은 object_store 와 같은 1 MiB 간격.** 1 MiB 안쪽으로 붙은 범위는 한 요청으로 받되, 디코더에는 요청한 범위 그대로 잘라(`Bytes::slice`, 복사 없음) 넘긴다(디코더는 정확히 일치하는 범위만 해제한다). 16 MiB 를 넘는 범위는 ADR-0102 와 같은 16 MiB 창으로 나눠 창마다 재시도한다.
4. **받은 양을 센다.** `ScannedRows` 에 `read_file_bytes`(통째로 읽었다면 받았을 바이트), `fetched_bytes`(실제로 받은 바이트), `range_requests` 를 더하고, 스캔 끝에 `read the data files by byte range` 로그로 남긴다. 운영 효과는 이 로그로 샤드마다 측정한다.
5. **행과 행 수 회계는 바뀌지 않는다.** 범위 읽기는 메모리 읽기와 같은 행을 같은 순서로 넘기고, `decoded + skipped == footer 행 수` 와 `ScannedRows::ensure_complete`(ADR-0164)는 그대로 성립한다. 시험 `a_ranged_read_hands_over_the_rows_a_whole_read_does` 가 접두어 없음·전체·row group 하나·페이지 몇 개·행 없음 × 끝까지/중간 멈춤에서 둘을 비교한다.
6. **가드: 시험용 리더는 데이터 파일 통째 읽기를 거부한다.** `pruning_tests`·`ranged_parquet_tests` 의 리더는 `.parquet` 을 `read` 로 요구하면 실패한다. 막는 사건(ADR-0001): 2026-10-08 parcel 베이크 126 샤드가 각자 `gold.parcel_panel` 33 파일을 통째로 받았고, 2026-10-14 kind 베이크 775 샤드의 72%(행 없는 접두어)가 각각 파일 하나를 통째로 받았다 — 누군가 스캔을 다시 `read` 로 되돌리면 이 시험이 실패한다.

## Consequences

계산값(운영 실측 파일 모양 × 이 설계; 실측 아님 — 배포 뒤 4의 로그로 확인):

| 샤드 | 전(통째) | 후: offset index 없음 | 후: offset index 있음 |
|---|---:|---:|---:|
| `parcel_panel` 90만 행 샤드 | 파일 1~2개 ≈ 451~902 MB | footer + row group 1~2개 ≈ 120~240 MB | footer + `pnu` 청크(수 MB) + 그 행의 페이지 ≈ 90만/3,986만 × 4.06 GB ≈ 92 MB → **약 0.1 GB** |
| 행 없는 접두어 샤드(2026-10-14 의 72%) | 파일 1개 ≈ 451 MB | footer + `pnu` 청크 하나 ≈ 수 MB | 같음 ≈ 수 MB |

시험 픽스처(8 row group × 4,000행, `label` 256 B, 페이지 250행, 약 9 MB) 계산값: row group 하나 샤드는 footer 512 KiB + row group 약 1.1 MB ≈ 1.7 MB(파일의 약 18%), 1,000행 샤드는 footer + `pnu` 청크 약 92 KB + `label` 페이지 4개 약 260 KB ≈ 0.9 MB. 시험은 "row group 3 밖·footer 밖 바이트를 하나도 받지 않음", "받은 양 < 파일의 1/4", "1,000행 샤드는 row group 바이트의 절반 미만"을 CI 에서 단언한다.

- 메모리도 준다: 파일 전체 `Vec<u8>` 대신 디코딩할 열 청크(또는 페이지)만 잡는다.
- 요청 수는 는다(파일당 footer 1 + row group·열 병합 단위). 지금은 한 파일의 범위를 순서대로 받는다. 동시 요청(Trino/arrow-rs 처럼 row group 선행 읽기)은 지연이 문제로 측정되면 후속.
- footer 가 512 KiB 보다 짧은 작은 파일(`parcel_public_auction` 0.2~0.3 MB 등)은 꼬리 한 번에 파일 전체를 받는다 — 이전과 같은 바이트, 요청은 1회.
- `read_range` 는 트레잇 필수 메서드다. 구현이 셋(운영 R2, 시험 둘)뿐이고, 기본 구현(통째로 받아 자르기)을 두면 새 리더가 조용히 전량으로 돌아가므로 두지 않는다.

## Alternatives rejected

- **`parquet` 의 `async` 기능 + `object_store` 로 `ParquetObjectReader` 를 그대로 쓰기** — 같은 디코더 위의 껍데기인데, R2 접속·재시도·자격 증명을 우리 `R2ObjectStorage`(ADR-0102 재시도, 읽기 전용 자격 분리)와 별개로 하나 더 세운다. 새 의존성·기능 플래그(workspace-hack 재생성)도 든다. push decoder 는 IO 를 우리 것으로 두고 계획만 arrow-rs 에 맡긴다.
- **footer·열 청크 범위를 직접 계산하는 자체 planner** — row filter 뒤 페이지 선택, 사전 페이지, offset index 해석을 다시 구현하게 된다. arrow-rs 디코더가 이미 하는 일이고, 두 구현은 언젠가 어긋난다(SSOT).
- **HEAD 로 길이를 받아 쓰기** — 파일마다 요청 하나가 더 들고, 매니페스트가 이미 정확한 길이를 갖고 있다.
- **Gold 를 더 잘게(row group 을 작게) 다시 쓰기** — 쓰기 쪽 변경은 별개 이득이 있으나, 읽기가 통째로 받는 한 row group 크기와 무관하게 파일 전체를 받는다. 읽기를 먼저 고친다.

## Open

- 운영 Gold 파일에 offset index 가 실제로 있는지는 확인하지 못했다(파일 footer 를 읽으려면 R2 자격이 필요하고, 이 작업은 비밀을 열지 않는다). 없으면 "offset index 없음" 열의 값이 된다 — 그래도 row group 단위로 받는다. 배포 뒤 4의 로그로 판정한다.
- 한 파일 안의 범위 동시 요청, 파일 간 선행 읽기.
