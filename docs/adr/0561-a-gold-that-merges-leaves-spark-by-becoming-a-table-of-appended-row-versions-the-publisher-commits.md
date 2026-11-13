# ADR 0561: MERGE 로 갈아 끼우는 Gold 는 바뀐 행을 쌓는 표가 되어 Spark 를 떠난다 — 아직 태어나지 않은 두 Gold 에서 먼저 잰다

- Status: Accepted
- Date: 2026-11-12 (권고), 2026-11-14 (수락)
- Builds on: [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md) §4,
  [ADR-0560](./0560-the-publisher-commits-duckdb-staged-parquet-with-iceberg-rust-fast-append-and-only-tables-whose-commit-is-an-append-move-off-spark.md)(Consequences: "Gold 의 커밋 모양을 따로 결정해야 한다"),
  [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)(행 판),
  [ADR-0344](./0344-gold-merges-write-only-the-changed-rows-as-merge-on-read-deletes-that-compaction-folds.md)(바뀐 행만 쓰는 MERGE),
  [ADR-0347](./0347-unit-ids-and-link-evidence-are-named-by-content-and-every-gold-commit-records-its-changed-keys.md)(`__changes`)
- 받아들여지면 고치는 것(옮겨지는 표에 한해): ADR-0344 의 merge-on-read 커밋, ADR-0347 의 `__changes` 쓰기, ADR-0286 의 그 표 분류.
- 결정한 사람: 사장(2026-11-14, 조정자 전달: "1번은 대기업 수준으로 해" — 길 ⓐ, 바뀐 행을 쌓는 Gold 와 발행기 커밋).
  이 문서는 조정자가 요청한 권고였고(W75c), 수락과 함께 범위가 넓어졌다: 사장은 아직 없는 두 표만이 아니라 `MERGE` 로 쓰는 Gold 전부와
  덮어쓰는 Gold 여섯을 이 길로 정했다. 그래서 아래 Decision 2 번("기존 `Replaced` Gold 는 이 문서로 정하지 않는다")은 수락 시점에
  "옮긴다, 순서와 방법은 [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)"로
  읽는다. 3 번의 전제들을 어떻게 채우는지도 그 문서가 정한다.
- 수락 시점에 달라진 사실(2026-11-14 실측): `gold.parcel_real_transaction` 은 2026-11-12 14:00Z 에 태어났고(12,094,440 행) 11-13 부터
  서빙된다. "아직 태어나지 않은 두 Gold" 가운데 지금도 없는 것은 `gold.parcel_nearby` 하나다. ADR-0564 가 이 차이를 다룬다.

## 사장에게: 세 줄

- 권고: `MERGE` 로 쓰는 Gold 를 "바뀐 행을 쌓기만 하는 표"로 바꾸면 발행기가 커밋할 수 있어 Spark 가 필요 없다. **먼저 아직 없는 두 표
  (`gold.parcel_nearby`, `gold.parcel_real_transaction`)에서만** 그렇게 하고, 나머지 Gold 는 그 둘을 잰 뒤 따로 정한다.
- 값: 그 표를 읽는 모든 곳이 "키마다 가장 새 행"만 골라 읽어야 한다. 그냥 조회하면 옛 행과 지워진 키가 함께 보인다. 옛 행은 접는 작업을
  따로 만들기 전에는 표에 남는다.
- 받아들이지 않으면: `MERGE` 로 쓰는 Spark 파일 18 개는 계산만 DuckDB 로 옮기고 커밋은 Spark 에 남는다.

## Context

ADR-0560 으로 발행기는 Iceberg 에 **추가**를 커밋한다. 첫 표를 옮기는 PR(#1366·#1367)이 들어가면 장부는 57 → 56 이 된다. 그 뒤 남는
56 줄 가운데 **18 줄이 `MERGE` 를 쓴다**(스캔: `infra/lakehouse/spark/jobs/*.py`). 모양은 셋이다.

| 모양 | 파일 | 무엇을 하나 |
|---|---|---|
| 증분 Gold 생산자 | `build_or_merge(` 를 부르는 16 개 | 아래 "지금의 Gold 커밋" |
| 곁가지 `MERGE` | `industrial_complex_silver_to_gold` | 본 쓰기는 덮어쓰기이고 `MERGE` 는 갱신만 한다 |
| 원천에 없는 키를 지우는 `MERGE` | `place_reference_queue_to_gold` | `WHEN NOT MATCHED BY SOURCE … DELETE` |

표 계약으로는 Gold 33 개 가운데 29 개가 `row_versions: replaced` 다(나머지: `latest_per_key` 2, `events` 1, `unwritten` 1). 덮어쓰기만
하는 `replaced` Gold(서빙 경계 셋, `place_id`, `lineage_review_queue` 등)는 18 줄 밖이고 이 문서가 다루지 않는다.
`real_transaction_silver_to_gold` 는 `gold.real_transaction_region_stats` 도 덮어쓰므로, 그 표의 커밋 모양이 정해지기 전에는 그 파일이
장부에서 빠지지 못한다.

**지금의 Gold 커밋.** `gold_incremental.build_or_merge` 는 계획하고 검사만 한다. 커밋은 그 뒤다: 증분이면 `main` 에 바로
merge-on-read `MERGE`(바뀐 행만 새 파일 + 위치 삭제, ADR-0344; 요약에 입력 핀·코드 판·바뀐 키 수), 전체면 `main` 덮어쓰기(내용 지문
포함), 내용이 같으면 빈 추가다. 가지를 쓰는 것은 코드 변경 공개(`publish_by_key`)뿐이다. 차분(`row_digest` 비교)과 쓰기가 한 Spark
`MERGE` 안에서 일어난다. 같은 세션이 `gold.<t>__changes` 에 그 커밋의 바뀐 키를 따로 덧붙인다.

**독자가 지금 기대는 것.** Rust 의 by-PNU 독자 셋은 한 키에 행이 둘이면 거부한다(`parcel_by_pnu_serving_export`·
`building_by_pnu_serving_export`·`by_pnu_serving_rebase`: "Gold snapshot carries more than one row"). "키별 최신 행" 규칙은 Spark 에
있다(`platform_contracts.current_rows`). Rust 에는 계약 값(`current_row_latest()`)과 Trino SQL 을 만드는 함수뿐이고, 스캔한 행에
규칙을 적용하는 코드는 없다. 지금의 독자 가드(#809)는 `jobs/*.py` 를 모듈 단위로 본다.

**길마다 확인한 것(2026-11-12).**

| 길 | 근거 | 잰 것인가 |
|---|---|---|
| ⓐ Gold 를 **바뀐 행을 쌓는 표**로(ADR-0286 `LatestPerKey`, 사라진 키는 삭제 표시 행), 발행기가 추가로 커밋 | 추가 커밋은 실제 R2 카탈로그에서 증명됐다(ADR-0560 탐침). 계약 어휘는 있고 Silver 84 표와 Gold 2 표(`land_use_zone_area_by_tile`, `complex_document_facts`)가 그 모양이다. by-PNU 로 굽는 큰 Gold 에는 적용해 본 적이 없다 | 커밋 길: 잼. Gold 에 적용: 재지 않음 |
| ⓑ 발행기에 **행 단위 삭제를 싣는 커밋**(iceberg-rust `RowDelta`) | 0.11.0 소스(crates.io 의 `iceberg-0.11.0`)에 `RowDelta` 동작도 위치 삭제 **쓰기**도 없다. `Transaction` 이 내는 동작은 여덟(판 올림·표 속성·스키마·추가·정렬·위치·통계·만료)이고, 위치 삭제는 읽는 쪽(`arrow/reader/positional_deletes.rs`, `delete_file_index.rs`)뿐이다. 커밋 본문 builder 가 `pub(crate)` 다. 상류 날짜는 없다 | 소스를 읽음 |
| ⓒ **DuckDB 가 `MERGE` 를 커밋** | DuckDB 2.0.0 은 v2 표에 위치 삭제를 쓴다(ADR-0559). 표가 `Replaced` 그대로라 독자를 바꿀 필요가 없다. 그러나 스냅샷 요약 속성과 태그를 못 쓴다. 가지 커밋은 재지 않았다 | 일부 잼 |
| ⓓ **pyiceberg** 로 커밋 | 문서도 소스도 읽지 않았다 | 재지 않음 |
| ⓔ 계산만 옮기고 **Spark 가 커밋**(지금의 생산자마다) | 장부가 18 줄 밑으로 내려가지 않는다. 합치기만 하는 세션의 메모리는 재지 않았다(ADR-0559 §4) — 2026-11-11 의 실패가 조인과 추론에서 났다면 ⓔ 만으로 실패는 없어질 수 있다 | 재지 않음 |
| ⓕ 준비된 Parquet 을 `MERGE` 하는 **공용 Spark 커밋 작업 하나** | 18 줄이 1 줄이 되고 독자·계약·지문이 그대로다. Spark 세션과 JVM 은 남는다 | 재지 않음 |

**ⓐ 가 쌓는 양(계산, 아래쪽 추정).** ADR-0344 가 잰 `gold.parcel_panel` 은 행당 101.9 바이트이고 월 단위 토지 판 하나가 48,477 행(0.12%)을
바꾼다: 판 하나 약 4.9 MB, 열두 판 약 59 MB(표 4.266 GB 의 1.4%). 지금의 merge-on-read 커밋(약 5.3 MB)과 같은 자릿수다. 작게 흩어진
추가는 큰 정렬 파일보다 덜 압축되므로 실제는 이보다 크다. 날마다 바뀌는 표(실거래·경매)는 재지 않았다.

## Decision (권고)

1. **`gold.parcel_nearby` 와 `gold.parcel_real_transaction` 은 처음부터 `LatestPerKey` 로 태어나고, 발행기가 추가로 커밋한다.** 두 표는
   카탈로그에 아직 없다(ADR-0559). 다시 구울 문서도 옮길 데이터도 없다. 생산자·굽기 레인·재생성 작업의 코드는 이미 있으므로 그것들은 고친다.
2. **기존 `Replaced` Gold 는 이 문서로 정하지 않는다.** 1 번의 두 표에서 굽기 시간, 샤드가 읽는 파일 수, 죽은 판 비율, 독자를 옮긴 비용을
   잰 뒤 따로 결정한다. 그때까지 그 생산자들은 계산만 옮길 수 있다(ADR-0560 §5).
3. **1 번의 전제 — 이것들이 없으면 첫 표를 시작하지 않는다.**
   - **순서 칸.** 모든 쌓는 Gold 에 발행기가 커밋마다 매기는 필수·고정 폭·단조 증가 칸을 `newest_first` 로 둔다(`keep_ties` 없음).
     `published_at_utc`(문자열)나 스냅샷 id(시간순이 아니다, ADR-0286)에 기대지 않는다. 같은 순서 값의 두 행은 `current_rows` 가 거부한다.
   - **삭제 표시 행.** 삭제 표시 칸의 이름과 형, 삭제 표시 행에서 필수 내용 칸과 `row_digest` 가 갖는 값을 계약에 적는다
     (지금 두 표의 계약은 `replaced`, 키 `pnu`, 삭제 표시 칸이 없고 내용 칸이 필수다).
   - **Rust 독자의 규칙.** `lakehouse_snapshot_scan` 이 스캔한 행에 키별 최신·삭제 표시를 적용한다(메모리 상한은 현재 행이 아니라 읽은 행
     기준으로 다시 잰다). by-PNU 독자 셋의 "한 키에 한 행" 거부는 그 규칙을 거친 뒤의 행에 대한 것이 된다. 독자 가드를 표 단위로 좁히고
     Rust 독자를 덮게 한다. 막는 사건: 2026-10-16 `silver.industrial_complexes` 가 현재 행 규칙 없이 세 벌(4,326 행)로 읽혔다.
   - **차분과 붙이기 사이.** 발행기의 DuckDB 가 고정한 스냅샷 S 의 현재 행과 `row_digest` 로 차분한다. 붙이기는 `main` 의 머리가 아직
     S 일 때만 한다. 아니면 그 run 은 실패하고 다음 run 이 다시 차분한다(라이브러리의 "바뀐 기준 위에 다시 적용"은 눈먼 추가에만 안전하다).
     표마다 쓰는 작업은 하나다(ADR-0560 §6).
   - **재실행 가드.** 머리의 입력 핀과 코드 판이 이 run 의 것과 같으면 붙이지 않는다. 응답을 잃은 커밋 뒤의 재시도가 같은 판을 두 번
     쌓지 않게 한다(두 번 쌓이면 모든 키가 같은 순서 값으로 겹쳐 독자 전부가 멈춘다).
   - **되돌리기.** 나쁜 추가 뒤 `main` 을 부모 스냅샷으로 되돌리는 동작(ADR-0560 §8 ③: 아직 없다).
   - **검사의 자리.** 지금도 증분 검사는 커밋 전에 돈다. 준비 파일만으로 되는 검사(`lane.validate`)와 S 의 현재 행이 있어야 하는 검사
     (변경 비율 상한, 대조 표본, 기대 행 수와 행 하한, 섹션 포함, 내용 지문)를 나눠 적고, 둘 다 발행기의 DuckDB 에서 돈다.
4. **행 수와 지문의 뜻.** 쌓는 Gold 에서 `foundation.content-fingerprint` 는 **현재 행**의 지문이고, 현재 행 수는 이름 있는 요약 속성으로
   싣는다. `total-records` 를 키 수로 읽는 곳(`gold_rebuild` 의 행 하한·변경 비율, `by_pnu_panel_delta.live_rows`, 샤드 계획, 재고)은 그
   속성으로 옮긴다. Gold 를 계약 규칙 없이 읽는 곳(`by_pnu_panel_delta` 의 `pnu` 조인, `by_pnu_kind_sizes`, `section_coverage`)은 규칙을 거친다.
   옮기는 PR 이 그 독자를 전부 찾아 스캔 범위와 함께 적는다.
5. **`__changes`.** 쌓는 표에서는 그 커밋이 붙인 행이 곧 바뀐 키다. 옮기는 PR 이 `__changes` 를 발행기의 둘째 추가로 남길지, 그 표에서
   없앨지를 정해 적는다.
6. **옛 판을 접는 다시 쓰기는 만들지 않는다.** 접기는 행을 지우는 쓰기라 지금 허락된 쓰기 의도가 없다. 파일만 합치는 합치기는 판을 그대로
   둔다. 접기를 다시 볼 때: 표의 죽은 판 비율((매니페스트 행 − 현재 행) / 매니페스트 행)이나 샤드당 굽기 시간이 옮기기 전 기준을 넘는 것이
   잰 값으로 보일 때다. 문턱 값은 1 번의 두 표를 잰 PR 이 적는다.
7. **기존 표를 옮기게 될 때의 순서**(2 번이 그렇게 정하면): 독자는 표가 아직 `Replaced` 인 동안 규칙을 거치게 옮긴다. 계약 변경과 생산자
   이동은 한 배포다 — `admit_write` 가 `replaced` 가 아닌 표의 `replace` 를 거부하므로 계약만 먼저 바꾸면 지금의 생산자가 멈춘다.

## Consequences

- 받아들여지면 첫 물결의 두 작업(주변·실거래 Gold)이 커밋까지 Spark 를 떠날 길이 생긴다. 다만 둘 다 아직 막혀 있다: 실거래는 문서 조립과
  지역 통계의 이식·전국 판정이 없고, 주변은 시설 Silver 여섯에 스냅샷이 없다(ADR-0560).
- 쌓는 Gold 를 그냥 조회하면(운영자·임시 질의) 옛 판과 삭제 표시 행이 보인다.
- by-PNU 굽기는 접두 하나의 모든 판을 읽는다. 전국에 흩어진 추가 파일은 PNU 경계로 건너뛸 수 없어 샤드마다 열린다 — 1 번의 두 표에서 잰다.
- 쌓는 표에서 스냅샷 만료는 자리를 돌려주지 않는다(옛 판이 현재 스냅샷의 행이다).
- 이 권고는 Gold 가 다시 만들 수 있는 파생 표라는 점을 바꾸지 않는다. "쌓기만 한다"는 원칙을 Gold 까지 넓혀 읽은 것은 이 문서의 해석이다.

## 하지 않은 것, 확인하지 못한 것

- 아무것도 짓지 않았다. Gold 하나를 쌓는 표로 바꿔 굽기 시간과 독자를 잰 것이 아니다.
- DuckDB 의 가지 커밋, pyiceberg, 합치기만 하는 Spark 세션의 메모리, R2 관리형 합치기가 판을 쌓는 표에서 하는 일.
- 응답을 잃은 추가 커밋 뒤 라이브러리 재시도가 같은 파일을 두 번 붙이는지(중복 파일 검사가 막을 것이나 재지 않았다).
- 두 표의 파티션 명세를 그대로 둘지(커밋마다 새 파티션이면 합치기가 판을 넘어 파일을 합치지 못한다).
- 설계 검토: EGM 검토자 넷(설계·코드 대조·구현 준비·범위)이 한 번 검토했다(차단 4·주요 11·경미 7). 반영: 지금의 Gold 커밋 서술(가지가
  아니라 `main` 직접), Rust 독자에 규칙이 없다는 사실, 순서 칸·삭제 표시·차분과 붙이기 사이·재실행 가드·되돌리기를 전제로 올림, 권고를
  두 표로 좁힘, 지문과 행 수의 뜻, 빠진 독자, 가드의 범위, 계약 변경의 순서, 세 모양, 접기, 대안 ⓒⓓⓔ 의 근거와 빠진 대안 ⓕ, 숨은 결정,
  크기 계산. 남긴 것: iceberg-rust 소스 줄의 영구 링크, 두 표의 삭제 표시 칸 이름(전제로만 적었다).

## 기각한 대안

- **ⓑ 를 기다리거나 손으로 짓는다.** 상류에 날짜가 없고, 손으로 지으면 라이브러리가 막아 둔 것(`pub(crate)`)을 포크로 여는 일이다
  (AGENTS.md §해결 접근 순서 5). 상류가 `RowDelta` 를 내면 2 번을 정할 때 다시 본다.
- **ⓒ DuckDB 가 `MERGE` 를 커밋한다.** 독자를 바꾸지 않아도 되는 유일한 길이다. 그러나 요약 속성을 못 써서 입력 핀·코드 판·바뀐 키 수·
  내용 지문을 읽는 독자(증분 계획, by-PNU 델타, 재생성 계획)를 전부 다른 기록으로 옮겨야 하고, 그 기록을 표 속성에 따로 적으면 두 커밋이
  된다. 가지 커밋이 된다면 기존 참조 옮기기로 한 커밋에 묶을 수 있을지도 모른다 — 재지 않았으므로 "기각"이 아니라 "재지 않음"으로 남긴다.
- **ⓓ pyiceberg.** 발행기 밖에 Python 실행기가 하나 더 생긴다. 그 이상은 확인하지 않았다.
- **ⓔ·ⓕ Spark 가 커밋한다.** 실패는 없앨 수 있을지 모르나 사장이 정한 "Spark 는 표 유지보수만"(ADR-0559)에 닿지 못한다. 1 번의 두 표를
  재는 동안 기존 Gold 가 머무는 자리가 ⓔ 다.
