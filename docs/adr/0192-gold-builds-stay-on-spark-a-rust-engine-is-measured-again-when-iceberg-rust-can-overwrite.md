# ADR 0192: Gold 빌드는 Spark 에 둔다 — Rust 엔진은 iceberg-rust 가 덮어쓰기·브랜치를 할 수 있을 때 다시 잰다

- Status: Proposed
- Date: 2026-10-12
- Spike: `spikes/datafusion-gold/` (어디에도 연결하지 않는다, 병합하지 않는 draft PR)
- Relates to: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분 Gold 의 copy-on-write MERGE),
  [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md)(브랜치 발행), ADR-0189(append 경로), ADR-0190(가격 분리, PR #428–430)

## Context

Gold 빌드는 서버 한 대(20코어·62 GB 공유)에서 Spark 3.5 `local[8,8]`, 16g JVM 으로 돈다. 건물 패널은 입력 약 2e8 행에서
한 시간 가까이 걸렸고 JVM 힙·Arrow 2 GiB 한계에 부딪혔다. 2026-10-12 운영(append 빠른 경로 이후) 건물 Gold:
affected 57 s, restrict 349 s, build 531 s, parity 148 s. 사장 지시는 Rust 우선이다. 물음: 단일 노드 Rust 엔진
(Apache DataFusion + iceberg-rust)이나 DuckDB 가 이 Gold 빌드에 **눈에 띄게 빠르고 가벼우며**, 우리 Iceberg 표
(R2 의 REST 카탈로그)를 **안전하게 읽고 쓸 수** 있는가 — ADR-0180 의 copy-on-write MERGE 와 ADR-0183 의 브랜치 발행까지.

### 무엇을 쟀나

- `building_panel_silver_to_gold.py` 의 `build_gold_panel_frame` 과 그 검사 전부를 DataFusion 55.2 SQL 로 옮겼고,
  같은 SQL 을 DuckDB 1.5.6(`duckdb` crate `=1.10506.0`, 플랫폼이 고정한 판)으로도 돌렸다(구문 차이만 갈랐다).
  Spark 는 운영 잡을 **그대로**(`--input-mode parquet --write-mode parquet`) 돌렸다. 가격 없는 판은 PR #430 의 잡
  (패널 v3)과 `SPIKE_NO_PRICES=1` 로 비교했다.
- 입력은 생성기(`bench/generate.py`)가 만든 합성 Parquet 이다. 실데이터·R2·서버는 쓰지 않았다. 행 수는 운영
  비율 그대로(표제부 7.6e6, 호 1.98e7, 면적 1.14e8, 가격 2e8 이 scale 1), 아파트 블록 쏠림까지 흉내 냈다.
  scale 0.1 = 표제부 76만·층 228만·호 197만·면적 1,120만·가격 1,967만 행(Parquet 254 MB).
- 노트북: 8 스레드, RAM 20 GB(측정 중 여유 5–9 GB). Spark 는 driver 8g. 벽시계·프로세스 트리 최대 RSS·CPU 초를
  `bench/measure.py` 가 잰다. 결과 원본: `spikes/datafusion-gold/bench/results-2026-10-12.jsonl`.
- **동치**: 모든 성공 실행을 Spark 출력과 `pnu, buildings_json, unlinked_units_json, row_digest, source_snapshot_id`
  전 칸 행 단위로 비교했다. 일부러 다른 출력(가격 있는 Spark 대 가격 없는 DataFusion)을 넣으면 525행 불일치로
  거부되는 것도 확인했다.

### 결과

| scale | 변형 | Spark 8g | DataFusion (풀 무제한) | DuckDB (기본 한도 = RAM 80%) |
|---|---|---|---|---|
| 0.025 | 가격 포함 | 100 s · 5.6 GiB · CPU 307 s | **18 s · 4.8 GiB** · 53 s · 동일 | 30 s · 6.4 GiB · 73 s · 동일 |
| 0.025 | 가격 없음 | 90 s · 4.1 GiB · 261 s | **12 s · 3.1 GiB** · 31 s · 동일 | 19 s · 3.3 GiB · 49 s · 동일 |
| 0.05 | 가격 포함 | 127 s · 8.0 GiB · 442 s | 53 s · 5.5 GiB · 182 s · 동일 | **실패**(메모리) |
| 0.05 | 가격 없음 | 101 s · 6.2 GiB · 340 s | 25 s · 4.1 GiB · 82 s · 동일 | 30 s · 6.6 GiB · 93 s · 동일 |
| 0.1 | 가격 포함 | 159 s · 8.8 GiB · 678 s | **실패**(10.2 GiB 에서 할당 실패) | **실패**(12.4 GiB) |
| 0.1 | 가격 없음 | 120 s · 8.7 GiB · 495 s | 45 s · 5.5 GiB · 183 s · 동일 | **실패**(11.3 GiB) |

메모리를 제한한 실행:

| 실행 | 결과 |
|---|---|
| DataFusion 0.1, `FairSpillPool` 4 GiB, 가격 없음 / 포함 | 둘 다 `ResourcesExhausted … FinalHashAggregateStream`. 가격 포함은 풀이 4 GiB 인데 RSS 11.0 GiB |
| DuckDB 0.05 가격 포함: `memory_limit` 6 GiB / `preserve_insertion_order=false` / 스레드 4 | 셋 다 Out of Memory |

### 읽는 법

1. **작은 입력의 큰 배수는 대부분 JVM·Spark 기동비다.** 가격 없는 판에서 scale 0.025→0.1 기울기는 DataFusion
   약 437 s/scale, Spark 약 391 s/scale 로 비슷하고, 차이는 고정비(Spark 약 80 s, DataFusion 약 1 s)다. 선형으로
   늘이면 scale 1(운영 크기)에서 둘 다 노트북 8스레드로 7–8분대다. 가격 포함 판은 DataFusion 이 0.025→0.05 에서
   2.9배로 늘어(초선형) 0.1 에서 죽었다.
2. **두 Rust 쪽 엔진 모두 이 빌드를 디스크로 흘려 보내지(spill) 못한다.** 건물 패널의 본체는 PNU·건물별
   `array_agg` 로 호·층·가격을 **중첩 배열**로 묶는 집계다. DataFusion 은 그 최종 해시 집계가 풀 한도에 걸리면
   흘리지 못하고 실패하며, 중첩 배열·JSON 직렬화 메모리는 풀 밖에서 자라 RSS 가 풀의 2.7배였다. DuckDB 는 한도를
   어떻게 줘도 0.05 가격 포함부터 실패했다. Spark 는 같은 데이터에서 8g 힙으로 끝까지 갔다(셔플·spill).
   가격 없는 판 DataFusion 의 최대 RSS 는 scale 당 약 31 GiB 로 늘었다 → 운영 크기에서 약 33 GiB, 공유 서버
   62 GB 에서 다른 잡과 함께 돌릴 수 없다.
3. **DataFusion 55 의 순서 있는 `array_agg(… ORDER BY …)` 는 중첩 구조에서 5.6 MB 입력도 끝내지 못했다**
   (4분 뒤 메모리 할당 실패). 고친 PR(#24392 열 단위 상태, #25497)은 main 에만 있고 56 에서 나온다.
   스파이크는 `array_sort(array_agg(struct))` 로 돌아갔다 — 각 구조체의 첫 필드가 유일한 정렬 키라 순서가 같다
   (Spark 잡의 `sort_array(collect_list(struct))` 와 같은 꼴).
4. **동치는 된다.** 성공한 실행은 전부 Spark 와 행·바이트 단위로 같았다(`row_digest` 포함). Java `Double.toString`
   을 흉내 내는 JSON 인코더가 필요했다(DataFusion). DuckDB 의 JSON 은 1e7 이상·1e-3 미만 실수를 Java 와 다르게
   쓴다 — 합성 데이터엔 그런 값이 없어 같았을 뿐이다.

### Iceberg 쓰기 (2026-10 현재, 출처는 PR 본문)

| 필요한 것 | iceberg-rust 0.10.1 / main | DuckDB iceberg 1.5.x | Spark 3.5 |
|---|---|---|---|
| REST 카탈로그·S3 호환 엔드포인트 | 있음(R2 는 공식 예시 없음, 멀티파트 이슈 #1406 미해결 종료) | 있음, R2 문서 있음 | 있음 |
| 스냅숏 id 로 읽기 | 있음 | 있음 | 있음 |
| append + 스냅숏 summary 속성(`foundation.source-iceberg-snapshots`) | append 에만 있음 | append 있음, **속성 없음**(#1249) | 있음 |
| overwrite / copy-on-write 재작성(ADR-0180 MERGE) | **없음**(Overwrite #2185·#3300, RewriteFiles #3046 열림) | merge-on-read 만(삭제 파일 누적) | 있음 |
| 태그·브랜치 생성, 브랜치에 커밋 후 main 빨리감기(ADR-0183) | **없음**(#3150, 커밋은 main 고정) | **없음**(#1252) | 있음 |
| DataFusion SQL 쓰기 | `INSERT INTO`(append)만, 파티션 쓰기 버그(datafusion-iceberg #35) | — | — |

우리 Gold 커밋은 summary 속성 + copy-on-write 덮어쓰기 + 브랜치 발행 세 가지를 모두 쓴다. 오늘 어느 Rust 경로도
셋 중 둘을 못 한다.

## Decision

1. **Gold 빌드는 Spark 에 둔다. DataFusion·DuckDB 로 옮기지 않는다(현재).** 근거는 위 결과 2(중첩 집계를 흘리지
   못해 운영 크기에서 메모리가 모자람)와 Iceberg 표(덮어쓰기·브랜치 부재)다. 작은 입력에서 본 5–8배는 기동비라
   운영 크기의 이득이 아니다.
2. **다시 재는 조건 — 셋이 모두 갖춰지면 이 스파이크를 다시 돌린다**:
   - iceberg-rust 가 overwrite(또는 RewriteFiles)·스냅숏 속성·브랜치 커밋/참조 관리를 릴리스한다;
   - DataFusion 56 이상(열 단위 순서 있는 `array_agg`)에서 같은 빌드가 `FairSpillPool` 한도 안에서 끝난다 —
     또는 빌드를 PNU 묶음별로 나눠 묶음 하나가 메모리에 들어가게 다시 짠다(단위가 연결된 건물의 PNU 로 옮겨 가는
     규칙 때문에 단순 PNU 해시 분할로는 안 되고, `restrict_inputs` 같은 입력 축소가 필요하다);
   - scale 0.25 이상을 노트북이 아닌 서버급 기계에서 재어, 운영 크기에서도 벽시계·메모리 이득이 남는다.
3. **먼저 옮겨 볼 후보는 읽기만 하는 작업이다.** 스냅숏 id 로 읽기는 iceberg-rust 가 오늘 된다. parity 검사(148 s)처럼
   Iceberg 를 읽어 비교만 하고 쓰지 않으며 중첩 배열을 만들지 않는 작업이 첫 후보다 — 단 그 자체의 측정을 먼저 한다.
4. **DuckDB 는 Gold 빌드 후보에서 뺀다.** 이 작업 꼴(중첩 배열 집계)에서 가장 먼저 메모리로 실패했고 Iceberg 쪽도
   스냅숏 속성·브랜치가 없다.
5. 스파이크 코드는 `spikes/datafusion-gold/`(자체 workspace, CI·영역 밖)에 두고 draft PR 로만 남긴다. 저장소의
   cargo-deny 설정(`platforms/foundation-platform/deny.toml`)으로 advisories·bans·licenses·sources 모두 통과한다
   (DataFusion 의 `compression` 기능은 bzip2 라이선스 때문에 껐다).

## Consequences

- 서버 Gold 의 메모리 압박은 엔진 교체가 아니라 Spark 쪽(증분 범위 축소 ADR-0180/0189, 가격 분리 ADR-0190)으로 푼다.
- 측정은 노트북 하나·합성 데이터·scale ≤ 0.1 이다. 운영 크기로의 연장은 선형 가정이고, 서버의 20코어·디스크에서
  기울기가 다를 수 있다. 결정 2 의 재측정이 그 빈자리를 메운다.
- 스파이크가 드러낸 DataFusion 함정 둘(순서 있는 중첩 `array_agg`, 풀 밖에서 자라는 중첩 배열 메모리)은 재측정
  때 먼저 확인할 항목이다.
