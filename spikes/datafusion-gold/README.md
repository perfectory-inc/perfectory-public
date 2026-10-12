---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-10-12
---

# DataFusion·DuckDB Gold 스파이크

건물 패널 Gold 빌드를 DataFusion 과 DuckDB 로 옮겨 Spark 와 시간·메모리·출력 동치를 잰 스파이크다. 결론과 숫자는
[ADR-0192](../../docs/adr/0192-gold-builds-stay-on-spark-a-rust-engine-is-measured-again-when-iceberg-rust-can-overwrite.md)
에 있다. 어디에도 연결하지 않으며 자체 workspace 다(CI 밖).

## 다시 재는 법

1. 빌드: `CARGO_TARGET_DIR=<공유 target> cargo build --release` (DuckDB 를 번들로 컴파일해 첫 빌드가 길다).
2. 합성 입력: `python bench/generate.py --out <dir>/s01 --scale 0.1` (pyarrow·numpy; 실데이터 없음, PNU 는 99999 예약 대역).
3. 실행: `python bench/measure.py spark|datafusion|duckdb --data <dir>/s01 --out <dir>/out-x --results results.jsonl`.
   가격 없는 판은 `--no-prices` 를 붙이고, Spark 는 `--job` 으로 PR #430 의 잡 파일을 준다. 메모리 한도는
   `--df-memory-gib`. Spark 는 pyspark 3.5 와 JDK 가 필요하고, Windows 에서는 `HADOOP_HOME`(winutils) 도 필요하다.
4. 비교: `python bench/measure.py compare <spark 출력> <다른 엔진 출력>` — 모든 행·칸이 같아야 `IDENTICAL`.

2026-10-12 측정 원본은 `bench/results-2026-10-12.jsonl` 이다.
