# ADR 0557: 변환과 조회는 발행기 안의 DuckDB 가 하고 Spark 에는 표 유지보수만 남는다 — 변환은 엔진 동등성 판정으로만 옮긴다

- Status: Accepted
- Date: 2026-11-12
- Supersedes: [FP-ADR-0007](../../platforms/foundation-platform/docs/adr/0007-netflix-style-lakehouse-compute-architecture.md) 의
  "역할 분리" 표 가운데 Batch compute(Spark)·SQL query(Trino)·Local validation(DuckDB) 세 줄과 "왜 Spark 인가"·"왜 Trino 인가" 절.
  저장(R2)·표 형식(Iceberg)·카탈로그·제어 계층·소비자 경계는 그대로다.
- Builds on: [ADR-0127](./0127-floor-duckdb-staging-preserves-normalization-and-native-resource-contracts.md)(발행기 안의 DuckDB 와
  자원 계약 — 층 자료 한 곳에서 모든 변환으로 넓힌다), [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md)(`row_digest`),
  [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md) §1(내용 지문),
  [ADR-0344](./0344-gold-merges-write-only-the-changed-rows-as-merge-on-read-deletes-that-compaction-folds.md)(위치 삭제를 읽는 독자),
  [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)(행 판),
  [ADR-0237](./0237-lakehouse-snapshots-are-kept-30-days-12-month-ends-and-every-year-end.md)·[ADR-0338](./0338-a-reader-of-an-old-snapshot-pins-it-with-its-own-tag-and-the-rest-expire-in-30-days.md)(유지보수 규칙),
  [ADR-0465](./0465-a-parcel-shows-what-is-near-it-computed-in-gold-with-sedona-and-h3.md)(주변 — Sedona 는 옮길 때까지)
- 관련 사건: 2026-11-11 하루의 실패가 대부분 JVM 메모리였다. 실거래 Gold 는 20 GB JVM 으로 26 번 실패했고(전국 Gold 가 아직 없다),
  rt-molit Silver 적재는 힙이 모자랐고, Trino 는 컨테이너 상한에서 죽었고([ADR-0504](./0504-one-query-cannot-take-trino-past-its-container-cap-and-trino-follows-the-release.md)),
  Spark 풀 하나가 18 개 작업을 세워 두었다. 한 주(11-05~11-12) 저널에서 `foundation-rt-molit-silver-load` 는 79 번,
  `foundation-gold-panel-rebuild-unconditional` 은 29 번 실패로 끝났다.
- 결정한 사람: 사장(2026-11-12, 조정자 전달). 이 문서는 그 역할 배정을 정책으로 적고, 옮기는 방법과 측정을 붙인다.

## Context

**지금의 모양(2026-11-12, main `76ec2f77`).** `infra/lakehouse/spark/jobs` 에 Python 파일 99 개가 있다. 그 가운데 Spark 세션을
스스로 여는 파일이 55 개(작업 54 + 공용 세션 1)이고 나머지 44 개는 그 안에서 불리는 모듈이다. Sedona SQL 을 쓰는 작업은
`parcel_nearby_silver_to_gold` 하나다. `regulation_zones_silver_to_gold` 는 Sedona 가 아니라 표준 라이브러리로 손수 짠 다각형 교차
커널(`zone_overlap.py`, ADR-0375)을 Python UDF 로 돌린다. Rust 발행기는 Iceberg 를 읽고(스냅샷 스캔, 위치 삭제 적용) 태그와 WAP
가지를 커밋하지만, **데이터 스냅샷은 쓰지 않는다** — 모든 표 쓰기는 Spark 제출이다.

**데이터 규모.** 가장 큰 입력이 `silver.land_ownership` 1.2 억 행·1.2 GB, `silver.parcel_boundaries` 0.8 억 행·14.8 GB(서빙 판
39,861,511 필지)다. 서버 한 대(62 GB, 20 코어)의 디스크와 메모리 안에 든다. 분산이 필요한 크기가 아닌데 JVM 드라이버 하나에
16~20 GB 를 주고도 죽는 것이 사건의 모양이다.

**측정(2026-11-12, ai-server, 읽기만; 시험 표는 자기 네임스페이스에 만들고 지웠다).** 고정한 입력 스냅샷을 서버 디스크에 Parquet 로
한 번 내려 두고(DuckDB 가 R2 카탈로그에서 직접 읽었다: 39,861,511 필지·4.37 GB 를 109 초), 두 엔진이 **같은 파일**을 읽게 했다.
컨테이너는 둘 다 메모리 8 GiB(스왑 없음)·CPU 8 개로 묶었다. Spark 는 운영 이미지(3.5.6)·운영 jar(Iceberg 1.11.0, Sedona 1.9.1)·
운영 릴리스의 함수 그대로(`parcel_nearby_silver_to_gold.gold_rows`, `real_transaction_silver_to_gold.placed_deals` — #1346 의 고침
포함)를 풀 밖 일회용 컨테이너에서 돌렸다(풀 점유 없음). DuckDB 는 2.0.0(Python 바인딩, spatial·h3 확장), 버퍼 한도 4 GB,
파일 기반 임시 DB 다. "메모리 최고"는 컨테이너 cgroup 의 익명 메모리 최고값(0.25 초 표본, 페이지 캐시 제외)이다. 같은 시간에 운영
Gold 재생성(Spark 20 GB)이 같은 호스트에서 돌고 있었으므로 벽시계는 양쪽 다 한가한 호스트보다 느리다.

| 작업(경기도, 가장 큰 시도) | 엔진 | 벽시계 | 메모리 최고 | 출력 행 | 동등성 판정 |
|---|---|---:|---:|---:|---|
| 주변(필지 5,208,587: 중심 + 시설 5 종 최근접 + 타원체 거리 + H3 상가 수) | Spark 3.5.6 + Sedona 1.9.1 (드라이버 6g) | 1,766 초 | 6.88 GiB | 5,208,587 | 기준 |
| 〃 | DuckDB 2.0.0 + spatial + h3 | **260 초** | **3.84 GiB** | 5,208,587 | 같음 5,168,120 (99.22%), 다름 40,467, 한쪽에만 0 |
| 실거래 Gold 의 배치 단계(거래 3,096,438 건, 가려진 지번 1,732,454 건) | Spark 3.5.6 (드라이버 6g, 셔플 12 GB 디스크) | 2,026 초 | 7.00 GiB | 10,027,082 | 기준 |
| 〃 | DuckDB 2.0.0 | **185 초** | **4.03 GiB** | 10,027,082 | **equal** — 10,027,082 행 전부, 지문 `rows:10027082;digest_sum:5777979693964182954507159` 양쪽 동일 |
| 〃 가려진 지번 추론만(보조) | Polars 2.1.0 | 26 초 | 4.39 GiB | 375,369 | 연결된 거래 수가 DuckDB 와 같다(375,369). 행 대조는 하지 않았다 |

**주변의 다른 40,467 행은 전부 Sedona 쪽이 더 먼 시설을 고른 것이다.** 문서의 글자(키 순서, null, 숫자)는 5,168,120 행에서 바이트까지
같았고, 상가 수와 고속도로 출입시설은 모든 행에서 같았다. 다른 행을 시설 종류별로 보면 DuckDB 가 더 가까운 시설을 고른 경우가
6,955~8,527 건, 미터로 반올림하면 같은 거리의 다른 시설이 40~1,565 건이고, **Spark 가 더 가까운 경우는 0 건**이다(최대 차이는
공항 214 m). 즉 `ST_KNN(…, 1, true)` 는 타원체 거리로 정확한 최근접이 아니다. DuckDB 쪽은 격자 칸마다 "칸 중심에서 가장 가까운 시설
+ 칸 지름" 안의 후보만 남긴 뒤(삼각 부등식으로 정확) 후보 전부에 타원체 거리를 재어 최소를 고른다. 따라서 주변은 "Sedona 를 그대로
흉내" 낼 수 없고, 옮기면 0.78% 의 문서가 더 정확한 답으로 바뀐다. `gold.parcel_nearby` 는 아직 발행된 적이 없으므로 다시 구울
문서는 없다.

DuckDB 쪽에서 배운 것 둘: 필지마다 다섯 결과를 다섯 번의 조인으로 붙이면 4 GB 한도에서 메모리가 모자랐고(해시 테이블 다섯이 함께
선다), 한 번의 묶음 집계로 바꾸니 끝났다. 임시 표는 메모리 DB 가 아니라 파일 기반 임시 DB 에 두어야 디스크로 넘친다.

**DuckDB 2.0.0 이 우리 R2 REST 카탈로그에 쓸 수 있는 것(시험 네임스페이스 `w75_engine_probe`, 끝나고 표와 네임스페이스 삭제).**

| 무엇 | 결과 |
|---|---|
| 카탈로그 붙기·읽기 | 됨. 토큰 하나로 붙고 `AT (VERSION => 스냅샷)` 로 고정 읽기. 읽기 토큰은 자격 위임(vended credentials)이 403 이라 `ACCESS_DELEGATION_MODE 'none'` + 읽기 S3 키로 읽는다 |
| 추가(`INSERT`) | 됨. `append` 스냅샷, 요약에 표준 개수 |
| 파티션 쓰기 | 됨. identity(17 파티션 → 파일 17), `truncate`·`bucket`(파일 160, 5,000 행에 29 초 — 작은 파일이 많아진다). `PARTITIONED BY` 는 `WITH` 앞에 온다 |
| `MERGE`·`UPDATE`·`DELETE` | merge-on-read 표에서만 됨. v2 표에 Parquet 위치 삭제 파일을 쓴다(200 행 MERGE → 위치 삭제 100). copy-on-write 로 선언된 표는 거부한다 |
| 스냅샷 요약에 우리 속성 쓰기 | **안 됨.** 설정도 구문도 없다. 우리 커밋은 요약에 내용 지문(`foundation.content-fingerprint`)·입력 핀·바뀐 키 수·코드 판을 적고 독자들이 그것을 읽는다 |
| 태그 만들기 | **안 됨**(`iceberg_refs` 로 읽기만). 서빙 핀·월말 태그는 지금처럼 발행기와 유지보수 작업이 만든다 |
| 표 속성 쓰기 | 됨(`set_iceberg_table_properties`) |
| 동시 커밋 | DuckDB 두 트랜잭션이 같은 표에 동시에 커밋 → 둘 다 들어감(2,000 행). 발행기의 태그 커밋과 겹치는 경우는 재지 않았다 |
| 합치기 | `iceberg_rewrite_data_files` 가 있다(이번에는 호출만 확인) |

Rust 독자는 Parquet 위치 삭제만 적용하고 동등 삭제와 삭제 벡터(v3, Puffin)는 거부한다(ADR-0344). DuckDB 는 v3 표에 삭제 벡터를 쓰므로
우리 표는 v2 로 둔다.

**대상 두 작업의 상태.** `gold.parcel_real_transaction` 과 `gold.parcel_nearby` 는 카탈로그에 아직 없다(첫 빌드가 끝난 적이 없다).
주변의 시설 Silver 표 여섯(공항·철도역·도시철도역·학교·대학·상가)은 등록만 되어 있고 스냅샷이 0 개다. 그래서 주변 측정의 시설 점은
실제 필지 중심에서 계약의 건수대로 뽑은 대역이고(필지 경계와 고속도로 출입시설은 실물), 대조할 "운영 Spark 결과"가 없어 두 엔진을 같은
입력에 직접 돌려 대조했다.

**다른 회사와 프로젝트.** 한 대에 드는 데이터에서 분산 엔진을 빼는 것은 흔한 선택이다. DuckDB 는 Iceberg REST 카탈로그 쓰기를 1.4 부터
넣었고 R2 Data Catalog 를 공식 예로 든다(DuckDB 문서 `core_extensions/iceberg`). Apache Iceberg 의 Rust 구현은 스냅샷 속성을 실은
`FastAppendAction` 과 `ExpireSnapshotsAction` 을 갖고 있고 행 단위 삭제(`RowDelta`)는 RFC 단계다(apache/iceberg-rust
`transaction`, `docs/rfcs/0003`). Netflix·Apple 이 Iceberg 로 얻은 것(엔진이 바뀌어도 표는 그대로)은 우리가 이 전환에서 쓰는 성질이다.

## Decision

1. **역할.**

   | 엔진 | 역할 |
   |---|---|
   | DuckDB (Rust 발행기 안, ADR-0127 의 방식) | **모든 변환**(Bronze→Silver, Silver→Gold, 학습용 표)과 **조회·검사·측정** |
   | Spark | **Iceberg 표 유지보수만**(`lakehouse_maintenance.py`: 합치기 → 만료 → 고아 정리). 하루 한 번 떴다 내려간다. 상시 풀이 아니다. 그리고 아직 옮기지 않은 변환을, 옮길 때까지 |
   | Trino | 상시 역할 없음. 독자가 모두 옮겨 가면 내린다(8 번) |
   | ai-server GPU 의 Python | 학습과 추론. 같은 Iceberg 표를 읽는다 |
   | tippecanoe / PostGIS | 타일과 경계 발행. 바꾸지 않는다 |

   모든 엔진은 R2 의 같은 Iceberg 표를 읽고 쓴다. 사본을 두지 않는다.
2. **Spark 에 새 작업을 더하지 않는다.** `lakehouse-engine.contract.json` 의 `engines.spark_sessions` 가 Spark 세션을 여는 파일
   전부의 장부이고(오늘 55), `test_engine_ledger.py` 가 디렉터리와 장부를 같게 묶는다. 막는 사건: 위 2026-11-11 의 JVM 메모리 실패 —
   장부에 없는 Spark 작업은 아무도 옮길 계획이 없는 작업이다. 새 변환은 DuckDB 로 쓴다.
3. **계약은 엔진과 무관하다.** 변환의 계약은 입력(고정한 Iceberg 스냅샷들), 출력(표 계약의 칸), 그리고 행 지문(`row_digest`)이다.
   변환 하나를 옮기려면 두 엔진이 같은 고정 입력을 읽어 낸 출력을 `engine_equality.py` 가 키로 대조해 **equal** 이어야 한다:
   모든 키가 양쪽에 있고 `row_digest` 가 같고 내용 지문(`gold_fingerprint`)이 같다. 대조는 공통 SQL 한 문장이며 비교되는 두 엔진
   어느 쪽의 것도 아니다(명령은 DuckDB 로, 시험은 SQLite 로 같은 문장을 돌린다). 지문은 내용의 JSON 글자에 대한 것이므로 equal 은
   서빙되는 바이트가 같다는 뜻이고, 옮긴 뒤 다시 굽는 문서가 0 이다. 쓰는 법만 다른 행(숫자 표기, null 과 빠진 키)은 equal 이
   아니다 — 판정이 따로 세어 주고(`written_differently`), 그런 채로 옮기려면 한 번의 전체 재발행을 ADR-0314 의 예외로 비용과 함께
   적는다.
4. **쓰기.** DuckDB 는 우리 표에 `UPDATE`·`DELETE`·`MERGE` 를 하지 않는다. 우리 커밋이 요약에 적는 속성과 태그를 DuckDB 가 못 쓰기
   때문이다(위 표). 따라서
   - DuckDB 는 계산하고 데이터 파일(Parquet)을 쓴다. **커밋은 발행기가 한다**: Iceberg REST 어댑터(`iceberg_rest_catalog.rs`)에
     apache/iceberg-rust 의 추가(fast append) 커밋을 더하고, 지금 Spark 가 적는 요약 속성을 같은 이름으로 싣는다. 데이터 엔진을
     다시 만드는 것이 아니라 성숙한 구현 위의 얇은 커밋 어댑터다.
   - 추가만 하는 표(행 판을 쌓는 Silver·`__changes`·판 표, ADR-0286)가 먼저 옮겨 간다. `MERGE` 로 갈아 끼우는 Gold(`replaced`)는
     커밋 모양이 추가가 되거나(행 판) 발행기 커밋이 행 단위 삭제를 싣게 될 때까지 Spark 에 남는다.
   - 표는 Iceberg v2 로 둔다(삭제 벡터를 Rust 독자가 거부한다).
5. **옮기는 순서**는 장부의 물결(`engines.waves`)이다: ① 지금 JVM 메모리로 죽는 것(실거래 Gold, rt-molit Silver, 주변),
   ② 16 GB Gold 생산자와 풀을 오래 잡는 것, ③ 나머지 Gold, ④ Silver 적재·내보내기·관문, ⑤ 기하 인계와 서빙 경계, ⑥ 표 DDL.
   옮기는 PR 은 판정 보고서(`engine_equality` 의 JSON)를 본문에 싣고 장부에서 그 줄을 지운다. 장부의 길이가 남은 수다:
   **55 → 1(유지보수) → 0**.
6. **판 고정과 오프라인.** DuckDB 판은 지금처럼 Foundation Cargo 워크스페이스가 소유한다(`duckdb` crate, bundled). 옮기기의 첫
   PR 이 측정에 쓴 2.0 계열(`duckdb = 1.20000.x` = DuckDB 2.0.0, `httpfs` feature)로 올린다. 그 밖의 확장(iceberg, spatial, h3)은
   그 판·플랫폼의 서명된 확장 파일을 릴리스 산출물에 다이제스트로 싣고 경로로 `LOAD` 한다. 실행 중 내려받지 않는다 —
   자동 설치·자동 로드는 ADR-0127 대로 꺼 둔다. 측정에 쓴 조합은 아래 표다.
7. **메모리.** DuckDB 변환은 `execution_profile` 의 상한 안에서 돈다(버퍼는 상한의 절반, ADR-0127). 중간 표는 호출이 소유한 파일
   기반 임시 DB 에 두어 디스크로 넘친다. 한 번에 안 드는 일은 시도 단위로 나눈다(ADR-0440 의 샤드와 같은 키).
8. **Trino 를 내리는 조건.** 독자 전부가 같은 결과로 DuckDB 로 옮겨 간 뒤다: Dagster 검사(`publish_checks`·`unit_price_checks`·
   `place_identity_checks`), `data-quality-check.py`, `lakehouse-production-shape.py`, `shape-ledger-check.py`,
   `parcel-boundary-equality.py`, `parcel-edition-readiness.sh`, `media_reconcile.py`, `unit_official_price_projection_load.rs`,
   중개사 명부 적재, dbt(`dbt-trino` 프로필), 운영자·에이전트의 `docker exec … trino`. 각 독자도 3 번의 방식으로 옮긴다(같은 고정
   스냅샷에 두 엔진의 답이 같다).
9. **유지보수의 다음 단계.** 합치기와 만료를 R2 Data Catalog 의 관리 기능이나 성숙한 오픈소스로 옮기는 것은, 시험 표에서 우리
   규칙(서빙 핀·월말 태그가 만료에서 살아남는다, Silver 적재 뒤에만 만료)이 지켜짐을 보인 뒤다. 그때 Spark 를 내린다. 손으로 짠
   만료·고아 정리는 만들지 않는다. 이 단계는 5 번의 옮기기를 막지 않는다.
10. **Spark·Trino 가 다시 들어오는 조건.** 넷 중 하나가 측정으로 보일 때만이고, 그때도 같은 Iceberg 를 읽는 필요할 때만 뜨는 형태다:
    시도로 나눠도 한 호스트에 안 드는 데이터; 상시 SQL 끝점이 필요한 다수의 동시 대화형 사용자·BI 도구; 외부 데이터베이스에 걸친
    연합 질의; 스트리밍.

## 측정에 쓴 판

| 무엇 | 판 |
|---|---|
| DuckDB | 2.0.0 (PyPI `duckdb==2.0.0`; Rust `duckdb` crate 의 같은 판은 1.20000.1) |
| 확장 | httpfs·iceberg·avro(iceberg 가 읽는다): v2.0.0 용 공식 빌드, spatial `a3dbcd1781`, h3(커뮤니티) v1.5.6. linux_amd64 파일 다섯 개 합계 221 MB(spatial 108 MB, iceberg 57 MB, avro 28 MB, httpfs 27 MB, h3 0.5 MB) |
| Spark 쪽 | apache/spark 3.5.6, Iceberg 1.11.0, Sedona 1.9.1 (운영 릴리스의 jar 그대로) |

## Consequences

- 오늘 죽는 작업 셋이 8 GiB 안에서 도는 엔진으로 간다. Spark 풀의 16~20 GB 슬롯이 물결마다 비고, 마지막에는 풀이 없어진다.
- 발행기에 Iceberg 추가 커밋이 생긴다(4 번). 이것이 옮기기 전체의 선행 조건이고 첫 물결의 일이다.
- 릴리스 빌드는 이미 bundled DuckDB 를 짓는다(측정: 익명 메모리 16.7 GiB·2,590 초의 대부분, `tools/release-build.contract.json`;
  캐시된 뒤에는 다시 짓지 않는다, ADR-0155). 판을 2.0 으로 올리는 PR 이 그 빌드를 한 번 다시 치르고, `httpfs` feature 만큼
  늘어난다 — 그 PR 이 CI 시간 예산(ADR-0203) 안에서 재서 적는다. 이 PR 은 Rust 를 바꾸지 않지만 `lakehouse-engine.contract.json`
  이 발행기에 포함되므로 그 crate 가 한 번 다시 지어진다.
- 동등성 판정이 바이트 동등을 요구하므로, Spark 의 `to_json` 이 쓰는 글자(키 순서, null 유지, 실수 표기)를 DuckDB 쪽이 그대로
  내야 한다. 이번 측정에서 주변 문서는 그렇게 맞췄다(아래). 실수가 많은 문서(실거래)는 표기 차이가 나올 수 있고, 그것은 3 번의
  예외 절차로 간다.
- `regulation_zones` 의 손수 짠 교차 커널은 옮길 때 DuckDB spatial(GEOS)의 교차 넓이로 바뀐다. 넓이가 끝자리까지 같지 않을 것이므로
  그 작업은 3 번의 예외(한 번의 전체 재발행)를 쓸 첫 후보다.
- PostGIS 는 줄일 수 있다: 서빙 경계 Gold 와 타일 인계가 DuckDB spatial 로 옮겨 가면 PostGIS 에 남는 것은 경계 발행의 마지막 적재뿐이다.
  이 ADR 은 그것을 결정하지 않는다.

## 하지 않은 것, 확인하지 못한 것

- 실거래는 **배치 단계**(`placed_deals`: 지번→필지, 가려진 지번 추론, 필지 배치; 계보 따라가기는 양쪽 다 끔)만 옮겨 대조했다.
  문서 조립(`gold_rows`)과 지역 통계는 옮기지 않았다. 입력 준비(법정동 코드 재번호 등)는 양쪽에 같은 파일을 주었을 뿐 운영
  `read_inputs` 와 한 줄씩 같지는 않다.
- 주변의 시설 점은 대역이다(위). 시설 Silver 가 실제로 실리면 다시 대조한다.
- 전국 한 번(3,986 만 필지)의 DuckDB 실행, Sail·Polars 의 온전한 대조는 아래 표에 적은 만큼만 했다.
- R2 관리형 합치기·만료의 시험 표 검증(9 번)은 하지 않았다. 만료는 스냅샷이 하루 이상 묵어야 판정할 수 있다. 표 단위로 켜는 명령은
  문서에 있다(`wrangler … compaction enable <bucket> <namespace> <table>`).
- DuckDB 커밋과 발행기의 태그 커밋이 같은 표에서 겹칠 때의 동작.

## 기각한 대안

- **JVM 에 메모리를 더 준다.** 20 GB 로 26 번 죽은 작업의 배치 단계가 4 GiB 로 끝난다. 원인은 크기가 아니라 엔진이다.
- **Sail(Spark Connect 를 구현한 Rust 엔진)로 코드를 그대로 돈다.** Sail 0.7.3 은 실거래 배치 단계의 운영 PySpark 코드를 고치지 않고 받아 실행했다(broadcast 힌트는 무시한다). 그러나 Sedona 는 JVM 확장이라 주변은 돌릴 수 없고, Iceberg 쓰기와 우리 요약 속성은 따로 풀어야 하며, PySpark 코드와 Python 드라이버 로직이 그대로 남는다. 발행기 안에서 도는 엔진이 아니다.
- **Polars.** 가려진 지번 추론 한 단계는 가장 빨랐다(26 초, 4.39 GiB). 공간 연산(중심·타원체 거리·H3)과 Iceberg REST 쓰기가 본체에 없다.
- **Trino 로 변환한다.** 같은 JVM 메모리 사고의 다른 쪽이다(ADR-0504).
- **한꺼번에 옮긴다.** 판정 없이 옮기면 서빙 문서가 조용히 바뀐다. 한 작업씩, 판정과 함께.
