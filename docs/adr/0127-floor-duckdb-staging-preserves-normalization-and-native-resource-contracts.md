---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-01
---

# ADR 0127: 층 자료의 DuckDB 임시 처리는 정규화와 공통 자원 계약을 보존한다

## 문제와 불변식

층 원문은 같은 건물의 행이 연속된다고 보장하지 않는다. 건물 식별자가 바뀔 때마다
정규화하면 A/B/A 순서의 A를 두 불완전한 그룹으로 나누므로 뒤의 증거를 앞 행에
적용하지 못한다. 표제부 전체와 제안 문맥을 메모리에 계속 보관하는 구조도 입력 크기에
비례하여 커진다.

기존 Rust parser·normalizer·제안 직렬화가 의미의 단일 정의다. 완전한 건물 그룹,
표제부의 첫 파싱 가능 관측 전체, 원문 ordinal·물리 행 번호·앞자리 0·NUL을 보존한다.
첫 관측의 NULL을 나중 관측의 값으로 채우지 않는다. JSONL·Parquet 행과 제안의
내용·순서·체크섬을 임시 엔진 교체 때문에 바꾸지 않는다.

## 결정

이미 승인된 FLOOR DuckDB 구현과 직접 의존성만 현재 main 호출 경로로 이식한다.
공식 MIT 라이선스 `duckdb-rs`를 사용하며 정확한 버전은 Foundation Cargo workspace가
소유한다. 기본 `bundled-duckdb` feature는 같은 엔진을 포함한다. DuckDB Arrow 타입과
기존 Parquet Arrow 타입은 각 라이브러리 경계 안에 둔다.

호출 전용 메모리 DB와 소유한 spill 디렉터리를 사용한다. 영속 DB·WAL·공유 저장소를
만들지 않는다. 읽기와 쓰기는 같은 DB의 별도 연결로 실행한다. ZIP 하나의 모든 층을
먼저 적재하고 `(pk, ordinal)` 순으로 읽어 기존 Rust 정규화에 완전한 그룹을 전달한다.
출력은 ordinal 순으로 복원하고 개수와 연속성을 대조한다. 여러 ZIP에 나뉜 건물까지
전역 그룹화한다고 주장하지 않는다. 정확한 한 쌍의 FLOOR·title 입력은 파일명과 OPN 날짜,
원천 slug, title 증거를 검사한다. 입력 지문 비교는 파일 불변 증거이며 Bronze 원장 인증을
대신하지 않는다.

### 임시 스키마와 실패 경계

- 쓰기 전에 출력·제안·완료표시와 Bronze 경로 전체의 겹침을 한 곳에서 검사한다. 기존 부모의
  실제 경로를 해석하고 Linux 파일 별칭도 비교한다. 파일명만 다른 같은 파일, 분할 출력 폴더
  안의 다른 출력·입력을 거부한다. 상위 경로 이동이나 해석할 수 없는 별칭은 허용하지 않는다.
  기존 분할 결과 정리는 이 사전 검증을 통과한 출력 폴더에만 적용한다.
- `title_observations`, `source_rows`, `resolved_rows`의 `ordinal`은 양수 BIGINT primary key다.
  물리 행 번호와 입력·결과 바이트 수도 양수 제약을 둔다.
- `proposal_json`과 `proposal_bytes`는 함께 NULL이거나 함께 존재한다.
- `title_counts`는 `row_number()`로 건물의 첫 관측 전체를 고른다. 식별자는 VARCHAR이며
  SQL에 정규화 규칙을 복제하지 않는다.
- 그룹 크기를 hydration 전에 검사하고 행·제안 직렬화에 같은 bounded writer를 사용한다.
  aggregate 제안 API도 단일 행 직렬화 함수에 위임한다.
- Appender의 명시적 flush와 transaction commit이 성공해야 묶음이 완료다. fallible
  `Statement::step()`으로 fetch·interrupt 오류를 전달한다. Drop은 성공 판정 수단이 아니다.
- 동기 엔진은 호출이 소유하는 blocking 작업에서 실행한다. 취소와 interrupt·연결 종료를
  조정하고, 연결이 닫힌 뒤 소유 디렉터리를 정리한다. async 호출자는 작업·출력 flush·정리·
  입력 지문 검증이 끝나야 완료 요약을 기록한다. release의 `panic = "abort"`까지 Drop 정리를
  보장한다고 주장하지 않는다.

### 자원 계약과 실제 호출

기존 `lakehouse-engine.contract.json`을 v3로 확장한다. `execution_profile`은 native 실행의
`memory_mib`, `cpu_slots`, `pids_limit`, `swap_mib`만 소유한다. `serving_parquet`는 행 그룹의
바이트 목표·상한과 검사 주기를 소유한다. 수치는 JSON에만 정의하며 Rust·Python reader가
필수 필드·정수·양수 범위·목표와 상한의 관계·무스왑 조건을 검사한다. 메모리는 절반으로
나누어도 양수가 되어야 한다. 기존 Iceberg·Hadoop·Spark 버전과 적재 메타데이터는 보존한다.

FLOOR의 기존 원격 호출은 검증한 profile에서 Compose 환경값을 만든다.
`compose.lakehouse-native.yml`은 FLOOR 호출에서만 기본 Compose에 더하며 메모리·CPU·PID·
총 메모리와 swap 한도를 필수 환경값으로 받는다. 누락값에 무제한 기본값을 두지 않는다.
Docker의 총 메모리와 swap 한도가 메모리 한도와 같아야 swap이 허용되지 않는다.
다른 native 호출과 Spark 전체의 자원 정책 완료를 이 변경으로 주장하지 않는다.

DuckDB buffer 예산은 profile 메모리의 절반이고 threads는 CPU 슬롯에서 읽는다. 나머지는
Rust·Arrow·압축 작업에 남긴다. `FOUNDATION_PLATFORM_SERVING_SCRATCH_MAX_BYTES`는 기존
spill 최대 바이트 계약이다. 기본 최대치가 실제 디스크 예약·여유·사용량이라는 뜻은 아니다.
SQLite API 없이 공통 행·전송·spill 예산 모듈을 둔다. 연결 후 live buffer manager에
`SET max_temp_directory_size`를 적용하고 확장 자동 설치·로드와 외부 파일 접근을 차단한다.
이 상한은 프로세스 RSS·출력 파일·Arrow batch 전체의 상한이 아니다.

## 기각한 대안과 기존 결정의 보존

- 오래된 작업본 전체 병합: 현재 폐기한 상세 PostgreSQL 사본과 과거 예약 구조를 되살린다.
- main에서 exporter 재작성: 이미 검증한 취소·바이트·전체 그룹 처리를 반복한다.
- SQLite SQL을 그대로 교체하거나 자체 정렬·spill 엔진 작성: 엔진 특성을 놓치거나 불필요한
  데이터 처리 계층을 소유하게 된다.
- 과거 v4 계약 전체 복사: 사용하지 않는 admission·host reserve·Spark tuning·write ID 정책을
  도입한다. 필요한 v3 확장만 현재 계약에 더한다.
- 영속 DB나 두 엔진 상시 이중 저장: 파일·WAL·실패 경로를 늘린다.

[ADR-0108](./0108-retire-the-postgres-serving-projections-superseded-by-r2-bakes.md)·
[0109](./0109-gongzzang-reads-parcel-and-building-detail-from-the-r2-edge.md)의 R2 상세 소유권,
[0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)의 예약·계보 방향,
[0120](./0120-a-changed-derivation-appends-the-same-source-object-once-more-under-its-label.md)의
파생 이름과 Iceberg append 계약을 바꾸지 않는다. PostgreSQL migration이나 새 예약기를
추가하지 않는다. 기존 CLI 이름은 유지하고 같은 library 구현을 await한다.

## 검증과 근거

회귀 검사는 interleaved A/B/A와 전체 그룹 정규화의 바이트 동등성, 물리 ordinal·앞자리 0·
NUL·NULL 첫 title, Parquet read-back, 그룹·행·제안 초과, Appender flush·fetch·취소·cleanup을
검사한다. spill 상한은 같은 합성 workload에서 충분한 한도와 작은 한도를 대조한다.
Rust·Python 계약 검사와 실제 Compose render를 함께 확인한다. 합성 검증은 원장 입력 선택,
실제 Iceberg 적재·read-back, 전국 처리, 예약 설치 완료의 증거가 아니다.

- [Rust 일괄 입력](https://duckdb.org/docs/current/clients/rust/data_import)
- [Rust 결과 처리](https://duckdb.org/docs/current/clients/rust/result_handling)
- [고정 클라이언트의 fallible fetch](https://github.com/duckdb/duckdb-rs/blob/v1.10506.0/crates/duckdb/src/statement.rs)
- [엔진 spill 할당](https://github.com/duckdb/duckdb/blob/v1.5.6/src/storage/temporary_file_manager.cpp)
- [DuckDB 메모리 한계](https://duckdb.org/docs/current/guides/performance/oom)
- [Compose 서비스 자원 설정](https://docs.docker.com/reference/compose-file/services/)
