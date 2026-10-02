---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-02
---

# ADR 0128: 층 입력은 Bronze 원장에, 재시도 시각은 실제 적재 커밋에 묶는다

- Builds on: [ADR-0062](./0062-an-ingest-batch-records-itself-in-the-table-it-writes.md),
  [ADR-0120](./0120-a-changed-derivation-appends-the-same-source-object-once-more-under-its-label.md),
  [ADR-0127](./0127-floor-duckdb-staging-preserves-normalization-and-native-resource-contracts.md)

## 배경과 불변식

기존 `building_register_floors_pipeline_full`은 원본 폴더 전체와 실행 당시 시각을 사용했다.
이어지는 scalar 명령은 `overwrite`, 파일 개수 단위 적재, 전체 표 행 수 확인을 사용했다.
이는 같은 원본의 재시도, 과거 판 보존, `source_snapshot_id` 하나로 표현되는 층 추출물의
분할 파일에 적합하지 않다. 파생 이름만 전달하면 readback이 이전 판까지 함께 세는 문제도 있다.

같은 원본 선택은 처리 시각·코드 버전과 무관한 식별자를 갖는다. 재시도는 같은 처리 시각을
유지하고, 파생 규칙 변경은 원본 이름을 바꾸지 않고 ADR-0120의 선택적 이름으로 구분한다.
완료 확인은 그 적재가 실제로 저장한 판만 대상으로 한다.

## 결정

1. 전체 층 실행은 정확한 FLOOR/title ZIP 이름과 보존한 `ingested_at_utc`를 요구한다.
   exporter는 기존 `BronzeIngestRepository`와 `BronzeIngestUnitOfWork`의 SELECT로 두 객체를
   조회한다. 공급자 파일 ID·원천·월 기준·크기·SHA-256을 검증하고, 기존 입력 해시 검사와
   대조한 뒤에만 변환·출력한다. 새 원장이나 별도 수집 목록은 만들지 않는다.
2. `source_snapshot_id`는 인증한 두 입력의 원천 역할·공급자 파일 ID·기준월·크기·SHA-256과
   식별 형식 버전에서 결정한다. 원장 복구로 달라지는 DB UUID·물리 키와 처리 시각·파생 이름은
   제외한다. `valid_from_utc`는 공급자 기준월을 사용하되, 검증된 과거 입력은 아래 보호된 과거 근거의 기존 식별자·유효 시각을 보존한다. 옛 원본을 새 이름으로 중복 적재하지 않는다.
3. 전체 층 실행은 `append`와 하나의 논리 배치를 사용한다. 여러 Parquet 파일을 Spark가 읽는
   것과 여러 번 커밋하는 것을 구분한다. scalar의 파일 배치 모드는 같은 계약 적재 식별자가
   여러 배치에 걸치면 쓰기 전에 거부한다. 실제 자료의 자원 요구량은 별도 측정 대상이다.
4. scalar CLI의 `--derivation`은 기존 `identities_under_derivation` 검사를 재사용한다.
   정기 실행은 생략한다. 새 파생 판의 시각은 같은 원본의 기존 판보다 뒤여야 한다.
5. readback은 ADR-0120의 `(source_snapshot_id, ingested_at_utc)` 쌍으로 한정한다.
   재시도는 기존 적재 원장이 가리킨 Iceberg 커밋에서 그 입력에 해당하는 행의 시각까지
   인증한다. 과거 전체 스냅샷에는 앞선 판도 있으므로, 부모 스냅샷 다음부터 해당 커밋까지의
   증분 append를 읽는다. 첫 커밋은 해당 스냅샷을 읽는다. 필요한 스냅샷이 없으면 성공 처리하지 않는다.
6. 중복으로 쓰기를 생략한 단일 실행도 readback과 결과 요약을 만든다. 전체 층 명령은 이 검사를
   사용하며, 이번 배치와 전체 표의 행 수를 비교하거나 Trino를 재시작하지 않는다.

## 재사용과 기각한 대안

Iceberg가 제공하는 `start-snapshot-id`·`end-snapshot-id` 증분 읽기를 사용한다.
부모는 제외되고 대상 커밋은 포함되며 append 전용이라는 제약을 그대로 검사한다.
[공식 Spark 조회 문서](https://github.com/apache/iceberg/blob/main/docs/docs/spark-queries.md#incremental-read).

- 처리할 때마다 원본 식별자를 바꾸는 방식은 재시도 중복 방지를 무효화하므로 기각한다.
- PostgreSQL 재시도 원장이나 별도 timestamp registry는 Iceberg 커밋과 이중 기록이 되어 기각한다.
- 현재 표에서 같은 시각의 행만 찾는 방식은 다른 파생 이름의 결과를 재시도 성공으로 오인할 수 있어 기각한다.
- 과거 컨트롤러·타이머 전체 이식은 최신 예약 실행 결정과 충돌하므로 하지 않는다.

## 적용 범위와 남은 작업

이 변경은 전체 층 원격 명령과 공통 scalar importer의 연결이다. 다른 원격 명령의 덮어쓰기·조회
정책까지 바꿨다는 뜻이 아니다. 기존 운영 식별자 인증과 선택된 원본의 서버 준비는 아래 결정으로
연결한다. 최신 선택 명령과 처리 시각 복구는 아래 결정으로 구현한다. 해당 FLOOR DAG의
호출 연결·실행 확인, 실제 R2/서빙 반영은 별도 미완료 작업이다.
스키마·제약·마이그레이션·배포는 변경하지 않는다. R2 Class A 비용 개선 완료도 주장하지 않는다.

## 완료된 FLOOR handoff 재사용 결정 (2026-10-01)

전체 FLOOR 원격 실행은 동일 입력과 파생 이름에 동일한 로컬 경로를 사용한다. 재시도 시 이미 게시된 ready 요약이 있으면 기본 producer 명령은 계속 거부한다. 원격 전체 FLOOR 경로만 명시적 `FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_REUSE_COMPLETED_HANDOFF=1`을 전달한다.

재사용은 ready 요약 하나를 완료 권위로 삼는다. 기존 Bronze 원장과 선택된 두 ZIP의 크기·SHA-256을 먼저 검증하고, 요약의 설정·보존 시각·입력·실행 파일 SHA-256·출력 및 proposal 파일의 정확한 목록과 크기·SHA-256을 현재 값과 비교한다. 구형 요약이나 누락·변경·추가 파일은 거부하며 ready 출력은 덮어쓰지 않는다. 같은 요약의 형제 lock 파일을 `File::try_lock`으로 점유하고 부모의 ready 게시까지 유지한다. lock 파일은 삭제하지 않는다.

별도 재시도 레지스트리는 ready 요약과 중복된 완료 권위를 만들므로 채택하지 않는다. 수동 version 문자열만으로는 실행 코드 변경을 차단할 수 없어 실행 파일 해시를 사용한다. `source_snapshot_id`와 Iceberg append/readback 계약은 변경하지 않는다.

`ExportReport`를 재사용 증거에 보존하고 기존 요약의 표시 건수와 대조한다. proposal 파일을
쓰지 않은 실행도 계산된 proposal 건수를 잃지 않는다. 메타데이터 제한은
`completed_handoff::MAX_SUMMARY_BYTES`와 `MAX_ARTIFACT_FILES`가 소유한다. 생성과 읽기가
같은 바이트 상한을 적용하며 파일 목록은 내용 해시 전에 개수와 정확한 구성부터 검사한다.
이는 완료 증거의 메모리 제한이며 데이터 행 수·Parquet 파일 크기 정책을 바꾸지 않는다.
특수 파일은 열기 전에 거부하며 스트리밍 해시는 각 읽기 사이에 취소를 확인한다.

별도 잠금 라이브러리 대신 고정 Rust 툴체인이 제공하는
[`File::try_lock`](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock)을 사용한다.
입력·실행 도구·결과 내용을 함께 인증하는 근거는
[Bazel remote caching](https://bazel.build/remote/caching)의 action/result와 내용 해시 구분을
참고한다. 빌드 캐시 서비스 자체를 도입하지 않고 기존 완료 요약에 필요한 보장만 적용한다.

## 원장 복구 후 과거 FLOOR 인증 (2026-10-01)

수동 remote `building_register_floors_pipeline_full`의 live 실행은 폐기한다. 검토용
`execute=false` 계획만 유지하고, live 요청은 SSH·빌드·staging 이전에 거절한다. 지원되는 운영
전체 경로는 기존 systemd/Airflow가 실행하는 `run-building-register-floor-cycle`이며 최신 complete
pair를 자동 선택한다. 아래의 과거 원본 인증·파생 알고리즘은 유지하지만, 더 최신 월이 존재할 때
과거 exact pair를 지정하는 수동 E2E 재파생 경로는 지원하지 않는다. 이는 최신 자동 cycle의
완료 여부와 별개의 운영 기능 제한이다. 다른 remote job과 저수준 stage/export 명령은 유지한다.

현재 Bronze 원장을 복구하면서 같은 원본의 UUID와 객체 키가 바뀌었다. 두 ZIP의 크기와
SHA-256이 같아도 물리 원장 필드 전체를 해시하면 다른 원본으로 취급하는 것이 근본 원인이다.
공급자 의미와 바이트 증거로만 `building-register-floor-content-v1-` 식별자를 계산한다.
UUID와 실제 키는 원장·입력 검증 및 완료 handoff 증거에 계속 남긴다.

이미 운영 표에 있는 2026-09 자료는 과거 selection-v2 식별자로 저장되었다.
과거 적재 근거는 `FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH`가 가리키는
릴리스 밖의 보호된 JSON 파일로 전달한다. 운영 표 UUID·snapshot·원본 키·실측 해시를 공개
소스나 테스트에 복사하지 않는다. 이 파일은 검증한 원본 쌍과 기존 식별자·시각·건수의 관계를
보존하는 입력이며, 저장 완료 여부의 권위는 계속 Iceberg의 snapshot과 적재 summary다.

호스트 호출은 이 파일을 한 번 읽어 구조와 최대 64 KiB를 확인하고 내용 SHA-256을 고정한다.
같은 호출의 selector·producer·결과 검증은 같은 해석 결과를 사용한다. native와 Spark에는
같은 파일의 읽기 전용 mount와 `FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_SHA256`을
전달하며, 자식은 내용을 다시 검증한다. 경로·근거 누락, symlink·특수 파일, 중복 JSON 필드,
잘못된 형태, 내용 변경은 작업 시작 전에 거부한다. 없는 근거를 신규 입력으로 간주하지 않는다.

해당 기준월과 그 이전 입력은 근거와 정확히 일치해야 한다. 새 원본 선택·원장 UUID 복구가
옛 원본의 이름을 바꾸어 중복 적재할 수 없도록 한다. 과거 자료를 추가로 수용하려면 실제
원장·원본 해시·Iceberg snapshot/행을 대조한 근거를 관리자 절차로 준비해야 한다.
배포 설정의 파일 경로만으로 해당 내용의 진실성을 인증했다고 주장하지 않는다.
공개 회귀 검사는 두 언어가 공유하는 가짜 원본·표·snapshot fixture를 사용한다.

파일 종류를 먼저 검사해도 `open` 직전에 일반 파일이 FIFO로 바뀌면 열기 자체가 멈출 수 있다.
Unix에서는 이미 lockfile에 있는 [rustix](https://github.com/bytecodealliance/rustix)의 안전한
[fs::open](https://docs.rs/rustix/latest/rustix/fs/fn.open.html)을 재사용해 `NOFOLLOW`, `NONBLOCK`,
`CLOEXEC`를 열기 시점에 적용한다. 직접 syscall·unsafe·OS별 숫자 상수를 구현하지 않는다.
Python은 표준 `os.open`의 같은 flag와 열린 descriptor 검사를 사용한다. 내용 인증과는 별도로
읽기 작업 자체가 특수 파일에서 멈추지 않아야 한다.

전체 FLOOR 원격 명령은 변환 전에 기존 native producer의 입력 검사 모드를 호출한다.
`FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH`가 지정되면
Bronze SELECT와 실제 ZIP 해시를 확인한 작은 검사 결과만 새 파일로 쓰며 DuckDB를 열지 않는다.
그 결과를 `building_register_floor_history.py`가 읽는다. 보호된 과거 근거과 일치하면 Iceberg
표 UUID·기존 커밋·적재 원장·실제 행 출처와 시각 및 건수를 확인한다. 현재 head가 해당 커밋
또는 append만 이어진 후손인지 확인하고, 검사 중 head가 바뀌면 재시도를 요구한다.

파생 변경이 없으면 `already_retained`로 끝내며 새 행·데이터 파일·Iceberg 커밋을 만들지 않는다.
새 원본 또는 명시적인 파생 변경은 `export_required`로 기존 producer → scalar 경로를 잇는다.
과거 원본의 새 파생은 기존 원본 식별자·유효 시각과 더 늦은 처리 시각을 사용하고, scalar의
기존 파생 이름·정확한 append readback 검사를 계속 거친다. 검사 결과 파일은 그 실행의 증거이며
다음 실행의 완료 판단에 재사용하는 원장이 아니다. 보존 확인은 새 적재 감사로 기록하지 않는다.

기존 `lakehouse_registry_backfill.py`는 원장이 없는 표에 빈 append를 쓰고,
`lakehouse_lineage_migration.py`는 행의 출처를 갱신하는 도구여서 이 경우에 적용하지 않는다.
과거 overwrite는 append 증분 읽기로 인증할 수 없다는
[Iceberg 공식 제약](https://iceberg.apache.org/docs/latest/spark-queries/#incremental-read)에 따라
`snapshot-id`로 고정한 조회와 [표 UUID API](https://iceberg.apache.org/javadoc/1.11.0/org/apache/iceberg/Table.html#uuid())를
재사용한다. 별도 매핑 DB, 행 재작성, 가짜 빈 append, 단순 식별자 덮어쓰기는 기각한다.
Rust·기존 Spark/Iceberg 외의 라이브러리나 데이터 처리 엔진을 추가하지 않는다.

## 원장에 있는 두 입력의 서버 준비 (2026-10-01)

전체 원격 명령은 `stage-building-register-floor-inputs`를 원본 검사보다 먼저 호출한다.
이 명령은 같은 `ExportConfig`와 committed Bronze 인증을 재사용한다. 선택된 FLOOR/title
두 객체의 정확한 키만 읽고, 원본 폴더나 R2 prefix를 순회해 입력을 고르지 않는다.
실제 내용이 원장의 크기·SHA-256과 같은 로컬 파일은 R2에 접근하지 않고 재사용한다.
기존 파일의 내용이 다르거나 일반 파일이 아니면 덮어쓰지 않고 실패한다.

없는 파일은 기존 `R2ObjectStorage::open_seekable_object`를
`silver_handoff_io::open_source`로 열어 제한된 크기로 복사한다. 기존 transport의
[Range 및 If-Match](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
읽기와 재시도를 사용하며 새 S3 client를 만들지 않는다. 원장이 허용한 크기를 넘으면 쓰기를
중단하고, 끝까지 받은 실제 크기·SHA-256을 확인한다. 두 입력의 검증을 모두 마친 후
살아 있는 호출자만 같은 폴더의 임시 파일을
[`NamedTempFile::persist_noclobber`](https://docs.rs/tempfile/latest/tempfile/struct.NamedTempFile.html#method.persist_noclobber)로
게시한다. 게시 중 경합해 두 번째 파일이 실패하면 먼저 게시된 정상 파일은 다음 실행에서
인증해 재사용한다. 두 파일을 하나의 파일시스템 트랜잭션으로 게시한다고 주장하지 않는다.

입력 준비는 원본의 위치를 마련할 뿐 적재 완료를 의미하지 않는다. ready 요약, Silver 행,
Iceberg 커밋은 만들지 않는다. 새 의존성이나 별도 완료 목록은 추가하지 않는다.
전송 중 실패·협력적 취소에서는 임시 파일을 정리하며, 프로세스 강제 종료까지 자동 정리를
보장하지는 않는다. 입력과 출력 경로가 겹치거나 부모가 심볼릭 링크이면 쓰기 전에 거부한다.

`aws s3 sync`를 새 입력 준비 경로로 사용하면 목록 조회와 별도 지문 판정이 생기므로 기각한다.
전체 ZIP을 `Vec`에 담는 방법은 파일 크기만큼 메모리가 늘어나므로 기각한다.

## 최신 원본 선택과 다음 날 재시도 (2026-10-01)

`select-building-register-floor-inputs`는 `BronzeIngestRepository`의 한 SELECT에서 두 원천이
모두 존재하는 가장 최근 `snapshot_date`를 고른다. 반환은 세 행으로 제한한다. 두 행만 있고
역할이 하나씩이어야 진행하며 세 번째 행은 중복의 증거다. 더 최근 월에 한 역할만 들어왔다면
최근의 완전한 월을 선택하지만, 선택된 월이 중복·손상·날짜 불일치라면 더 오래된 월로 숨기지
않고 거부한다. 기준월과 공급자 파일 날짜는 기존 `CommittedInputs::from_objects`가 인증한다.
수집 시각·UUID로 최신성을 정하지 않고, 전체 수집 실행의 성공을 개별 committed 객체의 조건으로
추가하지 않는다. 단일 SELECT의 MVCC snapshot을 재사용하며 새 테이블·인덱스·원장은 없다.

출력은 16 KiB 이내 JSON이다. 기존 원장의 키·크기·SHA-256과 같은 의미 식별자를 반환한다.
과거 인증 자료만 보존된 적재시각을 포함하고 신규 자료의 시각은 비워 둔다. `collected_at`을
`ingested_at_utc`로 바꾸지 않는다. 반환 행 수의 제한이 DB 메타데이터 스캔량 제한은 아니다.

신규 월도 `building_register_floor_history.py`가 기존 Iceberg snapshot summary의 원본·파생
식별자와 batch token을 읽어 처리 여부를 확인한다. 하나의 정확한 append여야 하며, 그 append의
행만 `read_recorded_append`로 읽어 출처·원본 키·유효 시각·단일 적재시각·행 수를 인증한다.
이 함수는 scalar 재시도의 시각 검사도 함께 사용한다. 이전 파생을 포함한 표 전체에서 시각을
골라내지 않는다. 표 UUID·과거 고정 기준부터 현재 head까지 append ancestry가 유지되어야 하며
검사 중 head가 바뀌면 거부한다. 메타데이터가 없는데 행만 남은 경우도 새 적재로 취급하지 않는다.
보존 기간이 이 ancestry를 끊으면 자동 처리를 멈추고 복구해야 한다.

source inspection v2에는 인증된 `bronze_object_key`를 포함한다. 원격 결과는 입력 역할·공급자
파일 ID·파생 이름을 현재 요청과 비교하고, 입력 의미 증거에서 기존 Rust 식별자 함수를 사용해
`source_snapshot_id`를 다시 확인한다. 과거 base overwrite 증거로 새 파생의 완료를 주장할 수 없다.
보존 확인은 새 행이나 적재 완료 원장을 만들지 않는다.

최종 적재 전에 중단되어 ready만 남았을 때는 `completed_handoff::restore_time`이 그 ready의
원래 처리 시각을 복구한다. 전체 설정이 같은지 확인하고 기존 export lock 안의 producer·입출력
바이트 인증도 계속 통과해야 한다. 시각 복구 자체는 파일 검증이나 적재 완료 판정이 아니다.
로컬 작업 폴더의 식별자는 정확한 두 입력과 파생 이름으로 정하고 실행 날짜는 제외한다.
기존 시각을 포함해 만든 임시 폴더를 자동 탐색·채택하지 않는다. 새 완료 목록을 만들거나 매번
현재 시각으로 새 중간 파일을 만드는 대안은 재시도 중복과 비용을 만들므로 기각한다.

Iceberg의 [snapshot metadata와 incremental read](https://iceberg.apache.org/docs/latest/spark-queries/#incremental-read)를
그대로 사용하며 새 처리 엔진·캐시·완료 DB를 도입하지 않는다. 최신 선택 CLI와 재시도 검증의
구현은 Airflow/systemd의 예약 호출이나 운영 설치 완료를 뜻하지 않는다.


## 기존 예약 경로에 연결하는 서버 주기 (2026-10-01)

`run-building-register-floor-cycle`은 기존 selector와 `building_register_floors_pipeline_full`을
한 번 호출한다. 원장 선택·원본 준비·과거 확인·native 변환·Spark 적재를 같은 Airflow `spark`
pool 작업이 소유한다. `orchestration/jobs.v1.json`이 주기·systemd unit·계보 edge의 정본이다.
수집 작업의 성공에 종속시키지 않아 이미 committed된 원본의 미완료 적재를 재시도할 수 있다.
새 timer·완료 DB·완료 파일 목록을 추가하지 않는다. 원장 조회와 ready/Iceberg 판정은 기존
구현을 공유한다. FLOOR 주기는 Bronze→FLOOR Silver edge만 선언하며 Gold/서빙 완료를 주장하지 않는다.

systemd가 기존 서비스 자격증명을 환경으로 주입한다. Linux 실행, release 밖의 실제 절대 state/Ivy
경로, 미리 준비한 immutable native image ID/digest를 요구한다. 매 실행의 image 빌드·pull은
하지 않는다. `compose run --pull never`와 사전 image 존재 검사, 기존 `lakehouse-target-init`의
쓰기 가능 검사를 사용한다. native는 기존 DB의 사용자 정의 Docker bridge에만 연결한다.
`FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK`와 `FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_ENDPOINT`가
배포 환경의 network와 내부 host:port를 지정한다. host selector/audit의 `DATABASE_URL`은 그대로
두고, child 환경에서만 표준 URL 파서로 주소를 바꾼다. 계정·비밀번호·DB·TLS 옵션은 동일하며
별도 자격증명 사본을 만들지 않는다. query로 주소를 다시 덮는 모호한 URL은 거절한다.
실행별 임시 Compose JSON은 검증한 image digest·native DB network·동일 과거 근거의
읽기 전용 mount와 경로·SHA를 투영한다. 그 밖의 자원·볼륨·
수명 주기는 기존 Compose가 소유한다. 비밀은 이 파일이나 명령 인자에 쓰지 않는다.
`network_mode: host`는 저장소 격리 정책에 위배되고 전체 호스트 연결을 열어 기각한다.
환경변수로 임의 image tag를 허용하는 대안도 기각하며 예약 호출은 immutable 검증을 통과해야 한다.
호스트의 state 경로와 컨테이너 `/workspace/target/lakehouse` 경로를 분리한다. native는 서비스
UID/GID, Spark는 기존 UID 185와 서비스 GID로 접근하며 전용 state/Ivy는 `2770`이다.

로그는 systemd로 바로 흘리고 현재 결과만 256 KiB 이내 일반 UTF-8 파일로 회수한다. 임시 결과는
다음 실행의 건너뛰기 근거가 아니다. 결과의 원본 식별자를 이번 원장 선택과 다시 비교한 뒤
기존 결과 검증·선택적 감사 기록 경로로 넘긴다. 재사용 확인을 신규 적재로 감사 기록하지 않는다.

실제 Compose SIGTERM 검사에서 호출 프로세스 그룹을 죽여도 daemon 소유 컨테이너가 남았다.
따라서 systemd `INVOCATION_ID`로 실행별 Compose project를 정하고 `ExecStopPost`가 같은 ID의
one-off 컨테이너만 명시적으로 중지·제거하고 부재를 확인한다. 이어 같은 project label의
네트워크도 제거·부재 확인하여 실행마다 Docker 주소 풀이 누적 소비되지 않게 한다. 정상 호출의 종료 경로도 같은
`stop-building-register-floor-cycle` 구현을 사용한다. Docker의 `--rm`과 호출자 signal만 믿는
대안은 이 재현 때문에 기각한다. 전역 이름이나 다른 작업의 컨테이너는 정리하지 않는다.
`TimeoutStopSec`와 Airflow task의 host unit 대기는 정리 시간을 포함한다. Docker daemon 장애나
호스트 강제 종료까지 정상 종료를 보장한다는 뜻은 아니며 정리 실패는 실패로 남는다.

구현은 기존 Rust/systemd/Docker Compose/Airflow만 재사용한다.
[Compose run 옵션](https://docs.docker.com/reference/cli/docker/compose/run/)과
[Compose external network](https://docs.docker.com/compose/how-tos/networking/#use-an-existing-external-network)를 따른다.
예약 중 컴파일, 별도 스케줄러, 별도 처리 완료 원장은 비용·권한·SSOT 중복 때문에 기각한다.
[systemd 실행 ID](https://github.com/systemd/systemd/blob/main/man/systemd.exec.xml)와
[ExecStopPost](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml)의 수명 주기를 사용한다.
운영 설치·실제 R2 쓰기·전체 자동반영의 판정은 [출시 로드맵](../roadmap/production-readiness.md)에 남긴다.

## 예약 실행 산출물의 버전과 자격증명 (2026-10-01)

소스 release만 바꾸고 공유 publisher를 덮어쓰면 다른 예약 작업도 새 실행 파일을 사용하며,
소스를 되돌려도 실행 파일은 돌아오지 않는다. 따라서 기존 설치기의 `prepare`, `publisher`,
`floor-config`가 소스·검증 바이너리·비밀 없는 실행 설정을 동일 release에 준비한다.
현재/이전 선택은 준비 단계에서 바꾸지 않는다. 바이너리와 설정은 SHA 확인 후 동일 디렉터리의
임시 파일을 hard link로 create-only 게시한다. 같은 파일의 재시도는 inode/mtime까지 유지하고,
다른 내용·경로의 심볼릭 링크는 거부한다. 설정 키는 설치된 env 예제에서 읽으며 다른 목록을 두지 않는다.
설치와 활성화는 기존 release 도구가 소유하고, 별도 배포 서비스나 완료 원장은 도입하지 않는다.

unit의 `RuntimeDirectory`가 비밀 없는 설정의 실행 중 사본을 보관한다. `ExecStart`가
`INVOCATION_ID` 파일을 create-only로 만들고 `ExecStopPost`도 동일 파일의 실제 release 경로를
호출한다. systemd가 종료 후 정리까지 이 디렉터리를 유지하고 마지막에 제거한다. 각 호출 때
`current`를 따로 해석하면 실행 중 버전 전환·롤백 후 다른 정리 프로그램이 호출될 수 있으므로
기각한다. 이 임시 사본은 처리 완료 원장이나 별도 비밀 저장소가 아니다.

처음 적용한 `LoadCredential`은 격리 Linux 시험 환경의 실제 systemd 255 보호 설정 아래에서
`ExecStart`는 성공했지만 `ExecStopPost`가 같은 파일을 읽지 못했다.
[상위 프로젝트 문제 32583](https://github.com/systemd/systemd/issues/32583)과
[시작 명령에만 적용된 수정 27279](https://github.com/systemd/systemd/pull/27279)에 해당하는 경로다.
보호 설정을 제거한 user-manager 시험은 이 조합의 검증이 아니었다. 종료 오류를 무시하거나
파일 시스템 보호를 없애는 대신 표준 `RuntimeDirectory` 수명 주기로 공개 설정을 전달한다.

호스트의 기본 디스크와 데이터 디스크는 같은 용량이 아니다. `timers`는 그 공개 설정에서
state/Ivy 경로를 읽어 전용 디렉터리를 준비한다. 설치기의 같은 namespace 허용
목록으로 설정 경로와 systemd의 쓰기 허용 범위를 검사·생성한다. sandbox는 허용된 두 전용 root만
포함하며 없는 root는 무시한다. 선택 경로만으로 전역 drop-in을 만들면 버전 전환·롤백 뒤
설정과 쓰기 권한이 어긋나므로 기각한다. unit에 두 번째 경로 목록을 넣지 않는다. 기존 `runtime_setup`이
remote 작업 경로를 선택된 state의 `remote` 하위 폴더로 만든다. 이 경로를 별도 공개 설정에 복제하지 않는다. 경로는 Foundation의 전용 namespace로 제한하고 심볼릭 링크·
기존 전용 디렉터리의 다른 소유자를 거부한다. 경로 검사 후 이름으로 권한을 바꾸면 그 사이에
폴더가 교체될 수 있으므로, no-follow 디렉터리 descriptor를 유지해 소유권·권한을 다룬다.
[Python의 descriptor·dir_fd](https://docs.python.org/3/library/os.html#files-and-directories)와
[systemd v255의 ReadWritePaths](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.exec.xml)를
사용하며 하위 데이터 파일을 재귀적으로 chown하지 않는다.
작은 시스템 디스크의 여유를 무시한 실행이나 작업 예산을 낮춰 통과시키는 대안은 기각한다.

systemd는 기존 자격증명 파일을 읽는다. 새 설정에는 비밀번호·토큰·DATABASE_URL을 넣을 수 없다.
호스트의 DB URL은 [Docker Compose config](https://docs.docker.com/reference/cli/docker/compose/config/)의
정규 JSON에서 기존 API 역할과 공개 loopback port를 읽고 Python 표준 URL 파서로 주소만 바꾼다.
최고관리자 비밀번호를 조합하는 네 번째 wrapper나 별도 비밀번호 사본을 만드는 대안은 기각한다.
명시적으로 전달한 DATABASE_URL은 격리 실행에서 유지하며, 운영 unit은 별도 값을 저장하지 않는다.
비밀을 포함하는 Compose 출력과 연결 문자열은 진단 로그에 싣지 않는다.

검증한 바이너리와 native image 내부 바이너리의 SHA-256을 대조하고 실제 명령을 실행한다.
기존 Dockerfile의 builder stage를 [BuildKit named context](https://docs.docker.com/reference/cli/docker/buildx/build/#additional-build-contexts---build-context)로
검증 산출물에 연결할 수 있다. 두 번째 Dockerfile·런타임 패키지 목록·별도 컴파일러를 만드는 대안은 기각한다.
이는 바이너리 재포장이며 Cargo 프로필을 변경한 새 빌드로 보고하지 않는다.
