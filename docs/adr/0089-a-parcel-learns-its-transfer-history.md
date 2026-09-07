# ADR 0089: 필지는 토지이동이력에서 제 연혁을 배운다

- Status: Accepted
- Date: 2026-09-07

## Context

임야대장(ADR-0088)은 필지의 현재 속성이지만 토지이동이력은 한 필지에 여러 사건이 있는
연혁이다. PNU별 마지막 행만 남기는 기존 투영을 그대로 쓰면 분할·합병·지목변경 이력이
사라진다. 불변식은 **원천 사건 한 행을 보존하고 `(pnu, transfer_history_seq)`로 식별한다**다.
순번의 필지 내 유일성은 가정이 아니라 적재 때 검사할 계약이다.

조정자가 측정한 [원천 객체 계약](../../platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-transfer-history-source-objects.json)은
`bronze/source=vworldkr__land_transfer_history/`의 `AL_D157` 전체 이력 153개 ZIP,
17개 시도 × 9개 빈티지와 최신 완전 빈티지 `20260531`을 기록한다.
EUC-KR CSV의 18개 이름 있는 헤더가 필드 배치의 근거다.

## Decision

1. `LAND_TRANSFER_LANE`은 `AL_D157_*.csv`만 허용하고 `CH_D157` 변경분은 거부한다.
   기존 ZIP 선택·EUC-KR·CSV·JSONL·gzip·R2 경로를 재사용한다. 헤더 이름과 순서가
   다르면 승격하지 않는다. `silver.land_transfer_history`는 원천 18개 열과 계보 3개를
   보존하며 PNU 중복을 제거하지 않는다. `transfer_history_seq`는 필수 64비트 정수다.
   원천의 폐쇄·말소 사건과 빈 날짜, 빈 면적·0 면적도 유지한다. 면적이 있으면 유한 숫자다.
2. 원천 계약이 지정한 빈티지가 최신 완전한 17개 시도 묶음인지 검사한다. 중복 객체·
   중복 시도·잘못된 내부 CSV 이름·불완전한 전국 입력은 거부한다. 여러 빈티지를 섞지 않는다.
3. `catalog.parcel_transfer_event`의 기본키는 `(pnu character(19), transfer_history_seq bigint)`다.
   `reason_code`, `reason`, `moved_at`, `erased_at`, `land_category`, `closure_seq`는 nullable text,
   `area_m2`는 nullable numeric, `source_snapshot_id`와 `loaded_at`은 필수다.
   날짜는 제공자 표기를 유지하며 소비자가 형식을 정한다. 원장 수집이 경계보다 먼저 올 수
   있으므로 `catalog.parcel` 외래키는 두지 않는다.
4. 단일 트랜잭션에서 17개 시도 전체를 COPY로 임시 표에 넣은 뒤 한 번 병합한다.
   임시 표는 서빙 표의 제약·기본키를 `LIKE ... INCLUDING CONSTRAINTS INCLUDING INDEXES`로
   상속한다. 같은 빈티지 안의 키 충돌은 COPY가 거부하고 전국 입력을 롤백한다.
   `DISTINCT ON`이나 덮어쓰기로 충돌을 숨기지 않는다. 이후
   `INSERT ... ON CONFLICT (pnu, transfer_history_seq) DO NOTHING`으로 새 사건만 추가한다.
   이전 적재의 사건과 충돌하는 재실행은 기존 사실을 보존한다. 기존 사건의 수정·삭제는
   이번 전체 이력 적재의 기능이 아니다. 읽은 행·COPY 행·신규 사건·기존 사건 수를 대조해 기록한다.
5. by-pnu의 `transfer_history`는 `Vec<ParcelTransferEventResponse>`이며 기본값은 빈 배열이다.
   `reason`, `reason_code`, `moved_at`, `erased_at`, `land_category`, `area_m2`, `history_seq`,
   `closure_seq`를 전달한다. `$1::character(19)`로 조회하고
   `moved_at DESC NULLS LAST, transfer_history_seq DESC`로 전체 목록을 제공한다.
   임의 상한은 두지 않는다. Gongzzang은 원천 문자열과 배열 순서를 그대로 통과시킨다.

## 대안과 재사용 근거

- PNU 단일키와 최신 행 upsert: 사건을 잃으므로 기각한다.
- `CH_D157`으로 증분 이력 재구성: 완전 원장이 이미 있어 순서·누락 복구 상태를 추가하지 않는다.
- 중복 키를 임의 선택하거나 무시: 원천 유일성이 깨졌음을 숨기므로 임시 표의 키 제약으로 거부한다.
- 직접 만든 중복 검출기·전송기·새 의존성: 기존 PostgreSQL·SQLx·`encoding_rs`·`zip`·`flate2`와
  Spark 계약 적재기가 충족한다. 이미 채택한 도구로 운영·보안·라이선스 부담을 늘리지 않는다.
- PostgreSQL 17 공식 [COPY](https://www.postgresql.org/docs/17/sql-copy.html),
  [기본키 제약](https://www.postgresql.org/docs/17/ddl-constraints.html),
  [ON CONFLICT](https://www.postgresql.org/docs/17/sql-insert.html)의 표준 동작을 사용한다.

## 실행과 검증

- `export-land-transfer-history-silver-handoff`: `FOUNDATION_PLATFORM_LAND_TRANSFER_` 접두사의
  `INPUT_PATH`/`INPUT_OBJECT_KEY`, `OUTPUT_PATH`/`OUTPUT_OBJECT_KEY`, `SOURCE_SNAPSHOT_ID`, 선택 `SUMMARY_PATH`.
- `silver_scalar_handoff_to_lakehouse.py --contract silver.land_transfer_history`는 기존 객체 단위 적재다.
- `load-parcel-transfer-event-catalog-projection`: `DATABASE_URL`, 기존 R2 설정,
  `FOUNDATION_PLATFORM_PARCEL_TRANSFER_EVENT_PROJECTION_LOAD_CONFIRM=true`, 선택 `LAND_TRANSFER_SOURCE_CONTRACT`.
- `99999` 합성 PNU의 EUC-KR ZIP → JSONL → COPY → 실제 조회 어댑터 경로를 검사한다.
  같은 PNU의 서로 다른 사건 보존·말소/폐쇄 보존·날짜/순번 정렬·빈 목록·헤더 변경 거부,
  같은 빈티지의 동일/상이한 중복 사건 모두 거부·트랜잭션 롤백·재실행 멱등성을 검증한다.
- 전국 Spark 실행과 운영 적재는 이 작업 범위가 아니다. PK 유일성의 전국 실측은 실제
  선택 빈티지를 COPY할 때 강제된다. 합성 fixture 통과를 전국 원천의 유일성 실측으로 보고하지 않는다.
