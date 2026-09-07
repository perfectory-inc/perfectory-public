# ADR 0090: 필지는 대지권 등록을 배운다

- Status: Accepted
- Date: 2026-09-07

## Context

대지권등록은 한 필지에 서 있는 건물의 동·층·호·실별 권리 원장이다. PNU 하나로
덮어쓰면 등록 단위를 잃고, 일련번호를 정수로 바꾸면 앞자리 0을 잃는다. 불변식은
**원천 한 행을 `(pnu, right_serial_no)`로 보존하고 등록 명칭·비율을 재해석하지 않는다**다.
ADR-0089의 여러 행 적재 경로가 이를 충족하지만 CSV 구분자가 쉼표로 고정되어 있다.

[측정 원천 계약](../../platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-right-registration-source-objects.json)은
`bronze/source=vworldkr__land_right_registration/`의 `AL_D006` 전체 원장 204개 ZIP,
17개 시도 × 12개 빈티지와 최신 완전 빈티지 `20260609`를 기록한다.
18개 이름 있는 EUC-KR CSV 헤더와 `csv_delimiter: "|"`가 배치의 근거다.

## Decision

1. `LAND_RIGHT_LANE`은 `AL_D006_*.csv`만 읽고 `CH_D006` 변경분은 거부한다.
   공통 `Lane`에 선택 구분자를 추가하고 기본값은 쉼표, 이 원장은 `b'|'`로 둔다.
   `CsvRecords`의 필드 분리만 구분자에 따르고 인용·줄바꿈·EUC-KR 복원 규칙은 유지한다.
   헤더 이름·순서가 다르면 승격하지 않는다.
2. `silver.land_right_registration`은 CSV 순서의 원천 18개 열과 계보 3개를 보존하는
   17번째 계약이다. `right_serial_no`는 필수 숫자 문자열이며 정수 변환하지 않는다.
   `building_name`, `dong_name`, `floor_name`, `ho_name`, `room_name`, `right_ratio`는
   공백·구분자·인용부호를 포함해 원문 그대로 유지한다. 빈 필드는 null이고,
   `3분의1` 같은 비율을 수치로 해석하지 않는다. 공개 등록 원장의 건물·전유부 명칭은
   등록 사실로 전달한다. 폐쇄 행과 `closure_kind_code`·`closure_kind_name`도 남긴다.
3. `catalog.parcel_land_right`의 기본키는 `(pnu character(19), right_serial_no text)`다.
   숫자 문자열 검사와 스냅숏 필수 제약을 둔다. 명칭·비율·`closure_kind_code`·
   `closure_kind`는 nullable text이며 `closure_kind`는 원천 `closure_kind_name`이다.
   `source_snapshot_id`와 `loaded_at`은 필수다. 경계보다 원장이 먼저 올 수 있으므로
   `catalog.parcel` 외래키는 두지 않는다.
4. 최신 완전한 17개 시도 빈티지만 단일 트랜잭션에서 COPY한다. 임시 표는
   `LIKE ... INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES`로 서빙 제약을
   상속한다. 같은 빈티지의 동일·상이한 중복키 모두 거부하고 전체를 롤백한다.
   이후 `ON CONFLICT (pnu, right_serial_no) DO NOTHING`으로 새 등록만 추가한다.
   재실행은 기존 사실을 바꾸지 않으며 원천 수정·삭제 반영은 이번 경로의 기능이 아니다.
   읽은 행·COPY 행·삽입 행·기존 행 수를 대조한다.
5. by-pnu의 `land_rights`는 기본값이 빈 배열인 전체 목록이다.
   `list_parcel_land_rights_by_pnu`는 `$1::character(19)`로 조회하고
   `ORDER BY right_serial_no ASC`의 문자열 순서를 사용한다. 임의 상한을 두지 않는다.
   Gongzzang은 공개 계약을 통해 명칭·비율·폐쇄 정보와 목록 순서를 그대로 전달한다.

## 대안과 재사용 근거

- PNU 단일키 upsert: 같은 필지의 다른 등록을 잃으므로 기각한다.
- 일련번호 bigint·비율 numeric: 앞자리 0·큰 식별자·비수치 비율을 잃으므로 기각한다.
- `CH_D006` 증분 재구성: 완전 원장이 있어 별도 순서·누락 복구 상태를 만들지 않는다.
- 독립 CSV 파서·중복 검출기·전송기·새 의존성: 기존 바이트 파서에 구분자만 전달하고
  `encoding_rs`, `zip`, `flate2`, SQLx와 PostgreSQL을 재사용한다. 새로운 데이터 전송 계층을
  만들 필요가 없어 기존 의존성의 유지보수·라이선스·호환성 경계를 넓히지 않는다.
- PostgreSQL 17 공식 [CREATE TABLE](https://www.postgresql.org/docs/17/sql-createtable.html)의
  `INCLUDING INDEXES`, [COPY](https://www.postgresql.org/docs/17/sql-copy.html)의 제약 검사,
  [INSERT](https://www.postgresql.org/docs/17/sql-insert.html)의 충돌 처리를 사용한다.

## 실행과 검증

- `export-land-right-registration-silver-handoff`: `FOUNDATION_PLATFORM_LAND_RIGHT_` 접두사의
  `INPUT_PATH`/`INPUT_OBJECT_KEY`, `OUTPUT_PATH`/`OUTPUT_OBJECT_KEY`, `SOURCE_SNAPSHOT_ID`, 선택 `SUMMARY_PATH`.
- `silver_scalar_handoff_to_lakehouse.py --contract silver.land_right_registration`: 기존 객체 단위 적재기.
- `load-parcel-land-right-catalog-projection`: `DATABASE_URL`, 기존 R2 설정,
  `FOUNDATION_PLATFORM_PARCEL_LAND_RIGHT_PROJECTION_LOAD_CONFIRM=true`, 선택 `LAND_RIGHT_SOURCE_CONTRACT`.
- `99999` 합성 PNU의 pipe EUC-KR ZIP → JSONL → COPY → 조회 어댑터를 검증한다.
  원문·앞자리 0·큰 일련번호·폐쇄 행 보존, 헤더/구분자 변경 거부, 중복키 롤백,
  최신 완전 빈티지, 재실행 멱등성, 전체 목록·정렬·빈 배열과 소비자 pin을 검사한다.
- 전국 실행과 운영 적재는 수행하지 않는다. fixture 검증은 전국 원천의 키 유일성
  실측을 뜻하지 않으며, 실제 적재 시 같은 데이터베이스 제약이 이를 강제한다.
