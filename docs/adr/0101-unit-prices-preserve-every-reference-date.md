# ADR 0101: 세대 공시가격은 모든 기준일을 보존한다

- Status: Accepted
- Date: 2026-09-11
- Supersedes: [ADR-0095](./0095-unit-official-prices-are-reindexed-through-the-exclusive-register.md)의 연도 키와 투영 삭제 결정

## Context

같은 세대의 `20100101` 가격 36,000,000원과 `20100601` 가격 35,000,000원은
서로 다른 사실이다. `base_year`로 축약하면 가격의 시점을 잃고 Gold의 유일성 검사를
통과하지 못한다. 파생 Silver·Gold·Catalog에서도 자료를 삭제하거나 연도별 최고가로
접지 않는다. 기존 연간 행에는 월·일이 없으므로 `0101`을 붙이는 변환은 금지한다.

## Decision

1. `silver.unit_official_price`는 `base_date string`의 `YYYYMMDD`를 보존한다.
   정정공시는 정규화한 `(mgmt_key,base_date)` 안에서만 `ROW_NUMBER`와
   `notice_date DESC, price_won DESC`로 확정한다. 전유부 관리번호 유일성 검사는 유지한다.
   서로 다른 관리번호가 같은 표시 키를 만들면 Silver 행을 남기고 Gold·Catalog가
   `(pnu,dong_name,ho_name,base_date)` 중복을 거부한다. 표시명으로 가격을 고르지 않는다.
2. Gold·HTTP·R2·제품은 `official_price_history`에 모든 `(base_date,price_won)`을
   기준일 역순으로 싣는다. R2 문서는 `foundation-platform.building_by_pnu_profile.v2`다.
   Gold 계약의 `buildings_json`과 `unlinked_units_json`은 기존처럼 JSON 문자열이다.
3. 전진 마이그레이션은 기존 Catalog 표를 `unit_official_price_legacy_year`로 이름만
   바꾸고 보존한다. 새 `unit_official_price`의 PK는
   `(source_snapshot_id,pnu,dong_name,ho_name,base_date)`다. 값은 음수가 아니고 날짜는
   ASCII 숫자 8자리다. 기존 마이그레이션과 기존 행을 수정하지 않는다.
4. 적재기는 운영자가 명시한 완전한 원천 조합을 원자적으로 append한다.
   `unit_official_price_publication`에는 그 조합을 선택한 순서를 append한다.
   읽기는 마지막 선택의 **전체 배치**만 읽는다. 연도·가격별 선택은 하지 않는다.
   잠금 아래서 행과 선택을 함께 커밋하므로 부분 적재를 읽지 않는다. 모든 이전 배치는
   그대로 남고, 이전 조합을 다시 적재하면 내용 일치를 검사한 뒤 새 선택 행으로 롤백한다.
   같은 원천 ID의 행이 다르거나 빠지면 재실행을 거부한다.
5. 세 Catalog 표 모두 `UPDATE`, `DELETE`, `TRUNCATE`를 트리거로 거부한다.
   기존 범용 트리거는 publisher 우회 경로가 있어 이 불변식에 재사용할 수 없다.
   새 트리거 함수는 해당 세 표의 변경 거부만 담당한다.

## 전환 순서

1. 기존 연간 Silver 표와 Iceberg snapshot을 보존한다. 쓰기 작업을 정지한 상태에서
   `ALTER TABLE r2.silver.unit_official_price RENAME TO r2.silver.unit_official_price_legacy_year`
   로 이름을 옮긴다. 대상이 이미 있으면 덮어쓰거나 삭제하지 말고 상태를 확인한다.
   연간 표에서 날짜를 추정하거나 `DROP COLUMN base_year`로 과거를 숨기지 않는다.
2. 원래 `building_register_apartment_price`·`building_register_exclusive_unit`의 고정
   snapshot에서 모든 시도를 다시 생성한다. 배치 ID는 `base-date-v2`를 포함하여 이전
   의미의 재실행 기록과 구분한다. 기존 연간 스키마에 쓰려 하면 계약 검사가 거부한다.
3. Gold의 `--price-source-snapshot-id`에는 선택할 원천 조합을 지정한다. Iceberg snapshot
   안에 여러 배치가 있어도 그 조합의 모든 기준일만 읽는다. 여러 배치가 있는데 명시하지
   않거나 없는 조합을 선택하면 거부한다. 이전 Silver 행은 삭제하지 않는다.
4. Catalog 마이그레이션, 전체 배치 적재, Gold 재생성, R2 v2 발행과 소비자 배포를 함께
   조정한다. 새 Catalog 표가 비어 있는 동안 읽기는 빈 이력을 반환한다. 배치 적재를
   마치기 전에 소비자를 전환하면 서비스 공백이 생기므로 전환 전에 적재를 검증한다.
   R2 v2를 먼저 발행하고 게이트웨이 계약의 `manifest_edge_cache_seconds`가 지난 뒤
   웹을 전환한다. 새 웹은 `?schema=2`로 요청하여 브라우저와 edge에 남은 v1 응답을
   재사용하지 않는다. 게이트웨이는 이 정확한 query와 기존 query 없는 경로만 허용한다.
   이 변경의 로컬 검증은 운영 재적재나 R2 발행을 실행했다는 뜻이 아니다.

## 검토한 대안과 근거

- 연도별 최고가·최신가: 다른 기준일을 소실하므로 기각한다.
- 기존 연도에 임의 월·일 부여: 없는 사실을 만들므로 기각한다.
- Catalog에서 snapshot을 섞어 정정공시 재판정: Silver의 명시적 원천 선택을 바꾸고
  추가 관리번호·공시일 계약이 필요하다. 이미 검증한 전체 배치를 선택하는 기존 의미를 유지한다.
- DELETE 후 재적재: 이전 Catalog 배치를 지우므로 기각한다.
- 새 엔진·라이브러리: 불필요하다. [Spark 3.5.6 window 함수](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-window.html),
  [Iceberg 스키마 진화](https://iceberg.apache.org/docs/1.6.1/evolution/),
  [PostgreSQL INSERT](https://www.postgresql.org/docs/17/sql-insert.html)의 기존 기능을 쓴다.

## 검증

공유 SQL fixture는 한 해의 두 기준일, 나중의 더 낮은 정정가격, 동일 공시일 가격 순서를
검사한다. 실제 Spark fixture는 Silver Parquet부터 Gold JSON과 Rust R2 문서까지 같은
날짜 이력을 대조한다. PostgreSQL 테스트는 기존 행 보존, 배치 재실행, 이전 배치 재선택,
내용 변경 거부와 세 종류의 변경 금지를 확인한다. DTO·웹 테스트는 두 날짜를 그대로 읽는다.
