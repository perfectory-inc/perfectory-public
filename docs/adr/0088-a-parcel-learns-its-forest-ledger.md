# ADR 0088: 필지는 임야대장에서 임야의 지목·면적을 배운다

- Status: Accepted
- Date: 2026-09-07

## Context

ADR-0087의 토지특성 CSV는 토지대장 필지를 덮지만 임야대장 필지의 공부 면적과 지목은
채우지 못한다. 필지 도형의 면적을 대신 쓰거나 토지특성 표에 다른 원장의 열을 섞으면
값의 출처와 의미가 사라진다. 불변식은 **한 원천 원장 → 한 Silver 계약 → 한 서빙 표**다.

제공자 정본은 [브이월드 임야 데이터셋](https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?svcCde=NA&dsId=29)의
이름 있는 CSV 헤더다. 제공자 페이지는 이번 환경에서 열리지 않았으며, 열 이름·인코딩·
객체 선택의 실측 근거는 조정자가 제공한
[원천 객체 계약](../../platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-forest-source-objects.json)이다.
`bronze/source=vworldkr__land_forest/`에는 전체 임야대장 `AL_D003`과 변경분 `CH_D003`이
함께 있다. 전체 원장 `AL_D003`은 17개 시도 × 12개 빈티지, 204개 ZIP이고
최신 `20260607`이 17개 시도를 완전히 덮는다. 재수집은 필요 없다.

## Decision

1. `LAND_FOREST_LANE`은 `AL_D003_*.csv`만 읽는다. ADR-0087의 ZIP 선택, EUC-KR 해석,
   JSONL·gzip·R2 전송을 재사용한다. 도형은 기존 `vworldkr__parcel` Shapefile이 소유한다.
   별도 CSV 파서·전송기·Spark 적재기를 만들지 않는다.
2. 제공자의 16개 한글 헤더를 순서까지 정확히 검사한다. 열의 위치·이름 정본은
   `LAND_FOREST_LANE.expected_header`와 `csv_columns`이며, 원천 객체 계약의
   `field_layout_authority`가 실측 근거다. 헤더가 달라지면 출력을 승격하지 않는다.
   `silver.land_forest_ledger`는 원천의 모든 열과 세 계보 열을 보존한다.
   `area_m2`는 양수 유한 숫자, `co_owner_count`는 nullable 비음수 32비트 정수다.
   `ownership_kind_code`는 앞자리 0을 보존하는 문자열이다. 무효 행은 이유별로 계수한다.
3. `dataset_series=AL_D003`, `load_granularity=sido`, 내부 CSV 이름·객체 경로를 검사한다.
   가장 최신의 완전한 17개 시도 빈티지를 선택하고 중복 객체·중복 시도·부분 전국을
   거부한다. 투영은 객체 계보·스냅샷·PNU 시도를 검사한다. `source_vintage`는 원천 객체
   계약의 빈티지이며 `data_reference_date`와는 별개다.
4. `catalog.parcel_forest_ledger`는 `pnu character(19)` 기본키와 `land_category`,
   `area_m2 numeric`, `ownership_kind`, `co_owner_count int`, `source_snapshot_id`,
   `source_vintage date`, `loaded_at`을 가진다. `ownership_kind`는 원천
   `ownership_kind_code`다. **소유구분은 개인·국유·공유 등의 범주 코드이지 소유자 신원이 아니다.**
   경계 수집보다 원장이 먼저 올 수 있으므로 `catalog.parcel` 외래키를 두지 않는다.
   PNU 조회의 `$1::character(19)` 캐스트를 유지한다.
5. 기존 SQLx·PostgreSQL 경로로 COPY → 임시 표 → PNU별 `DISTINCT ON` → 조건부 upsert를
   수행한다. 전국 입력은 한 트랜잭션이며 오래된 빈티지와 같은 빈티지의 재실행은 최신
   값을 덮지 않는다. 면적·공유 인수·PNU·계보 제약은 DB에서도 강제한다.
6. by-pnu API의 nullable `forest_ledger{land_category, area_m2, ownership_kind,
   co_owner_count}`와 Gongzzang 소비 계약을 함께 갱신한다. `characteristics`와
   `forest_ledger`는 각각 자기 원장의 값을 제공한다.

## 대안과 재사용 근거

- `catalog.parcel_characteristic`에 병합: 원천·대장·열이 달라 계보와 계약이 혼합되므로 기각한다.
- `CH_D003` 증분 재구성: 완전한 원장 CSV가 이미 있으므로 삭제·순서·누락 복구 상태를 새로
  관리할 이유가 없다. 이번 전체 적재에서는 기각한다.
- 새 적재 프레임워크나 직접 작성한 DB 전송기: 기존 `encoding_rs`, `zip`, `flate2`, SQLx,
  Spark 계약 적재기가 필요한 역할을 충족하므로 추가 의존성과 라이선스·운영 부담을 피한다.
- PostgreSQL 17 공식 [COPY](https://www.postgresql.org/docs/17/sql-copy.html)와
  [INSERT ON CONFLICT](https://www.postgresql.org/docs/17/sql-insert.html)의 연결 기반 적재·
  조건부 갱신을 사용한다. 별도 데이터 플레인을 구현하지 않는다.

## 실행과 검증

- `export-land-forest-silver-handoff`: `FOUNDATION_PLATFORM_LAND_FOREST_` 접두사의
  `INPUT_PATH`/`INPUT_OBJECT_KEY`, `OUTPUT_PATH`/`OUTPUT_OBJECT_KEY`, `SOURCE_SNAPSHOT_ID`,
  선택 `SUMMARY_PATH`를 받는다.
- `silver_scalar_handoff_to_lakehouse.py --contract silver.land_forest_ledger`는 기존
  객체 단위 적재 SSOT를 사용한다.
- `load-parcel-forest-ledger-catalog-projection`: `DATABASE_URL`, 기존 R2 설정,
  `FOUNDATION_PLATFORM_PARCEL_FOREST_LEDGER_PROJECTION_LOAD_CONFIRM=true`가 필요하다.
  `LAND_FOREST_SOURCE_CONTRACT`는 선택 계약 경로다.
- EUC-KR ZIP fixture가 모든 열의 대응과 헤더 변경·변경분 거부를 검사한다. Rust/JSON 계약,
  17개 시도 선택, COPY 병합·최신 빈티지·롤백, API와 소비 pin을 검증한다.
- 전국 Spark 실행과 운영 적재·승격은 조정자의 병합 후 작업이다. 이 변경의 로컬 검증으로
  실제 전국 적재 완료를 주장하지 않는다.
