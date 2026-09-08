# ADR 0094: 전유부 대장은 가격과 세대 사이의 다리다

- Status: Accepted
- Date: 2026-09-08

## Context

ADR-0092의 공동주택가격 `mart_djy_08`은 약 2.02억 행이 Silver에 착지했지만 동명·호명이
없다. 따라서 그 가격을 PNU만으로 세대에 붙이면 같은 필지의 서로 다른 호에 가격을
잘못 귀속한다. 코디네이터가 실측한 전유부 `mart_djy_09`에는 가격 파일과 같은
10자리 관리번호 체계와 동명·호명이 함께 있다. 연결 후보는
`가격(관리번호×기준일) → 전유부(관리번호→동·호) → catalog.building_unit(PNU+동·호)`다.
관리번호 체계의 공유는 조인율·유일성·시점 일치가 검증됐다는 뜻은 아니다.

R2 `bronze/source=hubgokr__building_register_exclusive_unit/`에는 202604~202608의
월별 전국 ZIP 다섯 개가 있다. 최신 파일은 917,907,836바이트이며 단일 deflate 멤버
`mart_djy_09.txt`를 담는다. 헤더 없는 UTF-8 파이프 27칸이고 0번 관리번호,
8~12번 PNU 구성요소, 21번 동명, 22번 호명이 실측됐다. 객체 키·크기·선택 빈티지의
정본은 [원천 객체 계약](../../platforms/foundation-platform/infra/lakehouse/contracts/hub-building-register-exclusive-unit-source-objects.json)이다.
24번 층구분명과 25번 층번호는
[필드 매핑의 `mart_djy_09`](../../platforms/foundation-platform/docs/catalog/building-register-field-mapping.v1.draft.md)와
표본을 교차 확인했다. 다른 칸의 의미는 이 결정에서 확장하지 않는다.

전국 ZIP 처리·회전·업로드를 레인마다 복제하면 무결성이나 실패 복구의 수정이 한쪽에만
적용된다. 불변식은 **소비 위치는 원천 계약 하나, 출력 스키마는 Silver 계약 하나,
착지 상태 전이는 공유 엔진 하나가 소유하며 원천 27칸과 모든 유효 폭의 관측을 보존한다**다.

## Decision

1. `selected_vintage="202608"`에 해당하는 전국 ZIP 하나만 처리한다. 원천 계약은
   ADR-0092와 같은 스키마를 쓰며 `column_count=27`, `has_header=false`,
   `encoding="utf-8"`, `csv_delimiter="|"`, `inner_file="mart_djy_09.txt"`를 선언한다.
   계약의 소비 칸은 `mgmt_key`, `sigungu_cd`, `bjdong_cd`, `san_gubun`, `bonbeon`,
   `bubeon`, `dong_name`, `ho_name`, `floor_kind`, `floor_no`다. 값은 문자열 그대로
   보존한다. 층구분명을 층 코드로 바꾸거나 층번호를 숫자로 강제 변환하지 않는다.
2. `hub_register_silver_export`가 계약 검증·ZIP 스트리밍·행 변환·PNU 조립·부분 회전·
   manifest 발행을 소유한다. 공동주택가격과 전유부의 어댑터는 환경변수 접두사와
   원천·Silver 계약만 공급한다. 계약의 소비 열 집합을 Silver 열에서 생성 열을 뺀
   집합과 대조하고 PNU 입력 누락·생성 열 덮어쓰기·중복 또는 범위 밖 위치를 거부한다.
   위치 맵을 Rust 분기문에 다시 만들지 않는다.
3. 기존 `R2SeekableObjectReader`의 bounded Range GET·ETag 고정,
   `zip::ZipArchive`의 ZIP64·중앙 디렉터리 검증,
   `flate2::bufread::DeflateDecoder`의 증분 해제,
   `silver_handoff_io::open_sink`의 create-only multipart·실패 abort를 재사용한다.
   선택 객체 크기를 실측 계약과 대조하고 ZIP 멤버·로컬 헤더·CRC·압축/해제 크기를
   검증한다. `rows_per_part=10000000`, `max_row_bytes=1048576`을 따른다.
4. 첫 행이 27칸이 아니면 전체 변환을 거부한다. 이후 잘못된 폭의 행은
   `rejected_rows`와 제한된 표본으로 남긴다. UTF-8 오류·행 크기 초과·ZIP 무결성
   오류에는 완료 manifest를 만들지 않는다. `raw_columns`는 27칸의 빈칸·공백·따옴표를
   순서대로 보존한다. 행 번호는 거부 행을 포함한 물리 행 번호다.
5. PNU는 기존 `standard_pnu_from_hub_register_codes`와 `Pnu::parse`로 조립한다.
   HUB `0/1`은 표준 `1/2`로 변환한다. 블록 `2`나 잘못된 구성요소는 `pnu=null`,
   `pnu_bad`로 보존하며 그 관측을 버리지 않는다. 관리번호·동호 유일성을 가정하지 않는다.
6. `silver.building_register_exclusive_unit`는 소비 열·nullable `pnu`·
   `raw_columns array<string>`·`vintage`와 계보 열을 append-only로 저장한다.
   Spark 계약 artifact에 표를 추가하고 기존 Rust/artifact 동등성 검사로 드리프트를
   막는다. 적재 단위는 실제 부분 객체 `source_part_id`이며 Bronze 객체 키만 비교하지
   않는다. `append_only` 게이트가 overwrite를 Spark 접속 전에 거부한다.
7. 각 시도는 `attempt=<uuid>/part-NNNN.jsonl.gz`에 기록한다. ZIP 전체가 검증된 후에만
   고정 `manifest.json`을 create-only로 발행한다. summary는
   `rows_read=rows_emitted+rejected_rows`, `rows_emitted=pnu_ok+pnu_bad=sum(parts.rows)`를
   만족한다. 적재기는 검증한 완료 manifest의 부분 목록만 소비한다. 미완료 시도의
   부분 파일은 검색해서 적재하지 않으며 운영 정리 대상으로 남는다.
8. `export-building-register-exclusive-unit-silver-handoff`는
   `FOUNDATION_PLATFORM_EXCLUSIVE_UNIT_` 접두사의 `INPUT_OBJECT_KEY`,
   `OUTPUT_OBJECT_PREFIX`, `SOURCE_SNAPSHOT_ID`, 선택 `SUMMARY_PATH`를 읽는다.
   `land-use-batch-load.sh`에 `LAND_USE_PLAN_SOURCE_CONTRACT`로 전유부 원천 계약,
   `SOURCE_HANDOFF_MANIFEST`로 완료 summary를 전달한다. `validate`와 `load`의 표 인자는
   `building_register_exclusive_unit`다. 출력 prefix를 바꾸면
   `SOURCE_HANDOFF_OUTPUT_PREFIX`도 같은 값으로 설정한다.
9. 그래프는 원천에서 Silver까지 구현된 경로만 추가한다. 서빙 DB 투영·마이그레이션·
   가격 조인은 추가하지 않는다. Silver 착지 후 관리번호 겹침율·중복도·PNU와 동호
   정규화 결과·빈티지 간 안정성을 측정하고 별도 ADR을 작성해야 투영을 시작할 수 있다.

## 대안과 재사용 근거

- 공동주택가격만 PNU로 세대에 조인: 동호가 없어 세대별 귀속을 증명할 수 없으므로 기각한다.
- 전유부 전용 변환기 복제: 동일한 ZIP·회전·발행 불변식이 두 구현으로 갈라지므로 기각한다.
- 모든 대장 형식을 미리 일반화: 현재 검증한 두 레인의 계약만 받는 공유 엔진으로 충분하다.
  미측정 레이아웃·투영까지 플러그인 체계로 확장하지 않는다.
- 새로운 다운로드·ZIP·압축·업로드 라이브러리: ADR-0092가 평가한 `zip`(MIT),
  `flate2`(MIT/Apache-2.0), 기존 S3 전송 어댑터를 그대로 쓴다. 새 의존성·서비스는 없다.
  [flate2 증분 해제 API](https://docs.rs/flate2/1.1.9/flate2/bufread/struct.DeflateDecoder.html)와
  [PKWARE APPNOTE](https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT)의 기존 보장을
  재사용하며 새 코드는 레인 계약 연결에 한정한다.

## 검증과 후속 조건

합성 `99999` PNU와 동호가 든 ZIP fixture로 원문 27칸·층 문자열·PNU 실패 보존·부분 회전·
계보·첫 행 26/28칸 거부·이후 잘못된 폭 계수·CRC 실패 후 manifest 부재를 검증한다.
공유 코어 추출 전 공동주택가격 테스트의 관측 단정은 유지한다. Spark 테스트는 소비 칸과
실측 인벤토리·출력 스키마·append-only를 대조하고 Rust/artifact 동등성 검사를 함께 실행한다.
fixture 검증은 전국 전유부의 운영 적재나 관리번호 조인율 측정을 대신하지 않는다.
