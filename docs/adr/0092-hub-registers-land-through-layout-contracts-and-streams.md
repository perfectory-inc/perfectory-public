# ADR 0092: 건축HUB 대장 계열은 레이아웃 계약과 스트리밍으로 착지한다

- Status: Accepted
- Date: 2026-09-07

## Context

건축HUB `mart_djy_08.txt`는 헤더 없는 UTF-8 파이프 구분 25칸 파일이다.
ADR-0087의 이름 있는 한국어 CSV 헤더 검사를 그대로 적용할 수 없다. 원천 칸의
위치를 코드가 임의로 해석하거나 추정한 이름을 붙이면 조용히 다른 값을 적재한다.
불변식은 **레이아웃 계약이 위치의 유일한 정의이며 원천 25칸을 그대로 보존한다**다.

코디네이터의 R2 목록·256KB 부분 해제 실측은
[원천 객체 계약](../../platforms/foundation-platform/infra/lakehouse/contracts/hub-building-register-apartment-price-source-objects.json)에 기록한다.
202604~202608 전국 전량 ZIP은 다섯 개이며 최신 파일은 5,893,057,447바이트다.
해제 원문은 약 43GB로 추정되고 서버 루트 여유는 약 27GB이므로 전체 파일을
로컬 디스크 또는 메모리에 만든 뒤 변환하는 방식은 사용할 수 없다.

## Decision

1. 원천 계약의 `selected_vintage`에 해당하는 전국 객체 하나만 변환한다.
   `columns`는 검증된 소비 칸 아홉 개의 index·의미만 정의한다. 나머지 칸은
   `raw_columns`의 원래 index로 보존하고 의미를 추정하지 않는다. `has_header=false`,
   `column_count=25`, `csv_delimiter="|"`, `encoding="utf-8"`를 검증한다.
2. 기존 `R2SeekableObjectReader`의 bounded Range GET·ETag 고정을 재사용한다.
   `zip::ZipArchive`는 단일 멤버·ZIP64 크기·CRC 메타데이터 검증에 사용한다.
   로컬 헤더의 이름 길이(26..28)·extra 길이(28..30)에서 데이터 시작을 확인하고,
   압축 크기로 제한한 스트림을 `flate2::bufread::DeflateDecoder`로 증분 해제한다.
   암호화·다른 압축 방식·다른 멤버·잘린 ZIP·CRC/크기 불일치는 승격하지 않는다.
3. 첫 행의 칸수가 계약과 다르면 파일을 거부한다. 이후 칸수 불일치는
   `rejected_rows`와 제한된 표본 로그로 남긴다. UTF-8 오류 또는 `max_row_bytes`를
   넘는 행은 변환 실패다. 이름 있는 소비 값은 문자열로 보존하며 가격·날짜를
   추측해서 정수·날짜로 강제 변환하지 않는다. 빈칸·공백·인용부호도 배열에 남긴다.
4. PNU 조립은 `standard_pnu_from_hub_register_codes`(Foundation ADR 0023)를
   재사용하고 코드 길이·숫자 및 `Pnu::parse`를 검증한다. HUB `0/1`은 표준
   `1/2`로 변환하고 블록 `2`·누락·잘못된 코드는 `pnu=null`, `pnu_bad`로
   계수한다. PNU를 만들 수 없다는 이유로 가격 관측 행을 버리지 않는다.
5. `rows_per_part=10000000`의 출력 행마다 `part-NNNN.jsonl.gz`를 회전한다.
   `silver_handoff_io::open_sink`의 create-only 멀티파트 업로드·실패 시 abort를
   사용한다. 각 시도는 별도 `attempt=<uuid>` 디렉터리를 사용하며 전체 ZIP 검증
   이후에만 고정 위치 `manifest.json`을 create-only로 발행한다. 완료되지 않은
   시도의 기존 부분 파일은 입력 계획에 들어가지 않고 재시도는 새 시도를 쓴다.
6. summary JSON은 `rows_read = rows_emitted + rejected_rows`,
   `rows_emitted = pnu_ok + pnu_bad = sum(parts.rows)`를 만족한다. `parts`에
   객체 키·행 수·바이트 수를 기록한다. 선택 `SUMMARY_PATH`는 R2 manifest 발행
   후 같은 내용을 원자적으로 기록한다. 실패 시 남은 이전 부분 파일은 운영
   정리 대상이며 입력으로 검색하지 않는다.
7. `silver.building_register_apartment_price`는 소비 칸·nullable PNU·
   `raw_columns array<string>`·`vintage`·계보를 append-only로 보존한다.
   `append_only` 계약 게이트는 Spark 적재기가 overwrite를 실행 전에 거부하게 한다.
   `source_record_id`는 Bronze 객체, `source_line_number`는 물리 행 번호,
   `source_part_id`는 실제 핸드오프 객체다. 적재 단위는 `source_part_id`다.
   Bronze 키만 비교하면 첫 부분 이후를 이미 적재했다고 건너뛰므로 그렇게 하지 않는다.
   관리 PK 단독 또는 날짜 결합의 유일성은 실측하지 않아 제약으로 가정하지 않는다.
8. 기존 `land-use-batch-load.sh`는 원천 계약의 `load_granularity`·개수를 읽는다.
   `manifest_parts`는 완료 summary의 선택 빈티지·원천 키·부분 순서·카운터를
   대조하고 그 목록만 Spark에 전달한다. 프로세스 치환이 Python 실패 코드를
   숨기지 않도록 결과를 먼저 성공 판정한다. 부분별 행 수는 Spark의
   `--expected-count`로 전달하며 배열은 Spark/Iceberg의 `ARRAY<STRING>`을 쓴다.
9. 서빙 DB 마이그레이션·투영·공개 조회 변경은 만들지 않는다. 조인 전수 측정과
   별도 ADR이 투영 설계의 선행 조건이다. 그래프는 Silver 연결까지만 명시한다.
10. 공개 fixture의 실물 식별자 금지는 유지한다. 원천 계보를 합성 이름으로 바꾸면
    R2 객체를 식별할 수 없으므로 `infra/lakehouse/contracts/*-source-objects.json`의
    `object_key` 문자열 값만 JSON 파싱으로 허용한다. 중복 JSON 키는 거부하며
    문서·주석·코드·계약의 다른 필드는 같은 금지 검사를 받는다. 검사는
    `public-fixture-safety.py` 한 곳이 소유하고 기존 공개 저장소 가드가 호출한다.

## 대안과 재사용 근거

- 전체 ZIP/해제 파일 다운로드: 여유 디스크보다 큰 원문을 실물화하므로 기각한다.
- 모든 vintage 동시 적재: 월별 전량 중복을 만들므로 최신 선택만 한다.
- 헤더 추정·25개 의미 전부 명명: 검증하지 않은 의미가 계약으로 굳으므로 기각한다.
- 새로운 ZIP 구현·HTTP 클라이언트·업로더: 기존 `zip`(MIT), `flate2`(MIT/Apache-2.0),
  S3 SDK와 저장소의 전송 어댑터가 ZIP64·Range·멀티파트를 지원하므로 새 의존성을
  추가하지 않는다. custom 코드는 레이아웃 어댑터·회전·승격 제어에 한정한다.
- ZIP local header만 신뢰: data descriptor·ZIP64·잘린 파일을 놓치므로 중앙
  디렉터리의 크기·CRC까지 검증한다.
- R2 prefix 검색으로 적재: 이전 실패 시도의 일부 파일도 완전 입력처럼 보이므로
  완료 manifest만 입력 목록으로 사용한다.
- 원천 파일 형식은 [PKWARE APPNOTE §4.3.7·§4.3.9·§4.5.3](https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT),
  증분 해제는 [flate2 DeflateDecoder](https://docs.rs/flate2/1.1.9/flate2/bufread/struct.DeflateDecoder.html),
  ZIP 메타데이터는 [zip ZipFile](https://docs.rs/zip/2.4.2/zip/read/struct.ZipFile.html)을 따른다.

## 실행과 검증

`export-building-register-apartment-price-silver-handoff`는
`FOUNDATION_PLATFORM_APARTMENT_PRICE_` 접두사의 `INPUT_OBJECT_KEY`,
`OUTPUT_OBJECT_PREFIX`, `SOURCE_SNAPSHOT_ID`, 선택 `SUMMARY_PATH`를 읽는다.
`OUTPUT_OBJECT_PREFIX` 아래 원천 ZIP stem/시도/부분 파일과 stem/manifest를 기록한다.

적재는 `LAND_USE_PLAN_SOURCE_CONTRACT`를 이 원천 계약으로,
`SOURCE_HANDOFF_MANIFEST`를 exporter의 `SUMMARY_PATH`로 설정한 뒤
`land-use-batch-load.sh validate building_register_apartment_price`,
`land-use-batch-load.sh load building_register_apartment_price` 순서로 실행한다.
출력 prefix를 재정의했다면 적재기의 `SOURCE_HANDOFF_OUTPUT_PREFIX`도 동일하게 명시한다.

합성 `99999` PNU ZIP으로 스트리밍·ZIP64·원문 보존·회전·PNU 실패 보존·첫 행
거부·이후 행 거부·무결성 오류와 미완료 manifest 거부를 검증한다. Rust 계약과
Spark JSON artifact는 기존 일치 테스트로 대조한다. fixture 결과는 전국 운영
적재량·조인율·원천 행 수를 측정한 증거가 아니다.
