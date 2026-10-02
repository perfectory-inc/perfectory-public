---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-02
---

# ADR-0131 — 지번 연결은 실제로 읽은 모든 입력에 묶는다

## 원인과 불변식

`parcel_lineage_to_silver.py`는 논리적 원본 번호로 행을 골랐지만 물리적 Iceberg 판은
고정하지 않았다. 경계·법정동 코드·토지이동 이력·선택적 건물 자료가 실행 중 바뀌면
동일한 인자로 다른 사실을 읽을 수 있었다. 중복 적재 방지용 `derivation_run_id`도 날짜
구간과 소유자 입력 내용을 제외해, 달라진 작업을 이미 처리한 작업으로 볼 수 있었다.

모든 입력을 실행 전에 고정하고, 그 동일한 입력 설명에서 실행 번호와 요약을 만들어야
한다. 자료를 보관한 날짜·원본의 기준 시점·Iceberg 커밋 번호는 서로 대신할 수 없다.
같은 6월 자료의 `vworldkr__parcel-202606`과 `vworldkr__parcel:202606`을 두 시점으로
취급하지 않는다.

## 결정

- 필수 `--source-snapshots-path`는 역할별 `table`, `snapshot_id`를 담는다.
  `boundaries_from`, `boundaries_to`, `codes`, `history`가 기본이며, 선택적 건물 입력을
  쓰면 `buildings_from`, `buildings_to`를 함께 요구한다. 표는 `namespace.table` 형식이다.
  기존 경계를 보존하면서 별도 검증 표의 다음 시점을 읽을 수 있다.
- [ADR-0130](./0130-panel-input-snapshots-are-complete-and-bound-before-spark.md)의
  공통 JSON 해석·크기 제한·중복 키 거부·판 번호 검사·Iceberg 고정 조회를 재사용한다.
  이 작업의 역할 해석만 `parcel_lineage_inputs.py`가 맡는다.
- 실제 조회는 고정 DataFrame의 임시 뷰만 사용한다. 법정동 코드의 기본 기준일 선택과
  두 번 읽는 토지이동 이력도 같은 고정 판을 사용한다. 없는 판을 현재 판으로 대체하지 않는다.
- 선택적 소유자 파일은 한 번 읽은 바이트에서 내용과 SHA-256을 함께 만든다.
  모든 물리적 표·판, 논리적 원본 번호, 날짜 구간, 정렬된 지역 범위, 실제 코드 기준일,
  소유자 내용 해시, 기존 규칙 버전을 하나의 provenance로 묶는다.
  이 문서가 요약과 `derivation_run_id`의 유일한 입력이다.
- 달력에 없는 날짜와 역전된 구간을 Spark 전에 거부한다. 명시적으로 나중 날짜의
  코드 자료를 쓰더라도 실제 관측 날짜를 기록하며 과거 날짜로 바꾸지 않는다.
- 연결 규칙·근거 등급은 기존 `parcel_lineage.py`, 중복 append는 기존
  `append_batch_once`가 계속 소유한다. 새 연결 엔진이나 별도 실행 원장을 만들지 않는다.

## 재사용과 기각한 대안

이미 채택된 Apache-2.0 Iceberg의 [snapshot-id 읽기](https://iceberg.apache.org/docs/1.11.0/spark-queries/#time-travel-queries-with-dataframe)를
쓴다. 별도 데이터베이스나 시간여행 구현은 필요하지 않다. 현재 표를 읽고 요약에만 판을
기록하는 방식은 실제 근거를 보장하지 못한다. 모든 원천을 복사해 고정하는 방식은
기존 Iceberg 기능보다 저장·읽기 비용을 늘린다. 도메인 입력 역할과 provenance 결합만
얇은 어댑터로 남긴다.

## 검증과 경계

달라진 날짜 구간·소유자 바이트가 기존 실행 번호와 충돌하는 실패를 먼저 재현한다.
누락·추가·중복 역할과 잘못된 표·판을 검사하며, 분리된 과거·다음 표의 현재 판을 바꿔도
고정된 자료를 읽는지 실제 Spark/Iceberg에서 검증한다. 같은 입력의 재실행은 append를
반복하지 않아야 한다.

이 변경은 9월 Bronze의 원장 복구나 전국 Silver 승격을 수행하지 않는다. 원본 ZIP의
존재만으로 수집 원장·기준 시점·전국 범위가 인증되지는 않는다. 두 시점의 현재 행을
정본에 단순 append하거나 일부 파티션만 덮어쓰는 것도 허용 근거가 아니다.
진행 상태는 [운영 준비 작업 목록](../roadmap/production-readiness.md)에서 관리한다.
