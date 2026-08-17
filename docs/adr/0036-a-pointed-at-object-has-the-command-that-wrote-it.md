# ADR 0036: 가리켜지는 객체는 그것을 쓴 커맨드를 가진다

- Status: Accepted
- Date: 2026-08-18

## Context

`catalog.industrial_complex_gold_pointer` 는 산업단지 하나마다 Gold 프로필 객체 하나를 가리킨다.
키 형태(`gold/industrial-complex/profiles/{artifact_id}.json`)는 FP-ADR-0005 가 정했고,
포인터를 발행하는 커맨드(`publish-industrial-complex-gold-pointer`)도, 그 응답을 읽는 API 도,
런북도 이미 있었다.

**그 객체를 쓰는 코드만 없었다.** 저장소 전체에서 그 키를 쓰는 생산자는 검색되지 않는다.
따라서 포인터를 발행하면 존재하지 않는 객체를 가리키게 되고, 체크섬·행수·크기는 사람이 손으로
채워 넣어야 하는 값이 된다 — 즉 지어낸 값이 된다.

이것은 이 저장소에서 반복된 결함 부류다: 소비자는 있고 생산자가 없다.

실측:

```
r2.gold.complex_catalog   현재 스냅샷 1859666716850025456   1,442행   데이터 파일 1개   delete 파일 0개
```

## Decision

1. `export-industrial-complex-gold-profiles` 커맨드가
   `gold/industrial-complex/profiles/{artifact_id}.json` 의 **유일한 생산자**다.
   `foundation-outbox-publisher` 의 서브커맨드이며 Rust 로 구현한다.

2. 입력은 **Iceberg 카탈로그**다. 로컬 파일이 아니다. Iceberg REST 카탈로그에서
   `gold.complex_catalog` 의 현재 스냅샷과 manifest list 를 얻고, manifest list → manifest →
   Parquet 데이터 파일 순서로 객체 저장소를 읽는다. delete manifest 나 delete 파일이 하나라도
   보이면 행을 읽지 않고 실패한다 — 그 스냅샷의 유효 행 집합은 이 스캐너가 계산할 수 없다.

3. 문서의 컬럼 집합은 산문이 아니라 `lakehouse_domain::GOLD_COMPLEX_CATALOG` 에서 온다.
   데이터 파일의 컬럼 집합이 그 계약과 정확히 같지 않으면 실패한다. 계약이 `required` 라고 한
   컬럼이 null 이면 실패한다.

4. **프로필 문서는 단지당 1건**이다. 포인터 표의 기본키가 `complex_id` 이고, 포인터가 요구하는
   `profile_row_count` 는 그 문서가 서술하는 산업단지 수이므로 언제나 1 이다.

5. **artifact_id 는 입력의 함수**다: `UUIDv5(namespace, "{gold_snapshot_id}:{complex_id}")`,
   namespace 는 문서 schema_version 으로부터 파생한다. 문서 본문에는 벽시계 시각도 실행 id 도
   싣지 않는다. 따라서 같은 Gold 스냅샷을 다시 export 하면 같은 키에 **바이트가 같은** 객체가
   나온다.

6. 쓰기는 **create-only** 다. 같은 키가 이미 있으면 저장된 바이트를 읽어 비교한다. 같으면 재실행
   으로 간주하고(`reused`), 다르면 실패한다. 덮어쓰지 않는다.
   막는 사고: 부분 실패 후 재실행이 이미 발행된 산출물을 조용히 다른 내용으로 바꾸는 것.

7. 커맨드는 **포인터를 발행하지 않는다.** 요약 JSON 이 `publish-industrial-complex-gold-pointer`
   의 입력(`current_version`, `profile_object_key`, `profile_size_bytes`,
   `profile_checksum_sha256`, `profile_row_count`, `source_snapshot_id`,
   `iceberg_snapshot_id`, `published_at_utc`)을 그 이름 그대로 싣는다.

8. 포인터의 `iceberg_snapshot_id` 는 **실제로 스캔한 Gold 스냅샷 id** 다.
   Gold 표에도 `iceberg_snapshot_id` 라는 컬럼이 있으나 그것은 Spark 잡에 넘긴 인자(Silver 쪽
   스냅샷)이고 Gold 표 자신의 스냅샷이 아니다. 그 컬럼 값은 프로필 문서의 `attributes` 에
   원문 그대로 실린다.

9. **없는 값은 지어내지 않는다.** `parcel_count` 는 지금 모든 행에서 0 이고
   `calculated_area_sqm` 는 모든 행에서 null 이다. 문서는 이 값을 그대로 싣는다.
   **`parcel_count` 의 0 은 "필지가 없다"는 사실이 아니라 자리표시다** — 필지 소속을 계산하지
   않기로 했기 때문에 Spark 잡이 상수 0 을 넣는다(`industrial_complex_silver_to_gold.py` 의
   `literal_zero` 계보). 소비자는 이 값을 필지 수로 읽어서는 안 된다. 요약 JSON 이
   `placeholder_parcel_count_row_count` 와 `null_calculated_area_row_count` 로 그 규모를 보고한다.

## Consequences

- Gold 프로필 객체가 처음으로 실재하게 되고, 포인터 발행이 지어낸 값 없이 가능해진다.
- 저장소에 처음으로 **Rust 쪽 Iceberg 데이터 읽기 경로**가 생긴다. `apache-avro` 로 manifest 를,
  `parquet`(zstd 포함) 으로 데이터 파일을 읽는다. zstd 는 Gold 표가 실제로 쓰는 압축이며,
  없으면 첫 레코드 배치에서 실패한다.
- 스캐너는 delete 파일을 처리하지 않는다. Gold 표가 merge-on-read 로 바뀌면 이 커맨드는 조용히
  틀린 답을 내지 않고 **실패**한다. 그때 스캐너를 확장하는 것은 별도 결정이다.
- Gold 스냅샷이 바뀌면 내용이 그대로인 단지도 새 artifact_id 를 받는다. 발행 단위가 스냅샷이므로
  의도된 동작이다.
- 프로필 문서에는 주소가 실리지 않는다. 주소는 ADR-0037 이 포인터 쪽에 둔다.
