---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-17
---

# ADR 0035: 쓰지 않는 지역은 필수가 아니다

- Status: Accepted
- Date: 2026-08-17
- 관련: [ADR-0033 주소 출처가 없는 산업단지는 표현할 수 없다](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md), [ADR-0034 행정구역 코드는 자기 정밀도를 싣고 다닌다](./0034-an-administrative-code-carries-its-own-granularity.md), [ADR-0020 도형은 사실의 근거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md), [ADR-0027 모든 가드는 자기 위협 모델을 선언한다](./0027-every-guard-declares-its-threat-model.md)
- 일부 대체: [ADR-0033](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md) 결정 2항의 파생 규칙, [ADR-0034](./0034-an-administrative-code-carries-its-own-granularity.md) 결정 4항의 "계약은 바꾸지 않는다"

## Context

[ADR-0034](./0034-an-administrative-code-carries-its-own-granularity.md)가 만든 주소 해소기는
실물 1,442건 중 1,441건을 풀었다. 남은 `446400 지도농공단지`는 `addr_cd`가 `1287000000`
(폐지된 광주·전남 통합코드)이고, 그 단지 고시가 0건이라 최빈값 규칙도 학습 표도 답을 주지 못한다.
상세 엔드포인트의 `addr_si_cd`·`addr_gu_cd`도 `null`이다. 남은 경로는 이름 매칭과 도형 추론뿐이고
둘 다 금지다([ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md)).

그 1건 때문에 `export-industrial-complex-bronze-raw-jsonl`은 실물 전수에 대해 아무것도 쓰지
못했다. [ADR-0033](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md)
결정 3항이 설계대로 작동한 것이다.

```
1 of 1442 industrial-complex rows have no sourced address
  (official_complex_code 446400); nothing was written
```

ADR-0034는 이 상태를 풀 방법을 (a) 읍면동 단위 출처 확보 또는 (b) 컬럼을 nullable로 바꾸기로
적어 두고 "둘 다 별도 결정"이라고 남겼다. 이것이 그 결정이다.

**소유자가 지역별 산업단지 기능을 지금 하지 않기로 했다.** 즉 `silver.industrial_complexes`와
`gold.complex_catalog`의 지역 컬럼은 **오늘 아무 소비자도 읽지 않는다.** 그런데 계약은
`sido_code`·`sigungu_code`를 `required: true`로 두고 `sido_code`를 파티션 키로 삼고 있었다.
쓰지 않는 값을 필수로 요구하는 계약은 두 결과 중 하나만 낳는다. 없는 값을 지어내거나, 전체가
멈추거나. 지금은 후자다.

파티션 키는 별개의 결함이다. 파티션 키는 행마다 경로나 매니페스트에 적히므로 `null`일 수 없다.
따라서 "`sido_code`는 선택 항목인데 `sido_code`로 파티션한다"는 계약은 **자기가 합법이라고 말한
행을 자기가 쓸 수 없는 계약**이다. 이 모순은 컬럼을 내리는 순간 생기는 것이 아니라, 컬럼을
내리려고 보니 이미 그 모양이었다.

## Decision

1. **`silver.industrial_complexes`와 `gold.complex_catalog`에서 `sido_code`·`sigungu_code`를
   `required: false`로 내린다.** `primary_bjdong_code`는 ADR-0034 이후 이미 선택 항목이고 그대로
   둔다. 정본은 Rust `lakehouse-domain`의 `LakehouseTableContract`이고
   `infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`은 그 산출물이다.
   `lakehouse_contract_artifact.rs`가 둘의 드리프트를 막는다.

2. **파티션 키를 `source_snapshot_id` 하나로 바꾼다.** 두 표 모두.
   `source_snapshot_id`는 `required: true`이고, 한 번의 적재가 정확히 한 값을 싣고, Iceberg
   기록 후 되읽기 검증이 이미 그 컬럼으로 필터한다. 즉 이 표의 자연스러운 물리 단위는 지역이
   아니라 기준월 스냅샷이다. `bucket(32, complex_id)`는 함께 뺀다 — 스냅샷당 1,442행이면 버킷당
   45행이고, 그것은 프루닝이 아니라 파일 개수다. 정렬도 지역을 뺀
   `["complex_name_normalized", "official_complex_code"]`(Gold는 `["name", "complex_id"]`)로
   바꾼다.

3. **파티션 키는 그 계약이 필수로 요구하는 컬럼이어야 한다.** 9개 계약 전부에 대해
   `no_contract_partitions_on_a_column_it_does_not_require`가 검사한다.
   *이 검사가 실패를 막는 실제 사고:* 선택 컬럼을 파티션 키로 둔 표는, 그 컬럼이 `null`인
   합법적인 행을 적재하는 순간 쓰기가 깨진다 — 계약이 허용한 행을 계약이 거부하는 상태다.

4. **Spark 잡은 파티션·정렬을 계약에서 읽는다.** 두 잡의 로컬 Parquet writer가
   `partitionBy("sido_code")`를 자기 소스에 적어 두고 있었다. 같은 목록이 두 곳에 있으면 한 곳만
   바뀐다. `partition_column_names()`/`sort_order()`가 계약에서 읽고,
   `test_industrial_complex_jobs_read_partitioning_from_the_contract`가 잡이 그 헬퍼를 쓰는지
   AST로 확인한다.

5. **`IndustrialComplexAddress`는 행정구역 코드를 `Option`으로 가진다.** 주소 글자와 출처 두
   필드는 **여전히 값으로 가진다.** 코드가 없는 주소를 만드는 방법은
   `try_new_without_administrative_code(address_text, dataset, record_id)` 하나뿐이다.
   빈 문자열을 `try_new`에 넘겨 만드는 경로는 없다 — 빈 값을 받을 수 있는 인자는 실수로 빈 값이
   들어가는 인자이고, "모른다"는 어떤 코드와도 다른 사실이기 때문이다.
   `sido_code()`·`sigungu_code()`·`primary_bjdong_code()`는 모두 `Option<&str>`이고, 행은 셋 다
   `null`로 적는다.

6. **해소 파일에서 `administrative_code`와 `administrative_code_granularity`는 함께 있거나 함께
   없다.** 한쪽만 있는 줄은 거부한다 — 코드 없는 정밀도는 아무것도 서술하지 않고, 정밀도 없는
   코드는 자기가 무엇을 가리키는지 말하지 않았다(ADR-0034 결정 1항).

7. **`resolution_tier`에 `address_text_only`를 더하고, tier와 코드의 유무가 서로를 검증한다.**
   `address_text_only`인데 코드가 실린 줄도, 코드를 만드는 tier인데 코드가 없는 줄도 거부한다.
   tier는 그 줄이 스스로 하는 주장이므로 값이 반박할 수 있어야 한다(ADR-0034 결정 5항과 같은
   형태). 코드가 없게 된 **이유**(`source_code_absent` / `source_code_unknown_and_no_notice_evidence`)는
   해소 파일이 아니라 빌드 요약의 `missing_administrative_codes`에 단지 코드와 함께 적는다.

8. **행정구역 코드의 시군구 접두 5자리가 전부 `0`이면 거부한다.** `0000000000`은 10자리 숫자이고
   끝 5자리가 `00000`이므로 ADR-0034의 두 검사를 모두 통과한다. 지역이 필수가 아니게 된 뒤
   그것은 `null` 대신 채워 넣기 가장 쉬운 모양이다. 실재하는 시군구 코드는 `00`으로 시작하지
   않는다.

9. **Silver 경계에도 같은 말을 하는 게이트를 둔다.** 계약의 **모든 문자열 컬럼**은 비어 있으면
   안 된다(필수 컬럼만 `null`을 금지한다). 그리고 지역 세 컬럼은 값이 있으면 자기 자릿수의
   숫자여야 하고 전부 `0`이면 안 된다(`invalid_region_code_count`).
   *이 검사가 실패를 막는 실제 사고:* 손으로 만든 Bronze JSONL이나 미래의 다른 생산자가
   `""`이나 `0`을 지역 컬럼에 넣어 정본 표에 올리는 일. 타입은 이 저장소의 생산자만 막는다.
   지역 세 컬럼은 이제 `trim_to_null` 대신 `trim`으로 읽는다 — 공백을 `null`로 바꾸면 이 결정이
   지키려는 구분이 바로 그 자리에서 지워진다.

### 위협 모델 ([ADR-0027](./0027-every-guard-declares-its-threat-model.md) 결정 1항)

- 부류: 결정 5~8항은 **표현 불가능(prevention)** 이고 ADR-0033·0034와 같은 급이다. 결정 3·9항은
  **탐지(detection)** 이고, 예방이 닿지 않는 곳(계약 자체의 모순, 이 저장소 밖에서 만들어진
  입력)에만 둔다.
- Prevents: 지역 코드를 `""`·`0`·`0000000000`으로 채우는 일. 생성자에 그 값을 넣을 인자가 없고,
  넣으면 자릿수·전부0·정밀도 세 검사가 차례로 거부한다.
- Prevents: 시군구 코드가 `primary_bjdong_code` 자리에 앉는 일. ADR-0034의 granularity 검사는
  코드가 있을 때 **그대로** 돈다. 이 결정은 코드를 선택으로 만들 뿐 검사를 약하게 하지 않는다.
- Prevents: 선택 컬럼을 파티션 키로 삼아 계약이 자기 행을 못 쓰게 되는 일.
- Does not prevent: 운영자가 **거짓 해소 파일을 일부러 만들어** 주입하는 일. 여전히 출처의
  문제이고 `address_source_dataset`·`address_source_record_id`·`resolution_tier`가 근거다.
- Does not prevent: `address_text`가 **사실인지**. 글자는 출처의 문장 그대로이고 대조되지 않는다.
- Does not prevent: 나중에 지역 기능을 할 때 이 1건에 지역이 생기는 일. 그건 출처의 문제다.

### 무엇이 느슨해지지 않는가

- **주소 글자는 여전히 필수다.** `IndustrialComplexAddress`는 `address_text`와 출처 두 필드를
  값으로 가지고, 공백이면 생성자가 거부한다. 코드가 없는 주소도 예외가 아니다. 실물 1,442건
  전부가 `danji_loc`을 가지고 있으므로 이것을 내릴 이유가 없다.
  (`silver.industrial_complexes` 계약의 `address_text`는 이 결정 **이전부터** `required: false`
  였고 이 결정은 그 플래그를 건드리지 않는다. 강제는 생산자 타입이 한다.)
- **한 행이라도 주소가 없으면 전체가 실패한다**(ADR-0033 결정 3항). 그대로다. 달라진 것은
  "주소가 없다"의 뜻이 "지역 코드가 없다"에서 "주소 글자가 없다"로 좁아진 것뿐이다.
- **도형으로 지역을 판정하지 않는다**(ADR-0020). 그대로다.
- **원본 라벨은 매핑에 없으면 실패한다**(ADR-0033 결정 6항). 그대로다.
- **출력은 append-only다**(ADR-0033 결정 5항). 그대로다.

## Consequences

- **1,442건 전부가 파이프라인을 통과한다.** 실측: 해소 1,442줄
  (`source_code_in_authority` 1,297 · `modal_notice_code` 138 · `learned_code_migration` 6 ·
  `address_text_only` 1), 미해소 0. Bronze JSONL 1,442행. Spark `bronze->silver` 로컬 Parquet
  1,442행, 되읽기 검증 통과. `silver->gold` 1,442행.
- `446400` 행은 `sido_code`·`sigungu_code`·`primary_bjdong_code`가 모두 `null`이고
  `address_text`는 `전라남도 신안군 지도읍 감정리 일원`이다.
- **지역 기능을 나중에 할 때 되돌리는 비용.** 컬럼을 다시 필수로 만들려면 (1) 계약 두 곳의
  플래그, (2) 파티션 키, (3) Spark 잡의 게이트가 아니라 **오직 계약**만 고치면 된다 — 파티션도
  정렬도 게이트도 계약에서 파생하므로 코드는 따라온다. 진짜 비용은 코드가 아니라 데이터다.
  그 시점에 지역이 없는 행이 이미 Silver에 있다면 그 행들의 출처를 새로 구해야 하고, 구하지
  못하면 다시 이 결정으로 돌아온다. 즉 **되돌리는 비용은 `446400`의 주소 출처를 구하는 비용과
  같다.** 파티션 키를 지역으로 되돌리는 것은 결정 3항이 막으므로, 그때는 컬럼을 필수로 만드는
  변경과 같은 변경 안에서만 가능하다.
- **ADR-0033과의 관계: 유지, 일부 대체.** 결정 1(출처 주입 필수)·3(한 건이라도 없으면 전체
  실패)·5(append-only)·6(라벨 매핑)은 그대로다. 결정 2의 "`sido_code`와 `sigungu_code`는
  `primary_bjdong_code`에서 파생한다"는 이미 ADR-0034가 `administrative_code`로 바꿨고, 이
  결정이 그 파생을 `Option`으로 만든다. 결정 2의 "`IndustrialComplexBronzeRawRow`는 이 타입을
  `Option`이 아니라 값으로 소유한다"는 **그대로 유지된다** — `Option`이 된 것은 주소가 아니라
  주소 안의 코드다. Consequences의 "계약은 그대로 둔다. 필수 컬럼도 파티션 키도 바꾸지 않는다"는
  이 결정이 대체한다.
- **ADR-0034와의 관계: 유지, 일부 대체.** 결정 1~3(정밀도를 싣는다 · 모양이 라벨을 검증한다 ·
  `primary_bjdong_code()`는 `Option`)과 5~7(해소 파일 형식 · 학습 표 · 권위 주입)은 그대로다.
  결정 4의 "`sido_code`와 `sigungu_code`는 그대로 채운다"는 코드가 있을 때만 참이 되고,
  같은 항의 "`silver.industrial_complexes` 계약은 바꾸지 않는다"는 이 결정이 대체한다. 결정 5의
  해소 파일 한 줄에 `address_text_only` tier와 두 선택 필드가 더해진다. 남은 일 (2)
  `446400`의 주소 출처는 **여전히 열려 있다** — 이 결정은 그 단지의 지역을 알아낸 것이 아니라,
  모른다고 적을 수 있게 만든 것이다.
- `silver.industrial_complex_boundaries`와 `silver.complex_parcel_memberships`는 건드리지 않는다.
  둘 다 생산자가 없고, 둘의 지역 컬럼은 도형·PNU에서 오므로 이 결정의 이유가 적용되지 않는다.
  결정 3항의 검사는 두 표에도 돌고 있고 지금 통과한다.
- 남은 일: (1) `446400`의 주소 출처. (2) 지역 기능을 실제로 할 때, 지역 컬럼을 다시 필수로 할지
  아니면 지역을 별도의 dated fact로 둘지([ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)의
  형태) 결정. (3) `catalog.industrial_complex.primary_bjdong_code`의 `NOT NULL`은 ADR-0034가
  적어 둔 대로 아직 열려 있다.
