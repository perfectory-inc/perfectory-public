# ADR 0043: 정본 id 는 다시 계산하지 않고 읽는다

- Status: Accepted
- Date: 2026-08-18
- 관련: [ADR-0036 가리켜지는 객체는 그것을 쓴 커맨드를 가진다](./0036-a-pointed-at-object-has-the-command-that-wrote-it.md), [ADR-0037 포인터는 객체 키와 함께 주소 틀을 싣는다](./0037-a-pointer-carries-the-address-template-with-its-object-key.md), [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md), [ADR-0027 모든 가드는 자기 위협 모델을 선언한다](./0027-every-guard-declares-its-threat-model.md)

## Context

`infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py` 의 `stable_uuid_v5` 는
docstring 에 "RFC 4122 version-5 UUID" 라고 적어 두고, 실제로는 SHA-1 hex 텍스트를 손으로 잘라
붙여 UUID 를 만들었다. version 니블(`5`)은 맞았고 **variant 니블 하나가 틀렸다.**

RFC 4122 4.1.1 은 octet 8 의 **상위 2비트만** `10` 으로 바꾸고 **하위 2비트는 보존**한다 —
즉 `nibble & 3`. 그 코드가 쓴 것은 `nibble >> 2` 였다.

```
RFC 4122     0,4,8,c -> 8    1,5,9,d -> 9    2,6,a,e -> a    3,7,b,f -> b
그 코드      0,1,2,3 -> 8    4,5,6,7 -> 9    8,9,a,b -> a    c,d,e,f -> b
```

2026-08-18, `r2.silver.industrial_complexes` 의 실물 값과 Python 표준 `uuid.uuid5` 의 값을
대조했다. seed 는 `foundation-platform:catalog:industrial_complex:{official_complex_code}`,
namespace 는 `NAMESPACE_URL` 이다.

```
111010   uuid.uuid5   7df3859c-768f-51fa-a78d-6398acd5f052
         실물         7df3859c-768f-51fa-978d-6398acd5f052
111011   uuid.uuid5   c00ffadd-4196-5a0b-a8a1-c203cf78b434
         실물         c00ffadd-4196-5a0b-98a1-c203cf78b434
445010   uuid.uuid5   0072ba44-d5c2-50e8-829c-87741ad04f81
         실물         0072ba44-d5c2-50e8-929c-87741ad04f81
```

앞뒤가 전부 같고 variant 니블 하나만 다르다. seed 와 namespace 와 SHA-1 은 맞았다는 증거이자,
variant 처리만 틀렸다는 증거다.

**이 결함의 실제 비용은 값이 아니라 조인이다.** 같은 날 경계 작업이 `complex_id` 를 Rust
`Uuid::new_v5` 로 **다시 계산**하려 했다. 그대로 갔으면 경계 1,344건이 조용히 하나도 안 붙는다.
값은 그럴듯하고, 두 규칙의 variant 니블은 **둘 다 `[89ab]` 안에 떨어지므로** 이 job 의
`complex_id must be a lowercase UUID string` 게이트도 통과하고, 조인 결과만 0 이 된다.
형태가 맞아서 형태 검사가 못 잡는 부류다.

검사가 이것을 못 잡은 두 번째 이유는 더 단순하다. **아무도 이 유도를 실행해 볼 수 없었다.**
이 job 은 module 최상단에서 PySpark 를 import 했고, CI 의 Python 레인에는 PySpark 가 없다. 즉
`stable_uuid_v5` 는 Spark 밖에서 호출할 방법이 없는 함수였고, 실행할 수 없는 규칙은 검사되지
않는 규칙이다.

지금 고치는 것이 가장 싸다. `catalog.industrial_complex_gold_pointer` 는 0행이므로 바깥에
약속한 주소가 아직 없고(2026-08-18 운영자 실측), Bronze 재생성이 어차피 예정되어 있으며,
이 값을 읽는 클라이언트 코드가 아직 없다.

## Decision

1. **`complex_id` 의 유도는 표준 라이브러리가 소유한다.** `stable_uuid_v5_string` 의 본문은
   `uuid.uuid5(uuid.NAMESPACE_URL, seed)` 한 줄이다. SHA-1 hex 를 잘라 붙이는 조립도, variant
   니블 매핑표도 남기지 않는다. 매핑을 어떤 형태로든 다시 적으면 그 사본이 정본과 어긋날 수
   있고, 이 ADR 이 다루는 사고가 정확히 그것이다.

2. **seed 는 이름이 붙은 상수다.** `COMPLEX_ID_SEED_PREFIX =
   "foundation-platform:catalog:industrial_complex:"` 이고 `complex_id_seed()` 가 그것을
   `official_complex_code` 에 붙인다. seed 는 identity 의 일부이지 형식이 아니다 — 바꾸면
   하위의 모든 정본 주소가 같이 바뀐다.

3. **하위 생산자는 `complex_id` 를 다시 계산하지 않는다. `silver.industrial_complexes` 에서
   읽는다.** 이것이 이 ADR 의 진짜 결정이다. 경계·필지 소속·Gold 투영·프로필·포인터 어느 것도
   자기 UUIDv5 를 만들지 않는다. 준수하는 라이브러리로 정확히 계산해도 seed 나 namespace 가
   한 글자 다르면 결과는 조인 0 이고, 그 실패는 어떤 형태 검사에도 걸리지 않는다.
   `gold.complex_catalog` 은 이미 이 규칙을 지킨다(`transform: identity`).

4. **값은 우리가 만들지 않은 벡터에 고정한다.**
   `infra/lakehouse/spark/tests/test_industrial_complex_bronze_to_silver.py` 의 기대값은 전부
   CPython `uuid.uuid5` 가 낸 것을 받아 적은 리터럴이다. 우리 코드로 기대값을 만들면 그 검사는
   정의상 통과하고 아무것도 못 잡는다. 벡터는 variant 니블 **네 갈래(`8`/`9`/`a`/`b`)를 전부**
   덮으며, 덮지 않게 되면 `VariantNibbleCoverageTest` 가 실패한다 — 옛 규칙은 전체 digest 의
   1/4 에 대해 **우연히 정답을 냈으므로**(벡터 `000002` 가 그 경우다) 벡터 하나로는 못 잡는다.

5. **이 job module 은 PySpark 없이 import 가능해야 한다.** `load_pyspark()` 가 PySpark 를
   `main()` 안으로 미룬다(`silver_scalar_handoff_to_lakehouse` 가 이미 쓰는 방식). 이 규칙이
   깨지면 위 테스트 파일이 import 단계에서 실패하므로, 별도의 가드가 아니라 테스트 자신이
   경계다.

6. **값이 바뀐다는 것을 결정에 포함한다.** 1,442건의 `complex_id` 가 전부 새 값이 된다. 옛 값과
   새 값이 섞이지 않도록 Bronze→Silver→Gold→프로필을 이어서 다시 만든다. 부분 재실행으로 두
   세대를 한 표에 섞지 않는다.

### 위협 모델 ([ADR-0027](./0027-every-guard-declares-its-threat-model.md) 결정 1항)

- 부류: 결정 1 은 **표현 불가능(prevention)** — 손으로 만질 비트가 남아 있지 않다.
  결정 4·5 는 **탐지(detection)** 다.
- *이 검사가 막는 실제 사고:* 하위 생산자가 `complex_id` 를 준수하는 UUIDv5 라이브러리로 스스로
  유도해 정본과 다른 값을 얻고, 모든 형태 검사가 초록인 채로 조인이 0행이 되는 일.
- Prevents: variant/version 비트를 손으로 다시 조립하는 일(결정 1).
- Prevents: seed 접두사가 조용히 바뀌는 일 — 테스트가 접두사 문자열 자체를 고정한다.
- Does not prevent: **새 표**가 자기만의 seed 로 자기 id 를 만드는 일. 결정 3 은 규칙이고
  기계 검사가 아니다. 오늘 `complex_id` 를 다시 계산하는 코드는 저장소에 없다(전수 확인:
  `stable_uuid_v5`·`new_v5`·`uuid5`·`NAMESPACE_URL` 로 `*.py`·`*.rs`·`*.sql`·`*.ts`·`*.sh` 전체).
- Does not prevent: 이미 R2 에 쓰인 옛 값. 그것은 재실행이 지운다(결정 6).
- Does not prevent: `official_complex_code` 자체가 틀린 경우. 이 결정은 코드에서 id 로 가는
  함수만 고정한다.

## Consequences

- **1,442건의 `complex_id` 가 전부 바뀐다.** `gold.complex_catalog` 은 Silver 의 투영이므로
  같이 바뀌고, Gold 프로필의 `artifact_id` 는 `UUIDv5(namespace, "{gold_snapshot_id}:{complex_id}")`
  이므로([ADR-0036](./0036-a-pointed-at-object-has-the-command-that-wrote-it.md) 5항)
  **프로필 객체 키가 전부 새로 생긴다.** 옛 객체는 append-only 원칙대로 남지만 가리켜지지 않는다.
- **바깥에 깨지는 주소는 없다.** `catalog.industrial_complex_gold_pointer` 가 0행이므로
  발행된 포인터가 없다(2026-08-18 운영자 실측). 이 값을 읽는 클라이언트 코드도 아직 없다.
- **Postgres canonical 표는 영향을 받지 않는다.** `industrial_complex_canonical_load` 는
  `official_complex_code` 자연키로 upsert 하고 Gold 행의 `complex_id` 를 읽지 않는다
  ([ADR-0040](./0040-a-column-no-producer-fills-cannot-be-required.md) 4항). 9개 외래 키가
  가리키는 `catalog.industrial_complex.id` 는 그대로다.
- **Spark 가 Python UDF 를 하나 부른다.** `stable_uuid_v5` 는 이제 네이티브 식이 아니라
  `stable_uuid_v5_string` 을 감싼 UDF 다. 대상은 1,442행이고 이 job 은 단일 컨테이너에서 돌므로
  비용은 무시할 수 있다. 함수 본문이 `spark-submit` 이 `__main__` 으로 읽는 이 module 안에 있어,
  Python 워커는 표준 라이브러리 외에 아무것도 필요로 하지 않는다.
- **문서가 틀린 것을 함께 고쳤다.** `platforms/foundation-platform/docs/catalog/industrial-complex-lakehouse-poc.md`
  의 §3 ID 표는 seed 를 `industrial_complex:{official_complex_code}` 로 적고 있었다. 실제 seed 와
  다르고, 그 seed 로 계산하면 전혀 다른 값이 나온다(`111010` -> `672f7248-...`). namespace 와
  version 도 적혀 있지 않았다.
- 남은 일: (1) Bronze→Silver→Gold→프로필 재실행은 운영자가 수행한다. (2) PoC 문서의 경계·필지
  소속 seed 는 아직 구현체가 없다 — 만들 때 결정 1~3 을 그대로 따른다.
