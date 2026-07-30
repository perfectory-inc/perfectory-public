---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-30
---

# ADR 0015: 키를 가진 Catalog mutation은 원장 하나를 쓴다

- Status: Accepted
- Date: 2026-07-30
- 관련: [ADR-0014 serving generation은 한 단위의 소스 선택만 추적한다](./0014-serving-generation-tracks-one-unit-source-selection.md), [ADR-0013 릴리스 유일성은 두 소스 종류를 함께 허용한다](./0013-release-uniqueness-admits-both-source-kinds.md), [FP-ADR-0004 정적 벡터 타일 런타임 계약](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)
- 마이그레이션: `20260730000004_catalog_mutation_idempotency_ledger.sql`

## Context

v2 공개 명령 다섯 개가 모두 `idempotency_key`를 들고 다니는데, **어느 컬럼에도 닿지 않는다.**
이중 발행을 막는 것은 우연이다 — 재시도의 `expected_active_release_id`가 낡아서 CAS가 거부한다.
안전하지만 멱등이 아니다. 이미 성공한 요청에 에러를 돌려주고, 호출자는 그것을 "실패한 요청"과
구분할 수 없다.

재시도를 알아보는 방식은 이 스키마에 **이미 세 갈래**로 적혀 있었다.

| 위치 | 형태 | 실제 사용 |
| --- | --- | --- |
| `catalog.collection_job` | `idempotency_key` + `request_fingerprint_sha256` + `request_fingerprint_schema_version` + `success jsonb` | 사용 중. 단 키에 UNIQUE가 없고 `job_id` PK로 dedup — gongzzang ADR-0047 §218과 어긋난다 |
| `catalog.vector_tile_build_job` | `UNIQUE (publication_unit_id, idempotency_key)` | **쓰는 코드 없음** |
| `catalog.vector_tile_refresh_observation` | `idempotency_key UNIQUE` | 관측 테이블 |

같은 지식이 세 곳에 복제되어 있고, 정작 필요한 활성화 경로에는 없다.

## Decision

### 1. `catalog.catalog_mutation_idempotency` 하나를 둔다

컬럼 어휘는 `collection_job`을 **그대로 베낀다** — 같은 이름, 같은 CHECK 형태, 그리고 같은
`*_schema_version` 개념. 네 번째 방언을 만들지 않는다. `request_fingerprint_schema_version`은
지문 알고리즘을 나중에 바꿔도 기존 키가 거짓 불일치를 내지 않게 하는 유일한 장치다.

키는 PK다. `collection_job`의 결함이 정확히 "키에 유일성이 없다"였다.

### 2. 원장 쓰기가 트랜잭션의 **첫 문장**이다

lock 순서가 `원장 → pointer → publication unit → release`로 바뀐다. pointer 락은 *테이블* 락이라
모든 단위의 활성화를 직렬화하므로, 원장을 그 뒤에 두면 재시도가 "할 일 없음"을 알아내려고 무관한
활성화 뒤에 줄을 선다. 그리고 API 풀은 커넥션마다 `statement_timeout = 2500ms`를 걸어 두므로 그
대기는 재시도 가능한 답이 아니라 **상세가 지워진 500**으로 끝난다.

교착은 불가능하다. 어떤 트랜잭션도 테이블 락보다 먼저 자기 원장 행을 얻으므로, pointer 락을 든
트랜잭션이 원장 행을 기다리는 상태가 존재할 수 없다.

### 3. 청구는 `ON CONFLICT DO NOTHING RETURNING`이다

맨 `INSERT`의 `23505`는 **트랜잭션을 중단시킨다.** Postgres에 재개 가능한 에러가 없으므로 재시도
경로가 두 번째 트랜잭션을 열어야 하고, 그러면 "변경·outbox 이벤트·원장 행이 한 트랜잭션"이라는
이 기능의 존재 이유가 사라진다. `DO NOTHING`은 충돌 트랜잭션을 기다린 뒤 0행을 돌려주고 이
트랜잭션을 살려 둔다. 선례: `foundation-normalization-infrastructure`의 `proposal.rs`.

`begin()` 이전의 사전 `SELECT`는 금지다 — 두 호출자가 모두 "없음"을 보고 경쟁한다.

### 4. 대기는 `SET LOCAL lock_timeout`으로 묶고 전용 에러로 답한다

`55P03`/`57014`를 `CatalogError::MutationContended`로 매핑해 409를 낸다. 근거는 추론이 아니라
테스트다 — `a_key_held_by_an_uncommitted_transaction_answers_contended`가 커밋되지 않은
트랜잭션이 키를 쥔 상태를 만들어 이 경로를 실제로 통과시킨다.

### 5. 결과는 **참조**로 저장한다

매니페스트를 다시 만드는 모든 행이 불변이고 `load_vector_tile_runtime_manifest_by_id`가 이미
재현한다. 응답 본문을 저장하면 와이어 형태의 정의가 둘이 되고, 그 둘은 어긋날 수 있다 —
`20260730000001`이 문서와 구현이 어긋난 채 오래 살아남은 것과 같은 구조다.

`outcome_manifest_id`의 FK는 **`DEFERRABLE INITIALLY DEFERRED`**다. 이 저장소 마이그레이션에
선례가 없으며, 그것이 이 제약의 요점이다: 청구는 첫 문장이어야 하고 매니페스트 id는 pointer를
잠근 뒤에야 쓸 수 있다(`manifest_generation`이 `max + 1`이고 게이트가 비증가를 거부한다). 지연
FK는 "두 행이 커밋 시점에 일치해야 하되 쓰기 순서는 자유"에 정확히 맞는 도구다.

### 6. 실패는 기록하지 않는다

실패한 변경은 자기 원장 행과 함께 롤백되고 재시도가 다시 실행된다. Postgres에 autonomous
transaction이 없으므로 실패를 남기려면 두 번째 커넥션과 두 번째 커밋이 필요하고, 그것이 곧 원자성의
포기다. 부수 효과로 Stripe가 문서화한 성질을 공짜로 얻는다 — 키가 **오염될 수 없다.**

따름정리를 명시한다: 재시도해서 에러를 받은 클라이언트는 첫 시도가 같은 이유로 실패했는지 알 수
없다. 상태를 읽어야 한다. 운영자용 거절 이력이 필요해지면 그것은 감사 테이블이지 이 원장이 아니다.

### 7. 지문의 범위

포함: `command_kind`, `unit_key`, `expected_*` 쌍, `data_revision`, snapshot, projection revision,
`martin_source_id`, URL 템플릿, 전체 layer 집합, lineage, `operator_staff_id`.
제외: `idempotency_key` 자신(키를 해싱하면 지문이 항상 유일해져 불일치 검사가 죽는다).

`operator_staff_id`를 넣는 것은 직관에 반한다 — 동료가 같은 본문을 다시 보내면 거절된다. 그러나
활성화는 첫 운영자 이름으로 기록되므로, 그 결과를 두 번째 운영자에게 재생하면 **그가 일으키지 않은
성공**을 돌려주면서 원장에는 다른 사람이 행위자로 남는다. 거절은 눈에 보이고, 감사 위조는 아니다.

인코딩은 `serde`가 아니라 손으로 쓴 길이 접두 방식이다. 두 가지 실패 모드 때문이다. derive는 필드
추가·재배열에 **조용히** 바뀌어 기존 키 전부를 거짓 불일치로 만든다 — 명시적 목록은 그때 컴파일을
깨서 사람이 "이건 신원인가"를 결정하게 만든다. 그리고 `serde_json::Map`이 `BTreeMap`인 것은
`preserve_order` 기능이 꺼져 있는 동안뿐이고, Cargo는 기능을 그래프 전역에서 통합한다.

부수적으로 실증됐다: 이 지문이 **테스트 픽스처의 결함을 먼저 잡았다.** 픽스처가 호출마다 새
랜덤 projection revision과 file asset id를 만들고 있어서, "동일한 재시도" 테스트가 키 재사용으로
거절됐다. 지문이 옳았다.

### 8. 반환에 재시도 여부를 담는다

`PublishedRuntimeManifest::{Published, Replayed}`. 재시도는 **첫** 매니페스트를 돌려주고 그
`manifest_generation`은 현재 pointer보다 낮을 수 있는데, FP-ADR-0004는 클라이언트가 generation을
비교해 무엇을 다시 받을지 정하게 한다. 구분하지 못하는 호출자는 낡은 세대를 넘긴다.
`bool`이 아니라 enum인 이유는 이 저장소에 반례가 있기 때문이다 — `PgNormalizationUnitOfWork`가
`created` 플래그를 돌려주는데 라우트가 버려서 dedup된 제출이 신규로 보고된다.

## 기각한 대안

### `status`/`INPROGRESS` 컬럼과 리스를 둔다 (Powertools·Shopify 방식)

pointer를 옮기는 명령들은 `SHARE ROW EXCLUSIVE`에서 직렬화되고 나머지는 이 테이블의 PK에서
직렬화되므로, 중복 요청은 실제 락에서 대기한 뒤 **커밋되었거나 없는** 행을 읽는다. "누가 실행
중"이라는 상태를 관찰할 창이 없다. 상태 컬럼은 Postgres가 이미 쥔 락의 약한 복사본이고, 죽은
백엔드의 트랜잭션을 Postgres가 중단시키는 것이 그 설계들이 필요로 하는 만료에 해당한다.

### 응답 본문을 저장한다 (Stripe 방식)

Stripe는 상태 코드와 본문을 그대로 저장한다. 여기서는 재현 재료가 전부 불변이고 리더가 이미
있으므로, 저장은 두 번째 정의를 만드는 것이다. **대가는 명시한다** — 재현은 *오늘의*
`VectorTileRuntimeManifest::validate`에 묶인다. 특히 `refresh_after_seconds`는 저장되지 않고 리더가
`4`를 상수로 쓴다. 그 상수를 바꾸면 재생된 응답이 조용히 달라진다. `남은 부채` 1번.

### `vector_tile_build_job.idempotency_key`와 그 UNIQUE를 없앤다

처음의 판단이었고 뒤집었다. 넷 다 반대 방향을 가리킨다. (a) 그 테이블의 `id`는 서버가 만들고 자연
키가 없어서, 나중에 TTL이 생기면 중복 빌드를 막는 유일한 장치가 그 제약이다. (b) ADR-0013이
`DROP CONSTRAINT` 선례를 "유일성 키 **교체**에 한정한다"고 명시했는데 이것은 *제거*다. (c) 구현
안내서 Task 6 Step 3이 `[x]`로 확정한 계획이 그 테이블을 요구한다. (d) 아직 쓰이지 않는 컬럼은
비용이 없다. 원장은 `키 → 결과` 조회를 소유하고, 그 제약은 단위별 백스톱으로 남는다 — 게이트가
도메인 상태기계를 반복하는 것과 같은 관계다.

### 다섯 명령 전부를 원장으로 덮는다

`record_vector_tile_build_result`에는 `idempotency_key`가 **없다.** 요청 신원이 이미
`(build_job_id, outcome)`이므로 키는 같은 사실의 두 번째 표기다 — `collection_job`이 걸린 병이다.
면제로 선언하고, 그 명령의 전이 규칙(`status = 'running'`을 대상으로 하는 UPDATE가 0행이면 이미
종결, 그때 outcome을 비교해 같으면 재생·다르면 충돌)은 그 명령을 쓸 때 정의한다.

### 보존 기간(TTL)을 이번에 넣는다

이 플랫폼에는 어떤 `catalog.*` 테이블도 정리하는 프로덕션 코드가 없다. 발행된 outbox 행조차
지우지 않는다. 클라이언트가 아직 없는(§남은 부채 2) 테이블에 스위퍼를 다는 것은 공상이다.
`created_at`은 지금 넣어 두었으므로 나중에 데이터 마이그레이션 없이 인덱스 조건이 된다. 재검토
방아쇠와, 키가 지워졌을 때 명령별로 무엇이 허용되는지는 `남은 부채` 3번에 적는다.

## Consequences

- 재시도가 첫 결과를 돌려준다. `an_identical_retry_is_answered_from_the_ledger_and_writes_nothing`이
  직렬화한 응답이 **바이트 단위로 같음**을 단정하고, 새 행·새 이벤트가 없음을 함께 단정한다.
- 키 재사용이 전용 에러로 거절된다. 호출자의 조치(새 키 발급)가 "본문 그대로 재전송"·"상태를 다시
  읽고 재시도"와 구분된다.
- 경합이 재시도 가능한 409로 답한다. 이전에는 상세가 지워진 500이었다.
- 원장 행이 자기 outbox 이벤트 id를 기록하므로 "변경·이벤트·원장이 한 트랜잭션"이 **단정 가능**해졌다.
  발행 capability가 꺼져 있으면 null이고, 나중에 켜도 재생은 소급 공지하지 않는다.
- `DEFERRABLE`이 이 저장소의 첫 선례가 되었다. 쓰기 순서 제약이 있는 트랜잭션 내 상호 참조에 한정한다.

## 남은 부채

1. **재현은 `validate()`에 묶여 있다.** `VectorTileRuntimeManifest::validate`를 조이는 변경은 저장된
   원장 행에 대해 **파괴적 변경**이다 — 재생이 원래 성공했던 응답 대신 에러를 낼 수 있다. 특히
   `refresh_after_seconds`는 저장되지 않고 리더가 상수로 쓴다. 조이는 변경에는 기존 매니페스트에
   대한 결정이 함께 와야 한다.
2. **키를 발급하는 클라이언트가 아직 없다.** 다섯 use case 어느 것도 서비스에서 호출되지 않고,
   `foundation-api`에는 `Idempotency-Key` 처리가 없다. 존재하는 유일한 발급 규칙은 테스트 픽스처의
   내용 파생 키다. 내용 파생을 유지한다면 지문은 파생에 **포함되지 않은** 필드만 지키게 되므로,
   "다른 요청에 재사용된 키를 거부한다"는 프로덕션에서 거의 도달 불가이며 관측이 아니라 시험된
   성질이다. `x-request-id`는 시도마다 바뀌므로 **키가 아니다.**
3. **명령별 키 삭제 영향.** 키가 사라지면: `mark_tile_layer_dynamic`은 release 유일성 키에 걸려
   안전(단 에러가 혼란스럽다), `promote_tile_layer_static`은 호출자가 선할당한 `release_id` PK에 걸려
   안전, `rollback_tile_layer_source`는 CAS에 걸려 안전, **`start_vector_tile_build`는 두 번째 빌드를
   연다.** TTL 설계의 구속 조건이다.
4. **커버되지 않는 경로 둘.** `catalog.promote_vector_tile_runtime_manifest`를 직접 부르는 포트
   메서드와, `foundation-outbox-publisher`의 raw SQL 운영 명령이다. 후자는 unit of work도, 키도,
   outbox 이벤트도 거치지 않으며 이어받은 단위에 `serving_generation + 1`을 쓰므로
   **[ADR-0014](./0014-serving-generation-tracks-one-unit-source-selection.md)의 게이트가 거부한다** —
   publication unit이 둘이 되는 순간 깨진다. 별도 변경이다.
5. **`catalog.collection_job.idempotency_key`는 이 원장으로 수렴하지 않았다.** 다른 영역(수집)의
   작업 큐이고 자체 상태기계를 가진다. "원장 하나"는 키를 가진 **공개(publication)** 명령에 한정한다.
