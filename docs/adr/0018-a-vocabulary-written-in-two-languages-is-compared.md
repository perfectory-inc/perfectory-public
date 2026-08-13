---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-06
---

# ADR 0018: 두 언어가 같은 어휘를 적으면 대조한다

- Status: Accepted
- Date: 2026-08-06
- 관련: [ADR-0013 릴리스 유일성은 두 소스 종류를 함께 허용한다](./0013-release-uniqueness-admits-both-source-kinds.md), [ADR-0015 키를 가진 Catalog mutation은 원장 하나를 쓴다](./0015-one-idempotency-ledger-for-keyed-catalog-mutations.md), [ADR-0016 PostGIS 적재는 신원을 가진 하나의 사실이다](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)
- 마이그레이션: 없음 (스키마 변경 없음)

## Context

`catalog_domain`에는 데이터베이스가 CHECK로 제약하는 어휘를 Rust로 다시 적는 enum이 셋 있다.

| Rust | SQL |
|---|---|
| `CatalogMutationKind` | `catalog_mutation_idempotency_command_kind_check` |
| `VectorTileBuildStatus` | `vector_tile_build_job_status_check` |
| `ServingSourceKind` | `vector_tile_release_source_kind_check` |

셋 다 **자기가 거울이라는 것을 문서 주석으로 적고 있었다** — "Mirrors ... exactly". 마이그레이션
주석도 반대 방향에서 같은 말을 했다. 그리고 그 주장을 확인하는 것은 어느 쪽에도 없었다.

이 형태가 위험하다는 것은 추측이 아니다. 같은 저장소의
`vector_tile_publication_unit_key_check`와 `r2_layout.rs`가 정확히 이 관계였고,
[ADR-0013](./0013-release-uniqueness-admits-both-source-kinds.md) 남은 부채 1은 그것을 "publisher가
더 좁다"고 적었다. 대조해 보니 포함 관계가 아니라 **세 방향으로 어긋나** 있었고, 어느 쪽도 다른
쪽의 부분집합이 아니었다. **거울이라고 적힌 주석은 거울이라는 증거가 아니다.**

세 번째는 한 겹 더 나빴다. `ServingSourceKind`는 DB 철자를 **소유하지 않았다.** 철자는
`sqlx_repository.rs`의 match arm과 SQL CHECK에 각각 적혀 있었고, 도메인 타입은 어느 쪽과도
연결되지 않은 세 번째 진술로 떠 있었다. 그 decoder의 catch-all은 모르는 값을 **서빙 시점에**
에러로 바꿨다 — CHECK에 소스 종류를 더하고 decoder를 고치지 않으면 컴파일도 마이그레이션도
통과하고, 런타임 매니페스트 읽기가 통째로 실패한다. 판정이 빌드에서 서빙으로 미뤄져 있었다.

## Decision

### 1. 철자는 도메인 enum이 소유한다

`ServingSourceKind`가 `as_str`·`parse`·`ALL`을 가진다. `sqlx_repository.rs`는 `parse`를 부르고
enum 위에서 match한다. catch-all이 사라졌으므로 **변형을 더하면 컴파일이 깨진다.** 서빙 시점까지
미뤄져 있던 판정이 빌드로 올라왔고, 이것이 이 결정에서 유일하게 동작을 바꾸는 부분이다.

### 2. 각 enum은 자기 전체 집합을 내놓는다

`CatalogMutationKind::ALL`, `VectorTileBuildStatus::ALL`, `ServingSourceKind::ALL`. 대조하는 쪽이
값을 다시 적으면 거울이 하나 더 생길 뿐이므로, 열거는 어휘의 주인이 제공한다.

### 3. 대조는 파일이 아니라 설치된 제약을 읽는다

`a_database_vocabulary_is_spelled_the_same_way_in_both_languages`가 `pg_get_constraintdef`으로
허용 집합을 읽어 `ALL`과 비교한다. 마이그레이션 **파일**이 아니라 **적용된 스키마**를 보므로
마이그레이션이 실제로 적용된다는 것까지 함께 증명한다 —
[ADR-0013](./0013-release-uniqueness-admits-both-source-kinds.md)이 `pg_proc.prosrc`를 읽어
승격 게이트와 도메인을 대조한 것과 같은 이유다.

이 테스트가 적는 것은 **결속(어느 enum이 어느 제약을 철자하는가)뿐**이다. 양쪽 값 목록은 각자의
주인에게서 읽는다. 결속은 파생될 수 없는 진짜 정보이고, 값은 그렇지 않다.

### 4. 한 목록으로 읽히지 않는 제약은 탐침한다

`catalog_mutation_idempotency_manifest_outcome_check`는 어휘를 두 분기에 나눠 적으므로 인용부호
추출로는 `answers_with_manifest`가 주장하는 **부분집합**을 얻을 수 없다.
`a_command_answers_with_a_manifest_in_both_languages_or_in_neither`는 네 명령 × {결과 있음, 없음}을
실제로 넣어 데이터베이스가 어느 쌍을 받는지 본다. 파싱한 제약이 아니라 **설치된 제약**을 시험한다.

## 기각한 대안

### 한쪽에서 다른 쪽을 생성한다

빌드 시점에 SQL에서 enum을 뽑거나 enum에서 마이그레이션을 뽑으면 거울이 사라진다. 그러나 이
저장소의 마이그레이션은 추가 전용이고 **적용된 뒤에는 파일이 아니라 데이터베이스가 정본**이다.
생성기는 파일만 볼 수 있으므로, 정작 물어야 할 것(적용된 스키마가 무엇을 허용하는가)에는 답하지
못한다. 그리고 생성된 코드는 리뷰에서 읽히지 않는다.

### PostgreSQL enum 타입으로 옮긴다

`CREATE TYPE ... AS ENUM`은 어휘를 타입 하나에 모은다. 그러나 값 제거가 사실상 불가능하고
(`ALTER TYPE`에 DROP VALUE가 없다), 이 저장소가 이미 겪은 문제는 값이 **다르다**는 것이지 값이
여러 테이블에 흩어졌다는 것이 아니다. 대조 없이 타입만 바꾸면 같은 자리에서 같은 방식으로
어긋난다.

### 텍스트 가드로 두 목록을 비교한다

가드 체인은 빠르고 데이터베이스가 필요 없다. 그러나 가드는 마이그레이션 **파일**을 읽지 적용된
스키마를 읽지 않으며, 이 저장소의 가드는 의도적으로 Rust를 파싱하지 않는다. `ALL`을 텍스트로
읽어내는 가드는 enum 정의의 형태에 묶이고, 그 형태가 바뀌는 순간 조용히 빈 집합을 비교하게 된다.

### 주석을 더 분명하게 쓴다

이미 세 곳이 "mirrors ... exactly"라고 적고 있었고, 그것이 이 ADR이 필요해진 이유다.

## Consequences

- 세 어휘의 불일치는 이제 `postgres` 레인에서 잡힌다. 통합 테스트가 117 → 119개가 되었다.
- `ServingSourceKind`에 변형을 더하면 `sqlx_repository.rs`가 **컴파일 단계에서** 막는다. 이전에는
  런타임 매니페스트를 읽는 순간에만 드러났다.
- `catalog_domain`이 제약 **이름**을 알지 않는다. 결속은 대조하는 테스트(인프라 계층)에 있고,
  도메인은 자기 값만 안다.
- 대조 대상은 늘어날 수 있다. 어휘 하나를 추가하려면 표에 한 줄, 값은 한 글자도 다시 적지 않는다.

## 남은 부채

1. **다른 도메인의 어휘는 아직 대조되지 않는다.** `collection-domain`, `lakehouse-domain`,
   `foundation-normalization-domain`에도 DB 철자를 적는 enum이 있고, 그중 일부는 이
   플랫폼 마이그레이션 밖의 저장소를 향한다. 각 도메인이 자기 레인에서 같은 대조를 해야 하며,
   이 증분은 catalog에 한정한다.
2. **제약 읽기는 한 목록 형태에만 정확하다.** `pg_get_constraintdef`을 인용부호로 쪼개는 것은
   값이 `[a-z_]+`이고 제약이 단일 `IN` 목록일 때만 맞다. 둘 다 assert로 막아 두어 형태가 바뀌면
   조용히 짧은 목록을 비교하는 대신 실패하지만, 형태가 바뀐 제약은 4번 항목처럼 탐침으로 옮겨야
   한다.
3. **`vector_tile_build_job`은 여전히 아무도 쓰지 않는다.** 이 ADR은 여섯 상태의 **철자가 같다**는
   것만 증명한다. 그 여섯 중 어느 것도 writer가 없다는 사실은 그대로이며
   ([ADR-0017](./0017-a-data-revision-belongs-to-the-unit-it-revises.md) 남은 부채 3), 정적 승격
   구현이 오기 전에는 닫을 수 없다. 철자 일치와 도달 가능성은 다른 질문이다.
