---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-30
---

# ADR 0013: 릴리스 유일성은 두 소스 종류를 함께 허용한다

- Status: Accepted
- Date: 2026-07-30
- 관련: [FP-ADR-0004 정적 벡터 타일 런타임 계약](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md), [단일 출처 공간 데이터 공개 아키텍처](../architecture/single-source-spatial-publication.md), [ADR-0012 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md)
- 마이그레이션: `20260730000001_static_release_object_key_prefix_gate.sql`, `20260730000002_release_uniqueness_admits_both_source_kinds.sql`

## Context

v2 공간 데이터 공개의 정적 승격 경로가 **실행 불가능한 상태로 배포되어 있었다.**
`catalog.vector_tile_release`의 제약이 이랬다.

```sql
UNIQUE (publication_unit_id, data_revision, canonical_iceberg_snapshot_id)
```

publication unit 하나와 data revision 하나에 릴리스는 **정확히 하나**다. 그런데 같은
스키마와 도메인의 다른 세 규칙이 **둘**을 요구한다.

1. `catalog.promote_vector_tile_runtime_manifest`는 정적 릴리스의 revision이 unit의 현재
   선택과 다르면 거부한다 — `static PMTiles must use the currently selected data revision`.
   따라서 정적 릴리스는 활성 revision을 그대로 들고 있어야 한다.
2. `vector_tile_publication_unit_fallback_revision_check`는 보존된 fallback이 활성 revision과
   같아야 한다고 요구한다. 따라서 대체되는 동적 릴리스도 같은 revision에 남는다.
3. `catalog_domain::validate_build_snapshot_binding`은 빌드의 frozen snapshot이 입력 릴리스의
   snapshot과 같아야 한다고 요구한다. 따라서 두 릴리스는 snapshot까지 공유한다.

세 규칙을 합치면 같은 `(unit, revision, snapshot)`에 동적·정적 릴리스가 공존해야 하고,
유니크 제약이 그것을 금지한다.

**추론이 아니라 실측이다.** 마이그레이션된 실제 데이터베이스가 두 번째 insert를 `23505`로
거부했다. 이것이 [단일 출처 공개 구현 안내서](../guides/single-source-spatial-publication-implementation.md)
Task 6 Step 6에 구현이 없는 이유다 — 트랜잭션을 쓸 수 없었다.

이 모순이 오래 살아남은 이유는 아무 검사도 그 지점을 보지 않았기 때문이다. 도메인 검증기는
object key의 **파일명만** 비교했고 데이터베이스 게이트도 같았다. 그래서 문서가 적은 중첩
레이아웃(`releases/{release_id}/…`)과 실제로 배포된 평평한 레이아웃이 모두 통과했다.

## Decision

### 1. 유니크 키에 `source_kind`를 더한다

```sql
ALTER TABLE catalog.vector_tile_release
    DROP CONSTRAINT vector_tile_release_unit_revision_snapshot_key,
    ADD CONSTRAINT vector_tile_release_unit_revision_snapshot_kind_key
    UNIQUE (publication_unit_id, data_revision, canonical_iceberg_snapshot_id, source_kind);
```

모델이 실제로 말하는 것이 이것이다 — **publication unit · data revision · snapshot ·
완전한 소스 종류당 릴리스 하나.** 그 이상 넓히지 않는다.

- 같은 revision을 정적으로 두 번 승격하면 여전히 충돌한다. 그것은 진짜 중복이다.
- 실패하거나 superseded된 빌드는 충돌할 수 없다. 릴리스 행은 **승격 성공만** 쓴다.

`id`를 포함한 두 키는 건드리지 않았다. `vector_tile_release_id_unit_revision_key`와
`vector_tile_release_selection_binding_key`는 `vector_tile_publication_unit`과
`vector_tile_runtime_manifest_unit`의 외래키가 이름으로 참조하는 인덱스이므로 그대로 둔다.

### 2. 게이트는 object key를 전체 비교한다

`catalog.promote_vector_tile_runtime_manifest`의 정적 identity 검사가 파일명 접미사가 아니라
key 전체를 비교한다. 함수 주석이 "직접 SQL 호출자가 우회할 수 없게 도메인 상태기계를
반복한다"고 주장했지만, 접두어가 자유로웠으므로 그 우회가 실제로 열려 있었다.

레이아웃의 정의는 `catalog_domain::static_release_pmtiles_object_key` 하나다. Rust 검증기와
`r2_layout.rs`가 그 함수를 쓰고, 중복이던 루트 상수는 삭제했다.

## 기각한 대안

### 유니크 제약을 그대로 두고 정적 릴리스에 다른 revision을 준다

`data_revision`이 다르면 유니크 충돌은 사라진다. 그러나 게이트의 "정적은 현재 선택된
revision을 쓴다"와 fallback CHECK의 "보존된 fallback은 활성 revision과 같다"를 둘 다
위반한다. 더 근본적으로, 정적 릴리스는 **같은 내용을 다른 방식으로 서빙하는 것**이므로
revision이 달라지면 그 사실 자체가 거짓이 된다.

### 유니크 제약을 완전히 삭제한다

같은 revision을 정적으로 두 번 승격하는 진짜 중복을 허용하게 된다. 필요한 것은 한 축을
더하는 것이고, 축을 없애는 것이 아니다.

### 모순을 남겨 두고 정적 승격을 포기한다

정적 `PMTiles`는 PostGIS의 지속 렌더링 부하를 줄이는 수단이고 [FP-ADR-0004](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)가 승인한 방향이다.
게이트·CHECK·도메인 검증이 모두 그 경로를 전제하므로, 모순을 남기면 문서화되고 기계로
강제되는 경로가 **영구히 도달 불가능**해진다.

### `DROP CONSTRAINT` 대신 새 테이블로 옮긴다

데이터 이관·외래키 재작성·이중 쓰기 기간이 필요하다. 유일성 키 교체는 데이터를 파괴하지
않으며 이 저장소의 마이그레이션은 추가 전용 원칙을 **행 데이터**에 적용한다. 제약 교체는
그 원칙과 충돌하지 않는다.

## Consequences

- 정적 승격 경로가 실행 가능해졌다. `catalog-infrastructure`의 통합 테스트
  `a_static_release_can_replace_the_dynamic_release_of_the_same_revision`이 실제 데이터베이스에서
  insert → 게이트 통과 → fallback 보존을 증명한다.
- `DROP CONSTRAINT`가 이 저장소 마이그레이션의 첫 선례가 되었다. 유일성 키 교체에 한정한다.
- 게이트가 primary 코드 경로보다 느슨하지 않다. 두 곳이 같은 전체 key를 본다.
- SQL 함수는 Rust 크레이트를 호출할 수 없으므로 루트 문자열이 plpgsql에 한 번 더 적힌다.
  이 중복은 `the_promotion_gate_and_the_domain_agree_on_the_release_object_root`가 `pg_proc.prosrc`를
  읽어 대조한다 — 파일이 아니라 **설치된 함수 본문**을 보므로 마이그레이션 적용까지 함께 증명한다.

## 남은 부채

1. **`r2_layout.rs`가 소문자 unit key를 요구한다.** `unit_key` 컬럼은 대소문자를 허용하므로
   publisher가 검증기보다 좁다. 현재 사용 중인 unit(`parcels`, `complex`, `buildings`)이 모두
   소문자여서 무해하지만, 두 규칙이 다른 것은 그대로다.
2. **정적 승격의 fallback 쓰기 순서가 규약이다.** `fallback_distinct_check`가 활성과 같은
   fallback을 금지하므로 게이트 호출 **뒤에** 써야 한다. 게이트는 fallback을 보존하거나 지울
   뿐 설정하지 않는다. 이 순서를 기계로 강제하는 것은 없다.
