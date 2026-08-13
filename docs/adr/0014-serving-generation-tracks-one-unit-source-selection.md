---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-30
---

# ADR 0014: serving generation은 한 단위의 소스 선택만 추적한다

- Status: Accepted
- Date: 2026-07-30
- 관련: [FP-ADR-0004 정적 벡터 타일 런타임 계약](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md), [ADR-0013 릴리스 유일성은 두 소스 종류를 함께 허용한다](./0013-release-uniqueness-admits-both-source-kinds.md), [단일 출처 공간 데이터 공개 아키텍처](../architecture/single-source-spatial-publication.md)
- 마이그레이션: `20260730000003_serving_generation_tracks_one_unit_source_selection.sql`

## Context

`catalog.promote_vector_tile_runtime_manifest`의 gap 검사가 매니페스트에 선택된 **모든** 단위에
`unit.serving_generation + 1`을 요구했다.

```sql
(unit.active_release_id IS NOT NULL
 AND manifest_unit.serving_generation <> unit.serving_generation + 1)
```

같은 함수가 `next_unit_count <> publication_unit_count`를 거부하므로, 한 단위만 바꾸는
활성화도 매니페스트에 모든 단위를 담아야 한다. 두 규칙을 합치면 **한 단위를 바꿀 때 모든 단위의
serving generation이 함께 올라간다.**

이것은 컬럼이 존재하는 이유와 어긋난다.
[구현 안내서](../guides/single-source-spatial-publication-implementation.md) Task 6 Step 5는 전역
manifest version 대신 단위별 `expected_serving_generation`을 CAS 키로 택했고, 근거는 이랬다 —
같은 데이터 리비전으로 rollback하면 **보존된** release가 다시 활성화되므로 하나의
`active_release_id`가 서로 다른 두 세대에서 선택될 수 있고, release id만으로는 그 두 상태를
구분할 수 없다. 즉 이 값은 **한 단위의 소스 선택**을 식별한다. 이어받은 단위의 선택은 바뀌지
않았으므로, 세대를 올리는 것은 일어나지 않은 변화를 단정하는 것이다.

취향이 아니라 두 가지 실제 비용이 있다.

1. **단위별 무효화가 사라진다.** [FP-ADR-0004](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)의
   런타임 계약에서 클라이언트는 `refresh_after_seconds` 주기로 폴링하며 단위별
   `serving_generation`을 비교해 **어느 단위를 다시 받을지** 결정한다. 매 발행마다 전 단위가
   올라가면 매 발행마다 전 단위가 무효화되고, 단위별 세대가 전역 `manifest_generation`이 이미
   주는 정보 외에 아무것도 전달하지 못한다.
2. **Task 6 Step 1이 성립하지 않았다.** Step 1은 서로 다른 두 단위를 바꾸는 두 편집이 **둘 다
   commit**되고 전역 generation이 순서대로 두 번 증가할 것을 요구한다. 불가능했다 — 두 번째
   writer의 `expected_serving_generation`이, 자기와 무관한 단위를 발행한 첫 writer에 의해 이미
   낡아 있었다. 참여하지도 않은 compare-and-swap에서 패배했다.

발견 경로: Increment C가 `mark_tile_layer_dynamic` 트랜잭션을 실제 데이터베이스까지 뚫으면서
드러났다. 계층을 넓게 쌓지 않고 한 경로를 끝까지 뚫으라는 루트 AGENTS.md 규칙이 의도한 그대로다.

## Decision

gap 검사를 **단위의 전이별로** 나눈다. 규칙을 없애지 않고 기대값만 좁힌다.

| 단위 상태 | 요구되는 `manifest_unit.serving_generation` |
| --- | --- |
| `active_release_id IS NULL` (발행 이력 없음) | `1` |
| `manifest_unit.release_id <> unit.active_release_id` (release 변경) | `unit.serving_generation + 1` |
| `manifest_unit.release_id = unit.active_release_id` (release 재선택) | `unit.serving_generation` |

세 갈래 모두 기대값을 **정확히** 못박으므로 낡은 단위 상태로 조립한 매니페스트는 이전과 똑같이
거부된다. gap 탐지가 약해지지 않는다 — 이어받은 단위의 세대가 현재값과 다르면 그것이 곧 낡은
조립의 증거다.

같은 데이터 리비전 rollback은 보존된 fallback release를 선택하므로 `release_id`가 활성과 달라
**변경 갈래**에 들어간다. 즉 rollback은 세대를 올린다 — Step 4가 요구하는 대로, 리비전은 그대로
두면서 소스 선택이 바뀌었음을 클라이언트에 알린다.

Rust 측 대응은 `plan_dynamic_activation`에서 이어받는 단위에 `unit.serving_generation`을 그대로
쓰는 것 하나다. 판정의 권위는 계속 게이트에 있고, Rust는 실패가 단위 이름을 말하도록 먼저
답할 뿐이다.

## 기각한 대안

### Step 1을 정정하고 게이트를 그대로 둔다

문서만 고치면 되므로 비용이 가장 낮다. 그러나 위 비용 1이 남는다 — 단위별 세대가 정보를 잃고,
4초 폴링 계약이 단위 단위 갱신이라고 말하면서 실제로는 전체 갱신이 된다. 계획 문장을 코드에
맞추는 것이지 근본 원인을 없애는 것이 아니다.

### 매니페스트가 바뀐 단위만 담게 한다 (완전성 요구를 없앤다)

완전성 검사가 사라지면 gap 규칙 충돌도 사라진다. 그러나 매니페스트는 **완전한 전역 선택**이고
그것이 v2의 요점이다 — 클라이언트가 소스를 추론하거나 병합하지 않는다. 부분 매니페스트는
"어느 단위가 지금 무엇으로 서빙되는가"를 여러 세대에 걸쳐 조립하게 만들고, 반쯤 커밋된 단위를
섞을 여지를 되돌려 준다.

### 이어받은 단위의 세대를 검사에서 아예 제외한다

`release_id = active_release_id`인 행에 어떤 값이 와도 통과시키는 방식. 낡은 조립 탐지를
잃는다. 필요한 것은 기대값을 옳게 만드는 것이고, 검사를 끄는 것이 아니다.

### 전역 `manifest_generation` 하나로 통일하고 단위별 세대를 삭제한다

가장 단순하다. 그러나 Step 5가 기각한 방향이고 근거가 유효하다 — 같은 리비전 rollback에서
보존된 release가 재활성화되는 두 상태를 전역 version 하나로는 구분할 수 없다. 삭제하면 Step 4의
same-data rollback을 표현할 수 없게 된다.

## Consequences

- Task 6 Step 1의 두 단위 interleaving이 처음으로 작성 가능해졌다. 두 writer가 pointer 락에서
  직렬화되어 순서대로 commit하고, 각 매니페스트에 두 selection이 정확히 한 번씩 들어가며, 전역
  generation이 두 번 증가한다. 두 번째 writer의 자기 단위 기대값은 첫 writer의 발행에 영향받지
  않는다.
- 단위별 무효화가 실제로 단위별이 되었다. `parcels`를 바꾸는 발행이 `complex`의
  `serving_generation`을 건드리지 않으므로 클라이언트가 `complex` 타일을 다시 받지 않는다.
- 같은 리비전 rollback은 여전히 세대를 올린다. 보존된 release는 활성 release와 다르기 때문이다.
- 단위 행의 `version`은 이어받는 단위에서도 계속 증가한다. 그것은 행 낙관적 동시성 컬럼이고
  서빙 세대가 아니다 — 게이트가 그 행을 만졌다는 사실은 참이다.
- `CREATE OR REPLACE FUNCTION`이 세 번째로 게이트 본문을 교체했다
  (`20260724000001` → `20260730000001` → `20260730000003`). 유효 집합 전체를 읽어야 하는
  구조이며, `the_promotion_gate_and_the_domain_agree_on_the_release_object_root`가 파일이 아니라
  `pg_proc.prosrc`를 읽으므로 설치된 본문이 대조 대상이다.

## 남은 부채

1. **정적 승격은 아직 이 규칙을 지나가 보지 않았다.** `promote_tile_layer_static`은 여전히 port
   기본 구현이 에러다. 정적 release는 항상 새 release id이므로 변경 갈래에 들어가야 하지만,
   실제 트랜잭션이 없으므로 실측이 아니다.
2. **이어받은 단위가 정말 바뀌지 않았는지는 release id만으로 판단한다.** release 행이 불변이므로
   현재는 충분하다. release를 변경 가능하게 만드는 변경이 오면 이 전제가 깨진다.
