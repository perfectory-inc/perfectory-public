# ADR 0265: 대조 관문은 자기가 읽는 필지 ID 원장이 생길 때까지 기다린다

- Status: Accepted
- Date: 2026-10-16
- Amends: [ADR-0253](./0253-the-parcel-tile-bake-runs-in-two-halves-and-is-rehearsed-before-an-edition-is-served.md) (Gold 반쪽과 예행)
- Builds on: [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §7,
  [ADR-0243](./0243-the-parcel-registry-is-registered-region-by-region-by-a-scheduled-job.md)

## Context

2026-10-16 03:35 UTC에 예행의 Gold 반쪽이 35초 만에 죽었다. 대조 관문(`parcel_matching_gate.py`)의 (b) 검사가
`silver.parcel_registry`를 읽는데, 운영에 그 표가 없다. 표를 만드는 작업 `parcel_registry`(ADR-0243)는 꺼진 채
등록되어 있다. 감독 첫 실행을 한 적이 없고, 유닛 기록도 없다.

관문과 원장 사이의 순서는 어디에도 적혀 있지 않았다.

- `parcel_served_gold`의 그래프 간선은 Silver 경계와 편집 원장만 입력으로 적었다.
- 그래서 작업 의존성 표에 원장 작업이 생산자로 나타나지 않았다.
- 관문은 표가 없으면 오류로 죽었다. 예전 작업자는 `--no-registry`로 손 실행했다.

## Decision

1. **의존을 적는다.** 그래프에 간선 `silver-parcel-registry-to-gold-parcel-boundary-served`(관계 `validates`)를 두고,
   `parcel_served_gold`가 그 간선을 가진다. 작업 의존성 표에 `parcel_registry`가 이 작업 입력의 생산자로 나온다.
2. **입력이 없으면 관문은 돌지 않는다.** 관문은 맨 먼저 `silver.parcel_registry`가 있는지 본다.
   - 없으면 아무것도 세지 않는다. `registry: wait`, `inputs_absent`를 담은 판정을 쓰고 3으로 끝난다.
   - Gold 반쪽은 3을 받으면 아무것도 쓰지 않고 `unchanged`로 끝난다. journal에 기다리는 입력을 남긴다. 다음 실행이 다시
     본다.
3. **판정은 원장을 어떻게 다뤘는지 적는다.** `registry`는 `checked`, `skipped`(`--no-registry`), `absent`(예행),
   `wait` 가운데 하나다. 오븐은 `checked`인 판정에만 build를 묶는다. 원장을 보지 않은 판정으로는 굽지 않는다.
4. **예행은 기다리지 않고 적는다.** 예행은 `--registry-when-present`로 관문을 돌린다. 표가 없으면 (b)를 건너뛰고
   `registry: absent`로 적는다. 예행 판정은 build에 쓰이지 않는다(ADR-0253).
5. **예행의 판은 사람이 정한다.** `parcel-tiles rehearse gold <YYYYMM>`이다. 원천 계약은 Silver가 아직 적재하지 않은
   판을 담을 수 있다. 2026-10-16에 202610이 계약에 들어왔지만 Silver에는 없다. 그래서 "계약의 가장 새 판"을 고르면
   안 된다.

## Consequences

- `parcel_registry`의 감독 첫 실행(ADR-0243 런북) 전에는 필지 타일 작업이 굽지 않는다. 대신 기다린다는 기록이 남는다.
- 남은 설계 문제 둘은 원장·계보 쪽이 정한다.
  - (b)는 원장이 있으면 검사한 시도의 **모든** 필지에 현재 id를 요구한다. 그래서 등록 중인 시도들도 거부된다.
    관문 설명("원장이 있는 지역에서")과 맞지 않는다.
  - 원장은 서빙 판(202606)만 등록한다. 새 판으로 넘기는 advance는 아직 없다(ADR-0243 5항). 그래서 새 판(202609)의
    번호가 바뀐 필지는 (b)를 통과할 수 없다.
