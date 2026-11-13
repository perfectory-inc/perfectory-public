# ADR 0522: 레인 색인의 모든 서빙 레인과 지도 타일 층은, 다른 무엇이 재지 않아도 신선도 예산에 묶인다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md)(예산 48시간), [ADR-0444](./0444-a-by-pnu-lane-is-stale-when-it-is-behind-its-inputs-not-when-its-manifest-is-old.md)(나이가 아니라 뒤처짐), [ADR-0418](./0418-the-artifacts-the-declared-definitions-generate-are-never-committed-and-every-checkout-and-release-generates-them.md)(레인 색인), [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md) §7(자산 조정기)
- 관련 사건: 2026-11-11 W70 실측 — parcel-nearby·parcel-real-transaction 은 굽기 작업이 켜진 채(11-09·11-10 18:00Z) 등록돼 있었지만 한 번도 발행되지 않았고, 서빙 모니터 타이머는 첫 발행이 켜므로 그 레인을 부르는 시계열이 하나도 없었다. 지도 타일 층에는 신선도 시계열 자체가 없었다: parcels 유닛은 2026-09-28 릴리스를 서빙하는데 `gold.parcel_boundary_served` 는 10-15 에 행이 바뀌었고, zoning 유닛은 같은 Silver 판에서 매일 굽기가 실패했다(`already serve Gold snapshot`). 어느 경보도 울리지 않았다.

## Context

ADR-0306·0444 의 `FoundationByPnuServingStale` 은 두 시계열로 판정한다: 매시 서빙 모니터가 쓰는
`foundation_by_pnu_served_manifest_age_seconds` 와, 자산 조정기가 쓰는 `foundation_by_pnu_lane_behind_seconds`.
뒤처짐이 없으면(조정기 `reconciles` 밖의 레인 — 굽기 작업이 꺼진 parcel-regulation-zones·parcel-court-auction) manifest
나이로 판정한다. 그 대체는 이미 있고 맞게 돈다(11-11 운영 Prometheus: 두 레인 모두 나이 시계열이 있고 뒤처짐은 없다).

구멍은 둘이다.

1. **잴 것이 없으면 조용하다.** 모니터 타이머는 레인의 첫 발행(cut-over, `by-pnu-pack-operator.sh`)이 켠다. 발행되지 않은
   레인은 나이도 뒤처짐도 없고, Prometheus 규칙은 없는 시계열에 대해 아무것도 말하지 않는다(Prometheus 문서가 `absent()`
   로 경고하는 고전적 구멍). 레인 21개 중 19개만 시계열이 있었다.
2. **지도 타일은 아예 재지 않는다.** 타일 층 4개(parcels·admin·complex·zoning)는 by-PNU 레인이 아니어서 어느 모니터·조정기
   대상도 아니었다. 사용자가 보는 것은 foundation-api 의 공개 런타임 manifest(`/catalog/v1/vector-tiles/runtime-manifest`,
   교통 정책 등록부 `foundation_platform.public.runtime_manifest_v2`)이고, 그 유닛마다 릴리스가 지어진 Iceberg 스냅샷
   (`canonical_iceberg_snapshot_id`)이 있다. 그러니 ADR-0444 의 규칙을 그대로 쓸 수 있다.

11-11 실측(Trino `$snapshots`, 운영 카탈로그 DB·API 읽기 전용):

| 유닛 | 서빙 스냅샷 (표) | 그 스냅샷 커밋 | 표의 마지막 커밋 |
| --- | --- | --- | --- |
| parcels | 4772101085368195119 (`gold.parcel_boundary_served`, 만료돼 기록에 없음) | 09-28 이전 | 10-15 18:52Z |
| admin | 7791031098697281691 (`gold.administrative_boundary_served`) | 10-16 04:01Z | 같음 |
| complex | 2925005290666209044 (`gold.industrial_complex_boundary_served`) | 09-29 02:06Z | 같음 |
| zoning | 5302716389191760467 (`silver.land_use_zone_area`) | 11-01 04:55Z | 11-11 10:14Z |

업계의 같은 방식: kube-prometheus·Google SRE Workbook 은 "기대되는 대상 목록" 을 따로 두고 그 목록 대비 빠진 대상을
알린다(`up == 0`·`absent()` 대신 대상 레지스트리와의 차집합). Dagster 의 freshness 검사는 정의된 모든 자산에 붙고,
materialization 이 한 번도 없는 자산도 "late" 다.

## Decision

1. **하나의 정본.** 레인 색인(`config/by-pnu-lane-index.v1.json`, `scripts/catalog/declared_definitions.py lane_index`, 생성물)이
   by-PNU 레인 21개에 더해 `tile_units` 를 낸다: 그래프의 `surface_kind: tiles` 서빙 면마다, 그 정의의
   `publication_unit` 이 적은 런타임 manifest 키(`key`)와 릴리스 스냅샷이 속한 표(`snapshot_table`), 그리고 그 면에 간선을
   가진 작업(`bake_jobs`). 새 타일 층은 정의에 이 두 값을 적어야 색인에 들어간다(시험 `ServedSurfaces` 가 그래프의 모든
   타일 면이 색인에 있음을 확인).
2. **등록 시각.** 자산 조정기(5분마다)가 색인의 레인마다 `foundation_by_pnu_lane_registered_since_timestamp_seconds` 를 쓴다:
   그 레인 굽기 작업 중 켜진 것의 가장 이른 `enabled_since`(jobs.v1.json), 켜진 굽기가 없으면 0. 규칙
   `FoundationByPnuLaneUnmeasured`(page)는 등록 뒤 예산이 지났는데 나이 시계열이 없는 레인을 알린다. 0 인 레인(굽기가
   꺼짐)은 재는 것이 없어지는 즉시 알린다 — 꺼진 굽기는 다시 발행하지 않으니 잴 수 없는 레인은 묵은 레인이다.
3. **타일 뒤처짐.** 조정기가 런타임 manifest 를 호스트의 foundation-api(주소는 `host-layout.contract.json readiness`, 경로는
   교통 정책 등록부)에서 읽고, 유닛마다 `foundation_tile_unit_served`(그 키가 manifest 에 있나)와
   `foundation_tile_unit_behind_seconds`(그 `snapshot_table` 에서 서빙 스냅샷 뒤 행을 바꾼 가장 오래된 커밋부터 지금까지 —
   ADR-0444 와 같은 `moved()`; 서빙 스냅샷이 만료로 기록에 없으면 남은 가장 오래된 변경, 하한)를 쓴다. 규칙:
   `FoundationTileUnitBehind`(page, 예산 초과), `FoundationTileUnitNotServed`(page, 30분), `FoundationTileUnitUnmeasured`
   (ticket, 1시간: 표를 못 읽었거나 manifest 를 못 읽음).
4. **예산은 하나.** 레인과 타일 모두 `r2-connections.contract.json by_pnu_section_packs.freshness_budget_hours`(48) 를
   조정기가 `foundation_serving_freshness_budget_seconds` 로 내고 규칙이 그것과 비교한다. 타일 전용 예산을 만들지 않는다.
5. promtool 시험(`host-resources.rules.test.yml`)이 각 규칙을 위 사건의 합성 시계열로 울리고, 예산 안·잰 레인·뒤처짐 0
   유닛은 조용함을 확인한다. 가드 `prometheus-rules-pass-their-tests`(ADR-0385)가 돌린다.

## Consequences

- 배포 직후 실제로 울릴 것(사실): parcels 타일 `FoundationTileUnitBehind`(약 27일; 판 202609 리허설로 `parcel_tile_bake`
  꺼짐, 이미 울리는 `FoundationParcelServedEditionBehind` 와 같은 원인), zoning 타일(Silver 가 11-01 판 뒤로 행을 바꿨다면
  그 시각부터 48시간 뒤), parcel-nearby(11-11 18:00Z 뒤)·parcel-real-transaction(11-12 18:00Z 뒤) `FoundationByPnuLaneUnmeasured`.
- 패치(ADR-0323)가 서빙하는 스냅샷은 R2 의 패치 manifest(`gold_snapshot`)에만 있고 런타임 manifest 에는 없다. 조정기는
  R2 권한이 없으므로 패치가 나간 유닛은 기본 릴리스 스냅샷부터 재서 실제보다 뒤처져 보일 수 있다(11-11 에는 어느 유닛도
  패치를 서빙하지 않는다: zoning 은 11-08 기본판, parcels 굽기는 꺼짐). 패치 발행이 그 스냅샷을 카탈로그 릴리스에 남기면
  이 측정은 그대로 맞아진다 — 그때까지의 알려진 한계다.
- 조정기는 매 차례 loopback HTTP 1회와 표 4개 이력 읽기(이미 캐시된 카탈로그 REST)를 더한다. 비용 0.
- 운영 준비도 표(ADR-0442)는 아직 타일 행을 읽지 않는다. 같은 색인을 읽으면 된다(후속).
