# ADR 0243: 필지 ID 원장의 첫 등록은 예약 작업이 시도마다, 시간 상한 안에서 이어 간다

- Status: Accepted
- Date: 2026-10-14
- Builds on: [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md)(필지 id 원장),
  [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)(지도가 쓰는 판),
  [ADR-0234](./0234-the-flow-map-says-job-missing-only-for-data-that-should-have-a-job.md)(`not_started` 이던 칸)

## Context

사장 결정(2026-10-14): `silver.parcel_registry` 첫 전국 적재는 예상 시간·메모리·R2 비용을 먼저 보고하고, 시간 상한 안에서
자동배포 창 밖에만 돌린다. 확인한 것:

- 표는 운영에 없다. 코드(`parcel_registry_to_silver.py`)는 있으나 손 절차도 런북도 없다.
- 판 202606 은 시도 17, 필지 39,861,511(가장 큰 시도 5,925,255). 스크립트는 지역의 PNU 와 결과 행을 드라이버의 파이썬 객체로
  모은다 — 전국 한 번(수십 GB)은 Spark 상한(20g)에 들어가지 않는다. 시도 하나는 들어간다.
- 운영자가 Spark 를 직접 돌릴 비밀번호 없는 길은 없다. 꺼진 작업의 감독 첫 실행은 ADR-0235 의 `job start` 다.
- 판을 넘기는 advance 는 `silver.parcel_lineage` 가 필요하다. 그 표는 이날 병합된 #542 부터 `lineage_stewardship` 이 판 쌍마다 만든다 — 운영에는 아직 행이 없다.

## Decision

1. `parcel_registry_to_silver.py --probe-missing` 은 지도가 쓰는 판의 시도 중 원장에 아무 행도 없는 곳을 필지 수와 함께, 작은
   것부터 낸다. 표를 만들지 않는다(없으면 모두 빠진 것).
2. 예약 작업 `parcel_registry`(`scripts/ops/parcel-registry-cycle.sh`, spark 3 자리, compose `spark`·드라이버 16g, 유닛 150 분,
   매일)는 빠진 시도를 하나씩 `--bootstrap` 한다. 시작한 지 100 분이 지나면 새 시도를 시작하지 않는다. 남은 시도는 다음 실행이
   잇는다. 시도마다 걸린 시간을 journal 에 남긴다. 모든 시도가 있으면 `unchanged`.
3. 작은 것부터인 까닭: 시간 상한에 걸린 실행도 최대한 많은 시도를 남기고, 첫 감독 실행이 큰 시도 전에 시간·메모리를 잰다.
4. 작업은 꺼진 채 등록되고 감독 실행은 `job start parcel_registry` 다. 그래프의 `not_started` 는 지운다(ADR-0234 가드).
5. advance(판 넘기기)와 필지 단위 발행 ID 는 이 작업에 없다 — `silver.parcel_lineage` 가 운영에 쌓인 뒤의 별도 변경이다.

## Consequences

- 첫 등록이 사람 손 없이, 상한 안에서, 끊겨도 이어지는 방식으로 돈다. 예상 비용은 R2 월 수십 원.
- 가장 큰 시도가 드라이버 메모리를 넘으면 감독 실행이 137 로 알려 준다. 그때는 켜지 않고 모으는 구조를 다시 연다(런북).
- 시험: `test_parcel_registry_to_silver.py`(빠진 시도·probe 인자), `orchestration/tests/test_parcel_registry_cycle.py`(순서·날짜·
  상한·실패한 시도). 남은 시도가 없을 때 빈 이름으로 한 번 돌던 결함을 이 시험이 잡았다.
