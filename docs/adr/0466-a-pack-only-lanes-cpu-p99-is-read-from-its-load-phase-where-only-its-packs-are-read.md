# ADR 0466: 팩 전용 레인의 CPU p99 는 자기 팩만 읽는 load 단계에서 잰다

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0190(팩 전용 레인은 기준 레인과 짝으로 잰다), ADR-0193(종류 레인은 호스트 Worker 가 서빙한다), ADR-0459·0460·0463(관문 (나)를 무엇이 실패했는지로 가른다)

## Context

관문 (나)는 Workers analytics 의 CPU p99 를 `worker_cpu_p99_max_ms`(10 ms) 로 두 번 잰다: 짝 읽기 창과 load 단계. analytics 는 스크립트 단위로만
물을 수 있다. 종류 레인(ADR-0193)은 호스트(parcel) Worker 가 서빙하고, 그 기준 레인도 같은 Worker 의 미리보기다. 그래서 짝 읽기 창의 CPU 는
**레인의 팩 읽기와 기준 레인(parcel 호스트 문서) 읽기가 섞인** 값이다. load 단계는 레인의 미리보기만 읽는다.

11-09 호스트 `*-pack-g1/latency.json`(관문의 나머지는 ADR-0459·0460·0463 아래 모두 통과):

| 레인 | 짝 창 CPU p99 | load CPU p99 | exceededResources |
|---|---|---|---|
| parcel-sub-parcels | **10.89 ms** | 5.45 ms | 0 |
| parcel-register-masters | **10.31 ms** | 3.64 ms | 0 |
| parcel-building-permits g2 | **10.37 ms** | 5.49 ms | 0 |
| parcel-energy | 8.02 ms | 2.98 ms | 0 |

짝 창 p99 가 상한을 넘은 몫은 기준 레인의 읽기다. 레인 자기 경로(load)는 상한의 절반 안이다. Google 의 카나리 판정(Kayenta)은
"시험 대상만 받는 트래픽의 지표"로 판정한다. 섞인 지표는 대상의 판정이 아니다.

## Decision

1. **팩 전용 레인(`baseline_lane` 이 있는 증거)의 CPU p99 는 load 단계에서 잰다.** `cpu_p99_ms ≤ worker_cpu_p99_max_ms` 는 load 의 analytics 에만 건다.
   짝 창의 CPU p99 는 증거에 남아 보고된다.
2. **잘림은 어디서든 실패다.** 두 창 모두 `requests > 0` 이고 `exceededResources` 0 이어야 한다(Worker 가 잘린 답은 사용자가 받는 503 이다).
3. 객체 경로가 있는 레인(기준선이 자기 객체 경로)은 그대로 두 창 모두 p99 상한을 받는다.

## Consequences

- 11-09 에 짝 창 CPU 로만 막힌 세 레인은 다음 시도에서 이 이유로 막히지 않는다. 팩 경로가 정말 비싸지면 load 에서 막힌다.
- 시험: `pack_lane_tests.rs` — 팩 전용 레인의 짝 창 p99 만 넘으면 통과, load 의 p99 가 넘으면 거부.
- 기준 레인의 CPU 는 그 레인 자신의 관문과 Worker 카나리가 잰다.
