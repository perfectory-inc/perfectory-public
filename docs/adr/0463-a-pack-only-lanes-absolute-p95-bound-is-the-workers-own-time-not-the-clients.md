# ADR 0463: 팩 전용 레인의 절대 p95 상한은 클라이언트가 아니라 Worker 자신의 시간에 건다

- Status: Accepted
- Date: 2026-11-09
- Supersedes: [ADR-0459](./0459-the-cutover-latency-gate-judges-availability-on-the-server-and-bounds-the-clients-lost-reads-apart.md) §3 의
  "팩 전용 레인의 절대 p95 상한은 바뀌지 않는다". 지연 **증가** 상한(`latency_max_increase_ms`)은 그대로 클라이언트 쪽에서 잰다.
- Builds on: ADR-0190(팩 전용 레인의 기준선과 절대 상한), ADR-0162(그 상한이 나온 parcel 세대 1 컷오버), ADR-0460(analytics 적용 범위)

## Context

ADR-0190 은 팩 전용 레인(종류 레인·단가 레인)의 관문 (나)에 자기 p95 의 절대 상한을 두었다. 기준 레인의 느린 읽기가 증가 상한 안에 느린 레인을
숨기지 못하게 하려는 것이고, 값 1,400 ms 는 "지금 서빙하는 레인이 컷오버 때 잰 가장 느린 차가운 p95 — parcel 세대 1, **ai-server 에서** 1,379 ms" 였다.
그 숫자는 클라이언트 시계다. 호스트 `*-pack-g*/latency.json` 의 잰 값:

| 레인 (잰 날) | 클라이언트 팩 p95 | Worker p95 (Server-Timing `total`) | 판정 |
|---|---|---|---|
| 종류 레인 10–11월 초 통과분 9개 | 422–661 ms | 80–251 ms | 통과 |
| parcel-factories g1 (11-09) | **1,623 ms** | 206 ms | 절대 상한 하나로 거부 (나머지 전부 통과, 증가 cold p95 −532 ms) |
| parcel-energy g1 (11-09) | 2,283 ms | 181 ms | — |
| parcel-demolitions g1 (11-09) | 2,069 ms | 170 ms | — |
| parcel-register-masters g1 (11-09) | 2,127 ms | 161 ms | — |
| parcel 세대 1 (상한의 출처, ADR-0162) | 1,379 ms | 703 ms | — |

Worker 의 시간은 그대로이고 클라이언트 p95 만 3–4 배가 됐다. 11-09 측정으로 한국 클라이언트(프로브 포함)는 Cloudflare LAX 로 들어가고,
그 경로는 패킷 손실 20%(코디네이터 측정)다. 절대 상한이 망을 재고 있다. 증가 상한은 같은 경로의 두 쪽을 견주므로 망이 지워져 공정하다.

Google SRE Workbook 은 지연 SLI 를 서버(로드 밸런서) 쪽에서 재고 클라이언트 프로버를 따로 둔다. 이 저장소의 Worker 는 답마다
`Server-Timing: total` 로 자기 시간을 말한다(관문이 이미 `server_timing.total_ms` 로 모은다).

## Decision

1. **절대 상한은 Worker 자신의 p95 에 건다.** 팩 전용 레인의 관문 (나)는 `server_timing.total_ms.p95 ≤ latency_p95_max_ms` 를 본다
   (`LatencyEvidence::verdict`). 클라이언트의 팩 p95(차가운·따뜻한)는 증거에 그대로 남아 보고된다.
2. **그 p95 는 충분한 답 위에 서야 한다.** `Server-Timing` 을 낸 답이 시간을 잰 짝의 `slo.analytics_min_coverage`(0.9, ADR-0460) 이상이어야 한다.
   `Server-Timing` 이 없으면 0 이라 닫힌다.
3. **값은 같은 원리, 서버 시계로**: `by_pnu_lane_rule.latency_p95_max_ms` 와 단가 레인의 값을 1,400 → **700 ms**. 상한의 출처였던 parcel 세대 1 컷오버의
   Worker p95(703 ms)다. 그 뒤 잰 종류 레인은 모두 251 ms 안이었다. 이름(`latency_p95_max_ms`)은 그대로 두고 계약의 이유에 뜻을 적는다.

## Consequences

- 11-09 에 이 상한 하나로 막혔거나 막힐 레인(factories·energy·demolitions·register-masters 등)은 Worker 가 느려지지 않는 한 망 때문에 막히지 않는다.
- Worker 가 정말 느린 레인은 여전히 막힌다: Worker p95 가 700 ms 를 넘거나, 기준 레인 대비 클라이언트 증가가 증가 상한을 넘으면.
- 시험: `pack_lane_tests.rs` 가 클라이언트 p95 만 상한을 넘는 증거는 통과, Worker p95 가 넘는 증거는 거부함을 잡는다.
- 경로(KR→LAX)는 W43 이 고친다. 사용자가 보는 지연은 그 뒤 클라이언트 p95 와 `edge_colos` 가 보여 준다.
