# ADR 0460: 컷오버 지연 관문은 클라이언트가 잃은 읽기를 보고만 하고, 서버 판정은 analytics 가 충분히 덮었는지로 세운다

- Status: Accepted
- Date: 2026-11-09
- Supersedes: [ADR-0459](./0459-the-cutover-latency-gate-judges-availability-on-the-server-and-bounds-the-clients-lost-reads-apart.md) §2
  (답 없이 잃은 읽기를 `client_transport_failure_max_share` 0.01 로 상한). 나머지(서버 가용성의 정의, 지연 SLO, `edge_colos`, 재시도 없음)는 그대로다.

## Context

ADR-0459 가 배포된 첫 측정(2026-11-09, parcel-building-permits 세대 2, 호스트 `latency.json`)에서 관문은 다시 망에 떨어졌다.

| 단계 | 보낸 읽기 | 답 받음 | 잃은 읽기 (분류) | analytics invocation | 그중 success | 클라이언트 p99 |
|---|---|---|---|---|---|---|
| 짝 읽기 (팩 쪽) | 10,000 | 9,995 | 5 (`request-timeout` 4, `request` 1) | 19,739 (양쪽, 표본 간격 1.10) | 전부 | — |
| load | 15,000 | 14,739 | **261 (1.74%)** (`request-timeout` 229, `body-timeout` 32) | 14,475 (표본 간격 1.73) | 전부 | 9,339 ms |

- 서버는 본 읽기를 모두 `success` 로 냈고 서버 실패는 0 이다. 그래도 ADR-0459 §2 의 1% 상한이 load 의 1.74% 로 관문을 닫았다.
  같은 시각 코디네이터의 측정은 사무실에서 LAX 까지 패킷 손실 20%, RTT 336 ms 였다. 상한은 그날의 망 상태를 재고 있었다.
- §2 를 둔 이유는 "망가진 길을 잰 프로브는 아무것도 증명하지 못한다" 였다. 그런데 증명해야 하는 것은 서버가 **충분한 읽기를 보고** 판정받았는지이고,
  그건 analytics 가 기록한 invocation 수로 직접 잴 수 있다. 위 load 에서 analytics 는 보낸 읽기의 96.5%(표본 추정)를 보았다.
- Google SRE Workbook 은 블랙박스 프로버의 망 실패를 SLI 가 아니라 프로버 건강으로 다룬다. Kayenta·Argo Rollouts 의 판정은 지표마다
  "표본이 충분한가"(최소 표본 수)를 먼저 묻는다. 이 저장소의 Worker 카나리도 같은 이유로 `canary.analytics_min_coverage` 를 둔다.

## Decision

1. **클라이언트가 답 없이 잃은 읽기는 보고만 한다.** `request*`·`connect`·`body*`(`gate::is_transport_class`)와 load 의 `shed` 는
   관문을 닫지 않는다. 증거의 `failures`·`availability`·`load.shed` 와 프로브 로그(`server`, `client_availability`, `edge_colos`)에 남는다.
   계약의 `slo.client_transport_failure_max_share` 는 없어진다.
2. **서버 판정은 충분히 덮여야 한다.** 계약의 새 값 `slo.analytics_min_coverage`(0.9): 짝 읽기와 load 각각에서 analytics 가 기록한 invocation 이
   보낸 미리보기 읽기의 90% 이상이어야 한다(`Served::coverage`). analytics 가 없으면 0 이라 닫힌다. 0.9 는 잰 값(짝 98.7%, load 96.5%, 표본 추정)
   아래이고, 읽기 열에 하나를 서버가 보지 못한 측정은 받지 않는다.
3. **지연을 잰 짝도 충분해야 한다.** 짝 읽기에서 둘 다 답한 짝이 표본의 `analytics_min_coverage` 이상이어야 하고, no-gzip 표본도 답 받은 읽기가
   그 비율 이상이어야 한다. 받은 답은 모두 200·무압축·같은 내용이어야 하는 것은 그대로다.
4. 서버 가용성(`availability_min`, ADR-0459 §1)과 양쪽 서버 실패의 상한(짝의 `answered + 잃은 읽기 ≥ 표본 × availability_min`)은 그대로다.

## Consequences

- 망이 나빠도(20% 패킷 손실) 서버가 본 읽기를 모두 답했고 충분히 보았다면 관문이 열린다. 망이 아주 나빠 analytics 가 90% 를 못 보거나,
  잰 짝이 90% 에 못 미치면 여전히 닫힌다 — 그때는 측정 자체가 모자란 것이다.
- `gate.rs` 의 `within_share` 는 없어지고, 시험(`tests.rs`)은 세 경우를 잡는다: 2% 시간 초과 + analytics 전부 success 는 통과,
  같은 수의 비성공 invocation 은 거부, analytics 80% 는 거부.
- 경로(KR→LAX) 자체는 W43 이 고친다. 클라이언트 p99 는 증거에 남아 그 결과를 보여 준다.
