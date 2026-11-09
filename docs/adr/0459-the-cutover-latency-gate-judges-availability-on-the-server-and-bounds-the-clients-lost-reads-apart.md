# ADR 0459: 컷오버 지연 관문은 가용성을 서버에서 판정하고, 클라이언트가 답 없이 잃은 읽기는 따로 상한한다

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0147 §6(관문 (나)), ADR-0151(섹션 팩 서빙), ADR-0157(버전 헤더), ADR-0190(팩 전용 레인의 기준선), ADR-0233(엣지의 gzip 해제)
- Replaces: draft #1211(답 없는 요청 한 번 재시도) — 재시도는 클라이언트 쪽 숫자를 부풀릴 뿐, 무엇이 실패했는지를 가르지 못한다

## Context

관문 (나)(`probe-<lane>-section-pack-latency`)는 `slo.availability_min` 0.9995 를 **클라이언트가 본 200 의 비율**로 쟀다. 잰 값으로 보면
그 숫자는 Worker 가 아니라 ai-server 에서 엣지까지의 망을 쟀다.

| 레인 (11-09, 호스트 `*-pack-g1/latency.json`) | 클라이언트 가용성 | load 가용성 | 불일치 | 실패 분류 | Workers analytics |
|---|---|---|---|---|---|
| parcel-ownership g2 (11-07, 통과) | 1.0 | 1.0 | 0 | 503 1 | success |
| parcel-building-permits | 0.9998 | **0.99947** | 0 | `request-timeout` 9 | 전부 success |
| parcel-register-masters | 0.9998 | **0.999** | 0 | `request-timeout` 14 | 전부 success |
| parcel-energy | **0.9992** | **0.99927** | 0 | `request-timeout` 17, no-gzip 199/200 | 전부 success |

- 실패는 모두 15 s(`REQUEST_TIMEOUT`) 안에 **답을 하나도 받지 못한** 요청이다. 같은 창에서 Worker 가 가장 늦게 낸 답은 450 ms, analytics 는
  모든 invocation 을 `success` 로 적었다. 서버 p50 은 11-07 과 같고(78→91 ms) 클라이언트 p99 만 2.2 s → 4.6–5.3 s 로 늘었다.
- 11-09 측정: 우리 호스트 이름은 한국 클라이언트(ai-server 프로브 포함)를 Cloudflare **LAX** 에서 받는다. 사용자도 같은 길로 들어온다.
  꼬리는 태평양 횡단 경로다. 경로를 고치는 일은 W43 이 맡는다. 관문이 할 일은 그 망의 정체를 Worker 의 실패로 세지 않는 것이다.
- 한편 관문을 그냥 풀면(상한을 낮추면) Worker 가 정말 멈춰도 통과한다. 재시도(#1211)도 같다: 사용자가 보는 것에 가깝게 만들지만,
  "서버가 답했는가"와 "클라이언트 망이 답을 가져왔는가"를 여전히 한 숫자에 섞는다.
- `latency_tests.rs` 가 이미 밝힌 사실: Worker 가 스스로 낸 503 도 analytics 에서는 `success` invocation 이다. analytics 만으로는 서버 가용성이 아니다.

대기업·오픈소스가 가용성을 재는 법:
- **Google SRE (SRE Workbook, "Implementing SLOs")**: 가용성 SLI 는 서버(로드 밸런서) 쪽 로그로 재고, 블랙박스 프로버는 따로 둔다.
  프로버의 망 실패는 SLI 가 아니라 프로버의 건강이다.
- **Cloudflare 자신(Workers analytics `status`)**: invocation 결과를 `success`·`clientDisconnected`·`exceededResources` 등으로 서버에서 남긴다.
- **Kayenta·Argo Rollouts 의 카나리 판정**: 서버 지표(오류율)와 클라이언트 지연을 별도 지표로 두고, 판정은 지표마다 한다.

## Decision

1. **서버 가용성이 `availability_min` 을 받는다.** 한 단계(짝 읽기의 팩 쪽, no-gzip 표본, load)의 서버 가용성은 둘 중 작은 값이다:
   - 클라이언트에 도착한 답 가운데 서버 실패가 아닌 비율: HTTP 상태(503 등), gzip 이 아닌 팩 답, 풀리지 않는 gzip 은 서버 실패다.
   - 그 창의 Workers analytics invocation 가운데 `success` 의 비율. `clientDisconnected` 도 실패로 센다: 미리보기는 프로브만 읽으므로,
     Worker 가 늦어 클라이언트가 끊은 것이면 여기에 남는다.
   analytics 가 없으면 0 이다(이전처럼 관문이 닫힌다). 판정은 `gate::served` 에서 하고, 증거에 이미 있는 `failures`·`worker_cpu.statuses` 로
   계산하므로 새 저장 필드가 없다.
2. **답 없이 잃은 읽기는 클라이언트 망이고, 따로 상한한다.** 분류 `request*`(시간 초과·리셋), `connect`, `body*`(멈추거나 끊긴 본문)는
   `gate::is_transport_class` 가 클라이언트 몫으로 가른다. 짝 읽기는 쪽마다, no-gzip·load 는 단계마다 그 비율이 계약의
   `slo.client_transport_failure_max_share`(0.01) 이하여야 한다. load 가 페이싱에서 버린(`shed`) 읽기도 클라이언트 몫이다. 그 위면 프로브가
   망가진 길을 잰 것이고 아무것도 증명하지 못한다. 0.01 은 11-09 최악(10,000 중 17, 0.17%)의 6배이고, 100 번에 한 번 답을 잃는 망은 여전히 거부한다.
3. **클라이언트 지연 SLO 는 그대로, 따로 보고한다.** cold/warm 지연 증가 상한(`latency_max_increase_ms`)과 팩 전용 레인의 절대 p95 상한은
   클라이언트 쪽 숫자이고 바뀌지 않는다. 클라이언트가 본 200 비율(`availability`)도 증거에 계속 남고, 프로브 로그는 서버 판정
   (`server`)·클라이언트 가용성·엣지를 나란히 찍는다.
4. **프로브는 사용자가 있는 곳에서 재고, 어디로 들어갔는지 남긴다.** 프로브는 사용자와 같은 한국(ai-server)에서 돈다. 증거의 새 필드
   `edge_colos` 는 팩 답을 낸 엣지 데이터센터를 `CF-Ray` 끝자리로 센다(11-09 이면 `LAX`). 지연 숫자가 어느 경로의 것인지 증거가 말한다.
5. **재시도하지 않는다.** 답 없는 요청을 다시 묻지 않는다(#1211 은 닫는다). 잃은 읽기는 2 의 상한에 그대로 세인다.

## Consequences

- 11-09 에 떨어진 세 레인과 그 뒤 레인(demolitions·sub-parcels·regulation-zones)은 다음 프로브에서 같은 망 정체로는 떨어지지 않는다.
  예전 증거 파일은 `passed: false` 로 남아 있으므로 발행은 새 프로브를 기다린다(publish 는 `verdict() && passed`).
- 엄격해진 곳: `clientDisconnected` 가 이제 서버 실패다. no-gzip 표본은 서버 실패가 0 이어야 하고(이전엔 200/200 이 아니면 실패),
  잃은 읽기는 상한 안이어야 한다.
- 시험: `tests.rs` 의 관문 시험이 세 경우를 잡는다 — 클라이언트 시간 초과 0.2% 는 통과, analytics 의 `clientDisconnected` 는 거부,
  상한을 넘은 `connect` 는 거부. load 의 서버 실패는 `http-503` 으로 거부.
- 이 결정은 컷오버 관문 (나)만 바꾼다. Worker 배포 카나리(`health.rs`)와 시간마다 도는 서빙 모니터(`monitor.rs`)는 클라이언트 실패를
  여전히 그대로 센다. 같은 정체가 그쪽을 깨면 같은 분류(`is_transport_class`)를 쓰는 후속 결정으로 옮긴다.
- 경로(LAX) 자체는 W43 이 고친다. 고친 뒤에는 `edge_colos` 가 그 결과를 보여 준다.

---

개정 각주(2026-11-09): §2 는 [ADR-0460](./0460-the-cutover-latency-gate-reports-the-clients-lost-reads-and-holds-the-servers-verdict-to-analytics-coverage.md) 이 대체했다 — 잃은 읽기는 보고만 하고, 서버 판정은 analytics 적용 범위(`analytics_min_coverage`)로 세운다.

개정 각주(2026-11-09): §3 의 "팩 전용 레인의 절대 p95 상한은 바뀌지 않는다" 는 [ADR-0463](./0463-a-pack-only-lanes-absolute-p95-bound-is-the-workers-own-time-not-the-clients.md) 이 대체했다 — 그 상한은 Worker 자신의 p95(Server-Timing) 에 건다.
