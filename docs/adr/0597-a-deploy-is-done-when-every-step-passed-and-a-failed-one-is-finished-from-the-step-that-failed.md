# ADR 0597: 배포는 모든 단계가 통과해야 끝난 것이고, 실패한 배포는 다음 tick 이 실패한 단계부터 정해진 횟수만큼 마저 한다

- Status: Accepted
- Date: 2026-11-21
- Amends: [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)("실패한 커밋은 다시 배포하지 않는다, 더 새 커밋이 지나가는 길"),
  [ADR-0173](./0173-a-failed-post-deploy-run-does-not-pause-the-schedule.md)(전환 중 멈춘 배포는 DAG 를 멈춘 채 둔다)
- Builds on: ADR-0308(배포가 잡는 작업만 멈춤), ADR-0441(운영자 유닛을 기다리는 종료 75), ADR-0245(사슬이 끝나야 API 교체)

이 글의 낱말: **tick** 은 `foundation-autodeploy.timer` 가 1분마다 돌리는 `foundation-autodeploy.sh` 한 번, **릴리스 링크**는
`/opt/foundation-platform/current`, **잡힌 DAG** 는 배포가 0단계에서 멈추는 작업(ADR-0308)이다. **진행 기록**은
`/var/lib/perfectory/deploy-progress/<commit>` 이다.

## Context

**2026-11-20 의 두 실패(ai-server 읽기 전용 측정).**

- 09:52Z, 9fe6525d: staging smoke 의 database 단계가 300 s 상한을 넘었다. 이 단계는 매번 같은 일이다(빈 `foundation_staging` 에
  스키마 사슬 전부). 11-19..20 의 27 번 중 17 s 부터 300 s 넘게까지 흔들렸고, 3 번이 상한을 넘었다(11-19 21:27Z, 11-20 04:44Z,
  09:52Z). 각 실패 뒤 다음 실행은 통과했다(21:58Z, 06:03Z, 10:17Z). 전환 전 실패라 DAG 는 다시 풀렸다.
- 10:28Z, 46ad8d1e: 4단계 `foundation-release.sh migrate` 안에서 Postgres 사슬(bootstrap·migrate·grants·finalize)은 두 번 모두 0 으로
  끝났다. 그다음 `up -d --wait foundation-api` 가 1 로 끝났다. API 의 첫 연결이 요청 예산(`acquire_timeout_ms` 500 ms) 안에 Postgres 에
  닿지 못해 프로세스가 10:28:28 에 끝났고, Postgres 로그에는 10:28:57 까지 `foundation_api` 의 연결 시도가 하나도 없다.
- 공통 원인: Postgres 의 데이터 디렉터리와 `parcel_bulk` 표공간이 회전 디스크 `sda`(ROTA=1)에 있고, Spark·OCR·docker 의 쓰기도 같은
  디스크에 있다. I/O pressure `full avg300` 47.5%, 10:28Z iowait 7.1 코어(바쁜 CPU 8.7/20)였다. 연결 수는 문제가 아니다(100 중 5).

**뿌리 버그.** 10:28Z 실패 때 릴리스 링크는 이미 46ad8d1e 를 가리켰다(4단계 `activate`). 다음 tick 들은 "호스트가 main 을 돈다"고 보고
아무것도 하지 않았다. 배포는 "THE HELD DAGS STAY PAUSED" 를 남겼고, 잡힌 DAG 13 개는 다음 커밋이 배포될 때까지 멈춰 있었다.
`failed/<commit>` 은 "다시 하지 않는다"는 뜻이었으므로 같은 커밋으로는 고칠 길이 없었다. 또 lakehouse 마이그레이션 실패는 종료 75 였고,
autodeploy 는 75 를 "운영자 유닛을 기다림"으로 읽어 실패를 1분마다 끝없이 다시 했다.

## Decision

1. **끝남의 기준.** 배포가 끝난 것은 `foundation-deploy.sh` 의 모든 단계가 통과해 `deployed/<commit>` 이 쓰였을 때다. 릴리스 링크가
   움직인 것은 끝남이 아니다. 링크가 가리키는 커밋에 `failed/` 는 있고 `deployed/` 가 없으면, tick 은 그 배포를 먼저 마저 한다.
2. **정해진 횟수의 재시도.** 실패한 시도마다 `failed/<commit>` 에 한 줄을 더한다. 줄 수가 `deploy_attempts_per_commit`(3)보다 적고
   마지막 시도에서 `retry_after_minutes`(10)가 지났으면, 그 커밋은 다시 후보다. 다 쓰면 예전처럼 더 새 커밋을 기다린다. 숫자의 근거는
   `config/autodeploy.contract.json` `retry_reason` 에 있다. 실패율 약 0.11(27 번 중 3 번, 측정)이면 세 번 모두 실패할 확률은 약
   0.0014 다(독립 가정). 10 분은 측정이 아닌 결정이다.
3. **실패한 단계부터.** `foundation-deploy.sh` 는 통과한 단계(2 prepare, 3 floor, 3b smoke, 4 activate·migrate·timers, 5, 7, 8, 9, 10)를
   진행 기록에 `done <step>` 으로 적고, 첫 줄에 바꾸는 릴리스 `old <commit>` 을 적는다. 같은 커밋을 다시 돌리면 통과한 단계는 건너뛴다.
   잡을 작업은 링크가 아닌 기록된 `old` 와 비교해 다시 계산한다. 그래서 링크가 이미 새 릴리스를 가리켜도 첫 시도와 같은 작업을 잡는다.
   7단계가 통과한 뒤의 실패(8–10)는 다시 돌릴 때 아무것도 멈추지 않는다. 끝나면 기록을 지운다.
4. **모든 종료 경로에서 일시정지를 푼다. 예외는 하나뿐이다.** 전환 중(4–5단계) 실패도 전환 전 실패처럼 켜진 DAG 를 모두 다시 푼다.
   예외는 `foundation-release.sh migrate` 가 77(`schema_not_applied`, `deploy-start-once.sh` 의 `SCHEMA_NOT_APPLIED`)로 끝난 때다. Postgres
   사슬, 스키마 확인, lakehouse 마이그레이션 자체가 실패한 경우다. 그때만 반쯤 적용된 스키마에 작업이 쓸 수 있으므로 잡힌 DAG 는
   재시도까지 멈춘 채 두고 대문자 줄로 말한다. lakehouse 실패는 75 를 더는 쓰지 않는다.
5. **API 시작은 요청이 아니다.** `foundation-api` 는 첫 연결을 `startup_timeout_ms`(90,000 ms,
   `FOUNDATION_PLATFORM_DB_STARTUP_TIMEOUT_MS`) 안에서 기다린다. 시간 초과와 I/O 오류만 250 ms 간격으로 다시 묻고, 그 밖의 오류(암호 거부,
   없는 DB)는 곧바로 끝낸다. 측정: 10:28Z 첫 연결은 컨테이너 첫 시작 36 s 뒤에 닿았다. compose 의 health check 창은 100 s(10 s × 10 번)이다.
   요청 예산 500 ms 는 그대로 둔다.

## 하지 않은 것

- **사슬 두 번 실행은 둔다.** `runtime_chain_before` 가 one-shot 사슬을 끝까지 돌린 뒤, `up -d --wait foundation-api` 가 compose 의 순서대로
  끝난 one-shot 을 다시 돌린다(10:20Z bootstrap 24 s, 10:24Z 2분 16 s). 두 번째를 없애려면 `--no-deps` 가 필요하다. 그런데 그것은
  `tools/conftest/compose/chain.rego` 가 막는다(2026-09-02: 사슬을 손으로 쓰다 grants 가 안 돌아 카탈로그가 하루 500). 마이그레이션은
  이미 적용된 것을 건너뛰므로 두 번째는 같은 결과이고, 디스크가 막힐 때만 느리다.
- **상한을 올리지 않는다.** smoke 의 300 s 와 health check 창은 그대로다. 흔들림의 원인은 디스크다. pg_default·WAL(0.8 GB)을 SSD 로 옮기는
  일은 사장이 승인했고(11-20), 별도 PR 에서 배포 경로로 한다.

## Consequences

- 10:28Z 같은 실패는 DAG 를 곧바로 다시 풀고, 10 분 뒤 4단계부터 마저 한다. prepare·floor·smoke 는 다시 하지 않는다(smoke 만 11-20 측정 16–19 분).
- 같은 커밋이 세 번까지 실패하면 Slack 실패 알림도 세 번까지 온다(시도마다 `OnFailure` 한 번).
- 77 로 실패한 마이그레이션은 재시도 동안 잡힌 DAG 만 멈춘다. 세 번 모두 실패하면 지금처럼 멈춘 채 남고, `FoundationDeploy*` 경보가
  멈춤의 길이를 말한다.
- 진행 기록은 root 만 쓰는 고정 경로이고, 다른 커밋의 기록은 다음 배포가 지운다.
