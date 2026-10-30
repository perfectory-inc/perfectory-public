# ADR 0363: 배포는 릴리스를 판정한다 — 공급자의 상태도, 아카이브의 바이트도 아니다

- Status: Accepted
- Date: 2026-10-30
- Amends: [ADR-0177](./0177-a-new-release-runs-a-staging-smoke-before-production-switches.md) §3("제공자가 아무것도
  내놓지 않으면 실패다" — 답한 제공자에만 적용한다)
- Builds on: [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(`waiting <이유>`),
  [ADR-0208](./0208-the-data-flow-maps-problems-reach-slack-through-the-existing-alertmanager.md)(기존 Alertmanager 경로)

## Context

2026-10-30 13:07Z, 59c0b68 배포가 단계 3b 스테이징 스모크에서 멈췄다.

```text
staging-smoke: ingest FAILED … failed to acquire VWorld dataset login session …
VWorld dataset login failed result=useChk message=이용약관 변경안내 페이지로 이동합니다.
```

VWorld 가 이용약관을 바꿨고, 수집 계정이 웹사이트에서 새 약관에 동의하기 전까지 로그인은 `useChk` 로 거부된다.
이것은 소유자만 할 수 있는 일이다. 그런데 결과는 이랬다.

| 무엇 | 실제 |
|---|---|
| 배포 | 모든 커밋이 같은 자리에서 실패. 자동 배포는 더 새 커밋을 기다릴 뿐, 어떤 릴리스도 이 상태를 고칠 수 없다 |
| 수집 | `source_sweep` 의 VWorld 레인과 `vworld_parcel_edition` 이 "login session" 오류로만 실패 |
| 알림 | "무엇을 누가 하면 풀리는가" 를 말하는 신호가 없었다 |

원인은 하나다: 스모크와 작업이 **릴리스가 만든 실패**와 **공급자 쪽 실패**를 구별하지 못했다. 구별할 재료는 이미
있었다. 수집기의 `request_resilience` 는 시도마다 `Retryable`/`Fatal` 을 가르고, 재시도를 다 쓴 실패는 공급자가
응답하지 않은 것이다. 그러나 그 구별이 `OutboundHttpError` 의 문자열로 납작해져 위로 올라가지 않았다.

## Decision

1. **누구 쪽인지는 오류의 타입이다.** `OutboundHttpError` 에 `OutboundFailureClass` 를 단다.
   - `Fatal`(기본): 우리 요청·우리 해석이 틀렸다. 릴리스가 고친다.
   - `ProviderUnavailable`: 재시도 가능한 실패(전송 오류, 408·429·5xx)가 정책의 시도를 다 썼다. 실행기
     (`execute_retryable`·`execute_single`)가 붙인다. 로그인이 JSON 대신 HTML 페이지(점검·공지)를 내면 같다.
   - `ProviderAccountActionRequired { provider, reason }`: 공급자가 결과 코드를 붙여 계정을 거부했다(`useChk`).
     `provider` 는 엔드포인트 카탈로그의 공급자 값(`vworld.kr`)이다. 결과 코드가 아예 없는 로그인 응답은 우리가 더는
     못 읽는 프로토콜이므로 `Fatal` 로 남는다.

   수집 경계에서 이것은 `CollectionError::ProviderUnavailable`·`ProviderAccountActionRequired` 가 된다
   (`into_collection_error`). AWS SDK 의 재시도 분류기, gRPC 의 `UNAVAILABLE` 과 `FAILED_PRECONDITION` 이 같은 선을 긋는다.
2. **발행기는 그 타입을 종료 코드와 한 줄로 말한다.** 오류 사슬에 공급자 쪽 오류가 있으면
   `foundation-outbox-publisher` 는 `foundation-provider-outcome {json}` 한 줄을 찍고 75(`EX_TEMPFAIL`, 공급자 응답 없음)
   또는 77(`EX_NOPERM`, 계정 조치)로 끝난다(`foundation_publisher_core::provider_outcome`, `refusal` 의 65 와 같은
   방식). 성공한 로그인은 `{"outcome":"ok","provider":…}` 를 찍는다. 그 밖의 실패는 1 그대로다 — 표시되지 않은
   실패는 릴리스의 것으로 본다.
3. **스모크는 릴리스를 판정한다.** `staging-smoke.sh` 의 공급자 입력(허브 계획, VWorld 계획·인벤토리, 표본 수집)이
   75·77 로 끝나면 그 입력과 그것에 기대는 단계(표본 → 측정)를 건너뛰고 `skipped-inputs.tsv` 에 결과 줄과 함께 적는다.
   그 밖의 종료는 지금처럼 릴리스를 거부한다. 답한 공급자가 아무것도 내놓지 않은 것도 여전히 실패다.
   - **모든 공급자 입력이 공급자 쪽이면 "inconclusive" 로 통과한다.** 이유: 릴리스 자신의 검사(이미지, 빈 DB 에서
     스키마 전체)는 통과했고, 공급자가 돌아올 때까지 모든 릴리스를 막으면 고치는 릴리스(공급자 문제를 다루는 것 포함)도
     막힌다. 빠진 검사는 운영의 같은 레인이 대신한다: 스모크와 매일 훑기는 `vworld-sweep-lane.sh` 한 벌의 설정을 쓰므로,
     공급자 복구 뒤 첫 `source_sweep` 실행이 바로 그 검사이고, 실패하면 ADR-0260·0208 경로로 빨강·알림이 된다.
     복구를 감지해 스모크를 다시 돌리는 장치는 만들지 않는다(같은 검사를 두 번 할 뿐이다).
   - 건너뛴 입력은 `FoundationStagingSmokeInputSkipped` 로 알린다(24시간 유지, 모두 검사한 다음 스모크가 닫는다).
4. **계정 거부는 소유자를 부른다, 공급자마다 한 번.** `config/provider-accounts.contract.json` 이 로그인하는 공급자와
   결과 코드별 소유자 조치를 가진다(`useChk`: "log into vworld.kr with the collection account and accept the new
   terms"). `scripts/ops/provider_alert.py accounts` 가 실행 로그의 결과 줄을 읽어 Alertmanager 에
   `FoundationProviderAccountActionRequired{provider}` 를 올린다(26시간 유지, 로그인이 성공하면 바로 닫힘;
   `service="provider-account"` 경로, 하루 한 번 상기). 경로는 `notify-unit-failure.sh` 가 쓰는 그 API 다 — 새
   전송·새 저장소는 없다. 로그인하는 모든 작업(스테이징 스모크, `source_sweep`, `vworld_parcel_edition`)이 부르므로
   작업마다가 아니라 원인마다 한 번 온다.
5. **작업은 이유를 말하고 기다린다.** VWorld 레인이 77 이고 알림이 올라갔으면 `source_sweep` 과
   `vworld_parcel_edition` 은 `foundation-job-outcome waiting provider account action required — <조치>` 로 끝난다
   (ADR-0260: 기다림은 빨강이 아니다; 알림은 4 가 이미 보냈다). 같은 실행에 새 파일이 왔으면 `changed` 가 이긴다.
   알림이 올라가지 않으면 대기는 조용해지므로 지금처럼 실패한다.
6. **새 로그인은 계약에 들어와야 한다.** `orchestration/tests/test_provider_alert.py` 는 계정 거부를 내는 Rust 파일이
   `ACCOUNT_PROVIDER` 로 계약의 공급자를 이름 짓지 않으면 실패한다. 막는 사고: 이유 없는 로그인 실패(2026-10-30).

7. **같은 커밋의 재시도는 같은 릴리스다.** 약관 동의 뒤 소유자가 `failed/59c0b68…` 를 지워 다시 시도하자 단계 2 가
   `release id already exists with a different archive` (65)로 멈췄다. 첫 시도가 이미 릴리스를 준비했고, 두 번째
   아카이브는 바이트가 달랐다. 원인(로컬 실측): `git archive <sha>:<하위 트리>` 는 커밋이 아니라 트리를 받으면 모든
   항목의 mtime 을 **지금 시각**으로 찍는다 — 2초 간격 두 번의 sha256 이 달랐다.
   - 아카이브는 결정적이다: `foundation-deploy.sh` 는 `--mtime=@<커밋 시각>` 을 준다(같은 커밋 → 같은 바이트, 실측
     일치). 순서는 git 의 트리 순서이고 git 의 gzip 은 이름·시각을 쓰지 않는다(`gzip -n`). 서버 git 2.43 이 `--mtime`
     을 가진다.
   - 릴리스의 정체는 내용이다: 이미 설치된 id 에 다른 바이트의 아카이브가 오면 `foundation-release.sh` 는 거부 전에
     admission `check-archive` 로 그 아카이브의 파일·모드·바이트가 정본 커밋의 것인지 본다. 같으면 받아들인다(설치된
     바이트는 바로 앞의 `seal`·`verify` 가 이미 정본과 대조했다). 다르면 지금처럼 65 로 거부한다. 재현 가능한 빌드가
     산출물을 바이트가 아니라 입력·내용으로 식별하는 것과 같다.
   - 준비만 되고 활성화되지 않은 릴리스를 지우고 다시 준비하는 길은 만들지 않는다. 그 릴리스는 이미 봉인(chattr +i)돼
     있고, 내용이 다른 릴리스는 `seal`/`verify` 가 먼저 거부하므로 남을 수 없다. 남는 차이는 아카이브 바이트뿐이고,
     위 내용 대조가 그것을 푼다. 지우기는 데이터를 쌓기만 한다는 원칙과도 어긋난다.
   - 소유자의 재시도는 `failed/<sha>` 하나를 지우는 것으로 끝난다. 단, 배포는 그 커밋의 제어 체크아웃으로 돌므로 이
     ADR 이전 커밋(59c0b68 포함)의 재시도에는 적용되지 않는다 — 그 커밋은 이 고침을 담은 다음 커밋이 대신 배포한다.

## Rejected

- **스모크의 VWorld 수집 단계를 끄기**(`staging-gate.off`): 릴리스의 실제 회귀까지 같이 놓친다. 끄는 것은 사람이
  고르는 비상 스위치로 남긴다.
- **오류 문자열(`useChk`, `HTTP 503`)을 셸에서 grep**: 문구가 바뀌면 조용히 틀린다. 분류는 오류가 생긴 자리에서
  타입으로 한다.
- **계정 거부를 실패(`failed`)로 두고 작업 알림에 맡기기**: 같은 원인이 작업 수만큼 알림이 되고, 그 문구는 "로그인
  실패" 이지 "약관에 동의하세요" 가 아니다.
- **모두 건너뛴 스모크는 배포를 막기**: 어떤 릴리스도 풀 수 없는 상태에 배포를 묶는다 — 이번 사고 그대로다.

## Consequences

- 약관이 동의되지 않은 동안 다음 배포: 스모크의 허브·VWorld 계획·인벤토리는 그대로 검사되고, 표본 수집(로그인)과
  측정만 건너뛴 채 "passed … with provider inputs skipped" 로 전환까지 간다. 소유자에게는 계정 알림 하나와 스모크
  알림 하나가 간다. 그동안 스테이징의 멀티파트 경로(ADR-0177 의 10-10 패닉 경로)는 검사되지 않는다.
- RAON 대용량 레인(`raon-large-files.sh`)은 컨테이너 안 RAON 에이전트가 로그인하므로 이 분류 밖이다. 실패는 지금처럼
  레인 실패로 보인다.
- 공급자 응답 없음(75)은 수집 작업에서 지금처럼 실패다(다음 실행이 다시 받는다). 바뀐 것은 스모크가 그것으로
  릴리스를 거부하지 않는다는 것뿐이다.
