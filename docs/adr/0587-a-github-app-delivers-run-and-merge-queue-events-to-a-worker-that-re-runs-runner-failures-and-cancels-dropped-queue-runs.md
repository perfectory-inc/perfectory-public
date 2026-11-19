# ADR 0587: GitHub App 이 실행·병합 큐 사건을 Worker 로 보내고, Worker 가 러너 실패를 다시 돌리고 버려진 큐 실행을 취소한다

- Status: Proposed
- Date: 2026-11-19
- Amends when implemented (지연만 바꾼다. 일정 작업은 남는다): [ADR-0421](./0421-a-run-that-failed-only-because-its-runner-did-is-re-run-once-and-the-merge-queue-keeps-building-two.md)(러너 실패 재실행을 일정 작업이 한다),
  [ADR-0553](./0553-a-fix-for-main-or-the-deploy-goes-to-the-front-of-the-queue-without-emptying-it-and-a-run-the-queue-dropped-is-cancelled.md) 결정 3·4(버려진 큐 실행 취소를 같은 일정 작업이 5분마다 한다),
  [ADR-0554](./0554-main-says-when-it-is-red-from-the-runs-github-already-made.md)(main 빨간불을 10분마다 본다)
- Builds on: [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(런타임 비밀 계약),
  [ADR-0175](./0175-workers-deploy-themselves-after-the-host-deploys-main.md)(Worker 는 호스트 배포 뒤 단계가 올린다)

## Context

세 가지 일이 GitHub Actions 일정 작업에 있다: 러너 때문에만 실패한 실행을 한 번 다시 돌리기(ADR-0421), 큐가 버린 커밋의
큐 실행 취소(ADR-0553), main 빨간불 이슈(ADR-0554). 일정은 `*/5` 와 `*/10` 이다.

**GitHub 는 이 저장소에서 그 일정을 하루 세 번쯤만 낸다**(실측, Actions API, 2026-11-15~19).

| 워크플로 | 적힌 일정 | 실제 실행 (11-15~19) |
|---|---|---|
| `rerun-runner-failures.yml` | 5분마다 (하루 288번) | 약 8시간마다: 11-18 은 02:37Z, 10:42Z, 19:06Z |
| `main-health.yml` | 10분마다 (하루 144번) | 약 8시간마다: 11-18 은 02:20Z, 10:26Z, 18:56Z |

그래서 "10분 안에 다시 돈다", "버려진 큐 실행의 러너를 5분 안에 돌려받는다", "main 이 빨개지면 10분 안에 이슈" 는
지금 최대 8시간 뒤다. 2026-11-18 하루에 실패한 실행은 140개, 병합 큐 실행은 432개였다. GitHub 문서도 일정 사건이 부하가
높을 때 늦어지거나 버려진다고 적는다([schedule](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)).

사건으로 바꾸는 길 가운데 워크플로 트리거(`workflow_run`, `pull_request: dequeued`)는 쓰지 않는다. zizmor 가
`workflow_run` 을 "근본적으로 위험한 트리거" 로 막고(설정으로 끌 수 없다), 워크플로 정책(Rego)이 `pull_request` 트리거의
모양을 고정한다. 공개 저장소에서 그 두 검사를 풀지 않는다(사장 결정 2026-11-19). 호스트 타이머도 쓰지 않는다.

상류 문서(2026-11-19 에 읽음):

- GitHub [Webhook events and payloads — `merge_group`](https://docs.github.com/en/webhooks/webhook-events-and-payloads#merge_group):
  동작은 `checks_requested` 와 `destroyed`. `destroyed` 는 `reason` 을 싣는다: `merged`, `dequeued`, `invalidated`(앞
  항목이 빠져 무효). 문서가 직접 "그 head SHA 에서 도는 검사를 취소하고 싶을 것" 이라고 적는다. GitHub App 만 받을 수
  있고 저장소 권한 "Merge queues" 읽기가 필요하다. 워크플로의 `merge_group` 트리거는 `checks_requested` 만 받는다.
- GitHub [Validating webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries):
  `X-Hub-Signature-256` 은 본문의 HMAC-SHA256 16진수다. `==` 로 비교하지 말고 시간이 일정한 비교를 쓴다.
- GitHub [Best practices for using webhooks](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks):
  10초 안에 2XX 로 답한다. `X-GitHub-Delivery` 는 전달마다 고유하고, 다시 보낸 전달은 같은 값을 갖는다.
- GitHub [Generating an installation access token](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app):
  토큰은 1시간 뒤 만료한다. 요청 본문의 `repositories`·`permissions` 로 그 토큰의 저장소와 권한을 App 이 가진 것보다
  좁힐 수 있다.
- GitHub [Permissions required for GitHub Apps](https://docs.github.com/en/rest/authentication/permissions-required-for-github-apps):
  `POST …/actions/runs/{id}/cancel`, `POST …/actions/runs/{id}/rerun-failed-jobs`, 워크플로 dispatch 는 Actions 쓰기,
  실행·작업 읽기는 Actions 읽기.
- Cloudflare [Workers secrets](https://developers.cloudflare.com/workers/configuration/secrets/): 비밀은 Worker 마다 암호화된
  바인딩이고 `wrangler secret put` 으로 넣는다. [Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/):
  `importKey`, HMAC, RSASSA-PKCS1-v1_5, `timingSafeEqual` 을 지원한다.

업계: Kubernetes 의 Prow(Tide), Rust 의 bors, Mergify 는 모두 GitHub App 이 웹훅을 받아 API 를 부르는 서비스다. CI 안의
폴링 작업이 아니다.

## Decision

1. **GitHub App 하나가 두 사건을 Worker 하나로 보낸다.** App 은 `perfectory-inc/perfectory-public` 에만 설치한다. Worker 는
   `platforms/foundation-platform/services/foundation-ci-events-gateway`(다른 Worker 와 같은 배포: ADR-0175).

2. **사건과 하는 일.**

   | 사건 | 조건 | 하는 일 |
   |---|---|---|
   | `merge_group` `destroyed` | `reason` 이 `dequeued` 또는 `invalidated`, **그리고 그 `head_sha` 가 main 에 없다** | 그 `head_sha` 의 `merge_group` 실행 가운데 대기·진행 중인 것을 취소한다 |
   | `merge_group` `destroyed` | `reason` 이 `merged`, 또는 `head_sha` 가 main 에 있다 | 아무것도 하지 않는다. 호스트가 병합된 커밋의 큐 검사로 배포한다(ADR-0167) |
   | `workflow_run` `completed` | `conclusion` 이 `failure`, 사건이 `merge_group` 이 아님, `run_attempt` 가 1, 실패한 작업 전부에 실패한 단계가 없음 | `rerun-failed-jobs` 를 한 번 부른다 |
   | `workflow_run` `completed` | 판정에 쓰이는 워크플로(`main_health.py` 가 보는 것, `main-health.yml` 자신과 일정 전용 워크플로는 아님)의 main 실행 또는 `merge_group` 실행이 끝남 | `main-health.yml` 을 `workflow_dispatch` 로 부른다 |

   `workflow_run` 은 `requested`·`in_progress` 로도 온다. `completed` 만 본다.

   취소와 재실행의 조건은 지금 `cancel_superseded_queue_runs.py`·`rerun_runner_failures.py` 와 같다. "main 에 없다" 는
   그 스크립트의 `on_main` 과 같은 `GET /compare/main...<sha>` 다: `reason` 만 믿지 않는다. 한 번에 여럿을 병합하는
   큐(`max_entries_to_merge: 5`)에서 무효가 된 묶음의 SHA 가 뒤에 main 에 오를 수 있는지는 문서가 말하지 않고, 병합된
   커밋의 큐 실행을 잘못 취소하면 배포가 선다(ADR-0167).

   main 빨간불의 판정과 이슈 쓰기는 옮기지 않는다: `scripts/ci/main_health.py` 가 그대로 하고, Worker 는 그것을 제때
   부르기만 한다. `main-health.yml` 자신의 완료는 부르지 않으므로 고리가 없다. 한꺼번에 여러 실행이 끝나도
   `main-health.yml` 의 `concurrency` 묶음이 도는 것 하나와 기다리는 것 하나만 남긴다(GitHub 가 한다. Worker 의 기억에
   기대지 않는다).

3. **권한은 가장 좁게.** 이 Worker 가 쓰는 것: Actions 읽기·쓰기, Contents 읽기(위 compare), Merge queues 읽기, Metadata
   읽기. 같은 App 을 감사 가져오기(ADR-0569, W88b)가 함께 쓰므로 App 의 권한은 합집합이고, 이 Worker 는 토큰을 받을
   때마다 `permissions` 를 `{"actions": "write", "contents": "read"}` 로, `repository_ids` 를 이 저장소 하나로 좁힌다.
   App 에는 Contents 쓰기도, Pull requests 쓰기도, Administration 쓰기도 없다.

   **좁히기는 키를 지키지 않는다.** 토큰을 좁히는 것은 키를 가진 쪽의 선택이다. 키가 새면 App 의 권한 전부(Actions
   쓰기 포함)가 샌다. App 하나를 두 소비자가 쓰면 감사 가져오기가 필요 없는 Actions 쓰기를 쥔 키를 갖게 된다. App 은
   개인 키를 여러 개 가질 수 있으므로 소비자마다 자기 키를 받아 따로 돌려쓴다. 권한까지 가르려면 App 을 둘로 나눠야
   한다: 사장 결정은 App 하나이므로 그대로 두되, 이 위험을 여기 적는다.

   | 저장소 권한 | 수준 | 누가 쓰나 |
   |---|---|---|
   | Actions | 읽기·쓰기 | 이 Worker(취소·재실행·dispatch) |
   | Merge queues | 읽기 | 이 Worker(`merge_group` 사건 구독) |
   | Checks | 읽기 | W88b |
   | Contents | 읽기 | 이 Worker(compare), W88b |
   | Pull requests | 읽기 | W88b |
   | Administration | 읽기 | W88b |
   | Metadata | 읽기 | 필수 |

   구독 사건: `workflow_run`, `merge_group`. W88b 는 웹훅이 아니라 API 로 읽으므로 사건을 더하지 않는다(W88b 와 확인할 것).

4. **서명 확인이 먼저다.** Worker 는 본문을 바이트로 읽고 `X-Hub-Signature-256` 을 `crypto.subtle.verify`(HMAC-SHA256,
   시간이 일정한 비교)로 확인한 뒤에만 JSON 으로 읽는다. 서명이 없거나 틀리면 401. `X-GitHub-Event` 가 위 둘이 아니면
   204. `repository.id` 가 이 저장소가 아니면 204. 사건의 문자열(가지 이름·제목)은 어떤 명령에도 넣지 않는다: 쓰는 값은
   숫자 id 와 SHA 뿐이고 SHA 는 16진수 40자로 확인한다.

5. **10초 안에, 한 일의 결과로 답한다.** API 호출을 답하기 전에 끝낸다(전체 8초 한도). 성공하면 200, 할 일이 없으면
   204, API 가 실패하거나 한도를 넘기면 500 이다. 그래야 실패가 GitHub 의 전달 기록에 남고 다시 보낼 수 있다. 답한
   뒤에 일하면(`waitUntil`) 실패가 어디에도 남지 않는다.

6. **두 번 와도 같다.** 저장소 없이 멱등하다: 이미 끝났거나 취소된 실행의 취소는 409 로 끝나고 무시한다. 재실행은
   `run_attempt == 1` 일 때만이고, 부르기 직전에 실행을 다시 읽어 여전히 1 이고 `completed` 인지 본다. 다시 온 전달은
   2 를 보고 멈춘다. 그래서 실행 하나는 누가 몇 번 보내도 한 번만 다시 돈다(fork PR 도 같다. 지금 일정 작업과 같은
   상한이다).

7. **API 한도.** 설치 토큰은 만료 5분 전까지 Worker 메모리에 둔다(없으면 다시 받는다: 기억은 최적화일 뿐이다).
   재실행은 사건당 최대 4번, 취소는 compare 1번 + 목록 1번 + 실행당 1번이다(커밋 하나에 워크플로 약 12개). 11-18 의
   양(실패 140, 큐 실행 432 = 큐 커밋 약 36개)이면 하루 수천 번이고 설치 토큰의 시간당 한도(5,000) 아래다.

8. **비밀은 Worker 비밀 둘이고, 넣는 단계는 새로 만든다.** `GITHUB_APP_PRIVATE_KEY`(PKCS#8 PEM: GitHub 가 주는 PKCS#1 을
   `openssl pkcs8 -topk8 -nocrypt` 로 바꾼 것, Web Crypto 가 PKCS#8 을 읽는다)와 `GITHUB_WEBHOOK_SECRET`. 지금 배포 뒤
   단계는 Worker 비밀을 넣지 않는다(`foundation-map-edit-gateway` 의 README 가 손으로 넣으라고 적는다). 구현 PR 이
   호스트 환경 파일 계약(ADR-0153)에 두 이름을 더하고, 배포 단계가 **첫 배포 전에** `wrangler secret put` 으로 넣게
   한다(값은 표준 입력으로, 로그에 남기지 않는다). 키를 돌려쓰는 순서도 그 PR 이 적는다: 새 키를 App 에 더하고,
   Worker 에 넣고, 옛 키를 App 에서 지운다. webhook secret 은 App 과 Worker 를 같은 배포에서 바꾼다. App id 와 설치
   id 는 비밀이 아니므로 `wrangler.jsonc` 의 `vars` 다.

9. **시험은 기록한 본문으로.** 실제 전달에서 받은 본문을 식별자만 합성값으로 바꿔 고정물로 둔다: `merge_group.destroyed`
   세 가지 `reason`, `workflow_run.completed` 의 러너 실패·단계 실패·두 번째 시도·`merge_group` 사건, 서명 없음·틀린
   서명·다른 저장소. 판단은 순수 함수이고 `fetch` 는 주입한다. 플러그인 없는 Vitest 로 돈다.

10. **일정 작업은 지우지 않는다. 맞추는 역할로 남는다.** GitHub 는 실패한 전달을 스스로 다시 보내지 않고, Worker 가
    죽거나 잘못 배포되거나 App 이 지워지거나 비밀이 어긋나면 그 사이의 사건은 사라진다. 실행이 생기기 전에 온
    `destroyed` 도 취소할 것이 없다. `rerun-runner-failures.yml`(하루 약 세 번 실제로 돈다)이 그 빈틈을 줍는다. 둘이 같은
    실행을 보아도 안전하다(409, `run_attempt`). Worker 는 첫날부터 실제로 행동한다. 무엇을 했는지는 Worker 의 로그 한
    줄(전달 id, 사건, 판단, 부른 API)로 남기고, 일주일 뒤 그 로그와 일정 작업의 로그를 대조한다.

## 소유자가 App 을 만드는 순서

App 은 소유자가 만든다. 에이전트는 만들지 않는다. W88b 의 권한이 위 표와 같은지 확인된 뒤에 한 번만 한다.

1. GitHub → 조직 `perfectory-inc` → Settings → Developer settings → GitHub Apps → **New GitHub App**.
2. **GitHub App name**: `perfectory-ci-events`. **Homepage URL**: 저장소 주소.
3. **Webhook**: Active 를 켠다. **Webhook URL**: Worker 주소(구현 PR 이 알려 준다. 그 전에는 Active 를 끄고 만든 뒤 나중에
   켠다). **Webhook secret**: 터미널에서 `openssl rand -hex 32` 로 만든 값. 이 값을 적어 둔다(8번에서 쓴다).
4. **Repository permissions**: Actions = Read and write, Merge queues = Read-only, Checks = Read-only, Contents = Read-only,
   Pull requests = Read-only, Administration = Read-only. Metadata 는 자동으로 Read-only. 나머지는 전부 No access.
   Organization permissions 와 Account permissions 는 전부 No access.
5. **Subscribe to events**: Workflow run, Merge group 둘만 체크한다.
6. **Where can this GitHub App be installed?**: Only on this account. **Create GitHub App**.
7. 만든 App 화면에서 **App ID** 를 적는다. **Generate a private key** 를 눌러 `.pem` 파일을 받는다.
8. 왼쪽 **Install App** → `perfectory-inc` → **Only select repositories** → `perfectory-public` → Install. 설치 뒤 주소
   끝의 숫자(`…/installations/<숫자>`)가 설치 id 다.
9. 호스트의 환경 파일에 넣는다(이름은 구현 PR 의 계약이 정한다): 3번의 webhook secret, 7번 `.pem` 을
   `openssl pkcs8 -topk8 -nocrypt -in <받은 파일> -out app.pkcs8.pem` 로 바꾼 내용. App ID 와 설치 id 는 구현 PR 에 알려
   준다(비밀이 아니다). 받은 `.pem` 원본과 바꾼 `app.pkcs8.pem` 은 넣은 뒤 둘 다 지운다.

## Consequences

- 버려진 큐 실행은 GitHub 가 알리는 순간 취소된다. ADR-0553 이 계산한 돌려받는 러너 시간(3.3일에 약 2,400 작업·분)은
  그때 처음 실제로 잴 수 있다. 러너 대기가 긴 지금(작업 시간의 74% 가 대기) 가장 직접적인 이득이다.
- 큐에서 러너 때문에만 실패한 항목은 여전히 다시 돌리지 않는다. 항목이 이미 큐에서 빠졌기 때문이고, 다시 넣는 것은
  작성자다(ADR-0585).
- 새로 생기는 것: App 하나, Worker 하나, 비밀 둘, 비밀을 넣는 배포 단계. 취소·재실행의 조건이 Python 과 TypeScript 두
  곳에 있게 된다(일정 작업이 맞추는 역할로 남기 때문이다). 같은 기록 본문으로 두 구현을 시험해 어긋남을 막는다.
- Worker 의 실패는 500 으로 GitHub 전달 기록에 남는다. 그 실패율 경보는 구현 PR 이 더한다.
- `workflow_run` 사건은 fork PR 의 실행에서도 온다. Worker 는 코드를 체크아웃하지도 실행하지도 않고, 같은 워크플로의 실패한
  작업을 다시 돌리기만 한다(fork PR 실행은 승인 정책 그대로다).
- Cloudflare Workers Paid 안에서 돈다. 하루 2,300 요청은 포함량 안이다(계산).

## Rejected alternatives

| 대안 | 왜 아닌가 |
|---|---|
| `workflow_run`·`pull_request: dequeued` 워크플로 트리거 | zizmor `dangerous-triggers` 무시 항목과 Rego 트리거 규칙 변경이 필요하다. 공개 저장소에서 보안 검사 둘을 푸는 일이다(사장 기각). |
| 호스트의 5분 타이머 | 범위 좁은 토큰을 호스트에 두고 폴링한다. 사건이 아니라 여전히 폴링이고, 호스트는 AWS 로 옮길 예정이다(사장 기각). |
| Worker 가 두 기존 워크플로를 dispatch 만 한다 | 논리를 옮기지 않아도 되지만 dispatch 한 작업이 러너를 기다린다. 러너가 20분씩 밀릴 때 취소가 20분 늦는다. 취소와 재실행은 Worker 가 직접 부른다. |
| main 빨간불 판정도 Worker 로 옮긴다 | `main_health.py` 227줄과 그 시험을 TypeScript 로 다시 쓰는 일이고 Issues 쓰기 권한이 더 든다. 제때 부르기만 하면 된다. |
| Probot·Octokit App SDK | Worker 에서 필요한 것은 JWT 서명 하나와 REST 호출 넷이다. Web Crypto 로 충분하고 의존성이 없다. |
