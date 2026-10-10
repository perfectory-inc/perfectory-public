# ADR 0175: Cloudflare Worker 도 서버가 main 을 배포한 뒤 스스로 따라 배포된다

- Status: Accepted
- Date: 2026-10-10
- Amends: [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md) Consequences 의 "Cloudflare Worker
  배포 … 의 자동화는 이 다음이다". [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md) A1 의 타일
  게이트웨이는 결정이 아니라 README 의 배포 절차(운영자의 `pnpm exec wrangler deploy`)만 바뀐다.
- Builds on: [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(운영 환경 파일 계약),
  [ADR-0160](./0160-one-by-pnu-gateway-source-serves-both-lanes.md)(by-PNU 소스 하나·레인 둘),
  [ADR-0173](./0173-a-failed-post-deploy-run-does-not-pause-the-schedule.md)(배포 뒤 실패는 배포 결과를 바꾸지 않는다)

## Context

ADR-0159 이후 서버는 main 을 스스로 배포한다. Worker 는 아니다. 게이트웨이 다섯(필지·건물 by-PNU, 타일, 산업단지
프로필, 지도 편집)은 병합 뒤 운영자가 노트북에서 `npx wrangler deploy -c wrangler.<레인>.jsonc [--env preview]` 로
손 배포했다. 그래서 main 의 게이트웨이 코드와 엣지에서 도는 코드가 언제 같은지 아무도 말할 수 없었고, 병합과 배포
사이의 간격은 사람의 기억에 달렸다. 2026-10-10 소유자 결정: Worker 도 서버처럼 main 을 스스로 따라간다.

- 계정 API 토큰은 이미 있다(노트북 `platforms/foundation-platform/.env.local` 의 `CLOUDFLARE_API_TOKEN`·
  `CLOUDFLARE_ACCOUNT_ID`). 분석 토큰(`cloudflare-analytics.env`)은 분석 읽기만 하므로 쓰지 않는다.
- 계정에는 이 저장소와 무관한 스크립트도 있다. 손대면 안 된다.
- 모든 생성 설정은 `keep_vars: true` 다. Wrangler 4.86.0 소스(`wrangler-dist/cli.js`)를 읽어 확인했다: `deploy` 와
  `versions upload` 둘 다 이 값이면 업로드에 `keep_bindings: ["plain_text", "json"]` 를 실어 보내고, 비밀값은 언제나
  유지한다(문서: "Secrets are never deleted by a deployment"). `keep_vars` 는 최상위 전용이라 `env.preview` 도 따른다.
  그러나 **물려받는 대상은 마지막 업로드**이지 지금 100% 로 서빙 중인 버전이 아니다. 카나리아(`by-pnu-gateway-canary.sh`)
  가 `--var <서빙 바인딩>:on` 으로 올렸다가 되돌린 뒤라면 마지막 업로드와 서빙 버전의 변수가 다를 수 있다.
- 대시보드에서 붙인 프로필 게이트웨이의 사용자 도메인은 설정에 `routes` 가 없어서 배포가 건드리지 않는다
  (문서: routes 키를 지우면 대시보드에서만 관리된다).

## Decision

1. **유닛 하나, 시작은 autodeploy.** `foundation-worker-autodeploy.service`(root, oneshot, `OnFailure` 알림)가
   `scripts/deploy/worker_autodeploy.py` 를 돌린다. 자기 타이머는 없다. `foundation-autodeploy.sh` 가 커밋을 배포한
   직후, 그리고 서버가 이미 main 의 머리를 돌리는 모든 틱에 `systemctl start --no-block` 으로 시작한다. 그래서 Worker
   배포의 성패는 그 유닛의 것이고 서버 배포의 결과·DAG 상태(ADR-0173)를 바꾸지 않는다. Worker 배포가 도는 동안
   새 서버 배포는 다음 틱까지 기다린다(둘 다 제어 체크아웃과 릴리스를 읽는다).
2. **믿는 트리에서만.** 제어 체크아웃(`/opt/perfectory-control/current`)의 커밋이 서버가 돌리는 릴리스와 같을 때만
   배포한다(ADR-0159 §3 의 신뢰 규칙). 다르면 서버 배포가 진행 중이거나 실패한 것이니 기다린다. 경로는 고정이다.
3. **목록은 계약 하나.** `platforms/foundation-platform/config/worker-deploys.contract.json` 이 배포할 Worker 다섯과
   각자의 게이트웨이 블록, 설정 파일, 입력 경로, 스모크를 적는다. 이름·호스트·미리보기 Worker 는
   `r2-connections.contract.json` 의 게이트웨이 블록에서 읽는다. 설정 파일의 `name`(과 미리보기 env 의 `name`)이 블록의
   이름과 다르면 아무것도 하기 전에 거부한다(exit 65) — 목록에 없는 스크립트에 닿는 길은 이것뿐이고, 시험이 지킨다.
   저장소의 모든 `wrangler*.jsonc` 가 목록에 있어야 하고, 게이트웨이 코드가 자기 디렉터리 밖에서 읽는 파일은 입력에
   있어야 한다(시험이 코드에서 물어본다).
4. **바뀐 Worker 만.** 입력 파일 전체의 해시(경로 + 내용)를 Worker 마다 `/data/foundation-platform/worker-autodeploy/
   workers/<id>.json` 에 커밋·버전 id 와 함께 남긴다. 해시가 같으면 건너뛴다. 기록이 없는 첫 실행은 전부 배포한다.
5. **순서.** 고정된 Node 이미지(`tools/container-images.env` 의 `WORKER_DEPLOY_IMAGE`, 기술 버전 계약의 Node 핀)
   컨테이너에서 lockfile 로 의존성을 설치하고 `config:check` 로 설정이 계약의 투영인지 확인한다(설치 스크립트에는
   토큰을 주지 않는다). Wrangler 는 lockfile 의 것이다. 그다음 Worker 마다:
   - D1 이 있으면 `d1 migrations apply --remote`(지도 편집, 미리보기가 없다).
   - 미리보기 env 가 있으면 미리보기 먼저, 그다음 운영. 각 대상에서: 지금 100% 인 버전 하나를 읽는다(나뉘어 있으면
     운영자의 카나리아가 진행 중이니 손대지 않고 실패), `versions upload`, **새 버전이 옛 버전의 변수·비밀을 하나도 잃지
     않았고 값도 같은지**(설정이 직접 정한 변수만 예외) `versions view` 로 비교하고 다르면 배포하지 않는다, `versions
     deploy <새>@100%`, 설정에 routes 가 있으면 `triggers deploy`, 스모크.
   - 스모크: by-PNU 레인은 그 호스트의 `_capabilities` 가 200 이고 `Foundation-Worker-Version` 이 새 버전 id 일 때까지
     기다린 뒤(2분), 그 레인의 시간별 서빙 감시(`by-pnu-serving-monitor.sh`, 표본 PNU 를 읽어 레인이 서빙하는 문서와
     비교)를 그 호스트로 돌린다(`MONITOR_BASE_URL`). 나머지는 계약의 경로 하나와 기대 상태(타일·프로필은 없는 객체의
     404, 지도 편집은 공개 overlay 의 200). 어느 경우든 배포 뒤 100% 버전이 새 버전인지 먼저 확인한다.
   - 스모크나 배포가 실패하면 그 대상을 옛 버전 100% 로 되돌린다(`versions deploy <옛>@100%`). 미리보기가 실패하면
     운영은 업로드조차 하지 않는다. 버전 id 는 모두 저널에 남는다.
   - 카나리아 판정(`by-pnu-gateway-health.sh`, 분석 기반 15분 단계)은 쓰지 않는다. 그것은 묶음 경로를 버전 비율로 켜고
     끄는 운영자 도구이고, `code` 단계는 서빙 바인딩을 `off` 로 올리므로 자동 배포에 쓰면 켜진 묶음을 끈다.
6. **실패와 재시도.** 실패한 Worker 는 같은 입력에 대해 계약의 `attempts_per_change`(3)번까지 다음 틱에 다시 한다
   (npm·API 일시 오류가 다음 병합을 기다리지 않게). 시도마다 유닛이 실패해 한 번 알린다. 그 뒤로는 입력이 바뀔
   때까지 조용히 기다린다(`failed/<id>.json` 을 지우면 다시 한다). 거부(설정 미비)는 이유가 바뀔 때까지 한 번만 알린다.
7. **켜고 끄기.** 토큰 파일 `/etc/foundation-platform/cloudflare-deploy.env`(root:root 0600, `CLOUDFLARE_API_TOKEN`·
   `CLOUDFLARE_ACCOUNT_ID`, Wrangler 의 이름 그대로 — 환경 변수 이름 계약의 `wrangler` 예외)가 없으면 아무것도 하지
   않는다. 운영 환경 파일 계약에 선택 그룹 `cloudflare-deploy` 로 적고, 유닛의 `EnvironmentFile` 은 거기서 렌더한다.
   파일이 있는데 이름이 빠졌거나 남이 읽을 수 있거나, by-PNU 레인의 감시 파일이 없으면 부작용 전에 exit 78.
   `/etc/foundation-platform/worker-autodeploy.off`(Worker 만) 또는 `autodeploy.off`(전부)가 있으면 아무것도 하지 않는다.
8. Wrangler 컨테이너의 메모리 상한(2g)은 이 계약에 있고 `tools/host-memory-budget.contract.json` 의 일회성 작업으로 센다.

## Consequences

- 병합하면 서버 배포 뒤 몇 분 안에 바뀐 Worker 가 미리보기 → 운영 순서로 따라간다. 노트북의 손 배포는 이제 비상용이다.
- 첫 실행은 다섯 Worker 를 모두 지금의 main 으로 다시 올린다. 엣지에서 돌던 코드가 main 과 달랐다면 그 차이가
  이때 나간다. 첫 실행은 운영자가 저널을 보며 지켜본다(런북 `platforms/foundation-platform/docs/runbooks/worker-autodeploy.md`).
- 변수 비교가 마지막 업로드와 서빙 버전의 차이를 잡으면 그 Worker 는 배포되지 않고 알린다. 운영자가 서빙 버전을
  기준으로 바로잡은(또는 그 차이를 의도로 확인한) 뒤 `failed/<id>.json` 을 지운다.
- 스모크의 강도는 Worker 마다 다르다. by-PNU 는 새 버전이 답하는 것과 실제 문서를 확인하지만, 타일·프로필은 "새 코드가
  버킷에 물어 404 를 답한다"까지만 본다(릴리스 id 가 굽기마다 바뀌어 늘 200 인 경로가 없다).
- D1 마이그레이션은 되돌리지 않는다. Worker 를 되돌려도 스키마는 앞으로 간 채다(서버의 Postgres 마이그레이션과 같다).
- 프로필 게이트웨이의 호스트(`profiles.gongzzang.app`)는 게이트웨이 블록에 없어 스모크에만 적었다.
- 출처: [Wrangler configuration — keep_vars, source of truth](https://developers.cloudflare.com/workers/wrangler/configuration/),
  [Wrangler commands — versions, deployments, triggers, rollback](https://developers.cloudflare.com/workers/wrangler/commands/),
  [Gradual deployments](https://developers.cloudflare.com/workers/configuration/versions-and-deployments/gradual-deployments/).

---

2026-10-10 (첫 운영 실행): 필지·건물(미리보기 포함)·타일·프로필은 배포되고 스모크를 통과했다. 지도 편집은 D1 단계에서
실패했다(시도 1): Wrangler 4.86.0 이 "missing a database_id, which is needed for operations on remote resources" 로
거부했다. 저장소의 설정은 의도적으로 `database_id` 를 갖지 않는다(Wrangler 가 만든 D1, `render-wrangler-config.mjs`
와 그 시험이 막는다; `map-edit-fold.md` 의 함정과 같은 일). 결정 §5 에 한 단계를 더한다: `config:check` 가 렌더된
사본을 계약에 맞춰 본 **뒤**, 설정의 `d1_databases` 중 id 가 없는 것마다 같은 컨테이너·토큰으로 `wrangler d1 list
--json` 에서 `database_name` 으로 id 를 찾아 작업 사본 안의 파생 설정(`autodeploy.<설정 파일>`)에만 쓰고, 그 뒤의
마이그레이션·업로드·배포는 그 파생 설정으로 한다. 이름이 계정에 없거나 둘 이상이면 아무것도 마이그레이션·업로드하지
않고 그 이름을 대며 실패한다. 저장소와 렌더된 사본은 바뀌지 않는다. 결정의 나머지는 그대로라 새 ADR 로 나누지 않았다.
이 고침은 지도 편집의 입력을 바꾸지 않는다. 그래서 §6 의 재시도 한도도 고친다: 실패 기록은 Worker 입력의 해시와
**배포기 파일**(`worker_autodeploy.py`, `worker-wrangler.sh`, `worker-deploys.contract.json`)의 해시 둘에 묶이고, 다른
배포기가 낸 실패는 세지 않는다. 배포기를 고친 커밋이 배포되면 포기했던 Worker 도 서버에서 손대지 않고 다시 시도된다.
