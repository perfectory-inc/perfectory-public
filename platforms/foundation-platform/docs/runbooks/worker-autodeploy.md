---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-29
---

# Worker 자동 배포 — 토큰 한 번 두기·켜고 끄기·확인

[루트 ADR-0175](../../../../docs/adr/0175-workers-deploy-themselves-after-the-host-deploys-main.md). 서버가 main 을
배포한 뒤 `foundation-worker-autodeploy.service` 가 소스가 바뀐 Cloudflare Worker 를 미리보기 → 운영 순서로 배포한다.
어떤 Worker 를, 어떤 설정으로, 어떤 스모크로 배포하는지는
[`config/worker-deploys.contract.json`](../../config/worker-deploys.contract.json) 한 곳에 있다. 목록에 없는 계정의
스크립트는 건드리지 않는다.

## 1. 토큰 두기 (소유자, 한 번)

토큰 파일이 없으면 유닛은 아무것도 하지 않는다. 새 토큰을 만들지 않는다: 노트북의
`platforms/foundation-platform/.env.local` 에 있는 계정 토큰과 계정 id 를 값이 화면에 나오지 않게 옮긴다.
분석 토큰 파일(`cloudflare-analytics.env`)은 쓰지 않는다. 저장소 루트(main 의 체크아웃)에서:

호스트 파일에서 토큰은 토큰 이름 `foundation-platform-workers-writer-prod` 에서 만든 이름
`FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN` 으로 둔다(루트 ADR-0247). wrangler 에게는 `worker-wrangler.sh` 가
`CLOUDFLARE_API_TOKEN` 으로 건넨다. 파일은 손으로 편집하지 않고 `foundation-release.sh env-set` 으로 쓴다.

```bash
# 1) 두 줄만 골라 이름을 바꿔 서버의 홈에 남만 못 읽는 임시 파일로 보낸다(값은 어디에도 출력되지 않는다).
grep -E '^(CLOUDFLARE_API_TOKEN|CLOUDFLARE_ACCOUNT_ID)=' platforms/foundation-platform/.env.local | tr -d '\r' \
  | sed 's/^CLOUDFLARE_API_TOKEN=/FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN=/' \
  | ssh ai-server 'umask 077 && cat > ~/.cloudflare-deploy.env'
# 2) 계약의 cloudflare-deploy 그룹으로 쓰고(root:root 0600) 임시 파일을 지운다. 출력은 이름뿐이다.
ssh ai-server 'sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set cloudflare-deploy ~/.cloudflare-deploy.env && shred -u ~/.cloudflare-deploy.env'
```

토큰에 필요한 권한: 계정의 Workers Scripts 편집(업로드·버전·배포·롤백), D1 편집(지도 편집의 마이그레이션), 사용자
도메인이 걸린 zone 의 Workers Routes 편집(`triggers deploy`). 부족하면 첫 실행의 해당 단계가 실패하고 알린다.

## 2. 첫 실행 지켜보기

토큰을 둔 뒤 다음 autodeploy 틱(간격은 `foundation-autodeploy.timer`)이 유닛을 시작한다. 기다리지 않으려면 지금 시작하고 따라 읽는다.
첫 실행은 다섯 Worker 를 모두 지금의 main 으로 다시 올린다(기록이 없으므로).

```bash
sudo systemctl start --no-block foundation-worker-autodeploy.service
journalctl -fu foundation-worker-autodeploy.service
```

| 저널 줄 | 뜻 |
|---|---|
| `off: …cloudflare-deploy.env does not exist` | 토큰 파일이 없다(1 절) |
| `refused: … must be … mode 0600` / `does not hold …` | 파일이 반쯤 설정됐다. 고치면 다음 틱에 돈다(exit 78, 한 번 알림) |
| `refused: …-gateway: its smoke runs the … serving monitor, whose file … does not exist` | 그 레인의 감시 파일이 없다. 감시를 먼저 켠다(각 레인의 묶음 전환 런북) |
| `<id>: preview … uploaded <버전> (serving <옛 버전>)` → `moved to` → `passed its smoke` | 정상 진행 |
| `… does not keep what <옛> serves with (variable … is missing)` | 새 버전이 서빙 버전의 변수를 잃었다. 배포하지 않았다(3 절) |
| `traffic is split across 2 versions` | 운영자의 카나리아가 진행 중이다. 끝나면 다음 틱에 다시 한다 |
| `<id>: … did not exist; created at <버전>` | 계약에 있는데 계정에 없던 Worker 를 `wrangler deploy` 로 만들었다(호스트명 포함). 이어서 평소 흐름을 탄다(root ADR-0196) |
| `p95 … was above the bound …, but read again it is … against <호스트>'s …: not this version's doing` | 지연만 한도를 넘었고 서빙 중인 버전도 같은 표본에서 그만큼 느렸다. 통과시켰다(root ADR-0196). 사용자 체감은 시간별 감시에서 본다 |
| `reads the sample at p95 … against <호스트>'s … (limit …)` | 새 버전이 비교 호스트명보다 계약의 배수 이상 느렸다. 막고 되돌렸다 |
| `rolled back to <옛>` | 스모크가 실패해 그 대상을 옛 버전으로 되돌렸다 |
| `THE ROLLBACK OF … FAILED` | 되돌리기도 실패했다. 줄에 적힌 `wrangler versions deploy <옛>@100%` 를 손으로 한다 |

## 3. 켜고 끄기·다시 시도

| 할 일 | 명령 (root) |
|---|---|
| Worker 배포만 잠시 끄기 | `touch /etc/foundation-platform/worker-autodeploy.off` (지우면 다시 돈다) |
| 서버 배포와 함께 끄기 | `touch /etc/foundation-platform/autodeploy.off` |
| 완전히 끄기 | `rm /etc/foundation-platform/cloudflare-deploy.env` |
| 지금 상태 | `journalctl -u foundation-worker-autodeploy.service -n 80`, `ls /data/foundation-platform/worker-autodeploy/{workers,failed}` |
| 한 Worker 의 마지막 배포 | `cat /data/foundation-platform/worker-autodeploy/workers/<id>.json` (커밋, 입력 해시, 대상마다 옛·새 버전 id) |
| 포기한 Worker 다시 시도 | 원인을 고친 뒤 `rm /data/foundation-platform/worker-autodeploy/failed/<id>.json` |
| 한 Worker 를 같은 입력으로 다시 배포 | `rm /data/foundation-platform/worker-autodeploy/workers/<id>.json` |

실패한 Worker 는 같은 입력·같은 배포기 파일에 대해 계약의 `attempts_per_change` 번까지 다음 틱에 다시 하고(시도마다
Slack 한 번), 그 뒤로는 Worker 의 입력이나 배포기 파일(`worker_autodeploy.py`, `worker-wrangler.sh`, 이 계약)이 바뀔
때까지 기다린다. 배포기를 고친 커밋이 배포되면 따로 지우지 않아도 다시 시도된다. 변수 비교가 실패했다면, 마지막으로 업로드된 버전(카나리아가 남긴 것일 수
있다)과 지금 서빙 중인 버전의 변수가 다르다는 뜻이다. `wrangler versions view <id> --name <Worker>` 로 둘을 비교해
서빙 버전이 맞는 값을 갖고 있음을 확인한 뒤, 손으로 서빙 버전의 값으로 한 번 업로드·배포하고(`versions upload
--var <이름>:<값>` → `versions deploy <새>@100%`) 위의 `failed/<id>.json` 을 지운다.

## 4. 엣지 탐침 (by-PNU, 15분마다와 배포 스모크)

[루트 ADR-0327](../../../../docs/adr/0327-a-worker-deploy-and-a-quarter-hourly-probe-read-what-users-read-twice.md).
`scripts/ops/by-pnu-edge-probe.py` 가 사용자가 읽는 대로 GET 으로 두 번 읽는다: by-PNU Worker 가 서빙하는 레인마다
게이트 표본에서 404 가 아닌 첫 문서, 필지 엣지에서는 공매 문서가 이름한 첫 사진(내용해시 대조). 두 번째 읽기는 늘
엣지 사본(캐시 적중)이다 — 2026-10-29 캐시된 공매 사진이 모두 500 이었고 HEAD 확인은 통과했다.

| 언제 | 무엇이 | 실패하면 |
|---|---|---|
| by-PNU Worker 배포마다, 미리보기·운영 각각 | `worker_autodeploy.py` 의 스모크(capabilities 다음, 감시 앞) | 그 대상을 옛 버전으로 되돌리고 유닛 실패로 Slack |
| `foundation-by-pnu-edge-probe.timer` (매시 07·22·37·52분) | 계약의 by-PNU Worker 셋의 운영 호스트명 | `foundation-unit-failed@` 가 Slack 에 알린다(런북: 이 절) |

| 저널 줄 (`journalctl -u foundation-by-pnu-edge-probe.service -n 80`) | 뜻·첫 확인 |
|---|---|
| `FAILED GET … (read 2 of 2) answered 500` | 엣지 사본에서 읽은 답이 깨졌다. `wrangler tail` 로 그 Worker 의 예외를 본다. 직전 배포가 원인이면 `wrangler versions deploy <옛>@100%` |
| `answered content type …` / `not a JSON document` / `hash to …` | 다른 것을 서빙한다. 같은 URL 을 `curl -sD- -o /dev/null` 로 두 번 확인 |
| `none of the first 20 entries of its gate sample is served` | 표본 PNU 가 하나도 서빙되지 않는다: 레인 장애 또는 표본이 낡았다(그 레인의 전환 런북 monitor-sample) |
| `… is not set, the lane is not cut over; skipped` | 그 레인의 감시 표본 파일이 아직 없다. 실패가 아니다 |
| `no lane was probed` | 어느 레인의 표본도 없다. 감시 파일(`/etc/foundation-platform/*-serving-monitor.env`)을 확인 |

## 5. 손 배포

토큰 파일이 없는 동안, 또는 비상시에는 각 게이트웨이 README 의 배포 절차를 그대로 쓴다. 손으로 배포한 뒤에는
기록이 바뀌지 않으므로, 다음 자동 실행은 입력이 바뀐 Worker 만 다시 올린다.
