---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-10
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

```bash
# 1) 두 줄만 골라 서버의 홈에 남만 못 읽는 임시 파일로 보낸다(값은 어디에도 출력되지 않는다).
grep -E '^(CLOUDFLARE_API_TOKEN|CLOUDFLARE_ACCOUNT_ID)=' platforms/foundation-platform/.env.local \
  | ssh ai-server 'umask 077 && cat > ~/.cloudflare-deploy.env'
# 2) root:root 0600 으로 설치하고 임시 파일을 지운다(sudo 비밀번호는 -t 로 묻는다).
ssh -t ai-server 'sudo install -o root -g root -m 0600 ~/.cloudflare-deploy.env /etc/foundation-platform/cloudflare-deploy.env && shred -u ~/.cloudflare-deploy.env'
# 3) 이름만 확인한다: CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN 두 줄이 나와야 한다.
ssh -t ai-server "sudo sed -n 's/=.*//p' /etc/foundation-platform/cloudflare-deploy.env | sort"
```

토큰에 필요한 권한: 계정의 Workers Scripts 편집(업로드·버전·배포·롤백), D1 편집(지도 편집의 마이그레이션), 사용자
도메인이 걸린 zone 의 Workers Routes 편집(`triggers deploy`). 부족하면 첫 실행의 해당 단계가 실패하고 알린다.

## 2. 첫 실행 지켜보기

토큰을 둔 뒤 다음 autodeploy 틱(10분 이내)이 유닛을 시작한다. 기다리지 않으려면 지금 시작하고 따라 읽는다.
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

실패한 Worker 는 같은 입력에 대해 계약의 `attempts_per_change` 번까지 다음 틱에 다시 하고(시도마다 Slack 한 번),
그 뒤로는 입력이 바뀔 때까지 기다린다. 변수 비교가 실패했다면, 마지막으로 업로드된 버전(카나리아가 남긴 것일 수
있다)과 지금 서빙 중인 버전의 변수가 다르다는 뜻이다. `wrangler versions view <id> --name <Worker>` 로 둘을 비교해
서빙 버전이 맞는 값을 갖고 있음을 확인한 뒤, 손으로 서빙 버전의 값으로 한 번 업로드·배포하고(`versions upload
--var <이름>:<값>` → `versions deploy <새>@100%`) 위의 `failed/<id>.json` 을 지운다.

## 4. 손 배포

토큰 파일이 없는 동안, 또는 비상시에는 각 게이트웨이 README 의 배포 절차를 그대로 쓴다. 손으로 배포한 뒤에는
기록이 바뀌지 않으므로, 다음 자동 실행은 입력이 바뀐 Worker 만 다시 올린다.
