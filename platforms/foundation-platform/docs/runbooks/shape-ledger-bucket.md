---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-19
---

# 모양 원장 버킷과 준비된 PR 봇 — 사장님이 한 번 하는 일

[ADR-0279](../../../../docs/adr/0279-a-stale-shape-ledger-arrives-as-a-prepared-pull-request-from-ci.md) 의 자리입니다.
서버가 매일 찍는 운영 표 모양 원장을 비공개 버킷에 올리고, 비공개 저장소 perfectory-ops 의 예약 작업이 읽어 비교하고
갱신 PR 을 준비합니다. **비밀값(액세스 키·시크릿·App 키)은 사장님만 보고 아래 정한 자리에만 넣습니다.** 에이전트와
코디네이터에게는 "만들었다"와 이름만 알려 주세요.

하지 않아도 아무것도 깨지지 않습니다. 서버는 "올리지 못함, 손으로 복사"를, 예약 작업은 "비밀 없음, 건너뜀"을 말할
뿐입니다.

## 결론

| 무엇 | 이름 | 넣는 곳 |
| --- | --- | --- |
| 버킷 | `foundation-platform-shape-ledger-prod` (Public access 끔, 잠금 규칙 없음: 매일 같은 두 파일을 덮어씁니다) | Cloudflare R2 |
| 서버 쓰기 열쇠 | `foundation-platform-shape-ledger-writer-prod` (Object Read & Write, 이 버킷만) | 서버 파일 `/etc/foundation-platform/shape-ledger.env` |
| 비교 읽기 열쇠 | `foundation-platform-shape-ledger-reader-prod` (Object Read only, 이 버킷만) | perfectory-ops 의 보호 환경 `shape-ledger` |
| 준비된 PR 봇 | GitHub App (아래 4) | 같은 환경 |

## 1. 버킷

Cloudflare → R2 object storage → Create bucket → 이름 `foundation-platform-shape-ledger-prod`. Settings 의
**Public access 는 꺼 둡니다**(r2.dev·사용자 도메인 연결하지 않음). 잠금 규칙은 걸지 않습니다.

## 2. 열쇠 둘

R2 object storage → API Tokens → **Create Account API token** 을 두 번:

| 이름 | Permissions | 버킷 범위 |
| --- | --- | --- |
| `foundation-platform-shape-ledger-writer-prod` | Object Read and Write | Apply to specific buckets only → 위 버킷 하나 |
| `foundation-platform-shape-ledger-reader-prod` | Object Read | 같음 |

각 토큰의 액세스 키 ID 와 시크릿은 만들 때 한 번만 보입니다.

## 3. 비교 읽기 열쇠 → perfectory-ops 보호 환경

perfectory-ops 저장소 Settings → Environments → New environment `shape-ledger` → Deployment branches 를 **main 만**.
Environment secrets 에 셋:

- `FOUNDATION_PLATFORM_SHAPE_LEDGER_READER_ACCESS_KEY_ID`, `FOUNDATION_PLATFORM_SHAPE_LEDGER_READER_SECRET_ACCESS_KEY`
  (비교 읽기 열쇠)
- `FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_ENDPOINT_HOST`: `<계정 ID>.r2.cloudflarestorage.com`

공개 저장소(perfectory-public)에는 아무것도 넣지 않습니다. 공개 워크플로우는 비밀을 쓰지 않습니다(공개 저장소 안전 가드).

## 4. 준비된 PR 봇 (GitHub App)

갱신 PR 을 공개 저장소에 여는 데 씁니다. App 이 연 PR 은 필수 검사를 띄웁니다(`GITHUB_TOKEN` 이 연 PR 은 띄우지 않음,
ADR-0158 평가 B). 권한은 ADR-0158 평가 C 그대로이며 npm 권고 PR 도 함께 씁니다.

1. Organization settings → Developer settings → GitHub Apps → New GitHub App. 이름 예: `perfectory-prepared-pr`.
   Webhook 끔. Repository permissions 는 Contents **Read and write**, Pull requests **Read and write**, Metadata Read-only
   만. "Only on this account".
2. Install App → Only select repositories → **perfectory-public 하나**.
3. perfectory-ops 의 `shape-ledger` 환경 비밀에 `PREPARED_PR_APP_ID`, `PREPARED_PR_APP_PRIVATE_KEY`.

## 5. 서버 쓰기 열쇠 → 서버 파일

서버에 접속해 서버 셸에서:

```text
sudo install -m 0640 -o root -g foundation-platform /dev/null /etc/foundation-platform/shape-ledger.env
sudo nano /etc/foundation-platform/shape-ledger.env
```

네 줄을 적고 저장합니다(값은 2 에서 받은 것):

```text
FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_BUCKET=foundation-platform-shape-ledger-prod
FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_ENDPOINT_HOST=<계정 ID>.r2.cloudflarestorage.com
FOUNDATION_PLATFORM_SHAPE_LEDGER_WRITER_ACCESS_KEY_ID=<서버 쓰기 열쇠의 액세스 키 ID>
FOUNDATION_PLATFORM_SHAPE_LEDGER_WRITER_SECRET_ACCESS_KEY=<서버 쓰기 열쇠의 시크릿>
```

## 6. 알려 주실 것

"버킷·열쇠 2개·perfectory-ops 환경·App·서버 파일 완료". 값은 알려 주지 않습니다. 다음 원장 점검(매일 12:30 UTC, 배포 뒤
마다)의 로그에 `captures published to foundation-platform-shape-ledger-prod/shape-ledger/` 가 찍히면 끝입니다.
