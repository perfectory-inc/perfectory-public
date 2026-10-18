---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-18
---

# 릴리스 산출물 버킷 — 사장님이 한 번 하는 일

[ADR-0276](../../../../docs/adr/0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md) §5 의 버킷과
열쇠 두 개를 만듭니다. **비밀값(액세스 키·시크릿)은 사장님만 보고, 아래 정한 자리에만 넣습니다.** 에이전트와 코디네이터에게는
"만들었다"와 이름만 알려 주세요.

## 결론

| 무엇 | 이름 | 넣는 곳 |
|---|---|---|
| 버킷 | `perfectory-foundation-releases` | Cloudflare R2 |
| CI 발행 열쇠 | `foundation-release-publish-ci` (Object Read & Write, 이 버킷만) | GitHub 환경 `release-publish` 의 비밀 |
| 서버 읽기 열쇠 | `foundation-release-read-server` (Object Read only, 이 버킷만) | 서버 파일 `/etc/foundation-platform/secrets/release-r2-reader.env` |

## 1. 버킷 만들기 (Cloudflare 대시보드)

1. R2 object storage → Create bucket.
2. 이름 `perfectory-foundation-releases`, 위치는 서버와 가까운 곳(Asia-Pacific 힌트), 기본 저장 등급 Standard.
3. 버킷 → Settings 에서 **Public access** 는 그대로 꺼 둡니다(r2.dev·사용자 도메인 연결하지 않음).

## 2. 덮어쓰기·삭제 금지 (Bucket lock rules)

버킷 → **Settings** → **Bucket lock rules** → **Add rule** 를 두 번:

| 규칙 이름 | 접두어(prefix) | 보존 |
|---|---|---|
| `releases-forever` | `releases/` | Indefinite(무기한) |
| `blobs-30-days` | `blobs/` | 30 days |

→ **Save changes**. 무기한 규칙은 나중에 지울 수 없다고 생각하고 접두어를 두 번 확인해 주세요.

## 3. 열쇠 두 개 (R2 API 토큰)

R2 object storage → Account Details 의 **API Tokens** 옆 **Manage** → **Create Account API token**:

| 칸 | CI 발행 열쇠 | 서버 읽기 열쇠 |
|---|---|---|
| 이름 | `foundation-release-publish-ci` | `foundation-release-read-server` |
| Permissions | **Object Read and Write** | **Object Read** |
| 버킷 범위 | **Apply to specific buckets only** → `perfectory-foundation-releases` 하나 | 같음 |
| TTL | 1년(만료 한 달 전에 다시 만듦) | 같음 |

만들면 액세스 키 ID 와 시크릿이 한 번만 보입니다. 바로 아래 4·5 의 자리에 넣고, 다른 곳에 적지 마세요.

## 4. CI 발행 열쇠 → GitHub 보호 환경

1. GitHub 저장소 → Settings → Environments → **New environment** → 이름 `release-publish`.
2. **Deployment branches and tags** → Selected branches → `main` 만.
3. Environment secrets 에 둘: `RELEASE_R2_ACCESS_KEY_ID`, `RELEASE_R2_SECRET_ACCESS_KEY`.
4. Environment variables 에 둘(비밀 아님): `RELEASE_R2_BUCKET` = `perfectory-foundation-releases`, `RELEASE_R2_ENDPOINT` = 계정의 R2 S3 주소.

저장소 수준 비밀(Repository secrets)에는 넣지 않습니다. 환경 비밀은 main 의 그 환경을 쓰는 작업만 받습니다.

## 5. 서버 읽기 열쇠 → 서버 파일

서버에 접속해 아래 두 줄을 그대로 칩니다(PowerShell 에서 ssh 로 들어간 뒤 서버 셸에서):

```text
sudo install -o root -g root -m 0600 /dev/null /etc/foundation-platform/secrets/release-r2-reader.env
sudo nano /etc/foundation-platform/secrets/release-r2-reader.env
```

편집기에 두 줄을 적고 저장합니다(값은 3 에서 받은 것):

```text
FOUNDATION_RELEASE_R2_ACCESS_KEY_ID=<서버 읽기 열쇠의 액세스 키 ID>
FOUNDATION_RELEASE_R2_SECRET_ACCESS_KEY=<서버 읽기 열쇠의 시크릿>
```

## 6. 다른 열쇠가 닿지 않는지 확인

API Tokens 목록에서 **Admin** 이 붙은 토큰과 버킷 범위가 "All buckets" 인 Object 토큰을 봐 주세요. 사장님 본인 관리용 말고는
이 버킷에 닿는 토큰이 없어야 합니다. 있으면 이름만 알려 주세요(지우거나 범위를 좁히는 것은 그 토큰을 쓰는 곳을 확인한 뒤에).

## 7. 알려 주실 것

"버킷·잠금 규칙 2개·열쇠 2개·환경·서버 파일 완료"와, 6 에서 본 토큰 이름. 값은 알려 주지 않습니다.
