# ADR 0436: 더니어의 Cloudflare 엣지(Access·Tunnel·DNS)는 OpenTofu 코드다 — 호스트가 이미 가진 계정 토큰으로 적용하고, 허용 목록은 신원 플랫폼의 직원 목록이다

- Status: Accepted
- Date: 2026-11-07
- Revises: [ADR-0434](./0434-dawneer-is-served-from-the-data-host-behind-cloudflare-access-and-a-tunnel-and-relays-zitadels-sign-in-pages.md)
  §3(이메일 목록은 소유자가 Access 정책에 넣는다), §7(첫 판은 소유자가 손으로), "소유자가 한 번 하는 일" 1–4
- Builds on: [ADR-0423](./0423-github-repository-settings-are-opentofu-state-in-r2-applied-by-the-host-with-a-scoped-token.md)(설정은
  OpenTofu, 상태는 R2 에 잠금과 함께, 적용은 호스트 — 제안),
  [ADR-0175](./0175-workers-deploy-themselves-after-the-host-deploys-main.md)(배포 뒤 단계가 호스트의 토큰 파일로 외부 상태를 맞춤),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)·[ADR-0247](./0247-credential-variable-names-are-derived-from-the-cloudflare-token-name.md)(런타임 비밀 계약)

## Context

ADR-0434 는 더니어를 호스트에서 띄우고 Cloudflare Access·Tunnel 뒤에 두었다. 그런데 엣지 설정은 사람 몫으로 남겼다. 대시보드 세 단계(Zero
Trust 켜기, Access 앱과 본인 메일, 터널 만들기)와 터널 토큰을 서버에 넣는 PowerShell 명령 하나다. 사장 규칙(2026-11-07)은 "평상시 사장 단계
0" 이다.

2026-11-07 실측(값은 출력하지 않고 상태 코드만):

| 무엇 | 결과 |
|---|---|
| 호스트 `cloudflare-deploy` 그룹의 계정 토큰(`foundation-platform-workers-writer-prod`, 노트북 `.env.local` 의 같은 이름) | Tunnel 목록 200, Tunnel 생성 요청은 권한을 지나 입력 검증(400)에서 멈춤, DNS 레코드 생성도 같음, R2 버킷 목록 200·생성은 입력 검증, Access 는 403 `access.api.error.not_enabled`(권한이 아니라 Zero Trust 조직이 없다는 뜻) |
| 그 토큰에서 낸 R2 S3 열쇠(토큰 id, 토큰 값의 SHA-256 — Cloudflare 문서의 방식) | ListBuckets 200 |
| Zero Trust 조직 | 이 조사 중 `POST /access/organizations` 를 빈 본문으로 보낸 요청이 **201 로 조직을 만들었다**(06:04Z, 이름 `crimson-bird-3c59`, 팀 도메인 자동). 의도한 쓰기가 아니었다. 그 뒤 Access 앱 생성 요청도 권한을 지나 입력 검증까지 간다 |
| 결제 수단 | 계정에 Workers Paid($5/월)와 R2 Paid 구독이 이미 있다 — 결제 수단이 등록돼 있다. Zero Trust Free(50명까지 0원)는 API 로 조직을 만들 때 요금제·결제 화면을 거치지 않았다 |
| 신원 플랫폼의 직원 | `identity.staff` 중 역할(`identity.staff_role`)을 가진 사람 1명, 메일 모양이 맞는 주소 1개(개수만 읽음) |
| ADR-0423 의 기계 | 저장소에 없다(제안 상태, `infra/github/` 도 OpenTofu 도 없음) |

즉 새 토큰이 필요 없다. 이미 호스트에 있는 계정 토큰이 엣지 설정에 필요한 범위(Tunnel·DNS·Access·R2 편집)를 모두 가진다.

대기업은 엣지 설정을 이렇게 다룬다. Cloudflare 자신이 Terraform 프로바이더(`cloudflare/cloudflare`)를 유지하고 Zero Trust 설정 예시를
그것으로 낸다. GitLab·Shopify 같은 회사는 DNS·CDN·접근 정책을 Terraform 저장소로 관리하고 병합이 곧 적용이다. Google 의 BeyondCorp 도 접근 정책을
사람 목록(디렉터리)에서 만든다 — 손으로 적은 메일 목록이 아니다.

## 비교

| | A. OpenTofu, 호스트가 적용, 기존 토큰 | B. cloudflared CLI·REST 를 셸로 | C. 소유자가 대시보드(지금) |
|---|---|---|---|
| 표류 | `apply` 가 하루에 한 번 이상 실제와 비교해 되돌린다 | 손으로 짠 비교 | 모른다 |
| 상태 | R2, 조건부 PUT 잠금, 암호화 | 없음(멱등을 손으로) | 없음 |
| 사장 단계 | 0 | 0 | 넷 |
| ADR-0423 과 | 같은 모양(그 GitHub 쪽 구현이 이 실행기를 따라가면 된다) | 다른 길 하나 더 | — |

**A 를 고른다.**

## Decision

1. **코드.** `platforms/foundation-platform/infra/edge/main.tf` 하나다. Zero Trust 조직(이미 있는 것을 `import` 블록으로 받는다, 이름
   `perfectory`, 팀 도메인은 그대로), 일회용 PIN 로그인 방법, Access 정책 `staff`(allow, 메일마다 include 하나), self-hosted 앱 `dawneer`
   (세션 24h, 그 로그인 방법만), Cloudflare 가 설정을 갖는 터널 `perfectory-console`(규칙 둘: 공개 주소 → 원점, 나머지 404), 그 터널을
   가리키는 proxied CNAME 이다. DNS 레코드는 Access 앱 뒤에 만든다(`depends_on`): 이름이 터널로 풀리는 순간 이미 Access 가 막는다.
   - 값은 복사하지 않는다. 공개 주소는 더니어 Zitadel 앱의 redirect URI(`staff-console-applications.v1.json`, configure-zitadel.sh 가 trusted
     domain 으로 쓰는 그 파일), 원점은 `compose.console.yml` 의 `DAWNEER_BIND` 에서 HCL 이 직접 읽는다.
   - OpenTofu 1.13.9 와 프로바이더 5.28.1 은 `required_version`·잠금 파일(`.terraform.lock.hcl`, 다섯 플랫폼 해시)로 고정한다. 호스트는
     `ghcr.io/opentofu/opentofu:1.13.9@sha256:…` 이미지로 돌린다(`tools/container-images.env` `EDGE_SETTINGS_IMAGE`).

2. **허용 목록은 신원 플랫폼의 직원 목록이다.** 적용 때마다 `identity.staff` 중 역할을 하나 이상 가진 사람의 메일을 identity DB 컨테이너에서
   읽어(`docker exec … psql`, 포트·네트워크 없음) 변수로 넘긴다. 공개 저장소에는 메일이 없다(변수는 `sensitive`, 로그는 개수만). 직원에게
   역할을 주면(`POST /identity/v1/staff/{id}/roles`) 다음 틱에 엣지 허용 목록에 들어가고, 역할이 모두 빠지면 나간다. **빈 목록은 거부한다**
   — 사장까지 잠그는 적용이 되기 때문이다(실행기와 변수 검증 두 곳).

3. **적용은 호스트가 한다.** `foundation-edge-settings.service`(root, oneshot)가 `scripts/deploy/edge_settings.py` 를 돌린다.
   `foundation-autodeploy.sh` 가 호스트가 main 을 돌 때 Worker 배포와 함께 시작하고, 다음 호스트 배포는 둘 다 끝날 때까지 기다린다.
   유닛은 Worker 배포 뒤에 선다(`After=`): 호스트 메모리 예산은 배포 컨테이너를 한 번에 하나로 센다(`worker-deploys.contract.json`
   `containers.edge_settings` 512m).
   - 바뀐 것만: 모듈과 그것이 읽는 파일, 허용 목록, 계정 id 의 해시가 마지막 성공과 같고 하루가 지나지 않았으면 아무것도 하지 않는다. 하루가
     지나면 같은 입력으로도 `apply` 한다 — 대시보드에서 바꾼 것(표류)이 하루 안에 코드로 돌아간다. ADR-0423 은 GitHub 표류를 되돌리지 않기로
     했지만, 접근 정책의 표류는 보안 구멍이라 되돌린다.
   - 실패는 같은 입력·같은 실행기 파일로 세 번까지, 매번 exit 1(OnFailure 알림). 그 뒤는 입력이나 실행기가 바뀔 때까지 기다린다(ADR-0175 와
     같은 규칙).
   - 끄기: `/etc/foundation-platform/autodeploy.off` 또는 `edge-settings.off`. 토큰 파일이 없거나 제어 체크아웃이 호스트가 도는 커밋이 아니면
     아무것도 하지 않는다.

4. **상태.** R2 버킷 `foundation-platform-edge-settings-state-prod`(없으면 실행기가 한 번 만든다), 키 `edge/terraform.tfstate`, S3
   백엔드의 `use_lockfile`(조건부 PUT 잠금)이다. 상태에는 터널 비밀이 들어 있으므로 OpenTofu 상태 암호화(pbkdf2 + AES-GCM, `enforced`)로
   잠근다. passphrase 는 실행기가 첫 적용에서 한 번 만들어 런타임 비밀 그룹 `edge-settings`(root 0600)에 둔다. 사람이 만들거나 보관하지 않는다
   — 잃으면 상태를 지우고 다시 받는다(런북).

5. **비밀은 하나다.** 엣지 설정은 Worker 배포와 같은 계정 토큰(`cloudflare-deploy` 그룹)으로 한다. 버킷의 S3 열쇠는 그 토큰에서 낸다(id,
   SHA-256). 새 토큰·새 버킷 열쇠·사람이 보관하는 passphrase 가 없다.

6. **터널 토큰은 사람이 옮기지 않는다.** `apply` 뒤 `tofu output -raw tunnel_token` 을 실행기가 받아 런타임 비밀 계약의 `set` 으로
   `console-tunnel` 파일에 쓴다(값은 출력하지 않는다). 값이 바뀌었으면 `foundation-release.sh console` 로 더니어와 `cloudflared` 를 바로
   띄운다 — 다음 배포를 기다리지 않는다.

## 소유자가 하는 일

없다. 위 실측대로 필요한 토큰이 이미 호스트에 있다.

토큰을 언젠가 바꿔야 할 때(만료·유출)만 아래가 필요하다. 이것은 사고 대응이고 평상시 단계가 아니다.
1. Cloudflare 대시보드 → 오른쪽 위 **My Profile** → **API Tokens** → **Create Token** → **Create Custom Token**. 이름
   `foundation-platform-workers-writer-prod`(같은 이름이면 변수 이름이 그대로다, ADR-0247).
2. Permissions: 지금 토큰의 Workers 권한 그대로 + **Account · Cloudflare Tunnel · Edit**, **Account · Access: Apps and Policies · Edit**,
   **Account · Access: Organizations, Identity Providers, and Groups · Edit**, **Account · Workers R2 Storage · Edit**, **Zone · DNS · Edit**,
   **Zone · Zone · Read**. Account resources: 이 계정만. Zone resources: `perfectory.io` 만.
3. 서버에 넣는 길은 이미 있는 런타임 비밀 경로 하나다(값은 채팅·명령줄·셸 기록에 남지 않는다):
   `ssh -t ai-server "umask 077; cat > /tmp/cf.env; sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set cloudflare-deploy /tmp/cf.env; rm -f /tmp/cf.env"`
   붙여 넣을 한 줄: `FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN=<토큰>` → Enter → Ctrl+D.

## Consequences

- ADR-0434 의 소유자 단계 1–4 가 없어진다. 남은 유일한 사람 쪽 결정은 "누가 직원인가"이고, 그것은 이미 신원 플랫폼의 역할 부여다.
- 첫 적용(계획, 2026-11-07 노트북에서 실계정에 대해 상태 없이 `plan` 만): 가져오기 1(조직), 추가 6, 변경 1(조직 이름), 삭제 0.
- 비용: Zero Trust Free 0원(50명까지), Tunnel 0원, DNS 0원, 상태 객체 하나(수 KB)의 R2 비용은 무료 한도 안이다. 호스트: 적용 때만 512m
  컨테이너 하나, Worker 배포 뒤에.
- 위험: 계정 토큰 하나가 Workers·R2·엣지 접근 정책을 모두 바꿀 수 있다. 이미 그 범위였고(실측), 이 결정은 범위를 넓히지 않는다. 좁히는 것
  (엣지 전용 토큰)은 소유자 단계를 하나 늘리므로 지금은 하지 않는다; 토큰을 교체할 때 두 개로 나눈다.
- 조사 중 의도하지 않게 Zero Trust 조직을 만들었다(Context). 결과는 이 결정이 만들려던 상태와 같고, 코드는 그것을 `import` 로 받는다.
- 출처: [Cloudflare Terraform provider — Zero Trust](https://developers.cloudflare.com/cloudflare-one/api-terraform/),
  [R2 — S3 credentials from an API token](https://developers.cloudflare.com/r2/api/tokens/),
  [OpenTofu S3 backend `use_lockfile`](https://opentofu.org/docs/language/settings/backends/s3/),
  [OpenTofu state encryption](https://opentofu.org/docs/language/state/encryption/),
  [Zero Trust plans](https://www.cloudflare.com/plans/zero-trust-services/).
