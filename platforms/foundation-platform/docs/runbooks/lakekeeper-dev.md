---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-27
---

# Lakekeeper dev 리그 — 카탈로그 전환 관문 시험을 AWS 의 dev 에서 돌린다

W109(코디네이터 2026-11-27). 사장은 카탈로그의 끝 상태를 자체 호스팅 오픈소스 Lakekeeper 로 정했다(옵션 C, dev 시험이 통과하면).
이 리그는 그 시험을 돌리는 곳이고 dev 에만 있다. 시험이 끝나면 내린다. 카탈로그 ADR(0624)은 시험 뒤에 쓴다.

## 결론

| 무엇 | 어디 | 누가 만드나 |
| --- | --- | --- |
| ECS 클러스터 `perfectory-lakekeeper-dev` | 플랫폼 계정(서울), 플랫폼 VPC 의 공개 서브넷(들어오는 규칙은 아래 보안 그룹뿐) | `infra/aws-lakekeeper-dev`(상태는 플랫폼 상태 버킷의 `lakekeeper-dev/terraform.tfstate`) |
| 카탈로그: Lakekeeper(버전 계약의 `quay.io/lakekeeper/catalog`) 태스크 둘, `catalog.perfectory-lakekeeper-dev.internal:8181` | Fargate ARM 0.25 vCPU / 0.5 GB. 그 포트는 탐침의 보안 그룹에서만 | 같은 스택(`catalog.tf`). `lakekeeper migrate` 는 서비스가 옮기기 전의 일회성 태스크 |
| 권한: OpenFGA(버전 계약의 `ghcr.io/openfga/openfga`) 태스크 하나, `authz.perfectory-lakekeeper-dev.internal:8081`(gRPC, 사전 공유 키) | 같은 클러스터. 그 포트는 카탈로그에서만 | 같은 스택(`authz.tf`). `openfga migrate` 가 먼저, 그다음 Lakekeeper 의 migrate 가 모델을 쓴다 |
| 데이터베이스 `lakekeeper_dev`·`openfga_dev`, 로그인도 저마다 | 플랫폼의 RDS 인스턴스(Dagster·원장과 다른 데이터베이스와 로그인, 연결 수 제한) | 같은 스택의 부트스트랩 태스크(`database-bootstrap.sql`, master 로) |
| 기계의 신원: Cognito 사용자 풀 `perfectory-lakekeeper-dev`, 주체마다 앱 클라이언트(client credentials) | Cognito(서울). 토큰 끝점 `perfectory-lakekeeper-dev.auth.ap-northeast-2.amazoncognito.com` | 같은 스택(`identity.tf`). 주체는 계약 `config/lakekeeper-dev.contract.json` 의 `identity.principals` |
| 탐침: 주체마다 태스크 정의 하나(`perfectory-lakekeeper-dev-probe-<주체>`, 익명 하나 더) | 같은 클러스터. 주체의 비밀은 그 탐침의 실행 역할만 넣는다 | 같은 스택(`probes.tf`, `probes/probe.py`). `probe.sh` 가 한 걸음을 돌리고 결과 줄을 찍는다 |
| 창고(warehouse) | 아직 없다 | R2 의 저장 자격(부모 토큰)은 사장의 결정을 기다린다. 기존 토큰은 어디에도 복사하지 않는다. 창고는 `foundation-platform-lakehouse-dev` 버킷에만 둔다 |

## 적용 (노트북)

```bash
cd platforms/foundation-platform/infra/aws-lakekeeper-dev
export AWS_PROFILE=perfectory-platform-prod-admin
export TF_DATA_DIR="$HOME/.cache/perfectory/tofu/lakekeeper-dev"
tofu init -input=false
tofu plan -input=false -out="$TF_DATA_DIR/plan"   # 플랫폼 스택의 자원은 바뀌지 않는다: 데이터베이스 보안 그룹에 들어오는 규칙 셋만 더한다
tofu apply "$TF_DATA_DIR/plan"
rm -f "$TF_DATA_DIR/plan"
```

적용은 일회성 태스크 셋을 차례로 돌리고 기다린다(`infra/aws/pre-deploy.sh`): 데이터베이스 부트스트랩 → OpenFGA migrate → OpenFGA
서비스 → Lakekeeper migrate → Lakekeeper 서비스. 하나라도 0 이 아니면 적용이 거기서 멈춘다. 로그는 `/perfectory-lakekeeper-dev` 다.

처음 적용 뒤 한 번: 운영자(부트스트랩 주체)가 Lakekeeper 를 부트스트랩한다. 다른 주체가 먼저 부르면 그 주체가 관리자가 되므로 바로 한다.

```bash
./probe.sh bootstrap bootstrap      # 204
./probe.sh bootstrap info           # bootstrapped true, authz-backend openfga
```

## 사장이 한 번 하는 것: dev 창고의 R2 토큰을 넣기

사장 결정 2026-11-27(A): 이 리그만 쓰는 **새** Cloudflare 계정 API 토큰이다. 기존 `CLOUDFLARE_API_TOKEN` 은 쓰지 않는다.

1. Cloudflare 대시보드 → R2 Object Storage → Overview → `{} API` 의 **Manage API tokens** → **Create Account API token**.
   이름은 `perfectory-lakekeeper-dev`, 권한은 **Admin Read & Write**(Lakekeeper 문서: R2 의 임시 자격 끝점이 이것만 받는다).
   만든 화면의 **Token value**·**Access Key ID**·**Secret Access Key** 셋과 계정 ID(R2 Overview 오른쪽의 Account ID)를 넣는다.
2. 새 PowerShell 창에서(값은 화면·명령 기록·디스크에 남지 않는다):

```powershell
$account = Read-Host 'Cloudflare 계정 ID'
$keyId = Read-Host 'Access Key ID' -AsSecureString
$secretKey = Read-Host 'Secret Access Key' -AsSecureString
$token = Read-Host 'Token value' -AsSecureString
$plain = { param($s) [System.Net.NetworkCredential]::new('', $s).Password }
$value = $account + ':' + (& $plain $keyId) + ':' + (& $plain $secretKey) + ':' + (& $plain $token)
aws secretsmanager put-secret-value --profile perfectory-platform-prod-admin --region ap-northeast-2 --secret-id perfectory-lakekeeper-dev/r2-warehouse-credential --secret-string $value --query VersionId --output text
Remove-Variable value, token, secretKey, keyId, account, plain
```

비밀은 `<계정 ID>:<Access Key ID>:<Secret Access Key>:<Token value>` 한 줄이다. 따옴표가 없어 PowerShell 5.1 이 그대로 넘긴다.
그 비밀은 창고 태스크(`perfectory-lakekeeper-dev-probe-warehouse` 의 실행 역할)만 읽는다. 비밀 자체의 정책이 다른 모든 주체의 읽기를
거부한다(적용하는 관리자도 `AccessDeniedException`). Lakekeeper 는 AWS 의 비밀을 읽지 않는다. 창고 태스크가 창고를 만들 때 한 번 넘기고,
Lakekeeper 는 그것을 제 데이터베이스에 암호화해 둔다(`catalog-encryption-key`).

넣은 뒤 창고를 만든다(한 번, 다시 돌려도 있으면 그대로 둔다):

```bash
./probe.sh warehouse warehouse-create     # 창고 foundation-platform-lakehouse-dev, 네임스페이스는 그다음
```

## 탐침

```bash
./probe.sh <주체> <걸음> [인자...]   # 주체: bootstrap, lane-a, lane-b, outsider, anonymous
```

걸음(`probes/probe.py`): `health`, `info`, `claims`, `bootstrap`, `whoami`, `identity`, `expiry`, `authz-denied`, `authz-role-creator`,
`grant <권한> <주체>`, `revoke <권한> <주체>`, `refused`, `loop <초>`. 줄마다 JSON 하나이고 토큰·비밀·응답 본문은 찍지 않는다.
끝 코드 0 은 모든 검사가 맞았다는 뜻이다.

## 내리기

```bash
tofu destroy -input=false
```

데이터베이스 둘과 로그인은 RDS 에 남는다(부트스트랩이 만든 것이고 OpenTofu 의 자원이 아니다). 내린 뒤 master 로 `DROP DATABASE lakekeeper_dev; DROP DATABASE openfga_dev; DROP ROLE lakekeeper_dev; DROP ROLE openfga_dev;`
를 일회성 태스크로 돌린다. 비밀은 복구 기간 없이 바로 지워진다(dev).
