---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-27
---

# AWS 플랫폼 계정의 바탕 — 예산·상태 버킷·망을 노트북에서 적용한다

[루트 ADR-0619](../../../../docs/adr/0619-the-aws-platform-account-is-one-opentofu-stack-pinned-to-its-account-with-its-budget-first-and-images-from-ghcr.md).
데이터 작업이 옮겨 갈 계정(루트 ADR-0616, 검토 중)의 바탕이다. 계정·이름·크기·이미지의 자리는 `config/aws-platform.contract.json` 이
정하고, `infra/aws` 는 그것과 버전 계약(`tools/technology-versions.contract.json`)·호스트와 같은 SQL 파일을 읽는다. 포트·시각 같은 기계적인
수는 `.tf` 에 있다. 값을 바꾸려면 PR 로 바꾸고, 병합된 main 에서 아래대로 적용한다. 호스트는 이 스택을 적용하지 않는다.

## 결론

| 무엇 | 어디 | 누가 만드나 |
| --- | --- | --- |
| 월 예산 `perfectory-platform-monthly`(한도 200, 실제 100·200, 예측 200 USD 를 넘으면 메일) | AWS Budgets | 이 스택. 계정의 첫 자원 |
| 상태 버킷 `perfectory-platform-tofu-state-091109217099` | S3, 같은 계정 | 이 스택(처음 한 번은 아래 절차) |
| VPC `perfectory-platform`(/16, 주소 블록은 적용 때 `TF_VAR_vpc_cidr`), 공개 서브넷 둘(태스크), 사설 서브넷 둘(DB), 인터넷 게이트웨이 | VPC, ap-northeast-2a·2c | 이 스택. NAT·ALB 없음. VPC·서브넷은 `prevent_destroy` |
| Dagster(A1, [루트 ADR-0616](../../../../docs/adr/0616-data-jobs-run-as-dagster-assets-on-aws-ecs-and-the-host-job-layer-is-not-ported.md)): ECS 클러스터 `perfectory-platform`, 늘 켠 서비스 둘 `dagster-webserver`·`dagster-daemon`(각자 태스크, 크기는 계약의 `dagster.services`: 지금 각 Fargate ARM 0.25 vCPU / 0.5 GB), run 마다 태스크(`EcsRunLauncher`: 레인이 역할을 주지 않으면 정책 없는 기본 run 역할, 동시에 10 개·수집 3·Silver 1 까지) | ECS, 공개 서브넷(들어오는 규칙 없음) | 이 스택(`infra/aws/dagster.tf`). 이미지는 적용할 릴리스 커밋 하나의 확인한 기록에서(아래 '이미지 고르기'). 일은 아직 없다 |
| Dagster 의 코드 위치: 위치마다 서비스 `code-<위치>`(gRPC 코드 서버, Fargate ARM 0.25 vCPU / 0.5 GB), 웹서버·데몬은 `<위치>.perfectory-platform.internal`(Cloud Map) 로 부른다 | ECS, 공개 서브넷(같은 보안 그룹 안에서만 그 포트) | 이 스택(`infra/aws/code-servers.tf`, 계약의 `dagster.code_locations`). 코드 릴리스는 코드 서버만 다시 띄운다. 웹서버·데몬의 이미지는 Dagster 의 것(`Dockerfile.aws-dagster`)이라 그 입력이 바뀔 때만 바뀐다 |
| Dagster 의 저장소: RDS PostgreSQL 17 `perfectory-platform`(db.t4g.micro, 20 GB 에서 스스로 100 GB 까지, 사설 서브넷만) | RDS. Dagster 와 그 run 은 이 데이터베이스만 가진 역할 `dagster` 로 붙는다(비밀 `perfectory-platform/dagster-database`, 상태 파일에 없다). master 의 비밀은 부트스트랩·마무리 태스크만 읽는다 | 이 스택. 삭제 보호·마지막 스냅숏·`prevent_destroy` |
| Bronze 원장의 자리(A1b): 같은 인스턴스의 데이터베이스 `foundation`. 데이터 호스트와 같은 스키마(같은 sqlx 마이그레이션), 빈 채로 | RDS. 마이그레이터 `foundation_migrator`(스키마의 주인)와 런타임 역할 `foundation_api`(줄만), 비밀은 각자 (`perfectory-platform/foundation-migrator`·`foundation-runtime`, 상태 파일에 없다) | 이 스택(`infra/aws/foundation-database.tf`). 스키마는 적용이 마이그레이션한다(아래 '배포 전 단계') |
| 경보 `perfectory-platform-dagster-webserver-not-running`·`perfectory-platform-dagster-daemon-not-running`(계약 `alarms.not_running.window_minutes` 분 내내 그 서비스의 태스크가 없으면: 배포가 태스크 없이 두는 2 분 22 초보다 길다), 늘 켠 서비스마다 `perfectory-platform-<서비스>-{memory,cpu}-near-limit`(15 분 동안 태스크 크기의 계약 `alarms.near_limit` 몫을 넘으면: 잰 크기를 계약에서 올린다)·`perfectory-platform-database-storage-low`(디스크가 늘지 못하고 1 GiB 밑), Dagster 의 센서 둘(run 실패: job·run id, 신선도 FAIL: 자산 키; `orchestration/dagster/aws/alerts.py`) → SNS `perfectory-platform-alarms` → Slack 채널 하나. OK 는 경보 자신이 보내지 않고 EventBridge 규칙 `perfectory-platform-alarms-ok-after-alarm` 이 ALARM 에서 OK 로 갈 때만 보낸다(새 경보가 INSUFFICIENT_DATA 를 떠날 때는 보내지 않는다, 계약 `alarms.ok_reason`) | CloudWatch·SNS(ap-northeast-2). 채널 묶음 AWS Chatbot `perfectory-platform-alarms` 는 그 서비스의 제어 리전 us-east-2 | 이 스택(`infra/aws/chatbot.tf`). 저장하는 토큰은 없다. 사장이 Slack 작업 공간을 콘솔에서 한 번 허가한다(아래 'Slack 으로 잇기'). 작업 공간·채널 ID 는 적용 때 `TF_VAR_slack_team_id`·`TF_VAR_slack_channel_id` |
| 예산 메일 주소 | 적용할 때의 환경 변수 `TF_VAR_budget_alert_emails` | 사람이 적용할 때 준다. 파일·저장소에 적지 않는다(공개 저장소) |
| 프런트 도어의 CloudFront 전용 예산 `perfectory-cdn-free-tier` | AWS Budgets | `infra/cdn`(호스트의 엣지 적용, [엣지 설정](./edge-settings.md)) |

## 적용 (병합된 main 에서)

OpenTofu 는 버전 계약의 판이다(`tools/technology-versions.contract.json` 의 `ghcr.io/opentofu/opentofu:<판>`; 모든 스택의 `required_version` 이
그 판이고 `scripts/guard/container-images-match-the-contract.sh` 가 같은지 본다). 노트북의 `tofu` 도 그 판을 설치한다. 작업 디렉터리는 저장소 밖에 둔다.
`gh` 는 로그인해 있어야 한다(서명 확인).

```bash
cd platforms/foundation-platform/infra/aws
export AWS_PROFILE=perfectory-platform-prod-admin
export TF_DATA_DIR="$HOME/.cache/perfectory/tofu/aws-platform"
# 1) 계정 확인: 계약의 account.id 가 아니면 여기서 멈춘다(공급자의 allowed_account_ids 가 같은 것을 다시 막는다).
test "$(aws sts get-caller-identity --query Account --output text)" = \
     "$(python3 -c 'import json;print(json.load(open("../../config/aws-platform.contract.json"))["account"]["id"])')"
# 2) 예산 메일 주소: 계정의 등록 주소를 JSON 목록으로. 화면 기록·파일에 남기지 않는다.
export TF_VAR_budget_alert_emails='["<계정의 등록 주소>"]'
# 3) VPC 의 주소 블록: VPC 가 있으면 AWS 에서 읽어 온다(공개 저장소에는 사설 주소를 적지 않는다).
#    처음 한 번만 사람이 고른다: 우리가 잇는 어떤 망과도 겹치지 않는 RFC 1918 의 /16.
export TF_VAR_vpc_cidr="$(aws ec2 describe-vpcs --filters Name=tag:Name,Values=perfectory-platform   --query 'Vpcs[0].CidrBlock' --output text)"
test "$TF_VAR_vpc_cidr" != None   # 처음 적용이면 위 줄 대신 고른 블록을 넣는다
# 4) 이미지: 돌릴 릴리스 커밋 하나. 그 기록과 모든 이미지의 서명을 확인한 뒤에만 digest 를 적는다(아래 '이미지 고르기').
python3 ../../../../scripts/deploy/aws-release-images.py <돌릴 main 커밋> --out "$TF_DATA_DIR/release.tfvars.json"
# 5) 경보의 Slack 작업 공간과 채널: 이은 설정에서 읽어 온다(처음이면 아래 'Slack 으로 잇기').
chatbot() { aws chatbot describe-slack-channel-configurations --region us-east-2 \
  --query "SlackChannelConfigurations[?ConfigurationName=='perfectory-platform-alarms'] | [0].$1" --output text; }
export TF_VAR_slack_team_id="$(chatbot SlackTeamId)" TF_VAR_slack_channel_id="$(chatbot SlackChannelId)"
tofu init -input=false
tofu plan -input=false -var-file="$TF_DATA_DIR/release.tfvars.json" -out="$TF_DATA_DIR/plan"
tofu apply "$TF_DATA_DIR/plan"
rm -f "$TF_DATA_DIR/plan"   # 계획 파일에는 변수 값이 든다
```

## 처음 한 번 (상태 버킷이 아직 없을 때)

백엔드가 가리키는 버킷을 이 스택이 만들므로 첫 적용만 로컬 상태로 하고 옮긴다. 첫 자원은 예산이다.

```bash
cat > backend_override.tf <<EOF   # *_override.tf 는 .gitignore 에 있다
terraform {
  backend "local" {
    path = "$TF_DATA_DIR/bootstrap.tfstate"
  }
}
EOF
tofu init -input=false
tofu apply -input=false -target=aws_budgets_budget.monthly
tofu apply -input=false
rm backend_override.tf
tofu init -input=false -migrate-state -force-copy
tofu plan -input=false -detailed-exitcode   # 0 = 바뀔 것 없음. 옮긴 상태가 맞다는 증거
rm -f "$TF_DATA_DIR/bootstrap.tfstate" "$TF_DATA_DIR/bootstrap.tfstate.backup"
```

## 배포 전 단계 (적용마다, 저절로)

서비스가 새 태스크 정의로 옮기기 전에 일회성 태스크 둘이 돈다(루트 ADR-0616 §2-4, `infra/aws/foundation-database.tf`). 각 태스크가 멈추고
그 모든 컨테이너가 끝 코드 0 이어야 다음으로 간다(`infra/aws/pre-deploy.sh`). 하나라도 아니면 적용이 거기서 실패하고 서비스는 옛 태스크
정의에 그대로 있다. 사람이 따로 돌리는 단계는 없다. 다음 걸음(A2 뒤, 코디네이터 2026-11-26): 적용 자체도 사람이 돌리지 않는다. main 의
CI 가 GitHub OIDC 역할(오래 사는 키 없음)로 적용한다.

| 순서 | 태스크(컨테이너) | 무엇 | 누구로 | 언제 도나 |
| --- | --- | --- | --- | --- |
| 1 | `perfectory-platform-database-bootstrap` | `database-bootstrap.sql`(Dagster 의 역할), `foundation-bootstrap.sql`(데이터베이스 `foundation`, 역할 셋과 비밀번호, 스키마, PostGIS) | master | 그 태스크 정의(SQL 이 그 안에 있다)나 세 비밀번호의 판이 바뀔 때 |
| 2 | `perfectory-platform-database-migrate`: `open` → `migrate` → `grants` → `finalize`, 앞 것이 0 으로 끝나야 다음이 시작 | `foundation-migrate-open.sql`(CREATE 를 준다), 런타임 이미지의 `foundation-migrate`(`migrations/` 전부), `infra/compose/grant-foundation-runtime.sql`, `infra/compose/finalize-foundation.sql`(CREATE 를 거두고 역할을 검사) | master, 마이그레이터, 마이그레이터, master | 그 태스크 정의가 바뀔 때. 런타임 이미지가 그 안에 있다 |

- 코드 위치 이미지만 바뀐 적용(되돌리기 포함)에서는 둘 다 돌지 않는다. `tofu plan` 에 `terraform_data.database_bootstrap`·
  `terraform_data.database_migrate must be replaced` 로 보이는 것이 도는 것이다.
- 마이그레이션은 앞 릴리스를 깨지 않는다(expand/contract, ADR-0616 §2-5): 지우기·이름 바꾸기·형 바꾸기·NOT NULL 은 그것을 읽던 코드가
  떠난 뒤의 마이그레이션에서, 그 문장 위의 `-- squawk-ignore <규칙>` 과 이유로. PR 의 lint(`scripts/ci/lint_changed.py`, squawk)가 본다.
- 기다림: 태스크 하나마다 계약의 `database.one_off_wait_minutes` 까지. 그 뒤에는 적용이 실패하지만 태스크는 계속 돈다. 다음 적용은
  마이그레이터의 잠금(sqlx)을 기다린다.
- 실패하면: 적용 출력의 `FAIL pre-deploy: <태스크> (task <ID>, <stopCode>) has containers that did not exit 0` 아래의 컨테이너 이름과 끝 코드를
  본다(시작하지 못한 컨테이너는 `None`). 로그는 `/perfectory-platform/dagster` 의 `database-bootstrap/…`·`database-migrate/<컨테이너>/…` 다.
  스크립트는 로그를 찍지 않는다(psql 의 오류가 비밀번호를 담을 수 있다). 고친 PR 을 병합하고 다시 적용한다. 실패한 단계는 tainted 로 남아
  다음 적용이 다시 돌린다.
- 비밀번호를 바꿀 때: 그 비밀의 `secret_string_wo_version` 을 하나 올리는 PR 을 병합하고 적용한다. 부트스트랩이 새 비밀번호를 맞춘다.
  Dagster 의 비밀번호라면 같은 적용이 그 뒤에 서비스를 다시 띄운다(서비스의 `triggers`). ECS 는 비밀을 태스크가 뜰 때 넣고, run 태스크는 뜰 때마다 읽는다.

## 이미지 고르기 (적용마다)

서비스와 그 run 태스크는 릴리스의 AWS 코드 위치 이미지를, 마이그레이션은 런타임 이미지를 digest 로 돈다(루트 ADR-0620, ADR-0616 §2-4).
입력은 릴리스 커밋 하나다. 그 커밋의 릴리스 워크플로 `aws-images` 잡이 모든 이미지의 digest 를 기록(`aws-images.json`)에 적고, 이미지와
함께 서명하고, `<commit>-aws-images` 로 올린다. `scripts/deploy/aws-release-images.py` 가:

1. 그 기록을 레지스트리에서 익명으로 읽는다.
2. 기록과 기록이 적은 모든 이미지를 `gh attestation verify` 로 확인한다: 계약(`images`)의 저장소·워크플로·ref 가 그 커밋에서
   (`--source-digest`), GitHub 러너에서 서명했는가. 하나라도 아니면 아무것도 적지 않고 멈춘다.
3. `dagster_image`(코드 위치)와 `runtime_image`(런타임)를 digest 로 `--out` 에 적는다. 스택은 계약의 저장소 밖 이미지를 거부한다.

코드 릴리스는 코드 위치의 서비스만 옮긴다: 새 코드 서버가 뜨고 이름에 오른 뒤 옛 것이 멈춘다(위치는 늘 코드 서버를 가진다). Dagster
서비스 둘(웹서버·데몬)은 Dagster 이미지가 바뀔 때만 옮긴다. 웹서버는 새 태스크가 뜬 뒤 옛 것이 멈추고(UI 가 끊기지 않는다), 데몬은 옛 태스크를
멈춘 뒤 새 태스크를 띄운다(데몬은 언제나 하나). 그 사이 몇 분은 예약·센서의 틱이 없다. 진행 중인 run 태스크는 어느 쪽이든 그대로 돈다.
적용은 새 태스크가 도는 것을 기다린다(`wait_for_steady_state`). ECS 가 배포를 되돌리면 적용이 실패한다: 초록 적용은 새 태스크가 돈다는 뜻이다.

## Slack 으로 잇기 (AWS Chatbot, 처음 한 번)

경보는 SNS `perfectory-platform-alarms` 에서 Slack 채널 하나로 간다. 저장하는 토큰은 없다. Slack 작업 공간을 AWS 에 허가하는 것만
사람이 콘솔에서 한 번 한다(Slack 의 OAuth). 채널 묶음과 어느 경보가 가는지는 코드다(`infra/aws/chatbot.tf`, 주제에 알리는 경보).
이 서비스는 서울에 제어 평면이 없다. 콘솔은 us-east-2 에만 있고, 설정도 us-east-2 로 다룬다(계약의 `alarms.chatbot`).
주제·경보·다른 모든 자원은 ap-northeast-2 에 있다. 바깥으로 나가는 것은 채널 설정과 알림뿐이다.

**규칙(코디네이터 2026-11-26).** 이 서비스는 알림을 네 리전(서울 밖) 어디서든 두고 처리한다. 그래서 이 주제에는 개인 정보나
사업 데이터를 싣지 않는 경보만 알린다. 경보의 이름·상태·지표와 플랫폼 자원의 이름 같은 메타데이터다(예: 'Dagster 가 돌지 않는다').
데이터를 실을 경보는 이 주제를 쓰지 않는다. 주제에 경보를 더하는 PR 은 이 규칙을 지키는지 본다.

사장이 하는 것(한 번):

1. AWS 콘솔에 로그인하고 역할 전환으로 플랫폼 계정에 들어간다:
   `https://signin.aws.amazon.com/switchrole?account=091109217099&roleName=OrganizationAccountAccessRole&displayName=perfectory-platform-prod`.
   오른쪽 위 계정 번호가 0911-0921-7099 인지 본다. 관리 계정에서는 하지 않는다.
2. `https://console.aws.amazon.com/chatbot/` 에서 채팅 클라이언트로 **Slack** 을 고르고 **Configure** 를 누른다.
3. Slack 의 허가 화면 오른쪽 위에서 작업 공간을 고르고 **Allow** 를 누른다. 채널 구성(**Configure new channel**)은 누르지 않는다. 코드가 만든다.
4. Slack 에서 경보를 받을 채널에 `/invite @Amazon Q` 를 입력하고 **Invite Them**. 채널 이름을 오른쪽 클릭해 링크를 복사해 보낸다
   (링크 끝의 `C…` 가 채널 ID 다).

적용하는 사람이 하는 것(작업 공간 ID 는 AWS 에서 읽는다):

```bash
export TF_VAR_slack_team_id="$(aws chatbot describe-slack-workspaces --region us-east-2 --query 'SlackWorkspaces[0].SlackTeamId' --output text)"
export TF_VAR_slack_channel_id=<받은 링크 끝의 C…>
# 위 '적용' 그대로(plan → apply). 그다음 경보를 한 번 일부러 울려 채널에 뜨는지 본다.
# 경보는 다음 평가(1 분 안)에 실제 상태로 돌아가고, 그 OK 도 채널에 뜬다(ALARM 에서 OK 로 가는 규칙의 확인).
aws cloudwatch set-alarm-state --alarm-name perfectory-platform-dagster-daemon-not-running --state-value ALARM --state-reason 'Slack 연결 확인'
```

Slack 이 앱 승인을 요구하면 작업 공간 관리자가 승인한다. 채널을 바꾸려면 `TF_VAR_slack_channel_id` 만 바꿔 적용한다.

## 자격 발급 창구 (레이크하우스 자격, ADR-0623 §2)

AWS 의 일이 레이크하우스(R2)에 닿는 길은 함수 하나다(`tofu output -raw credential_vendor`). Cloudflare 토큰 둘은 그 함수만 읽는다.
레인마다 그 함수의 alias 와 run 역할이 하나씩 있다(`tofu output lane_roles`, 경로 `/perfectory-platform/run/`). 레인의 run 은 제 alias 만 부른다.

- `pin`: run 마다 한 번 부른다. 레인이 읽는 표들의 현재 스냅숏을 준다(`metadata_location`·manifest list). run 은 끝까지 그 스냅숏을 읽는다.
- `credentials`: R2 임시 자격 증명과 만료(`expiration`)를 준다. 쓰기는 레인의 쓰기 접두사에만, 읽기는 그 표들의 위치에만 된다.
  유효 시간은 계약의 `collect.credential_ttl_seconds` 하나다. 범위가 비면 거부하고, 버킷 전체로 넓히지 않는다.
- 접근은 태스크가 끝나고 한 유효 시간 안에 끝난다. 한 자격을 일찍 철회하는 길은 R2 에 없다.

### 사장이 한 번 하는 것: Cloudflare 토큰 둘을 넣기

토큰 둘은 코디네이터가 Cloudflare 에서 AWS 전용으로 만든다.
- 민팅: `Workers R2 Storage Bucket Item Write`, 레이크하우스 버킷 하나.
- 카탈로그 읽기: `Workers R2 Data Catalog Read` + `Workers R2 Storage Bucket Item Read`, 같은 버킷.

그 값을 넣는 것은 사장이 노트북의 PowerShell 에서 한다. 값은 화면·명령 기록·디스크에 남지 않는다.
비밀 이름은 `tofu output credential_vendor_secrets` 와 같다. 민팅 비밀은 `<계정 ID>:<토큰>` 한 줄이다. 따옴표가 없어 PowerShell 5.1 이 그대로 넘긴다.

```powershell
$account = Read-Host 'Cloudflare 계정 ID'
$secure = Read-Host '민팅 토큰' -AsSecureString
$value = $account + ':' + [System.Net.NetworkCredential]::new('', $secure).Password
aws secretsmanager put-secret-value --profile perfectory-platform-prod-admin --region ap-northeast-2 --secret-id perfectory-platform/r2-mint-token --secret-string $value --query VersionId --output text
$secure = Read-Host '카탈로그 읽기 토큰' -AsSecureString
$value = [System.Net.NetworkCredential]::new('', $secure).Password
aws secretsmanager put-secret-value --profile perfectory-platform-prod-admin --region ap-northeast-2 --secret-id perfectory-platform/r2-catalog-reader-token --secret-string $value --query VersionId --output text
Remove-Variable value, secure, account
```

넣기 전에는 함수가 `inert` 라고 답한다. 레인은 그것을 실패하는 검사로 보인다. 토큰을 바꿀 때도 같은 명령이다. 다음 부름부터 새 값을 쓴다.

### 확인과 비상

```bash
# 함수의 기록: 부름마다 한 줄 {"vend": {lane, run_id, call, expiration | inert | refused}}. 값은 없다.
MSYS_NO_PATHCONV=1 aws logs tail "/aws/lambda/$(tofu output -raw credential_vendor)" --since 1h
```

- 거부: 레인의 run 은 `refused` 의 문장으로 실패한다(HTTP 코드와 Cloudflare 오류 코드만, 값은 없다).
- 비상(토큰이 샌 것 같을 때): Cloudflare 에서 그 부모 토큰을 돌리고(roll) 새 값을 위 명령으로 넣는다.
  이미 발급된 임시 자격이 그것으로 죽는지는 확인되지 않았다(ADR-0623 §2-6). 늦어도 한 유효 시간 뒤에 끝난다.

## Dagster 의 UI 열기 (SSM 포트 포워딩)

공개 주소도 부하 분산기도 없다. ECS Exec 의 SSM 세션으로 노트북의 포트를 웹서버로 잇는다. AWS CLI 의 Session Manager 플러그인이 있어야 한다.

```bash
task="$(aws ecs list-tasks --cluster perfectory-platform --service-name dagster-webserver --query 'taskArns[0]' --output text)"
runtime="$(aws ecs describe-tasks --cluster perfectory-platform --tasks "$task" \
  --query "tasks[0].containers[?name=='webserver'].runtimeId | [0]" --output text)"
aws ssm start-session --target "ecs:perfectory-platform_${task##*/}_$runtime" \
  --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["3000"],"localPortNumber":["3000"]}'
# 그다음 브라우저에서 http://127.0.0.1:3000
```

## 확인 (읽기만)

```bash
aws budgets describe-budget --account-id 091109217099 --budget-name perfectory-platform-monthly \
  --query 'Budget.[BudgetLimit.Amount,CalculatedSpend.ActualSpend.Amount]' --output text
aws budgets describe-notifications-for-budget --account-id 091109217099 --budget-name perfectory-platform-monthly \
  --query 'Notifications[].[NotificationType,Threshold]' --output text
aws s3api get-bucket-versioning --bucket perfectory-platform-tofu-state-091109217099
aws ecs describe-services --cluster perfectory-platform --services dagster-webserver dagster-daemon \
  --query 'services[].[serviceName,runningCount,deployments[0].rolloutState]' --output text   # 서비스마다 1 COMPLETED
aws cloudwatch describe-alarms --alarm-names perfectory-platform-dagster-webserver-not-running perfectory-platform-dagster-daemon-not-running \
  --query 'MetricAlarms[].[AlarmName,StateValue]' --output text   # 둘 다 OK (처음 10 분은 INSUFFICIENT_DATA 이거나 ALARM 일 수 있다)
aws rds describe-db-instances --db-instance-identifier perfectory-platform \
  --query 'DBInstances[0].[DBInstanceStatus,PubliclyAccessible,StorageEncrypted]' --output text   # available False True
tofu plan -input=false -detailed-exitcode   # 0 이면 코드와 계정이 같다
```

## 되돌리기

- 예산·망: 계약이나 `main.tf` 를 되돌리는 PR 을 병합하고 위의 적용을 한다. 망을 지우는 적용은 그 안에 태스크·DB 가 없을 때만 된다.
- 상태: 버킷은 버전을 지닌다(지난 판은 90일). 잘못된 적용 뒤의 상태는 그 객체의 앞 판으로 되돌린다.
- Slack: 채널 묶음을 끊으려면 `infra/aws/chatbot.tf` 를 지우는 PR 을 병합하고 적용한다. 경보는 CloudWatch 에 남는다.
- Dagster: 코드 위치만 앞 릴리스로 되돌린다. 런타임은 지금 것 그대로 둔다:
  `aws-release-images.py <앞 릴리스 커밋> --runtime-commit <지금 커밋> --out "$TF_DATA_DIR/release.tfvars.json"`, 그다음 위의 plan → apply.
  런타임이 그대로라 마이그레이션 태스크는 돌지 않고, 앞 릴리스의 코드는 새 스키마에서 돈다(expand/contract). 마이그레이션은 앞으로만 간다:
  스키마를 되돌리는 것은 데이터 호스트처럼 고치는 새 마이그레이션이다.
  서비스를 멈추려면 `desired_count` 를 0 으로 하는 PR 을 병합하고 적용한다.
- 데이터베이스는 삭제 보호와 `prevent_destroy` 다. 지우는 일은 이 런북이 하지 않는다. 마지막 스냅숏 `perfectory-platform-final` 이 남는다.
- 상태 버킷은 `prevent_destroy` 다. 스택 전체를 내리는 일은 이 런북이 하지 않는다.
