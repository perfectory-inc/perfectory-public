---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-26
---

# AWS 플랫폼 계정의 바탕 — 예산·상태 버킷·망을 노트북에서 적용한다

[루트 ADR-0619](../../../../docs/adr/0619-the-aws-platform-account-is-one-opentofu-stack-pinned-to-its-account-with-its-budget-first-and-images-from-ghcr.md).
데이터 작업이 옮겨 갈 계정(루트 ADR-0616, 검토 중)의 바탕이다. `infra/aws/main.tf` 는 `config/aws-platform.contract.json` 만 읽는다.
값을 바꾸려면 계약을 PR 로 바꾸고, 병합된 main 에서 아래대로 적용한다. 호스트는 이 스택을 적용하지 않는다.

## 결론

| 무엇 | 어디 | 누가 만드나 |
| --- | --- | --- |
| 월 예산 `perfectory-platform-monthly`(실제 100·150, 예측 150 USD 를 넘으면 메일) | AWS Budgets | 이 스택. 계정의 첫 자원 |
| 상태 버킷 `perfectory-platform-tofu-state-091109217099` | S3, 같은 계정 | 이 스택(처음 한 번은 아래 절차) |
| VPC `perfectory-platform`(/16, 주소 블록은 적용 때 `TF_VAR_vpc_cidr`), 공개 서브넷 둘(태스크), 사설 서브넷 둘(DB), 인터넷 게이트웨이 | VPC, ap-northeast-2a·2c | 이 스택. NAT·ALB 없음. VPC·서브넷은 `prevent_destroy` |
| Dagster(A1, [루트 ADR-0616](../../../../docs/adr/0616-data-jobs-run-as-dagster-assets-on-aws-ecs-and-the-host-job-layer-is-not-ported.md)): ECS 클러스터 `perfectory-platform`, 늘 켠 서비스 `dagster`(웹서버 + 데몬, Fargate ARM 0.5 vCPU / 2 GB), run 마다 태스크(`EcsRunLauncher`) | ECS, 공개 서브넷(들어오는 규칙 없음) | 이 스택(`infra/aws/dagster.tf`). 이미지는 적용 때 `TF_VAR_dagster_image`(릴리스의 `<commit>-aws-code-location` digest). 일은 아직 없다 |
| Dagster 의 저장소: RDS PostgreSQL 17 `perfectory-platform`(db.t4g.micro, 20 GB, 사설 서브넷만) | RDS. Dagster 와 그 run 은 이 데이터베이스만 가진 역할 `dagster` 로 붙는다(비밀 `perfectory-platform/dagster-database`, 상태 파일에 없다). master 의 비밀은 부트스트랩 태스크만 읽는다 | 이 스택. 삭제 보호·마지막 스냅숏·`prevent_destroy` |
| 경보 `perfectory-platform-dagster-not-running`(10 분 동안 Dagster 태스크가 없으면) → SNS `perfectory-platform-alarms` | CloudWatch·SNS | 이 스택. Slack 으로 잇는 AWS Chatbot 은 사장이 Slack 작업 공간을 한 번 허가한 뒤의 다음 걸음이다. 그때까지는 CloudWatch 에서만 보인다 |
| 예산 메일 주소 | 적용할 때의 환경 변수 `TF_VAR_budget_alert_emails` | 사람이 적용할 때 준다. 파일·저장소에 적지 않는다(공개 저장소) |
| 프런트 도어의 CloudFront 전용 예산 `perfectory-cdn-free-tier` | AWS Budgets | `infra/cdn`(호스트의 엣지 적용, [엣지 설정](./edge-settings.md)) |

## 적용 (병합된 main 에서)

OpenTofu 는 1.13.9(`tools/technology-versions.contract.json` 의 `ghcr.io/opentofu/opentofu:1.13.9` 와 같은 판)다. 작업 디렉터리는 저장소 밖에 둔다.

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
# 4) Dagster 의 이미지: 돌릴 릴리스의 AWS 코드 위치를 digest 로(아래 'Dagster 의 이미지 고르기').
export TF_VAR_dagster_image="ghcr.io/perfectory-inc/foundation-release@$digest"
tofu init -input=false
tofu plan -input=false -out="$TF_DATA_DIR/plan"
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

## Dagster 의 데이터베이스 역할 (A1 처음 한 번, 그리고 그 비밀번호를 바꾼 뒤)

Dagster 는 RDS 의 master 사용자로 붙지 않는다. `infra/aws/database-bootstrap.sql` 이 이 데이터베이스만 가진 역할을 만들고 비밀번호를 맞춘다.
그 SQL 은 일회성 태스크 `perfectory-platform-database-bootstrap` 이 master 로 돌린다. master 의 비밀을 읽는 것은 그 태스크의 실행 역할뿐이다.
SQL 은 몇 번을 돌려도 같다. 서비스가 처음 뜨기 전에 한 번 돌린다.

```bash
# 1) 데이터베이스, 두 비밀, 부트스트랩의 역할·정책·태스크 정의, 그리고 태스크가 DB 에 닿을 규칙만 먼저
#    (태스크 정의는 역할만 가리키므로 역할의 정책 둘은 따로 집는다. 없으면 태스크가 비밀을 못 읽는다)
tofu apply -input=false -target=aws_ecs_task_definition.database_bootstrap -target=aws_ecs_cluster.platform \
  -target=aws_iam_role_policy.database_bootstrap_execution -target=aws_iam_role_policy_attachment.database_bootstrap_execution \
  -target=aws_vpc_security_group_egress_rule.tasks_https -target=aws_vpc_security_group_egress_rule.tasks_database \
  -target=aws_vpc_security_group_ingress_rule.database_from_tasks
# 2) 부트스트랩을 한 번 돌리고, 끝 코드가 0 인지 본다(로그: /perfectory-platform/dagster 의 database-bootstrap/…)
sg="$(aws ec2 describe-security-groups --filters Name=group-name,Values=perfectory-platform-tasks --query 'SecurityGroups[0].GroupId' --output text)"
subnet="$(aws ec2 describe-subnets --filters Name=tag:Name,Values=perfectory-platform-public-ap-northeast-2a --query 'Subnets[0].SubnetId' --output text)"
task="$(aws ecs run-task --cluster perfectory-platform --task-definition perfectory-platform-database-bootstrap --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$subnet],securityGroups=[$sg],assignPublicIp=ENABLED}" --query 'tasks[0].taskArn' --output text)"
aws ecs wait tasks-stopped --cluster perfectory-platform --tasks "$task"
aws ecs describe-tasks --cluster perfectory-platform --tasks "$task" --query 'tasks[0].containers[0].exitCode' --output text   # 0
# 3) 이제 나머지(서비스·경보)
tofu apply -input=false
```

비밀번호를 바꿀 때: `dagster.tf` 의 `secret_string_wo_version` 을 하나 올리는 PR 을 병합하고 적용한다. 그다음 위 2) 를 돌리고,
서비스가 새 비밀번호로 다시 뜨게 한다(`aws ecs update-service --cluster perfectory-platform --service dagster --force-new-deployment`).
ECS 는 비밀을 태스크가 뜰 때 넣는다.

## Dagster 의 이미지 고르기 (A1, 적용마다)

서비스와 그 run 태스크는 릴리스의 AWS 코드 위치 이미지를 digest 로 돈다(루트 ADR-0620). 태그 `<commit>-aws-code-location` 은
그 커밋의 릴리스 워크플로 `aws-images` 잡이 만들고 서명한다. 태그를 digest 로 읽고, 서명을 확인한 digest 만 적용한다.

```bash
commit=<돌릴 main 커밋>
token="$(curl -s 'https://ghcr.io/token?scope=repository:perfectory-inc/foundation-release:pull' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')"
digest="$(curl -sI -H "Authorization: Bearer $token" -H 'Accept: application/vnd.oci.image.index.v1+json' \
  "https://ghcr.io/v2/perfectory-inc/foundation-release/manifests/$commit-aws-code-location" \
  | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}')"
gh attestation verify "oci://ghcr.io/perfectory-inc/foundation-release@$digest" --repo perfectory-inc/perfectory-public \
  --signer-workflow perfectory-inc/perfectory-public/.github/workflows/foundation-release-build.yml
```

새 digest 로 적용하면 태스크 정의의 새 개정이 생기고 서비스가 옛 태스크를 멈춘 뒤 새 태스크를 띄운다(데몬은 언제나 하나).
그 사이 몇 분은 Dagster 가 없다. 일이 들어온 뒤(A2)에는 진행 중인 run 태스크는 그대로 돈다.

## Dagster 의 UI 열기 (SSM 포트 포워딩)

공개 주소도 부하 분산기도 없다. ECS Exec 의 SSM 세션으로 노트북의 포트를 웹서버로 잇는다. AWS CLI 의 Session Manager 플러그인이 있어야 한다.

```bash
task="$(aws ecs list-tasks --cluster perfectory-platform --service-name dagster --query 'taskArns[0]' --output text)"
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
aws ecs describe-services --cluster perfectory-platform --services dagster \
  --query 'services[0].[runningCount,deployments[0].rolloutState]' --output text   # 1 COMPLETED
aws cloudwatch describe-alarms --alarm-names perfectory-platform-dagster-not-running \
  --query 'MetricAlarms[0].StateValue' --output text   # OK (처음 10 분은 INSUFFICIENT_DATA 이거나 ALARM 일 수 있다)
aws rds describe-db-instances --db-instance-identifier perfectory-platform \
  --query 'DBInstances[0].[DBInstanceStatus,PubliclyAccessible,StorageEncrypted]' --output text   # available False True
tofu plan -input=false -detailed-exitcode   # 0 이면 코드와 계정이 같다
```

## 되돌리기

- 예산·망: 계약이나 `main.tf` 를 되돌리는 PR 을 병합하고 위의 적용을 한다. 망을 지우는 적용은 그 안에 태스크·DB 가 없을 때만 된다.
- 상태: 버킷은 버전을 지닌다(지난 판은 90일). 잘못된 적용 뒤의 상태는 그 객체의 앞 판으로 되돌린다.
- Dagster: 앞 릴리스의 digest 로 `TF_VAR_dagster_image` 를 주고 다시 적용한다. 서비스를 멈추려면 `desired_count` 를 0 으로 하는 PR 을 병합하고 적용한다.
- 데이터베이스는 삭제 보호와 `prevent_destroy` 다. 지우는 일은 이 런북이 하지 않는다. 마지막 스냅숏 `perfectory-platform-final` 이 남는다.
- 상태 버킷은 `prevent_destroy` 다. 스택 전체를 내리는 일은 이 런북이 하지 않는다.
