---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-25
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
| VPC `perfectory-platform`, 공개 서브넷 둘(태스크), 사설 서브넷 둘(DB), 인터넷 게이트웨이 | VPC, ap-northeast-2a·2c | 이 스택. NAT·ALB 없음 |
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

## 확인 (읽기만)

```bash
aws budgets describe-budget --account-id 091109217099 --budget-name perfectory-platform-monthly \
  --query 'Budget.[BudgetLimit.Amount,CalculatedSpend.ActualSpend.Amount]' --output text
aws budgets describe-notifications-for-budget --account-id 091109217099 --budget-name perfectory-platform-monthly \
  --query 'Notifications[].[NotificationType,Threshold]' --output text
aws s3api get-bucket-versioning --bucket perfectory-platform-tofu-state-091109217099
tofu plan -input=false -detailed-exitcode   # 0 이면 코드와 계정이 같다
```

## 되돌리기

- 예산·망: 계약이나 `main.tf` 를 되돌리는 PR 을 병합하고 위의 적용을 한다. 망을 지우는 적용은 그 안에 태스크·DB 가 없을 때만 된다.
- 상태: 버킷은 버전을 지닌다(지난 판은 90일). 잘못된 적용 뒤의 상태는 그 객체의 앞 판으로 되돌린다.
- 상태 버킷은 `prevent_destroy` 다. 스택 전체를 내리는 일은 이 런북이 하지 않는다.
