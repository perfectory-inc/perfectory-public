# ADR 0619: AWS 플랫폼 계정의 바탕은 그 계정에 묶인 OpenTofu 스택 하나다 — 예산이 첫 자원이고, 상태는 그 계정 안에, 망은 NAT 없이, 이미지는 GHCR 에서

- Status: Accepted
- Date: 2026-11-25
- 함께 읽는 것: 루트 ADR-0616(데이터 작업을 AWS 의 ECS 에서 Dagster 자산으로, PR #1706 에서 검토 중). 이 ADR 은 그 ADR 의 걸음 A0(계정·망·예산을 코드로)을 정한다.
  ADR-0616 의 "이미지는 ECR" 은 이 ADR 의 §6(GHCR)이 대신한다. ADR-0616 이 받아들여질 때 그 줄은 이 ADR 을 따른다.

## Context

**사장 승인(2026-11-25).** 수집을 AWS 에서 돌리기 시작한다. 예산은 ARM 으로 월 약 92 USD, 일괄 수집이 있는 달은 160 USD 다.
돈이 드는 자원보다 예산이 먼저 있어야 한다. 대상 계정은 조직의 platform 계정(091109217099) 하나뿐이다.
관리 계정과 옛 공짱 운영 계정에는 어떤 자원도 만들거나 바꾸지 않는다.

**계정의 지금(2026-11-25, 읽기만 해서 잼).** 자원은 둘뿐이다. 하나는 프런트 도어 스택(`infra/cdn`, 루트 ADR-0461)의 예산
`perfectory-cdn-free-tier` 다. 한도 1 USD 이고 실제 0.01 USD 를 넘으면 메일한다. 다른 하나는 계정이 처음부터 가진 기본 VPC 다.
이 예산은 "계정 전체가 무료 등급"이라는 전제로 만들어졌다. 그래서 수집이 AWS 에서 첫 센트를 쓰면 울리기 시작하고, 그 뒤로는 아무 뜻이 없어진다.

**이 저장소의 OpenTofu 스택(루트 ADR-0423·0436·0461·0476).** 셋 다 데이터 호스트(ai-server)의 `scripts/deploy/edge_settings.py` 가 적용한다.
상태는 R2 에 두고, 비밀은 호스트의 runtime-secrets 묶음에서 읽는다. AWS 가 전제가 된 뒤(사장 2026-11-24)로 호스트는 떠날 자리다.
AWS 의 바탕을 호스트에 묶으면 떠날 자리에 새 짐을 싣는 것이다.

## Decision

### 1. 스택 하나, 계약 하나

계정의 바탕은 OpenTofu 루트 모듈 `platforms/foundation-platform/infra/aws/` 하나다. 이 모듈은 `config/aws-platform.contract.json` 만 읽는다.
그 계약이 계정·리전·태그·상태 버킷·예산·망을 정한다. A1 부터 더해지는 ECS·RDS·IAM 도 같은 스택, 같은 계약에 든다.
모든 자원은 공급자의 `default_tags` 로 `project=perfectory-platform` 을 단다.

### 2. 계정에 묶는다

공급자와 S3 백엔드가 모두 `allowed_account_ids = [계약의 account.id]` 다. 다른 계정의 자격으로 돌리면 OpenTofu 가 무엇도 읽거나 바꾸기 전에 멈춘다.
2026-11-25 에 관리 계정 프로필로 `tofu plan` 을 돌리자 `Error: AWS account ID not allowed` 로 멈췄다.
런북의 적용은 그 앞에서 `sts get-caller-identity` 의 계정을 계약과 한 번 더 견준다.

### 3. 적용은 병합된 main 에서, 노트북의 SSO 역할로

리뷰된 PR 이 병합된 뒤, 적용은 노트북에서 SSO 프로필 `perfectory-platform-prod-admin` 으로 한다(OrganizationAccountAccessRole).
절차는 `docs/runbooks/aws-platform.md` 다. 호스트는 이 스택을 적용하지 않는다. 손으로 콘솔에서 만드는 자원은 없다.

### 4. 상태는 그 계정 안의 S3 버킷

버킷 `perfectory-platform-tofu-state-091109217099` 도 이 스택이 만든다. 첫 적용만 로컬 상태로 하고, 그 뒤 버킷으로 옮긴다(런북).
버킷의 설정은 다음과 같다.

- 버전을 지닌다. 지난 판은 90일 뒤에 지운다.
- SSE-S3 로 암호화하고, 공개 접근을 모두 막고, TLS 가 아닌 요청은 거부한다.
- 잠금은 S3 백엔드의 `use_lockfile` 이다.

상태에는 비밀이 없다. 그래서 클라이언트 쪽 암호화(KMS 키, 월 1 USD)를 두지 않는다.
비밀을 가진 자원도 비밀을 상태에 넣지 않게 만든다. RDS 는 `manage_master_user_password` 로 하고, 비밀의 값은 OpenTofu 밖에서 넣는다.

### 5. 예산이 첫 자원이다

월 비용 예산 `perfectory-platform-monthly` 는 한도 150 USD 다. 실제 지출이 100 USD 를 넘을 때, 150 USD 를 넘을 때, 예측이 150 USD 를 넘을 때 메일한다.
메일은 계정의 등록 주소로 간다. 주소는 적용할 때의 변수(`TF_VAR_budget_alert_emails`)로 주고 저장소에는 적지 않는다(공개 저장소).
예산의 메일 구독자는 확인 단계가 없고, 동작이 없는 예산은 값이 0 이다.
처음 적용할 때 `-target` 으로 이 예산을 먼저 만든다.

AWS 쪽 경보를 Slack 으로 보내는 길은 A1 에서 정한다. 비밀이 AWS 로 들어가는 길과 함께 정하고, 사장이 토큰을 넣는 방법을 고른다.
이 ADR 은 호스트의 비밀 파일을 읽거나 옮기지 않는다.

프런트 도어의 예산은 CloudFront 비용만 센다(`cost_filter` Service = `Amazon CloudFront`, `config/cdn-front-door.contract.json` 의 `budget.service`).
그 예산의 뜻("프런트 도어는 무료 등급 안에 있다")은 그대로다. 계정 전체의 지출은 위 예산이 본다.

### 6. 망: 제 VPC, NAT·ALB 없음

| 것 | 값 | 이유 |
|---|---|---|
| VPC | /16 하나. 주소 블록은 적용할 때 준다(`TF_VAR_vpc_cidr`). 서브넷은 /20 이고 계약의 번호로 자른다 | 기본 VPC 는 코드가 아니다. 쓰지 않고 그대로 둔다. 공개 저장소에는 사설 주소를 적지 않는다(`public-repository-safety.sh`). VPC 가 생긴 뒤에는 런북이 AWS 에서 그 블록을 읽어 오고, VPC·서브넷은 `prevent_destroy` 라 다른 블록을 주면 계획이 멈춘다 |
| 공개 서브넷 | 2a·2c 에 하나씩 | 태스크가 공인 IPv4 로 원천·R2·GHCR·AWS API 에 나간다. 들어오는 규칙은 없다 |
| 사설 서브넷 | 2a·2c 에 하나씩 | DB 만 둔다. 밖으로 가는 길이 없다. RDS 는 두 영역의 서브넷을 요구한다 |
| 인터넷 게이트웨이 | 하나 | NAT 게이트웨이(월 약 43 USD + GB 당)를 두지 않는다 |
| 기본 보안 그룹 | 규칙 없음 | 제 보안 그룹을 잊은 것은 아무와도 말하지 못한다 |

UI 는 SSM 포트 포워딩으로 연다(루트 ADR-0616 §1). 공개 주소도 ALB 도 없다.

### 7. 이미지는 GHCR 의 공개 패키지에서, digest 로

AWS 에서 도는 이미지(발행기, Dagster 코드 위치)는 `ghcr.io` 의 공개 패키지다. CI 가 GitHub 의 무료 러너에서 linux/arm64 와 linux/amd64 로 짓는다(A0b).
태스크 정의는 태그가 아니라 digest 를 가리킨다. ECR 은 두지 않는다.

- 이 저장소는 공개이고, 이미지에 드는 것(발행기, 계약 파일, DuckDB 확장)도 모두 공개다. 비밀은 이미지에 들지 않는다(§4·§5).
- 공개 패키지는 익명으로 받는다. 그래서 태스크 정의에 레지스트리 자격(`repositoryCredentials`)을 둘 필요가 없고, CI 가 AWS 에 올리는 자격도 필요 없다.
  공개 패키지의 저장은 무료이고, 받는 쪽(AWS 로 들어오는 전송)도 무료다.
- 릴리스 빌드는 이미 `ghcr.io/<owner>/foundation-release` 를 캐시로 쓴다(`foundation-release-build.yml`).
- 위험은 GHCR 의 장애나 익명 받기의 제한이다. 그때는 태스크가 시작하지 못하고, Dagster 의 죽은 run 재시도가 다시 시작한다(ADR-0616 §2-7).
  그것이 잦으면 ECR 의 pull-through cache(GHCR 를 상류로 지원한다)를 앞에 둔다. 이미지 이름은 그대로 두고 주소만 바꾼다.

### 8. 비용(A0, 월)

| 자원 | USD |
|---|---|
| 예산(동작 없음) | 0 |
| 상태 버킷(KB 단위의 객체 몇 개) | 0.01 미만 |
| VPC·서브넷·인터넷 게이트웨이·경로표·보안 그룹 | 0 |
| **합** | **0.01 미만** |

공인 IPv4(시간당 0.005 USD)는 태스크가 뜨는 A1 부터 든다.

타일 굽기의 합치는 태스크를 잰 값(W86d)과 그 금액은 ADR-0616 §10 의 타일 표에 있다. 금액은 그 표 한 곳에 둔다(ADR-0618 §8).

## 대기업·오픈소스는 어떻게 하나

- 계정에 묶기: AWS 공급자의 `allowed_account_ids` 는 HashiCorp 문서가 "잘못된 계정에 적용하는 것을 막는" 용도로 적은 설정이다.
  여러 계정을 쓰는 조직은 스택마다 이것을 둔다.
- 상태: S3 백엔드의 버전 관리와 잠금이 OpenTofu·Terraform 문서의 기본 권고다. 이 백엔드는 OpenTofu 1.10 부터 DynamoDB 없이 lock 파일로 잠근다.
- 예산: AWS Budgets 는 새 계정을 위한 템플릿으로 "영(0) 지출 예산"과 "월 비용 예산"을 준다. 이 계정의 프런트 도어 예산이 앞의 것이고, 이 ADR 의 예산이 뒤의 것이다.
- NAT 없는 공개 서브넷의 Fargate 태스크는 AWS 의 ECS 문서가 적은 배치다. 태스크가 `assignPublicIp` 로 주소를 받는다.

## Consequences

- AWS 의 모든 자원은 이 스택 하나에서 나온다. 계정의 지출은 첫 자원인 예산이 본다.
- 프런트 도어의 예산은 CloudFront 만 센다. 호스트의 엣지 적용이 다음 배포에서 그 필터를 단다(입력이 바뀌었으므로).
- 아직 CI 가 이 스택을 `plan` 하지 않는다. PR 의 계획은 적용하는 사람이 노트북에서 내고 PR 본문에 적는다.
- 노트북에서 적용하는 동안에는 적용한 사람과 때가 CloudTrail 에만 남는다.

## 기각한 대안

- **호스트가 적용(다른 스택처럼).** AWS 가 전제다. 호스트는 떠날 자리이고, AWS 키를 호스트의 runtime-secrets 에 새로 두어야 한다.
- **R2 에 상태(다른 스택처럼).** 노트북에 R2 쓰기 키가 더 필요하다. AWS 스택의 상태를 AWS 밖에 두면 계정의 IAM 으로 지키지 못한다.
- **CI(GitHub OIDC)가 main 에서 적용.** 목표 모양이지만, 공개 저장소의 워크플로가 계정을 바꾸는 역할을 갖는 범위를 따로 정해야 한다.
  그 범위(역할의 권한, 환경 보호, 누가 승인하나)는 이 ADR 이 정하지 않는다.
- **기본 VPC.** 코드로 만든 것이 아니고, 기본 보안 그룹이 서로 다 통한다.
- **ECR.** §7. 저장 비용과 CI 의 AWS 자격이 들고, 공개 이미지에 더해 주는 것이 없다.
- **NAT 게이트웨이와 사설 서브넷의 태스크.** 월 약 43 USD 에 GB 당 요금이 더 든다. 들어오는 규칙이 없는 보안 그룹이 같은 일을 한다.

| 구분 | 내용 |
|---|---|
| 기본 기능 | OpenTofu S3 백엔드(`use_lockfile`), AWS 공급자의 `allowed_account_ids`·`default_tags`, AWS Budgets 메일, VPC·인터넷 게이트웨이, GHCR 공개 패키지 |
| 임시 | 노트북에서 SSO 역할로 적용(CI 적용의 범위가 정해질 때까지) |
| 운영 | `infra/aws` 스택과 `config/aws-platform.contract.json`, 런북 `aws-platform.md` |
