---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-27
---

# Lakekeeper — 레이크하우스 카탈로그의 dev 리그와 운영(prod)을 한 스택에서 적용한다

카탈로그의 결정은 루트 ADR-0624 다(사장 승인 2026-11-27). 스택 `infra/aws-lakekeeper` 하나가 두 환경을 짓는다: 입력은 `environment`
(`dev`·`prod`) 하나이고, 그것이 계약 `config/lakekeeper-<환경>.contract.json` 을 고르며 이름·크기·차이(상태 키까지)는 모두 그 계약에 있다.
dev 는 관문 시험을 돌린 리그(W109)이고 탐침이 있다. prod 는 Silver·Gold 가 커밋할 카탈로그(A1, W109c)이고 제 DB 인스턴스·경보·조정기가 있다.

**dev 리그.** W109(코디네이터 2026-11-27). 리그는 T8·T17 이 끝날 때까지 남고(T17 은 A1 의 운영 리그에서 돈다), 그동안
업그레이드 카나리아다(코디네이터 결정 6, 아래 "업그레이드 카나리아"): Lakekeeper·OpenFGA 의 새 버전은 여기서 먼저 카나리아를 통과한다.

## 결론

| 무엇 | 어디 | 누가 만드나 |
| --- | --- | --- |
| prod: ECS 클러스터 `perfectory-lakekeeper-prod`, Lakekeeper 둘·OpenFGA 둘, 제 RDS `perfectory-lakekeeper-prod`, Cognito 풀, 조정기, 경보, 창고 R2 자격(사장이 넣음 2026-11-28), 창고 `foundation-platform-lakehouse-prod`·네임스페이스 `silver`·`gold`, 인수 태스크(아래) | 같은 계정·VPC. `catalog.perfectory-lakekeeper-prod.internal:8181`, 부르는 것은 조정기뿐(레인은 제 구성 요소와 함께) | `infra/aws-lakekeeper`, `environment=prod`(상태 키 `lakekeeper-prod/terraform.tfstate`). 2026-11-27 W109c 적용, 2026-11-28 W109d 가 조정기로 창고를 만들었다 |
| ECS 클러스터 `perfectory-lakekeeper-dev` | 플랫폼 계정(서울), 플랫폼 VPC 의 공개 서브넷(들어오는 규칙은 아래 보안 그룹뿐) | `infra/aws-lakekeeper`, `environment=dev`(상태는 플랫폼 상태 버킷의 `lakekeeper-dev/terraform.tfstate`) |
| 카탈로그: Lakekeeper(버전 계약의 `quay.io/lakekeeper/catalog`) 태스크 둘, `catalog.perfectory-lakekeeper-dev.internal:8181` | Fargate ARM 0.25 vCPU / 0.5 GB. 그 포트는 탐침의 보안 그룹에서만 | 같은 스택(`catalog.tf`). `lakekeeper migrate` 는 서비스가 옮기기 전의 일회성 태스크 |
| 권한: OpenFGA(버전 계약의 `ghcr.io/openfga/openfga`) 태스크 하나, `authz.perfectory-lakekeeper-dev.internal:8081`(gRPC, 사전 공유 키) | 같은 클러스터. 그 포트는 카탈로그에서만 | 같은 스택(`authz.tf`). `openfga migrate` 가 먼저, 그다음 Lakekeeper 의 migrate 가 모델을 쓴다 |
| 데이터베이스 `lakekeeper_dev`·`openfga_dev`, 로그인도 저마다 | 플랫폼의 RDS 인스턴스(Dagster·원장과 다른 데이터베이스와 로그인, 연결 수 제한) | 같은 스택의 부트스트랩 태스크(`database-bootstrap.sql`, master 로) |
| 기계의 신원: Cognito 사용자 풀 `perfectory-lakekeeper-dev`, 주체마다 앱 클라이언트(client credentials) | Cognito(서울). 토큰 끝점 `perfectory-lakekeeper-dev.auth.ap-northeast-2.amazoncognito.com` | 같은 스택(`identity.tf`). 주체는 계약 `config/lakekeeper-dev.contract.json` 의 `identity.principals` |
| 탐침: 주체마다 태스크 정의 하나(`perfectory-lakekeeper-dev-probe-<주체>`, 익명 하나 더) | 같은 클러스터. 주체의 비밀은 그 탐침의 실행 역할만 넣는다 | 같은 스택(`probes.tf`, `probes/probe.py`). `probe.sh` 가 한 걸음을 돌리고 결과 줄을 찍는다 |
| rust 탐침(T8): 릴리스의 AWS 코드 위치 이미지(digest), 그 안의 퍼블리셔가 `check-lakehouse-catalog-writes` 를 운영자로 | 같은 클러스터. 저장 키 없음, 비밀은 운영자의 클라이언트뿐 | 같은 스택(`probes.tf` 의 `rust_probe`). 적용 때 `code_location_image` 를 줄 때만 생긴다 |
| 창고(warehouse) `foundation-platform-lakehouse-dev`, 네임스페이스 `silver`·`gold`, 표는 `tables/<네임스페이스>/<표>/`(표 전용 키 접두, 결정 2) | dev 버킷만. 저장 자격은 이 리그만 쓰는 새 Cloudflare 토큰(사장 결정 2026-11-27, A) | 창고 태스크(`./probe.sh warehouse warehouse-create`, 아래). 그 토큰을 읽는 것은 이 태스크뿐이다 |

## 적용 (노트북)

```bash
cd platforms/foundation-platform/infra/aws-lakekeeper
env=dev                                   # 또는 prod
export AWS_PROFILE=perfectory-platform-prod-admin
export TF_DATA_DIR="$HOME/.cache/perfectory/tofu/lakekeeper-$env"   # Git Bash 에서는 드라이브 문자 꼴(cygpath -m)로: MSYS_NO_PATHCONV=1 이면 tofu.exe 가 /c/... 꼴을 못 읽는다
tofu init -input=false -var environment=$env
tofu plan -input=false -var environment=$env -out="$TF_DATA_DIR/plan"   # dev: 플랫폼 스택에는 데이터베이스 보안 그룹의 들어오는 규칙만 더한다
tofu apply "$TF_DATA_DIR/plan"
rm -f "$TF_DATA_DIR/plan"
```

계약이 다르게 하는 것: dev 는 플랫폼의 공유 RDS 에 데이터베이스 둘(`databases.instance` 없음)과 탐침(`probes`), 드리프트는 보고만(`reconciler.drift`
`report`), 경보 없음. prod 는 제 RDS 인스턴스 `perfectory-lakekeeper-prod`(db.t4g.small, 지금 Single-AZ, 자동 백업 7 일과 시점 복원),
OpenFGA 둘, 드리프트는 실패(`fail`), 경보 다섯(아래), 비밀의 복구 기간 7 일, 로그 보존 365 일, Cognito 풀의 삭제 보호.

rust 탐침(T8)까지 두려면 그 릴리스 커밋의 검증된 이미지를 변수로 준다: `scripts/deploy/aws-release-images.py <커밋> --out <파일>` 이
서명을 확인하고 적은 `dagster_image` 값을 `tofu plan -var code_location_image=<그 값>` 으로 넘긴다(스택은 계약의 릴리스 저장소 밖 이미지와
태그를 거부한다). 그 이미지의 퍼블리셔가 이 카탈로그에 쓰는 길을 그대로 잰다.

적용은 일회성 태스크 셋을 차례로 돌리고 기다린다(`infra/aws/pre-deploy.sh`): 데이터베이스 부트스트랩 → OpenFGA migrate → OpenFGA
서비스 → Lakekeeper migrate → Lakekeeper 서비스. 하나라도 0 이 아니면 적용이 거기서 멈춘다. 로그는 `/perfectory-lakekeeper-dev` 다.

처음 적용 뒤 한 번: 운영자(부트스트랩 주체)가 Lakekeeper 를 부트스트랩한다. 다른 주체가 먼저 부르면 그 주체가 관리자가 되므로 바로 한다.

```bash
./probe.sh bootstrap bootstrap      # 204
./probe.sh bootstrap info           # bootstrapped true, authz-backend openfga
```

## 사장이 한 번 하는 것: 창고의 R2 토큰을 넣기

사장 결정 2026-11-27(A): 환경마다 그 카탈로그만 쓰는 **새** Cloudflare 계정 API 토큰이다. 기존 `CLOUDFLARE_API_TOKEN` 은 쓰지 않는다.
dev 는 `perfectory-lakekeeper-dev`(넣었다, 2026-11-27), prod 는 `perfectory-lakekeeper-prod` 다. 아래는 prod 의 것이고, dev 는 이름의 `prod` 를
`dev` 로 읽는다.

1. Cloudflare 대시보드 → R2 Object Storage → Overview → `{} API` 의 **Manage API tokens** → **Create Account API token**.
   이름은 `perfectory-lakekeeper-prod`, 권한은 **Admin Read & Write**(Lakekeeper 문서: R2 의 임시 자격 끝점이 이것만 받는다).
   만든 화면의 **Token value**·**Access Key ID**·**Secret Access Key** 셋과 계정 ID(R2 Overview 오른쪽의 Account ID)를 넣는다.
2. 새 PowerShell 창에서(값은 화면·명령 기록·디스크에 남지 않는다):

```powershell
$account = Read-Host 'Cloudflare 계정 ID'
$keyId = Read-Host 'Access Key ID' -AsSecureString
$secretKey = Read-Host 'Secret Access Key' -AsSecureString
$token = Read-Host 'Token value' -AsSecureString
$plain = { param($s) [System.Net.NetworkCredential]::new('', $s).Password }
$value = $account + ':' + (& $plain $keyId) + ':' + (& $plain $secretKey) + ':' + (& $plain $token)
aws secretsmanager put-secret-value --profile perfectory-platform-prod-admin --region ap-northeast-2 --secret-id perfectory-lakekeeper-prod/r2-warehouse-credential --secret-string $value --query VersionId --output text
Remove-Variable value, token, secretKey, keyId, account, plain
```

비밀은 `<계정 ID>:<Access Key ID>:<Secret Access Key>:<Token value>` 한 줄이다. 따옴표가 없어 PowerShell 5.1 이 그대로 넘긴다.
그 비밀은 조정기(`<이름>-reconciler-execution`, dev 는 창고 탐침 `perfectory-lakekeeper-dev-probe-warehouse-execution` 도)의 실행 역할만 읽는다. 비밀 자체의 정책이 다른 모든 주체의 읽기를
거부한다(적용하는 관리자도 `AccessDeniedException`). Lakekeeper 는 AWS 의 비밀을 읽지 않는다. 창고 태스크가 창고를 만들 때 한 번 넘기고,
Lakekeeper 는 그것을 제 데이터베이스에 암호화해 둔다(`catalog-encryption-key`).

넣은 뒤 창고를 만든다. prod 는 조정기가(아래), dev 는 처음에 창고 탐침이 만들었다(`./probe.sh warehouse warehouse-create`).
값이 들기 전에는 그 비밀을 넣는 태스크가 시작하지 못한다: 사장이 넣기 전에 운영 R2 에는 아무것도 쓰이지 않는다.

## 조정기 (reconcile-lakehouse-catalog)

퍼블리셔의 `reconcile-lakehouse-catalog`(`services/foundation-outbox-publisher/src/lakehouse_catalog_reconcile.rs`)가 운영자 주체로
Lakekeeper 를 한 번 부트스트랩하고, 창고(R2 자격을 넘겨)·네임스페이스 `silver`·`gold`·선언된 표(Rust 표 계약에서)·레인의 권한을 만들고,
선언 밖의 네임스페이스·표·권한과 계약과 다른 표 모양·창고 프로필을 드리프트로 센다(prod 는 실패, dev 는 보고). 지우는 것은 없다.
선언은 계약의 `warehouse`·`reconciler` 다. A1 에는 표도 레인도 선언하지 않는다(코디네이터 2026-11-27): 구성 요소의 전환 PR 이 제 표와 레인의
권한을 더한다.

조정기는 릴리스의 코드 위치 이미지(digest)이므로, 적용에 `code_location_image` 를 줄 때만 태스크가 있고, 그 적용이 새 정의마다 한 번 돌린다
(실패하면 적용이 실패한다):

```bash
scripts/deploy/aws-release-images.py <커밋> --out "$TF_DATA_DIR/release.json"   # 서명 확인, dagster_image 를 적는다
tofu plan -input=false -var environment=$env -var code_location_image="<dagster_image 값>" -out="$TF_DATA_DIR/plan"
```

prod 에는 사장이 토큰을 넣은 뒤에만 이미지를 준다(그 전에는 태스크가 시작하지 못하고 적용이 실패한다).

## 경보 (prod)

`alarms.tf`, 플랫폼의 경보 주제(Slack)로 ALARM 만 간다(OK 는 플랫폼 스택의 규칙이 이 스택의 경보를 모른다). 루트 ADR-0624 §7.

| 경보 | 무엇 |
| --- | --- |
| `perfectory-lakekeeper-prod-pool-timeouts` | Lakekeeper 의 풀이 연결을 기다리다 시간을 넘김(`pool timed out while waiting for an open connection`), 하나라도 |
| `perfectory-lakekeeper-prod-authorization-errors` | `internal_authorization_error`·`AuthorizationBackendError`(OpenFGA 를 못 써 503), 하나라도 |
| `perfectory-lakekeeper-prod-check-latency` | OpenFGA 확인(`grpc_req_complete`, `Check`·`BatchCheck`)의 p95 가 1.5 초 이상 5 분 |
| `perfectory-lakekeeper-prod-database-connections` | DB 연결이 `max_connections` 150 의 80 % 를 넘음 5 분 |
| `perfectory-lakekeeper-prod-database-memory` | DB 여유 메모리가 256 MB 아래 5 분 |

지표는 로그의 지표 필터이고 두 환경 모두에 있다(이름공간 `Perfectory/<이름>`). 2026-11-27 dev 에서 확인: OpenFGA 의 `query_duration_ms` 는
문자열이지만 필터가 숫자로 싣고 p95 가 나온다(동시 10 의 T13 묶음에서 1 분 112 건, p95 616 ms).

## 운영 인수 시험 (prod, A1: T13·T17)

운영에서만 돌 수 있는 A1 의 시험 둘이다(루트 ADR-0624 결정 4-4·4-7): 운영의 제 DB 위의 동시 50 T13 과, 이전 방법의 T17. 계약의 `acceptance`
이고 `acceptance.tf` 가 태스크 정의 `perfectory-lakekeeper-prod-probe-operator` 하나를 만든다(적용에 `code_location_image` 를 줄 때만).

- 이미지는 릴리스의 코드 위치 이미지(digest)이고, 그 안의 Python 이 `probes/probe.py` 를 돈다. DuckDB 는 해시로 고정한 `duckdb==2.0.0` 휠
  (퍼블리셔가 묶은 DuckDB 와 같은 판)이고, 확장은 그 이미지가 실은 파일을 경로로 `LOAD` 하고 계약(`duckdb-extensions.contract.json`)의 빌드와
  맞춘다. 실행 중 내려받는 확장은 없고, 그 보안 그룹에는 평문 HTTP 로 나가는 길이 없다(루트 ADR-0559 §6).
- 실행 역할은 조정기의 것(`perfectory-lakekeeper-prod-reconciler-execution`)이다: 창고 R2 자격을 읽는 유일한 역할이고, 새 읽는 주체도 새 비밀도
  없다(코디네이터 2026-11-28, (b)). 그 자격은 이 계정의 Admin Read & Write 토큰이라 Basin 도 받는다. T17 은 그것으로 Basin 을 `READ_ONLY` 로
  붙인다. 이 Basin 읽기 길은 A9 에서 Basin 과 함께 없어진다.
- 표는 네임스페이스 `probe` 에만 선다(창고가 선언한 `silver`·`gold` 에는 만들지 않는다). 시험 바로 뒤 `lake-probe-drop` 이 그 네임스페이스를 안의
  모든 표·파일과 함께 지운다(`recursive=true&force=true&purge=true`: 소프트 삭제 창고에서 재귀 지우기는 `force` 가 들고, 그 아래 표를 한 번에
  지우고, 파일은 `purge` 일 때만 지운다 — 상류 concepts v0.15.1). 그다음 조정기를 다시 돌려 드리프트가 없음을 본다.

```bash
export LAKEKEEPER_ENVIRONMENT=prod
./probe.sh operator lake-migrate silver.industrial_land_sale_notice_read        # T17: 모양을 Basin 에서, INSERT … SELECT, 비교
./probe.sh operator lake-mints 100 50 probe.industrial_land_sale_notice_read     # T13: 동시 50, 운영의 제 DB
./probe.sh operator lake-mints 200 50 probe.industrial_land_sale_notice_read
./probe.sh operator lake-probe-drop                                             # 네임스페이스 probe 와 그 파일 모두
tofu plan -input=false -var environment=prod -var code_location_image="<dagster_image 값>" \
  -replace='terraform_data.reconcile[0]' -out="$TF_DATA_DIR/plan"                # 조정기를 다시: 드리프트가 있으면 적용이 실패한다
tofu apply "$TF_DATA_DIR/plan"
```

`lake-migrate <표>` 는 Basin 의 그 표의 현재 스키마·파티션 명세·정렬 순서를 REST 로 읽어 `probe.<이름>` 을 같은 모양으로 비게 만들고(전환 때는
조정기가 표 계약에서 만든다), DuckDB 가 Basin(`READ_ONLY`)과 Lakekeeper(OAuth2 비밀, 벤딩 자격)를 붙여 Basin 의 머리 스냅숏을 고정하고
`INSERT … SELECT` 로 옮긴 뒤, 두 쪽의 행 수, 키 유일(표 계약의 `current_row_latest.key` 와 `newest_first`, 그 품질 관문), 행마다와 열마다의 해시
(`count`·`sum(hash(…))`)가 같은지 본다.

## 구성 요소 전환 (A1, 첫째: 산업용지 분양공고, W109e)

전환 PR 하나가 구성 요소의 표를 마지막 이름으로 바꾸고(이름 바꾸기 목록의 제 항목을 지운다), 표 노드에 `catalog: lakekeeper` 를 달고, 그 작업들을
AWS 레인으로 옮기고 호스트에서 끈다(`enabled: false`, 인계). 카탈로그가 쥐는 것은 손으로 적지 않는다: 생성기(`scripts/catalog/declared_definitions.py`
`lakekeeper_declaration`)가 파이프라인 그래프에서 `config/lakekeeper-declaration.v1.json` 을 만든다 — 표, 레인마다의 권한(읽는 표에 `select`, 적재가
쓰는 표에 `write`), 레인과 함께 옮기는 Bronze 원천. 그 표를 건드리는 켜진 호스트 작업이 있으면 생성이 거부한다. 이 스택은 그 파일에서 레인마다
Cognito 클라이언트와 칸(`perfectory-lakekeeper-prod/principal-<레인>`: 클라이언트와 카탈로그 주소·토큰 끝점·범위·창고, 그 레인의 실행 역할만 읽는다)을
만들고, 플랫폼의 태스크 보안 그룹에서 카탈로그 포트로 가는 길을 열며, 조정기가 표를 만들고 권한을 준다.

순서(병합 뒤, 노트북; 플랫폼 스택은 `docs/runbooks/aws-platform.md` 의 적용):

1. 호스트: 병합이 자동 배포되면 그 작업들이 꺼지고, 원장 내보내기(`bronze_ledger_export`, 배포 뒤 한 번)가 원천마다
   `control/evidence/ledger_move/<날짜>-<원천>.jsonl.gz` 를 쓴다. 그 객체들이 있는지 읽기 키로 본다.
2. 이 스택 prod 적용(`code_location_image` = 그 커밋의 `dagster_image`): 클라이언트·칸·보안 그룹 규칙, 그리고 조정기가 표 둘을 비게 만들고 권한을
   주며 드리프트 0 이어야 적용이 끝난다.
3. 플랫폼 스택 적용(같은 이미지): 레인 역할·창구 alias, 배포 전 사슬의 셋째 걸음 `ledger-import`(원장 내보내기를 AWS 원장에 들인다; 한 번 들인
   줄은 다시 들이지 않는다), 그다음 서비스가 새 코드 위치로 옮긴다.
4. 다시 짓기: 적재 레인(`industrial_land_sale_notice_silver`)은 수집 레인이 바꾼 것 뒤에 eager 로 돈다. 표가 비어 있는 동안 수집 레인은 아무것도
   묻지 않고 `ready` 가 표를 이름한다(표가 그 레인의 상태다). 적재 자산은 처음 평가될 때 없는 것(`newly_missing`)이라 eager 가
   스스로 한 번 띄운다(Dagster 1.13.27 `NewlyTrueCondition`: 앞 상태가 없으면 지금 없는 것이 새로 없는 것). 한 run 이 할 일을 다 하고
   끝난다: `max_objects` 60 씩 계획·적재를 빚이 없을 때까지 되풀이한다(객체 85 개 = 두 번). 늦음 검사는 표가 있던 동안 창이 끝난 객체만 센다.
5. 비교(T17, 다시 짓기): `./probe.sh operator lake-compare silver.industrial_land_sale_notice_read silver.industrial_land_sale_notice_fetch_attempt`,
   그리고 `lake-compare silver.industrial_land_sale_notice_attachment silver.industrial_land_sale_notice_attachment`. Basin 쪽(전환에서 얼었다)의
   객체마다 행 수와 적재가 쓰지 않는 열의 해시로 서명해 다시 지은 쪽에 모두 있어야 하고(Bronze 시기 뒤 키가 바뀌었으므로 `source_record_id` 로 짝짓지
   않는다), 다시 지은 쪽에만 있는 객체는 Basin 의 마지막 적재보다 새것이어야 한다. 키 유일, `ingested_at_utc` 는 있음만 본다.
6. 조정기를 다시 돌려(위의 `-replace='terraform_data.reconcile[0]'`) 드리프트 0 을 본다. 그다음 수집 레인이 매일 박자에 돈다.

호스트의 미디어 대조기는 이 카탈로그에 닿지 않는다: 전환한 색인 표의 적재가 커밋 전에 그 행의 키를 `control/media_index/<ns>/<표>/` 에 쓰고, 대조기의
`media-index-delta` 가 그것을 Basin 의 나머지 색인과 합친다(코디네이터 2026-11-28). 마지막 색인이 옮기면 대조기가 통째로 AWS 로 간다.

## 탐침

```bash
./probe.sh <주체> <걸음> [인자...]   # dev. 주체: bootstrap, lane-a, lane-b, outsider, anonymous, warehouse, logscan, maintenance
LAKEKEEPER_ENVIRONMENT=prod ./probe.sh operator <걸음> [인자...]   # prod 의 인수 태스크(위)
```

- 주체 `rust`: 퍼블리셔의 `check-lakehouse-catalog-writes`(T8, `services/foundation-outbox-publisher/src/lakehouse_catalog_writes_check.rs`).
  iceberg-rust 0.11 이 OAuth2(클라이언트 자격)로 토큰을 받고, load-table 이 준 벤딩 자격만으로 우리 커미터가 `silver.w109_rust_smoke` 를
  만들고 덧붙이고, 우리 REST 클라이언트가 태그를 만들고 지우고, `main` 이 떠난 가지를 지우고, 적재 가지를 게시한다. 마지막에 틀린 정적 키를
  옆에 두고 같은 덧붙이기가 실패하고 아무것도 커밋하지 않는지 본다.
- `probes/probe.py` 의 걸음. 신원·권한: `health`, `info`, `claims`, `bootstrap`, `whoami`, `identity`, `expiry`, `config`, `provision <주체>`,
  `authz-denied`, `authz-role-creator`, `grant|revoke <권한> <주체>`, `refused`, `loop <초>`. 창고: `lake-setup`, `lake-scope [카나리아]`,
  `lake-privileges`, `lake-duckdb`, `lake-history <커밋 수>`, `lake-escape`, `lake-escape-create`, `lake-drop drop|purge [표]`, `lake-mints <수> [동시] [표]`,
  `lake-commits <초>`, `lake-reconcile inject|reconcile`, `lake-refresh`, `lake-register-from <표> <metadata.json>`, `lake-maint setup|verify`,
  `lake-tasks`, `lake-cleanup`, `lake-vended-keys`(load-table 의 자격이 iceberg-rust 0.11 이 읽는 `config` 에 있는가, 키 이름만), `warehouse-drop`
  (창고를 지운다: 저장 프로필의 키 접두는 바꿀 수 없어 접두를 바꾸면 창고를 새로 만든다). 창고 태스크(R2 토큰을 쥔 유일한 태스크):
  `warehouse-create`, `warehouse-list|put|hash|delete <키>…`, `warehouse-vend-check`. prod 의 인수 태스크만: `lake-migrate <표>`(T17),
  `lake-probe-drop`.
- `logscan <분>`: 로그 그룹에서 리그의 비밀 값(R2 토큰 빼고 전부)과 JWT·키 모양을 센다(`probes/log-scan.sh`). 값과 줄은 찍지 않는다.
- `maintenance <표> <지울 표>`: 상류 그대로의 Spark 와 Iceberg 1.12.0 의 `expire_snapshots`·`remove_orphan_files`(`prefix_listing`)·
  `DROP TABLE … PURGE`(`rest-catalog-purge`), 벤딩 자격만으로(`probes/maintenance.py`, `maintenance.tf`).
- 줄마다 JSON 하나이고 토큰·비밀·응답 본문은 찍지 않는다(쥔 비밀의 값은 줄에서 가린다). 끝 코드 0 은 모든 검사가 맞았다는 뜻이다.

## 업그레이드 카나리아

```bash
./canary.sh     # 계약의 upgrade_canary 걸음을 차례로, 걸음마다 한 줄, 모두 기대대로면 0
```

Lakekeeper·OpenFGA 의 새 버전은 버전 계약(`tools/technology-versions.contract.json`)에서 바꾸고, 이 리그에 먼저 적용하고(rust 탐침은 그
버전을 쓸 릴리스의 `code_location_image` 로), `canary.sh` 가 통과해야 다른 곳에 핀한다. 걸음과 기대는 계약의 `upgrade_canary` 다.
첫 실행(2026-11-27, #1777 병합 커밋의 이미지, 두 번): T13 을 빼고 모든 걸음이 기대대로였다(T31·T36 은 기대한 실패). 첫 번에 탐침이 다시 돌 때의
버그 넷을 찾아 고쳤다(T3 이미 있는 권한 409, T5 키 접두를 모르는 키, T7 객체인 `allowed-actions` 의 정렬, T33 앞 실행의 행). T13(동시 50)은 공유
micro 위에서 간헐 실패라(시험 기록의 T13) 카나리아는 T13 을 동시 10 으로 돈다(W109c, 루트 ADR-0624 결정 4-4). 동시 50 의 T13 은 운영의 제 DB
위에서 A1 에서 돈다.

`known_failure` 가 있는 걸음(T31, T36)은 실패가 기대이고 엄격하다: 통과하면 카나리아가 실패한다. T36 이 통과하면 lakekeeper#2217 을 담은
릴리스가 들어온 것이니 그 버전을 핀하고 그 항목을 지운다(결정 3). T31 이 통과하면 상류가 `write` 를 좁힌 것이니 결정 1 을 다시 본다.

## 시험 기록 (2026-11-27, Lakekeeper v0.15.0 → v0.15.1, OpenFGA v1.14.2)

판정은 개념 문서(concepts brief "Lakekeeper to the limit" §4)의 통과 기준이다. 걸음은 위 탐침이다.

| 시험 | 판정 | 걸음 | 결과 |
| --- | --- | --- | --- |
| T1 | 통과 | 적용, `health`, `info`, `warehouse-create`, `warehouse-list ""` | migrate 일회성 태스크 0, `/health` 200(OpenFGA·풀 ok), 창고 201, 버킷 루트에 남은 검증 객체 0 |
| T3 | 통과 | `lake-setup` | 표가 `s3://<버킷>/silver/<표>`, 식별자 `silver.<표>`, 문자열 접두 형제(`w109_smoke`·`w109_smoke_versions`)가 따로 선다. 키 접두 뒤(W109b)에는 `s3://<버킷>/tables/silver/<표>` |
| T4 | 통과(결정 2 뒤) | `warehouse-drop`, `warehouse-create`, `lake-escape-create`(운영자·레인) | 처음(창고 = 버킷 루트): 레인은 어디든 403, 운영자는 `bronze/`·`media/` 에도 만들었다. 표 전용 키 접두 `tables` 로 창고를 다시 만든 뒤(W109b): 운영자도 `bronze/`·`media/`·버킷 루트·다른 버킷·접두의 문자열 형제(`tablesx/`)에서 `InvalidLocation`(400), 다른 표 경로와 그 안에서 `LocationAlreadyTaken`. 레인은 일곱 곳 모두 403 |
| T5 | 통과 | `lake-scope`, `warehouse-vend-check` | 제 표 접두의 GET·LIST(쓰기면 PUT·DELETE)만 200, 형제·루트·bronze 카나리아·운영 버킷 403, 읽기 자격의 PUT 403, 세션 토큰·갱신 끝점 있음, 응답에 부모의 비밀 키·토큰 없음(access key id 는 부모의 것을 그대로 쓴다) |
| T6 | 통과 | `identity`, `expiry`, `config`, `provision` | 무토큰·엉터리·서명 위조·미선언 클라이언트·다른 범위 401, 선언된 레인은 `oidc~<client id>` 로 매핑, 만료 90 초 뒤 401(시간 검사의 유예 60 초), 새 토큰 200 |
| T7 | 통과(단서) | `authz-*`, `grant`, `revoke`, `refused` | 권한 없음 403, 부여 즉시 되고 회수 즉시 빠진다. OpenFGA 를 내리면 판단이 드는 모든 호출 503(운영자도). 발급자에 못 닿으면 이미 받은 키로 토큰을 계속 받는다 |
| T9 | 통과 | `lake-duckdb` | DuckDB 2.0.0 이 OAuth2 비밀로 ATTACH, 벤딩 자격으로 SELECT·INSERT·고정 스냅숏 읽기. bronze 범위나 표 경로 범위의 엉터리 S3 비밀이 있어도 벤딩 자격이 이긴다 |
| T11a | 통과 | `lake-drop drop`, `lake-drop purge` | 보호된 표 지우기 거부, 소프트 삭제·목록·복구, 정리 전 같은 이름은 `LocationAlreadyTaken`, 만료 뒤 퍼지 작업이 파일을 지우고 같은 이름이 다시 선다. PyIceberg 는 `purgeRequested=false` 를 보내므로 그때는 파일이 영영 남는다 |
| T11b | 통과(설정 하나) | `lake-maint setup`, `warehouse-put`, `maintenance`, `warehouse-hash`, `lake-maint verify` | 참조(main·태그·브랜치)가 모두 읽히고 태그 없는 옛 스냅숏이 만료(태그된 스냅숏의 조상도), 표의 고아만 지워지고 형제·`work/`·`bronze/` 의 파일은 해시가 그대로, Spark 의 `DROP … PURGE` 는 소프트 삭제를 거친다. 단 정비 실행기에 `s3.delete-enabled=true` 가 있어야 한다: 없으면 Lakekeeper 가 보낸 false 로 아무것도 지우지 않고 지웠다고 센다 |
| T12 | 일부 | `lake-refresh` | 자격이 끝나는 시각(3,600 초)과 갱신 끝점이 오고, 갱신 끝점이 같은 표 접두의 자격을 준다. 자격보다 긴 스캔·두 자격에 걸친 멀티파트는 돌리지 않았다 |
| T13 | 통과(결정 4 뒤) | `lake-mints 100 50`, `lake-mints 200 50` | 처음(OpenFGA 풀 4): 동시 50 은 100 번 모두 503(OpenFGA 의 확인이 DB 연결 4 개에서 3 초 기한을 넘김), 동시 10 은 모두 200. 풀 16·유휴 4(W109b): 조용한 뒤 첫 동시 50 은 100 번 중 49 번 503(연결을 여는 동안 OpenFGA p95 3.3 초), 바로 다음 200 번은 모두 200, 1.8 초(p95 88 ms). 풀 16 을 시작 때 다 열고 두니(`min_open_connections`) 새 태스크의 첫 동시 50 이 100 번 모두 200, 0.7 초, 다음 200 번도 모두 200, 1.8 초. 자격 발급은 어느 경우나 1 번(캐시). 그때 공유 RDS 의 연결 24, 여유 메모리 최저 약 99 MB. 단 간헐적이다: 업그레이드 카나리아 안의 두 묶음(10:25Z, 11:06Z)은 풀이 이미 열려 있는데도 100 번 중 22 번·9 번 503(OpenFGA p95 3.1 초, RDS 는 1 분 단위로 포화 없음, CPU 크레딧 충분), 따로 돌린 세 묶음은 100/100(1.5·0.8·0.8 초). 공유 micro 위에서 동시 50 은 Lakekeeper 의 약 3 초 권한 기한에 닿을 수 있다: 운영은 제 DB(결정 4), ADR-0624 §7 의 순서, `AuthorizationBackendError` 경보. **운영(2026-11-28 W109d, 제 DB db.t4g.small, OpenFGA 둘·풀 16 시작 때 열기)**: `lake-mints` 동시 50 다섯 묶음(100·200·100·100·100) 모두 200(0.8·1.1·1.9·0.3·0.3 초), 자격 발급 묶음마다 1 번. 그 15 분 DB 연결 최대 40(150 의 27 %), 여유 메모리 최저 975 MB, 다섯 경보 모두 OK. OpenFGA 확인 p95 는 대개 3 ms, 1.9 초 묶음의 1 분만 1,763 ms(경보는 1.5 초 이상 5 분) |
| T14 | 일부 | `loop`, `lake-commits` + 태스크 멈춤 | 호출 3,992 번 중 1 번, 커밋 129 번 중 4 번 태스크를 멈춰도 실패·중복·누락 0. 전부 내리면 새 호출은 곧바로 실패. 동시 두 쓰기·RDS 장애 조치는 돌리지 않았다(공유 단일 AZ 인스턴스) |
| T15 | 일부 | `lake-register-from` | 카탈로그가 잃은 표가 metadata.json 하나로 스냅숏·행 그대로 돌아온다. Lakekeeper 에는 `unregister` 가 없다(404). DB 스냅숏 복원·키 없는 비밀 저장소는 돌리지 않았다 |
| T16 | 통과 | `loop` + v0.15.0→v0.15.1 적용, `lake-commits` + 강제 재배포 | 업그레이드(migrate 뒤 롤링) 동안 호출 9,596 번 실패 0, 롤링 재시작 동안 커밋 155 번 실패 0 |
| T22 | 통과 | `logscan` (info, 그리고 `-var catalog_rust_log=debug`) | 비밀 8 개의 값·JWT·AWS 키 모양 0 줄. R2 토큰의 값은 찾지 않았다(그 비밀은 창고 태스크만 읽는다); 64 자 hex 모양 0 줄 |
| T31 | 실패 | `lake-privileges` | `write` 로 추가·태그는 되고 만들기·이름 바꾸기·지우기는 403이지만, 열 추가와 속성 변경도 된다. 상류 문서가 그렇게 적는다(`write` = 데이터·스키마·파티션·정렬·속성·브랜치·태그·이력). 결정 1: 표의 모양은 권한이 아니라 조정기의 드리프트 검사(fail)와 "조정기 밖 ALTER·스키마·속성 호출 없음" semgrep 규칙이 잡는다. 카나리아의 알려진 실패 |
| T32 | 통과 | `lake-reconcile inject`, `lake-reconcile reconcile` | 선언 밖의 권한을 표마다 읽은 목록에서 찾아 한 번에 지우고, 살아 있는 권한이 선언과 같다 |
| T33 | 통과 | `lake-history 101` | 커밋 101 번 뒤 스냅숏 id·태그로 읽히고, 첫 metadata 파일은 지워져(404) `metadata_location` 고정은 깨진다 |
| T35 | 통과 | 계획에 `allowall` 또는 `LAKEKEEPER__INSECURE_ALLOW_UNAUTHENTICATED` | 두 경우 모두 `tofu plan` 이 전제 조건으로 멈춘다. 정적 R2 키로 작업이 시작을 거부하는 것은 작업 코드의 일이라 여기 없다 |
| T36 | 실패 | `lake-escape` | 다른 표의 파일을 가리키는 커밋이 v0.15.1 에서 받아들여진다. 고친 것(lakekeeper#2217, 2026-11-26 닫힘)은 아직 어느 릴리스에도 없다(2026-11-27 확인: 최신 v0.15.1). 결정 3: 그것이 든 첫 릴리스를 나오는 대로 핀하고, 그때까지 쓰는 쪽은 iceberg-rust 이고 표 위치 아래에만 쓴다. 카나리아의 알려진 실패 |
| T8 | 통과 | `lake-vended-keys`, 그다음 `rust check-lakehouse-catalog-writes`(#1777 병합 커밋의 arm64 코드 위치 이미지, 서명 확인 뒤) | 앞 조건: load-table 의 `config` 에 벤딩 자격(키·비밀·세션 토큰), `s3.endpoint`(R2), `s3.region` `apac` 이 있고 경로 방식 키는 없다. iceberg-rust 0.11 은 표의 파일 접근을 `config` 로만 만들고(`storage-credentials` 는 읽기만 하고 쓰지 않는다) 그 위에 사용자 속성을 덮는다. 본 시험(저장 키 없이, OAuth2 클라이언트 자격만): 우리 커미터가 `silver.w109_rust_smoke` 를 만들고 3 행씩 두 번 덧붙였고(파일은 `tables/silver/w109_rust_smoke/data/` 아래), 우리 REST 클라이언트의 `create_tag`·`remove_stale_branch`(`main` 이 떠난 가지 지움)·`publish_branch`·`remove_tag` 가 모두 되었고, 끝에 `main` 만 남았다. 틀린 정적 키를 옆에 두자 같은 덧붙이기가 첫 저장 호출(manifest list 읽기)에서 실패했고 `main` 은 그대로였다: 정적 키가 벤딩 자격을 이긴다 |
| T17 | 통과(prod, 2026-11-28 W109d) | prod 의 `lake-migrate silver.industrial_land_sale_notice_read`, 그다음 `lake-probe-drop` | 이전 방법(DuckDB 가 Basin 을 `READ_ONLY` 로, Lakekeeper 를 OAuth2 비밀과 벤딩 자격으로 함께 `ATTACH` 하고 `INSERT … SELECT`)을 운영의 실제 꼴로 돌렸다: 릴리스 1128cf47 의 이미지, 그 안의 확장 파일(httpfs 77392a7207·avro 345c3f4e82·iceberg 4c0c0874c4, DuckDB v2.0.0)을 경로로. Basin 의 머리 스냅숏 7855697870624222868 에서 3,292 행을 5.4 초에 옮겼고, 두 쪽의 행 수 3,292 = 3,292, 계약의 키(`notice_source`·`notice_id`·`source_snapshot_id`·`source_line_number`) 유일 3,292 = 3,292, 행마다 해시 같음, 열 12 개 모두 수와 해시가 같다. 표는 `tables/probe/industrial_land_sale_notice_read` 에 섰고, 시험 뒤 네임스페이스 `probe` 를 `recursive`·`force`·`purge` 로 지웠다(204, 그 뒤 404, 만료 대기 0, `tabular_purge` 뒤 `tables/` 의 객체 0). 그다음 조정기의 다시 돌기: 드리프트 0 |

잰 값: Lakekeeper 태스크의 메모리 최대 약 18 MB(0.5 GB 의 3.6 %), CPU 최대 21 %(0.25 vCPU 의). OpenFGA 는 CPU 최대 45 %(동시 100 로드), 메모리 최대 약 98 MB.
공유 RDS 의 여유 메모리는 시험 전 약 160 MB 에서 시험 중 최저 84 MB 로, 연결은 최대 16 개로 늘었다. 로그는 시험이 가장 바쁠 때 4 시간에 56 MB 였다(감사 로그가 권한 확인마다 한 줄).

## 내리기

```bash
tofu destroy -input=false
```

시험의 `_smoke` 표와 심은 파일은 `lake-cleanup`·`warehouse-delete` 로 지운다. 데이터베이스 둘과 로그인은 RDS 에 남는다(부트스트랩이 만든 것이고 OpenTofu 의 자원이 아니다). 내린 뒤 master 로 `DROP DATABASE lakekeeper_dev; DROP DATABASE openfga_dev; DROP ROLE lakekeeper_dev; DROP ROLE openfga_dev;`
를 일회성 태스크로 돌린다. 비밀은 복구 기간 없이 바로 지워진다(dev).
