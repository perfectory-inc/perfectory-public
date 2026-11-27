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
| 창고(warehouse) `foundation-platform-lakehouse-dev`, 네임스페이스 `silver`·`gold`, 표는 `<네임스페이스>/<표>/` | dev 버킷만. 저장 자격은 이 리그만 쓰는 새 Cloudflare 토큰(사장 결정 2026-11-27, A) | 창고 태스크(`./probe.sh warehouse warehouse-create`, 아래). 그 토큰을 읽는 것은 이 태스크뿐이다 |

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
./probe.sh <주체> <걸음> [인자...]   # 주체: bootstrap, lane-a, lane-b, outsider, anonymous, warehouse, logscan, maintenance
```

- `probes/probe.py` 의 걸음. 신원·권한: `health`, `info`, `claims`, `bootstrap`, `whoami`, `identity`, `expiry`, `config`, `provision <주체>`,
  `authz-denied`, `authz-role-creator`, `grant|revoke <권한> <주체>`, `refused`, `loop <초>`. 창고: `lake-setup`, `lake-scope [카나리아]`,
  `lake-privileges`, `lake-duckdb`, `lake-history <커밋 수>`, `lake-escape`, `lake-escape-create`, `lake-drop drop|purge [표]`, `lake-mints <수> [동시]`,
  `lake-commits <초>`, `lake-reconcile inject|reconcile`, `lake-refresh`, `lake-register-from <표> <metadata.json>`, `lake-maint setup|verify`,
  `lake-tasks`, `lake-cleanup`. 창고 태스크(R2 토큰을 쥔 유일한 태스크): `warehouse-create`, `warehouse-list|put|hash|delete <키>…`, `warehouse-vend-check`.
- `logscan <분>`: 로그 그룹에서 리그의 비밀 값(R2 토큰 빼고 전부)과 JWT·키 모양을 센다(`probes/log-scan.sh`). 값과 줄은 찍지 않는다.
- `maintenance <표> <지울 표>`: 상류 그대로의 Spark 와 Iceberg 1.12.0 의 `expire_snapshots`·`remove_orphan_files`(`prefix_listing`)·
  `DROP TABLE … PURGE`(`rest-catalog-purge`), 벤딩 자격만으로(`probes/maintenance.py`, `maintenance.tf`).
- 줄마다 JSON 하나이고 토큰·비밀·응답 본문은 찍지 않는다(쥔 비밀의 값은 줄에서 가린다). 끝 코드 0 은 모든 검사가 맞았다는 뜻이다.

## 시험 기록 (2026-11-27, Lakekeeper v0.15.0 → v0.15.1, OpenFGA v1.14.2)

판정은 개념 문서(concepts brief "Lakekeeper to the limit" §4)의 통과 기준이다. 걸음은 위 탐침이다.

| 시험 | 판정 | 걸음 | 결과 |
| --- | --- | --- | --- |
| T1 | 통과 | 적용, `health`, `info`, `warehouse-create`, `warehouse-list ""` | migrate 일회성 태스크 0, `/health` 200(OpenFGA·풀 ok), 창고 201, 버킷 루트에 남은 검증 객체 0 |
| T3 | 통과 | `lake-setup` | 표가 `s3://<버킷>/silver/<표>`, 식별자 `silver.<표>`, 문자열 접두 형제(`w109_smoke`·`w109_smoke_versions`)가 따로 선다 |
| T4 | 레인 통과, 결정 거리 | `lake-escape-create` | 레인은 어디든 만들기 403. 운영자는 창고가 버킷 루트라 `bronze/`·`media/` 에도 만든다(다른 표 경로·버킷 루트·다른 버킷은 거부) |
| T5 | 통과 | `lake-scope`, `warehouse-vend-check` | 제 표 접두의 GET·LIST(쓰기면 PUT·DELETE)만 200, 형제·루트·bronze 카나리아·운영 버킷 403, 읽기 자격의 PUT 403, 세션 토큰·갱신 끝점 있음, 응답에 부모의 비밀 키·토큰 없음(access key id 는 부모의 것을 그대로 쓴다) |
| T6 | 통과 | `identity`, `expiry`, `config`, `provision` | 무토큰·엉터리·서명 위조·미선언 클라이언트·다른 범위 401, 선언된 레인은 `oidc~<client id>` 로 매핑, 만료 90 초 뒤 401(시간 검사의 유예 60 초), 새 토큰 200 |
| T7 | 통과(단서) | `authz-*`, `grant`, `revoke`, `refused` | 권한 없음 403, 부여 즉시 되고 회수 즉시 빠진다. OpenFGA 를 내리면 판단이 드는 모든 호출 503(운영자도). 발급자에 못 닿으면 이미 받은 키로 토큰을 계속 받는다 |
| T9 | 통과 | `lake-duckdb` | DuckDB 2.0.0 이 OAuth2 비밀로 ATTACH, 벤딩 자격으로 SELECT·INSERT·고정 스냅숏 읽기. bronze 범위나 표 경로 범위의 엉터리 S3 비밀이 있어도 벤딩 자격이 이긴다 |
| T11a | 통과 | `lake-drop drop`, `lake-drop purge` | 보호된 표 지우기 거부, 소프트 삭제·목록·복구, 정리 전 같은 이름은 `LocationAlreadyTaken`, 만료 뒤 퍼지 작업이 파일을 지우고 같은 이름이 다시 선다. PyIceberg 는 `purgeRequested=false` 를 보내므로 그때는 파일이 영영 남는다 |
| T11b | 통과(설정 하나) | `lake-maint setup`, `warehouse-put`, `maintenance`, `warehouse-hash`, `lake-maint verify` | 참조(main·태그·브랜치)가 모두 읽히고 태그 없는 옛 스냅숏이 만료(태그된 스냅숏의 조상도), 표의 고아만 지워지고 형제·`work/`·`bronze/` 의 파일은 해시가 그대로, Spark 의 `DROP … PURGE` 는 소프트 삭제를 거친다. 단 정비 실행기에 `s3.delete-enabled=true` 가 있어야 한다: 없으면 Lakekeeper 가 보낸 false 로 아무것도 지우지 않고 지웠다고 센다 |
| T12 | 일부 | `lake-refresh` | 자격이 끝나는 시각(3,600 초)과 갱신 끝점이 오고, 갱신 끝점이 같은 표 접두의 자격을 준다. 자격보다 긴 스캔·두 자격에 걸친 멀티파트는 돌리지 않았다 |
| T13 | 실패(dev 크기) | `lake-mints 100 50`, `lake-mints 100 10` | 동시 50 은 100 번 모두 503(OpenFGA 의 확인이 DB 연결 4 개에서 3 초 기한을 넘김). 동시 10 은 100 번 모두 200, 2.3 초, 자격 발급 1 번(캐시) |
| T14 | 일부 | `loop`, `lake-commits` + 태스크 멈춤 | 호출 3,992 번 중 1 번, 커밋 129 번 중 4 번 태스크를 멈춰도 실패·중복·누락 0. 전부 내리면 새 호출은 곧바로 실패. 동시 두 쓰기·RDS 장애 조치는 돌리지 않았다(공유 단일 AZ 인스턴스) |
| T15 | 일부 | `lake-register-from` | 카탈로그가 잃은 표가 metadata.json 하나로 스냅숏·행 그대로 돌아온다. Lakekeeper 에는 `unregister` 가 없다(404). DB 스냅숏 복원·키 없는 비밀 저장소는 돌리지 않았다 |
| T16 | 통과 | `loop` + v0.15.0→v0.15.1 적용, `lake-commits` + 강제 재배포 | 업그레이드(migrate 뒤 롤링) 동안 호출 9,596 번 실패 0, 롤링 재시작 동안 커밋 155 번 실패 0 |
| T22 | 통과 | `logscan` (info, 그리고 `-var catalog_rust_log=debug`) | 비밀 8 개의 값·JWT·AWS 키 모양 0 줄. R2 토큰의 값은 찾지 않았다(그 비밀은 창고 태스크만 읽는다); 64 자 hex 모양 0 줄 |
| T31 | 실패 | `lake-privileges` | `write` 로 추가·태그는 되고 만들기·이름 바꾸기·지우기는 403이지만, 열 추가와 속성 변경도 된다. 상류 문서가 그렇게 적는다(`write` = 데이터·스키마·파티션·정렬·속성·브랜치·태그·이력) |
| T32 | 통과 | `lake-reconcile inject`, `lake-reconcile reconcile` | 선언 밖의 권한을 표마다 읽은 목록에서 찾아 한 번에 지우고, 살아 있는 권한이 선언과 같다 |
| T33 | 통과 | `lake-history 101` | 커밋 101 번 뒤 스냅숏 id·태그로 읽히고, 첫 metadata 파일은 지워져(404) `metadata_location` 고정은 깨진다 |
| T35 | 통과 | 계획에 `allowall` 또는 `LAKEKEEPER__INSECURE_ALLOW_UNAUTHENTICATED` | 두 경우 모두 `tofu plan` 이 전제 조건으로 멈춘다. 정적 R2 키로 작업이 시작을 거부하는 것은 작업 코드의 일이라 여기 없다 |
| T36 | 실패 | `lake-escape` | 다른 표의 파일을 가리키는 커밋이 v0.15.1 에서 받아들여진다. 고친 것(lakekeeper#2217)은 아직 어느 릴리스에도 없다 |
| T8 | 돌리지 않음 | — | iceberg-rust 0.11 과 우리 클라이언트는 이 노트북에서 지을 수 없다(cargo 빌드 금지). CI 가 지은 arm64 시험 이미지가 필요하다 |

잰 값: Lakekeeper 태스크의 메모리 최대 약 18 MB(0.5 GB 의 3.6 %), CPU 최대 21 %(0.25 vCPU 의). OpenFGA 는 CPU 최대 45 %(동시 100 로드), 메모리 최대 약 98 MB.
공유 RDS 의 여유 메모리는 시험 전 약 160 MB 에서 시험 중 최저 84 MB 로, 연결은 최대 16 개로 늘었다. 로그는 시험이 가장 바쁠 때 4 시간에 56 MB 였다(감사 로그가 권한 확인마다 한 줄).

## 내리기

```bash
tofu destroy -input=false
```

시험의 `_smoke` 표와 심은 파일은 `lake-cleanup`·`warehouse-delete` 로 지운다. 데이터베이스 둘과 로그인은 RDS 에 남는다(부트스트랩이 만든 것이고 OpenTofu 의 자원이 아니다). 내린 뒤 master 로 `DROP DATABASE lakekeeper_dev; DROP DATABASE openfga_dev; DROP ROLE lakekeeper_dev; DROP ROLE openfga_dev;`
를 일회성 태스크로 돌린다. 비밀은 복구 기간 없이 바로 지워진다(dev).
