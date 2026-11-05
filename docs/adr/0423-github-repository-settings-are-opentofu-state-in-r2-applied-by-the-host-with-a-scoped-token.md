# ADR 0423: GitHub 저장소 설정은 OpenTofu 코드다 — 상태는 R2 에 잠금과 함께, 적용은 호스트가 범위 좁은 토큰으로

- Status: Proposed
- Date: 2026-11-06
- Builds on: [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(런타임 비밀은 계약 하나),
  [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)(호스트가 main 을 스스로 배포),
  [ADR-0175](./0175-workers-deploy-themselves-after-the-host-deploys-main.md)(배포 뒤 단계가 호스트의 토큰 파일로 외부 상태를 맞춤),
  [ADR-0197](./0197-ci-downloads-pinned-release-binaries-only-through-one-cached-retrying-fetcher.md)(고정 내려받기),
  ADR-0422(Rego 정책, #1092)

## Context

`perfectory-inc/perfectory-public` 의 GitHub 설정은 지금 세 조각이다(2026-11-06 main).

| 조각 | 무엇 | 크기 |
|---|---|---:|
| 원하는 상태 | `tools/github/*.json` 17개 — 저장소 설정, main ruleset(merge queue 포함), 가지·태그 방화벽, Actions 권한·선택 Action·워크플로 토큰 권한·보존·fork 승인·캐시 | 파일 17 |
| 적용 | `scripts/github/configure-public-repository.sh` — REST 호출을 손으로 순서대로 부른다(`PUT rulesets`, `PATCH repos`, `PUT actions/permissions` …). 소유자가 자기 토큰으로 손으로 돌린다 | 546줄 |
| 검사 | `scripts/guard/public-github-policy.sh` — 원하는 상태 JSON 이 약해지지 않았는지 단정한다(인벤토리 G061) | 695줄 |

이 구조의 문제는 셋이다.
- **표류를 모른다.** 누가 UI 에서 ruleset 을 바꾸면 다음에 누군가 configurator 를 손으로 돌릴 때까지 아무도 모른다.
- **적용 순서와 멱등성이 손으로 짠 셸에 있다.** 새 설정 하나가 REST 호출 하나, 검사 단정 하나, JSON 하나를 함께 요구한다.
- **적용은 사람의 넓은 토큰이다.**

업계에는 두 길이 있다.
- **Terraform/OpenTofu + `integrations/github` 프로바이더.** GitHub 가 유지하는 프로바이더다. 많은 조직이 저장소·팀·ruleset 을 이것으로
  코드화한다. HashiCorp 를 비롯해 Kubernetes·CNCF 계열 조직이 org 관리에 Terraform 을 쓴다.
- **safe-settings.** GitHub 가 만든 Probot 앱이다. org 의 `admin` 저장소에 YAML 로 둔 설정을 webhook 이나 cron 으로 모든 저장소에
  맞춘다.

## 비교

| | A. OpenTofu + `integrations/github` | B. safe-settings |
|---|---|---|
| 우리 원하는 상태를 다 덮는가 | 저장소 설정(`github_repository`), ruleset·merge queue(`github_repository_ruleset`), Actions 권한·선택 Action(`github_actions_repository_permissions`), 워크플로 토큰 권한·PR 승인(`github_workflow_repository_permissions`), 취약점 알림 | 저장소 설정·ruleset·가지 보호·팀·협업자·라벨·환경·변수·custom property. **Actions 권한, 워크플로 토큰 권한, 보존, fork PR 승인, 취약점 알림은 관리 대상이 아니다**(README 의 플러그인 목록) |
| 표류 | `tofu plan -detailed-exitcode` 가 정확한 차이를 낸다. 매일 돌려 알린다 | webhook 으로 즉시 되돌린다(장점). 단 앱이 늘 돌아야 한다 |
| 실행 장소 | 호스트의 배포 뒤 단계(ADR-0175 와 같은 자리) | 상주 앱(서버·Lambda) 또는 Actions |
| 비밀 | 범위 좁은 토큰 하나. 호스트 파일, root 0600 | GitHub App 개인 키. 앱 호스팅 쪽 비밀 |
| 공개 저장소 제약 | 공개 워크플로는 비밀을 읽지 못한다(`check-workflow-policy.sh`). 호스트가 적용하므로 문제 없다 | Actions 로 돌리면 공개 저장소에 비밀이 필요하다 → 금지. 다른 데서 호스팅해야 한다 |
| 새 저장소 필요 | 없다 | org 의 `admin` 설정 저장소 |
| 기존 도구와 맞음 | 계획(`tofu show -json`)에 Rego(ADR-0422)로 "약하게 만들지 않는다"는 성질을 건다. 상태 버킷·잠금은 이미 쓰는 R2 의 조건부 PUT 이다 | 별도 런타임(Node 앱)과 별도 설정 언어 |
| 규모의 목적 | 저장소 하나부터 | org 안 저장소 수십~수천에 같은 기본값 |

## Decision (제안)

**A 를 고른다.** B 는 우리 원하는 상태의 절반(Actions 쪽 전부)을 관리하지 못한다. 공개 저장소에서는 Actions 로 돌릴 수도 없어
상주 앱을 하나 더 늘려야 한다. 저장소가 하나인 지금 B 의 장점(많은 저장소에 같은 기본값)은 쓸 데가 없다.

1. **코드.** `infra/github/` 에 OpenTofu 루트 모듈 하나를 둔다. 리소스는 지금의 `tools/github/*.json` 을 그대로 읽어 만든다
   (`jsondecode(file(...))`). JSON 이 계속 정본이고 HCL 에 값을 복사하지 않는다. 프로바이더 `integrations/github` 와 OpenTofu 의 판은
   잠금 파일(`.terraform.lock.hcl`)과 고정 내려받기(ADR-0197)로 고정한다.
2. **상태.** 전용 비공개 R2 버킷 `perfectory-github-settings-state` 에 S3 백엔드로 둔다.
   - 잠금은 `use_lockfile = true`: OpenTofu 1.10+ 의 S3 네이티브 잠금이다. `If-None-Match: *` 조건부 PUT 으로 `.tflock` 을 만든다.
     R2 는 조건부 PUT 을 지원하고, 우리는 이미 `foundation-outbox` 의 `object_storage/r2/conditional.rs` 에서 쓴다.
     DynamoDB 같은 잠금 테이블은 없다.
   - 상태는 OpenTofu 의 클라이언트 쪽 상태 암호화(state encryption)로 암호화한다.
   - 버킷 열쇠는 이 버킷 하나에만 Object Read & Write 다.
   - 버킷 이름은 구현 PR 에서 R2 연결 계약에 적는다(lane freeze 가 풀린 뒤).
3. **토큰.** fine-grained personal access token 하나다.
   - 범위: 리소스 소유자 `perfectory-inc`, 저장소 `perfectory-public` 하나만. Repository permissions 는 **Administration: Read and
     write** 와 Metadata: Read(자동)뿐이다.
   - 토큰은 호스트 파일 `/etc/foundation-platform/github-settings.env`(root:root 0600, `GITHUB_TOKEN`)에만 있다. 런타임 비밀 계약
     (ADR-0153)에 선택 그룹 `github-settings` 로 적는다. 파일이 없으면 단계는 아무것도 하지 않는다(ADR-0175 와 같은 켜고 끄기).
   - 공개 저장소의 어떤 워크플로도 이 토큰을 보지 않는다.
4. **적용.** 호스트가 main 을 배포한 뒤(ADR-0159) `infra/github/` 의 입력이 바뀌었을 때만 `tofu plan` → `tofu apply` 한다.
   - 바뀐 것만 하는 원칙이다. 입력 해시가 마지막 적용과 같으면 건너뛴다.
   - 매일 한 번 `plan -detailed-exitcode` 로 표류를 재고, 차이가 있으면 알린다.
   - 표류는 자동으로 되돌리지 않는다. UI 에서 바꾼 이유가 사고 대응일 수 있으므로 사람이 본다.
5. **검사.**
   - `public-github-policy.sh` 의 원하는 상태 단정은 계획 JSON 위 Rego 정책(`tools/conftest/github/`)으로 옮긴다. 성질은 그대로다:
     merge queue 필수, 우회자 0, 쓰기 기본 권한 금지 등이다.
   - CI 는 토큰 없이 `tofu validate` 와 Rego 단위 시험만 돌린다.
   - `configure-public-repository.sh` 의 적용 경로는 지운다. 공개 전 순서(법률 확인 → 저장소 생성 → 최초 적용)는 그 런북에 남는다.
6. **덮지 못하는 것.** 구현 PR 에서 프로바이더가 Actions 산출물·로그 보존(`artifact-and-log-retention`)과 fork PR 기여자 승인
   (`fork-pr-contributor-approval`) 리소스를 갖는지 확인한다.
   - 없으면 그 둘만 이름 붙은 예외로 남긴다. configurator 의 해당 REST 호출 두 개를 같은 배포 뒤 단계가 부른다.
   - 프로바이더에 기여(upstream PR)하는 것을 후속으로 적는다.

## 소유자가 한 번 하는 일 (구현 전 필수)

구현은 아래가 끝난 뒤에만 시작한다. 비밀값은 소유자만 보고, 에이전트에게는 "만들었다"와 이름만 알린다.

1. **토큰.**
   1. GitHub → 오른쪽 위 프로필 → **Settings** → **Developer settings** → **Personal access tokens** → **Fine-grained tokens** →
      **Generate new token**.
   2. Token name `perfectory-github-settings`, Expiration 은 조직 정책이 허용하는 최대(만료 전 알림으로 교체).
   3. **Resource owner `perfectory-inc`**.
   4. Repository access: **Only select repositories** → `perfectory-public`.
   5. Repository permissions: **Administration → Read and write**. 그 밖은 모두 No access(Metadata 는 자동 Read).
   6. 조직이 fine-grained 토큰 승인을 요구하면: `perfectory-inc` → Settings → Personal access tokens → Pending requests → 승인.
   7. 서버에서 root 로, 값이 화면·셸 기록에 남지 않게 붙여 넣는다:
      `sudo install -o root -g root -m 0600 /dev/null /etc/foundation-platform/github-settings.env`
      `sudoedit /etc/foundation-platform/github-settings.env` → 한 줄 `GITHUB_TOKEN=<토큰>`.
2. **상태 버킷과 열쇠**(같은 날, [모양 원장 버킷 런북](../../platforms/foundation-platform/docs/runbooks/shape-ledger-bucket.md)과
   같은 방식).
   1. Cloudflare → R2 → Create bucket `perfectory-github-settings-state`. Public access 는 끈다. 잠금 규칙은 없다.
   2. R2 → Manage API tokens → **Object Read & Write, 이 버킷만** → 이름 `perfectory-github-settings-state-prod`.
   3. 같은 env 파일에 `AWS_ACCESS_KEY_ID`·`AWS_SECRET_ACCESS_KEY` 로 넣는다(S3 백엔드가 읽는 이름 그대로).
   4. 상태 암호화 키 `TF_ENCRYPTION` 의 passphrase 도 같은 파일에 넣는다. 소유자가 비밀번호 관리자에 사본을 둔다. 잃으면 상태를
      `tofu import` 로 다시 만든다.

## Consequences

- UI 에서의 표류가 하루 안에 보인다. 설정 변경은 PR → 병합 → 배포 뒤 단계의 `plan`/`apply` 하나의 길이 된다.
- `configure-public-repository.sh` 546줄과 `public-github-policy.sh` 의 단정 대부분이 프로바이더 리소스와 계획 위 Rego 로 바뀐다.
  인벤토리 G061 의 나머지다.
- 새 비밀 셋(토큰, 버킷 열쇠, 암호화 passphrase)이 호스트 파일 하나에 생긴다. 토큰은 저장소 하나·권한 하나로 좁고, 만료가 있어
  교체가 강제된다.
- 기각: GitHub App 인증(프로바이더의 `app_auth`). 토큰이 1시간짜리라는 장점이 있다. 하지만 저장소 하나에는 앱 등록과 개인 키라는
  또 하나의 장기 비밀만 더한다. 저장소가 늘거나 org 수준 관리가 필요해지면 다시 본다.
- 기각: B(safe-settings). 이유는 위 Decision.
- 기각: Pulumi(GZ-ADR-0009 가 AWS 공짱 인프라에 쓴다). 그 영역은 손대지 않는다. GitHub 설정만을 위해 런타임 언어 SDK 와 Pulumi
  상태 백엔드를 더할 이유가 없다.
