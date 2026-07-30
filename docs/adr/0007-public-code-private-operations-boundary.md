---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-28
---

# ADR 0007: 공개 코드 정본과 비공개 운영 경계

- Status: Accepted
- Date: 2026-07-23
- Amends: ADR-0002, ADR-0003

## Context

기존 비공개 모노레포 이력에는 날짜가 붙은 계획·검토·인수인계·실시간 운영 증거,
provider/account binding과 과거 build output이 들어 있다. 이 저장소를 공개하면 현재 트리에서
삭제된 내용까지 포함해 도달 가능한 모든 commit이 공개된다. Private GitHub Actions 자체가
불가능한 것은 아니지만, 계정의 billing·quota 정책 때문에 이 저장소를 권위 있는 CI 게이트로
쓰는 운영 방식은 채택하지 않는다.

제외 목록을 둔 반복 mirror는 쓰기 가능한 코드 원천 두 개와 저장소 트리에서 어긋날 수 있는
두 번째 분류 체계를 만든다. 기존 history를 다시 쓰는 작업은 파괴적이며, 과거의 모든 binary와
workflow artifact를 다시 인증해야 한다.

## Decision

`perfectory-inc/perfectory-public`은 공개 기간 동안 소스 코드·pull request·CI의 정본 저장소다.
공개 저장소는 공개 가능한 tracked tree에서 만든 감사 완료 root commit 하나로 시작한다. private
commit·tag·pull request·issue·workflow run·artifact·Git metadata는 어느 것도 옮기지 않는다.

공개 코드 트리는 `**/docs/archive/**`, `**/docs/review/**`, agent memory snapshot, 날짜가 붙은
handoff, 현재 resource inventory, host/deployment 증거, provider username, account 식별자,
account별 endpoint, public-storage host binding과 credential을 넣지 않는다. 현재 contract는
유지되는 ADR·specification·runbook 또는 코드에 둔다.

안정적인 논리 resource namespace label은 의도적인 fail-closed 애플리케이션·schema 계약에서
허용될 때만 공개한다. account·endpoint·credential·host와 현재 상태 binding은 비공개로 둔다.

정본 공개 저장소와 소유 조직의 immutable numeric/node ID는 제한된 예외로 공개한다. 이 값은
rename/transfer, 삭제 후 이름 재사용, 잘못된 owner·host 대상의 publication을 거부하는 공개
control-plane 불변식이며 runtime account binding이 아니다. 개인 GitHub login, numeric ID,
node ID와 numeric `noreply` 주소는 비공개로 둔다. 정본 공개 CI는 저장소에 기록된 positive
identity를 요구하고 numeric repository·owner ID를 `GITHUB_REPOSITORY_ID`,
`GITHUB_REPOSITORY_OWNER_ID`와 대조한다. Private·fork·local 구조 검사는 의도적으로 비워 둔
repository pair를 non-positive 예외로만 허용하며 publication 권한을 주지 않는다. Node ID 형식을
검사하고 configurator는 어떤 모드든 진행하기 전에 GitHub API 식별자를 다시 읽는다.
부모가 없는 공개 root commit은 중립적인 결정론적 identity `Perfectory
<public-root@perfectory.invalid>`를 사용하며 특정 maintainer의 GitHub account로 귀속하지 않는다.

기존 비공개 저장소는 두 번째 코드 원천이 아니라 전환 archive다. 이전 후에는 읽기 전용으로
보관한다. 새로운 운영 증거는 별도의 private operations repository 또는 외부 evidence store에
기록한다. secret을 포함한 live workflow와 self-hosted runner는 비공개 영역에 두고, 공개
저장소에는 재현 가능한 script와 secretless GitHub-hosted CI만 둔다.

First-party source code는 inspection을 위해 공개하지만 저장소의 정본 All Rights Reserved
license file에 따라 proprietary로 유지한다. 공개 visibility는 open-source grant를 만들지 않는다.
GitHub에는 공개 사용자가 pull request를 만드는 것을 막는 switch가 없다. 따라서 서면
contribution/assignment 절차가 `CONTRIBUTING.md`에 마련되기 전까지 외부 code contribution을
받지 않는다. 열린 pull request는 수락이나 license grant가 아니며 third-party asset은 자체
notice와 REUSE annotation을 유지한다.

이 source-code 규칙은 수집한 public data, sample record, fixture, font 또는 기타 asset을
first-party로 분류하지 않는다. 실제처럼 보이는 data fixture는 명백한 synthetic data로 교체하거나
기록된 provenance와 redistribution 조건으로 뒷받침하기 전까지 publication을 차단한다.
출처를 알 수 없으면 실패이며 묵시적으로 허용하지 않는다.

`tools/github/legal-identity.json`은 공개 법적 identity와 사람의 확인을 기록하는 SSOT다.
전체 legal/licensing contract는 canonical root·proprietary license 본문, 전체 REUSE annotation
allowlist와 `tools/github/third-party-artifact-policy.json`을 exact pin한다. 보호 registry는
`.gitattributes`, `THIRD_PARTY_NOTICES.md`, 두 OFL 사본, Pretendard CSS와 hash manifest의
SHA-256을 고정한다. public-tree safety는 이 manifest로 tracked WOFF2 집합과 hash를 강제한다.
root `.gitattributes`가 attributes SSOT이며 기존 `products/gongzzang/.gitattributes` 삭제도 이
registry와 함께 반영한다. 공개 repository를 생성·설정·게시하기 전에 모든 first-party/proprietary
파일을 대상으로 private provenance·소유권·assignment 검토를 완료해야 한다. 기록된
`copyright_holder`는 법적으로 뒷받침 가능한 실제 권리자여야 하며 canonical proprietary license와
root REUSE annotation과 일치해야 한다. 증거와 signoff는 비공개로 둔다.
`first_party_ownership_or_assignment_confirmed`는 검토 후에만 `true`가 될 수 있고, 이는 fail-closed
사람 확인이지 법적 증거가 아니다. 엄격한 검증은 `bootstrap`, `prepublish`, publication 준비,
publisher와 정본 공개 CI 모두에서 수행되므로 확인된 `true`를 다시 `false`로 바꾸는 것도 거부한다.

Publication과 지속 운영은 다음 가드로 fail-closed 처리한다.

- 금지된 evidence/artifact 경로와 위험한 file type을 막는 tracked-tree guard;
- proprietary package license와 non-publishability contract 하나;
- Action full-SHA 및 container digest 강제;
- 공개 workflow에서 variable/self-hosted runner와 repository secret 금지;
- 제한된 gitleaks 예외와 worktree·전체 history scan;
- GitHub secret scanning, push protection, read-only workflow token, Action allowlist와 보호된
  `main`;
- public issue를 끄고 private vulnerability reporting을 보안 신고 경로로 사용.

GitHub desired state는 `tools/github/`에서 버전 관리하고 `scripts/github/configure-public-
repository.sh`가 적용한다. `main`에는 bypass actor가 없고 squash merge만 허용하며, conversation이
해결된 pull request를 요구하고 삭제·non-fast-forward update를 거부한다. GitHub Actions가 만든
안정적인 `required/*` check만 받는다. maintainer가 한 명뿐인 동안 approval 수는 0으로 둔다.
pull-request 작성자의 approval을 요구하면 repository를 merge할 수 없기 때문이다. 두 번째
maintainer가 합류하면 이 수를 올린다.

Required workflow는 top-level path filter 없이 모든 pull request에서 실행한다. 전체 workflow가
path filter로 건너뛰면 GitHub가 성공한 required check를 만들지 않아 pull request가 영구적으로
pending 상태가 될 수 있기 때문이다. push에는 path filter를 남겨도 된다. 각 multi-job workflow는
내부 결과를 안정적인 `required/<area>` terminal check 하나로 합치므로 내부 job 이름을 바꿔도
branch protection은 바뀌지 않는다.

저장소 script는 digest-pinned container image에서 Lychee와 REUSE 도구를 실행한다. commit으로
고정한 wrapper Action도 검증되지 않은 executable을 내려받거나 변경 가능한 base image에서
build하면 충분하지 않다. 따라서 public guard는 직접 참조한 third-party Action, exact-SHA GitHub
allowlist, 고정된 검증 image, required-check 이름과 ruleset payload를 하나의 contract로 대조한다.

## GitHub Actions cost boundary

공개 visibility가 모든 Actions resource를 무제한으로 만들지는 않는다. GitHub 호스팅 표준
runner는 공개 저장소에서 무료지만 더 큰 runner는 과금된다. 따라서 workflow 정책은 표준 runner
label `ubuntu-24.04`를 그대로 요구한다. 변수 label, self-hosted label, runner group과
larger-runner label은 검증에서 실패한다.

Artifact storage는 GitHub Packages와 공유하는 별도 pooled allowance다. 현재 조직용 GitHub Free는
artifact storage 500 MB를 포함하므로 allowance를 다 쓴 뒤 billing이 켜져 있으면 public repository도
storage 비용이 발생할 수 있다. repository는 `tools/github/artifact-retention.json`으로 artifact와
log 보존 기간을 7일로 설정하고 policy guard와 configurator가 이 값을 적용한 뒤 다시 읽는다.
Artifact를 만드는 job은 개별 보존 기간을 7일로 유지하거나 repository 값을 상속해야 한다. CI
출력은 진단 증거이며 영구 release storage가 아니다.

지출 상한은 사람이 설정한 환경변수 증명이나 특정 시점 사용량이 아니라 GitHub 읽기 전용 조직
Budgets API에서 확인한다. Publication에는 `ProductPricing/actions`와
`SkuPricing/actions_cache_storage` 각각에 대해 `budget_amount: 0`,
`prevent_further_usage: true`인 조직 범위 budget이 정확히 하나 있어야 한다. Verifier는 완전한
단일 페이지 inventory를 요구하며 대상 누락·중복, 잘못된 scope/type/SKU, 0이 아닌 금액,
비활성 hard stop, 잘못된 데이터 또는 `has_next_page: true`이면 fail-closed한다. 모든 configurator
모드는 모드별 변경 전에 이 값을 읽는다. Bootstrap과 publication은 budget을 만들거나 갱신·삭제하지
않으며, 누락된 budget은 조직 owner가 별도로 만든 뒤 검증을 다시 실행해야 한다.

Actions dependency-cache storage는 다른 SKU이며 artifact/GitHub Packages pool에 포함되지 않는다.
따라서 configurator는 GitHub REST API `2026-03-10`으로 별도의 읽기 전용 repository cache-capability
검사를 수행하며 `PUT`으로 cache limit을 올리지 않는다. storage와 retention endpoint가 `200`을
반환하면 publication은 `max_cache_size_gb <= 10`, `max_cache_retention_days <= 7`을 요구한다.
허용되는 유일한 대안은 organization plan이 정확히 `free`이고 GitHub가 HTTP `402`와 정확한
메시지 `Please ensure your account has a valid payment method on file to access this service.`를
반환하는 현재 fail-closed 상태다. 이는 billing state를 바꾸기 전에는 유료 cache 용량을 선택할
수 없다는 뜻이다. 다른 response·message·plan 또는 더 큰 limit은 publication을 차단한다.

Cache usage는 특정 시점 측정값일 뿐 미래 지출 상한을 증명하지 못한다. 검증된 budget이나
configured-limit/capability 검사를 대체해서는 안 된다. Free plan의 정확한 `402` cache-limit
response는 현재 유료 cache-limit opt-in이 불가능하다는 증거지만, 필요한 두 zero-dollar budget
record를 대신하지 않는다.

조직 소유자는 included-usage alert를 켜고 Actions artifact/Packages와 cache storage를 따로
감시한다. 정확히 zero-dollar인 hard-stop budget 하나라도 만들거나 검증할 수 없으면 publication은
**NO-GO**다. 양수이거나 단지 작은 budget은 대체안으로 인정하지 않는다. configurator는 bootstrap과
prepublication 모두에서 조직 소유 live setting을 읽어 billing plan이나 budget drift가 발생하면
fail-closed한다.

Sources: [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions),
[larger-runner billing](https://docs.github.com/en/actions/concepts/runners/larger-runners),
[artifact retention](https://docs.github.com/en/organizations/managing-organization-settings/configuring-the-retention-period-for-github-actions-artifacts-and-logs-in-your-organization),
[budgets and hard stops](https://docs.github.com/en/billing/how-tos/set-up-budgets),
[Budgets REST API](https://docs.github.com/en/rest/billing/budgets?apiVersion=2026-03-10),
[Actions cache REST API](https://docs.github.com/en/rest/actions/cache?apiVersion=2026-03-10), and
[cache usage limits and eviction policy](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#usage-limits-and-eviction-policy).

공개 저장소는 출시 전에 비공개로 바꿀 수 있다. 그러면 새로운 익명 접근은 막지만 공개 기간에
생성된 clone이나 public fork를 회수할 수는 없다.

## Publication and feature-transfer procedure

Publication is a one-way, exact-tree operation. The required order is:

1. 공개 repository를 만들거나 설정하거나 publisher를 실행하기 전에 모든 first-party/proprietary
   파일의 provenance·소유권·assignment를 비공개로 검토한다. 증거와 review signoff는 비공개로
   유지한다. 검토가 끝난 뒤에만 `tools/github/legal-identity.json`의 `copyright_holder`를 법적으로
   뒷받침 가능한 실제 권리자로 설정하고 `first_party_ownership_or_assignment_confirmed`를
   `true`로 둔다. 두 값을 추측하거나 만들어내지 않는다. 이 boolean은 fail-closed 사람 확인이지
   법적 증거가 아니다. 엄격한 validator는 해당 JSON, canonical root `LICENSE`와
   `LICENSES/LicenseRef-Proprietary.txt` 본문, 전체 REUSE annotation allowlist와 보호된
   third-party digest registry를 exact pin한다. registry는 `.gitattributes`,
   `THIRD_PARTY_NOTICES.md`, 두 OFL 사본, Pretendard CSS와 hash manifest를 포함하며 public-tree
   safety는 manifest로 WOFF2 집합과 hash를 강제한다. proprietary license 본문에는 추가 grant를
   넣을 수 없고 holder/year는 root REUSE annotation과 일치해야 한다. 엄격한 validator는
   `--allow-unconfirmed` 없이 통과해야 한다.

   ```bash
   bash scripts/github/validate-legal-publication.sh
   ```

통과하기 전까지 publication은 NO-GO다. legal identity, 두 license file, `REUSE.toml`, digest
registry, 직접 hash하는 registry artifact 6개와 manifest에 기록된 WOFF2 file을 같은 경로 집합으로
`git diff`, `git add`, `git diff --cached`하여 검토·원자적으로 stage한다. root `.gitattributes`가
유일한 SSOT이므로 `products/gongzzang/.gitattributes` 삭제도 같은 경로 집합에 포함한다. private
readiness branch에 public registry를 commit하고 step 2 또는 `bootstrap` 전에 clean worktree를
요구한다. private 증거와 signoff는 stage·commit하지 않는다. 정본 public CI도 같은 strict mode를
사용하므로 나중에 `true`를 `false`로 바꾸는 것을 거부한다.
2. `perfectory-inc/perfectory-public`을 `main`을 default로 하는 빈 public repository로 만든다.
   README, license, `.gitignore`, branch, tag를 초기화하지 않는다.
3. 읽기 전용 `show-public-repository-identity.sh`를 실행해 정본 GitHub.com repository·owner ID를
   검토하고 그 canonical output을 `tools/github/repository-identity.json`에 반영해 commit한다.
placeholder ID/node ID는 의도적인 NO-GO 상태다. Immutable ID는 repository의 rename·transfer,
삭제 후 이름 재사용, 잘못된 owner·host 대상을 거부하는 데 사용한다. commit 전에 저장소에 고정된
엄격한 검증을 실행한다.

   ```bash
   bash scripts/github/validate-public-repository-identity.sh
   ```

정본 public CI는 저장소에 기록된 numeric repository·owner ID를 `GITHUB_REPOSITORY_ID`와
`GITHUB_REPOSITORY_OWNER_ID`에도 대조한다. Private·fork·local 구조 검사는 placeholder를
non-positive 예외로만 허용하며 publication 권한을 주지 않는다. Node ID 형식을 검사하고
configurator는 live API identity를 모두 다시 읽는다.
4. `configure-public-repository.sh bootstrap`을 적용한다. 먼저 billing state를 바꾸지 않고 조직의
   정확한 zero-dollar hard-stop budget 두 개를 읽어 검증한다. 이어서 한 번의 생성을 허용하되
   삭제·후속 update를 거부하고, `main` 이외 모든 branch와 tag를 차단하며, 자동 security fix를
   끈 zero-bypass `main` policy를 설치하고 빈 상태를 검증한다.
5. identity가 고정된 clean source commit을 input tree로 사용한다. private ancestry는 어느 것도
   publication 대상이 아니다. 신뢰하는 유일 publisher entry point를 호출한다.

   ```bash
   source_commit="$(git rev-parse HEAD)"
   bash scripts/github/publish-public-root.sh "$(git rev-parse --show-toplevel)" "$source_commit"
   ```

publisher는 source와 control worktree가 publisher 자신의 repository root인지 요구한다. private
temporary bare snapshot을 만들고 `prepare-public-root.sh`를 내부 호출해 전체 monorepo/publication
gate를 수행한다. 정확한 source tree와 저장소에 기록된 neutral identity에서 부모 없는 1 commit
`main` root를 결정론적으로 계산하고 다시 확인한다. literal canonical URL만 대상으로 하며 private
repository에 public remote를 추가하지 않는다. 정확히 빈 canonical remote에서만 새 publication을
수행하거나, `HEAD`가 `main`을 가리키는 symref이고 `HEAD`·`main`이 예상 root SHA이며 다른 ref가
없는 완전한 원격 상태에서만 재개한다. 불일치는 모두 fail-closed한다. 새 경로에서는 첫 explicit-
URL push와 `lock` 직전에 `prepublish` 뒤 sole-writer authority를 다시 확인한다. 새 경로와 정확한
재개 경로 모두 `activate` 전에 독립 clone을 검증한다. Activation은 조직 maintainer에게만
non-`main` branch firewall 우회를 주고 자동 security fix는 끈 채 `main` update 거부를 유지한다.
publisher는 종료할 때 private temporary snapshot을 삭제하고 최종
`OK public-root-publisher mode=<fresh|resume> commit=<root-sha> ...` 줄에 root를 보고한다.
성공한 뒤에는 canonical `main` ref도 읽을 수 있다.
6. 보고된 root commit의 모든 required check가 성공할 때까지 기다린다. 그런 다음 `protect`를
   적용하고 마지막에 `verify`를 실행한다. `protect`는 예상 parentless root와 pinned-App green
   check를 확인하고 bootstrap `main` rule을 full pull-request policy로 원자적으로 교체한 뒤 root
   SHA를 다시 확인한다.

정본 저장소는 승인되지 않은 `main` 이외 branch와 모든 tag를 거부한다. Bootstrap에는 우회가
전혀 없다. 부모 없는 root를 독립적으로 검증한 뒤 `activate`가 지정된 조직 maintainer에게만
non-`main` branch firewall 우회를 준다. 일반 feature 작업은 조직 소유 branch에서 하고
canonical `main`으로 pull request를 연다. tag firewall에는 우회가 없다. bootstrap `main` update
거부는 첫 push부터 root CI까지 유지하며, required check가 성공한 뒤 최종 `protect`만 양쪽의
예상 SHA 검사를 확인하고 이를 교체한다.

`scripts/github/import-private-feature-diff.sh`는 private branch에 이미 있는 작업을 옮기는 유일한
지원 bridge다. private base→feature tree diff를 public `origin/main`에서 만든 clean named branch에
적용하고 public-tree guard를 실행한 뒤 변경을 unstaged 상태로 남겨 검토하게 한다. commit object는
복사하지 않는다. 검토한 branch는 개인 fork가 아니라 조직 소유 remote로 push한다. private worktree에
public repository를 remote로 추가하지 말고, public clone 안에서 private remote를 추가하거나 fetch하지
말며, alternate·bundle·graft·replacement ref·cherry-pick으로 private history를 경계 밖으로 옮기지
않는다.

## Consequences

- 코드와 PR은 하나의 SSOT를 가지며 표준 public GitHub-hosted Actions를 minute charge 없이 쓸 수
  있다. pooled allowance를 넘는 larger runner와 storage까지 무료라는 뜻은 아니다.
- 코드 history를 공개하지 않고도 private 운영 증거를 보존할 수 있다.
- 과거 plan은 의도적으로 public tree에서 link하지 않는다. 유지되는 ADR과 코드는 모든 현재
  contract를 직접 담아야 한다.
- 기존 private feature 작업은 public `main`에 기반한 조직 branch에 검토된 tree diff로만 옮기며,
  private ancestor를 push하거나 fetch하지 않는다.
- 출시 전에 repository visibility를 private로 바꾸면 billing과 feature 차이를 다시 평가한다.
