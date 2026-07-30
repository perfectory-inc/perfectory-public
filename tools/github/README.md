---
status: current
owner: repository-tooling
doc_type: README
last_reviewed: 2026-07-29
---

# 공개 GitHub 저장소 정책

이 디렉터리는 `perfectory-inc/perfectory-public`의 원하는 상태 SSOT다. 파일 내용은 GitHub 저장소·
Actions 권한·보존·fork 승인·저장소 규칙 API에 직접 대응한다. 이 디렉터리를 바꾸고 검증기를 실행하지
않은 채 실제 설정을 직접 수정하지 않는다.

## 최초 공개

아래 순서를 그대로 따른다. configurator는 저장소를 만들거나 공개 여부를 바꾸지 않는다. 1단계의
법률 전제가 통과하기 전에는 공개 저장소를 만들거나 설정하거나 publisher를 실행하지 않는다.

저장소 공개는 되돌릴 수 없는 공개다. 다시 private으로 바꿔도 새 익명 접근만 막을 뿐 기존 clone과
public fork를 회수할 수 없다. [ADR 0007](../../docs/adr/0007-public-code-private-operations-boundary.md)의
경고를 나중 rollback 계획이 아니라 공개 전제조건으로 취급한다.

1. first-party 법률 검토를 비공개로 완료한다. first-party/proprietary로 공개할 모든 파일의 출처,
   소유권, 양도 여부를 확인하고 증거와 승인 기록은 private operations/evidence 시스템에만 둔다.

   검토가 끝난 뒤에만 `tools/github/legal-identity.json`의 `copyright_holder`를 실제로 법적 근거가
   있는 권리자로 설정하고 `first_party_ownership_or_assignment_confirmed`를 `true`로 둔다.
   결론을 추측하거나 만들지 않는다. boolean은 검토가 있었다는 사람의 fail-closed 확인이며 그 자체가
   법적 증거는 아니다.

   strict 검증은 공개 법률·라이선스 registry 전체를 하나의 계약으로 본다. 법적 identity, 루트
   `LICENSE`와 `LICENSES/LicenseRef-Proprietary.txt`의 정본 본문, 전체 `REUSE.toml` annotation
   allowlist를 검사한다. `REUSE.toml`에는 정본 버전과 정해진 root-proprietary·Pretendard-OFL
   annotation만 둘 수 있다. proprietary license 본문에 추가 허여를 넣을 수 없고 권리자/연도는
   루트 REUSE annotation과 같아야 한다.

   보호된 `tools/github/third-party-artifact-policy.json` 레지스트리는
   `.gitattributes`, `THIRD_PARTY_NOTICES.md`, 두 개의
   `LICENSES/OFL-1.1.txt`, `products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt`,
   Pretendard CSS 및 `pretendard-v1.3.9.sha256`의 정확한 경로 허용 목록과
   SHA-256 다이제스트를 고정한다. 두 OFL 사본은 바이트 단위로 같아야 한다.
   해시 매니페스트는 `public-repository-safety.sh`를 통해 추적 대상 WOFF2 집합과
   해시까지 고정한다. 매니페스트만 바꾸거나 글꼴만 바꿔도 공개 게이트는 실패한다.
   루트 `.gitattributes`가 유일한 속성 SSOT이며, 삭제한
   `products/gongzzang/.gitattributes`는 최종 깨끗한 워크트리 검사에서 뒤늦게
   발견하지 않도록 레지스트리와 함께 검토하는 경로 집합에 포함한다.
   `--allow-unconfirmed` 없이 검증이 통과해야 한다.

   ```bash
   bash scripts/github/validate-legal-publication.sh
   ```

   저장소를 만들거나 `bootstrap`을 실행하기 전에 private readiness branch에서 공개 법률 SSOT를
   검토·커밋한다.

   ```bash
   legal_registry_paths=(
     tools/github/legal-identity.json
     LICENSE
     LICENSES/LicenseRef-Proprietary.txt
     REUSE.toml
     tools/github/third-party-artifact-policy.json
     .gitattributes
     products/gongzzang/.gitattributes
     THIRD_PARTY_NOTICES.md
     LICENSES/OFL-1.1.txt
     products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt
     products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css
     products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256
     'products/gongzzang/apps/web/public/fonts/**/*.woff2'
   )
   git status --short -- "${legal_registry_paths[@]}"
   git diff -- "${legal_registry_paths[@]}"
   git add -- "${legal_registry_paths[@]}"
   git diff --cached -- "${legal_registry_paths[@]}"
   git commit -m "chore: confirm legal publication identity"
   test -z "$(git status --porcelain=v1 --untracked-files=all)"
   ```

   완전한 공개 registry를 원자적으로 검토·stage한다. provenance와 review signoff를 포함한 private
   증거는 절대 포함하지 않는다. strict validator가 미커밋 내용에 대해 통과해도 readiness worktree가
   dirty면 **NO-GO**다.

   attestation이 `false`이거나 권리자가 drift하거나 strict 검증이 실패하면 공개는 **NO-GO**다.
   정본 public CI도 같은 strict 검증을 실행하므로 이후 `true`→`false` 변경은 거부한다.
   정본 public repository 밖 구조 검사에서 사용하는 `--allow-unconfirmed`는 공개 권한을 주지 않는다.

2. `perfectory-inc/perfectory-public`을 비어 있는 **public** 저장소로 생성한다.
   기본 브랜치는 `main`이어야 하며 README, license, `.gitignore`, 브랜치, 태그를
   초기화하지 않는다. 아래 조직 결제 게이트를 확인한다.

3. configurator나 공개 명령을 실행하기 전에 저장소의 변경 불가능한 GitHub
   식별자를 고정한다. 체크인된 `repository_id: 0`과
   `repository_node_id: UNSET_AFTER_REPOSITORY_CREATION`은 의도적인 fail-closed
   자리표시자이며 `bootstrap`과 publisher 내부 준비 단계 모두 이를 거부한다.

   ```bash
   identity_candidate="$(mktemp)"
   bash scripts/github/show-public-repository-identity.sh >"$identity_candidate"
   cat "$identity_candidate"
   ```

    새로 만든 저장소·조직과 `hostname`, `full_name`, `repository_id`,
    `repository_node_id`, 소유자의 `login`, `id`, `node_id`를 대조한다. 검토가
    끝난 뒤에만 정본 출력을 적용하고 공개 안전 트리의 일부로 커밋한다.

   ```bash
   cp -- "$identity_candidate" tools/github/repository-identity.json
   bash scripts/github/validate-public-repository-identity.sh
   git diff -- tools/github/repository-identity.json
   git add tools/github/repository-identity.json
   git commit -m "chore: pin public repository identity"
   rm -f -- "$identity_candidate"
   test -z "$(git status --porcelain=v1 --untracked-files=all)"
   ```

   읽기 전용 도우미는 `github.com`만 사용하고 정본 대상만 조회하며 변경할 수 없는
   소유자를 검증한다. 엄격한 래퍼는 저장소에 기록된 양의 repository ID와 유효한
   repository node ID를 요구하고 owner ID/node ID도 정확한 정본 불변값으로
   유지한다. 정본 public CI는 이 엄격한 양의 식별자를 요구하며 숫자형
   `GITHUB_REPOSITORY_ID`, `GITHUB_REPOSITORY_OWNER_ID`가 체크인된 저장소·소유자
   ID와 일치하는지도 확인한다. 저장소·소유자 node ID는 형식을 검사하고,
   configurator는 저장소와 소유자의 식별자를 실제 GitHub API에서 전부 다시 읽는다.

   비공개 저장소·포크·로컬 구조 검사는 `validate-public-repository-identity.sh
   --allow-unset`을 사용한다. 유효한 양의 식별자 또는 유일한 비양수 예외인
   의도적인 `0`/`UNSET_AFTER_REPOSITORY_CREATION` 자리표시자 쌍만 허용한다.
   이 예외는 설정·공개 권한을 절대 부여하지 않는다. 체크인된 ID는 이후 저장소
   이름 변경·이전·삭제/이름 재사용·잘못된 호스트 로그인이 조용히 공개 대상이
   되는 것을 막는다. 실제 식별자를 검토·엄격 검증·커밋하고 워크트리가 깨끗해질
   때까지 공개는 **NO-GO**다.

4. Bootstrap the identity-pinned empty repository:

   ```bash
   bash scripts/github/configure-public-repository.sh bootstrap
   ```

   `bootstrap`은 조직의 GitHub 예산 목록을 읽고
   `ProductPricing/actions`와 `SkuPricing/actions_cache_storage` 각각에 대해
   조직 범위 USD 0 강제 중지 예산이 정확히 하나인지 요구한다. 누락·중복·형식
   오류·페이지 분할된 예산 데이터는 **NO-GO**다. configurator는 예산을 만들거나
   수정하거나 삭제하지 않는다. 어느 예산이든 없으면 조직 소유자가 GitHub Billing
   (또는 별도 검토한 관리자 API 호출)로 만든 뒤 `bootstrap`을 다시 실행한다.

   Bootstrap은 최초 생성만 허용하고 삭제와 이후 모든 업데이트를 거부하는
   zero-bypass `main` 정책을 설치한다. 모든 non-`main` 브랜치와 태그도 zero
   bypass로 차단한다. 자동 보안 수정은 계속 비활성화하므로 감사한 root가 공개되고
   독립 검증되기 전에는 어떤 통합도 브랜치를 만들 수 없다.

5. Publish a history-free audited root from the exact clean identity-pinned
   commit through the single trusted entry point:

   ```bash
   test -z "$(git status --porcelain=v1 --untracked-files=all)"
   source_commit="$(git rev-parse HEAD)"
   bash scripts/github/publish-public-root.sh "$(git rev-parse --show-toplevel)" "$source_commit"
   ```

   게시자는 제어 워크트리 자체와 정확히 깨끗한 `HEAD`에서 실행해야 한다. 비공개
   임시 bare snapshot을 만들고 같은 publisher 호출 안에서
   `prepare-public-root.sh`를 호출한 뒤 모노레포·공개 게이트 전체를 실행한다.
   소스 트리와 체크인된 중립 식별자로부터 하나의 커밋이며 부모가 없는 `main`
   root를 결정적으로 계산하고, 트리·메타데이터·ref·객체 폐쇄·원격 부재를
   독립적으로 다시 검사한다. dirty 워크트리, 다른 `HEAD`, symlink, gitlink,
   추가 객체, 게이트 실패가 있으면 공개를 중단한다.

   대상은 호출자가 바꿀 수 없는 정해진 정본 URL
   `https://github.com/perfectory-inc/perfectory-public.git`이며 호출자 인자가
   아니다. 준비 뒤 원격이 정확히 비어 있을 때만 새로 공개한다. `HEAD`가
   `main`을 가리키는 symref이고 `HEAD`와 `refs/heads/main`이 결정적으로 예상한
   root와 같으며 다른 ref가 없을 때만 재개한다. 그 밖의 원격 상태는 모두
   fail-closed다. 새 경로에서는 `prepublish`를 실행하고 첫 push 직전에 단일
   writer 공개 권한 검사를 다시 실행한 뒤 명시적 URL로 push하고 `lock`을
   실행한다. 새 경로와 정확한 재개 경로 모두 `activate` 전에 새 독립 clone을
   검증한다. publisher는 비공개 저장소에 public 원격을 추가하지 않는다.

   성공한 실행은
   `OK public-root-publisher mode=<fresh|resume> commit=<root-sha> ...`를 출력한다.
   내부 snapshot은 비공개 임시 상태이며 종료 시 삭제되므로 여기서 `root_sha`를
   읽으려 하지 않는다. 성공 뒤 잠긴 정본 `main` ref에서 같은 SHA를 얻고 정확히
   하나의 40자리 16진수 결과를 요구한다.

   ```bash
   root_sha="$(
     bash scripts/github/safe-git-transport.sh --no-repository \
       ls-remote --exit-code \
       https://github.com/perfectory-inc/perfectory-public.git \
       refs/heads/main \
       | awk 'NR == 1 && $2 == "refs/heads/main" && length($1) == 40 && $1 !~ /[^0-9a-f]/ { sha = $1 }
              END { if (NR != 1 || sha == "") exit 1; print sha }'
   )"
   test "${#root_sha}" -eq 40
   ```

    `lock`은 remote `HEAD`/`main`이 예상 parentless root와 같은지, 다른 ref가 없는지,
    bootstrap update-deny policy가 여전히 활성인지 확인한다. Activation은 zero-bypass
    non-`main` firewall만 최종 organization-maintainer firewall로 바꾸고 자동 security fix는
    끈다. bootstrap `main` update deny는 root CI가 끝날 때까지 그대로 둔다.

6. root 커밋의 모든 workflow가 끝날 때까지 기다린 뒤 최종 보호를 활성화하고
   전체 정책을 다시 읽는다.

   ```bash
   gh run list --repo perfectory-inc/perfectory-public --commit "$root_sha"
   gh run list --repo perfectory-inc/perfectory-public --commit "$root_sha" \
     --json databaseId --jq '.[].databaseId' \
     | while IFS= read -r run_id; do
         gh run watch --repo perfectory-inc/perfectory-public \
           "$run_id" --exit-status
       done
   PERFECTORY_EXPECTED_PUBLIC_ROOT="$root_sha" \
     bash scripts/github/configure-public-repository.sh protect
   bash scripts/github/configure-public-repository.sh verify
   ```

    `protect`는 예상한 parentless root와 고정된 GitHub Actions App의 모든
    `required/*` 결과를 확인하고, bootstrap의 업데이트 거부 정책을 전체
    pull-request 정책으로 원자적으로 교체한 뒤 root SHA를 다시 확인한다.
    workflow 누락, 오래된 성공 결과, 이동한 `main`, 이른 호출은 fail-closed로
    실패한다.

`integration_id: 15368`은 GitHub Actions App이며 `gh api /apps/github-actions`로
독립적으로 읽을 수 있다. Bootstrap에는 우회 actor가 없다. `activate` 뒤에는
브랜치 방화벽 우회 actor가 지정된 조직 관리자(`user_id: 253390842`) 하나뿐이다.
태그 방화벽과 최종 `main` ruleset에는 우회 actor가 없다.

Bootstrap은 dependency alerts, push protection이 있는 secret scanning, 비공개
취약점 보고를 활성화한다. 자동 보안 수정은 정책상 계속 비활성화한다.
보고서는 공개 issue가 아니라 루트 `SECURITY.md`의 비공개 advisory 채널을 사용한다.

## 공개 후 기능 작업

정본 `main` 이외 브랜치 방화벽은 지정된 조직 관리자가 조직 저장소에 일반 기능
브랜치를 만들 수 있게 한다. 정본 저장소를 복제해 `origin/main`을 권위로 삼고,
정확히 그 커밋에서 로컬 브랜치를 만든 뒤 `origin`으로 푸시하고 `main`을 대상으로
풀 리퀘스트를 연다. 개인 포크는 관리자 작업 흐름에 포함하지 않는다.

비공개 이력에만 있는 기능은 기존 트리 전용 연결 도구를 사용한다.

```bash
git clone https://github.com/perfectory-inc/perfectory-public.git public-clone
git -C public-clone switch -c feature/example origin/main
bash scripts/github/import-private-feature-diff.sh \
  /path/to/private-perfectory PRIVATE_BASE PRIVATE_FEATURE public-clone
git -C public-clone diff --check
git -C public-clone status --short
```

가져오기 도구는 비공개 저장소에서 바이너리 트리 차이를 계산하고 임시 인덱스로
적용한 뒤 공개 가드와 트리 비밀값 검사를 실행하고 결과를 스테이징하지 않은 채
남긴다. 결과를 검토한 뒤 공개 복제본에서 커밋하고 조직의 `origin` 원격으로
푸시한다. 비공개 Git 객체는 절대 가져오지 않는다.

공개 저장소를 비공개 워크트리의 원격으로 추가하지 않는다. 공개 복제본에 비공개
원격을 추가하거나 가져오지 않는다. 경계 사이에서 alternates, bundle, graft,
대체 ref, cherry-pick, 공유 객체 저장소를 사용하지 않는다.

GitHub는 public 저장소의 pull-request 생성을 끌 수 없다. `CONTRIBUTING.md`에
따라 외부 코드 기여는 서면 기여·양도 계약이 생길 때까지 받지 않으며, 원치 않는
pull request가 이 정책을 바꾸지 않는다.

## 조직 결제 게이트

저장소 구성 도구는 조직 결제를 소유하지 않는다. CI를 켜기 전에 조직 소유자가
다음을 수행해야 한다.

1. Actions artifact storage의 포함 사용량 90%·100% 알림을 활성화한다.
2. `budget_amount: 0`, `prevent_further_usage: true`인 조직 범위
   `ProductPricing/actions` 예산을 정확히 하나 만든다.
3. 같은 0달러 강제 중지 설정의 조직 범위
   `SkuPricing/actions_cache_storage` 예산을 정확히 하나 만든다. Actions 제품
   예산은 이 SKU 예산을 대신할 수 없다.
4. bootstrap 뒤 artifact/log 보존 기간이 7일인지 확인한다.
5. artifact/Packages 저장소와 dependency-cache 저장소를 별도로 모니터링한다.

표준 `ubuntu-24.04` runner 실행은 public 저장소에서 무료지만, 더 큰 runner와
요금제 허용량을 넘는 저장소는 무료가 아니다. workflow 정책은 문자 그대로
`ubuntu-24.04`가 아닌 모든 runner label을 기계적으로 거부하며, 청구 예산이
측정 과금 초과의 강제 중지 장치다.

`bootstrap`과 `prepublish`를 포함한 모든 configurator 모드는 모드별 변경을 수행하기
전에 읽기 전용 조직 Budgets API로 두 예산을 확인한다. 페이지가 나뉘지 않은 완전한
응답 하나만 허용하며, 대상 누락·중복, 잘못된 scope/type/SKU, 0이 아닌 금액,
`prevent_further_usage`가 `true`가 아닌 경우, 잘못된 데이터, `has_next_page: true`를
거부한다. 예산 변경 endpoint는 절대 호출하지 않는다. 현재 청구 사용량이 작다는
사실은 미래 한도를 보장하는 대체 증거가 아니다.

Dependency-cache 저장소는 artifact/GitHub Packages 저장소 풀과 다르다.
configurator는 GitHub REST API 버전 `2026-03-10`으로 저장소 cache 크기·보존
한도만 읽고 cache-limit `PUT`은 절대 보내지 않는다. `200` 응답은
`max_cache_size_gb <= 10`, `max_cache_retention_days <= 7`일 때만 허용한다.
유일한 fail-closed 무결제 예외는 요금제가 정확히 `free`이고 한도 요청이 정확한
HTTP `402` 메시지(`Please ensure your account has a valid payment method on file to access this service.`)를 반환하는 조직이다. 이 상태에서는 유료
cache-limit 선택을 막는다. 그 밖의 상태·본문·요금제·더 큰 한도는 모두 공개
실패다.

현재 cache 사용량은 관측값일 뿐 설정된 상한이 아니다. 사용량이 작다는 결과만으로
향후 쓰기가 제한된다고 증명할 수 없다. 검증된 예산 기록과 한도·능력 확인이 모두
필요하다. 다음 문서를 참고한다.
[Budgets REST API](https://docs.github.com/en/rest/billing/budgets?apiVersion=2026-03-10),
[Actions cache REST API](https://docs.github.com/en/rest/actions/cache?apiVersion=2026-03-10)
와 [cache usage limits and eviction policy](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#usage-limits-and-eviction-policy)를 참고한다.

최상위 pull-request 경로 필터는 금지한다. GitHub가 workflow 전체를 건너뛸 때
필수 check를 pending으로 남길 수 있기 때문이다. push 필터는 허용한다.
`scripts/guard/public-github-policy.sh`가 이 디렉터리와 workflow job 이름,
정확한 서드파티 Action 참조를 서로 대조한다.
