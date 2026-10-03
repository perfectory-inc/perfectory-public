---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-03
---

# ADR-0136 — 릴리스 인증은 GitHub 로그인 없이 공개 저장소 identity를 읽는다

이 결정은 [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)
§5의 호스트 선행 조건 중 "root의 `gh` 인증" 한 줄을 바꾼다. ADR-0134 §5는 이 변경에 별도 결정이 필요하다고
적었다. 나머지 ADR-0134 결정(정본 main 독립 fetch, 바이트 대조, 산출물·설정 분리, sudo 경로)은 그대로다.

## 원인과 불변식

ADR-0134는 `prepare`가 정본 저장소의 불변 identity(저장소 id·node_id, 소유자 id·node_id)를
`show-public-repository-identity.sh`로 읽어 `tools/github/repository-identity.json`과 대조하게 했다.
그 스크립트는 `gh api`를 불렀고, 그래서 운영 호스트 root의 `gh` 로그인이 설치 선행 조건이 됐다.

운영 호스트에는 `gh`가 없다(2026-10-03 소유자 확인). 정본 저장소는 공개 저장소다. 대조에 필요한
값은 모두 로그인 없이 읽힌다.

- root의 `git ls-remote`와 literal HTTPS fetch는 이미 동작한다(소유자 측정).
- `GET https://api.github.com/repos/perfectory-inc/perfectory-public`은 인증 없이 `id`·`node_id`·
  `full_name`·`owner.{login,id,node_id}`를 돌려준다(2026-10-03 개발 호스트에서 익명 조회한 값이
  `tools/github/repository-identity.json`과 키 순서만 다르고 같았다).

공개 데이터를 읽으려고 운영 호스트에 GitHub 자격증명을 두면 필요 없는 비밀이 생기고, 그 비밀이
없다는 이유로 설치가 막힌다. 불변식:

1. 릴리스 인증의 네트워크 읽기(identity 조회, 정본 main fetch)는 어떤 자격증명도 쓰지 않고, `gh`가
   없는 호스트에서도 같은 판정을 낸다.
2. identity를 읽지 못하거나(도달 불가, 200이 아닌 응답) 읽은 값이 정본 정책과 다르면 지금과 똑같이
   설치를 거부한다. 읽기 방식이 바뀌어도 거부는 약해지지 않는다.

## 결정

### 1. identity는 공개 REST API에서 자격증명 없이 읽는다

`scripts/github/show-public-repository-identity.sh`는 `gh` 대신 `curl`로 고정 URL
`https://api.github.com/repos/perfectory-inc/perfectory-public` 하나를 읽는다.

- `--proto '=https'`·`--tlsv1.2`: HTTPS 밖으로 내려가지 않는다. `--max-redirs 0`: 다른 곳으로 따라가지 않는다.
- `--connect-timeout 10`·`--max-time 30`: 응답 없는 API가 설치를 무한정 붙잡지 않는다.
- `Accept: application/vnd.github+json`, `X-GitHub-Api-Version: 2022-11-28`.
- 자격증명 없음: `-q`로 `~/.curlrc`를 읽지 않고, `--netrc`·`--user`·`Authorization` 헤더를 쓰지 않는다.
- curl이 실패하면 "unreachable", HTTP 상태가 200이 아니면 "HTTP <상태>, not 200"을 대며 실패한다.

응답은 `github-policy-json.py repository-identity-from-rest`가 기존 후보 모양
(`hostname`·`full_name`·`repository_id`·`repository_node_id`·`owner`)으로 옮기고, 기존과 같은
`validate-repository-identity`와 `canonical`을 거친다. 출력 모양이 같으므로
`tools/github/README.md`의 identity 고정 절차(공개 저장소 생성 직후)도 그대로 쓴다 — 이제 그 절차도
로그인이 필요 없다.

### 2. `GH_HOST` 거부는 없앤다

`GH_HOST`는 `gh`가 읽는 변수였다. 새 스크립트는 `gh`를 부르지 않고 호스트는 URL에 고정돼 있어서,
`GH_HOST`가 무엇이든 조회 대상이 바뀌지 않는다. 의미 없는 검사를 남기면 "이 변수가 무언가를
바꿀 수 있다"는 틀린 신호가 되므로 지운다. 자기 시험은 거부 대신 `GH_HOST`를 심어도 같은 URL을 읽는지
확인한다. 다른 `gh api` 스크립트(`check-*.sh`, `configure-public-repository.sh`)의 `GH_HOST` 거부는
그대로다 — 거기서는 여전히 `gh`가 호스트를 고른다.

### 3. 인증 fetch 경로는 credential helper 없이 돈다

`safe-git-transport.sh`는 모든 호출에 `credential.helper=!gh auth git-credential`을 붙였다. 공개
HTTPS fetch에서는 서버가 인증을 요구하지 않으므로 보통 불리지 않지만, 401이 나면 `gh`를 찾고, 이는
"gh 없이 동작한다"를 보장하지 못한다.

`--anonymous` 선택을 더한다. 이 모드는 `credential.helper=`(목록 초기화)만 두고 helper를 더하지 않는다.
`GIT_TERMINAL_PROMPT=0`은 원래 모든 모드에 있다. 그래서 인증을 요구받으면 묻지도 않고 바로 실패한다.
`foundation-release-admission.py`의 `CanonicalSource`는 `init --bare`와 모든 Git 호출(fetch 포함)에
`--anonymous`를 쓴다. 옵션 없는 기본 모드는 그대로 `gh` helper를 쓴다 — `tools/github` 공개 게시
스크립트(`publish-public-root.sh` 등)는 push에 인증이 필요하다.

### 4. `gh`는 호스트 선행 조건이 아니다

ADR-0134 §5 표의 "root의 `gh` 인증" 행은 "root가 자격증명 없이 공개 identity를 읽는다
(`curl https://api.github.com/repos/<정본>`)"로 바뀐다. `prepare`의 거부 문구는
`root cannot read the public repository identity from https://api.github.com without credentials (ADR-0136)`이고
`gh`를 말하지 않는다. 런북 "릴리스 인증 전환" 1단계의 확인 명령에서 `gh` 두 줄을 지우고, root가
`curl`로 익명 조회해 정본 `full_name`을 받는지 보는 줄을 더한다.

### 5. 요청 한도

GitHub는 인증 없는 REST 요청을 IP마다 시간당 60회로 제한한다. 인증은 identity를 `prepare` 한 번에
한 번만 읽는다. `verify`·`verify-current`·`activate`·`rollback`·등록 작업 실행 전 검사는 root 소유 Git
cache만 보고 네트워크를 쓰지 않는다(ADR-0134 §2). 그래서 한도에 닿으려면 한 시간에 `prepare`를 60번
해야 한다. 같은 IP를 쓰는 다른 익명 요청이 한도를 먹었다면 응답은 403/429이고, 1항에 따라 "HTTP 403, not
200"으로 거부된다 — 조용히 통과하지 않는다. 기다렸다가 다시 `prepare`하면 된다.

## 재사용과 기각한 대안

- **root `gh` 로그인 유지(ADR-0134 원안)**: 기각. 운영 호스트에 `gh`가 없고, 공개 데이터를 읽으려고
  장기 자격증명을 운영 호스트에 두는 것은 얻는 것 없이 비밀만 늘린다.
- **GraphQL API**: 기각. 인증 없이는 쓸 수 없다.
- **`git ls-remote`만으로 대조**: 기각. 저장소 이름만 보이고 불변 id가 없어 이름 바꾸기·이전·삭제 후 재사용을
  구분하지 못한다. 그것이 identity 대조의 목적이다(ADR-0007).
- **API 주소를 환경변수로 받기(시험용)**: 기각. 바꿀 수 있는 주소는 identity 조회를 다른 곳으로 돌리는
  통로다. 시험은 `PATH`의 가짜 `curl`로 하고, 운영 인증은 `PATH=/usr/bin:/bin`만 넘기므로 가짜가 끼어들 수
  없다.
- **python `urllib`**: 가능하지만 기각. `curl`의 `--proto`·`--max-redirs`·`-q`가 "HTTPS만, 따라가지 않음,
  설정 파일 없음"을 한 줄씩 드러내고, 자기 시험이 인자를 그대로 검사할 수 있다.
- 인증 fetch 전체를 새 스크립트로 만들지 않는다. 기존 `safe-git-transport.sh`의 격리(설정·프록시·URL
  재작성 차단)를 그대로 쓰고 helper만 뺀다.

## 검증

- `scripts/guard/repository-identity-capture-self-test.sh`(가짜 `curl`, 실패하는 가짜 `gh`): 고정 URL·
  `=https`·`Accept` 헤더·`--max-time` 사용, 자격증명 인자 없음, `gh` 미호출, `GH_HOST`가 대상을 바꾸지
  않음, 잘못된 소유자·404·도달 불가의 거부와 그 이유 문구.
- `scripts/guard/safe-git-transport-self-test.sh`: 기록하는 가짜 `gh`를 `PATH`에 두고 `git credential fill`을
  돌려, 기본 모드는 `gh auth git-credential`에 닿고 `--anonymous`는 어떤 helper도 부르지 않음을 확인한다.
- `platforms/foundation-platform/tests/release_admission`: `gh`가 없는 `PATH`에서 실제 identity 스크립트로
  `refresh`가 통과하고 릴리스가 설치·검증됨, API가 준 저장소 id·소유자 id가 다르면 거부, 404·403·도달
  불가는 identity 읽기를 말하며(그리고 `gh`를 말하지 않고) 거부, 인증의 Git 호출이 `--anonymous`이고 그
  모드에 helper가 없음.
- 각 검사는 위반을 심어 거부되는 것을 확인했다(PR 본문에 변이별 결과).
- 운영 호스트에서 root 익명 조회가 되는지는 런북 1단계 확인 명령이 판정한다. 이 ADR의 합성 검증은 그
  관측을 대신하지 않는다.

## 1차 자료

- [GitHub REST — Get a repository](https://docs.github.com/en/rest/repos/repos#get-a-repository)
- [GitHub REST — Rate limits for the REST API](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)
- [GitHub REST — API versions](https://docs.github.com/en/rest/about-the-rest-api/api-versions)
- [git-credential — credential.helper](https://git-scm.com/docs/gitcredentials)
- [curl — `--proto`, `--max-redirs`, `-q`](https://curl.se/docs/manpage.html)
