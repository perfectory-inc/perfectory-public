# ADR 0158: npm 오버라이드는 정본 하나에서 생성하고, 새 권고는 준비된 변경으로 도착한다

- Status: Accepted
- Date: 2026-10-07
- Amends: [ADR-0008](./0008-manual-dependency-updates-and-organization-branches.md) 운영 규칙 "critical 보안 공지는
  수동으로 즉시 PR 을 만든다" — 이 ADR 이 ADR-0008 이 요구한 "자동화를 다시 도입할 때의 도구·권한·PR 생성 규칙"이다.

## Context

2026-10-05–06 이틀 동안 npm 권고 4건(katex `GHSA-238p-pmpm-9mq7`, smol-toml `GHSA-r4xh-jqrq-34v2`,
source-map-js `GHSA-68fv-2mgg-jv7q`, sharp `GHSA-rgj7-g3m4-5g8c`)이 OSV 래칫
(`scripts/ci/osv-vulnerability-gate.sh`)을 통해 **모든** PR 과 머지 그룹을 막았다. 매번 사람이 최대 7개 트리의
`package.json` `pnpm.overrides` 를 손으로 고치고 lockfile 7개를 engines 핀(node 24.20.0 / pnpm 9.12.0)으로
다시 만들었다. 이 ADR 을 쓰는 중에 다섯 번째(sharp `GHSA-wq5f-xc86-pv6w`, 2026-10-06 13:43Z 공개)가 같은 경로로
main 을 빨갛게 만들었고, #354 가 다시 손으로 manifest 6개와 lock 6개를 고쳤다.

실측(2026-10-07, origin/main `9c6065fb`):

| 사실 | 값 |
|---|---|
| 추적 `pnpm-lock.yaml` 트리 | 7 (dawneer web, foundation 게이트웨이 5, gongzzang) |
| 트리마다 다른 override 집합 | dawneer 1개, profile 게이트웨이 5개, 나머지 게이트웨이 6개, gongzzang 13개 |
| 같은 패키지, 트리마다 다른 실제 해석 | ws 8.21.0 / 8.21.3, esbuild 0.28.1 / 0.28.2, postcss 8.5.15 / 8.5.28, undici 7.30.0 / 7.29.1·6.28.1 |
| 정확판 핀이 낳은 부작용 | gongzzang 의 `brace-expansion: 5.0.12` 가 `^2.0.1` 을 선언한 minimatch@5 까지 5.x 로 끌어올림 |

근본 원인은 같은 사실(어떤 패키지를 어디까지 올려야 하는가)이 7곳에 손으로 적혀 있다는 것이다. 목록이 이미
갈라져 있고, 정확판 핀(`"ws": "8.21.0"`)을 한 목록으로 합치면 더 새 판을 쓰는 트리를 **내린다**.

pnpm 의 override 선택자는 해석된 판이 아니라 **선언된 범위**에 대해 맞춰진다(선언 범위가 선택자 범위의 부분집합일
때 적용, pnpm `createVersionsOverrider` 의 `semver.subset`). 그래서 `ws@<8.21.0` 같은 "취약 구간" 선택자는
`^8.18.0` 을 선언한 의존을 잡지 못한다 — `pnpm audit --fix` 가 쓰는 형태가 실제로는 자주 빗나가는 이유다.

## Decision

1. **정본은 `tools/npm/security-overrides.contract.json` 하나다.** 항목마다 `kind`(`security` |
   `technology-pin`), `package`, `version`, `advisories`(GHSA/CVE, 정렬·유일), `reason`, `date`, 선택적 `from`·`trees`.
   보안이 아닌 override 도 같은 목록에 `kind` 로 들어간다(현재 vite 하나, 판은
   `tools/technology-versions.contract.json` 의 `manifest_exact_pins` 에서 `version_from` 으로 읽는다 — 다시 적지 않는다).
2. **보안 항목은 핀이 아니라 바닥(floor)이다.** 렌더 결과는 `"<pkg>@>=<from> <<line end>>": "^<version>"` 이다.
   선택자는 `version` 이 속한 릴리스 라인 전체(메이저, 0.x 면 마이너)를 덮어 선언 범위를 잡고, 값은 캐럿이라
   이미 바닥 위에 있는 트리는 내려가지 않으며 다른 메이저는 건드리지 않는다. 더 오래된 라인까지 끌어올려야 할 때만
   `from` 을 적는다(esbuild·sharp `0.0.0`, katex `0.11.0` — 권고의 introduced 와 선언 범위가 근거).
3. **트리 목록은 아무도 적지 않는다.** `git ls-files '*pnpm-lock.yaml'` 의 디렉터리가 트리다. 항목은 그 트리의 lock 이
   해당 패키지를 항목 범위 안에서 해석할 때만 들어간다(`trees` 로 좁힐 수 있다). 끌어올린 판도 범위 안이라 렌더는
   lock 갱신 전후로 같다.
4. **렌더와 검사.** `node tools/npm/security-overrides.mjs render` 가 모든 트리의 `pnpm.overrides` 를 쓰고,
   `check` 는 (a) `package.json` 이 렌더 결과와 바이트 단위로 같은지, (b) lock 의 `overrides:` 블록이 같은지,
   (c) lock 이 바닥 아래 판이나 기술 핀과 다른 판을 해석하지 않는지를 본다. 가드
   `scripts/guard/npm-security-overrides.sh` 와 자기시험 `npm-security-overrides-self-test.sh`(34개 node:test,
   검사 하나씩 위반을 심어 거부 확인)가 `monorepo-guard.sh` 사슬에 들어가 CI(`required/repository`)와 pre-push 에서 돈다.
5. **lock 재생성은 한 명령, 한 툴체인이다.** `bash tools/npm/refresh-locks.sh` 가 트리마다
   `node:<node 핀>-bookworm@<digest>` 컨테이너 안에서 `corepack pnpm@<pnpm 핀> install --lockfile-only
   --ignore-scripts` 를 돌리고 끝에 `check` 를 돌린다. 핀은 기술 계약에서 읽고, 컨테이너 참조는 그 계약의 검사된 투영인
   `tools/container-images.env` 의 `NODE_VERIFY_IMAGE`(컨테이너 런타임 정책이 읽을 수 있는 형태)를 쓰되, 계약의 node 핀으로
   만든 참조와 다르면 거부한다(핀만 오르고 이미지가 남은 경우). Linux 와 Git Bash(Windows)
   에서 같은 바이트가 나온다(2026-10-07 로컬 실행 결과와 아래 7. 의 자동 준비 결과가 lock 7개·manifest 7개 모두 일치).
6. **OSV 래칫은 저장소 컨텍스트로 옮긴다.** 래칫은 모든 영역의 lockfile 을 보는 저장소 검사인데 `required/foundation`
   안에 있었다. `monorepo-guard.yml` 의 `required/repository` 로 옮기고(foundation-ci 에서는 제거, 중복 실행 없음),
   같은 워크플로우에 **매일 21:17 UTC 예약 실행**을 더한다. 새 권고는 커밋 없이 main 에 도착하므로, 다음 PR 보다
   예약 실행이 먼저 찾는다.
7. **새 권고는 준비된 변경으로 도착한다.** 그 잡이 실패하면 `Prepare npm advisory fix`(`if: failure()`,
   `check-workflow-policy.sh` 허용 목록에 추가)가 `tools/npm/prepare-advisory-fix.sh` 를 돌린다: OSV 보고서에서 기준선에
   없는 npm 권고의 첫 수정판을 계산해 계약의 바닥을 올리고(`propose`; 수정판이 없는 권고는 올리지 않고 "override 로 못
   고침"으로 보고), 렌더, lock 재생성, 검사, OSV 재실행까지 한 뒤 patch 와 요약을 `repository-supply-chain`
   아티팩트로 올린다. 유지보수자는 `bash tools/npm/open-advisory-fix-pr.sh <run-id>` 한 줄로 main 기준 이름 있는
   브랜치에 적용·커밋·푸시·PR 생성까지 한다(run id 없이 부르면 로컬에서 스캔부터 한다).
8. **워크플로우는 쓰기 토큰을 갖지 않는다.** 푸시와 PR 은 유지보수자 자신의 자격으로 한다. 아래 대안 평가의 결론이다.

### 자동 PR 경로 평가 (증거와 보안 절충)

| 대안 | 필요한 것 | 판정 |
|---|---|---|
| A. Dependabot 보안 업데이트(그룹 PR) | Dependabot 알림 + 방화벽 우회 + ADR-0008 폐기 | **기각.** 우리 계약을 모르고 lock 만 고친다 → 4.의 검사가 그 PR 을 거부한다. 정본이 둘이 된다. ADR-0008 이 이미 잡음 때문에 껐다. |
| B. `GITHUB_TOKEN` 으로 브랜치·PR 생성 | 워크플로우 쓰기 권한, `can_approve_pull_request_reviews: true`, 방화벽 접두사 예외 | **기각.** `check-workflow-policy.sh` 가 공개 워크플로우의 쓰기 권한을 금지하고, 그 저장소 설정은 PR **승인**까지 같이 연다. 게다가 `GITHUB_TOKEN` 이 만든 PR 은 `pull_request` 워크플로우를 트리거하지 않아(GitHub 문서 "Triggering a workflow from a workflow") 필수 검사가 영영 안 뜬다. |
| C. GitHub App 설치 토큰(`actions/create-github-app-token`, contents·pull-requests 쓰기, 이 저장소만) | App 생성·설치, 비밀 키 1개, 정책 가드 예외 1개, 방화벽 우회 행위자 1개 | **다음 단계로 권장, 지금은 구현하지 않음.** 업계 표준(GitHub 문서가 워크플로우발 PR 트리거용으로 권장)이고 App 이 만든 PR 은 CI 를 트리거한다. 절충: 장수명 비밀 키가 생기고, 공개 워크플로우에서 `secrets` 참조를 금지한 규칙과 "자동 PR 금지"(ADR-0008)를 둘 다 고쳐야 한다. |
| D. 알림만(Slack 웹훅) | 비밀 1개 | **기각.** 비밀이 필요한데 얻는 것은 7.보다 적다(실패한 예약 실행은 GitHub 가 이미 알린다). |
| E. 이 ADR(준비된 patch + 한 줄 PR) | 없음 | **채택.** 새 비밀·권한·방화벽 예외 0. 사람 손에 남는 것은 run id 하나 붙여 넣는 일과 PR 검토뿐이다. |

C 로 옮길 때 소유자가 할 일(코드는 별도 PR — 정책 가드가 지금은 이를 거부한다):

1. Organization settings → Developer settings → GitHub Apps → New GitHub App: webhook 끔, Repository permissions 는
   Contents **Read and write**, Pull requests **Read and write**, Metadata Read-only 만. "Only on this account".
2. Install App → Only select repositories → 이 저장소 하나.
3. 저장소 Settings → Secrets and variables → Actions: 변수 `NPM_ADVISORY_APP_ID`, 비밀 `NPM_ADVISORY_APP_PRIVATE_KEY`.
   비밀은 `npm-advisory` Environment 에 두고 required reviewer 없이 `main` 브랜치에만 열면 PR 워크플로우에서는 읽히지 않는다.
4. 규칙 집합 `tools/github/non-main-branch-firewall.json` 의 `bypass_actors` 에 그 App(Integration) 을 넣거나, 더 좁게는
   방화벽에서 `refs/heads/automation/npm-advisory/**` 를 제외하고 그 접두사만 App 이 쓸 수 있는 별도 규칙 집합을 둔다
   (후자를 권장: App 이 다른 브랜치를 못 만든다).
5. 후속 PR 이 `check-workflow-policy.sh` 에 그 워크플로우 하나만의 예외(예약 실행 전용, required 컨텍스트 아님, 비밀 1개)를
   넣고, 그 워크플로우가 7.의 아티팩트를 App 토큰으로 푸시·PR 한다.

### 무관한 PR 을 막지 않기 (평가, 미구현)

권고는 코드 변경 없이 도착하므로, 지금은 그 트리를 건드리지도 않은 PR 까지 막힌다. 검토한 안:

- **PR 이 건드린 트리만 판정** — 머지 그룹도 그렇게 하면 main 의 문이 약해지고, 머지 그룹만 전체로 두면 PR 은
  통과해도 큐에서 막혀 얻는 것이 없다. 기각.
- **차등 판정(권장)**: `pull_request`·`merge_group` 은 "기준 커밋보다 발견이 늘었는가"(head 보고서 − base 보고서)로
  판정하고, `push`(main)와 예약 실행은 지금처럼 기준선 전체로 판정한다. GitHub `dependency-review-action` 과 같은
  모델이다. 변경이 들여오는 취약점은 여전히 PR·큐에서 막히고, 세상이 바꾼 것은 main 의 빨간 불과 7.의 준비된 변경이
  맡는다. 다만 기준선 갱신 규칙(새 기준선 행은 사람이)과 base 보고서의 출처(같은 잡에서 base 를 한 번 더 스캔)를
  정하는 별도 ADR 이 먼저다. 7.로 막힘 시간이 "사람이 고칠 때까지"에서 "한 줄 명령까지"로 줄었으므로 그 다음에 판단한다.

## Consequences

- 7개 manifest 가 계약 렌더로 바뀌었고 lock 7개를 핀 툴체인으로 다시 만들었다. 해석이 바뀐 것은 하나뿐이다:
  gongzzang minimatch@5 의 brace-expansion 이 선언 라인(2.1.7, dawneer 와 같은 판)으로 돌아왔다. OSV 래칫은
  `unique=11, warnings=16` 으로 초록.
- 실증: #354 이전의 main 에서 `GHSA-wq5f-xc86-pv6w` 로 빨개진 보고서를 `prepare-advisory-fix.sh` 에 넣자 47초 만에
  sharp 바닥 0.35.5 와 lock 7개를 만들었고 OSV 재실행이 초록이었다. 그 결과를 #354 위에 다시 렌더·재생성한 트리와
  비교하면 manifest·lock 이 바이트 단위로 같고, #354 의 lock 과는 해석된 판이 전부 같다(위 brace-expansion 하나 제외) —
  사람이 12개 파일로 한 일을 계약 한 줄이 같은 결과로 한다.
- 이제 override 를 손으로 고치면 `npm-security-overrides` 가드가 거부한다. 새 트리(lock 을 추적하기 시작한 디렉터리)는
  자동으로 대상이 되고, 렌더 전에는 가드가 거부한다.
- `required/repository` 잡의 제한 시간을 20분으로 늘렸다(실패 시 lock 재생성이 컨테이너 이미지를 내려받는다).
  예약 실행은 하루 한 번 저장소 가드 전체를 돈다.
- 남은 일: C 경로(소유자 결정), 차등 판정 ADR, Cargo 권고의 같은 처리(지금은 보고서만 남는다).
