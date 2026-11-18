# ADR 0572: pnpm 12 로 올리고, 트리마다 pnpm 설정은 pnpm-workspace.yaml 에 있으며 override 계약은 그곳에 렌더된다

- Status: Accepted
- Date: 2026-11-18
- Amends: [ADR-0158](./0158-npm-overrides-have-one-source-and-a-new-advisory-arrives-as-a-prepared-change.md) 1·4항(렌더 대상 `package.json` `pnpm.overrides` → `pnpm-workspace.yaml` `overrides`)
- Builds on: [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md)(최신을 따르고 핀은 계약 하나; 툴체인은 파도로),
  [ADR-0414](./0414-the-osv-allow-list-is-osv-scanner-s-own-config-with-an-expiry.md)(OSV 게이트, 2026-11-18 개정 각주의 스캐너 2.7.1)

## Context

2026-11-18 Renovate 가 처음 연 보안 PR 중 하나가 #1431(pnpm 9.12.0 → 10, 권고 13건)이었다. 툴체인은 파도로 옮긴다(ADR-0097
결정 1). 소유자의 상시 규칙은 최신이고, 그날 npm 의 `latest` 는 12.21.0 이었다(`latest-10` 10.34.9, `latest-11` 11.28.12).
pnpm 자신의 v10 → v11 이행 문서와 v12 lockfile 문서에서 우리에게 닿는 것을 골랐다.

| 바뀐 것 (pnpm) | 우리에게 | 이 ADR |
|---|---|---|
| v11: `package.json#pnpm` 을 읽지 않는다 | ADR-0158 의 렌더러가 트리 여섯의 `package.json` `pnpm.overrides` 에 보안 바닥을 쓴다. 그대로 두면 **바닥이 소리 없이 사라진다**. gongzzang 의 `pnpm.onlyBuiltDependencies` 도 같다 | 1·2 |
| v11: `onlyBuiltDependencies` 등 → `allowBuilds` 하나, 목록에 없는 빌드 스크립트는 설치 실패(`ERR_PNPM_IGNORED_BUILDS`) | pnpm 9 는 모든 빌드 스크립트를 돌렸다. 실측: 게이트웨이 넷은 `esbuild`·`workerd`, gongzzang 은 기존 여섯, dawneer web 은 없음 | 3 |
| v11: `.npmrc` 는 인증·레지스트리만 | gongzzang `.npmrc` 다섯 설정 | 3 |
| v11: `npm_config_*` 환경 변수를 읽지 않는다(`pnpm_config_*`) | `scripts/verify/cargo-verify.sh` 의 오프라인·저장소 설정 | 4 |
| v11: Docker 는 `pnpm-workspace.yaml` 을 복사해야 한다 | dawneer Dockerfile, 검증 이미지 | 4 |
| v12: `packageManager` 를 적은 트리의 lockfile 은 두 YAML 문서(부트 문서 + 프로젝트) | OSV-Scanner 2.5.0 은 첫 문서만 읽었다 — ADR-0414 개정 각주(2.7.1, #1445)가 먼저 들어가야 한다 | 5 |
| v11: `minimumReleaseAge` 기본 1440분, 비엄격 | 맞는 판이 없으면 더 새 판으로 물러난다. 보안 바닥은 그대로 걸린다 | — |

Node 24 는 지원된다. 카탈로그·`node-linker`·workspace 프로토콜은 쓰지 않는다.

## Decision

1. **pnpm 은 12.21.0 이다.** 핀은 `tools/technology-versions.contract.json` 의 `manifest_exact_pins.pnpm`·`packageManager`
   그대로 한 곳이고, 트리 여섯의 `packageManager`·`engines.pnpm`, 워크플로우 넷의 `pnpm/action-setup` `version`,
   검증 이미지, 문서가 그 사본이다(기술 버전 가드).
2. **override 계약은 트리의 `pnpm-workspace.yaml` `overrides` 에 렌더된다.** `tools/npm/security-overrides.mjs render` 는
   그 파일의 최상위 `overrides:` 만 쓰고 나머지 줄은 그대로 둔다(없으면 만든다). 키와 값은 JSON 문자열로 쓴다(YAML 이 그대로
   읽는다). `check` 는 (a) 파일이 렌더 결과와 같은지, (b) lockfile 의 `overrides:` 가 같은지, (c) 바닥·핀, 그리고
   (d) **`package.json` 에 `pnpm` 필드가 있으면 거부**한다 — pnpm 11 이후 읽히지 않는 설정은 있는 것처럼 보이는 것이 사고다.
   advisory-fix 울타리(`cargo xtask npm`)는 `pnpm-workspace.yaml` 을 허용 파일에 더한다.
3. **설치 스크립트는 `allowBuilds` 로 고른다.** 트리마다 그 파일에 적는다: 게이트웨이 넷 `esbuild`·`workerd`(wrangler 의 번들러와
   로컬 런타임), gongzzang 은 옮겨 온 여섯. 다른 의존성이 빌드 스크립트를 들고 오면 설치가 실패한다 — pnpm 의 기본값이고 끄지
   않는다. gongzzang `.npmrc` 의 다섯 설정은 같은 파일의 camelCase 키로 옮기고 `.npmrc` 는 지운다.
4. **설정을 읽는 길을 따라 옮긴다.** dawneer Dockerfile 과 검증 이미지는 `pnpm-workspace.yaml` 을 함께 복사한다.
   `cargo-verify.sh` 는 `pnpm_config_store_dir`·`pnpm_config_offline` 을 넘긴다.
5. **두 문서 lockfile 을 받아들인다.** `pmOnFail: ignore` 로 한 문서를 유지하지 않는다 — pnpm 버전 검사를 끄는 대가다.
   OSV 게이트는 2.7.1 과 그 자기 시험(ADR-0414 개정 각주)이 맡는다. 그래서 이 변경은 #1445 뒤에만 병합한다.
   우리 lock 읽기(`security-overrides.mjs readLockfile`, OSV 판정기)는 `---` 를 절의 끝으로 읽어 두 문서를 그대로 다룬다.
6. Renovate 는 `pnpm-workspace.yaml` 을 고치지 않는다(`matchFileNames`). 고침은 계약을 옮긴다.

실패가 막는 사고: pnpm 11 이후 `package.json#pnpm` 의 보안 바닥이 읽히지 않은 채 lockfile 이 바닥 아래 판으로 풀리는 일
(2026-10-05~06 권고 넷, ADR-0158 이 막은 그 일이 소리 없이 다시 열린다).

## 확인 (2026-11-18)

| 무엇 | 결과 |
|---|---|
| `cargo xtask npm refresh-locks`(node 24.20.0 고정 이미지, pnpm 12.21.0) | 트리 여섯, `OK npm-security-overrides (trees=6, entries=22)` |
| 프로젝트 문서의 패키지 집합, 전 → 후 | dawneer 167=167, 게이트웨이 160·160·160·162 같음, gongzzang 755=755. 바뀐 것은 부트 문서뿐 |
| `pnpm install --frozen-lockfile`, 트리 여섯 | 모두 rc=0 (게이트웨이는 `allowBuilds` 전 `ERR_PNPM_IGNORED_BUILDS: esbuild, workerd`) |
| gongzzang web `next build`, dawneer web `build`, 타일 게이트웨이 `typecheck`·`test` | 모두 rc=0 |
| `node --test tools/npm/security-overrides.test.mjs` | 35/35 (워크스페이스 렌더·되읽기·멱등, `package.json#pnpm` 거부 포함) |

## Consequences

- 보안 바닥은 그대로 걸린다. 위치만 바뀌었고 가드가 옛 위치를 거부한다.
- 새 의존성이 빌드 스크립트를 들고 오면 PR 의 설치가 실패하고, 사람이 그 트리의 `allowBuilds` 에 `true`/`false` 를 적는다.
- lockfile 마다 부트 문서(pnpm 과 플랫폼 바이너리 약 16 항목)가 붙는다.
- 남은 일: 없음. 다음 pnpm 메이저도 같은 파도로 옮긴다.

## Rejected alternatives

| 대안 | 판정 |
|---|---|
| pnpm 10.34.9 에 멈춘다 | 기각. 소유자 규칙은 최신이다. 10 도 `package.json#pnpm` 은 읽지만 v11 의 같은 이행을 한 번 더 미룰 뿐이다. |
| `pmOnFail: ignore` 로 한 문서 lockfile | 기각. pnpm 버전 검사를 끈다. 스캐너를 올리는 쪽이 맞다. |
| `strictDepBuilds: false` | 기각. 검토하지 않은 빌드 스크립트를 경고로만 넘긴다. pnpm 의 기본값(실패)을 쓴다. |
| pnpm 의 codemod(`pnpx codemod run pnpm-v10-to-v11`) | 쓰지 않음. 렌더러가 정본이라 결과를 렌더러가 써야 하고, 손댈 곳은 위 표가 전부다. |
