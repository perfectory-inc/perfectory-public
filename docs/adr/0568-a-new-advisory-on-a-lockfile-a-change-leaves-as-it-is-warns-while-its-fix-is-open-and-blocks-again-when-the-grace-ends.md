# ADR 0568: 바꾸지 않은 lockfile 의 새 권고는 고치는 PR 이 열려 있는 동안 경고이고, 유예가 끝나면 다시 막는다

- Status: Superseded by ADR-0414 (Revision 2026-11-25: PR·머지 그룹은 OSV-Scanner 자신의 PR 모드로, 그 변경이 들여온 권고에서만 실패한다)
- Date: 2026-11-18
- Amends: [ADR-0271](./0271-dependency-updates-arrive-as-renovate-pull-requests-and-main-keeps-one-advisory-issue.md) 4항(대시보드),
  [ADR-0414](./0414-the-osv-allow-list-is-osv-scanner-s-own-config-with-an-expiry.md) 1항(목록에 없는 권고는 게이트를 실패시킨다 — PR·머지 그룹에서는 아래 규칙으로 판정),
  [ADR-0158](./0158-npm-overrides-have-one-source-and-a-new-advisory-arrives-as-a-prepared-change.md) "무관한 PR 을 막지 않기 (평가, 미구현)" — 그 "차등 판정" 을 정하는 별도 ADR 이 이것이다
- Builds on: [ADR-0271](./0271-dependency-updates-arrive-as-renovate-pull-requests-and-main-keeps-one-advisory-issue.md)(Renovate 가 보안 갱신 PR 을 연다, main 은 매일 검사),
  [ADR-0554](./0554-main-says-when-it-is-red-from-the-runs-github-already-made.md)(main 이 깨지면 이슈 하나)

## Context

권고는 커밋 없이 도착한다. main 이 이미 잠근 패키지에 새 권고가 붙으면, 그 lockfile 을 건드리지 않은 PR 까지
`required/repository`(OSV 래칫)와 각 영역의 `cargo-deny` 에서 실패한다. 엿새 사이 네 번이다(`gh pr view`, 2026-11-18).

| 도착 | 권고 | 고친 PR | 막힌 것 |
|---|---|---|---|
| 11-13 | basic-ftp (npm) 둘 | #1375 (00:29Z 열림 → 01:18Z 병합) | 그 사이 모든 PR |
| 11-13 | quinn-udp RUSTSEC-2026-0538, lockfile 넷 | #1379 (13:15Z → 14:58Z) | 그 사이 모든 PR |
| 11-16 | next GHSA-mfr8-8qww-v76h, 다음 날 GHSA-vc7v-qv4w-wrv8 | #1418 | `required/repository` 를 도는 모든 PR |
| 11-17 | picomatch GHSA-fc5q-39xc-vjpf, chrono RUSTSEC-2026-0543, libz-sys RUSTSEC-2026-0544 | #1418 | 위에 더해 23:57Z 부터 Rust 영역의 `cargo-deny`. lockfile 열둘 중 열에서 15건, W81c·W83c·W84 의 PR 이 멈춤 |

막는다고 고침이 빨라지지 않는다. 고치는 사람은 막힌 PR 의 작성자가 아니고, 막힌 PR 은 그 권고와 무관하다.
반대로 아무도 막지 않으면 권고는 잊힌다. 2026-10-16 zip(ADR-0271)이 그랬다.

권고의 날짜로 유예를 잴 수는 없다. 같은 보고서에서 chrono RUSTSEC-2026-0543 은 `published` 가 2026-09-14 인데
OSV 에 나타난 것은 2026-11-17(`modified`)이고, picomatch 는 `published` 2026-10-19·`github_reviewed_at`
2026-11-17 이다. 우리에게 닿은 때는 우리 쪽 기록으로만 안다.

같은 시각 Renovate(ADR-0271)는 한 번도 PR 을 열지 않았다. 앱은 2026-10-22 부터 조직에 설치되어 있지만 살아 있는
비-main 방화벽 규칙 집합(19595435)의 우회 행위자에 Renovate(Integration 2740)가 없다 — 정본
`tools/github/non-main-branch-firewall.json` 에는 있다. `has_issues` 도 정본은 true, 실물은 false 이고, 그래서
`dependency-advisories` 의 이슈 잡도 매일 실패한다. `scripts/github/configure-public-repository.sh verify` 가 이
어긋남을 보고한다. 적용은 권한 변경이라 소유자의 결정이다(아래 "소유자가 할 일").

## Decision

1. **PR 과 머지 그룹은 변경이 한 일로 판정한다.** `cargo xtask ci osv-gate` 는 스캐너가 허용 목록 밖의 발견을 보고하고
   `CHANGE_SCOPE_EVENT` 가 `pull_request`·`merge_group` 이면 판정을 `scripts/ci/osv_change_verdict.py` 에 맡긴다.
   발견은 스캐너 보고서의 권고 묶음(`groups[]`: 같은 취약점의 ID·별칭과 `max_severity`) 하나 × lockfile 하나다.

   | lockfile (`git diff -z --name-only <base> HEAD`) | 고치는 PR | 판정 |
   |---|---|---|
   | 변경이 건드림, 또는 경로·base 를 읽지 못함 | 무엇이든 | 실패 |
   | 건드리지 않음 | 열린 것이 없음 | 실패 |
   | 건드리지 않음 | 열린 것이 있고, **첫** 고침이 열린 지 유예 미만 | 경고 `advisory-grace` |
   | 건드리지 않음 | 열린 것이 있고, **첫** 고침이 열린 지 유예 이상 | 실패 |

   - **고치는 PR** 은 `security` 라벨을 달고, 제목이나 본문에 그 발견의 ID 나 별칭(GHSA·CVE·RUSTSEC, 대소문자 무시,
     더 긴 ID 의 일부는 아님) 하나를 적고, 그 발견의 lockfile 이나 허용 목록 `osv-scanner.toml` 을 **바꾸는** PR 이다
     (초안 포함). 본문에 ID 만 적고 둘 다 바꾸지 않는 PR 은 고침이 아니다. ID 는 발견마다 하나씩 그대로 적는다 —
     `RUSTSEC-2026-0574..0582` 같은 범위는 첫 ID 하나만 맞는다(2026-11-19 heapless 아홉, #1505).
   - **시계는 첫 고침이 열린 시각부터** 돈다. 라벨·ID 가 나중에 붙어도 PR 의 생성 시각이다. 닫히거나 병합된 고침도
     "첫" 을 정하는 데 센다(`gh pr list --state all`) — 닫고 새로 열어 유예를 늘릴 수 없다. 유예가 적용되려면 고침
     하나는 열려 있어야 한다.
   - 읽지 못한 것은 실패 쪽으로 떨어진다: 스캐너가 발견을 보고했는데 하나도 읽지 못한 보고서, base, lockfile 경로,
     PR 목록(읽지 못하면 `::warning` 으로 알리고 고침 없음으로 본다), 라벨 PR 500개 이상.
   - PR·머지 그룹의 실패 주석 제목은 `advisory-change` 다. `advisory` 제목은 main 의 실행만 낸다(ADR-0278 의 호스트가
     main 의 매일 실행에서 읽는다).
2. **유예는 심각도별이고 계약 하나에 있다.** `tools/osv-gate.contract.json`: `max_severity`(CVSS 기본 점수)가
   9.0 이상이면 24시간, 7.0 이상이면 72시간, 그 밖은 168시간, 점수가 없거나 읽지 못하면(관리 중단·unsound 같은
   정보성 권고, 악성 패키지, CVSS 4 벡터) 24시간 — 모르는 심각도는 가장 짧다. 168시간을 넘는 값, 빠진 등급,
   critical 보다 긴 unscored, 뒤집힌 문턱은 판정기가 거부한다. 숫자는 W83 의 제안(critical 24h, high 72h, 그 밖 7d)이다.
3. **main 의 판정은 그대로다.** push 와 매일 21:17Z 예약 실행은 지금처럼 모든 발견으로 판정한다. main 은 권고가 닿은
   순간부터 빨갛다. 유예는 무관한 PR 이 지나가게 할 뿐 main 을 초록으로 칠하지 않는다.
4. **유예가 끝나면 지금처럼 모든 PR 이 막힌다.** 유예 안에 고침이 병합되지 않으면 그 권고는 다시 그 lockfile 을 건드리지
   않은 PR 까지 실패시킨다. 마감이 게이트다. 빠져나가는 길은 둘이다: 고침을 병합하거나, 수정판이 없으면 ADR-0414 의
   만료 있는 허용 항목을 넣는다(그 PR 도 고침이다 — `osv-scanner.toml` 을 바꾼다). 유예가 끝난 뒤 이미 초록인 PR 은
   머지 그룹에서 다시 판정되어 막힌다. main 규칙 집합은 병합 큐를 요구하고 우회 행위자가 없다(2026-11-18 `gh api`).
5. **`cargo-deny` 도 같은 경계를 쓴다.** 다섯 영역 워크플로우의 `cargo-deny` 단계는
   `cargo xtask ci cargo-deny <workspace> --locked` 다(foundation·gongzzang 에도 `--locked` 를 더했다 — 해석이 lockfile
   과 달라지면 cargo-deny 가 거부하므로 판정하는 그래프는 lockfile 그대로다). PR·머지 그룹이 그 워크스페이스의
   `Cargo.lock` 도 `deny.toml` 도 건드리지 않았으면 `bans licenses sources` 만 본다. 그 lockfile 의 RustSec 권고
   (취약점·관리 중단·unsound)는 위 OSV 판정이 같은 규칙으로 본다 — OSV 는 RustSec 권고 DB 를 정보성 권고까지 그대로
   싣는다(2026-11-18 보고서의 chrono `informational: unmaintained`). yank 는 ADR-0271 의 매일 검사가 main 에서 본다.
   건드렸으면, 또는 base 를 모르면 지금처럼 전부 본다.
6. **고치는 PR 은 `security` 라벨을 단다.** Renovate 의 보안 갱신은 `.github/renovate.json` 의
   `vulnerabilityAlerts.labels` 로 이미 그 라벨을 단다(ADR-0271). 사람이 여는 고침도 같은 라벨과 권고 ID 를 적는다.
   라벨은 2026-11-18 저장소에 만들었다.
7. **Renovate 의 보안 PR 은 동시에 셋까지다.** `.github/renovate.json` 의 `vulnerabilityAlerts.prConcurrentLimit: 3`.
   Renovate 는 보안 PR 에 일정·`prHourlyLimit` 을 적용하지 않고, `vulnerabilityAlerts` 안의 동시 상한만 따른다(기본 0 =
   무제한, Renovate 문서 `vulnerabilityAlerts`). 2026-11-18 방화벽을 고친 직후 14:04Z 에 #1430(next)·#1431(pnpm)이 함께
   열려 큐 앞에 실행 44개를 세웠다. 묶지는 않는다(`groupName` 없음): 한 패키지의 고침이 다른 패키지의 고침을 붙잡지 않게.
   Renovate 는 `vulnerabilityAlerts` 의 키(`enabled`·`draftPR`·`labels`)를 자기 규칙에 강제로 덮으므로, `packageRules` 의
   툴체인 끔(node·pnpm·rust·packageManager, ADR-0097)은 보안 PR 을 막지 못한다 — #1431(pnpm 9.12.0 → 10, 권고 13건)이 그렇게
   열렸다. `vulnerabilityAlerts.draftPR: false` 를 빼면 툴체인 규칙의 초안이 서지만, 같은 이유로 major 규칙(`draftPR: true`)도
   모든 보안 major 를 초안으로 만들어 ADR-0302(보안은 major 도 CI 초록이면 병합)를 깬다. 그래서 Renovate 쪽에서는 고르지
   않는다. 툴체인 보안 PR 은 코디네이터의 큐 투입 규칙이 잡아 두고(`TOOLCHAIN-HOLD`: node·pnpm·packageManager 핀을 바꾸는
   `renovate/*` PR 은 넣지 않는다) 툴체인 파도(ADR-0097)로 다룬다.
8. **Renovate 의존성 대시보드를 켠다.** `dependencyDashboard: true`. ADR-0271 4항은 이슈가 꺼진 저장소라 대시보드를 만들지
   않았다. 2026-11-18 소유자 승인으로 `has_issues` 가 정본대로 켜졌으므로 그 이유가 없다. 대시보드는 대기·막힘·한도로 밀린
   갱신을 이슈 하나에 적고, 수동 실행 상자를 준다 — 이번에 Renovate 가 4주 동안 아무것도 열지 않은 것을 볼 곳이 없었다.

실패가 막는 사고: (1) 변경이 취약한 판을 들여오는 일 — 건드린 lockfile 은 지금처럼 실패한다(PR #76, ADR-0302 zstd).
(2) 새 권고가 아무도 고치지 않은 채 잊히는 일 — 고치는 PR 이 없거나 유예가 지나면 모든 PR 이 실패한다(2026-10-16 zip).

## 재현 (2026-11-18 보고서)

#1418 이전 main(1d0ded6c)의 보고서(발견 15건)를, lockfile 을 건드리지 않는 열린 PR #1413
(`data-go-kr-onbid.contract.json`·시험 파일만 바꿈)의 head 트리에 놓고 판정기를 돌렸다. PR 목록은 GitHub 에서 실제로 읽었다.

| 경우 | 판정 |
|---|---|
| 지금(이 ADR 전) | 15건 모두 실패, #1413 막힘 |
| `security` 라벨의 #1418(11:56:47Z 열림, lockfile 열과 `osv-scanner.toml` 을 바꿈)이 열려 있음 | 15건 모두 경고 — picomatch(8.7) 11-21T11:56:47Z, chrono·libz-sys(점수 없음) 11-19T11:56:47Z, next(6.3) 11-25T11:56:47Z 까지. #1413 통과 |
| 같은 보고서를 #1418 자신의 트리에서 | 그 PR 이 바꾼 lockfile 셋(foundation·intelligence Cargo.lock, gongzzang pnpm-lock)의 8건은 실패, 나머지 7건은 경고. 고친 뒤에는 스캐너 rc=0 이라 판정기에 오지 않는다 |

판정기 자기 시험(`scripts/ci/test_osv_change_verdict.py` 24개, `required/repository` 에서 래칫 바로 앞)은 건드린 lockfile,
읽지 못한 경로, 고치는 PR 없음, 유예 지남, 닫고 다시 연 고침, 병합·닫힘만 있음, 라벨 없음, 다른 ID, 본문에만 적고 파일을
바꾸지 않음, base 없음, 모르는 보고서 모양, 발견을 하나도 못 읽음, 계약의 잘못된 값 다섯을 하나씩 심어 실패를 확인하고,
실제 git 저장소 하나에서 건드린 lockfile 과 건드리지 않은 lockfile 을 함께 판정한다.

## Consequences

- 새 권고 하나가 모든 PR 을 막는 시간이 "누가 고칠 때까지"에서 "누가 고치는 PR 을 열 때까지"로 줄고, 그 뒤로는 그
  lockfile 을 바꾸는 PR 만 막힌다. 고침이 유예 안에 병합되지 않으면 지금과 같아진다.
- 유예 중 머지 큐는 그 권고가 있는 main 위에 다른 변경을 병합한다. main 은 그 권고를 이미 갖고 있었으므로 노출은 늘지
  않는다. 그 lockfile 에 판을 바꾸는 변경은 여전히 막힌다. 머지 그룹의 base 는 큐 앞의 PR 을 포함하므로, 앞 PR 이 그
  lockfile 을 바꾸면 뒤 PR 은 건드리지 않았어도 그 변경과 함께 판정된다(실패 쪽).
- 유예를 여는 열쇠는 세 가지가 함께다: 쓰기 권한자만 다는 라벨, 그 lockfile·허용 목록의 변경, 권고 ID. 고치지 않는
  "고침" 으로 얻는 것은 첫 고침부터 잰 유예 한 번뿐이다.
- PR 실행마다 발견이 있을 때만 라벨 PR 목록을 한 번 읽는다(`gh pr list`, 작업 토큰). 토큰은 `contents: read` 뿐이다.
  공개 저장소의 공개 자료는 그것으로 읽힌다(ADR-0554 의 `main_health.py` 가 같은 토큰으로 실행·작업 목록을 읽는다). 읽지
  못하면 경고를 내고 지금처럼 막는다.
- 자동으로 고치는 PR 을 여는 쪽은 새로 만들지 않는다. ADR-0271 의 Renovate 가 그 일을 한다(Dependabot·자체 스크립트보다
  Renovate 를 고른 이유는 ADR-0271 2항). 지금은 Context 의 어긋남 때문에 PR 을 열지 못하고, 사람이 연다. main 의
  `main-red`(ADR-0554)·`Cargo advisories on main`(ADR-0271) 이슈도 `has_issues` 가 켜져야 열린다.

## 소유자가 할 일 (저장소 밖, 한 번)

`bash scripts/github/configure-public-repository.sh verify` 가 보고하는 어긋남을 정본대로 맞춘다: 비-main 방화벽에
Renovate(Integration 2740) 우회 행위자, `has_issues: true`. ADR-0271 의 "소유자가 할 일" 2항 그대로다. 그 전까지는
고치는 PR 을 사람이 연다 — 유예 규칙은 누가 열었는지 보지 않는다.

## Rejected alternatives

| 대안 | 판정 |
|---|---|
| 권고의 `published` 로 유예를 잰다 (상태 없음) | 기각. chrono 는 `published` 가 OSV 에 나타나기 두 달 전이다. 유예가 0 이 되거나 과거로 늘어난다. |
| main 의 예약 실행이 `osv-scanner.toml` 에 만료 있는 항목을 써서 커밋한다 (W83 제안 2항) | 기각. 워크플로우에 쓰기 토큰이 필요하다(ADR-0158 B·C 기각 사유). 첫 고침 PR 의 생성 시각이 같은 시계를 비밀 없이 준다. |
| head 보고서 − base 보고서 (GitHub `dependency-review-action` 모델) | 부분 채택. "변경이 들여온 것만" 은 lockfile 단위로 같다(건드린 lockfile 은 같은 파일의 기존 발견까지 판정한다). 마감이 없으면 권고가 잊히므로 고치는 PR 과 유예를 더했다. |
| PR 은 건드린 트리만, 마감 없음 | 기각. 2026-10-16 zip 처럼 아무도 모른다. ADR-0158 이 같은 이유로 미뤘다. |
| 머지 그룹만 전체 판정 | 기각. PR 은 초록인데 큐에서 막혀 얻는 것이 없다(ADR-0158). |
| 고침 PR 의 본문만 본다 | 기각(EGM 검토). 작성자가 본문을 고쳐 아무 권고에나 유예를 걸 수 있다. 그 lockfile·허용 목록을 바꾸는 PR 로 좁혔다. |
