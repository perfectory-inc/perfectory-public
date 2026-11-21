# ADR 0570: 저장소는 매일 tools/github 와 대조되고, main 에 권고가 있는데 Renovate 가 조용하면 같은 이슈가 열린다

- Status: Accepted
- Date: 2026-11-18
- Builds on: [ADR-0271](./0271-dependency-updates-arrive-as-renovate-pull-requests-and-main-keeps-one-advisory-issue.md)(Renovate, 매일 권고 검사와 이슈 하나),
  [ADR-0554](./0554-main-says-when-it-is-red-from-the-runs-github-already-made.md)(GitHub 이 이미 한 일을 읽어 이슈로 알린다),
  ADR-0568(#1438, Renovate 가 고치는 PR 을 연다는 전제)

## Context

2026-11-18 `bash scripts/github/configure-public-repository.sh verify`(관리자 토큰)가 정본 `tools/github/*.json` 과
살아 있는 저장소의 어긋남 넷을 보고했다. 소유자 승인으로 같은 날 모두 정본대로 맞췄다.

| 어긋남 | 언제부터 | 결과 |
|---|---|---|
| 비-main 방화벽(19595435)에 Renovate(Integration 2740) 우회 행위자 없음 | 2026-10-22 앱 설치 때부터 | Renovate 가 브랜치를 못 만들어 4주 동안 PR 0. 그 사이 권고 넷이 모든 PR 을 막았다(ADR-0568 Context) |
| `has_issues` false (정본 true) | 같음 | `dependency-advisories` 의 이슈 잡이 매일 실패, ADR-0554 의 `main-red` 이슈도 못 열림 |
| Actions 허용 목록에 정본에 없는 `dtolnay/rust-toolchain@…` | ADR-0155 가 그 액션을 뺀 뒤 | 쓰는 워크플로우 없음(`rust-ci-setup` 가드가 거부) |
| main 규칙 집합의 merge queue `HEADGREEN` (정본 `ALLGREEN`) | 2026-11-12 ADR-0552(#1347) 병합 뒤 | ADR-0552 의 결정(자기 큐 실행이 초록인 PR 만 병합)이 실물에 적용되지 않았다 |

정본 파일은 맞았다. 대조를 돌린 것이 없었다. `verify` 는 관리자만 읽는 끝점(규칙 집합의 `bypass_actors`, Actions 권한,
결제 예산)을 읽으므로 사람이 손으로만 돌렸고, 아무도 돌리지 않았다. 공개 저장소의 워크플로우에는 관리자 비밀을 두지
않는다(ADR-0007, ADR-0158 B·C).

익명 요청(작업 토큰과 같은 읽는 사람)으로 읽히는 것을 2026-11-18 재었다: `repos/{repo}` 의 `has_issues`·`has_projects`·`has_wiki`·
`has_discussions`·`description`(병합 설정과 `security_and_analysis` 는 null), 규칙 집합마다 `rules`·`conditions`·
`enforcement`·`target`(`bypass_actors` 는 없음), Actions 권한은 401.

## Decision

1. **매일 저장소를 정본과 대조한다.** `dependency-advisories.yml`(예약 21:41Z) 에 잡 `Repository drift and Renovate pulse`
   를 더한다. `scripts/ci/repository_drift.py check` 가 작업 토큰으로:
   - `repository-settings.json` 의 키 중 읽는 사람에게 주어지는 다섯(`has_issues`·`has_projects`·`has_wiki`·
     `has_discussions`·`description`)을 비교한다. 비운 설명도 어긋남이다.
   - 저장소의 규칙 집합 목록(부모 포함)을 이름·대상·집행·출처로 비교하고(`verify` 의 effective-set 검사,
     `github-policy-json.py ruleset_summaries`), 정본 셋(main, 비-main 방화벽, 태그 방화벽)은 `verify` 와 같은 정규화
     (`normalize_ruleset`)로 비교하되 `bypass_actors` 만 뺀다.
2. **보이지 않는 것은 증상으로 본다: Renovate 의 맥박.** main 에 권고가 있을 때 — 같은 실행의 `Every Cargo workspace on main`
   단계가 실패했거나(잡이 아니라 그 단계의 결과: 설치 실패는 권고가 아니다), main 의 마지막으로 끝난 예약 `monorepo-guard`
   실행에서 `Repository OSV vulnerability ratchet` 단계가 실패 — Renovate 의 열린 PR 이 하나도 없고 지난 48시간 안에 연
   PR 도 없으면 발견이다. 의존성 대시보드(ADR-0568 8항)는 세지 않는다: 브랜치를 못 만드는 Renovate 도 대시보드는 고친다.
   우회 행위자가 빠지거나 앱이 지워져 Renovate 가 쓰지 못하는 것이 여기서 드러난다. 어느 릴리스도 고치지 않는 권고도
   여기서 보인다 — 그것을 움직이는 것이 없다는 뜻이 맞고, 빠져나가는 길은 ADR-0414 의 만료 있는 허용 항목이다. 권고가
   없거나 있는지 알 수 없으면(검사가 끝나지 않음) 맥박은 보지 않는다.
3. **신호는 따로 읽는다.** 설정·규칙 집합·맥박 중 읽지 못한 것은 "읽지 못함" 으로 적고(`::warning`), 나머지 발견은 그대로
   센다. 셋 다 읽지 못한 실행만 이슈를 그대로 둔다(ADR-0554 4항과 같다).
4. **알림은 이슈 하나다.** 발견이 있으면 제목 `Repository drift or Renovate silence` 인 이슈를 열고, 발견이 바뀔 때만 본문을
   고치고 댓글을 단다(줄바꿈·끝 공백은 정규화하고 매일 바뀌는 실행 링크는 비교하지 않는다). 깨끗하면 닫는다. 실행도 빨갛게
   끝난다 — 이슈를 쓰지 못해도. 이슈는 전체 그림을 위해 관리자가 `verify` 를 돌리라고 적는다. 새 비밀·권한·유료 서비스는
   없다: 이 파일은 이미 잡 수준 `issues: write` 를 받는 파일이다(ADR-0271 7항, `tools/conftest/workflow/contracts.rego`
   는 파일 단위로 허용한다). 쓰는 잡이 둘이 되고, 둘 다 크레이트를 읽는 잡과 분리되어 있다.
5. 자기 시험 `scripts/ci/test_repository_drift.py`(19개)가 `required/repository` 에서 돈다: `has_issues` 꺼짐, 비운 설명,
   `HEADGREEN`, 여분·상속 규칙 집합, 규칙 집합 없음·둘, 집행 꺼짐, 맥박 없음(한 번도·48시간 넘음), 열린 PR 과 최근 PR 은 맥박,
   이슈 열기·같은 발견은 쓰지 않음(CRLF 본문)·바뀐 발견은 고치고 댓글·깨끗하면 닫기·읽지 못함과 결과 없음은 그대로·이슈를
   못 써도 빨강, 신호 하나를 못 읽어도 다른 발견은 남음.

6. **2026-11-21 개정(W83e): 이슈가 아니라 잡의 결론과 Slack 이다.** 4 의 이슈는 이것으로 대체된다. 저장소의 이슈는 꺼
   둔 채로 두고 `tools/github/repository-settings.json` 도 `has_issues: false` 다(ADR-0554 5).
   - `repository_drift.py report` 가 잡의 판정이다. 깨끗하면 초록이다. 발견이 있으면 실패하면서 `repository-drift-count`
     주석과 발견마다 `repository-drift` 주석을 남긴다(단계마다 error 주석 10개 한도라 9개까지). 읽지 못함이나 결과 없음은
     개수 주석 없이 실패하고, 그것은 판정이 아니다.
   - 호스트의 서비스 점검이 main 위의 가장 새 판정을 `repository` 부분에 읽는다. 읽는 길은 main-health 와 같은 부품이다
     (`REPORTS`). 그다음 foundation-api 의 `foundation_repository_drift{finding}` 과 `FoundationRepositoryDrift`(ticket)를
     거쳐 Slack `#alerts` 로 간다. 판정 없이 이틀이면 `FoundationRepositoryDriftUnreadable` 이다.
   - 이 파일의 `issues: write` 는 없어졌다. conftest 는 이제 어떤 워크플로의 `issues: write` 도 거부한다.

실패가 막는 사고: 정본은 맞는데 실물이 다른 채 몇 주가 지나는 일(위 표의 넷 — 특히 Renovate 4주 침묵과, 병합된 ADR-0552 가
실물에 없던 6일).

## 재현 (2026-11-18, 살아 있는 저장소)

| 상태 | `check` |
|---|---|
| 정본에 GitHub 의 새 매개변수 `require_extra_approval_for_unattributed_changes` 가 아직 없을 때(#1429 전) | `ruleset perfectory-public-main: rules differs` |
| 그 매개변수를 적은 정본(#1429) | `none` |
| 11-18 이전 상태: Renovate PR 이 하나도 없고 Cargo 권고가 있음(시험으로 심음) | 맥박 없음(`last opened: never`) |
| 지금: Renovate 의 #1431(초안, pnpm 툴체인 파도)이 열려 있음 | 맥박 있음 |

## Consequences

- 우회 행위자·Actions 허용 목록·결제 예산 같은 관리자 전용 어긋남은 직접 보지 못한다. 그중 PR 을 막는 것(Renovate 가 못 씀)은
  맥박으로 보고, 나머지는 이슈가 열릴 때 관리자가 `verify` 로 본다.
- GitHub 이 규칙 집합에 새 매개변수를 더하면 다음 날 발견이 된다(#1429 가 그 첫 사례). 정본에 적어 닫는다.
- 하루 한 번 API 호출 많아야 9번(저장소, 규칙 집합 목록과 셋, `monorepo-guard` 실행 목록과 그 작업, Renovate PR 목록), 표준 러너 1분 미만, 비용 0.
- 작업 토큰(`contents: read`)으로 공개 PR·실행을 읽는 것은 ADR-0554 의 `main_health.py` 가 이미 하는 일이다. 첫 예약 실행이 실제 토큰으로 이를 확인하고, 읽지 못하면 그 신호만 "읽지 못함" 으로 이슈에 남는다.

## Rejected alternatives

| 대안 | 판정 |
|---|---|
| 관리자 토큰을 비밀로 두고 `verify` 전체를 예약 실행 | 기각. 공개 저장소 워크플로우에 관리자 비밀(ADR-0007, ADR-0158 C). 이 파일 하나의 `issues: write` 보다 훨씬 넓다. |
| ai-server 의 호스트 검사(ADR-0278)에서 대조 | 기각. 호스트는 GitHub 를 익명으로 시간당 60번 읽고 그 한도를 자동 배포 판정이 쓴다(ADR-0554 기각 사유와 같다). 관리자 자격도 없다. |
| Renovate 가 7일 조용하면 무조건 알림 | 기각. 할 일이 없을 때도 울린다. 권고가 있을 때만 본다. |
| `main-health.yml`(10분마다)에 넣기 | 기각. 설정 어긋남은 하루에 한 번이면 족하고, 10분마다 API 호출을 늘릴 이유가 없다. |
