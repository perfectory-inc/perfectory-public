# ADR 0270: 의존성 갱신은 Renovate PR 로 도착하고, main 의 Cargo 권고는 이슈 하나가 들고 있다

- Status: Accepted
- Date: 2026-10-17
- Amends: [ADR-0008](./0008-manual-dependency-updates-and-organization-branches.md) 결정 "Dependabot 과 자동 보안
  수정 PR 을 사용하지 않는다", [ADR-0158](./0158-npm-overrides-have-one-source-and-a-new-advisory-arrives-as-a-prepared-change.md)
  자동 PR 경로 평가 A 행. ADR-0008 이 요구한 "자동화를 다시 도입할 때의 도구·권한·PR 생성 규칙"이 이 ADR 이다.

## Context

2026-10-16–17 이틀 동안 zip 이 두 번 모든 PR 을 몇 시간씩 막았다. 10-16 03:32Z 상류가 9.0.x 를 뺀 모든
릴리스 라인을 yank 했고, 10-09/10 에 공개된 RUSTSEC-2026-0367·0368·0369 가 그 뒤에 따라왔다(#662). 둘 다 커밋
없이 main 에 도착했고, 아무도 몰랐다가 무관한 PR 의 `cargo-deny` 가 처음 빨개졌다.

| 사실 | 값 |
|---|---|
| 추적 Cargo 워크스페이스 | 6 (dawneer, foundation, identity, intelligence, gongzzang, tools/xtask) |
| main 에서 예약 실행되는 Rust 권고 검사 | 0 — OSV 래칫(ADR-0158 §6)은 매일 돌지만 yank 를 보지 않고, `cargo-deny`(yanked = deny)는 PR 에서만 돈다 |
| 의존성 갱신 PR 을 여는 것 | 사람(ADR-0008). npm 은 준비된 patch(ADR-0158 §7), Cargo 는 보고서만 |
| 저장소 설정 | issues 꺼짐, 자동 병합 꺼짐, 쓰기 권한 워크플로우 0, 비-main 브랜치는 유지보수자 한 명만 생성 |

## Decision

1. **갱신 PR 을 여는 것은 호스팅 Renovate 앱이다.** 설정은 `.github/renovate.json` 하나. Cargo 워크스페이스 전부,
   pnpm 트리 전부, GitHub Actions, 컨테이너 이미지가 대상이다. 앱이 브랜치를 쓰고 PR 을 열므로 워크플로우에 쓰기
   토큰·비밀이 생기지 않고, 앱이 연 PR 은 `GITHUB_TOKEN` PR 과 달리 필수 검사를 트리거한다. 비용은 0 이다(공개
   저장소의 Mend 호스팅 앱, 표준 러너).
2. **Renovate 를 고른 이유는 버전 계약(ADR-0097)이다.** 핀의 정본은 `tools/technology-versions.contract.json` 이고
   매니페스트·Dockerfile·compose·스크립트는 그 사본이다. Dependabot 은 정해진 매니페스트 형식만 고칠 수 있어 계약을
   건드리지 못한다 — 그 PR 은 `technology-version-consistency`·`container-images-match-the-contract` 가드가 전부
   거부한다(ADR-0158 이 A 를 기각한 이유와 같다). Renovate 는 정규식 관리자로 계약 자체를 읽고 고친다:
   `manifest_exact_pins`(npm), `container_images`(repository:tag → digest), 그리고
   `scripts/catalog/sync-container-images.py` 가 검사하는 것과 같은 `repository:tag@sha256:` 패턴으로 모든 사본을.
   같은 의존성·같은 새 판은 같은 브랜치에 모이므로 한 PR 이 계약과 모든 사본을 함께 옮기고, 가드는 한 값을 본다.
   내장 dockerfile·docker-compose 관리자는 끈다(사본을 둘째 정본으로 읽지 않기 위해).
3. **Renovate 가 만지지 않는 것.** node·pnpm·rust 는 런타임·툴체인 파도(ADR-0097 결정 1)라 끈다.
   `pnpm.overrides` 는 `tools/npm/security-overrides.contract.json` 의 렌더(ADR-0158)라 끈다. SHA 가
   `tools/github/selected-actions.json` 과 실제 Actions 허용 목록에 박힌 서드파티 액션 둘(rust-cache, pnpm
   action-setup)은 그 목록이 먼저 움직여야 하므로 끈다.
4. **큐를 넘치게 하지 않는다.** 데이터소스(crate·npm·docker·actions)별로 patch·digest 를 한 PR, minor 를 한 PR 로
   묶고, major 는 의존성 하나씩이다. 버전 갱신은 월요일 09시(KST) 전에만, 동시 PR 6개·시간당 3개 상한. 보안 갱신
   (`vulnerabilityAlerts`, `osvVulnerabilityAlerts`)은 일정을 기다리지 않는다. 의존성 대시보드 이슈는 만들지 않는다.
5. **자동 병합은 저장소 자동 병합이 아니라 병합 큐다.** patch·digest·보안·lockfile 유지보수 PR 은 준비 상태로 열리고,
   초록이 되면 병합 큐(코디네이터가 초록 non-draft PR 을 넣는다)가 전체 CI 를 다시 돌려 병합한다. minor·major 는
   draft 로 열려 큐에 들어가지 않고 사람이 검토한다. `allow_auto_merge` 는 계속 꺼 둔다 — 큐가 이미 "초록일 때만"을
   보장하고, 큐를 거치지 않는 병합 경로를 하나 더 만들 이유가 없다.
6. **main 의 Cargo 권고는 매일 검사하고 이슈 하나가 들고 있다.** `.github/workflows/dependency-advisories.yml`
   (예약 21:41Z + 수동, PR 을 막지 않는다)이 `scripts/ci/cargo-advisories.sh check` 로 추적 `Cargo.lock` 이 있는 모든
   디렉터리에서 그 워크스페이스의 `deny.toml` 로 `cargo-deny check advisories` 를 돈다(yank 포함). 그 앞에서
   `cargo-advisories-self-test.sh` 가 매일 먼저 심은 권고(RUSTSEC-2021-0003 을 가진 lockfile)를 거부하는지 본다 —
   거부하지 못하는 검사는 깨끗한 main 을 보고할 수 없다. 마지막 잡이 `issue` 로 제목 `Cargo advisories on main`
   인 열린 이슈 하나를 맞춘다: 실패면 열거나, 발견이 바뀌었을 때만 본문을 다시 쓰고 댓글로 알리며, 성공이면 닫는다.
7. **쓰기 권한은 그 잡의 `issues: write` 하나뿐이다.** `check-workflow-policy.sh` 는 이 파일 이름 하나, 예약 전용일
   때, 잡 수준 권한 맵에 `contents: read` 와 함께인 경우에만 `issues: write` 를 받는다(자기시험이 다른 파일·다른
   트리거·최상위·다른 쓰기 권한을 각각 거부한다). 그 잡은 크레이트를 읽은 잡과 분리되어 main 에서만 돌고, 스크립트
   하나만 체크아웃한다.

## 소유자가 할 일 (저장소 밖, 한 번)

1. Renovate 앱 설치: <https://github.com/apps/renovate> → Only select repositories → 이 저장소. 설정 파일이 이미
   main 에 있으므로 온보딩 PR 없이 시작한다.
2. `bash scripts/github/configure-public-repository.sh` 로 원하는 상태를 적용한다: `repository-settings.json` 의
   `has_issues: true`(이슈 하나를 위해), 비-main 방화벽의 우회 행위자에 Renovate 앱(Integration 2740) 추가 —
   main 규칙 집합은 그대로라 앱은 PR 없이 main 에 쓸 수 없다.

## 기각한 대안

| 대안 | 판정 |
|---|---|
| Dependabot 버전·보안 갱신 | 기각. 계약(ADR-0097)과 렌더된 사본을 못 고쳐 핀·이미지 PR 이 가드에 전부 거부된다. 정규식 관리자가 없다. |
| 워크플로우에서 Renovate 자체 실행 | 기각. App 토큰 비밀과 공개 워크플로우 쓰기 권한이 필요하다(ADR-0158 C). 호스팅 앱이 같은 일을 비밀 0 으로 한다. |
| 실패한 예약 실행만으로 알림 | 기각. GitHub 은 마지막으로 cron 을 고친 사람에게만 알린다. 10-16 에 아무도 몰랐던 이유다. |
| `allow_auto_merge` 로 patch 자동 병합 | 기각. 병합 큐가 이미 초록 non-draft 를 병합한다. 큐 밖 경로를 하나 더 열 뿐이다. |
| 방화벽 밖 `renovate/**` 전용 규칙 집합 | 다음 단계로 남김. 더 좁지만 구성기·정책 가드에 규칙 집합을 하나 더 넣어야 한다. 지금은 앱을 기존 방화벽의 우회 행위자로 둔다. |

## Consequences

- 계약 핀·이미지·Cargo·npm 갱신이 사람 손 없이 PR 로 도착한다. 남은 손일: vite 처럼 `pnpm.overrides` 에 기술 핀이
  렌더되는 패키지는 `node tools/npm/security-overrides.mjs render` 한 번(가드가 그 PR 에서 알려 준다), 툴체인 파도,
  허용 목록에 박힌 서드파티 액션.
- 공개 저장소에 이슈가 켜진다. 외부인도 이슈를 열 수 있다는 대가를 이슈 하나의 가시성과 바꾼다.
- 예약 실행은 하루에 Rust 설치 + `cargo metadata` 6번 + 심은 권고 2개를 쓴다(표준 러너, 무료).
