# ADR 0279: 낡은 모양 원장은 CI 가 준비한 PR 로 온다 — 서버는 비공개 버킷에 올리기만 한다

- Status: Accepted
- Date: 2026-10-19 (코디네이터 결정)
- Amends: [ADR-0259](./0259-ci-migrates-the-production-table-shape.md) §5 (갱신 PR 을 서버가 아니라 CI 가 연다)

## Context

`foundation-shape-ledger-check` 는 배포 뒤와 매일 운영 표 모양을 다시 찍어, 커밋된 원장과 다르면 갱신 PR 을 열도록
만들어졌다. 그 PR 은 서버에 둔 GitHub 토큰(`shape-ledger-pr` 묶음)으로 열게 되어 있었으나 토큰은 한 번도 놓이지 않았다.
그래서 낡은 원장은 매번 "손으로 복사하라"로 끝났고, 2026-10-18 에는 표 넷이 48시간을 넘겨 점검이 실패했다(#735 로 손 갱신).

서버에 저장소 쓰기 토큰을 두는 것은 받아들이지 않는다: 서버가 뚫리면 저장소에 쓸 수 있다. 공개 워크플로우는 비밀·변수를
읽을 수 없고(`scripts/guard/check-workflow-policy.sh`), `GITHUB_TOKEN` 이 연 PR 은 필수 검사를 띄우지 않는다(ADR-0158 평가 B).

## Decision

1. **서버는 올리기만 한다.** 점검은 매 실행 두 원장을 비공개 버킷 `foundation-platform-shape-ledger-prod` 의
   `shape-ledger/<원장 파일 이름>` 에 쓴다. 열쇠는 그 버킷에만 쓰는 `foundation-platform-shape-ledger-writer-prod` 하나,
   묶음 `shape-ledger` 에 든다. 올리기 실패나 열쇠 없음은 실행을 실패시키지 않는다(48시간 경보는 그대로).
   서버의 GitHub 토큰 자리(`shape-ledger-pr`)는 없앤다.
2. **비공개 ops 저장소가 읽고 비교한다.** 공개 워크플로우는 비밀을 쓰지 않는다(공개 저장소 안전 가드: 실제 운영
   작업은 비공개 ops 경계에 둔다). 그래서 비교는 비공개 저장소 perfectory-ops 의 예약 워크플로우가 한다. 공개
   저장소를 토큰 없이 체크아웃해 그 안의 `scripts/ops/shape-ledger-compare.py` 를 돌린다. 보호 환경 `shape-ledger`
   (main 만)의 읽기 전용 열쇠(`foundation-platform-shape-ledger-reader-prod`, 그 버킷만)로 두 파일을 받아 커밋된 원장과
   표 단위로 비교하고 실행 요약에 적는다. 비밀이 없으면 "건너뜀"과 이유를 적고 성공으로 끝난다.
3. **PR 은 App 이 연다(후속).** 같은 워크플로우가 준비된 PR 봇(GitHub App: 공개 저장소에만 설치, Contents·Pull requests
   쓰기)의 토큰으로 공개 저장소에 `shape-ledger/refresh` 브랜치와 PR 을 연다(`refresh_pull_request`). App 은 ADR-0158 평가 C 의 권한 그대로이며 npm 권고 PR 도 함께 쓴다.
   App 이 생기기 전에는 2 의 요약까지만 한다.
4. 사장님 할 일(버킷·열쇠 둘·perfectory-ops 환경·서버 파일·App)은 한 번에 하도록
   `platforms/foundation-platform/docs/runbooks/shape-ledger-bucket.md` 에 모은다.

## Consequences

- 서버가 가진 새 열쇠는 원장 버킷 하나에 쓰는 것뿐이다. 비교의 읽기 열쇠도 그 버킷 하나만 읽고, perfectory-ops main 의
  보호 환경에만 있다. 공개 저장소에는 비밀도 가드 예외도 없다.
- 비공개 저장소의 Actions 는 조직 무료 한도에서 차감된다(하루 1분 남짓, 표준 러너).
- 버킷은 비공개다(r2.dev·사용자 도메인 없음). 원장 내용은 이미 공개 저장소에 있으나 공개 경로를 새로 만들지 않는다.
- 병합 판단은 그대로 사람이 한다: PR 의 표별 변화가 예상된 것인지(새 표는 어느 PR 이 만들었나) 확인한다.
