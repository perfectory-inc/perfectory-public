# ADR 0585: 준비된 PR 은 GitHub 자동 병합으로 병합 큐에 들어간다 — 노트북의 스크립트가 아니라

- Status: Accepted
- Date: 2026-11-19
- Amends: [ADR-0271](./0271-dependency-updates-arrive-as-renovate-pull-requests-and-main-keeps-one-advisory-issue.md) 결정 4 의
  "의존성 대시보드 이슈는 만들지 않는다" 와 결정 5 의 "`allow_auto_merge` 는 계속 꺼 둔다",
  [ADR-0553](./0553-a-fix-for-main-or-the-deploy-goes-to-the-front-of-the-queue-without-emptying-it-and-a-run-the-queue-dropped-is-cancelled.md)
  결정 1 "큐에 넣는 길은 `scripts/github/merge_queue_enqueue.py <PR>` 하나다"(앞에 세우는 길은 그대로 그 스크립트다)와
  Consequences 의 "코디네이터의 keeper 는 … 이 스크립트를 부른다"(이제 고치는 PR 의 작성자가 부른다)
- Builds on: [ADR-0552](./0552-the-merge-queue-merges-a-pull-request-only-when-its-own-queue-run-is-green.md)(큐는 자기 실행이 초록인 PR 만 병합)

## Context

GitHub 병합 큐는 PR 을 스스로 큐에 넣지 않는다. 누군가 "Merge when ready" 를 눌러야 한다. ADR-0271 결정 5 는 그 일을
"코디네이터가 초록 non-draft PR 을 넣는다" 로 맡겼고, 그 코디네이터는 노트북에서 도는 스크립트다(저장소 밖,
`keeper.sh` ~ `keeper5.sh` 와 보조 파일 약 605줄). 5분마다 열린 PR 을 훑어 필수 검사가 초록인 것을
`enqueuePullRequest` 로 넣는다.

이 구조가 낸 사고와 빈틈:

- **노트북이 꺼지면 아무것도 병합되지 않는다.** 스크립트는 55분짜리 실행이고 사람이 다시 띄운다.
- **2026-11-13: #1332 를 414번 넣었다.** 옛 base 위에서 초록이던 PR 이 모든 큐 실행에서 실패했고, 스크립트는 떨어질 때마다
  다시 넣었다. 넣을 때마다 뒤의 PR 이 다시 지어졌다. 그 뒤 "큐에서 실패한 PR 은 푸시가 있을 때까지 넣지 않는다" 를
  스크립트에 손으로 더했다.
- **Renovate 의 자동 병합은 한 번도 GitHub 것을 쓰지 못했다.** `.github/renovate.json` 의 보안 갱신은 `automerge: true`
  인데 저장소 설정 `allow_auto_merge` 가 꺼져 있었다.
- **Renovate 가 조용해도 보이지 않았다.** 대시보드 이슈가 없어 4주 동안 PR 이 없다는 것을 아무도 몰랐다(#1441 이 매일
  검사를 더한다).

상류 문서(2026-11-19 에 읽음):

- GitHub, [Merging a pull request with a merge queue](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/merging-a-pull-request-with-a-merge-queue):
  `gh pr merge` 는 큐가 필수인 가지에서 필수 검사가 통과했으면 큐에 넣고, 아니면 자동 병합을 켠다. 요건이 맞으면 GitHub 가
  큐에 넣는다. 저장소의 "Allow auto-merge" 가 꺼져 있어도 된다. 큐 검사가 실패하면 PR 은 큐에서 빠진다.
- GitHub, [Automatically merging a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/automatically-merging-a-pull-request):
  쓰기 권한이 있는 사람이 켠다. 쓰기 권한 없는 사람이 head 에 푸시하거나 base 를 바꾸면 꺼진다.
- Renovate 44.252.8, [Automerge — GitHub Merge Queue](https://docs.renovatebot.com/key-concepts/automerge/#github-merge-queue):
  `platformAutomerge`(기본값 켜짐)이면 GitHub 자동 병합이 PR 을 큐에 넣고, **이때 저장소의 "Allow auto-merge" 가 켜져
  있어야 한다.** 소스 `lib/modules/platform/github/index.ts` 도 `autoMergeAllowed` 가 거짓이면 시도하지 않는다.
- Renovate, [`dependencyDashboard`](https://docs.renovatebot.com/configuration-options/#dependencydashboard): 대기·열림·
  닫힘·오류 상태의 PR 을 이슈 하나에 적는다. `config:recommended` 에 기본으로 들어 있고, 켜도 Renovate 의 일정과 한도는
  바뀌지 않는다.

업계: Renovate 자신의 저장소가 병합 큐와 자동 병합을 이렇게 쓴다(위 문서가 자기 큐 화면을 예로 든다). Kubernetes 의
Tide, Rust 의 bors 처럼 큰 프로젝트는 "넣는 일" 을 서버 쪽 서비스가 한다. 개인 장비의 스크립트가 아니다.

## Decision

1. **PR 은 GitHub 자동 병합으로 큐에 들어간다.** 작성자(사람·에이전트)는 PR 을 준비 상태로 바꾸면서
   `gh pr merge <번호> --auto --squash` 를 한 번 부른다. 필수 검사가 초록이 되면 GitHub 가 큐에 넣는다. 초안에는 자동
   병합을 켤 수 없으므로 "초안 = 아직 넣지 않는다" 는 그대로다(다른 PR 위에 쌓인 PR 은 초안으로 둔다).
2. **저장소 설정 `allow_auto_merge` 는 켠다.** `tools/github/repository-settings.json` 이 정본이고
   `scripts/guard/public-github-policy.sh` 가 켜져 있음을 단정한다. 막는 사고: 2026-11-13 의 414번 재투입과, 노트북이
   꺼진 동안의 병합 정지.
3. **Renovate 의 patch·digest·pin·pinDigest 묶음 넷과 lockfile 유지보수는 `automerge: true` 다.** 보안 갱신은 이미
   그랬다. minor·major
   는 초안으로 열려 사람이 본다(ADR-0097, ADR-0302 그대로).
4. **도구 사슬 보류는 Renovate 설정에 있다.** `node`·`pnpm`·`rust`·`packageManager` 는 `enabled: false` 라 PR 이
   열리지 않는다(ADR-0097 결정 1). 스크립트가 PR 을 열어 본 뒤 걸러 내던 일은 필요 없다.
5. **Renovate 의존성 대시보드를 켠다.** `config:recommended` 가 기본으로 켜는 것을 `.github/renovate.json` 이 끄고
   있었다. 그 줄을 지운다(Renovate 44 에서 이 옵션은 `dependencyDashboard.enabled` 객체다). Renovate 가 무엇을 찾았고
   무엇을 왜 미뤘는지가 이슈 하나에 있다.
6. **앞에 세우는 길은 그대로이고, 부르는 사람이 바뀐다.** `hotfix:main-red`·`hotfix:deploy-stop` 라벨이 붙은 PR 은
   `scripts/github/merge_queue_enqueue.py` 가 `jump: true` 로 넣는다(ADR-0553). 지금까지는 노트북 스크립트가 불렀고,
   이제 고치는 PR 의 작성자가 라벨을 붙이고 한 번 부른다(`main-red` 이슈가 그 명령을 적어 준다). 자동 병합으로 이미 뒤에
   선 PR 도 그 스크립트가 자기 항목만 빼서 앞에 세운다. 라벨 없는 PR 에 이 스크립트를 부를 일은 없다.
7. **노트북 스크립트의 큐 투입 부분은 지운다.** 작업자 깨우기·실패 유닛 기록 같은 코디네이터 일은 이 결정의 범위가
   아니다.

## Consequences

- 병합이 노트북에 기대지 않는다. PR 이 초록이 되는 순간 큐에 들어가므로 5분 훑기 지연도 없다.
- 큐에서 실패해 빠진 PR 은 누가 다시 켜기 전까지 다시 들어가지 않는다. 다시 넣는 것은 작성자의 결정이다(고치는 푸시 뒤
  `gh pr merge --auto`). 큐에서 빠질 때 자동 병합 요청이 함께 지워지는지는 문서에 없어, 첫 실제 사례에서 확인하고 이
  문서에 날짜와 함께 덧붙인다.
- 워크플로는 자동 병합을 켜지 않는다. 워크플로 토큰의 기본 권한은 읽기이고(`tools/github/workflow-permissions.json`,
  `public-github-policy.sh` 가 단정), 어떤 작업도 `pull-requests`·`contents` 쓰기를 갖지 않는다. 켜는 주체는 작성자와
  Renovate 앱이다.
- 스크립트의 "lane freeze"(특정 파일을 건드리는 PR 을 잠시 붙잡기)는 옮기지 않는다. 필요하면 그 PR 을 초안으로 돌린다.
- 실제 설정은 이 PR 이 병합될 때 `gh api -X PATCH repos/{owner}/{repo} -f allow_auto_merge=true` 로 맞춘다(관리자 토큰.
  ADR-0423 의 OpenTofu 적용이 들어오면 그 길로 간다). 매일 도는 대조(#1441)가 어긋남을 알린다.

## Rejected alternatives

| 대안 | 왜 아닌가 |
|---|---|
| 스크립트를 GitHub Actions 예약 작업으로 옮긴다 | 큐에 넣으려면 쓰기 토큰이 필요하다. 공개 저장소 워크플로에 쓰기 권한을 더하는 일이고, 훑기 지연과 재투입 규칙을 그대로 손으로 들고 간다. GitHub 가 이미 하는 일이다. |
| `ready_for_review` 이벤트에서 워크플로가 자동 병합을 켠다 | 같은 이유로 쓰기 토큰이 필요하다. 작성자의 한 줄이면 된다. |
| Mergify·Kodiak 같은 병합 봇 | 앱 하나와 설정 문법 하나가 더 생긴다. 병합 큐와 자동 병합으로 충분하다. |
| 설정은 끈 채 `gh pr merge` 만 쓴다 | 사람 PR 은 되지만(문서상 큐 가지에서는 설정과 무관) Renovate 는 설정이 켜져 있어야 한다. |
