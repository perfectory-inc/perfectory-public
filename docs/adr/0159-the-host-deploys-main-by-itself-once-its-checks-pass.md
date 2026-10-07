# ADR 0159: 서버는 검사를 통과한 main 을 스스로 받아 배포한다

- Status: Accepted
- Date: 2026-10-07
- Builds on: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)(릴리스 인증),
  [ADR-0136](./0136-release-admission-reads-the-public-repository-identity-without-a-github-login.md)(공개 저장소 익명 읽기),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(Airflow 는 유닛을 시작만 한다)

## Context

2026-10-07 까지 ai-server 배포는 사람이 했다. 절차(DAG 멈춤 → 작업 끝나기 기다림 → 제어 체크아웃 → 인증 빌드 →
FLOOR 설정 → 활성화·마이그레이션·타이머 → Airflow → 작업 한 번씩 → DAG 재개)는 저장소가 아니라 작업자 임시 폴더의
스크립트 사본에 있었고, 작업 목록도 그 사본에 손으로 적혀 있었다. 그날 하루에 병합 3번마다 작업자가 사본에 커밋을
채워 넣고 SSH 로 띄우고 끝나기를 지켜봤다(배포 1회 약 15분 + 기다림).

- 배포는 Airflow 자신도 다시 띄운다(`airflow-runtime.sh up -d`). Airflow 가 배포를 SSH 로 시작하고 기다리면
  ADR-0122 의 시작 명령은 연결이 끊길 때 그 유닛을 멈추므로, 배포가 자기 스케줄러를 재시작하는 순간 배포도 멈춘다.
- main 의 커밋은 머지 큐에서 필수 검사를 통과한 그 트리다. 그 커밋의 check run 은 공개 API 로 익명으로 읽힌다
  (2026-10-07 실측: f57c845b 의 check run 49개 모두 success).
- 사례: 대규모 운영의 배포 에이전트는 배포 대상 밖에서 돌고, 저장소의 원하는 상태를 스스로 가져와 맞춘다(pull 방식,
  Argo CD·Flux 의 GitOps). 사내망 서버는 밖에서 들어오는 연결 없이 이 방식으로 배포된다.

## Decision

1. **배포 절차는 저장소에 있다.** `platforms/foundation-platform/scripts/deploy/foundation-deploy.sh <sha>` 가 위
   절차 전체다. 멈추고 기다리고 재개하는 작업, 배포 뒤 한 번 돌리는 작업은 모두 `orchestration/jobs.v1.json` 에서
   읽는다(`enabled`, `systemd_service`, 새 칸 `started_once_after_deploy`, 각자의 `timeout_minutes`). 사람이 돌려도
   같은 스크립트다.
2. **서버가 스스로 배포한다.** `foundation-autodeploy.timer` 가 10분마다 root 로
   `foundation-autodeploy.sh` 를 돌린다. 그것은 main 의 머리를 익명으로 읽고, 서버가 이미 그 커밋이면 아무것도 하지
   않는다. 그 커밋의 check run(`release_checks.py`)이 모두 성공·건너뜀·중립이면 배포하고, 하나라도 진행 중이면 다음
   번에 다시 보고, 하나라도 실패면 그 커밋은 배포하지 않는다.
3. **배포는 지금 믿는 제어 체크아웃의 코드가 한다.** 배포할 커밋의 스크립트가 자기를 배포하지 않는다. 경로는 고정이고
   환경 변수로 바꿀 수 없다(ADR-0134 와 같은 이유).
4. **실패는 한 번 알리고 다시 하지 않는다.** 검사 실패로 거부했거나 배포가 실패한 커밋은 상태 폴더
   (`/var/lib/perfectory/autodeploy/{refused,failed}/<sha>`)에 남고, 그 한 번만 유닛이 실패해 기존 Slack 알림
   (`OnFailure`)이 간다. 넘어가는 길은 main 의 더 새 커밋이다. 배포 뒤 작업이 실패하면 DAG 는 멈춘 채로 둔다.
5. **켜고 끄기는 서버의 파일이다.** 서버가 배포자 계정을 `/etc/foundation-platform/release-deploy.conf`
   (`FOUNDATION_DEPLOYER=<계정>`)에 적은 뒤에만 릴리스의 `timers` 가 타이머를 켠다.
   `/etc/foundation-platform/autodeploy.off` 가 있으면 아무것도 하지 않는다.
6. **배포는 Airflow 작업이 아니다.** DB 백업처럼 서버의 systemd 타이머다(ADR-0118 §1). 유닛 시간 한도(17h)는
   기다림 5h + 빌드·마이그레이션 1h + 배포 뒤 작업 한도의 합보다 크고, 시험이 그 관계를 지킨다.

## Consequences

- 병합하면 서버가 10분 안에 알아채고 배포한다. 작업자가 배포 사본을 만들고 띄우고 지켜보는 일이 없어진다.
- main 에 병합하는 것이 곧 운영 배포 승인이다. 머지 큐와 필수 검사가 그 문이다.
- GitHub Actions 사용량은 늘지 않는다(배포는 서버에서 돈다). 비공개로 바꾸면 익명 읽기 두 곳(머리, check run)에
  읽기 토큰 하나가 필요하다(ADR-0136 의 후속).
- Cloudflare Worker 배포와 단계 배포 판정의 자동화는 이 다음이다. 단계 배포 판정은 양쪽 모두 처음 읽는 PNU 로
  비교하게 고친 뒤에 자동으로 돌린다(2026-10-07 건물 묶음 1% 단계의 불공정 비교).
- 출처: [Argo CD](https://argo-cd.readthedocs.io/en/stable/), [Flux](https://fluxcd.io/flux/concepts/),
  [GitHub check runs API](https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference).
