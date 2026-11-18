# ADR 0555: 호스트 설정은 병합 전에 호스트 모양의 루트에 적용해 보고, 배포가 연달아 두 번 실패하면 호출한다

- Status: Accepted
- Date: 2026-11-12
- Builds on: [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)(작업 상태 디렉터리는 생성된 tmpfiles 한 파일),
  [ADR-0345](./0345-the-release-image-is-built-before-merge-and-a-host-behind-main-alerts-with-its-reason.md)(자동 배포가 틱마다 지표를 남긴다),
  [ADR-0477](./0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md)(intelligence 문서 상태 디렉터리, #1337 이 고친 줄)

## Context

2026-11-11 15:34Z 부터 22:01Z 까지 6시간 27분 동안 모든 배포가 실패했다(ai-server `foundation-autodeploy` 일지, #1337).

- #1256 이 작업 상태 디렉터리에 `/data/foundation-platform/intelligence-documents/lakehouse` 를 더했다.
- `/data/foundation-platform` 은 운영자 계정 소유다. `systemd-tmpfiles` 는 그 계정의 디렉터리에서 우리 계정의
  디렉터리로 건너가는 경로를 거부한다("unsafe path transition", exit 73).
- 배포는 `systemd-tmpfiles --create` 에서 멈췄다. 그 뒤 병합된 커밋마다 같은 줄에서 실패했다.

두 가지가 없었다.

1. **그 파일을 `systemd-tmpfiles` 로 돌려 본 검사.** 파일은 작업 목록에서 생성되고 시험은 "생성물과 같다"만 본다.
   처음 실행한 곳이 운영 호스트였다.
2. **연달아 실패한다는 신호.** 실패한 배포는 유닛의 `OnFailure` 로 Slack 에 한 번씩 갔다(15:34Z, 15:41Z, …).
   그날의 다른 알림과 같은 모양이었다. "호스트가 뒤처졌다"는 경보(`FoundationHostBehindMain`)는 6시간을 기다린다.
   고치는 PR 은 2시간 29분 뒤(18:03Z)에 열렸다.

실측(2026-11-12, 읽기 전용): 호스트는 Ubuntu 24.04, systemd 255.4-1ubuntu8.18 이다. GitHub 의 `ubuntu-24.04` 러너와
같은 판이다. 같은 판의 `ubuntu:24.04` 컨테이너에서 `systemd-tmpfiles --root=<임시 루트> --create` 는 호스트와 같은
소유 관계를 주면 같은 문장으로 exit 73 을 낸다.

## Decision

1. **`scripts/deploy/host-config-check.sh` 가 릴리스가 호스트에 놓는 설정을 호스트 모양의 임시 루트에 적용한다.**
   호스트의 도구(`systemd-tmpfiles`, `systemd-analyze`)를 `--root` 로 쓴다.
   - `infra/tmpfiles/*.conf` 를 `--create` 로 두 번. 둘째는 첫째가 만든 디렉터리를 만난다(첫 배포 뒤의 모든 배포).
   - `infra/host/tmpfiles.d/*.conf` 를 `--create` 와 `--clean` 으로.
   - `infra/systemd` 의 모든 유닛을 릴리스처럼(root 0644) 놓고 `systemd-analyze verify`. "명령이 없다"는 임시 루트에
     릴리스 트리와 Docker 가 없어서 나오는 말이라 뺀다. 그 밖의 말(모르는 키, 읽지 못한 값)은 전부 실패다.
2. **임시 루트의 처음 모양은 `scripts/deploy/host-layout.measured` 다.** 릴리스의 tmpfiles 줄이 그 아래에 만들어지는
   호스트 디렉터리의 경로·모드·소유자를 호스트에서 잰 값이다(4줄). 계정 번호는 `infra/sysusers` 가 적은 값, 운영자는
   잰 값(1000), 나머지는 서로 다른 번호를 준다. 거부는 번호가 아니라 소유자가 바뀌는 자리에서 난다.
3. **시험 `orchestration/tests/test_host_config_check.py` 가 그것을 돌린다.** Foundation 의 Python 시험 묶음에 든다.
   - 커밋된 설정은 통과한다.
   - 11-11 의 그 줄을 심으면 "unsafe path transition" 으로 실패한다.
   - systemd 가 무시할 유닛 줄(`MemoryMax=lots`)을 심으면 실패한다.
   - CI 러너에서 돌 수 없으면(리눅스·systemd·root) 건너뛰지 않고 실패한다. 노트북에서는 건너뛴다.
   - 실패가 막는 사고: 11-11 의 exit 73, 곧 호스트가 거부할 설정이 병합되어 모든 배포를 멈추는 일.
4. **자동 배포는 연달아 실패한 커밋 수를 지표로 남긴다.** `foundation_autodeploy_failed_deploys_in_a_row` 는 마지막으로
   성공한 배포 뒤에 배포가 실패한 커밋의 수다(`state/failed` 에서 `state/deployed` 의 가장 새 파일보다 새로운 것).
   0 이면 계열이 없다. 실패한 틱도 끝날 때 지표를 쓰므로 둘째 실패와 같은 틱에 2 가 된다.
5. **`FoundationDeployFailedTwice` 는 그 값이 2 이상이면 호출(`severity: page`)한다.** 한 번의 실패는 그 커밋의 것이고
   지금처럼 Slack 알림으로 간다. 두 커밋이 연달아 실패하면 호스트가 릴리스를 거부하는 것이다.

## 전/후

| | 전 (11-11 실측) | 후 |
|---|---|---|
| 호스트가 거부할 tmpfiles 줄 | 병합됨, 운영 배포에서 처음 실행 | 병합 전 CI 에서 실패 (컨테이너에서 그 줄을 심어 확인: exit 73 과 같은 문장) |
| systemd 가 무시할 유닛 줄 | 검사 없음 | 병합 전 실패 (심어 확인) |
| 배포 정지를 알아챈 때 | 2시간 29분 뒤(고치는 PR 이 열린 때), 호출 없음 | 둘째 실패의 틱에 지표 2 → 호출. 11-11 이었다면 15:41Z, 7분 뒤(계산) |

## Consequences

- 검사는 호스트의 **모양**만큼만 본다. 호스트에만 있는 것(디스크, 다른 사람이 만든 디렉터리, 실행 중인 컨테이너)에서
  나는 실패는 여전히 배포에서 처음 난다. 그런 실패가 나면 `host-layout.measured` 에 그 사실을 더한다.
- `host-layout.measured` 는 손으로 잰 값이다. 호스트의 소유자가 바뀌면 낡는다. 낡으면 검사가 통과시킨 것을 배포가
  거부하고, 넷째·다섯째 항이 그것을 7분 안에 알린다.
- 한 배포가 실패한 뒤 다음 커밋이 성공하면 값은 사라진다. 한 커밋의 실패는 호출하지 않는다.
- 러너와 호스트의 systemd 판이 갈라지면(러너 이미지가 먼저 오른다) 검사는 더 새 판의 말로 판정한다.

## Rejected alternatives

- **경로 규칙을 Rust 가드로 하나씩 더하기**: #1337 이 "운영자 루트의 2단 아래"를 막았다. 다음 거부는 다른 규칙에서
  난다. systemd 의 규칙을 다시 쓰는 대신 systemd 에게 묻는다.
- **실패 한 번에 호출**: 한 커밋의 배포 실패는 흔하고(마이그레이션 승인 대기, 스모크 실패) 그 커밋을 고치면 끝난다.
- **`FoundationHostBehindMain` 의 6시간을 줄이기**: 긴 bake 가 배포를 붙잡는 정상 대기가 p99 7.5시간이다(ADR-0345 실측).
  뒤처진 시간은 실패와 대기를 가르지 못한다.
