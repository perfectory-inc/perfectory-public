# ADR 0184: "어떤 작업이 등록됐나"·"돌고 있나"는 한 곳이 답하고, 설치된 릴리스는 커널이 불변으로 지킨다

- Status: Accepted
- Date: 2026-10-11
- Amends: [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md)(빌드는 혼자 돈다),
  [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)(설치된 릴리스는 불변),
  [ADR-0181](./0181-the-granted-operator-script-runs-the-raon-lanes-supervised-runs.md) 결정 2("도는 중"의 질의 형태)

## Context

2026-10-10/11 운영 호스트에서 세 결함이 드러났다. 셋 다 같은 지식이 여러 곳에 따로 적혀 있었던 것이 원인이다.

1. **"등록된 작업"이 두 번 계산됐다.** 릴리스 승인의 `require_no_registered_job_running()` 은
   `orchestration/jobs.v1.json` 의 모든 작업의 `systemd_service` 를, `foundation-deploy.sh` 0단계는 자체 인라인
   파이썬(`jobs_of services`)을 썼다. 배포는 켜진 작업만 기다렸고, 운영자가 시작한 꺼진 레인의 감독 실행 옆에서
   빌드에 도달해 승인에 거부당했다. 같은 목록이 세 번째로 `foundation-release.sh timers` 의 인라인 파이썬에도 있었다.
2. **"돌고 있나"가 곳곳에서 다른 규칙으로 판정됐다.** 승인은 `ActiveState ∉ {inactive, failed}`, 배포 0단계는
   `inactive|failed` 문자열 비교, 운영자 스크립트는 `list-units --state=active,activating`(그리고 한 곳은
   `is-active --quiet`), 자동 배포는 `active,activating,deactivating,reloading`, 스케줄러 디스패처는
   `activating|active|deactivating`. oneshot 작업은 도는 내내 `activating` 이고 `systemctl is-active` 는 그것을
   멈춤이라고 답한다. 그래서 Gold 재생성이 돌고 있는 Gold 재생성 옆에서 시작될 수 있었다(ADR-0181 이 한 곳만 고침).
3. **릴리스의 불변성은 쓰는 쪽의 자제에 기대고 있었다.** `/opt/foundation-platform/releases/<sha>` 는 0555/0444
   이지만 root 는 모드를 무시하고 쓴다. root 로 돈 import 가 `__pycache__` 를 릴리스 안에 썼고, 승인은 그 뒤 모든
   작업을 "writable" 로 거부했다. 고친 방법(`PYTHONDONTWRITEBYTECODE`, 배포 끝의 `verify-current`)은 그 한 작성자를
   막고 사후에 발견할 뿐, 다음 root 작성자를 막지 못한다.

## Decision

1. **한 파일이 답한다: `platforms/foundation-platform/scripts/deploy/job_units.py`.**
   - `services <jobs.v1.json>`: 등록된 모든 작업의 서비스(꺼진 것 포함). 승인(`running_registered_jobs`)과
     `foundation-release.sh timers` 의 승인 drop-in 설치가 이것을 쓴다.
   - `running <unit|pattern>...`: 도는 서비스마다 `<unit> <ActiveState>`. **도는 중 = ActiveState 가 `inactive`
     도 `failed` 도 아님**(`activating`·`deactivating`·`reloading`·`maintenance`·앞으로 systemd 가 더할 상태 포함).
     질의는 `systemctl list-units --all --type=service` 하나뿐이다: 이름과 glob 을 함께 받고, 상태 필터는 systemd
     가 아니라 이 파일의 규칙이 건다. systemd 가 답하지 못하면 exit 2 — 어느 호출자도 그것을 "아무것도 안 돈다"로
     읽지 않는다(승인 65, 배포 실패, 운영자 70, 자동 배포 이번 틱 실패, 디스패처는 다시 묻는다).
2. **배포 0단계는 승인에게 묻는다.** 승인에 `running-jobs` 하위 명령을 두고(0 = 없음, 75 = 있음·목록 출력, 그 밖 =
   답 없음), 배포는 1단계가 제어 체크아웃을 옮기기 전에 그 체크아웃의 승인으로 이것을 묻는다. 빌드가 거부하는
   바로 그 함수·그 목록이므로 둘은 다시 어긋날 수 없다. 배포 스크립트의 `jobs_of services` 는 지웠다.
3. **모든 판정 자리가 1을 거친다.** 승인, 배포(2를 통해), 자동 배포(운영자 transient 유닛·Worker 배포),
   운영자 스크립트(`refuse_while_active`, 팩 굽기 패턴, Silver 레인 패턴, 세대 유닛), 스케줄러 디스패처(진행 중 실행
   합류·실행 종료 대기). `orchestration/tests/test_job_units.py` 의 `NothingElseAsks` 가 스크립트·DAG·infra·루트
   `scripts/deploy` 의 실행 줄에서 `list-units`, `is-active --quiet`, `ActiveState` 질의를 찾아 거부한다(주석은
   읽지 않는다). 사람에게 상태를 보여 주기만 하는 `systemctl is-active <unit>`(운영자 `status`)은 판정이 아니므로
   허용한다. 런북의 수동 확인도 `job_units.py running` 으로 바꿨다.
4. **설치된 릴리스는 `chattr -R +i` 로 봉인한다.** 승인에 `seal <target>` 을 둔다: 모든 바이트를 확인하고(봉인
   전 상태 허용), `chattr -R +i`, 다시 확인한다. `verify`·`build`·`verify-current` 의 `verify_release` 는 이제
   모든 항목(디렉터리 포함)에 inode 의 immutable 속성(`FS_IOC_GETFLAGS` 의 `FS_IMMUTABLE_FL`)을 요구한다.
   - `foundation-release.sh install` 은 staging 을 `releases/<sha>` 로 옮긴 **뒤** 봉인하고 그다음 빌드한다(불변
     디렉터리는 rename 이 안 되므로 순서가 이렇다). 이미 있는 대상도 빌드 전에 봉인한다.
   - `assert_installed_release`(activate·rollback·FLOOR 설정이 지나는 곳)는 `seal` 다음 `verify` 를 부른다. 그래서
     이 결정 이전에 설치된 릴리스(`previous`, 비상 릴리스)도 처음 쓰일 때 봉인된다. 이미 봉인된 릴리스에는 아무 변화가 없다.
   - `prune` 은 지울 트리에만, 봉인한 것과 같은 승인 코드(`set_immutable(path, False)`)로 `chattr -R -i` 한 뒤 지운다.
   - 봉인을 못 읽는 파일시스템이면 승인은 그 이름으로 거부한다(조용히 통과하지 않는다).

## 대안과 선택 이유

- **읽기 전용 bind mount(`mount --bind` + `remount,ro`)**: 프로세스 단위가 아니라 마운트 단위라 root 의 쓰기도 막지만,
  재부팅 후 다시 걸어 줄 mount 유닛이 릴리스마다 필요하고, 마운트를 잡은 프로세스가 있으면 prune 의 umount 가 실패하며,
  승인이 "이 경로가 ro 마운트 아래인가"를 inode 단위로 확인할 수 없다. 상태가 파일 밖(마운트 테이블)에 산다.
- **squashfs/erofs 이미지로 릴리스를 마운트**: 진짜 불변이지만 빌드·검증·rollback 경로 전체(승인이 파일을 하나씩 읽는
  구조)를 다시 만들어야 한다. 이 규모의 문제에 비해 크다.
- **각 작성자를 고치기(`PYTHONDONTWRITEBYTECODE`, `-B`)**: 유지하지만(이중 방어) 다음 작성자를 막지 못한다.
- **`chattr +i` 채택**: inode 속성이라 재부팅에도 남고, `CAP_LINUX_IMMUTABLE` 없이는 root 도 쓰기·rename·하드링크·
  삭제·chmod 를 못 한다(하드링크 금지는 승인의 `st_nlink == 1` 검사도 커널이 보장하게 한다). 모드·소유자·링크 수는
  바뀌지 않으므로 기존 `verify_release` 검사와 빌드(릴리스를 읽기만 함)는 그대로다. 승인이 항목마다 직접 확인할 수
  있다. ext4·xfs·btrfs 가 지원한다. 운영 호스트의 `/opt` 파일시스템을 이 변경에서 원격으로 확인하지 않았다:
  지원하지 않으면 첫 배포의 `seal` 이 이름을 대고 거부하고(새 릴리스는 활성화되지 않음, 0단계 이전 릴리스 그대로),
  그때는 위 bind mount 로 간다.

## Consequences

- 같은 부류의 세 결함이 구조적으로 막힌다: 목록과 판정 규칙은 한 파일에만 있고, 릴리스에 쓰는 일은 누가 하든 커널이
  `EPERM` 으로 거부한다(파이썬은 바이트코드 쓰기 실패를 조용히 무시하므로 root import 는 그냥 돈다).
- 승인 `verify-current`(모든 작업의 ExecStartPre)는 봉인되지 않은 current 를 거부한다. 이 변경이 처음 배포될 때 새
  릴리스는 설치 시 봉인되므로 작업은 계속 돈다. 배포 중(0단계로 DAG 가 멈춘 동안) 운영자가 손으로 시작한 작업은
  아직 봉인되지 않은 옛 current 에서 거부될 수 있다.
- 릴리스를 손으로 고치거나 지우려면 먼저 `chattr -R -i` 가 필요하다. 그것이 의도다. `artifacts/<sha>` 는 이번에
  봉인하지 않았다: 런북이 이미지 재빌드 때 그것을 옆으로 옮기라고 하고(불변 디렉터리는 옮길 수 없다), 내용은 이미
  sha256 으로 묶여 있다.
- 시험: `test_job_units`(정의·질의 형태·답 없음·등록 목록·`NothingElseAsks` 와 그 스캔의 거부 증명),
  `test_foundation_release_admission`(봉인 요구·항목별 거부·봉인 전 확인·chattr 인자·inode 플래그 읽기·`running-jobs`
  종료 코드), `test_foundation_autodeploy`(배포가 승인의 `running-jobs` 로 기다림·답 없으면 빌드 안 함, 자동 배포의
  상태별 판정·systemd 무응답), `test_by_pnu_pack_operator`(모든 거부 자리의 상태별 판정·systemd 무응답 70),
  `test_start_scheduled_job`(실행 대기가 정의를 따름), `foundation-release-test.sh`(설치 후 봉인·봉인이 풀린 릴리스
  거부·prune 의 봉인 해제, 그리고 봉인 안 하는 install·해제 안 하는 prune 변이가 거부됨).
