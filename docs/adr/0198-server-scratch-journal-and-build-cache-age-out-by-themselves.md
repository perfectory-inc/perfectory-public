# ADR 0198: 서버의 임시 작업·저널·빌드 캐시는 스스로 사라진다

- Status: Accepted
- Date: 2026-10-12

## Context

2026-10-12 ai-server 에서 측정한 사실이다.

1. **데이터 디스크의 `/data/foundation-platform/claude-scratch` 가 198GB 까지 자라 손으로 지웠다.** 에이전트와
   운영자가 측정 결과, 재적재 중간물, 옛 릴리스 빌드 트리를 두는 곳인데, 이것을 지우는 장치가 없었다.
2. **루트 디스크가 86%(217GB 중 176GB, 남은 30GB)였다.** 무엇이 채웠나:

   | 항목 | 크기 | 회수 가능 |
   | --- | --- | --- |
   | Docker BuildKit 빌드 캐시 | 52GB | 52GB (사용 중 0) |
   | Docker 이미지 64개 | 43.5GB | 16.7GB |
   | Docker 볼륨 48개 | 23.8GB | 1.4GB |
   | Docker 컨테이너 | 5.4GB | 0.06GB |
   | `/home` | 12GB | (사람 소유) |
   | `/opt` (릴리스 4개 1.5GB 포함) | 9.3GB | 릴리스 정리가 이미 N개만 남김 |
   | `/tmp` | 4.7GB | Ubuntu 기본 30일 정리가 이미 있음 |
   | systemd 저널 | 2.9GB (23일치) | 기본 상한 4GB |

   빌드 캐시는 릴리스 빌드(ADR-0134·0137)마다 쌓이고 아무것도 지우지 않았다. 릴리스 디렉터리 정리
   (`foundation-release.sh prune`, `keep_newest`)는 이미 동작하고 있었다.
3. **컨테이너 로그 상한(PR #309)은 compose 가 띄운 컨테이너 36개 전부에 있다**(50m×3). 없는 것은 손으로 띄운
   `martin-static`·`martin-complex` 둘과 끝난 일회성 컨테이너들이다(runbook `host-memory-budget.md` 가 이미
   손 컨테이너로 적어 둔 것).

## Decision

1. **스크래치는 3일 뒤 사라진다.** `infra/host/tmpfiles.d/foundation-scratch.conf` 의 규칙 한 줄
   `e /data/foundation-platform/claude-scratch - - - mM:3d` 를 OS 의 일일 `systemd-tmpfiles-clean.timer` 가
   집행한다. `e` 는 디렉터리를 만들거나 소유를 바꾸거나 지우지 않고 안을 정리만 한다. `mM:` 은 수정 시각만
   보므로 읽기만 한 파일도 3일 뒤 사라진다. 운영 상태(`/var/lib/foundation-*`, by-PNU 굽기 작업 파일,
   레이크하우스, 백업, `/opt` 의 릴리스)는 규칙에 이름도, 포함 관계도 없다 — `test_host_retention.py` 가
   어느 규칙이든 그것들을 이름 짓거나 품으면 거부한다.
2. **저널은 2GB, 남은 공간 20GB 를 지킨다.** `infra/host/journald.conf.d/50-foundation-retention.conf`.
3. **빌드 캐시는 최신 20GB 만, 매달린 이미지는 매일 지운다.** `foundation-host-retention.timer`(매일 04:40)가
   `scripts/ops/host-retention.sh` 를 부른다: `docker builder prune --max-used-space 20GB` 와
   `docker image prune`(태그 없는 것만). `--all`·`system prune`·`volume prune` 은 쓰지 않는다 — 컨테이너가
   없는 태그 이미지는 굽기 이미지가 다음 실행을 기다리는 모습이고, 볼륨은 상태다. 실행마다 루트 디스크의
   전후 여유 공간을 저널에 남긴다.
4. **배포가 이것을 설치한다.** `foundation-release.sh timers`(자동 배포가 매 배포마다 부른다)가
   `infra/host/<디렉터리>/*.conf` 를 `/etc` 의 같은 자리로 설치하고, 스크래치 규칙은 그 자리에서 한 번 집행하고,
   저널 설정이 바뀌었을 때만 journald 를 재시작하고, 타이머를 켠다. 규칙은 저장소 한 곳에만 있다.
5. 시험: `orchestration/tests/test_host_retention.py`. 리눅스 CI 에서는 실제 `systemd-tmpfiles --clean` 으로
   나흘 전에 쓰고 오늘 읽은 파일은 지워지고 오늘 쓴 파일은 남는 것을, 가짜 `docker` 로 정리 명령 둘만 나가는
   것을 확인한다. 같은 확인을 서버(systemd 255, 일반 사용자)의 임시 디렉터리에서 손으로 돌려, 규칙을 10일로
   망가뜨리면 파일이 남는 것까지 보았다.

검토한 대안:

- **Docker 데몬 설정(`daemon.json` 의 BuildKit GC·기본 로그 상한).** 근본적이지만 적용하려면 dockerd 재시작이
  필요하고, 운영 컨테이너 전부가 함께 재시작된다. 자동 배포가 할 일이 아니다. 매일 정리 명령은 재시작 없이
  같은 상한을 지킨다.
- **배포 스크립트 끝에서 지우기.** 배포가 없는 날에는 아무것도 지워지지 않고, 스크래치는 배포와 무관하게 자란다.
  OS 의 tmpfiles 타이머와 자체 타이머가 배포 주기와 독립이다.
- **스크래치를 `/tmp` 아래로 옮긴다.** `/tmp` 는 루트 디스크(남은 30GB)라 198GB 를 담지 못한다.

## Consequences

- 이 PR 이 병합되어 자동 배포된 첫 실행에서 빌드 캐시 약 32GB 가 회수될 것으로 본다(52GB → 20GB). 매달린
  이미지는 실제 회수량이 실행 기록에 남는다.
- 3일보다 오래 남겨야 할 측정 결과는 스크래치가 아니라 R2·레이크하우스·저장소에 둔다.
- 남은 일: 손으로 띄운 `martin-*` 두 컨테이너는 로그 상한이 없다(ADR-0110 이 정적 PMTiles 로 대체 중).
  태그가 붙은 에이전트 실험 이미지(`foundation-task77*`, `perfectory-verify:task*` 등 약 10GB)는 이름으로
  지울지 사람이 정한다. 데몬 수준 기본 로그 상한은 다음 계획된 dockerd 재시작 때 넣는다.
