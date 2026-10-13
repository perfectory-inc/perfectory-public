# ADR 0198: 서버의 임시 작업·저널·쓰지 않는 이미지는 스스로 사라진다

- Status: Accepted
- Date: 2026-10-13

## Context

ai-server 에서 측정한 사실이다.

1. **데이터 디스크의 `/data/foundation-platform/claude-scratch` 가 198GB 까지 자라 손으로 지웠다**(2026-10-12).
   에이전트와 운영자가 측정 결과, 재적재 중간물, 옛 릴리스 빌드 트리를 두는 곳인데, 이것을 지우는 장치가 없었다.
2. **루트 디스크가 86–89%(217GB 중 여유 24–30GB)였다.** 무엇이 채웠나(`docker system df`, 2026-10-13):

   | 항목 | 크기 | Docker 가 말하는 회수 가능 |
   | --- | --- | --- |
   | Docker 빌드 캐시 | 57.9GB | **0.5GB** |
   | Docker 이미지 64개 | 43.5GB | 16.7GB(컨테이너가 쓰지 않는 것) |
   | Docker 볼륨 48개 | 23.8GB | 1.4GB |
   | `/home` | 12GB | (사람 소유) |
   | `/opt` (릴리스 4개 1.5GB 포함) | 9.3GB | 릴리스 정리가 이미 N개만 남김 |
   | `/tmp` | 4.7GB | Ubuntu 기본 30일 정리가 이미 있음 |
   | systemd 저널 | 2.9GB (23일치) | 기본 상한 4GB |

   빌드 캐시가 가장 커 보이지만, 이 호스트의 Docker 29 는 containerd 이미지 저장소를 쓰고 빌드 캐시와 이미지가
   같은 층을 공유한다. 캐시를 지워도 이미지가 그 층을 붙잡고 있어 실제로 풀리는 것은 0.5GB 뿐이다. 공간을
   돌려주는 것은 이미지 정리다. 아무도 이미지를 지우지 않았다: 에이전트 실험 이미지(`perfectory-verify:task*`,
   `foundation-task77*`), 고정되지 않은 옛 기반 이미지(`python:3.12*`, `rust:1.98-bookworm`, `amazon/aws-cli` 세 판)가
   쌓여 있었다. 릴리스 디렉터리 정리(`foundation-release.sh prune`, `keep_newest`)는 이미 동작한다.
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
3. **아무것도 필요로 하지 않는 이미지를 매일 지운다.** `foundation-host-retention.timer`(매일 04:40)가
   `scripts/ops/host_retention.py` 를 부른다. 이미지는 다음 넷을 **모두** 만족할 때만 지운다:
   어떤 컨테이너(멈춘 것 포함)도 쓰지 않는다 / 만든 지 7일이 지났다 / 릴리스 정리가 소유한 저장소
   (`foundation-outbox-publisher`, `foundation-tippecanoe` — 비상 릴리스 것 포함)가 아니다 / 호스트가 배포하는
   체크아웃(`/opt/perfectory-control/current`)의 어느 파일도 그 태그나 다이제스트를 적지 않는다. 마지막 조건이
   compose·계약·작업 스크립트가 고정한 이미지를 전부 지킨다. 체크아웃이 없으면 아무것도 판정하지 않고 실패한다.
   볼륨·컨테이너·네트워크는 건드리지 않는다. 같은 나이의 쓰지 않는 빌드 캐시 기록도 지우는데, 이것은 Docker 가
   말하는 0.5GB 이지 58GB 가 아니다. 실행마다 지운 이미지와 루트 디스크의 전후 여유 공간을 저널에 남긴다.
   유닛은 `ProtectHome`·`ProtectSystem=strict` 아래에서 돌기 때문에 Docker CLI 설정 디렉터리를 유닛의 사설
   `/tmp` 로 준다(`DOCKER_CONFIG=/tmp/docker-config`).
4. **배포가 이것을 설치한다.** `foundation-release.sh timers`(자동 배포가 매 배포마다 부른다)가
   `infra/host/<디렉터리>/*.conf` 를 `/etc` 의 같은 자리로 설치하고, 스크래치 규칙은 그 자리에서 한 번 집행하고,
   저널 설정이 바뀌었을 때만 journald 를 재시작하고, 타이머를 켠다. 규칙은 저장소 한 곳에만 있다.
5. 시험: `orchestration/tests/test_host_retention.py`. 리눅스 CI 에서는 실제 `systemd-tmpfiles --clean` 으로
   나흘 전에 쓰고 오늘 읽은 파일은 지워지고 오늘 쓴 파일은 남는 것을 확인한다. 이미지 정리는 지킬 이유
   다섯(사용 중·7일 미만·릴리스 소유·고정 태그·고정 다이제스트)이 각각 지키는지, 실제로 나가는 명령이 고른
   이미지의 `image rm` 과 나이 든 캐시 정리뿐인지를 확인한다. 각 조건을 일부러 빼면 시험이 실패한다.

검토한 대안:

- **`docker builder prune --max-used-space 20GB`**(이 ADR 의 첫 초안). 공유 층 때문에 실제로 풀리는 것이 거의
  없다는 것을 측정이 보였다.
- **`docker image prune --all --filter until=…`.** 이름으로 지킬 방법이 없어 컨테이너 없이 다음 실행을 기다리는
  고정 이미지(Spark, GDAL 등)까지 지운다. 다음 작업이 운영 중에 다시 내려받아야 한다.
- **Docker 데몬 설정(`daemon.json` 의 GC·기본 로그 상한).** 적용하려면 dockerd 재시작이 필요하고, 운영 컨테이너
  전부가 함께 재시작된다. 자동 배포가 할 일이 아니다.
- **스크래치를 `/tmp` 아래로 옮긴다.** `/tmp` 는 루트 디스크라 198GB 를 담지 못한다.

## Consequences

- 2026-10-13 같은 스크립트를 서버에서 한 번 손으로 돌린 실측: 이미지 21개(64 → 43) 삭제,
  **루트 디스크 여유 24.7GB → 34.2GB(8.88GiB 회수), 89% → 85%.** 빌드 캐시는 기록 321개·57.9GB 그대로였다
  — 위 판단의 확인이다. 남은 회수 가능 이미지 11.7GB 는 저장소가 고정한 것이라 일부러 남긴다.
- 3일보다 오래 남겨야 할 측정 결과는 스크래치가 아니라 R2·레이크하우스·저장소에 둔다. 남겨야 할 에이전트
  이미지는 7일 안에 저장소에 고정하거나 다시 만든다.
- 남은 일: 손으로 띄운 `martin-*` 두 컨테이너는 로그 상한이 없다(ADR-0110 이 정적 PMTiles 로 대체 중).
  데몬 수준 기본 로그 상한은 다음 계획된 dockerd 재시작 때 넣는다.
