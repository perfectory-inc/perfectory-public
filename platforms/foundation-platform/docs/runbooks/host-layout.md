---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-14
---

# 호스트 디스크 배치

결정: [루트 ADR-0241](../../../../docs/adr/0241-the-host-keeps-the-os-on-its-system-disk-and-docker-on-its-data-disk.md).
배치의 정본은 [`config/host-layout.contract.json`](../../config/host-layout.contract.json) 이다. 이 문서는 경로를 다시 적지 않는다.

| 디스크 | 담는 것 |
|---|---|
| 시스템 디스크(`/`) | OS, 패키지, `/opt/foundation-platform` 릴리스, `/etc` 설정, journald |
| 데이터 디스크(계약의 `data_mount`) | Docker data-root(볼륨·컨테이너), containerd root(이미지·스냅숏·빌드 캐시), 레이크하우스 작업 공간, Postgres 테이블스페이스 |

Docker 29 는 이미지를 containerd 이미지 저장소에 둔다. 그래서 Docker 의 data-root 만 옮기면 이미지와 빌드 캐시는
시스템 디스크에 남는다. 둘 다 옮긴다.

## 새 호스트

컨테이너를 하나도 띄우기 전에, Docker 를 설치하고 데이터 디스크를 마운트한 직후에 실행한다. 빈 트리를 옮기는 것이라
몇 초면 끝난다. 이것을 건너뛰면 엔진이 시스템 디스크(`/var/lib/docker`, `/var/lib/containerd`)에 쌓기 시작한다.

```bash
sudo bash /opt/foundation-platform/current/scripts/ops/host-docker-data-root.sh dry-run
sudo bash /opt/foundation-platform/current/scripts/ops/host-docker-data-root.sh run
sudo bash /opt/foundation-platform/current/scripts/ops/host-docker-data-root.sh delete-old
```

## 이미 도는 호스트를 옮기기

1. `dry-run`(root 불필요): 사전 점검, 크기, 데이터 디스크 여유, 멈췄다가 되살릴 DAG, 쓸 `daemon.json`·`config.toml`.
   지금 돌릴 수 없으면 이유를 말하고 0 이 아닌 코드로 끝난다.
   실행 직전에는 **root 로** 한 번 더 돌린다: `ssh -t ai-server "sudo bash /opt/foundation-platform/current/scripts/ops/host-docker-data-root.sh dry-run"`.
   root 만 하는 점검(release admission, `du`)과 실행이 기록하는 것(돌던 컨테이너, healthy 목록, 배포자 계정으로
   부르는 Airflow DAG 목록)을 실행과 같은 코드로 임시 디렉터리에 기록해 본다. 2026-10-15 첫 실행은 이 기록 단계에서
   멈췄다(healthy 목록 함수의 종료 코드).
2. 소유자가 자기 PowerShell 에서 한 줄로 실행한다(root 가 필요하다).

   ```powershell
   ssh -t ai-server "sudo bash /opt/foundation-platform/current/scripts/ops/host-docker-data-root.sh run"
   ```

   - 자동 배포가 돌거나, 등록된 작업이 돌거나, Spark 컨테이너가 있으면 아무것도 바꾸지 않고 거부한다.
   - 실행 동안 `/etc/foundation-platform/autodeploy.off` 를 잡아 자동 배포가 끼어들지 못하게 하고, 끝나면 돌려준다.
   - 서비스가 도는 채로 한 번 복사한다(긴 부분). 그다음 엔진을 멈추고 바뀐 것만 다시 복사한다. 서비스가 멈추는 것은
     이 두 번째 복사와 재시작·확인 동안이다.
   - 확인: 엔진의 root, 전에 돌던 컨테이너 전부, 전에 healthy 였던 컨테이너 전부, 계약의 `readiness` URL 전부.
   - 확인이 하나라도 실패하면 스스로 되돌린다(설정 파일 복원, 옛 트리 이름 복원, 재시작, DAG 재개).
3. 소유자가 서비스를 직접 확인한 뒤에 `delete-old` 로 옛 트리(`<트리>.old`)를 지운다. 그 전까지는 시스템 디스크에
   남아 있어 되돌릴 수 있다: `sudo bash …/host-docker-data-root.sh rollback`.

실행 기록(전에 돌던 컨테이너, 재개할 DAG, 원래 설정 파일)은 `/var/lib/foundation-host-layout/<시각>` 에 남는다.

## 이후

- `host_retention.py` 는 이미지 저장소가 있는 디스크의 여유를 잰다(계약의 containerd root).
- 일일 spark 작업과 백업 타이머를 피하는 실행 창은 UTC 13:00–17:00(KST 22:00–02:00)이다. 사전 점검이 실제로 막는다.
