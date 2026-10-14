# ADR 0241: 호스트는 OS 를 시스템 디스크에, Docker 와 containerd 를 데이터 디스크에 둔다

- Status: Accepted
- Date: 2026-10-14

## Context

ai-server 의 시스템 디스크(217GB)가 2026-10-14 에 92%(여유 19GB)였다. 10-02 에 루트 디스크가 두 번 0 이 된 사고와
원인이 같다: 데이터와 함께 자라는 것이 시스템 디스크에 있다. 데이터 디스크(`/data`, 1.8TB)에는 1.2TB 가 비어 있었다.

같은 날 실측(읽기 전용 `du`):

| 트리 | 크기 | 담는 것 |
|---|---|---|
| `/var/lib/docker` | 21.4GB | 볼륨 20.9GB(그중 운영 Postgres `pgdata` 20GB), 컨테이너 0.5GB |
| `/var/lib/containerd` | 105GB | 이미지 스냅숏 94.9GB, 콘텐츠 10.1GB |

Docker 29 는 이미지·스냅숏·빌드 캐시를 containerd 이미지 저장소에 둔다(`driver-type io.containerd.snapshotter.v1`).
Docker 의 data-root 만 옮기면 21GB 만 빈다. 둘 다 옮겨야 원인이 사라진다.

선례: 시스템 볼륨과 데이터 볼륨을 나누는 것은 일반 관행이다. Docker 문서는 data-root 를 별도 볼륨으로 옮기는 절차(엔진
정지 → 복사 → `daemon.json` 의 `data-root`)를 안내하고, containerd 는 `config.toml` 의 `root` 로 같은 일을 한다. 클라우드
노드 이미지들도 컨테이너 런타임의 저장소를 별도 디스크에 둔다.

## Decision

1. 배치의 정본은 `platforms/foundation-platform/config/host-layout.contract.json` 하나다: 데이터 디스크 마운트, Docker
   data-root, containerd root, 여유 공간 배수, 시간 상한, 확인할 readiness URL.
2. `scripts/ops/host-docker-data-root.sh` 가 그 배치를 만든다(호스트 설정을 코드로).
   - `dry-run`(root 불필요), `run`, `rollback`, `delete-old`. 이미 옮겨진 호스트에서는 아무것도 하지 않는다.
   - 사전 점검: 자동 배포·등록된 작업·Spark 컨테이너가 없을 것, 데이터 디스크에 실측 크기의 2배 여유.
   - 서비스가 도는 채로 한 번 복사하고(`rsync -aHAXx --numeric-ids --delete`), 엔진을 멈춘 뒤 바뀐 것만 다시 복사하고,
     세 번째 패스가 아무것도 복사하지 않는지 확인한다. 서비스 중단은 두 번째 복사와 재시작·확인 동안뿐이다.
   - `daemon.json` 은 다른 키를 모두 보존하고 `data-root` 만 넣는다. `config.toml` 은 다른 줄을 모두 보존하고 최상위
     `root` 만 넣는다. 두 편집은 `host_docker_layout.py` 에 있고 시험이 고정한다.
   - 두 유닛에 `RequiresMountsFor=<데이터 디스크>` 를 둔다. 디스크가 붙기 전에 엔진이 빈 경로로 뜨지 않는다.
   - 확인: 엔진 root, 전에 돌던 컨테이너 전부, 전에 healthy 였던 컨테이너 전부, readiness URL 전부. 하나라도 실패하면
     스스로 되돌린다.
   - DAG: 실행 전에 unpaused 였던 `foundation_` DAG 만 기록해 멈추고, 끝나면 그것만 consumers-first 순서로 재개한다.
     설계상 멈춰 둔 DAG 는 건드리지 않는다.
   - 옛 트리는 `<트리>.old` 로 남는다. 소유자가 확인한 뒤 `delete-old` 가 지운다.
3. 새 호스트는 컨테이너를 띄우기 전에 같은 스크립트를 돈다(런북 `host-layout.md`). `/var/lib/docker` 를 쓰는 호스트는
   다시 생기지 않는다.
4. `host_retention.py`(루트 ADR-0198)는 시스템 디스크가 아니라 이미지 저장소가 있는 디스크의 여유를 잰다.

## Consequences

- 시스템 디스크에서 약 126GB 가 빈다(옛 트리를 지운 뒤).
- 운영 Postgres `pgdata`(20GB)가 SSD 에서 HDD 로 간다. 2026-09-09 에 "서빙 속도 때문에 SSD 잔류"로 정했던 것과 반대다.
  그 뒤 카탈로그 서빙은 R2 엣지로 넘어갔다(루트 ADR-0194). 필요하면 그 볼륨만 SSD 경로로 되돌리는 것은 별도 결정이다.
- 실행에는 root 가 필요하다. 소유자가 한 줄로 실행한다(런북).

## 기각한 대안

- **Docker data-root 만 옮기기**: 105GB 가 시스템 디스크에 남는다(Context).
- **`/var/lib/docker` 를 데이터 디스크로 가리키는 심링크·바인드 마운트**: 경로가 두 이름을 갖고, 패키지 갱신이나 복구 때
  어느 쪽이 진짜인지가 설정 파일 밖으로 나간다. 엔진이 읽는 설정 한 곳(`data-root`, `root`)에 둔다.
- **빌드 캐시만 줄이기**: 이미 ADR-0198 이 하고 있다. 자라는 것이 시스템 디스크에 있는 한 다시 찬다.
