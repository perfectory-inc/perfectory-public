# ADR 0504: 쿼리 하나가 Trino 를 컨테이너 상한 밖으로 밀지 못하고, Trino 는 릴리스를 따라간다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §6(모든 컨테이너에 상한),
  [ADR-0343](./0343-an-always-on-service-holds-only-what-it-is-using.md) §2(상한과 질의 한도는 측정으로 정한다 — 이 ADR 이 새 측정으로 바꾼다),
  [ADR-0241](./0241-the-host-keeps-the-os-on-its-system-disk-and-docker-on-its-data-disk.md)(자라는 것은 데이터 디스크에)
- 관련 사건: 2026-11-11 01:25Z ai-server 에서 표본을 뜨지 않은 측정 쿼리 하나가 돌던 중, 커널이 Trino 를 죽였다
  (`Memory cgroup out of memory: Killed process … trino-server-co … anon-rss:16550188kB`, `constraint=CONSTRAINT_MEMCG`).
  `query.max-memory=8GB` 는 발동하지 않았다. Trino 는 01:28Z 에 다시 떴고 그동안의 질의는 모두 실패했다.

## Context

측정은 모두 2026-11-11 ai-server 에서 읽기만 했다(`journalctl -k`, `docker inspect`, `/proc/1/status`, `/proc/1/smaps`, `jcmd`).

| 무엇 | 값 |
|---|---|
| 컨테이너 상한 | 16g (`compose.lakehouse.yml`) |
| 힙 상한 | `MaxRAMPercentage=80` → 12.81GiB (`-XX:MaxHeapSize=13757317120`) |
| 죽을 때 anon RSS | 16.55GB → 힙 밖 최소 **2.97GiB** |
| 재기동 뒤 유휴 | RSS 3.99GB, 힙 커밋 0.98GB → 힙 밖 **2.9GiB** |
| 힙 밖의 내용(유휴) | 64MB 경계에서 시작하는 익명 매핑 164개 = 2.5GB. glibc malloc 아레나다(기본 상한 8×코어 = 160, ai-server 20코어) |

Trino 의 메모리 회계(질의 한도, low-memory killer)는 **힙만** 본다. 힙 상한이 상한의 80% 라 힙 밖 몫은 3.2GiB 뿐인데,
유휴 때 이미 2.9GiB 가 아레나·스레드·GC 구조에 있었다. 질의가 힙을 채우자 합이 16g 를 넘었고, 그 순간 Trino 의 어떤 한도도
"넘었다" 고 보지 못했다. 즉 원인은 질의 한도가 아니라 **힙과 상한 사이의 몫**이다.

두 번째 원인: Trino 컨테이너는 2026-10-30 에 만들어진 뒤 배포가 한 번도 다시 만들지 않았다. 마운트한 설정 파일은
`/opt/foundation-platform/current/…` 경로라 재시작 때만 새 릴리스 파일을 읽고, compose 의 상한·환경·마운트 변경은 재생성
때만 들어간다. 즉 메모리 고침을 병합해도 다음 사고 뒤에야 적용된다.

Trino 문서는 힙을 노드 메모리의 70–85% 로 두라 하고(`installation/deployment.md` JVM config),
`query.max-memory-per-node + memory.heap-headroom-per-node < 힙` 을 요구한다(`admin/properties-resource-management.md`).
Hadoop 은 같은 현상(컨테이너 상한 안의 JVM 이 아레나로 자람)에 `MALLOC_ARENA_MAX=4` 를 기본으로 둔다(HADOOP-7154).

## Decision

1. **힙은 상한의 70%**(11.2GiB, 문서 범위의 아래 끝). 힙 밖 몫 4.8GiB 는 사고 때 힙 밖(2.97GiB)의 1.5배 이상이다.
2. **glibc 아레나 상한 `MALLOC_ARENA_MAX=4`**(compose 환경). 유휴 2.5GB 의 원천을 막는다.
3. **질의 한도는 힙 안에서 맞는다**: 노드당 7GB + 추적 밖 여유 3GB = 10GiB < 11.2GiB. `query.max-memory=7GB`(노드 하나),
   `query.max-total-memory=8GB`. 측정된 가장 큰 질의(09-25, 7.0GB)는 들어간다.
4. **넘치는 조인·집계·정렬·윈도는 데이터 디스크로 spill**(`spill-enabled`, `/spill` = `config/host-layout.contract.json`
   `trino_spill`, 노드 200GB·질의 100GB). 실패 대신 느려진다. 시스템 디스크에는 쓰지 않는다.
5. **리소스 그룹**(`resource-groups.json`): `trino-cli`(운영자·에이전트의 `docker exec … trino`)는 `global.adhoc`,
   동시 1개·메모리 50%; 나머지(서비스)는 `global.service`, 동시 5개. 세션 관리자가 adhoc 에 `query_max_memory=5GB`,
   `query_max_total_memory=6GB`, `query_max_execution_time=30m` 을 준다. 사람의 임시 질의 하나가 서비스 질의를 밀어내지
   못하고, 둘이 겹쳐 힙을 나눠 갖지도 않는다.
6. **Trino 는 릴리스를 따라간다**(`foundation-release.sh trino`, 배포 10단계, 카탈로그 뒤). compose 설정 해시(Compose 가 스스로
   비교하는 값; `trino_catalog_dir` 을 넣으면 운영 라벨 `48ea061f…` 이 그대로 재현된다) 또는 마운트한 파일의 바이트가 다르면
   다시 만들고, 180초 안에 `SELECT 1` 이 답하지 않으면 배포를 실패로 낸다. 같으면 아무것도 건드리지 않는다.
7. 가드: `orchestration/tests/test_trino_memory_budget.py` 가 1–6 을 숫자로 묶는다(이 사건의 수치를 상수로 둔다).

## 증명

로컬에서 같은 이미지(trinodb/trino:481, 같은 다이제스트)를 같은 비율로 줄여(상한 4g, 힙 70%, 노드당 1.5GB + 여유 0.75GB,
같은 리소스 그룹·세션 파일, `MALLOC_ARENA_MAX=4`) 띄웠다:

| 질의 | 결과 | Trino |
|---|---|---|
| `SHOW SESSION` (CLI) | `query_max_execution_time=30m` — 세션 관리자가 adhoc 에 적용 | 살아 있음 |
| `system.runtime.queries` | 모든 CLI 질의가 `[global, adhoc]` | 살아 있음 |
| `array_agg` 한 묶음, sf1, `query_max_memory=200MB` | `Query exceeded distributed user memory limit of 200MB` | 살아 있음 |
| `array_agg(comment‖comment)` 한 묶음, sf3 (노드 한도 초과) | 그 질의만 실패 | OOMKilled=false, 재시작 0, 직후 `SELECT 1` 응답 |

운영에서는 배포 뒤 같은 질의 모양을 r2 표본으로 다시 돌려 확인한다(결과는 PR 에 남긴다).

## Consequences

- 힙이 12.8 → 11.2GiB 로 줄어 질의 하나의 한도가 8 → 7GB 다. 그 사이 질의는 spill 로 느려지거나, spill 못 하는 연산이면 실패한다.
- 임시 질의는 한 번에 하나다. 둘째는 줄을 선다(최대 20).
- 배포마다 Trino 의 설정을 확인하고, 다르면 그 순간 돌던 질의는 끊긴다(재시도는 호출자 몫).
- `trino_catalog_dir` 이 계약에 들어왔다. 운영 호스트가 다른 곳에 두면 재생성된 Trino 가 뜨지 않고 배포가 실패한다(조용히 r2 없이 뜨지 않는다, `create_host_path: false`).
