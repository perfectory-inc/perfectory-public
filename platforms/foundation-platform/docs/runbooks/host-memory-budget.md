---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-01
---

# ai-server 메모리 예산

[ADR-0118](../../../../docs/adr/0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §6:
모든 컨테이너는 상한을 갖고, 상한의 합이 물리 메모리(62GB)를 넘지 않는다.

## 어디에 적혀 있나

| 무엇 | 정본 |
|---|---|
| 서비스마다의 상한 | 각 compose 파일의 `mem_limit` (여기에는 다시 적지 않는다) |
| 어떤 compose 묶음이 ai-server 에서 도는지, 어떤 profile 로, 호스트 몫과 상한 밖에 있는 것 | [`tools/host-memory-budget.contract.json`](../../../../tools/host-memory-budget.contract.json) |
| 합계 검사 | [`scripts/guard/every-container-has-a-memory-cap.sh`](../../../../scripts/guard/every-container-has-a-memory-cap.sh) |

지금의 합계는 가드에게 묻는다.

```bash
PERFECTORY_MEMORY_BUDGET_VERBOSE=1 bash scripts/guard/every-container-has-a-memory-cap.sh
```

## 셈법

- 계속 떠 있는 서비스는 상한 그대로 더한다.
- 한 번 돌고 끝나는 작업(`restart: "no"`, 또는 다른 서비스가 `service_completed_successfully` 로 기다리는 것)은
  배포나 타이머가 하나씩 돌리므로 가장 큰 하나만 더한다.
- 호스트가 켜지 않는 profile 의 서비스는 더하지 않는다.
- 여기에 `host_reserved`(OS, Docker, 컨테이너 밖에서 도는 발행기·타이머 실행 파일, Open WebUI·LiteLLM)를 더한다.
- compose 파일이 새로 생기면 계약이 그 파일을 어디에 둘지(ai-server 묶음, 호스트 밖, 범위 밖) 정하기 전까지 가드가 막는다.

## 큰 상한의 근거

| 서비스 | 상한 | 근거 |
|---|---|---|
| Trino | 16g (힙 12.8GB, 질의당 8GB) | 보관된 질의 100개 중 가장 큰 것이 7.0GB(2026-09-25). 질의 한도는 `infra/lakehouse/trino/config.properties` |
| Spark | 20g (드라이버 힙 16g) | 아래 "Spark 측정" |
| DataHub 7개 | 합계 7.1g (+ 업그레이드 작업 1g) | 실측 약 4.5GB. actions 는 137MB 를 써서 512m |
| Airflow 5개 | 합계 2.8g (+ DB 이전 작업 1g) | 2026-10-01 실측 합계 약 0.75GB |
| Foundation PostgreSQL | 4g | `shared_buffers` 160MB, 실측 165MB. 대량 적재 때의 작업 메모리와 페이지 캐시를 위한 여유 |

### Spark 측정

적재기의 기본 드라이버 메모리는 24g 였지만 근거가 남아 있지 않다. 8월 30일 이후의 실제 적재들은 16g 로 돌았다.
2026-10-01 에 전국 필지(255개 객체, 3,986만 행)와 용도지역(17개 파일, 2.48억 행)을 검사 모드(쓰지 않음)로
드라이버 16g 에서 다시 돌려 2초마다 컨테이너 사용량을 쟀다.

| 적재 | 행 | 결과 | 걸린 시간 (이전 24g) | 최대 사용량 |
|---|---:|---|---|---:|
| 전국 필지 | 39,861,511 | 16묶음 전부 통과 | 10분 (19분) | 17.1GiB |
| 용도지역 | 247,584,668 | 17묶음 전부 통과 | 29분 (42분) | 3.1GiB |

용도지역은 흘려 보내며 처리해 힙을 거의 쓰지 않는다. 필지는 힙 16g 를 채우고 그 밖에서 1GiB 남짓을 더 쓴다.
그래서 적재기 기본값은 16g, 컨테이너 상한은 20g 다. 더 큰 적재가 상한에 걸리면 컨테이너가 137 로 끝나고,
그때는 이 표와 상한, 호스트 합계를 함께 고친다.

## 상한 밖에 있는 것

| 무엇 | 상태 |
|---|---|
| `ollama.service` | systemd 서비스, `MemoryMax=infinity`. 8GB GPU 를 넘는 모델은 호스트 메모리로 넘친다(가장 큰 모델 23GB). 2026-10-01 그대로 두기로 결정 — 큰 모델과 Spark 적재가 겹치면 호스트가 바닥날 수 있고, 막는 장치는 없다 |
| `martin-complex`, `martin-static` | compose 가 아니라 손으로 띄운 컨테이너. 상한이 없고 둘 다 unhealthy. 지도 서빙은 R2 정적 타일로 옮겨졌다(ADR-0110). 정리할지 compose 로 옮길지 결정이 남았다 |
| `foundation-platform-runtime-redis-1` | 7월에 만들어진 뒤 compose 에서 이름이 `valkey` 로 바뀌어 남은 컨테이너. 읽는 코드가 없다 |

## 서버 반영

상한은 컨테이너를 다시 만들어야 걸린다. 묶음마다 해당 배포 경로로 `up -d` 한다. PostgreSQL 이 다시 만들어지는 동안 API 가
잠깐 멈추므로 타이머가 돌지 않는 시간에 한다. Trino·Spark 의 compose 는 원격 적재 뿌리
`/home/perfectory/foundation-platform-compute` 에 따로 복사돼 있으니([remote-lakehouse-job-runner](./remote-lakehouse-job-runner.md))
그 사본도 같은 판으로 맞춘다.

반영 뒤 확인:

```bash
docker ps -q | xargs docker inspect -f '{{.Name}} {{.HostConfig.Memory}}' | awk '$2 == 0'
```

빈 출력이어야 한다(위 "상한 밖" 표의 손으로 띄운 컨테이너는 예외로 남는다).
