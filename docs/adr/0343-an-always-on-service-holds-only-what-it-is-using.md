# ADR 0343: 상시 서비스는 쓰는 만큼만 잡고, 배치만 쓰는 서비스는 쓸 때만 뜬다

- Status: Accepted (1·2·6항. 3·4·5항은 실행 때 사장 확인)
- Date: 2026-10-29
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §6(모든 컨테이너에 상한),
  [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(풀·슬롯 메모리 셈),
  [ADR-0335](./0335-batch-compute-leaves-the-serving-host-at-a-declared-trigger.md)(격리 등급: Trino 는 `batch`),
  [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md) §10·[ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md)(Martin 제거)
- 관련 사건: 2026-10-29 ai-server 유휴 상태에서 Trino 가 12.9GiB 를 잡고, 카탈로그 Kafka 브로커가 `docker stats` 에서
  88–101% CPU 로 보였다. Martin 두 컨테이너는 상한 없이 13일째 떠 있다.

## Context

측정은 모두 2026-10-29 14:17–14:30Z ai-server 에서 읽기만 했다. 시계열 출처는 셋이다: 컨테이너 cgroup 의 누적
계수(`cpu.stat usage_usec`·`memory.peak`·`memory.events`, 컨테이너가 뜬 뒤 전체), sar(`/var/log/sysstat`, 10월 21–29일,
10분 간격), Trino `http-request.log`(10월 14–29일). Prometheus 는 쓸 수 없었다: node-exporter 가 textfile 수집기만
켜고 있고(`node_cpu_*`·`node_memory_*` 없음) 컨테이너 지표(cAdvisor)는 없다.

**호스트(sar, 하루 단위):** 가용 메모리 하루 최소 23.1–46.6GiB, 평균 39.1–48.3GiB. CPU 평균 6.0–21.9%, 10분 최대
13.4–59.1%(20 vCPU). 조용한 날(10-25, 10-27)은 평균 6% 즉 1.2 코어다.

**상시 컨테이너(유휴, `docker stats` + cgroup):**

| 서비스 | 지금 메모리 / 상한 | 평균 CPU(코어, 가동 기간 전체) | 누가 쓰나(측정) |
|---|---|---|---|
| Trino | 12.9GiB / 16g (힙 커밋 10.4GB, 사용 2.7GB) | 0.11 (19시간) | 하루 220–2,750 질의. 전부 배치: `shape_ledger_check`(호스트에서 HTTP, `foundation-lakehouse-production-shape`), `data_quality` 와 Silver 확인·운영자·에이전트(`docker exec … trino`). 배포 직후와 정해진 시각에 몰려 한 번에 몇 분. 서빙 경로에는 없다 |
| DataHub 6개 + 중계 | 4.46GiB / 7.1g | 0.31 (13일; 그중 Kafka 0.20) | 화면: 사용 사건 38건이 전부 09-30·10-01(설치), 10-16 로그인 1회. 쓰기: 하루 약 2,500 aspect, 전부 `__datahub_system`(Airflow OpenLineage·데이터 품질 결과). Dawneer 경유 읽기(foundation-api): 측정 가능한 7시간 동안 0건 |
| Airflow 4개 + 중계 | 1.19GiB / 2.8g | 0.16 | 모든 예약 작업의 스케줄러 |
| Foundation PostgreSQL | 0.12GiB / 4g | 0.015 | 운영 DB, API |
| 신원(Zitadel·DB·identity-api·중계) | 0.14GiB | 0.06 | 직원 로그인, 서비스 토큰 |
| foundation-api·Prometheus·Alertmanager·node-exporter | 0.18GiB | 0.01 | 운영 상태, 경보 |
| Martin 2개 | 22MiB / **상한 없음** | 0.008 | 연결 0. `0.0.0.0:3110·3111`(host 네트워크)에서 듣고, healthcheck 는 3000 을 봐서 늘 실패 |
| **합계** | **19.0GiB** | **약 0.67** | |

`memory.events` 의 `oom_kill` 은 모든 컨테이너가 0 이다. `max`(상한에 닿아 페이지 캐시를 비운 횟수)는 Trino 20,203,
GMS 1,572, Kafka 1,228, OpenSearch 576, Airflow 스케줄러 118 — 상한이 캐시를 묶을 뿐 작업 집합을 자르지 않았다.

**Kafka 의 CPU.** 30초 동안 컨테이너 cgroup 은 6.13 CPU-s, 브로커 JVM 프로세스는 0.59 CPU-s 를 썼다. 나머지 90% 는
healthcheck 다: `kafka-broker-api-versions` 가 매 5초 새 JVM 을 띄우고, 한 번에 약 1.1 CPU-s(20 코어에 퍼지는 JIT·GC
스레드)를 쓴다. 하루 17,280번, 약 5.3 CPU-시간. 브로커 스레드(`top -H`)는 모두 0.3% 이하, 로그 클리너·소비자 루프는
아니다. `docker stats` 의 88–101% 는 그 JVM 이 도는 순간을 잡은 값이다.

**Trino 의 메모리.** 이미지의 `jvm.config` 는 `-XX:InitialRAMPercentage=80` 과 `MaxRAMPercentage=80` 을 함께 둔다. 힙의
하한이 상한과 같아(12.8GB) G1 이 쓰지 않는 힙을 운영체제에 돌려줄 수 없다. 측정된 가장 큰 질의는 7.0GB(2026-09-25)라
상한은 맞지만, 쉬는 동안 그만큼을 잡을 이유는 없다.

**Trino 의 기동 시간(컨테이너 로그).** `Logging to stderr`→`SERVER STARTED`: 10-13 7초, 10-15 6초·6초, 10-16 2분 45초,
10-28 8분 39초(컨테이너 시작부터는 12분 23초, 굽기가 돌던 때).

**배치.** 실행 단위 Spark 컨테이너는 `run --rm` 이라 남지 않는다(종료 컨테이너는 메모리를 잡지 않는다). 지난 7일
Airflow 작업 시간 합계는 약 76시간이고 무거운 계산(굽기 20.0h, 패널 9.4h, 종류 2.5h, 유지보수 1.6h)은 하루 약 5시간이다.
by-PNU 굽기 유닛의 `MemoryPeak` 는 14.17GB(`MemoryMax=14G`)였다. Spark 컨테이너의 실행별 최대 메모리는 어디에도
남지 않는다 — 컨테이너가 지워지면서 cgroup 도 사라진다.

**대기업·오픈소스가 하는 것.**

- 쓰지 않을 때 0 으로: Snowflake 웨어하우스 auto-suspend/auto-resume, Databricks SQL warehouse auto-stop, Starburst
  Galaxy 클러스터 auto-suspend, Knative·KEDA scale-to-zero. 공통 조건: 사용자가 첫 질의의 기동 지연을 견디는 배치·분석
  용도이고, 상태는 바깥(객체 저장소·카탈로그)에 있다. Trino 는 둘 다 맞다(R2 Iceberg).
- JVM 이 쓰지 않는 힙을 돌려준다: JEP 346(*Promptly Return Unused Committed Memory from G1*, JDK 12)이 바로 컨테이너
  과금 때문에 생겼다. `G1PeriodicGCInterval` 동안 수집이 없으면 동시 수집을 돌려 하한까지 커밋을 줄인다.
- 기동 확인과 상시 확인을 나눈다: Kubernetes `startupProbe`(촘촘) 와 `livenessProbe`(드물게·가볍게). Strimzi 는
  Kafka 프로브에서 JVM CLI 를 띄우지 않는다. Docker 에서는 `start_interval`(Engine 25+)이 그 자리다.
- 메타데이터 카탈로그는 배치 적재가 기본이다: DataHub 의 수집은 예약 recipe 이고, OpenLineage 는 HTTP 외에 file·Kafka
  전송으로 수집기와 카탈로그를 떼어 놓는다.
- 상한은 작업 집합의 이력으로 정한다: Kubernetes VPA 추천기는 8일 반감기 히스토그램의 백분위에 여유 15% 를 더한다.

## Decision

1. **상태 프로브는 기동용으로 촘촘하고, 뜬 뒤로는 드물다.** 첫 성공까지는 `start_interval`, 그 뒤 `interval` 은 분 단위.
   프로브가 새 JVM·인터프리터를 띄우면 상시 간격은 5분 이상이다. Docker 는 unhealthy 로 재시작하지 않으므로 상시
   프로브는 보고만 한다. 첫 적용: Kafka 브로커(#954, 5초 → 기동 중 5초·이후 5분).
2. **상시 JVM 은 힙의 하한을 상한과 같게 두지 않는다.** 하한은 상한의 10% 안팎, G1 은 `G1PeriodicGCInterval=60000` 으로
   1분 쉬면 돌려준다. 상한과 질의 한도는 측정에서 정하고 바꾸지 않는다. 첫 적용: Trino
   (`platforms/foundation-platform/infra/lakehouse/trino/jvm.config`, 이미지 파일에서 이 두 줄만 다르다). 예외는 이유를
   적는다: OpenSearch 는 Elastic 이 크기 변경 멈춤 때문에 하한=상한을 권하고, 상한이 1GB 라 그대로 둔다.
3. **사용자가 배치뿐인 상시 서비스는 쓸 때만 뜬다(scale to zero).** 대상: Trino. 방식:
   - 쓰기 전에 하나의 확인으로 띄운다. 이미 있는 `foundation-outbox-publisher wait-trino-ready` 를 넓혀 멈춰 있으면
     `docker start foundation-platform-trino` 하고 health 를 기다린다(설정은 컨테이너가 이미 갖고 있어 compose 환경이
     필요 없다). `data-quality-check.py`·`lakehouse-production-shape.py`·Silver 확인이 이것을 부른다.
   - 15분 동안 끝난 질의도 도는 질의도 없고, 뜬 지 15분이 지났으면 멈춘다(`system.runtime.queries`). Airflow 의 짧은
     예약 작업 하나가 판단하고 `docker stop` 한다. Trino CLI 실패 재시도(`internal_http.retry_trino`)는 다시 띄운 뒤 묻는다.
   - **사장 확인이 필요한 이유:** 운영자와 에이전트가 `docker exec foundation-platform-trino trino …` 로 읽는데, 멈춘
     컨테이너에는 들어갈 수 없다. 읽기 전용 규칙의 에이전트가 `docker start` 를 해도 되는지, 그리고 바쁜 호스트에서
     첫 질의가 최대 12분 기다리는 것을 받아들일지를 정해야 한다.
4. **사람이 거의 보지 않는 카탈로그는 수집 창에서만 뜬다.** 대상: DataHub 묶음. 방식: Airflow OpenLineage 전송을
   HTTP 에서 file 로 바꿔 사건을 쌓아 두고(덮어쓰지 않는다), 하루 한 번 창 작업이 DataHub 를 띄워 쌓인 사건을 순서대로
   보내고 recipe 와 데이터 품질 결과를 올린 뒤 내린다. Dawneer 의 카탈로그 화면은 열 때 띄운다. **사장 결정이 필요한
   이유:** ADR-0118 §2("모든 실행이 카탈로그에 보고한다")의 반영 지연이 실시간에서 하루로 늘고, 카탈로그 화면이 처음
   열릴 때 기동을 기다린다. 결정 전까지는 지금처럼 상시이고, 1항으로 CPU 만 줄인다.
5. **손으로 띄운 컨테이너는 남기지 않는다.** Martin 두 개는 ADR-0112 §10·ADR-0133 이 이미 지우기로 했고 실행 때 사장
   확인을 받기로 했다. 이 결정은 그 실행을 요청한다. 그때까지 `tools/host-memory-budget.contract.json` 의 `host.uncapped`
   에 상태와 이유를 적는다. 상한을 씌우려면 아무도 쓰지 않는 컨테이너를 다시 만들어야 하므로 씌우지 않는다.
6. **상한은 이력으로 고친다.** 컨테이너별 작업 집합(`working_set`)과 CPU 를 Prometheus 에 남기고(cAdvisor, 15일 보존은
   이미 있다), 상한 = 8일 이상 최대 작업 집합 × 1.15 를 올림한 값으로 고친다. 지금 남은 자료(`memory.peak`)는 페이지
   캐시를 포함해 상한에 붙어 있으므로 이것만으로 상한을 내리지 않는다. 고치면 같은 PR 에서 예산 계약과 런북의 근거
   표를 맞춘다(가드 `every-container-has-a-memory-cap.sh`).

**항상 떠 있어야 하는 것과 이유:** Foundation PostgreSQL·foundation-api(운영 DB 와 그 API, 콜드 스타트가 곧 장애),
신원(Zitadel·identity-api: 직원 로그인과 서비스 토큰), Airflow(쓸 때만 뜨는 모든 것을 띄우는 주체), Prometheus·
Alertmanager(나머지가 자는 동안에도 경보가 울려야 한다).

## 이행

| 단계 | 내용 | 상태 |
|---|---|---|
| 1 | Kafka 프로브(1항) | #954 draft |
| 2 | Trino 힙 하한(2항), Martin 을 계약에 기록(5항) | 이 PR. Trino 는 compose 로 다시 만들 때 반영된다: 배포는 Trino 를 다시 만들지 않으므로 `lakehouse-compute-engines.md` 의 재생성 명령을 운영자가 한 번 돌린다 |
| 3 | cAdvisor 와 node-exporter 의 cpu·meminfo 수집기(6항, ADR-0335 0단계와 같음) | 다음 PR |
| 4 | Trino scale to zero(3항) | 사장 확인 뒤 |
| 5 | DataHub 수집 창(4항) | 사장 결정 뒤 |
| 6 | Martin 제거(5항) | 사장 확인 뒤, 배포 경로로 |

## Consequences

유휴 메모리(계산, 위 표 기준):

| 시점 | 상시 컨테이너 합계 |
|---|---|
| 지금(측정) | 19.0GiB |
| 2단계 뒤(추정) | 약 10–12GiB. Trino 가 12.9GiB 에서 3.5–5.5GiB 로(힙 1.6–2.7GB + 힙 밖 약 2–3GB: 지금 anon 12.8GiB − 커밋 힙 9.9GiB). 배포 뒤 `docker stats` 로 확인한다 |
| 4·5·6단계 뒤(계산) | 약 1.6GiB(Airflow 1.19 + 신원 0.14 + 관측·API 0.18 + PostgreSQL 0.12). Trino·DataHub 는 쓰는 동안만 |

유휴 CPU: Kafka 프로브가 하루 약 5.3 CPU-시간에서 약 0.1 CPU-시간으로(코어 0.18개 절감, 계산). DataHub 창은 나머지
약 0.11 코어를 창 밖에서 없앤다.

예산 가드의 합(61.02g/62g)은 바뀌지 않는다: 상한은 예약이 아니고, 쓸 때만 뜨는 Trino 도 작업과 겹쳐 뜰 수 있으므로
상시 몫으로 계속 센다. 실제로 비는 것은 페이지 캐시와 다른 작업이 쓸 수 있는 가용 메모리다.

위험: Trino 의 힙이 커지는 동안 GC 가 더 돌 수 있다(배치 질의라 받아들인다). 3항의 기동 지연은 바쁜 호스트에서 분
단위다. 4항은 카탈로그의 실시간성을 잃는다.
