# ADR 0335: 배치 계산은 정해진 방아쇠에서 서빙 호스트를 떠나고, 그 전에는 작업 종류별 격리 경계를 계약에 적는다

- Status: Proposed
- Date: 2026-10-29
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(호스트 메모리 예산), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(한 풀·슬롯·모든 조합 메모리), [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)(한 spec·한 실행기), [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(자산·파티션)
- 관련 사건: 2026-10-28 — `foundation-gold-panel-rebuild` 가 160분 상한에 두 번 걸려 끝났고(18:03, 20:45 UTC; 두 번째 실행 18:05→20:45) unit_price 가 잘렸다(#923). 같은 날 `foundation-by-pnu-serving-bake` 는 00:42→15:19(14시간 37분) 돌다 신호로 끝났다. 2026-10-18 일회성 측정 Spark 작업이 `/data` 를 포화시켜 층 레인이 1분→14분이 됐다. 10-02 배포 tar 가 루트 디스크를 두 번 0 으로 만들었다.

## Context

**한 호스트가 모든 것을 한다.** ai-server(20 vCPU, 62 GiB, 루트 217G·`/data` 1.8T)에서 수집기, Spark, Trino,
Postgres(운영 `foundation`), Airflow, by-PNU 굽기, 타일 굽기, DataHub, 신원(Zitadel), 관측, 배포가 함께 돈다.

측정(2026-10-29 04:31 UTC, `free -g`·`docker stats --no-stream`·`systemctl --failed`, 읽기 전용):

| 항목 | 값 |
|---|---|
| 물리 메모리 / 사용 / 가용 | 62 GiB / 25 GiB / 36 GiB(버퍼 캐시 36 GiB 포함) |
| 부하 평균(1·5·15분) | 10.87 · 6.70 · 4.92 (20 vCPU) |
| Trino | 15.35 GiB / 상한 16 GiB(RSS 16.3 GB, 프로세스 1위) |
| DataHub 묶음(gms·opensearch·kafka·mysql·frontend·actions) | 약 4.4 GiB |
| Airflow 묶음(scheduler·apiserver·dag-processor·db) | 약 1.1 GiB |
| 실행 중 Spark(`lakehouse-maintenance`) | 1.62 GiB / 상한 20 GiB |
| 실패 상태 유닛 | 14(서빙 모니터 8, 굽기, 패널 재생성, 타일 굽기 리허설 2, 법원경매 수집, 신원 복원 훈련) |
| 노드 CPU 시계열 | **없음** — Prometheus 에 `node_cpu_seconds_total` 이 없어 7일 사용률을 잴 수 없었다 |

예산 계약(`tools/host-memory-budget.contract.json`)은 물리 62g, 호스트 예약 5g, 상주 서비스 상한 합 + 가장 큰
일회성 작업이 들어가야 한다고 적는다. ADR-0138 은 `spark` 풀을 3슬롯으로 나누고 메모리를 슬롯 조합으로 센다:
필지 내보내기 샤드 하나가 14.0 GiB, 굽기 `MemoryMax=14G`, Spark 컨테이너 상한 20g, Trino 16g. 이 설계는 **한
호스트 안에서 겹침을 막는 장치**이고, 그 결과가 차례 기다림이다(배포 0단계가 작업을 최대 5시간 기다림 #908,
`takes_turns` 라이브락 ADR-0305, 20시간 필지 레인이 공매를 며칠 막음 ADR-0303).

대기업은 서빙과 배치를 다른 계산 자원에 둔다: Netflix(배치 Spark 는 Titus/EMR 위, 서빙과 분리), Uber(Peloton →
배치 전용 클러스터), Google Borg(우선순위 대역: 서빙 prod 대역은 배치 대역을 선점). 공통 보장: **배치의 메모리·
디스크·CPU 포화가 서빙·운영 DB·스케줄러에 닿지 않는다.**

## Decision

1. **격리 등급을 계약에 적는다(지금, 비용 0).** `tools/host-memory-budget.contract.json` 의 서비스·작업마다
   `isolation_class` 를 둔다: `serving`(Postgres·foundation-api·신원·관측), `control`(Airflow·DataHub·배포),
   `batch`(Spark·굽기·타일·Trino 배치 질의), `collect`(수집기, 한국 공공 API 에 붙어야 하므로 국내). 새 칸이 아니라
   기존 계약의 확장이다(§새 메커니즘 전에 증명: 메모리 예산 계약이 이미 모든 컨테이너와 작업을 열거한다).
2. **같은 호스트에 있는 동안은 커널로 가른다.** `batch` 는 systemd `batch.slice`(cgroup v2 `memory.high`·
   `cpu.weight=20`·`io.weight=20`), `serving` 은 `serving.slice`(`memory.min` 보장)로 둔다. Borg 의 우선순위 대역을 한
   호스트에서 흉내 내는 표준 방법이다(systemd.resource-control). 배치 디스크 쓰기는 `/data` 의 배치 전용 디렉터리로만
   하고 계약이 그 상한을 적는다(10-18 `/data` 포화 사건).
3. **떠나는 곳: 두 번째 전용 호스트(배치 전용)를 1순위로 한다.** 비교(2026-10-29 공시가, 환율 가정 1 EUR=1,600원,
   1 USD=1,400원; **추정치**이며 작업량은 아래 가정):

   | 선택지 | 월 비용(원) | 판정 |
   |---|---|---|
   | A. 국내 두 번째 호스트 구매(Ryzen 9 16코어·128GB·NVMe 4TB, 약 400만원) | 36개월 상각 약 11만 + 전기 약 2만 = **약 13만** | **채택(1순위)**. 같은 LAN, R2 이그레스 0, 운영 모델이 지금과 같음 |
   | B. Hetzner AX102(16코어·128GB, €159+설치 €129) | **약 25만** | 2순위: 구매 없이 내일 시작. 독일 리전, 한국 수집 불가(수집은 A/현 호스트에 남김) |
   | C. EMR Serverless(서울, 하루 4h × 16 vCPU·64GB 가정 = 월 1,920 vCPU-h·7,680 GB-h) | 계산 약 20만 + **AWS→R2 쓰기 이그레스**(월 1.5TB 가정 × $0.126/GB ≈ 26만) = **약 46만** | 기각: 이그레스가 계산보다 비싸다. 또한 공짱 운영 AWS 계정과 섞이면 안 된다(별도 계정 필요) |
   | D. Databricks(Jobs Compute DBU + EC2) | C + DBU(약 $0.15/DBU-h) = **60만+** | 기각: C 의 이그레스 문제 + 벤더 런타임; Iceberg REST 카탈로그와 이중 카탈로그 |
   | E. k8s + Spark-operator | 호스트 비용 + 운영 인력 | 기각(지금): 한 대에서는 격리를 늘리지 않는다(같은 커널). 호스트 3대 이상에서 다시 본다 |

   작업량 가정의 근거: 노드 CPU 시계열이 없어 측정할 수 없었다(위 표). 1단계가 이를 측정한다.
4. **방아쇠(어느 하나라도 참이면 옮긴다):**
   - (a) 출시(외부 사용자에게 공짱이 공개되는 날) — 서빙 SLO(ADR-0334)가 배치에 지면 안 된다.
   - (b) 배치 때문에 서빙 SLO 오류 예산을 한 달에 25% 넘게 태운 달.
   - (c) 배치 대기(슬롯 대기 + `takes_turns` 미룸)가 한 주에 24시간을 넘음 — 1단계가 재는 지표.
   - (d) 7일 p95 호스트 메모리 가용이 8 GiB 아래.
   방아쇠는 운영자의 판단이 아니라 지표다. 옮기는 결정은 비용 승인(소유자)과 함께 한다.
5. **옮기는 단위는 자산 파티션이다(ADR-0333).** 실행기(ADR-0270)가 `isolation_class=batch` 인 자산의 실행 위치를
   계약에서 읽는다(`runs_on: ai-server | batch-1`). 한 번에 한 레인씩 옮기고, 같은 입력을 두 호스트가 굽는 그림자
   기간에 결과 `row_digest` 를 대조한다(ADR-0321).

## 이행(strangler, 한 번에 한 조각)

| 단계 | 내용 | 끝났다는 증거 |
|---|---|---|
| 0 | 노드 지표: node-exporter 의 cpu·meminfo·diskstats 수집기를 Prometheus 대상에 더하고 30일 보존 | 7일 p95 CPU·메모리·`/data` IO 가 Grafana 에 보인다 |
| 1 | `isolation_class` 를 예산 계약에 적고 가드가 모든 항목에 값을 요구 | 가드 녹색 |
| 2 | `batch.slice`·`serving.slice` 를 정의하고 by-PNU 굽기 하나를 먼저 `batch.slice` 로 | 굽기 중 foundation-api p99 가 굽기 없는 시간과 같은 대역 |
| 3 | 방아쇠 지표(c)(d)를 Prometheus 규칙으로 | 경보 규칙 시험 |
| 4 | 방아쇠 도달 시: 두 번째 호스트 구매·설치, 공매 레인(가장 작은 레인, ADR-0333 첫 조각과 같음)을 먼저 옮김 | 그림자 대조 일치 7일 |
| 5 | 필지·건물·단가 레인, 타일 굽기 순서로 옮김. Trino 배치 질의는 마지막 | 원 호스트의 `batch` 항목 0 |

## 가드(ADR-0001)

- **`isolation_class` 필수**(`scripts/guard/every-container-has-a-memory-cap.sh` 확장): 막는 사고 — 10-18 일회성
  Spark 측정이 `/data` 를 포화시켜 서빙 레인이 14배 느려진 일(배치 경계가 계약에 없었다).
- **방아쇠 경보**: 막는 사고 — 10-28 패널 재생성 160분 시간 초과로 unit_price 가 잘린 일이 경보 없이 화면 신선도
  사고로만 드러난 것.

## 기각한 대안

- **지금 바로 클라우드 배치로**: 출시 전이고 작업량을 재지 못했다. 이그레스가 지배하는 비용 구조(위 C)를 재기
  전에는 옮기지 않는다.
- **현 호스트 증설(128GB)**: 메모리는 풀리지만 디스크 IO·커널·재부팅을 서빙과 계속 공유한다. 격리가 아니다.
- **서빙을 옮기고 배치를 남김**: 서빙은 이미 대부분 엣지(R2 + Workers)다. 남은 서빙은 Postgres·API·신원으로 작고,
  배치가 큰 쪽이다. 큰 쪽을 옮긴다.

## Consequences

- 0~3단계는 비용 0, 코드는 계약·유닛·Prometheus 설정뿐이다.
- 4단계부터 월 약 13만원(A) 또는 25만원(B). 방아쇠 도달 시 소유자 승인으로 결정한다(운영 전 유료 대량 작업 규칙).
- 두 호스트가 되면 배포(ADR-0159)가 두 대를 다룬다. 배치 호스트는 서빙 배포와 무관하게 배포되므로 "배포가 굽기를
  죽인다"(10-28 굽기 신호 종료)는 구조적으로 사라진다.
