# ADR-0046: Kafka·Kubernetes 선행 설계(조건이 생길 때까지 보류)

| | |
|---|---|
| Date | 2026-06-22 |
| Status | Accepted — **둘 다 보류**; 구축 순서가 아니라 도입 조건과 이전 경로를 기록 |
| Scope | cross-repo event transport + deployment runtime (gongzzang · foundation-platform · dawneer) |
| Owner | perfectoryinc (platform owner) |
| Governs under | [✱ Product-first](../../AGENTS.md) · [ADR-0044](./0044-bazel-transition-reconciliation.md) (no premature infra) · [sss-charter.md](../sss-charter.md) B-2 reliability |

> 이것은 **선행 설계**다. Kafka와 Kubernetes를 *언제·왜·어떻게* 도입할지, 그리고 지금은
> **둘 다 만들지 않을 것**을 결정한다. 조건 전에 만드는 것은 ADR-0044가 되돌린
> "사용자보다 인프라 우선" 함정이다. 출시 전 사용자 0명인 지금은 운영 비용을 정당화하지 못한다.

## 배경(2026-06-22 현재 사실)

- **이벤트:** Foundation/Identity 변경은 **outbox 테이블**에 기록하고 `OutboxWorker`가
  (`crates/outbox-publisher`) polling한 뒤 교체 가능한 **`EventBroadcaster` trait**로 발행한다.
  운영 어댑터는 gongzzang/dawneer에 HTTP fan-out하는 `WebhookBroadcaster`이고 개발용
  `LoggingBroadcaster`도 있다. outbox는 이미 최소 한 번 전달과 aggregate별 순서를 보장한다.
  **새 전송 방식은 재작성 아닌 어댑터다.**
- **배포:** 아직 **운영 런타임이 없다**. 릴리스 산출물은 `cargo build --release` 바이너리이며
  **Dockerfile은 0개**다. `infrastructure/`(Pulumi)는 아직 ECS/EKS/EC2 compute를 프로비저닝하지
  않는다. 로컬 개발은 `docker-compose`(Postgres)를 사용하고 저장소마다 서비스는 약 2~3개다.
- **규모:** 출시 전 사용자 0명, 3서비스 아키텍처([ADR-0030](./0030-three-service-architecture.md))다.

## 결정

1. **Kafka와 Kubernetes를 보류한다.** 출시 전에는 만들지 않는다. 이벤트는
   Outbox→WebhookBroadcaster로 전달하고 첫 배포 시 작동하는 가장 단순한 런타임을 선택한다.
2. **명시된 조건이 생길 때만 도입한다.** "선행 구축"은 하지 않고 선행 *설계*만 둔다.
3. **먼저 싼 단계를 선택한다.** Kafka와 K8s는 하위 단계가 측정된 필요를 충족하지 못할 때만
   도달하는 최상위 단계다.
4. **현재 구조는 둘을 도입할 준비가 되어 있다.** broadcaster는 교체 가능하고 바이너리는
   stateless이므로 기다려도 비용이 없다.

## Kafka — 전송 단계

**단계(필요를 충족하는 가장 낮은 단계만 도입):**
1. **WebhookBroadcaster(현재)** — 직접 HTTP fan-out. 알려진 소비자가 몇 개면 충분하다.
2. **관리형 queue/topic — AWS SQS/SNS** — 운영 부담 없이 내구성 있는 fan-out을 제공하는
   `SqsBroadcaster`/`SnsBroadcaster` 어댑터. webhook 전달 신뢰성/backpressure가 문제가 될 때
   첫 단계다.
3. **Kafka(또는 Redpanda)** — **log/replay 의미**가 실제 요구일 때만 사용한다.

**3단계(Kafka)로 가는 조건:**
- 이벤트 로그에서 **내구성 있는 replay/new-consumer backfill**이 필요함(SQS/SNS는 과거 재생 불가)
- partition별 순서와 consumer group이 필요한 **다수 소비자 또는 고처리량 fan-out**
- 같은 순서 로그를 소비하는 stream processing/CDC 파이프라인

**조건 충족 시 경로:** `EventBroadcaster for KafkaBroadcaster`를 구현한다(outbox는 정본으로
남아 topic에 발행). 소비자는 webhook endpoint에서 Kafka consumer group으로 바꾼다.
도메인이나 outbox는 재작성하지 않는다. broker를 직접 운영하기 전에 **관리형**
(MSK/Redpanda Cloud)으로 실행한다.

출시 전 알려진 소비자 3개 때문에 Kafka를 세우지 않는다. broker/partition/KRaft 운영 비용이
이익보다 크며 webhook(또는 SQS/SNS)으로 충분하다.

## Kubernetes — 런타임 단계

**단계(필요를 충족하는 가장 낮은 단계만 도입):**
1. **서비스별 Dockerfile** — 모든 container runtime의 전제 조건이다. 실제 배포할 때 먼저
   만든다(저렴하고 상위 모든 단계를 연다).
2. **관리형 container runtime — AWS ECS Fargate 또는 App Runner**(Pulumi 프로비저닝).
   클러스터 운영 없이 autoscaling과 rollout을 제공하며 장기간 기본 운영 환경으로 삼는다.
3. **Kubernetes(EKS)** — 관리형 runtime으로 할 수 없는 세밀한 orchestration이 필요할 때만 도입한다.

**3단계(K8s)로 가는 조건:**
- Fargate/App Runner가 표현할 수 없는 advanced scheduling/autoscaling/self-healing/service mesh가
  필요한 다수 서비스
- 관리형 runtime을 넘어서는 multi-tenant 격리, 복잡한 network, GPU/batch scheduling 요구

**조건 충족 시 경로:** 이미 컨테이너(1단계)가 있으므로 EKS 도입은 Pulumi를 통한 provisioning과
manifest 작업이며 앱 재작성은 아니다.

출시 전에 EKS를 세우지 않는다. 서비스 약 3개·사용자 0명의 클러스터는 보상 없이 업그레이드,
보안, node 운영 부담만 만든다. 첫 운영 runtime은 Fargate/App Runner가 맞다.

## 영향

- 두 기술 모두 명확한 조건과 진행 경로가 있어 "지금 Kafka?" 논쟁을 멈출 수 있다.
- 출시 전 운영 부담이 늘지 않고 개발은 데이터/제품 본선(Bronze 수집)에 집중한다.
- 이벤트는 어댑터 교체가 가능하고 서비스는 stateless 바이너리라 전환 비용이 낮다.
- 정직한 한계: 실제 첫 배포 때 Dockerfile 작성과 Fargate/App Runner 선택이 필요하며,
  이 ADR에서는 그 작업을 하지 않고 방향만 정한다.

## 재평가 조건(하나라도 발생하면 이 ADR을 다시 본다)

- Webhook 전달 신뢰성 또는 처리량이 측정된 장애 원인이 되면 SQS/SNS, 이후 Kafka를 검토한다.
- 두 번째 이상 팀/서비스가 이벤트 이력을 replay하거나 독립 소비해야 하면 Kafka를 검토한다.
- 서비스 수나 orchestration 요구가 Fargate/App Runner를 넘으면 EKS를 검토한다.
- 그 전까지는 **Kafka도 Kubernetes도 `MODULE`식 선행 scaffolding도 만들지 않는다.**

## 참고 문서

- [ADR-0030](./0030-three-service-architecture.md) three-service architecture; [ADR-0032](./0032-eventual-consistency-strategy.md) eventual consistency (outbox).
- [ADR-0044](./0044-bazel-transition-reconciliation.md) product-first / no premature infra; [AGENTS.md](../../AGENTS.md) ✱ product-first.
- [ADR-0047](./0047-collection-event-fabric.md) refines this: the Kafka-shaped (broker-deferred) Collection Event Fabric for Bronze ingestion. Adopted by `foundation-platform` ADR-0013.
- `crates/outbox-publisher` (foundation-platform): `EventBroadcaster` trait + `WebhookBroadcaster`.
