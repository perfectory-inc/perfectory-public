# Foundation Platform Pipeline Graph Control Plane

Status: Draft
Scope: foundation-platform pipeline graph contract, registry, status, and consumer boundary
Date: 2026-05-19

## 1. 목적

이 문서는 foundation-platform의 데이터 수집, 저장, 변환, 검증, 게시, 이벤트 전파 파이프라인을 누락 없이 표현하기 위한 L4 운영 그래프 기준이다.

목표는 React Flow 화면을 먼저 만드는 것이 아니다. 목표는 `foundation-platform`이 파이프라인의 원장 데이터를 소유하고, Dawneer와 Gongzzang Admin 같은 제품 UI가 그 원장을 읽어 시각화할 수 있게 만드는 것이다.

```text
foundation-platform
  pipeline registry
  runtime status
  lineage
  SLO
  evidence
  graph API

Dawneer / Gongzzang Admin
  graph API consumer
  React Flow viewer
```

React Flow는 viewer다. React Flow node, edge, x/y 좌표, 접힘 상태는 canonical data가 아니다.

## 2. 기대 효과

이 구조를 만들면 다음 효과가 있다.

- 파이프라인이 어디서 시작해서 어디로 흘러가는지 한눈에 볼 수 있다.
- 어떤 단계가 정상, 지연, 실패, 차단, 수동 승인 대기인지 즉시 알 수 있다.
- 새 파이프라인이 추가될 때 문서와 UI가 따로 놀지 않는다.
- 누락된 runbook, owner, evidence, status source를 CI에서 차단할 수 있다.
- 운영 전 개발 단계에서도 구조적 결함을 빨리 발견할 수 있다.
- Dawneer와 Gongzzang Admin은 같은 foundation-platform graph contract를 재사용할 수 있다.
- 성공한 것뿐 아니라 아직 없는 capability도 `missing` 노드로 표현할 수 있다.

가장 중요한 가치는 "정상 동작 그림"이 아니라 "막힌 곳을 숨기지 않는 운영 지도"다.

## 3. 범위

1차 범위는 foundation-platform 파이프라인이다.

포함한다.

- 공공 API 원천 수집
- Bronze object 저장
- Silver 변환
- Gold 변환
- quality gate
- lineage event
- Gold pointer publish
- Catalog API
- outbox event
- consumer receiver contract
- R2 inventory, R2 smoke, R2 billing metric
- SLO, alert, dashboard source
- production orchestrator, deployed receiver, OpenLineage receiver 같은 아직 미완성 capability

포함하지 않는다.

- Dawneer 사이트 생성 파이프라인 상세
- Gongzzang 매물/경매 파이프라인 상세
- 지도 폴리곤 렌더링
- 3D viewer
- React Flow UI 구현

다만 Dawneer와 Gongzzang Admin은 이 graph를 읽는 consumer로 모델링한다.

## 4. 원칙

### 4.1 Registry가 원장이다

모든 pipeline node와 edge는 foundation-platform registry에 등록되어야 한다. 코드, 스크립트, manifest, runbook, SLO 정책이 존재하더라도 registry에 없으면 운영 그래프 기준으로는 누락이다.

### 4.2 화면은 원장이 아니다

제품 UI는 graph API 응답을 React Flow node와 edge로 변환한다. UI는 canonical status를 수정하지 않는다.

UI가 저장할 수 있는 값:

- x/y layout
- zoom
- pan
- expanded/collapsed group
- selected node
- user filter preset

UI가 저장하면 안 되는 값:

- pipeline status
- owner
- dependency
- evidence
- source/output artifact
- SLO state
- run result

### 4.3 없는 것도 표현한다

L4 운영 그래프는 미완성 capability를 숨기지 않는다.

예:

- production orchestrator가 없으면 `missing_capability` node로 표현한다.
- Dawneer deployed receiver E2E가 없으면 `blocked` 또는 `missing`으로 표현한다.
- OpenLineage production receiver가 없으면 `missing`으로 표현한다.
- manual approval이 필요하면 `manual_approval` node로 표현한다.

### 4.4 Node id는 안정적이어야 한다

`node_id`는 label이 아니다. 화면용 이름이 바뀌어도 id는 유지한다. id 변경은 breaking change로 취급한다.

### 4.5 Consumer는 fallback을 가져야 한다

Dawneer와 Gongzzang Admin은 모르는 `node_type`이나 `status`를 만나도 generic node로 표시해야 한다. 새 node type 추가가 즉시 UI 장애로 이어지면 안 된다.

## 5. Graph Contract

### 5.1 Document

```json
{
  "schema_version": "foundation-platform.pipeline_graph.v1",
  "graph_id": "foundation-platform-lakehouse",
  "generated_at": "2026-05-19T00:00:00Z",
  "producer": "foundation-platform",
  "nodes": [],
  "edges": []
}
```

### 5.2 Node

필수 필드:

```json
{
  "id": "industrial-complex-bronze-to-silver",
  "type": "silver_job",
  "label": "Industrial complex Bronze to Silver",
  "project": "foundation-platform",
  "domain": "catalog",
  "owner": "catalog",
  "status": "manual_approval",
  "status_source": {
    "kind": "spark_job",
    "ref": "infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py"
  },
  "runbook_ref": "docs/runbooks/production-orchestrator-cutover.md",
  "evidence_refs": [],
  "inputs": [],
  "outputs": [],
  "tags": []
}
```

권장 필드:

```json
{
  "slo_refs": [],
  "lineage_refs": [],
  "command_ref": "infra/lakehouse/spark/jobs/industrial_complex_silver_to_gold.py",
  "api_refs": [],
  "event_refs": [],
  "artifact_refs": [],
  "last_run": {
    "run_id": "string",
    "started_at": "timestamp",
    "finished_at": "timestamp",
    "duration_ms": 0,
    "outcome": "succeeded"
  },
  "blocker": {
    "kind": "pending_approval",
    "description": "Production orchestrator ADR is not approved."
  },
  "deprecation": {
    "state": "active",
    "replacement_node_id": null,
    "remove_after": null
  }
}
```

### 5.3 Edge

```json
{
  "id": "edge-industrial-complex-bronze-to-silver--industrial-complex-silver-to-gold",
  "source": "industrial-complex-bronze-to-silver",
  "target": "industrial-complex-silver-to-gold",
  "relation": "depends_on",
  "required": true
}
```

Allowed `relation` values:

- `depends_on`
- `produces`
- `consumes`
- `publishes`
- `invalidates`
- `observes`
- `guards`
- `approves`
- `blocks`

## 6. Node Types

초기 node type은 다음을 사용한다.

| Type | 의미 |
|---|---|
| `source` | 외부 원천 또는 내부 원천 |
| `ingest` | 원천 데이터를 받아오는 실행 단계 |
| `bronze_object` | Bronze R2 object 또는 Bronze metadata |
| `silver_job` | Silver 변환 작업 |
| `silver_dataset` | Silver table/object contract |
| `gold_job` | Gold 변환 작업 |
| `gold_dataset` | Gold table/object contract |
| `quality_gate` | promotion blocking 품질 검사 |
| `lineage_event` | lineage artifact 또는 OpenLineage mapping |
| `catalog_pointer` | DB thin pointer, current/previous version |
| `api_endpoint` | foundation-platform API surface |
| `outbox_event` | foundation-platform outbox event |
| `event_fabric_registry` | Adapter-ready event publishing registry |
| `consumer_receiver` | Dawneer/Gongzzang receiver contract |
| `r2_inventory` | R2 inventory/cost/cleanup control |
| `billing_metric` | R2 billing or provider quota metric |
| `slo_objective` | SLO/alert 기준 |
| `dashboard_panel` | Grafana/observability panel source |
| `manual_approval` | 수동 승인 gate |
| `missing_capability` | 아직 없는 capability |

## 7. Status

Allowed `status` values:

| Status | 의미 |
|---|---|
| `healthy` | 최신 성공 상태가 SLO 안에 있음 |
| `running` | 현재 실행 중 |
| `waiting` | 의존성 또는 schedule 대기 |
| `stale` | 마지막 성공이 freshness 기준을 넘김 |
| `failed` | 최근 실행 실패 |
| `blocked` | 외부 승인, 외부 consumer, 미충족 조건 때문에 진행 불가 |
| `manual_approval` | 수동 승인 필요 |
| `missing` | capability, receiver, deployment, runtime 연결이 없음 |
| `disabled` | 의도적으로 비활성화 |
| `unknown` | status source가 아직 연결되지 않음 |

상태 우선순위는 다음과 같다.

```text
failed > blocked > missing > manual_approval > stale > running > waiting > healthy > disabled > unknown
```

## 8. Initial Foundation Platform Graph

초기 registry는 현재 foundation-platform에 존재하는 파일과 계약을 기준으로 만든다.

### 8.1 Lakehouse path

| Node id | Type | 주요 근거 |
|---|---|---|
| `industrial-complex-source` | `source` | public data source catalog, seed/import |
| `data-go-kr-building-register-source` | `source` | `DATA_GO_KR_SERVICE_KEY`, building register ingest |
| `vworld-cadastral-source` | `source` | VWorld cadastral ingest |
| `vworld-land-register-source` | `source` | VWorld land register ingest |
| `building-register-bronze-ingest` | `ingest` | `services/foundation-outbox-publisher/src/building_register_ingest.rs` |
| `vworld-cadastral-bronze-ingest` | `ingest` | `services/foundation-outbox-publisher/src/vworld_cadastral_ingest.rs` |
| `vworld-land-register-bronze-ingest` | `ingest` | `services/foundation-outbox-publisher/src/vworld_land_register_ingest.rs` |
| `industrial-complex-bronze-to-silver` | `silver_job` | `infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py` |
| `silver-industrial-complexes` | `silver_dataset` | `silver.industrial_complexes` |
| `industrial-complex-silver-to-gold` | `gold_job` | `infra/lakehouse/spark/jobs/industrial_complex_silver_to_gold.py` |
| `gold-complex-catalog` | `gold_dataset` | `gold.complex_catalog` |
| `industrial-complex-gold-pointer-publish` | `catalog_pointer` | `foundation-outbox-publisher publish-industrial-complex-gold-pointer` |
| `catalog-read-api-smoke` | `api_endpoint` | `live-readonly-smoke` |

### 8.2 Spatial and tile path

| Node id | Type | 주요 근거 |
|---|---|---|
| `vworld-parcel-boundaries-handoff-to-silver` | `silver_job` | `infra/lakehouse/spark/jobs/vworld_parcel_boundaries_handoff_to_silver.py` |
| `silver-parcel-boundaries` | `silver_dataset` | `silver.parcel_boundaries` |
| `vector-tile-manifest-promote` | `catalog_pointer` | vector tile manifest promote contract |
| `vector-tile-manifest-rollback` | `catalog_pointer` | rollback contract |

### 8.3 Quality, lineage, observability

| Node id | Type | 주요 근거 |
|---|---|---|
| `lakehouse-quality-gate` | `quality_gate` | `docs/data-quality/lakehouse-quality-rules.v1.example.json` |
| `lakehouse-lineage-event` | `lineage_event` | `docs/events/lineage/lakehouse-lineage-event.v1.example.json` |
| `openlineage-production-receiver` | `missing_capability` | production receiver E2E absent |
| `slo-silver-industrial-complexes` | `slo_objective` | `docs/observability/slo-policy.v1.example.json` |
| `slo-gold-complex-catalog` | `slo_objective` | `docs/observability/slo-policy.v1.example.json` |
| `grafana-foundation-platform-dashboard` | `dashboard_panel` | `infra/observability/grafana/foundation-api-dashboard.json` |
| `prometheus-foundation-platform-rules` | `dashboard_panel` | `infra/observability/prometheus/foundation-api.rules.yml` |

### 8.4 Outbox and consumers

| Node id | Type | 주요 근거 |
|---|---|---|
| `outbox-catalog-event-fanout` | `outbox_event` | `docs/events/webhook/outbox-webhook-envelope.v1.example.json` |
| `event-fabric-registry` | `event_fabric_registry` | `docs/events/event-fabric-registry.v1.example.json` |
| `future-event-fabric-consumers` | `missing_capability` | future Search / AI / Notification consumers |
| `gongzzang-catalog-consumer-receiver` | `consumer_receiver` | `docs/events/webhook/receiver-contract.v1.example.json` |
| `dawneer-catalog-consumer-receiver` | `consumer_receiver` | `docs/events/webhook/receiver-contract.v1.example.json` |

`outbox-catalog-event-fanout`은 출시 전 확정한 v1 Foundation Catalog event fan-out 노드 ID다.
이 ID는 Staff Identity ownership 또는 runtime을 뜻하지 않으며, runtime source와 binding은
Foundation/Catalog 범위만 사용한다.

`outbox-catalog-event-fanout` is the pre-launch final v1 node identifier for Foundation Catalog
event fan-out. It does not represent Staff Identity ownership or runtime. Its title, description,
and runtime binding remain Foundation/Catalog-only.

Event publication path:

```text
Gold pointer / artifact publish
  -> transactional outbox
  -> current webhook receivers
  -> Event Fabric registry
  -> future Kafka adapter
  -> future Search / AI / Notification consumers
```

Kafka/MSK is not an initial launch dependency. Service code must depend on the
event publisher contract and outbox envelope, not on a Kafka client. A future
Kafka publisher is an adapter behind the same event idempotency, retry, DLQ,
schema version, and consumer effect acknowledgement rules.

### 8.5 Missing or blocked capability

| Node id | Type | 상태 |
|---|---|---|
| `production-orchestrator` | `missing_capability` | `manual_approval` 또는 `missing` |
| `consumer-deployed-receiver-e2e` | `missing_capability` | `blocked` |
| `production-dashboard-deployment` | `missing_capability` | `missing` |
| `production-alert-routing` | `missing_capability` | `missing` |
| `canonical-silver-gold-live-write` | `missing_capability` | `blocked` until evidence accepted |

## 9. Runtime Status Resolution

초기 status는 registry와 evidence fixture에서 계산한다. 이후 runtime source를 단계적으로 연결한다.

Status source 우선순위:

1. 최근 실행 실패 또는 quality gate 실패
2. blocker/evidence 문서의 blocked 상태
3. SLO freshness violation
4. outbox pending age violation
5. manual approval state
6. latest successful run summary
7. static registry default

Runtime source 후보:

- `catalog.lakehouse_batch_run_audit`
- Spark run summary artifact
- lineage event artifact
- `catalog.outbox_event`
- Prometheus scrape metrics
- R2 smoke metrics
- R2 inventory/billing metrics
- cutover evidence artifact
- receiver E2E evidence artifact

Runtime metrics are attached through `runtime_bindings` in
`docs/catalog/pipeline-graph.v1.json`. Product viewers and API handlers must
not hard-code source slug, lakehouse contract, outbox scope, or cutover blocker
to node-id mappings outside that registry.

## 10. Completeness Guard

L4로 가려면 registry completeness guard가 필요하다.

CI는 다음을 실패시켜야 한다.

- node에 `id`, `type`, `label`, `owner`, `status_source`, `runbook_ref`가 없음
- edge source/target이 존재하지 않음
- cycle이 존재함
- `missing` 또는 `blocked` node에 blocker 설명이 없음
- executable command가 있는데 registry node가 없음
- SLO objective가 있는데 registry node가 없음
- outbox event contract가 있는데 registry node가 없음
- webhook consumer가 있는데 registry node가 없음
- quality rule이 있는데 graph에 연결되지 않음
- `api_endpoint` node가 OpenAPI 또는 route와 연결되지 않음
- `artifact_refs`가 file/object naming rule을 통과하지 않음
- deprecated node가 replacement 또는 remove policy 없이 방치됨

이 guard의 목적은 "예쁜 그래프"가 아니라 "빠짐없는 운영 원장"을 강제하는 것이다.

## 11. API Boundary

초기 API는 read-only여야 한다.

후보 endpoint:

```text
GET /catalog/v1/pipeline-graph
GET /catalog/v1/pipeline-graph/nodes/{id}
GET /catalog/v1/pipeline-graph/runs/{run_id}
```

권한:

- foundation-platform 내부 smoke: staff 또는 internal token
- Dawneer/Gongzzang Admin: Staff read 권한
- public site: 접근 불가

API는 React Flow 형식을 반환하지 않는다. API는 provider-neutral graph contract를 반환한다.

Runtime overlay rule: API responses keep registry node/edge definitions separate from observed runtime state. Product UIs merge `runtime.nodes[<node_id>]` into the viewer layer and must not write React Flow layout state back into foundation-platform canonical registry.

## 12. Viewer Boundary

Dawneer와 Gongzzang Admin은 다음 책임만 가진다.

- graph API 호출
- node/edge를 React Flow로 변환
- 모르는 type/status를 generic fallback으로 표시
- 사용자의 layout, filter, collapse state 저장
- node 클릭 시 detail panel 표시

제품 UI가 foundation-platform canonical field를 수정하면 안 된다.

## 13. Lifecycle

Node lifecycle:

```text
planned -> active -> deprecated -> removed
planned -> missing -> active
active -> blocked -> active
active -> disabled
```

Breaking change:

- node id 변경
- edge relation 의미 변경
- required field 삭제
- status enum 삭제

Non-breaking change:

- node 추가
- edge 추가
- label 변경
- optional field 추가
- 새 node type 추가, consumer fallback 전제

## 14. 단계적 구현

### Phase 1 - Contract and Registry

- 이 문서 확정
- `docs/catalog/pipeline-graph-control-plane.md`
- registry fixture 추가
- registry completeness guard 추가

### Phase 2 - Domain and API

- Rust DTO/domain model 추가
- registry parser/validator 추가
- read-only graph API 추가
- OpenAPI contract 추가

### Phase 3 - Runtime Status

- lakehouse batch audit 연결
- SLO policy 연결
- outbox pending status 연결
- lineage event 연결
- cutover evidence 연결

### Phase 4 - Dawneer Viewer

- Dawneer Staff 화면에서 React Flow viewer 제공
- foundation-platform graph API consumer
- layout/collapse/filter는 Dawneer local preference로 저장

### Phase 5 - Gongzzang Admin Viewer

- Gongzzang Admin에 같은 viewer 패턴 적용
- Gongzzang consumer receiver 상태 표시

### Phase 6 - L4 Completion

- production orchestrator 연결
- deployed receiver E2E evidence 연결
- production dashboard/alert/on-call 연결
- 모든 `missing_capability`가 active, blocked, disabled 중 명확한 상태를 가짐

## 15. L4 완료 기준

L4 완료는 다음이 모두 참이어야 한다.

- foundation-platform의 모든 파이프라인 단계가 registry node로 존재한다.
- 모든 node는 owner, status source, runbook, evidence policy를 가진다.
- 모든 executable job/script/API/event/SLO가 graph에 연결된다.
- graph validation이 CI에서 실행된다.
- runtime status가 최소 lakehouse, outbox, SLO, lineage에 연결된다.
- missing capability가 숨겨지지 않는다.
- Dawneer 또는 Gongzzang Admin 중 하나 이상이 graph API를 React Flow로 표시한다.
- UI가 모르는 node type/status를 generic fallback으로 표시한다.
- node id lifecycle과 compatibility policy가 지켜진다.

## 16. 결론

Foundation Platform Pipeline Graph Control Plane은 UI 기능이 아니다. `foundation-platform`이 데이터 운영 흐름을 누락 없이 설명하고, 현재 상태와 막힘을 기계적으로 드러내는 control plane이다.

React Flow는 각 제품의 viewer로 사용한다. 원장은 foundation-platform registry와 runtime status에 있다. 이 경계를 지키면 파이프라인이 변경되어도 Dawneer와 Gongzzang Admin은 같은 graph API를 다시 읽어 자동으로 최신 운영 지도를 보여줄 수 있다.
