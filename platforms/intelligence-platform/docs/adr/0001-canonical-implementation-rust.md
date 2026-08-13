# ADR-0001: Rust를 Intelligence Platform 정본 구현으로 사용하고 Python 서비스는 폐기

- **Status:** Accepted
- **Date:** 2026-07-08
- **Deciders:** Platform owner
- **Architecture:** [Intelligence Platform Architecture](../architecture.md)

## 배경

Intelligence Platform 구현이 두 개 공존했다.

- `intelligence-platform-rs`(Rust + Axum, 현재 `intelligence-platform`으로 이름 변경) — inbound
  auth, admission stack, lease 기반 claim을 사용하는 durable Postgres outbox, Idempotency-Key
  제출, live Postgres CI가 있는 production 방향.
- 과거 `intelligence-platform` Python + FastAPI tree — 자체 README에서 "production" service,
  다른 곳에서 "reference contract"로 설명하던 경로.

감사 결과 이중 구현은 실제로 해로웠다.

- **Contract drift (G1/RC7):** 두 client가 같은 Foundation intake에 서로 다른 wire contract를
  말했다. request shape, status enum(`queued|rejected` 대 `submitted|accepted|rejected|queued`),
  idempotency-key 공식(4-field 대 3-field), default path, header set이 달랐고 어느 쪽이
  권위인지 선언한 process artifact가 없었다.
- **Python service는 deployable하거나 안전하지 않았다:** inbound authentication이 없고
  request body의 `tenant_id`를 신뢰했다(C0으로 평가된 미인증 cross-tenant write surface),
  hardcoded stub `/v1/rag/query`, embedding으로 SHA-256 hash 사용, async event loop를 막는
  synchronous client, 요청마다 client 생성, 동작하지 않는 in-memory outbox, Dockerfile·CI·
  dependency lockfile 부재, tooling은 3.12를 목표로 하지만 CPython 3.14에서 만든 `.pyc` artifact.

Maintaining both meant every contract change had to land in two places or drift; the Python surface added attack surface and false capability with no path to production.

## 결정

**저장소 루트의 Rust workspace를 플랫폼 경계의 유일한 정본 구현·출처로 사용한다.** 이전
`intelligence-platform-rs` 경로는 역사 기록일 뿐이다. `intelligence-platform/`에 있던 폐기 Python
프로젝트는 **저장소에서 제거**했으며 배포 대상·계약 참조 집합·CI에 포함하지 않는다.

Foundation Platform과의 wire 계약은 Rust client와 `schemas/` 아래 스키마만 정의한다.

## 영향

- 미인증 cross-tenant write surface(C0), stub RAG path, hash embedding, event-loop blocking,
  Python 쪽 contract drift를 병행 유지하지 않고 제거로 없앤다.
- Python prototype에만 있던 기능은 이제 Rust 작업으로 추적한다. 실제 retrieval/RAG 연결, 장난감이
  아닌 embedding adapter를 위한 port, core의 source 권위 순서가 남은 항목이다(P1/P2 파동과 과거
  RAG 설계 문서 참고). *(2026-07-20 기록: 해당 hardening 계획과 RAG 설계 문서는 이 모노레포로
  이전하지 않았고 흡수 전 기록/보관소에만 있다. RAG 작업을 재개하기 전에 새 설계 문서가 필요하다.)*
- Python 서비스를 production 또는 계약 참조로 설명하던 문서를 정정한다.

## 복구

폐기된 Python 트리와 전환 전 실험은 [루트 ADR-0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md)이
정한 비공개 전환 보관소에만 보관하며 공개 정본 기록에는 넣지 않는다. 복구 snapshot은 실행 저장소
밖에서 읽기 전용으로 확인하고 정본 Rust 경로 위에 복원하지 않는다. 개별 검토한 기능만 새 설계와
일반 변경 절차로 이식한다.

## 관련 문서

- Current module and platform boundaries: `docs/architecture.md`
