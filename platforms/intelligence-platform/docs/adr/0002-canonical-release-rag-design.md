# ADR 0002 — canonical release를 읽는 RAG 설계 경계

- 상태: 제안(구현 보류)
- 소유자: Intelligence Platform
- 전제: Foundation Platform이 canonical release와 source lineage를 먼저 승인한다.

## 결정

RAG는 Foundation의 canonical data를 복사해 자체 사실 데이터베이스로 만들지 않는다.
Foundation이 발행한 release envelope를 읽고, 각 인덱스 레코드에 다음 식별자를 함께
저장한다.

- `canonical_entity_id`: Foundation Catalog의 안정적인 엔티티 ID
- `source_snapshot_id`: 원본 Iceberg/공급자 snapshot ID
- `release_id`: Foundation이 승인한 immutable release ID
- `content_checksum`: 임베딩 입력 정규화 결과의 SHA-256

질의 결과는 원문 답변보다 먼저 위 식별자와 Foundation 조회 링크를 반환한다. Foundation이
철회(tombstone)한 엔티티는 같은 `canonical_entity_id`로 삭제 이벤트를 만들고 모든 활성
인덱스에서 제거한다. 이전 release는 롤백을 위해 보존하지만 기본 검색에서는 비활성이다.

## 권한과 신뢰 경계

- Foundation만 canonical 사실과 삭제 상태를 쓴다.
- Intelligence는 release를 읽고 embedding/index 작업을 수행할 수 있지만 Catalog를
  직접 변경하지 않는다.
- 쿼리 API는 Identity 인증·tenant 권한·감사 로그 없이는 운영 노출하지 않는다.

## 구현하지 않는 것

현재 Intelligence 영역에는 실제 retrieval/vector 구현이 없으므로 다음을 이 ADR의 승인
조건으로 둔다.

- hash를 임베딩으로 속이는 fixture 구현 금지
- 승인되지 않은 embedding provider 또는 vector index를 코드에 고정 금지
- canonical release envelope, 삭제/철회 이벤트, freshness SLA가 정해지기 전 production
  RAG endpoint 금지

먼저 Foundation이 `release_id + source_snapshot_id + entity identity`를 원자적으로
발행하고, 이후 Intelligence가 승인된 provider/index adapter를 선택하는 별도 ADR을 작성한다.
