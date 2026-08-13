# ADR 0025 - Bronze 생산자 격리

| Field | Value |
|---|---|
| Date | 2026-05-08 |
| Status | Superseded operationally by [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md) and [ADR 0048](./0048-horizontal-platform-redefinition.md) |
| Amends | [ADR 0022](./0022-bronze-scraping-isolated-python-service.md) |

## 결정

지속되는 결정은 특정 CI workflow가 아니라 격리 경계다.

- product runtime은 provider별 acquisition runtime을 spawn하거나 import하지 않는다.
- 서로 다른 acquisition·transformation 단계는 immutable object와 명시적 manifest로
  교환한다.
- retry, resource sizing, failure ownership은 단계별로 격리한다.
- promotion 단계는 producer의 validation contract를 통과한 artifact만 소비한다.

과거 Gongzzang 소유 Bronze workflow와 구현 경로는 더 이상 현재가 아니다.
현재 Foundation Platform이 public-data acquisition, Bronze/Silver/Gold 처리,
공개·기준 벡터 타일 발행을 소유한다. Gongzzang은 발행된 HTTP·event·immutable-artifact
contract만 소비한다.

## 영향

- Gongzzang code는 Foundation 소유 acquisition을 위한 provider scraper나 subprocess
  adapter를 다시 포함하지 않는다.
- Foundation Platform은 위의 단계·artifact 경계를 명시적으로 유지하는 한 자체
  orchestrator를 선택할 수 있다.
- workflow 파일명과 runner 배치는 deployment detail이므로 architecture contract가
  아니며 이 ADR에서 의도적으로 다루지 않는다.

## 강제 지점

- [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md)가 현재
  owner 경계를 정의한다.
- `docs/architecture/foundation-platform-boundary.v1.json`과 repository boundary
  guard가 Gongzzang에 Foundation 내부 구현이 재도입되는 것을 막는다.
