# ADR 0029 - 환경·시크릿 명시적 분리

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Accepted invariant; legacy compatibility removed by [ADR 0035](./0035-legacy-r2-removal-and-atomic-namespace.md), Foundation-owned ETL implementation moved by [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md) |

## 결정

외부 상태를 변경할 수 있는 모든 process는 명시적이고 typed된 environment를 받아야 한다.
environment가 없거나 잘못되면 fail closed해야 한다. credential은 해당 environment에
scope를 두고 원자적으로 로드한다. credential 일부만 있으면 오류이며 다른 environment의
credential을 fallback으로 사용해서는 안 된다.

이는 소유권과 무관한 안전 불변식이다. 각 platform은 자신의 environment type과 secret
namespace를 사용하며, 우연히 존재하는 credential로 target을 추론해서는 안 된다.

## Gongzzang 경계

Gongzzang은 더 이상 공공데이터 ETL이나 객체 저장소 자격증명을 소유하지 않는다.
따라서 Gongzzang 환경 예시는 제품 소유 설정과 발행된 Foundation integration contract만
노출한다. 일반 ETL과 raw-data R2 credential은 이 경계에서 금지한다.

## 영향

- local, staging, production 변경은 모호한 secret namespace를 공유하지 않는다.
- 불변식을 약화하는 compatibility alias는 금지하며 ADR 0035가 제거를 기록한다.
- 구체적인 workflow 이름과 secret 값은 이 public architecture contract 밖의 deployment
  detail로 남긴다.

## 강제 지점

- `docs/architecture/foundation-platform-boundary.v1.json`이 허용된 Gongzzang
  environment 표면을 정의한다.
- environment parser와 configuration test는 누락·잘못됨·부분적인 변경 설정을 거부해야 한다.
- secret scanner와 repository policy가 credential이 source-controlled default가 되는 것을
  막는다.
