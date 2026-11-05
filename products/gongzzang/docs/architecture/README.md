---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# architecture/

시스템 아키텍처 SSOT. 주제마다 파일 하나다.

## 문서

| 파일 | 내용 | 상태 |
|------|------|------|
| [data-flow.md](./data-flow.md) | 사용자 요청 → Gongzzang API → Foundation Platform contract / Gongzzang DB → 응답 | Active |
| [layers.md](./layers.md) | 계층과 의존성 방향 | Active |
| [mcp-vs-api.md](./mcp-vs-api.md) | 에이전트 경로 vs 프로덕션 경로 상세 | Active |
| [geo-pipeline.md](./geo-pipeline.md) | PostGIS 인덱싱·타일·공간쿼리 파이프라인 | Active |
| [caching.md](./caching.md) | Valkey(Redis 프로토콜) 캐시, Foundation Platform 캐시 무효화, 정적 타일 캐시 | Active |
| [observability.md](./observability.md) | 요청 식별자·tracing·Sentry·health·audit | Active |
| [partner-listing-exchange-boundary.md](./partner-listing-exchange-boundary.md) | 공급자 중립 매물 교환 소유권·정규화 경계 | Active |
| [foundation-platform-boundary.v1.json](./foundation-platform-boundary.v1.json) | Foundation Platform/Gongzzang 경로 소유권 계약 | Active |
| [foundation-platform-catalog-api-contract.v1.pin.json](./foundation-platform-catalog-api-contract.v1.pin.json) · [foundation-platform-webhook-receiver-contract.v1.pin.json](./foundation-platform-webhook-receiver-contract.v1.pin.json) | Foundation Platform 계약 소비자 핀 | Active |
| [platform-integration/](./platform-integration/index.v1.json) | 플랫폼 연동 정책 선언 (`index.v1.json` + 정책별 파일) | Active |
| [traffic-auth-policy-registry.v1.json](./traffic-auth-policy-registry.v1.json) | Traffic/Auth 정책 단일 SSOT (손 편집). 생성기가 읽어 산출물을 재생성 | Active |
| [traffic-auth-policy-registry/](./traffic-auth-policy-registry/README.md) | 위 aggregate 와 생성기·drift 검사 안내 | Active |

## 현재 확정된 원칙

1. **3-service 데이터 접근** — [영역 AGENTS.md 의 소유 표](../../AGENTS.md)
2. **계층 분리** — 도메인 crate는 프레임워크·DB·HTTP client에 의존하지 않는다 ([layers.md](./layers.md))
3. **Foundation Platform가 Catalog SSOT** — parcel geometry, PNU anchors, public/reference spatial layers
4. **Gongzzang은 B2C product semantics SSOT** — listing, listing photo, user, market, insights
5. **좌표계 규칙** — Foundation Platform contract 가 SRID를 명시하고, Gongzzang read model은 사본임을 드러냄

요청·데이터 경로 다이어그램은 [data-flow.md](./data-flow.md)가 소유한다.
