---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# architecture/

시스템 아키텍처의 정본. 주제마다 파일 하나다.

## 문서

| 파일 | 내용 | 상태 |
|------|------|------|
| [data-flow.md](./data-flow.md) | 사용자 요청 → 공짱 API → 파운데이션 계약 / 공짱 DB → 응답 | 사용 중 |
| [layers.md](./layers.md) | 계층과 의존성 방향 | 사용 중 |
| [mcp-vs-api.md](./mcp-vs-api.md) | 에이전트 경로와 운영 경로의 차이 | 사용 중 |
| [geo-pipeline.md](./geo-pipeline.md) | PostGIS 색인·타일·공간 질의 파이프라인 | 사용 중 |
| [caching.md](./caching.md) | Valkey(Redis 프로토콜) 캐시, 파운데이션 캐시 무효화, 정적 타일 캐시 | 사용 중 |
| [observability.md](./observability.md) | 요청 식별자·추적·Sentry·상태 점검·감사 | 사용 중 |
| [partner-listing-exchange-boundary.md](./partner-listing-exchange-boundary.md) | 공급자 중립 매물 교환의 소유권·정규화 경계 | 사용 중 |
| [foundation-platform-boundary.v1.json](./foundation-platform-boundary.v1.json) | 파운데이션/공짱 경로 소유권 계약 | 사용 중 |
| [foundation-platform-catalog-api-contract.v1.pin.json](./foundation-platform-catalog-api-contract.v1.pin.json) · [foundation-platform-webhook-receiver-contract.v1.pin.json](./foundation-platform-webhook-receiver-contract.v1.pin.json) | 파운데이션 계약의 소비자 핀 | 사용 중 |
| [platform-integration/](./platform-integration/index.v1.json) | 플랫폼 연동 정책 선언 (`index.v1.json` + 정책별 파일) | 사용 중 |
| [traffic-auth-policy-registry.v1.json](./traffic-auth-policy-registry.v1.json) | 트래픽·인증 정책의 단일 정본(손으로 편집). 생성기가 읽어 산출물을 다시 만든다 | 사용 중 |
| [traffic-auth-policy-registry/](./traffic-auth-policy-registry/README.md) | 위 정본과 생성기·어긋남 검사 안내 | 사용 중 |

## 현재 확정된 원칙

1. **세 서비스의 데이터 접근** — [영역 AGENTS.md 의 소유 표](../../AGENTS.md)
2. **계층 분리** — 도메인 크레이트는 프레임워크·DB·HTTP 클라이언트에 의존하지 않는다 ([layers.md](./layers.md))
3. **파운데이션이 카탈로그의 정본** — 필지 도형, PNU 기준점, 공공·참조 공간 레이어
4. **공짱은 소비자 제품 의미의 정본** — 매물, 매물 사진, 사용자, 시장, 인사이트
5. **좌표계 규칙** — 파운데이션 계약이 SRID 를 밝히고, 공짱의 읽기 모델은 그 사본임을 드러낸다

요청·데이터 경로 다이어그램은 [data-flow.md](./data-flow.md)가 소유한다.
