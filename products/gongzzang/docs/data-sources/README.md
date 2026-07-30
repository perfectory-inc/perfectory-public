---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# 데이터 원천

이 폴더에는 원천 경계 규칙만 기록하며 로컬 Catalog client 구현은 두지 않는다.

V-World와 data.go.kr 같은 Catalog 원천 연동은 M3.2 물리 추출 이후 Foundation Platform이 소유한다.
Gongzzang은 원천을 직접 호출하지 않고 Foundation Platform 계약으로 Catalog 데이터를 소비한다.

## 등록된 원천

| 원천 | 소유자 | Gongzzang 진입점 | 문서 |
|---|---|---|---|
| V-World | Foundation Platform Catalog | Foundation Platform contracts only | [v-world.md](./v-world.md) |
| data.go.kr Catalog APIs | Foundation Platform Catalog | Foundation Platform contracts only | [data-go-kr.md](./data-go-kr.md) |
| Korean law API | Gongzzang only when product feature needs it | Direct API with breaker/audit/raw lineage | [korean-law.md](./korean-law.md) |
| NICE identity | Gongzzang auth/compliance | Direct provider integration | [nice-identity.md](./nice-identity.md) |
| Naver Maps | Gongzzang frontend/map UX | Approved maps integration | [naver-maps.md](./naver-maps.md) |

## 기본 시스템 규칙

Gongzzang이 소유한 외부 호출은 timeout·retry·circuit breaker·관측성·audit/logging 규칙을 사용한다.
V-World와 data.go.kr의 Catalog raw lineage는 Gongzzang이 아니라 Foundation Platform에 둔다.

## 에이전트 전용 규칙

MCP 도구는 개발 탐색에 사용할 수 있지만 `apps/`, `services/`, `crates/`, `packages/`가 MCP/LLM SDK를
import하면 안 된다.
