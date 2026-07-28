---
status: current
owner: foundation-platform
---

# 공공데이터 문서 안내

이 문서는 perfectory 루트에서 공공데이터 문서를 찾기 위한入口입니다. 실제 데이터 정의와 수집 상태는 Foundation이 소유합니다.

## 기준 문서

| 목적 | 문서 |
|---|---|
| 사람이 읽는 전체 데이터 목록 | [Foundation 공공데이터 수집 카탈로그](../platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md) |
| 정확한 endpoint·source slug·수집 허용 여부 | [공공 소스 endpoint 카탈로그](../platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json) |
| 실행 레인과 기본 실행 여부 | [Bronze 수집 레인 레지스트리](../platforms/foundation-platform/docs/catalog/public-data-bronze-lane-registry.v1.json) |
| 제공기관·데이터명·slug 매핑 | [Bronze source slug 매핑](../platforms/foundation-platform/docs/catalog/bronze-source-slug-rename.v1.md) |
| 실행 게이트·운영 절차 | [공공데이터 Bronze 수집 런북](../platforms/foundation-platform/docs/runbooks/public-data-bronze-lane-orchestration.md) |

## 책임 경계

- Foundation: 공공데이터 수집, R2 Bronze, Postgres 원장, Silver/Gold 승격 기준
- Intelligence: 정규화 제안만 제출하며 Foundation 원장을 직접 변경하지 않음
- Gongzzang: Foundation이 공개한 계약을 소비

루트 문서는 목록을 복제하지 않습니다. 목록을 변경할 때는 Foundation의 JSON SSOT를 수정하고 자동 생성 문서를 갱신합니다.

## 관련 문서

- [전체 문서 지도](./README.md)
- [기술 스택](./technology-stack.md)
- [Foundation 문서 지도](../platforms/foundation-platform/docs/README.md)
