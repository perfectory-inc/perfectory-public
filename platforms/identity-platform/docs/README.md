---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-07-28
---

# Identity Platform 문서 안내

Identity Platform은 직원·서비스 신원 인증과 인가 정책 결정 API를 소유합니다.
제품 인증은 각 제품이, 공공데이터·카탈로그는 Foundation이 소유합니다.

## 문서 트리

```text
docs/
├── adr/         설계 결정
├── openapi/     Identity HTTP 계약
└── runbooks/    workload identity 운영 절차
```

## 먼저 보는 문서

- [Identity API 계약](./openapi/identity.v1.json)
- [Workload identity 발급·회전 런북](./runbooks/workload-identity-provisioning.md)
- [영역 ADR](./adr/README.md)
- [플랫폼 시작 안내](../README.md)
- [전체 모노레포 문서 색인](../../../docs/document-catalog.md)

## 문서 배치

| 폴더 | 책임 |
|---|---|
| `adr/` | Identity 고유 설계 결정 |
| `openapi/` | 외부에 공개하는 HTTP 계약 |
| `runbooks/` | 발급·회전 등 운영 절차 |

루트 공통 규칙은 [루트 문서 안내](../../../docs/README.md)와
[AGENTS.md](../../../AGENTS.md)를 따릅니다.
