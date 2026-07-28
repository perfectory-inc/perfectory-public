---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-07-28
---

# Intelligence Platform 문서 안내

Intelligence Platform은 LLM 정규화 제안을 만들고 Foundation에 제출합니다.
Foundation 원장과 제품 데이터는 이 영역이 직접 쓰지 않습니다.

## 문서 트리

```text
docs/
├── adr/             설계 결정
└── architecture.md  모듈 경계·플랫폼 계약
schemas/
└── README.md        Avro 이벤트·C2 검증
```

## 먼저 보는 문서

- [모듈 경계와 플랫폼 계약](./architecture.md)
- [Avro 이벤트 스키마와 C2 검증](../schemas/README.md)
- [영역 ADR](./adr/README.md)
- [플랫폼 시작 안내](../README.md)
- [전체 모노레포 문서 색인](../../../docs/document-catalog.md)

## 문서 배치

| 위치 | 책임 |
|---|---|
| `architecture.md` | 모듈 경계와 Foundation 연동 규칙 |
| `adr/` | Intelligence 고유 설계 결정 |
| `../schemas/` | 이벤트 스키마와 호환성 규칙 |

루트 공통 규칙은 [루트 문서 안내](../../../docs/README.md)와
[AGENTS.md](../../../AGENTS.md)를 따릅니다.
