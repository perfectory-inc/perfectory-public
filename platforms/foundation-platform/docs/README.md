---
status: current
owner: foundation-platform
---

# Foundation Platform 문서 지도

Foundation은 공공데이터 수집과 데이터 원장 SSOT를 소유합니다. 루트에서 시작하려면 [공공데이터 문서 안내](../../../docs/public-data-catalog.md)를 먼저 보세요.

## 문서 트리

```text
docs/
├── adr/             설계 결정
├── architecture/    시스템 경계·실행 구조
├── catalog/          데이터·endpoint·정책 정본
├── data-quality/    품질 규칙
├── db/              DB 계약
├── events/          이벤트 계약
├── openapi/         HTTP 계약
├── observability/   SLO·관측성
├── runbooks/        운영 절차
└── security/        보안·공급망
```

## 가장 먼저 보는 문서

- [공공데이터 수집 카탈로그](./catalog/public-data-collection-catalog.md)
- [공공 소스 endpoint 카탈로그](./catalog/public-source-endpoint-catalog.v1.json)
- [Bronze 수집 레인 레지스트리](./catalog/public-data-bronze-lane-registry.v1.json)
- [공공데이터 Bronze 수집 런북](./runbooks/public-data-bronze-lane-orchestration.md)
- [전체 모노레포 문서 색인](../../../docs/document-catalog.md)

## 문서 영역

| 영역 | 책임 |
|---|---|
| `adr/` | Foundation의 설계 결정과 불변식 |
| `architecture/` | 시스템 경계, 저장소, 실행 구조 |
| `catalog/` | 데이터·endpoint·정책·파이프라인 카탈로그 |
| `data-quality/` | 품질 규칙과 검증 계약 |
| `db/` | 데이터베이스 계약 fixture |
| `events/` | 이벤트와 웹훅 계약 |
| `openapi/` | 외부 HTTP 계약 |
| `observability/` | SLO와 관측성 계약 |
| `runbooks/` | 실행·장애 대응·복구 절차 |
| `security/` | 보안·공급망 계약 |

## SSOT 원칙

- 현재 endpoint 목록과 source slug는 `catalog/public-source-endpoint-catalog.v1.json`이 기준입니다.
- 어떤 레인으로 실행하는지는 `catalog/public-data-bronze-lane-registry.v1.json`이 기준입니다.
- 사람이 읽는 카탈로그는 JSON에서 생성되며 직접 목록을 편집하지 않습니다.
- 예제 JSON과 fixture는 테스트 계약이므로 사용처를 확인하지 않고 삭제하지 않습니다.
