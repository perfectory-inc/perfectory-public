---
status: current
owner: repository-maintainers
doc_type: README
last_reviewed: 2026-07-28
---

# perfectory 문서 지도

루트 `docs/`에는 모노레포 전체에 적용되는 규칙과 진입점만 둡니다. 각 플랫폼의 실제 책임 문서는 해당 플랫폼의 `docs/`가 소유합니다.

## 문서 트리

```text
docs/
├── README.md                  이 문서 지도
├── document-catalog.md        전체 문서 자동 색인
├── adr/                       전역 설계 결정
├── architecture/             모노레포 전체 구조·경계
├── guides/                    전역 개발 안내
├── glossary.md                전역 용어 정본
└── technology-stack.md        기술·버전·환경 기준
```

## 루트 문서

| 문서 | 책임 |
|---|---|
| [공공데이터 문서 안내](./public-data-catalog.md) | Foundation 공공데이터 문서의 루트 진입점 |
| [기술 스택](./technology-stack.md) | 전역 기술·버전·환경 기준 |
| [전역 용어집](./glossary.md) | 문서에서 사용하는 한글 용어 정본 |
| [ADR](./adr/README.md) | 모노레포 전역 설계 결정 |
| [문서 운영 안내](./guides/documentation-lifecycle.md) | 문서 작성·검토·기록·번역 절차 |
| [생산 준비 로드맵](./architecture/platform-production-readiness-roadmap.md) | 전역 출시 준비 순서 |
| [전체 문서 색인](./document-catalog.md) | 영역·유형·상태별 자동 생성 문서 목록 |

## 플랫폼별 문서

| 영역 | 문서 지도 |
|---|---|
| Gongzzang 제품 | [products/gongzzang/docs](../products/gongzzang/docs/README.md) |
| Foundation Platform | [platforms/foundation-platform/docs](../platforms/foundation-platform/docs/README.md) |
| Identity Platform | [platforms/identity-platform/docs](../platforms/identity-platform/docs/README.md) |
| Intelligence Platform | [platforms/intelligence-platform/docs](../platforms/intelligence-platform/docs/README.md) |

## 문서 배치 규칙

- `adr/`: 전역 설계 결정
- `architecture/`: 모노레포 전체 구조와 경계
- 각 플랫폼의 `docs/`: 해당 플랫폼의 계약, 카탈로그, 런북
- 문서 내용은 한 곳만 정의하고 다른 위치에서는 링크만 둡니다.
- 예제·fixture 문서는 코드와 테스트가 참조할 수 있으므로 사용처를 확인하지 않고 삭제하지 않습니다.

상세 규칙은 [ADR 0002](./adr/0002-docs-taxonomy-and-archive.md)와 [ADR 0003](./adr/0003-docs-physical-taxonomy.md)를 따릅니다.
