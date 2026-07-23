---
status: current
---

# perfectory docs — 모노레포 문서 지도

문서는 설명하는 코드 옆에 둔다 (docs-as-code). 루트 `docs/`는 모노레포 횡단
문서만 담고, 영역 문서는 각 영역 `docs/`에 있다.
물리 배치: [ADR-0003](./adr/0003-docs-physical-taxonomy.md) ·
분류(현행|기록): [ADR-0002](./adr/0002-docs-taxonomy-and-archive.md).

## 루트 docs/

| 경로 | 내용 |
| --- | --- |
| [README.md](./README.md) | 이 지도 |
| [adr/](./adr/README.md) | 전역 ADR 시퀀스 — 신규 결정은 영역 무관 여기서 번호를 받는다 |

## 영역 docs/ 진입점

| 영역 | 진입점 |
| --- | --- |
| gongzzang (product) | [products/gongzzang/docs/](../products/gongzzang/docs/README.md) |
| foundation-platform | [platforms/foundation-platform/docs/](../platforms/foundation-platform/docs/) |
| identity-platform | [platforms/identity-platform/docs/](../platforms/identity-platform/docs/) |
| intelligence-platform | [platforms/intelligence-platform/docs/](../platforms/intelligence-platform/docs/) |

## 표준 골격 (ADR-0003)

각 영역 `docs/`의 공개 트리는 아래 7종으로 수렴한다 (영역 이동은 후속 단계 진행 중).
`architecture/`·`openapi/`·`adr/`은 코드·CI에 배선된 계약이라 이동 금지.

| 항목 | 정의 |
| --- | --- |
| `README.md` | 영역 문서의 지도 — 진입점·색인 |
| `adr/` | 결정 기록 — 영역 시퀀스 동결, 신규는 루트 전역 시퀀스 |
| `architecture/` | 시스템 설명 + 코드 배선 계약 (pin·boundary·registry JSON) |
| `openapi/` | 생성 산출물 — HTTP API를 내는 영역만 |
| `runbooks/` | 운영 절차 |
| `guides/` | 개발 how-to |
| `reference/` | 용어·스키마·레지스트리 |

과거 plans/specs/handoff/research/migration 기록은 공개 코드 트리가 아니라
[ADR-0007](./adr/0007-public-code-private-operations-boundary.md)의 비공개 전환 archive에
보존한다. 현재 계약은 `adr/`, `architecture/`, `runbooks/` 또는 코드로 승격해야 한다.
