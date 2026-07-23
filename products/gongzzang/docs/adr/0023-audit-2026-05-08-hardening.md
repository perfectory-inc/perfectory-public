# ADR 0023: 2026-05-08 보안 및 신뢰성 감사

- 상태: Accepted (historical audit record)
- 작성일: 2026-05-08
- 소유권 후속 결정: [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md),
  [ADR 0048](./0048-horizontal-platform-redefinition.md)

## 확인된 원칙

감사에서 내부 인증 이벤트의 무인증 수신, 인증 차단 목록 장애 시 fail-open, 필수 설정
누락 시 조용한 대체 동작, 지도 오류의 무관측성이 발견되었다. 다음 원칙을 Gongzzang
런타임에 적용한다.

- 내부 이벤트는 인증된 서비스 호출만 수락한다.
- 운영 인증 경로는 의존 서비스 장애 시 fail-closed 한다.
- 필수 운영 설정이 없으면 시작 단계에서 실패한다.
- 사용자 기능의 핵심 오류는 구조화 로그와 추적 식별자로 관측한다.
- 외부 Catalog 원본 수집은 Gongzzang에서 구현하지 않는다.

## 소유권 정리

감사 당시 Gongzzang에 있던 V-World/data.go.kr 수집, 원본 보관, API 드리프트 감시는
Foundation Platform으로 이관되었다. Gongzzang은 Foundation Platform의 공개 계약과
불변 아티팩트만 소비한다. 삭제된 로컬 수집 구현을 복원하는 후속 작업은 금지한다.

이 문서는 당시 감사의 결정 원칙만 보존한다. 구체적인 현재 경계는
`docs/architecture/foundation-platform-boundary.v1.json`이 소유한다.
