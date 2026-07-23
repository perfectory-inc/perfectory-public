# 0001. 모노레포 공통 컨벤션 상속

- Status: Accepted
- Date: 2026-07-20

## Context

identity-platform 은 perfectory 모노레포로 통합되었고, 통합 시점까지 영역
자체 ADR 이 없었다. 툴체인·커밋·마이그레이션·OpenAPI·CI 등 공통 규칙의
SSOT 는 루트
[ADR-0001](../../../../docs/adr/0001-monorepo-governance-and-conventions.md)이다.

## Decision

identity-platform 은 루트 ADR-0001 의 모노레포 거버넌스·컨벤션을 그대로
상속한다. 이 영역에서 별도 결정이 필요한 사안(인증 모델, 정책 결정 계약,
데이터 소유 경계 등)은 `0002` 부터 이 디렉토리에 영역 ADR 로 기록한다.

## Consequences

- 공통 규칙 변경은 루트 ADR 에서만 일어나고, 이 영역은 링크로만 참조한다.
- 영역 고유 결정 이력이 `docs/adr/` 에 축적되어 다른 영역과 동일한
  탐색 경로(`영역/docs/adr/`)를 갖는다.
