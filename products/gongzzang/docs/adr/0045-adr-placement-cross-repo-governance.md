# ADR-0045: ADR 배치와 영역 간 거버넌스

| | |
|---|---|
| Date | 2026-06-20 |
| Status | **Superseded by root ADR-0001** |
| Decision owner | perfectoryinc (platform owner) |

## 배경

모노레포 통합 전에는 아키텍처 결정이 독립적으로 관리되는 여러 codebase에 흩어져
정본 위치가 모호했다. 원래 결정은 공유 ADR의 임시 위치로 Gongzzang을 사용했다.

그 규칙은 이제 repository topology와 맞지 않는다. 유지하면 하나의 monorepo 안에 두
개의 거버넌스 위치가 생긴다.

## 현재 결정

- 한 product나 platform 영역에 한정된 결정은 해당 영역의 `docs/adr/` directory에 둔다.
- 여러 영역이나 repository 전체 규칙을 지배하는 결정은 root `docs/adr/`에 둔다.
- area ADR은 root ADR을 가리킬 수 있지만 규범 contract를 복제하지 않는다.
- Dawneer를 포함한 외부 product·consumer는 발행된 contract로 통합하며, 존재 자체가
  별도의 architecture SSOT를 만들지 않는다.

이 역사적 ADR이 현재 monorepo 배치와 충돌하면 root ADR-0001과 root `AGENTS.md`가
권위 있는 기준이다.

## 영향

- 결정 배치는 소유권과 범위를 따른다.
- 영역 간 규칙은 찾기 쉬운 root 위치 하나만 갖는다.
- 이 monorepo가 code SSOT인 동안 별도 거버넌스 repository는 필요하지 않다.

## 참고 문서

- [Root ADR-0001](../../../../docs/adr/0001-monorepo-governance-and-conventions.md)
- [Root AGENTS.md](../../../../AGENTS.md)
