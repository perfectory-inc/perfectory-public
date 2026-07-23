# ADR 0012: 파이프라인 시각화 검토

- 상태: Superseded
- 작성일: 2026-05-03
- 대체 결정: [ADR 0048](./0048-horizontal-platform-redefinition.md)

## 역사적 결정

초기 Gongzzang 내부 데이터 파이프라인을 React Flow로 시각화하는 방안을 검토했다.
이후 수평 플랫폼 경계가 확정되면서 공공 데이터 수집과 오케스트레이션은 Foundation
Platform 책임이 되었고, Gongzzang은 해당 운영 UI나 실행 장부를 소유하지 않는다.

React Flow 채택안은 구현되지 않았으며 현재 의존성 계약도 아니다. 통합 운영 화면은
Dawneer의 별도 제품 설계에서 실제 요구가 확인될 때 새 ADR로 결정한다.
