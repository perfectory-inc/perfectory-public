# ADR 0014: 지도 기본 레이어 PMTiles 보류

- 상태: Superseded
- 작성일: 2026-05-06
- 대체 결정: [ADR 0016](./0016-medallion-base-layer-postgis-silver-pmtiles-gold.md)

## 역사적 결정

초기 Gongzzang 저장소에서 V-World 및 data.go.kr 원본을 직접 수집하고 R2 PMTiles를
생성하는 방안을 검토했으나, 데이터 수집과 공공 기준 레이어의 소유권은 Foundation
Platform으로 확정되었다. Gongzzang은 Foundation Platform이 발행한 계약과 불변
아티팩트만 소비한다.

PMTiles 생성과 Medallion 계층의 최종 결정은 ADR 0016이 소유한다. 이 문서는 과거
결정 번호와 대체 관계를 보존하기 위한 기록이며 구현 지침이 아니다.
