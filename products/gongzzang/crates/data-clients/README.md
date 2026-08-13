---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# crates/data-clients

이 디렉터리는 Gongzzang이 소유하는 비-Catalog 외부 API 안티코럽션 어댑터만 둔다.

Foundation Platform으로 분리한 뒤 Catalog 원천 어댑터는 이 저장소 소유가 아니다. V-World 필지 데이터,
data.go.kr Catalog API, Catalog 원자료 보관, 공개·기준 공간 데이터 조회기를 이곳에 다시 만들지 않는다.
Gongzzang은 Foundation Platform의 공개 계약·이벤트 수신자·승인된 조회 모델 산출물을 통해서만 이를
소비한다.

허용할 미래 어댑터는 코드를 추가하기 전에 ADR과 경계 갱신이 필요하다.
Catalog 소유 경로가 아닌 Gongzzang 소유 Identity·법령·지도·embedding 제공기관은 예가 될 수 있다.

## 규칙

- 모든 외부 호출은 Circuit Breaker·재시도·timeout·감사 로그를 사용한다.
- API 키는 승인된 시크릿 로더나 환경변수로만 읽는다.
- 응답 계보는 소유 서비스 계약에 속한다. Gongzzang은 Catalog 원자료 테이블이나 로컬 원자료 수집
  crate를 추가하지 않는다.
- 어댑터는 Gongzzang 소유 DTO나 port를 반환하며 외부 스키마를 도메인 crate에 노출하지 않는다.

자세한 내용은 [docs/data-sources](../../docs/data-sources/README.md)와
[docs/backend/circuit-breaker.md](../../docs/backend/circuit-breaker.md).
