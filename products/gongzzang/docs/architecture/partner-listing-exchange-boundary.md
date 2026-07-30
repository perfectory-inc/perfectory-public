---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# 파트너 매물 교환 경계

상태: 유지 중인 공개 계약

## 목적

이 계약은 제공기관 중립 매물 교환의 소유권 경계를 정의한다. 제공기관 이름·계정 바인딩·엔드포인트
목록·필드 카탈로그·원천 문서·측정 용량·현재 출시 상태는 비공개 운영 기록에 두며 이 저장소에 넣지 않는다.

## 소유권

| 영역 | 소유자 | 규칙 |
|---|---|---|
| 매물 정본 상태 | Gongzzang | Gongzzang application command로 생성·수정·검토·공개 |
| 교환 payload와 lineage | Gongzzang | 제품의 승인된 audit·보존 정책으로 수신/발행 payload 보존 |
| provider mapping adapter | Gongzzang | 외부 계약을 매물 후보 또는 outbound 요청으로 번역 |
| 필지·건물·PNU·주소 reference | Foundation Platform | 공개 계약만 소비하고 Foundation storage 직접 접근 금지 |
| 직원·서비스 권한 | Identity Platform | 발급 identity와 공개 authorization 계약 사용 |

## Canonical Flow

```text
provider-neutral exchange payload
  -> immutable exchange evidence
  -> provider adapter and validation
  -> listing candidate
  -> policy or staff review when required
  -> Gongzzang listing command
  -> canonical listing state
  -> outbound exchange request and delivery status
```

교환 transport는 push 또는 pull일 수 있다. transport 선택은 소유권을 바꾸지 않으며 provider adapter가
정본 table을 직접 쓰는 것을 허용하지 않는다.

## Invariants

- 외부 ID는 provider namespace identity로 남고 Gongzzang 매물 ID가 되지 않는다.
- inbound replay와 outbound retry는 멱등적이다.
- raw 교환 증거는 매물 정본 상태가 아니다.
- 정본 쓰기는 Gongzzang application layer를 거친다.
- 서비스 간 직접 DB 접근은 금지한다.
- Foundation reference data는 enrichment·검증을 위해 공개 계약으로만 사용한다.
- provider별 schema와 live binding은 private runtime 입력이다. 공개 계약에서 빠진 것은 의도적이다.

## 범위 밖

- Gongzzang 매물 소유권을 Foundation Platform으로 옮기는 것
- provider proprietary 연동 자료를 공개하는 것
- Identity Platform 내부를 exchange adapter에 노출하는 것
- 현재 partner rollout이나 운영 queue를 아키텍처로 취급하는 것
