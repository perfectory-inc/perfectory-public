---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# 스테이징 Identity 연동 계약

## 상태

실제 제공기관 스테이징 검증은 배포 소유 출시 게이트다. 일반 변경 검증은 결정적인 로컬 Identity
fixture를 사용하며 저장소 시크릿이나 실제 tenant가 필요하지 않다.

## 필요한 스테이징 증거

운영 승격 전에 승인된 운영자가 전용 non-production tenant에서 다음을 모두 증명해야 한다.

1. 승인된 machine 또는 test-user 흐름으로 표준 OIDC access token을 얻는다.
2. 배포된 API에서 issuer·audience·signature·expiry·필수 claim을 검증한다.
3. 허용 요청과 거부 요청을 종단 간 실행한다.
4. key rotation과 provider unavailable 상황이 fail-closed임을 증명한다.
5. redacted 증거를 공개 저장소 밖에 기록한다.

staging workflow·tenant ID·client ID·credential은 private 배포 경계에 속한다. 나중에 자동화를 추가하면
protected environment, 단기 credential, 멱등 setup, 명시적 cleanup을 사용해야 한다. 그 경로는 이
공개 계약에 포함하지 않는다.

## 변경 검증과 분리

신뢰하지 않는 pull-request 코드는 staging credential을 받아서는 안 된다. 저장소 CI는 fixture로
token-verifier 동작을 검증할 수 있지만 fixture 테스트 통과는 실제 provider 출시 증거가 아니다.
