# system-alert-domain

운영자가 확인하고 처리하는 시스템 알림을 소유하는 `SystemAlert` 도메인 crate에요.

## 책임

- spec § 5.5 `system_alert` 테이블에 대응하는 Aggregate를 정의해요.
- severity, acknowledge, resolve 불변식을 소유해요.
- `SystemAlertRepository`가 이 capability의 저장/조회 계약을 소유해요.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `SystemAlertMarker`).
