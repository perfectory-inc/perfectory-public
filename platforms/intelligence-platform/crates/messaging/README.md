# intelligence messaging

Kafka·Avro·Karapace 어댑터와 이벤트 계약 테스트를 담당합니다. C2 이벤트 백본은 선택적
검증 단계이며, 실제 운영 발행 여부는 런타임 구성과 검증 결과로 판단합니다.

- 스키마·검증: [`schemas/README.md`](../../schemas/README.md)
- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p messaging-infrastructure`
