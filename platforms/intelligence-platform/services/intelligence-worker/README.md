# intelligence-worker

정규화 작업·이벤트 소비·outbox 전달을 담당하는 백그라운드 worker입니다. 운영 이벤트
백본의 활성 여부는 C2 live 검증 결과와 런타임 설정을 따릅니다.

- 스키마 검증: [`schemas/README.md`](../../schemas/README.md)
- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p intelligence-worker`
