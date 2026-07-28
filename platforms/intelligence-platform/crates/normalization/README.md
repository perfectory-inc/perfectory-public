# intelligence normalization

LLM을 이용해 정규화 proposal을 생성하고 Foundation에 제출하는 계층입니다. canonical
데이터 적용 권한은 Foundation에 있으며 이 crate는 proposal-only 경계를 지킵니다.

- 설계: [`docs/architecture.md`](../../docs/architecture.md)
- 검증: `cargo test -p intelligence-normalization-application`
