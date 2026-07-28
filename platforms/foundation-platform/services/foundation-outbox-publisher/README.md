# foundation-outbox-publisher

Foundation 운영 CLI입니다. 국가 공공데이터 수집, Bronze ledger resume, outbox 발행,
검증·복구 명령을 실행하며 정책과 계약은 Foundation 문서 정본을 읽습니다.

- 수집 카탈로그: [`docs/catalog/public-data-collection-catalog.md`](../../docs/catalog/public-data-collection-catalog.md)
- 실행 절차: [`docs/runbooks/public-data-bronze-lane-orchestration.md`](../../docs/runbooks/public-data-bronze-lane-orchestration.md)
- 실행: `cargo run -p foundation-outbox-publisher -- --help`
- 검증: `cargo test -p foundation-outbox-publisher`
