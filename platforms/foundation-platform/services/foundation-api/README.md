# foundation-api

Foundation 카탈로그·원장·상태를 외부 플랫폼에 공개하는 Axum HTTP 서비스입니다.
공개 경로와 응답 형식은 OpenAPI 계약을 정본으로 사용합니다.

- 계약: [`docs/openapi/catalog.v1.json`](../../docs/openapi/catalog.v1.json)
- 실행: `cargo run -p foundation-api`
- 검증: `cargo test -p foundation-api`
- 헬스: `/healthz` · `/readyz` · `/metrics`
