# ADR 0001 - gongzzang ADR 상속

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-11 |
| 상태 | Accepted |

## 결정

foundation-platform 는 gongzzang 의 다음 ADR 결정을 그대로 상속한다.
현재 ADR 파일은 모노레포 경로 `products/gongzzang/docs/adr` 에 있다
(이 파일 기준 [`../../../../products/gongzzang/docs/adr/`](../../../../products/gongzzang/docs/adr/)).

| ADR | 주제 | 상속 이유 |
|---|---|---|
| 0001 | language-rust-ts | 동일 stack (Rust + TS consumer) 유지 |
| 0002 | monorepo-cargo-pnpm-turbo | foundation-platform 는 단일 Cargo workspace. TS pnpm 측은 consumer 의 영역 |
| 0004 | db-postgres-postgis | foundation-platform 도 Postgres. PostGIS 는 polygon 컬럼 진입 시 활성 |
| 0005 | auth-zitadel | Staff Identity Context 의 upstream |
| 0006 | api-rest-openapi | 동일 — OpenAPI 3.1 SSOT, OpenAPI 에서 Rust + TS 양쪽 코드 생성 |
| 0008 | observability | tracing + Sentry. foundation-platform 가 자체 환경에서 운영 |
| 0009 | iac-pulumi | Pulumi |
| 0018 | pnu-first-identity | shared-kernel 의 PNU 검증 규칙이 gongzzang 과 동일해야 함 |
| 0029 | explicit-environment-separation | foundation-platform 도 동일 env namespace 정책 |
| 0036 | static-vector-tile-runtime-contract | foundation-platform cutover 이후 Catalog 가 manifest owner. 상세 결정은 ADR 0004 로 확정 |

## 컨텍스트

foundation-platform 가 별도 repo 라고 해서 무관한 결정 트리를 가질 필요는 없다. 같은 팀이
운영하고, gongzzang 의 안정화된 결정이 그대로 통한다.

새로운 foundation-platform 특화 결정은 이 repo 의 ADR 002+ 로 추가한다 (예: outbox publisher
배포 토폴로지, OpenAPI generator 도구 선택 등).

## 참고

- [gongzzang ADR index](../../../../products/gongzzang/docs/adr/)
- [Horizontal platform decision](0021-adopt-horizontal-platform-redefinition.md)
