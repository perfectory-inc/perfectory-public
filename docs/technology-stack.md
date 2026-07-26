# perfectory 역할별 기술 스택·환경 기준표

이 문서는 모노레포에서 **무슨 역할에 어떤 기술을 쓰는지**, 그리고 그 역할이
local/CI/staging/production에서 어떤 backend를 바라보는지를 한 곳에 고정한다.
실제 코드·manifest·Compose·ADR과 충돌하면 실제 정의가 우선이며, 이 문서는 그 drift를
찾아내고 변경 절차를 강제하는 기준표다.

## 운영 원칙

1. 같은 역할은 하나의 canonical 기술과 버전만 사용한다.
2. local/CI/staging/production의 차이는 endpoint, credentials, tenancy, capacity뿐이다.
3. 다른 major/minor 버전이나 다른 제품을 쓰려면 먼저 ADR·호환성 검증·마이그레이션을 남긴다.
4. `Redis`라는 환경변수·crate명은 Redis wire protocol client 계약일 뿐, runtime 제품은
   Valkey 8이다.
5. `local/CI 전용`과 `production 선정·배선 완료`를 같은 상태로 기록하지 않는다.

출시 전 임시 운영 정책: 개발자 PC에서 실행하더라도 현재 private `.env.local`은
`FOUNDATION_PLATFORM_RUNTIME_ENV=production`으로 명시하고, 이미 존재하는 production R2와
Iceberg REST 카탈로그를 공유한다. `FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer`와
`FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1`도 함께 요구한다. 이는 `local` 환경의 정의를 바꾸는 것이 아니다. Postgres,
Valkey, Kafka, Identity, Spark/Trino compute처럼 실제 production endpoint가 레포에 없는
백엔드는 로컬 구성을 유지하며, 주소를 추측해 운영에 연결하지 않는다. 외부 출시 전에
`runtime=local`과 전용 dev R2로 되돌리고 나머지 비운영 백엔드도 분리한다.

상태는 다음처럼 표시한다.

- **실행 중**: 코드/Compose/manifest에 실제 호출 경로가 있다.
- **로컬·CI 전용**: 개발·계약 테스트에만 연결되어 있다.
- **계획·미연결**: adapter/template/ADR은 있으나 production provider와 배선이 없다.

## 1. canonical 버전 기준

| 역할 | canonical 기술/버전 | 근거·현재 상태 |
|---|---|---|
| Rust backend/toolchain | Rust `1.96.0` | 루트 `rust-toolchain.toml`, Docker builder, `docs/adr/0001`가 단일 기준. 실행 중 |
| HTTP backend | Axum `0.8`, Tokio workspace | 네 플랫폼의 API/worker Cargo manifest. 실행 중 |
| JavaScript runtime | Node `20.19.0` | `products/gongzzang/.nvmrc`, CI `node-version`, package `engines` exact pin |
| JavaScript package manager | pnpm `9.12.0` | `products/gongzzang/package.json#packageManager` exact pin |
| Frontend core | Next.js `16.2.6`, React/React DOM `19.2.5`, TypeScript `5.9.3` | Gongzzang workspace manifests와 lockfile exact pin |
| Frontend build/test | Tailwind `4.2.4`, Vite `6.4.2`, Vitest `4.1.7`, Turborepo `2.9.15`, Biome `2.4.14` | Gongzzang manifests/lockfile exact pin |
| Relational database | PostgreSQL `17` | 모노레포 ADR 0001의 전역 규칙. 모든 Compose DB 이미지 통일 |
| Spatial database | PostGIS `3.5` on PostgreSQL `17` | Foundation/Gongzzang/tile proof Compose 통일 |
| Cache | Valkey `8` (Redis protocol) | Gongzzang ADR 0007 및 모든 앱 Compose 통일 |
| Object storage | Cloudflare R2, S3 API | `aws-sdk-s3` adapter. 출시 전에는 private 설정으로 production R2 공유; 이후 dev R2 분리. MinIO 미채택 |
| C2 broker | Redpanda `v24.3.6` | Intelligence local/CI compose 전용 |
| C2 schema registry | Karapace `6.2.0` | Intelligence local/CI compose 전용 |
| Lakehouse compute | Spark `3.5.6`, Trino `481` | Foundation lakehouse Compose |
| Lakehouse table/catalog | Iceberg Spark runtime `1.6.1`, R2 REST catalog 설정 | 자격증명 주입 시 실행; 기본 Compose는 shell |
| SQL transform | dbt-trino project | 9개 model, staging/intermediate/silver만 존재 |
| Observability | Prometheus `v3.5.0`, Alertmanager `v0.28.1`, tracing/OpenTelemetry/Sentry adapters | Compose/manifest 실행 경로 확인 |
| Identity provider | Zitadel `v2.65.1` local image | local Compose만 실행 중; production provider/version 배선 미완료 |

## 2. 역할별 환경 매트릭스

| 역할/소유자 | local | CI | staging | production | 상태 |
|---|---|---|---|---|---|
| 공공 source HTTP / Foundation | `reqwest` + rustls로 VWorld/data.go.kr/hub.go.kr/rt.molit 호출 | mock/fixture 또는 보호된 live smoke | 같은 HTTP adapter + staging credentials | 같은 HTTP adapter + production credentials/quota | 실행 중. provider credential은 환경별 |
| Bronze raw bytes / Foundation | 기준은 원격 dev R2지만 출시 전 private 설정은 production R2 공유 | 보호된 live smoke는 전용 CI R2, credential-free smoke는 logging adapter | 전용 staging R2 bucket | 전용 production R2 bucket | 실행 중. `FOUNDATION_PLATFORM_RUNTIME_ENV`와 bucket guard가 강제 |
| Bronze ledger / Foundation | PostgreSQL 17 + SQLx | ephemeral PostgreSQL | staging PostgreSQL 17 | production PostgreSQL 17 | 실행 중. DB endpoint만 변경 |
| outbox/event ledger / Foundation | PostgreSQL outbox + 선택형 Kafka 전달기 + 기존 웹훅/로그 전달기 | 고정 Redpanda/Karapace 실브로커 + Postgres round-trip + outage 계약 테스트 | 관리형 Kafka 호스트와 인증정보 필요 | 관리형 Kafka와 주제/권한 운영 필요 | Kafka는 명시적으로 켤 때만 사용하며 Postgres outbox가 원장; 운영 전 `kafka-integration` 게이트 필수 |
| collection JobBus / Foundation | PostgresJobBus claims/acks are used by the live hub.go.kr bulk collector | in-memory/fixture + disposable Postgres contract test | same adapter with staging PostgreSQL | same adapter with production PostgreSQL | legacy API executor is disabled; dry-run intentionally skips DB/JobBus |
| Silver handoff / Foundation | Spark/Python fixture·JSONL/Parquet | deterministic fixture jobs | Spark batch runtime + R2/Iceberg credentials 필요 | managed Spark-compatible runtime + R2/Iceberg | batch code 존재, end-to-end backend 증거 제한 |
| Gold projection / Foundation | Spark `industrial_complex_silver_to_gold.py` | contract/fixture test | Spark + catalog credentials 필요 | Spark + catalog credentials 필요 | Spark Gold job은 있으나 dbt `models/gold/` 없음 |
| Iceberg/Trino query / Foundation | Trino shell; R2 catalog properties 주입 시 실제 조회 | credential-less shell 또는 protected R2 | R2 catalog + staging credentials | R2 catalog + production credentials | Compose만으로 backend 연결은 증명되지 않음 |
| dbt models / Foundation | local Trino profile/template | model contract tests | dbt-trino target 필요 | dbt-trino target 필요 | 9개 model, Gold tier 미구현 |
| Catalog publish / Foundation | 기준은 dev R2 + local Postgres지만 출시 전 private 설정은 production R2/catalog 공유 | logging/protected smoke | staging R2/Postgres | production R2/Postgres | code path 실행 중, production rollout gate 필요 |
| normalization proposal / Intelligence | local PostgreSQL workflow + mock/provider gateway | mock or explicit protected LLM lane | LLM provider credentials 필요 | LLM provider/quotas/approval policy 필요 | proposal adapter/worker 존재, provider standard 미선정 |
| normalization review/apply / Foundation | review/apply against local Postgres | fixture DB | staging approval gate | production approval gate | Foundation write authority; Intelligence write 권한 없음 |
| C2 event broker / Intelligence·Foundation | 개발·검증은 Redpanda `v24.3.6`; 운영은 관리형 Kafka 호환 브로커 | Redpanda/Karapace 실브로커 계약 테스트 + Postgres outbox live gate | 운영 브로커 주소·인증·주제 권한 필요 | 운영 브로커·보존·권한 정책 필요 | 로컬 Redpanda는 운영 대체가 아니며 Foundation 전달기는 선택형; Kafka 원장은 Postgres outbox |
| C2 schema registry / Intelligence·Foundation | 개발·검증은 Karapace `6.2.0`; 운영은 관리형 호환 등록소 | Karapace 계약·실등록 테스트 | 운영 등록소 주소·인증 필요 | 운영 등록소와 호환성 정책 필요 | 개발용과 운영용을 같은 이름으로 섞지 않음 |
| Identity / Auth | Zitadel `v2.65.1` + PostgreSQL 17 + Valkey 8 Compose | mock/protected identity tests | Zitadel deployment/issuer 필요 | Zitadel deployment/issuer 필요 | local IdP 실행, production 배선 미완료 |
| App cache/session / Gongzzang | Valkey 8; code env is `REDIS_URL` | test fixture or isolated cache | managed Valkey 8 endpoint | managed Valkey 8 endpoint | runtime version 통일, endpoint만 변경 |
| Search | PostgreSQL/index contracts; Meilisearch ADR only | fixture | provider 미선정 | provider 미선정 | 계획·미연결 |
| Embedding/LLM | developer-owned gateway/Ollama tools | mock/protected lane | provider credentials 필요 | provider/모델/비용 정책 미선정 | 계획·미연결 |
| Metrics/alerts | Prometheus + Alertmanager Compose | CI smoke/metrics checks | managed scrape/alert target 필요 | managed scrape/alert target 필요 | local Compose 실행, production target 별도 |

## 3. 제품·플랫폼 소유권

| 소유자 | 애플리케이션 | canonical data/backend |
|---|---|---|
| Gongzzang | Rust/Axum API·worker, Next.js web | PostgreSQL 17/PostGIS, Valkey 8, listing R2 namespace |
| Foundation | collection, Bronze, Catalog, normalization apply, lakehouse control | PostgreSQL 17/PostGIS ledger, R2 raw/object, Iceberg/Spark/Trino contracts |
| Identity | identity-api, policy worker, provisioner/migrator | PostgreSQL 17 identity schema, Zitadel issuer contract |
| Intelligence | intelligence-api/worker, proposal normalization | PostgreSQL workflow state, Valkey-compatible rate-limit adapter, Foundation proposal contract |

플랫폼 간 직접 DB/Cargo path 결합은 금지하고 published HTTP/event contract만 사용한다.

## 4. 현재 남은 미통합·미선정 항목

버전 혼용은 현재 canonical 기준으로 정리했지만 다음은 “버전 문제”가 아니라 아직 운영
선택·배선이 끝나지 않은 항목이다.

1. Production Kafka broker, schema registry, topic ownership, ACL, deployment가 없다.
   Redpanda/Karapace local compose가 운영 Kafka 생산을 의미하지 않는다.
2. dbt `models/gold/`가 없고 Gold는 Spark job projection으로만 존재한다.
3. R2/Iceberg Compose는 credential-less shell을 띄울 수 있을 뿐 실제 catalog backend 연결을
   증명하지 않는다.
4. Production Zitadel/LLM/search provider는 issuer, model, quota, secret wiring이 확정되지 않았다.
5. `Postgres 16`에서 17로 바뀐 기존 로컬 데이터 볼륨은 자동 재사용하지 않는다. 백업 후
   disposable volume 또는 공식 major-upgrade 절차를 사용한다.

## 5. 기계적 가드·변경 절차

- `scripts/guard/technology-version-consistency.sh`가 tracked Compose/Dockerfile/package
  정의의 canonical DB/cache/frontend runtime 버전을 검사한다.
- `scripts/guard/toolchain-consistency.sh`가 Rust `1.96.0`과 Docker builder를 검사한다.
- 두 guard와 self-test는 `scripts/guard/monorepo-guard.sh`를 통해 기존 CI 검증 SSOT에 포함된다.
- 새 버전 도입은 이 문서 갱신 → ADR/마이그레이션 → local/CI/staging 검증 → production rollout
  순서로 한다. Compose에 임의의 두 번째 버전을 추가하지 않는다.
- Foundation의 환경/bucket policy는
  [`ADR 0029`](../platforms/foundation-platform/docs/adr/0029-runtime-environment-backend-separation.md)와
  [`runtime_environment.rs`](../platforms/foundation-platform/services/foundation-outbox-publisher/src/runtime_environment.rs)가 소유한다.

## 근거 파일

- [`docs/adr/0001-monorepo-governance-and-conventions.md`](./adr/0001-monorepo-governance-and-conventions.md)
- [`products/gongzzang/package.json`](../products/gongzzang/package.json), [`products/gongzzang/pnpm-lock.yaml`](../products/gongzzang/pnpm-lock.yaml)
- [`platforms/foundation-platform/docker-compose.yml`](../platforms/foundation-platform/docker-compose.yml)
- [`platforms/foundation-platform/compose.lakehouse.yml`](../platforms/foundation-platform/compose.lakehouse.yml)
- [`platforms/intelligence-platform/docker/c2-event-backbone.compose.yml`](../platforms/intelligence-platform/docker/c2-event-backbone.compose.yml)
- [`platforms/foundation-platform/infra/lakehouse/dbt`](../platforms/foundation-platform/infra/lakehouse/dbt)
