# ADR 0029 - 런타임 환경별 백엔드 분리

## Status

승인됨. 이 ADR은 Foundation 런타임 백엔드의 환경 경계를 정의한다. 운영 Kafka 제공기관은 선택하지
않으며 별도 Kafka 구현 결정으로 남긴다.

## 결정

Foundation has four explicit runtime environments:

| Runtime | Object storage | Broker | Durable DB | Model/provider policy |
|---|---|---|---|---|
| `local` | Dedicated Cloudflare R2 development bucket | Redpanda + Karapace local C2 | Local Docker Postgres | Provider keys and model gateway are developer-owned |
| `ci` | Dedicated CI R2 bucket for protected live smoke; logging is allowed only for credential-free compose smoke | Redpanda + Karapace test fixture | Ephemeral Postgres service | Mock providers/models unless a protected live job says otherwise |
| `staging` | Dedicated Cloudflare R2 staging bucket | A managed production-compatible broker selected by the deployment | Staging Postgres | Staging provider/model credentials |
| `production` | `foundation-platform-lakehouse-prod` | The selected production broker | Production Postgres | Production credentials only |

개발자 환경도 의도적으로 R2를 사용한다. MinIO는 Foundation 개발 의존성이 아니다. 단위 테스트는
fake를 사용할 수 있지만 운영 명령은 런타임 환경에 선언된 백엔드를 사용해야 하며 조용히 fake로
대체하지 않는다.

## 출시 전 공유 예외

제품 출시 전까지 개발자 프로세스는 의도적으로
`FOUNDATION_PLATFORM_RUNTIME_ENV=production` and use the existing production R2/Data Catalog.
이는 새 `local` 환경이 아니라 임시 운영 선택이다. 실행 위치는
still a developer machine, while the selected backend environment is explicitly `production`.
process는 `FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer`와 좁은
acknowledgement `FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1`; the publisher rejects a developer→production
run without that acknowledgement.
실제 운영 endpoint를 비공개 운영 설정으로 제공하지 않는 한 로컬 메타데이터 DB·Valkey·Kafka·Identity
제공기관·계산 계층은 로컬로 유지한다. 로컬 Compose hostname으로 운영 주소를 추측하지 않는다. 외부
출시 전에는 프로세스를 `local`과 전용 개발 버킷으로 전환하고 비운영 endpoint를 별도로 만들고 검증한다.

## R2 버킷 식별자

비운영 버킷 이름은 다음과 같다.

- `local` (developer process): `foundation-platform-lakehouse-dev` (remote Cloudflare R2 development bucket)
- `ci`: `foundation-platform-lakehouse-ci`
- `staging`: `foundation-platform-lakehouse-staging`
- `production`: `foundation-platform-lakehouse-prod`

운영 버킷의 소유 정본은 레이크하우스 도메인의 다음 함수다.
`LakehouseOwnerService::FoundationPlatform::production_r2_bucket_name()`.
런타임 정책은 운영 버킷 문자열을 복제하지 말고 해당 함수를 호출해야 한다.

환경마다 R2 자격증명을 분리한다. 토큰은 환경 버킷으로 범위를 제한하고 개발자·CI는 운영 R2 자격증명을
가지지 않는다. 잘못된 버킷은 경고가 아니라 시작/preflight 오류다.

## 대체 규칙

- `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=local` is a local/CI bounded-test option only.
- `FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=log` is a local/CI compose-smoke option only.
- `InMemoryJobBus`, process-local Intelligence state, fixture topics, and local Parquet are test or
  development aids, not staging/production backends.
- Staging and production require `r2` for Bronze and Catalog object storage.
- Staging and production must fail closed when `FOUNDATION_PLATFORM_RUNTIME_ENV` is missing or
  unknown.
- Production must never inherit a default local, logging, memory, or fixture configuration.

## Redpanda/Kafka 경계

Redpanda/Karapace는 Kafka·Avro 계약을 검증하는 로컬·CI C2 broker/registry다. Foundation 운영이 Kafka
이벤트를 발행한다는 증거가 아니다. staging/production broker 설정을 승인하기 전에 별도 Kafka 설계로
운영 broker를 선택하고 연결해야 한다.

## 강제 지점

`foundation-outbox-publisher` validates the runtime environment at operational Catalog and Bronze
live-write boundaries. Every Bronze write path must build its adapter through the shared
preflighted `live_write_bronze_*_object_storage_from_env` boundary; an additional source-level
guard rejects direct use of the unvalidated configuration builders outside the policy module.
Callers may still run the same preflight before provider downloads so a bad target fails before
large response bodies are streamed. Unit tests remain credential-free; protected R2/Kafka live
smoke tests are separate from ordinary Cargo verification and must fail when explicitly required
services are unavailable.
