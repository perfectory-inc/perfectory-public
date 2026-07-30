---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# Circuit Breaker

이 문서는 보호된 outbound HTTP 호출에 대한 Gongzzang backend 정본이다.

## 1. 규칙

Gongzzang의 모든 운영 outbound 호출은 다음 기능을 제공하는 소유 adapter 경계를 거쳐야 한다.

- timeout
- retry
- circuit breaker
- service authentication when required
- traceable error mapping
- audit or lineage logging when the call is audit-relevant

현재 Rust crate `crates/circuit-breaker`는 앞의 세 기능을 제공한다.

- `Policy`
- `Breaker`
- `execute`

이 crate 자체는 idempotency key·audit persistence·rate limiting·provider별 lineage를 구현하지 않는다.
이는 소유 adapter 또는 service의 책임이다.

## 2. 소유권

### Gongzzang 소유 외부 호출

데이터·원천을 Gongzzang이 소유하고 ADR 또는 정책으로 승인한 경우에만 허용한다.

예시:

- Gongzzang-owned law, identity, map, notification, or media integrations
- Gongzzang service-to-service calls to Foundation Platform published APIs
- Gongzzang lakehouse registry calls through the approved Foundation Platform contract

### Foundation Platform 소유 Catalog 호출

Gongzzang은 V-World나 data.go.kr Catalog 원천을 직접 호출하면 안 된다.

Catalog 사실은 Foundation Platform 공개 계약만 호출한다. 공통 HTTP/auth 동작은
`crates/foundation-platform-client`에 두고, 서비스별 번역은 호출한 Gongzzang service가 소유한다.
raw source adapter와 raw lineage는 Foundation Platform에 속한다.

## 3. 현재 정책

현재 기본 제공 정책은 `Policy::foundation_platform_default()`다.

| 필드 | 값 |
|---|---:|
| `timeout_ms` | `5_000` |
| `max_retries` | `1` |
| `retry_base_ms` | `500` |
| `open_threshold` | `5` |
| `open_window_ms` | `10_000` |
| `open_cooldown_ms` | `30_000` |

의미:

- 한 요청 시도는 최대 5초 실행된다.
- 500ms 기본 backoff 뒤 retry 한 번을 허용한다.
- 10초 안에 5회 실패하면 circuit을 연다.
- 열린 circuit은 30초 동안 호출을 막고 half-open 시험을 허용한다.

## 4. 어댑터 형태

outbound 어댑터는 재사용 가능한 `reqwest::Client` 하나, `Breaker` 하나와 이름 있는 `Policy` 하나를
소유해야 한다.

호출 경로는 다음 형태다.

```rust
let response = execute(
    &self.breaker,
    &self.policy,
    "foundation_platform.catalog.get_parcel_by_pnu",
    || {
        let client = self.client.clone();
        let url = url.clone();
        let auth = self.auth.clone();
        async move { send_provider_get(&client, url, auth.as_ref()).await }
    },
)
.await?;
```

요청마다 새 breaker를 만들지 않는다. 요청별 breaker는 최근 실패를 기억하지 못하므로 진짜 circuit
breaker가 아니다.

## 5. Retriable Statuses

제공기관 어댑터는 `execute`에 전달하는 closure 안에서 재시도할 HTTP 상태를 오류로 변환해야 한다.

현재 Foundation Platform adapter는 다음 상태를 retry 대상으로 취급한다.

- HTTP 5xx
- HTTP 429

재시도하지 않는 상태는 보호된 호출에서 성공적인 HTTP response로 반환한 뒤 adapter가 매핑한다.
예를 들어 Foundation Platform parcel lookup은 보호 호출 뒤 HTTP 404를 `Ok(None)`으로 매핑한다.

## 6. Error Mapping

어댑터는 제공기관 원천 스키마를 도메인 crate에 노출하지 않는다.

필수 매핑:

- HTTP/client 실패는 adapter별 infra error가 된다.
- circuit breaker 실패는 제품용 backend error가 된다.
- provider response JSON은 Gongzzang 소유 DTO 또는 read model이 된다.
- 예상하지 못한 response 값은 parse/contract error가 된다.

도메인 crate는 `reqwest`·제공기관 SDK·응답 구조체가 아니라 port와 값 객체에 의존한다.

## 7. Audit And Lineage

Circuit breaker crate는 감사 기록을 쓰지 않는다.

adapter는 호출이 audit 대상인지 결정해야 한다.

- 사용자에게 보이는 mutation: audit 필수
- admin/security 민감 read: audit 필수
- raw 공공데이터 lineage: 소유 service lineage 필수
- 일반 Foundation Platform read-through lookup: 정책이 다르게 요구하지 않으면 trace/logging으로 충분

Catalog raw lineage는 Foundation Platform 소유다. Gongzzang 소유 외부 API raw lineage는 구현 전에
ADR 승인 archive/lineage 계약이 필요하다.

## 8. Forbidden Patterns

금지:

- Gongzzang runtime에서 V-World/data.go.kr Catalog API를 직접 호출한다.
- 요청마다 새 `Breaker`를 만든다.
- 운영 adapter에서 ad-hoc `reqwest::get`을 사용한다.
- 명시적 idempotency key 또는 operation key 없이 non-idempotent mutation을 retry한다.
- Authorization·Cookie·Set-Cookie·provider API key·service token·raw PII를 로그에 남긴다.
- fallback 계약을 문서화하지 않고 외부 호출 실패를 조용한 fallback으로 숨긴다.

## 9. Existing Good Examples

따라야 할 현재 예시:

- `services/gongzzang-api/src/foundation_parcel_lookup.rs`
- `services/gongzzang-api/src/building_reader.rs`
- `services/gongzzang-outbox-publisher/src/foundation_lakehouse_registry.rs`

이 어댑터는 Foundation Platform 호출을 서비스 소유 경계 뒤에 두고 `reqwest::Client`, `Breaker`,
`Policy`를 재사용한다.

## 10. Verification

외부 호출 동작을 바꾼 뒤에도 Foundation Platform 의존성 경계와 platform-integration 정책을 유지한다.
Foundation Platform Catalog 경계는 `scripts/lefthook/foundation-ownership-boundary.sh`와
`docs/architecture/foundation-platform-boundary.v1.json` 계약이 강제한다.

회로 차단 crate와 영향을 받은 service에 Rust 검사를 실행한다.

```bash
cargo test -p circuit-breaker
cargo check -p gongzzang-api
```

어댑터가 Foundation Platform 이벤트·레이크하우스·마커 계약을 건드리면 해당 계약 가드도 실행한다.
