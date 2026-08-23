---
status: current
owner: repository-maintainers
doc_type: reference
last_reviewed: 2026-07-28
---

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

## 1.1 채택 애플리케이션 라이브러리 — 직접 만들기 전에 여기부터 본다

§1이 런타임·툴체인 버전을 고정한다면 이 표는 **필요가 생겼을 때 무엇에 손을 뻗는지**를 고정한다.
[AGENTS.md 해결 접근 순서](../AGENTS.md) 4·5항(오픈소스 우선, 커스텀은 마지막 수단)은 이미
규칙으로 있었지만 **명단이 없었다.** 매번 스스로 찾아보라는 규칙은 지켜지지 않았고, 실제로
`products/gongzzang/apps/web/lib/panel/panel-entry-view.tsx`에 error boundary를 직접
구현한 클래스가 들어와 있다. 이 표가 그 빈자리다.

| 이 필요가 생기면 | 이것을 쓴다 | 직접 만들면 생기는 일 |
|---|---|---|
| `debounce`, `throttle`, `groupBy`, `chunk`, deep clone/equal 같은 범용 유틸리티 | [es-toolkit](https://github.com/toss/es-toolkit) `1.51.0` | 같은 함수가 파일마다 조금씩 다르게 존재하고, 어느 것이 맞는지 판정할 SSOT가 없다 |
| error boundary, Suspense 경계, loading flash 방지, client-only 렌더 | [@suspensive/react](https://github.com/toss/suspensive) `3.21.3` | 클래스 컴포넌트로 boundary를 매번 새로 짠다. reset·selective catch·중첩 boundary는 대개 빠진 채로 남는다 |
| 모달·다이얼로그·시트를 여닫고 결과를 돌려받기 | [overlay-kit](https://github.com/toss/overlay-kit) | 화면마다 `isOpen` state와 콜백이 흩어지고, 오버레이 UI가 그것을 띄운 화면에 묶여 재사용이 안 된다 |
| 초성 검색, 조사(은/는·이/가) 자동 선택, 한글 분해·조합 | [es-hangul](https://github.com/toss/es-hangul) | 자모 배열과 유니코드 산술을 손으로 적게 된다. 한국어가 1차 언어인 제품에서 가장 조용히 틀리는 자리다 |

운영 규칙:

1. **버전은 첫 도입 시점에 이 표에 고정한다.** 표의 버전은 manifest 범위가 아니라 lockfile의
   정확한 해소 버전이다. `@suspensive/react`는 manifest의 `^3.21.3`이 lockfile에서 `3.21.3`으로
   해소된 첫 도입 상태이며 React peer 범위는 `^18 || ^19`다. `es-toolkit`은 `apps/web`의
   `^1.51.0`이 lockfile에서 `1.51.0`으로 해소된 첫 도입 상태이며, 런타임 의존성도 peer 의존성도
   없다. 남은 두 라이브러리는 아직 어느 manifest에도 없으므로 첫 도입 커밋이 그 정확한 버전을
   표에 더한다. `overlay-kit` peer 범위는 `^16.8 || ^17 || ^18 || ^19`, `es-hangul`은 런타임
   의존성이 없고, 넷 다 MIT다.
2. **같은 역할의 두 번째 라이브러리는 들어올 수 없다.** `scripts/guard/utility-library-policy.sh`가
   package manifest의 dependency key만 읽어 거부한다.
3. **표를 바꾸는 것이 결정이다.** 다른 것을 쓰려면 이 표와 가드를 같은 커밋에서 함께 고친다.
   리뷰에서 라이브러리 선택을 매번 다시 논쟁하지 않기 위한 표다.

### AI 도구용 1차 자료

이 표의 목적은 "직접 만들기 전에 있는지 본다"이고, 그 확인은 라이브러리가 스스로 제공하는
1차 자료로 한다. 사용법 예제를 이 저장소에 베껴 오지 않는다 — 베낀 순간 낡기 시작하고,
[AGENTS.md 최상위 원칙](../AGENTS.md) 2항이 금지하는 지식 복제가 된다. 2026-08-11에 실제로
응답을 확인한 것만 적는다.

| 라이브러리 | 제공되는 것 |
|---|---|
| es-toolkit | Agent Skill (`npx skills add toss/es-toolkit`) — `guide`·`recommend`·`migrate`. Claude Code는 `/plugin marketplace add toss/es-toolkit` 후 `/plugin install es-toolkit@es-toolkit-plugin`. 문서 색인 `https://es-toolkit.dev/llms.txt`, 전문 `https://es-toolkit.dev/llms-full.txt` |
| @suspensive/react | 문서 색인 `https://suspensive.org/llms.txt` |
| overlay-kit | 문서 전문 `https://overlay-kit.slash.page/llms-full.txt` |
| es-hangul | 해당 경로 없음(404). `https://es-hangul.slash.page` 문서를 직접 본다 |

es-toolkit의 `recommend`는 이 표가 노리는 행동 그 자체다 — 필요를 말하면 이미 있는 함수를
알려준다. 다만 Agent Skill 설치는 저장소가 아니라 각자의 도구 환경에 남는 변경이라
`docs/technology-stack.md`가 강제할 수 없다. 그래서 여기서는 **어디에 있는지**만 고정하고,
설치 여부는 각 작업자가 정한다.

## 2. 역할별 환경 매트릭스

| 역할/소유자 | local | CI | staging | production | 상태 |
|---|---|---|---|---|---|
| 공공 source HTTP / Foundation | `reqwest` + rustls로 VWorld/data.go.kr/hub.go.kr/rt.molit 호출 | mock/fixture 또는 보호된 live smoke | 같은 HTTP adapter + staging credentials | 같은 HTTP adapter + production credentials/quota | 실행 중. provider credential은 환경별 |
| Bronze raw bytes / Foundation | 기준은 원격 dev R2지만 출시 전 private 설정은 production R2 공유 | 보호된 live smoke는 전용 CI R2, credential-free smoke는 logging adapter | 전용 staging R2 bucket | 전용 production R2 bucket | 실행 중. `FOUNDATION_PLATFORM_RUNTIME_ENV`와 bucket guard가 강제 |
| Bronze ledger / Foundation | PostgreSQL 17 + SQLx | ephemeral PostgreSQL | staging PostgreSQL 17 | production PostgreSQL 17 | 실행 중. DB endpoint만 변경 |
| outbox/event ledger / Foundation | PostgreSQL outbox + 선택형 Kafka 전달기 + 기존 웹훅/로그 전달기 | 고정 Redpanda/Karapace 실브로커 + Postgres round-trip + outage 계약 테스트 | 관리형 Kafka 호스트와 인증정보 필요 | 관리형 Kafka와 주제/권한 운영 필요 | Kafka는 명시적으로 켤 때만 사용하며 Postgres outbox가 원장; 운영 전 `kafka-integration` 게이트 필수 |
| collection JobBus / Foundation | 실행 중인 hub.go.kr bulk collector가 PostgresJobBus claim/ack를 사용 | 메모리/fixture + 폐기 가능한 Postgres 계약 테스트 | staging PostgreSQL을 쓰는 같은 adapter | production PostgreSQL을 쓰는 같은 adapter | 레거시 API executor는 비활성화했고 dry-run은 의도적으로 DB/JobBus를 건너뜀 |
| Silver handoff / Foundation | Spark/Python fixture·JSONL/Parquet | 결정론적 fixture 작업 | Spark batch runtime + R2/Iceberg 자격증명 필요 | 관리형 Spark 호환 runtime + R2/Iceberg | batch 코드는 있으나 실제 backend 종단 간 증거는 제한적 |
| Gold projection / Foundation | Spark `industrial_complex_silver_to_gold.py` | contract/fixture test | Spark + catalog credentials 필요 | Spark + catalog credentials 필요 | Spark Gold job은 있으나 dbt `models/gold/` 없음 |
| Iceberg/Trino query / Foundation | Trino shell; R2 catalog 속성을 주입하면 실제 조회 | 자격증명 없는 shell 또는 보호된 R2 | R2 catalog + staging 자격증명 | R2 catalog + production 자격증명 | Compose만으로 backend 연결은 증명되지 않음 |
| dbt models / Foundation | local Trino profile/template | 모델 계약 테스트 | dbt-trino target 필요 | dbt-trino target 필요 | 9개 모델, Gold tier 미구현 |
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

전체 실행 순서와 완료 조건은
[`production-readiness.md`](./roadmap/production-readiness.md)를
SSOT로 사용한다. 이 표는 기술 현황을, 로드맵은 실제 다음 작업과 운영 게이트를 관리한다.

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
- `scripts/guard/utility-library-policy.sh`가 tracked package manifest의 dependency
  key만 읽어, canonical 유틸리티 라이브러리 옆에 같은 역할의 두 번째 라이브러리가 들어오는
  것을 거부한다. 산문·주석·버전 값에는 반응하지 않으므로 이 문단처럼 이력을 적을 수 있다.
  손으로 쓴 `debounce`를 라이브러리 것으로 바꾸라는 쪽은 기계가 판정할 수 없어 리뷰 규칙으로
  남는다 — **새 유틸리티가 필요하면 직접 구현하기 전에 es-toolkit에 있는지 먼저 본다.**
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
