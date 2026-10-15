# ADR 0228: Zitadel 구성은 identity-platform 하나, 공짱 인프라 폴더는 `infrastructure/` 하나로 모은다

- Status: Proposed
- Date: 2026-10-14
- Related: [ADR-0080](./0080-the-issuer-answers-on-every-loopback.md) (운영 신원 기동),
  [ADR-0081](./0081-identity-endpoints-derive-from-one-contract.md) (Zitadel 구성은 멱등 코드),
  [GZ-ADR-0005](../../products/gongzzang/docs/adr/0005-auth-zitadel.md) (공짱 IdP = Zitadel),
  [GZ-ADR-0009](../../products/gongzzang/docs/adr/0009-iac-pulumi.md) (공짱 IaC = Pulumi, `infrastructure/`)

## Context

저장소에 Zitadel 을 띄우고 구성하는 정의가 둘, 공짱 제품 안에 인프라 폴더가 셋 있다. 같은 일을 두 곳이 정의하면
한쪽만 고쳐지는 순간 어긋난다(루트 AGENTS.md "같은 지식이 두 곳에 복제되면 그 자체가 결함"). 아래는 2026-10-14
`origin/main` 의 추적 파일 전체(`git grep`, 범위: 문서·워크플로·가드·Rust/TS/셸/JSON 계약 전부)를 훑은 결과다.

### Zitadel 구성 두 벌

| | `products/gongzzang/infra/zitadel/` | `platforms/identity-platform/infra/zitadel/` |
|---|---|---|
| 파일 | `docker-compose.yml`, `init-zitadel.sh`, `zitadel-setup.yaml`, `.env.example` | `docker-compose.yml`, `configure-zitadel.sh`, `actions/principal-kind.js`, `.env.example` |
| 용도 | 노트북 로컬 개발 전용 | ai-server 운영 신원(ADR-0080/0081) |
| 프로젝트 | `gongzzang-dev` + 공개 PKCE 앱 `gongzzang-web-dev` (B2C 웹 로그인) | `perfectory` + 기계 사용자(정책 산출물에서) + 직원 콘솔 앱(`config/staff-console-applications.v1.json`) |
| 발행 포트 | 호스트 기본 18443(Zitadel)·15433(DB)·6379(Valkey) | 계약 `config/identity-runtime-endpoints.contract.json` 에서 파생한 루프백 포트 |
| 전제 | 없음 — `docker compose up` 한 번 | 외부 네트워크 `identity-shared`, `/etc/identity-platform/` 의 env·PAT, 래퍼 `scripts/deploy/zitadel-runtime.sh` |
| 메모리 상한 | 없음(`tools/host-memory-budget.contract.json` 의 `outside_scope` 가 `products/gongzzang/` 를 뺀다) | 있음(같은 계약의 `zitadel-platform` 프로젝트) |

**공짱 쪽을 가리키는 곳(전수)**

| 위치 | 무엇 |
|---|---|
| `products/gongzzang/docs/auth/frontend-integration.md` §1 | 로컬 개발 절차 전체(env 생성 → compose → `init-zitadel.sh` → 웹 `.env.local`) 와 실 Zitadel E2E(`ZITADEL_E2E_REAL`) |
| `products/gongzzang/apps/web/.env.local.example` | `ZITADEL_ISSUER` 가 로컬 18443, `ZITADEL_CLIENT_ID=gongzzang-web-dev` |
| `platforms/foundation-platform/.env.example`, `.env.local.example` | `ZITADEL_ISSUER_URL` 이 같은 로컬 인스턴스 — "공짱 인스턴스를 공유" 라고 적혀 있다 |
| `platforms/foundation-platform/docker-compose.yml` 머리 주석 | 포트 충돌 표에 공짱 compose 의 15433·18443 |
| `products/gongzzang/infra/zitadel/init-zitadel.sh` | 자기 자신의 사용 예 |

가드·워크플로·Rust/파이썬 테스트·계약 중 공짱 쪽 경로를 읽는 것은 **없다**. CI 의 공짱 프런트 작업은 Zitadel
을 띄우지 않고 자리표시 issuer 를 쓴다(`.github/workflows/gongzzang-frontend.yml`).

**identity-platform 쪽을 가리키는 곳(전수)**: `scripts/guard/identity-endpoints-match-the-contract.sh` 와 그
자기시험, `tools/host-memory-budget.contract.json` 의 `zitadel-platform`, `scripts/deploy/zitadel-runtime.sh`,
foundation 의 서명 OIDC 스모크(`scripts/smoke/identity-foundation-signed-oidc.sh` 가 `actions/principal-kind.js` 를 읽는다),
런북 `production-bringup-on-a-lan-host.md`·`staff-console-sign-in.md`·`data-catalog.md`·`production-orchestrator-cutover.md`,
그리고 ADR-0080/0081.

**판정: 공짱 쪽은 지금 살아 있다.** 운영·CI 는 쓰지 않지만, 공짱 웹·공짱 API·foundation 의 로컬 로그인과 실
Zitadel E2E 가 이 인스턴스 하나에 기대고 있다. identity-platform 쪽은 그 자리를 그대로 대신하지 못한다 —
외부 네트워크와 `/etc` 의 비밀 파일이 있어야 뜨고, `configure-zitadel.sh` 는 B2C 웹 앱(`gongzzang-web-dev`)을
만들지 않는다. 그래서 지금 지우면 로컬 개발이 깨진다.

그 밖에 Zitadel 을 띄우는 정의가 둘 더 있다. 둘 다 이 결정의 범위에 넣는다.

- foundation 서명 OIDC 스모크가 `docker run` 으로 일회용 Zitadel 을 띄운다(이미지 고정값이 스크립트 안에 한 벌 더 있다).
- `products/gongzzang/infrastructure-v2/index.ts` 가 AWS Fargate 위의 Zitadel 서비스를 정의한다(적용된 적 없음).

### 공짱 인프라 폴더 세 개

| 폴더 | 실제 쓰임 | 가리키는 곳 |
|---|---|---|
| `products/gongzzang/infra/` | `zitadel/` 하나뿐 — 위 표 | 위 표 |
| `products/gongzzang/infrastructure/` | WAF 정책 Pulumi(`index.ts`, `Pulumi.yaml`), 로컬 앱 스택(`docker/` — PostGIS 15432·Valkey 6379), 생성 산출물 `security/*.generated.json` | `services/gongzzang-api/src/bin/generate_traffic_auth_policy.rs` 가 `security/` 를 쓴다; `.github/workflows/gongzzang-ci.yml` 의 `traffic-auth-policy-drift` 가 그 두 파일의 drift 를 검사; `pnpm-workspace.yaml` 의 패키지; `AGENTS.md`·`docs/README.md`·`docs/ssot-matrix.md`·`docs/database/migrations.md`·`migrations/README.md`·`.env.example`·GZ-ADR-0009 |
| `products/gongzzang/infrastructure-v2/` | 그린필드 AWS 플랫폼 Pulumi(VPC·RDS·ECS·ALB·Zitadel). README 스스로 "`pulumi up` 전에는 자원이 없다" — 적용된 적 없다 | **없다.** pnpm 워크스페이스에도, CI 에도, 문서에도 없다 |

덧붙여 공짱 Zitadel compose 의 Valkey 와 `infrastructure/docker/` 의 Valkey 가 같은 호스트 포트 6379 를 기본값으로
잡는다. 둘을 함께 띄우면 하나는 실패한다 — 같은 서비스가 두 곳에 정의된 또 하나의 증상이다.

## Decision

1. **Zitadel 을 띄우고 구성하는 정의는 `platforms/identity-platform/` 에만 있다.** 신원은 identity-platform 소유다(루트
   AGENTS.md 영역 경계). 다른 영역은 그 정의를 가져다 쓰고, 자기 compose·구성 스크립트를 두지 않는다.
2. **살아남는 것**: `platforms/identity-platform/infra/zitadel/` (compose, `configure-zitadel.sh`, actions). 로컬 개발도 이것으로 한다.
3. **공짱 인프라 폴더는 `products/gongzzang/infrastructure/` 하나다.** GZ-ADR-0009 가 이미 이 폴더를 IaC 정본으로 정했고,
   생성기·CI·pnpm 워크스페이스가 실제로 읽는 폴더는 이것뿐이다. `infra/` 는 Zitadel 이 빠지면 빈 폴더로 사라진다.
4. **제거 순서** — 앞 단계가 병합돼야 다음 단계를 연다. 각 단계는 PR 하나.
   1. (이 ADR) 전수 목록과 순서를 기록한다. 코드 변경 없음.
   2. identity-platform 에 **로컬 개발 진입점**을 만든다: 같은 `docker-compose.yml` 을 외부 네트워크·`/etc` 비밀 없이 띄우는
      덮어쓰기 파일 하나와, `configure-zitadel.sh` 가 로컬 PAT·로컬 issuer 로 도는 경로. 새 포트·이미지 값을 적지 않고
      기존 계약에서 읽는다(ADR-0081). 아래 6항의 결정에 따라 B2C 웹 앱도 여기서 만든다.
   3. 소비자를 옮긴다: `apps/web/.env.local.example`, foundation 의 `.env.example`·`.env.local.example`·`docker-compose.yml` 주석,
      `docs/auth/frontend-integration.md` §1. 공짱 로컬 Valkey 는 이미 있는 `infrastructure/docker/` 의 것을 쓴다.
   4. `products/gongzzang/infra/zitadel/` 를 지우고, **가드 하나**를 같은 PR 에 넣는다: Zitadel 이미지를 고정하는 파일은
      `platforms/identity-platform/` 아래에만 있을 수 있다. foundation 서명 OIDC 스모크는 이미지 값을 identity 의 compose 에서
      읽도록 바꿔 예외를 남기지 않는다. 가드가 막는 사고: 두 번째 Zitadel 이 생겨 프로젝트·앱·포트가 운영 구성과 어긋나는 것
      (`gongzzang-dev` 대 `perfectory` 가 바로 그 사례다). 위반을 심어 거부까지 확인한다.
   5. `infrastructure-v2/` 의 처분(7항의 결정).
5. **결과**: Zitadel 구성 2 → 1. 공짱 인프라 폴더 3 → 2(4.4 뒤) → 1(4.5 에서 삭제를 고르면).
6. **사장 결정 필요 ① — 공짱 B2C 로그인 앱을 어디에 두나.** identity-platform 의 AGENTS.md 는 "제품의 B2C 사용자 인증은 소관이
   아니다" 라고 적고 있다. 4.2 에서 `gongzzang-web-dev` 같은 B2C 앱을 identity 의 Zitadel 에 넣으려면 둘 중 하나를 골라야 한다:
   (가) 한 Zitadel 안에 공짱 전용 조직/프로젝트를 두고, 앱 목록은 identity 의 계약 파일에 둔다(인스턴스 하나, 소유는 프로젝트별);
   (나) 공짱이 B2C 용 IdP 를 따로 소유한다 — 그러면 1항은 "직원·서비스 신원의 Zitadel 은 하나" 로 좁혀지고 공짱 쪽은 남는다.
   이 ADR 은 (가)를 권한다. 고르기 전에는 4.2 이후를 시작하지 않는다.
7. **사장 결정 필요 ② — `infrastructure-v2/`.** 저장소 안에서 아무도 읽지 않지만, AWS 운영 자원을 겨냥한 IaC 이고 운영 공짱은
   AWS 에 있다. 이 저장소의 작업자는 AWS 자원에 어떤 명령도 실행하지 않으며(`pulumi preview` 포함), 이 폴더를 지우거나 합치는
   것도 사장 결정으로 한다. 선택지: 지운다(이력은 git 에 남는다) / `infrastructure/` 에 합친다 / 그대로 둔다. 1항과 함께 보면,
   여기 정의된 Fargate Zitadel 은 남기는 경우에도 identity-platform 의 구성을 배포하는 형태로 바뀌어야 한다.

## Consequences

- 이 ADR 이 Accepted 되기 전에는 아무 파일도 지우지 않는다. 지금 지우면 공짱 웹·foundation 로컬 로그인이 깨진다.
- 4.2 는 identity-platform 에 로컬 개발 경로라는 새 책임을 준다. 운영 compose 를 그대로 쓰고 덮어쓰기만 더하므로,
  운영과 로컬이 같은 이미지·같은 구성 스크립트를 쓰게 된다 — 지금은 이미지 고정값이 세 곳(공짱 compose, identity compose,
  foundation 스모크)에 있다.
- 4.4 의 가드는 4.3 이 끝난 뒤에만 넣을 수 있다. 그 전에 넣으면 위반이 0 이 아니라서 규칙이 아니라 예외 목록이 된다.
- AWS 운영 공짱(배포·설정)은 이 결정의 어떤 단계에서도 건드리지 않는다. 이 결정은 저장소 안의 파일만 다룬다.
