---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# Playwright 런타임 SSOT

## 목적

Playwright는 테스트가 소유한 Gongzzang 웹 앱을 대상으로 실행해야 한다. 우연히 `localhost:3000`을
사용하는 다른 로컬 프로젝트에 조용히 연결하면 안 된다.

## 단일 출처

`apps/web/playwright-runtime.ts` is the SSOT for Playwright endpoint selection.

Defaults:

| Target | Host | Port | URL |
|---|---:|---:|---|
| E2E | `127.0.0.1` | `3100` | `http://127.0.0.1:3100` |
| Probe | `127.0.0.1` | `3101` | `http://127.0.0.1:3101` |

Both `apps/web/playwright.config.ts` and `apps/web/playwright.probes.config.ts` derive:

- `use.baseURL`
- `webServer.url`
- `webServer.command`
- local `ZITADEL_REDIRECT_URI`

from that SSOT.

## 서버 재사용

암묵적인 서버 재사용은 기본 비활성화한다. 다른 프로젝트가 같은 포트를 사용해도 E2E가 거짓 성공하거나
멈추는 일을 막는다.

Local reuse is allowed only when explicitly requested:

```powershell
$env:PLAYWRIGHT_REUSE_EXISTING_SERVER='1'
pnpm --filter @gongzzang/web test:e2e
```

CI는 환경변수가 설정돼도 항상 재사용을 비활성화한다.

## 재정의

다음 설정은 의도적인 로컬 디버깅에서만 사용한다.

| Env | Example | Effect |
|---|---|---|
| `PLAYWRIGHT_HOST` | `localhost` | Changes dev server bind host |
| `PLAYWRIGHT_PORT` | `4100` | Changes managed test port |
| `PLAYWRIGHT_REUSE_EXISTING_SERVER` | `1` | Reuses an existing matching server outside CI |

잘못된 포트와 안전하지 않은 host 값은 Playwright가 잘못된 대상에 연결하기 전에 실패한다.

## 검증

Run:

```powershell
pnpm --filter @gongzzang/web exec vitest run tests/unit/playwright-runtime.test.ts tests/unit/playwright-config.test.ts
$env:CI='1'; pnpm --filter @gongzzang/web exec playwright test
```

기대 결과:

- runtime/config tests pass
- E2E starts a managed Next dev server on `127.0.0.1:3100`
- no dependency on `localhost:3000`
