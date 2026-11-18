---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# Playwright 실행 대상의 정본

## 목적

Playwright 는 시험이 직접 띄운 공짱 웹 앱을 대상으로 돌아야 한다. 우연히 `localhost:3000`을
쓰는 다른 로컬 프로젝트에 조용히 붙으면 안 된다.

## 단일 출처

`apps/web/playwright-runtime.ts` 가 Playwright 의 접속 대상을 고르는 정본이다.

기본값:

| 대상 | 호스트 | 포트 | 주소 |
|---|---:|---:|---|
| 종단 시험 | `127.0.0.1` | `3100` | `http://127.0.0.1:3100` |
| 탐침 | `127.0.0.1` | `3101` | `http://127.0.0.1:3101` |

`apps/web/playwright.config.ts` 와 `apps/web/playwright.probes.config.ts` 는 둘 다 다음을 이 정본에서
유도한다:

- `use.baseURL`
- `webServer.url`
- `webServer.command`
- 로컬 `ZITADEL_REDIRECT_URI`

## 서버 재사용

암묵적인 서버 재사용은 기본으로 끈다. 다른 프로젝트가 같은 포트를 써도 종단 시험이 거짓으로 통과하거나
멈추는 일을 막는다.

로컬 재사용은 명시적으로 요청할 때만 허용한다:

```powershell
$env:PLAYWRIGHT_REUSE_EXISTING_SERVER='1'
pnpm --filter @gongzzang/web test:e2e
```

CI 는 환경 변수가 설정돼도 늘 재사용을 끈다.

## 재정의

다음 설정은 의도적인 로컬 디버깅에서만 쓴다.

| 환경 변수 | 예 | 효과 |
|---|---|---|
| `PLAYWRIGHT_HOST` | `localhost` | 개발 서버가 붙는 호스트를 바꾼다 |
| `PLAYWRIGHT_PORT` | `4100` | 시험이 관리하는 포트를 바꾼다 |
| `PLAYWRIGHT_REUSE_EXISTING_SERVER` | `1` | CI 밖에서 맞는 서버가 이미 있으면 재사용한다 |

잘못된 포트와 안전하지 않은 호스트 값은 Playwright 가 엉뚱한 대상에 붙기 전에 실패한다.

## 검증

실행:

```powershell
pnpm --filter @gongzzang/web exec vitest run tests/unit/playwright-runtime.test.ts tests/unit/playwright-config.test.ts
$env:CI='1'; pnpm --filter @gongzzang/web exec playwright test
```

기대 결과:

- 실행 대상·설정 시험이 통과한다
- 종단 시험이 `127.0.0.1:3100` 에 관리되는 Next 개발 서버를 띄운다
- `localhost:3000` 에 기대지 않는다
