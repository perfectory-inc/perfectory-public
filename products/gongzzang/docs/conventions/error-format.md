---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# 에러 형식 컨벤션 (Problem Details)

## 1. 표준

[RFC 9457 — Problem Details for HTTP APIs](https://www.rfc-editor.org/rfc/rfc9457) (RFC 7807의 후속,
같은 wire 형식). 제품 API의 에러 응답은 `application/problem+json` 이다.

정본 구현:

- Rust: `services/gongzzang-api/src/http/problem.rs` (`ProblemDetails`, `problem()` 헬퍼)
- 웹: `apps/web/lib/http/problem.ts` (같은 shape)

## 2. 응답 본문

```json
{
  "type": "https://gongzzang.com/errors/not-found",
  "title": "매물을 찾을 수 없어요",
  "status": 404,
  "detail": "..."
}
```

## 3. 필드 규칙

| 필드 | 필수 | 형식 | 설명 |
|------|------|------|------|
| `type` | ✅ | URL | `https://gongzzang.com/errors/<id>` — `problem()` 이 접두를 붙인다 |
| `title` | ✅ | 한국어 | 사용자 노출 가능한 요약 |
| `status` | ✅ | HTTP 코드 | 4xx / 5xx |
| `detail` | 선택 | 문자열 | 구체 정보. 내부 에러(DB 등)는 넣지 않는다 |
| `instance` | 선택 | URI | 웹(`problem.ts`)만 지원한다. Rust 응답에는 없다 |

본문에 `code`·`correlationId`·`errors` 필드는 없다. 요청 추적 ID는 본문이 아니라 응답 헤더
`X-Request-Id`로 돌아간다(§7).

## 4. `type` id 규칙

- kebab-case. 기능별로 묶을 때는 `/` 로 앞에 범위를 붙인다.
- 공통 id: `validation`, `not-found`, `forbidden`, `version-conflict`, `internal-error`.
- 범위 id 예: `listings/invalid-filter`, `map/listing-marker-tile-coordinate`, `not-ready/db`.
- 전체 목록의 정본은 코드다: `git grep -n 'problem(' -- services/gongzzang-api/src`.

## 5. 도메인 에러 매핑

도메인 에러 → Problem 변환은 `problem.rs` 의 `from_*` 함수가 소유한다
(예: `from_listing_error`, `from_listing_repo_error`). 새 도메인 에러는 거기에 매핑을 더한다.

| 상태 | 의미 | 쓰는 `type` 예 |
|------|------|--------------|
| 400 | 잘못된 요청 | `validation`, `listings/invalid-filter`, `transaction-fields-mismatch` |
| 401 | 인증 필요 | `auth/required` |
| 403 | 권한 없음 | `forbidden` |
| 404 | 리소스 없음 | `not-found` |
| 409 | 충돌 | `version-conflict`, `invalid-transition`, `immutable-state` |
| 500 | 서버 에러 | `internal-error` |
| 503 | 일시 불가 | `not-ready/db`, `not-ready/redis` |

## 6. 메시지 작성 룰

- **한국어 해요체** ([ui-writing-korean.md](./ui-writing-korean.md)). `problem.rs` 의 도메인 매핑 일부는 아직 합니다체다.
- **원인 + 대응** ("응답이 늦어요. 잠시 후 다시 시도해 주세요")
- **PII 마스킹** (이메일·주민번호·전화번호·사업자번호 X)
- **기술 용어 회피** (사용자 노출용)
- **내부 정보 금지** — DB 에러 메시지는 `detail` 에 넣지 않는다(스키마 노출). 로그는 호출자가 `tracing` 으로 남긴다.

| ❌ 나쁨 | ✅ 좋음 |
|--------|--------|
| "Internal Server Error" | "일시적인 문제가 있어요. 잠시 후 다시 시도해 주세요" |
| "User john@a.com not found" | "사용자를 찾을 수 없어요" (이메일 마스킹) |
| "VWorld 503" | "토지 정보 서비스가 응답하지 않아요. 잠시 후 다시 시도해 주세요" |
| "DB connection failed" | "잠시 후 다시 시도해 주세요" |

## 7. 요청 추적 ID

- 모든 요청은 `X-Request-Id` 를 받는다 — 들어온 값을 쓰거나 없으면 `req_<ULID>` 를 만든다
  (`services/gongzzang-api/src/http/request_id.rs`).
- 같은 값이 응답 헤더로 돌아가고, tracing span과 DB `correlation_id` 컬럼(audit/outbox)에 들어간다
  ([관측성](../architecture/observability.md) §2).

## 8. 강제 범위

- 공개 route 핸들러는 `problem()` 으로 에러를 만든다. 이를 강제하는 미들웨어나 컴파일 검사는 없다.
  내부 route 일부(예: `services/gongzzang-api/src/routes/auth_event.rs`)는 평문 `(StatusCode, String)` 을 돌려준다.
- `ProblemDetails` 는 OpenAPI 스키마(`export-gongzzang-openapi`)에 들어 있지 않다.
