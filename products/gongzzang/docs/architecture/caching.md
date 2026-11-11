---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-11-05
---

# 캐시

이 문서는 현재 캐시와 최신성 모델을 설명한다.

## 1. 규칙

캐시는 가속기이며 정본이 아니다.

정본 데이터는 다음에 남는다.

- 공짱 제품 기록은 공짱 Postgres;
- 카탈로그 사실과 PNU 앵커는 파운데이션;
- 변경 불가 미디어·데이터 산출물은 R2/레이크하우스 객체;
- Valkey(Redis 프로토콜)는 캐시·세션·요청 속도 제한·잠금·수신함 중복 제거에만 쓴다. 이미지 핀은
  `../../../../tools/technology-versions.contract.json`이 소유한다.

## 2. 런타임 캐시 사용처

현재 Valkey 사용처:

- Next.js 세션 저장: `apps/web/lib/session/store.ts`
- 세션 갱신의 단일 비행(동시 요청 합치기): `apps/web/lib/session/single-flight.ts`
- 프런트엔드·API 프록시의 요청 속도 제한: `apps/web/lib/ratelimit.ts`
- 파운데이션 이벤트 수신함 중복 제거: `apps/web/lib/foundation-platform/event-inbox.ts`
- Rust API 의 JTI 거부 목록: `crates/product-identity-infrastructure/src/jti_denylist.rs`
- Rust API 백엔드 요청 속도 제한: `services/gongzzang-api/src/backend_rate_limit.rs`
- 매물 표식 서빙 캐시·단일 비행: `services/gongzzang-api/src/listing_marker_serving`

## 3. 운영 Valkey 요구 사항

Rust API 시작 시 환경별 Valkey 누락 처리는 다르다.

- 개발: Valkey 의존 검사를 늦추거나 건너뛸 수 있다.
- 운영: `REDIS_URL`이 없고 보안이 열린 채 실패할 수 있는 곳에서는 바로 실패한다.

주요 파일:

- `services/gongzzang-api/src/startup.rs`

Next.js 환경 변수 검증도 운영에 안전한 Valkey 주소를 요구한다(`REDIS_URL`은 Redis 프로토콜
환경 변수 이름으로 둔다).

주요 파일:

- `apps/web/lib/env.ts`

## 4. 표식 캐시 모델

매물 표식 제공은 보조 보호 수단으로 캐시와 단일 비행을 쓴다.

주 확장 전략은 다음이다.

- 미리 계산한 파생 서빙 색인;
- 안정된 필터 해시;
- 타일 모양의 표식 요청;
- 최신성을 위한 변경분·삭제 표시 겹침.

숫자 필터가 제한 없는 캐시만의 정합성을 만들면 안 된다. 캐시는 반복 요청을 빠르게 할 수 있지만
정합성은 색인과 정규화된 필터 계약에서 나와야 한다.

## 5. 파운데이션 캐시 무효화

파운데이션 이벤트는 카탈로그 관련 캐시를 무효화하고 기준점 투영 가져오기를 시작할 수 있다.

주요 파일:

- `apps/web/app/foundation-platform/events/route.ts`
- `apps/web/lib/foundation-platform/event-inbox.ts`
- `services/gongzzang-api/src/routes/foundation_events.rs`
- `docs/architecture/foundation-platform-webhook-receiver-contract.v1.pin.json`

## 6. 정적 타일 캐시

정적 기준 타일 수명 주기는 파운데이션이 소유한다.

공짱 지도 클라이언트는 매니페스트와 불변 타일 주소를 소비할 수 있지만 파운데이션 기준 타일의
굽기·승격 수명 주기는 소유하지 않는다.

## 7. 지키는 장치

트래픽·인증, 파운데이션 이벤트 수신기, PNU 기준점 PBF 표식 계약은 CI에서 강제한다(로컬 훅은 보조일 뿐
판정이 아니다). 파운데이션 경계는 `tools/semgrep/rules/gongzzang-catalog-boundary.yml`(루트 Semgrep 규칙)가 지키며 ADR-0149 이후
CI 전용이다(`.github/workflows/gongzzang-ci.yml` 저장소 안전장치). 트래픽·인증 정책 산출물은 등록부에서
`cargo run -p gongzzang-api --bin generate-traffic-auth-policy`로 다시 만들고, CI `traffic-auth-policy-drift`
작업이 어긋남을 막는다.
