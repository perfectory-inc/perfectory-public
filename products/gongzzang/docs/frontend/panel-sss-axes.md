---
status: current
---

# SSS-grade Panel System Axes

> AGENTS.md §10에서 이관 (2026-07-20, docs 대개편 Phase B2). 규범 본문은 원문 그대로이며,
> §10.x 번호 체계는 기존 인용([sss-charter.md](../sss-charter.md), [ADR 0047](../adr/0047-collection-event-fabric.md) 등)과의
> 호환을 위해 유지한다.

패널 시스템은 URL-driven enterprise interaction surface다. 모든 panel 변경은 아래 축을 만족해야 한다. (Claude + Codex 합의, 2026-05-08)

## 10.1 Day-1 BLOCKER (없으면 SSS 자격 박탈)

1. **Correctness**
   - URL serialize/deserialize roundtrip 100%
   - reload / back / forward / mobile back 동작 100%
   - hydration mismatch, race leak, memory leak 0

2. **Accessibility**
   - WCAG 2.2 AA 기준
   - keyboard-only 주요 flow 100%
   - dialog / focus / ESC / breadcrumb은 ARIA APG 패턴 준수
   - axe violation 0 in CI

3. **Type Safety**
   - TS strict + discriminated union
   - panel kind / view exhaustiveness compile-time enforced
   - API 계약은 Rust → utoipa → OpenAPI → generated TS only

4. **SSOT**
   - URL = panel state SSOT
   - registry = kind / view / component / fetch / i18n / telemetry SSOT
   - panel framework는 kind implementation을 import 금지
   - ad-hoc URL parsing 금지 — codec만 허용

5. **Security & Privacy**
   - user-facing string은 typed i18n only
   - PII log / span / event 금지
   - CSP / XSS / CSRF / rate-limit baseline 유지
   - audit-relevant panel/API actions는 correlation_id로 추적 가능해야 함

6. **Migration / Versioning**
   - 한 번 배포된 URL codec은 영구 backward-compatible
   - invalid / unknown URL은 safe recovery + telemetry
   - codec 변경은 ADR + compatibility corpus test 필수

## 10.2 Day-1 MUST

7. **Resilience** — per-panel error boundary, AbortController / query cancellation, loading / error / empty / auth-required / ok state 강제
8. **Observability** — `panel.opened` / `panel.url_decode_failed` / fetch latency span 필수, telemetry schema test 100%, panel open latency SLO 측정 가능해야 함
9. **Performance** — LCP < 2.5s p75, INP < 200ms p75, CLS < 0.1 p75, bundle budget CI gate
10. **Governance** — panel architecture 변경은 ADR 필요, lefthook + CI로 URL SSOT / codec / import boundary 강제

## 10.3 Phase-2 Hardening

11. **Contract Testing** — OpenAPI breaking change diff, generated client compile gate, no-mock integration tests for backing endpoints
12. **Supply Chain Integrity** — CycloneDX SBOM, cargo-deny / pnpm audit / gitleaks, signed artifacts
13. **Operations** — readiness / health checks, feature flag 및 rollback path, SLO dashboard + runbook + alert policy
14. **Data Lineage** — Catalog source lineage lives in Foundation Platform; Gongzzang-owned sources need source / fetched_at / SRID / license traceability and schema evolution policy
15. **Design System / Documentation** — Spec → ADR → Code traceability, Storybook + visual regression (critical states only), C4 recommended *not* CI gate

## 10.4 명시적 비포함 (SSS 라벨에 본질 아님)

- 모든 페이지 visual regression — critical states (panel shell / mobile fullscreen / side-by-side / 4-state) 만
- Unit 100% branch coverage — 핵심 순수 로직(codec / URL parser / permission / calculation)만 100%, UI는 risk-based
- Mutation testing 전체 적용 — 핵심 순수 로직에만 selective
- Property-based testing 전체 — codec / SRID / idPattern 등 selective
- Offline support — 산업용 부동산 조회에는 read-through cache로 충분
- Chaos engineering Day-1 — Phase-2 hardening에서 검토
- C4 diagram CI blocker — 문서 형식주의 위험

## 10.5 적용 범위

본 §10은 *패널 시스템* 한정 SSS 정의이며, 다른 도메인(auth, infra, listings backend 등)은 자체 SSS axis가 필요할 수 있다. §10의 BLOCKER 항목 중 *Type Safety / SSOT / Security & Privacy / Migration* 은 도메인 무관 일반 룰이므로 다른 영역에도 동일하게 적용한다.

참조 표준: W3C WCAG 2.2 AA, W3C ARIA APG, Google Core Web Vitals, OWASP ASVS, NIST SSDF SP 800-218, OpenTelemetry Semantic Conventions, CycloneDX SBOM, PIPC / 개인정보보호법.
