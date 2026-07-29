---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# auth/

인증·인가·세션 SSOT.

## 책임 영역
- Zitadel (OIDC/OAuth2 IdP)
- 소셜 로그인 (Google/Kakao/Naver/Apple — 단계적)
- NICE 본인인증
- WebAuthn / TOTP 2FA
- RBAC 권한 모델 (5종 사용자 역할)
- 사업자등록번호 검증 (홈택스 진위확인)
- 공인중개사 자격 식별 (사업자 업종 코드)
- JWT 검증 미들웨어 (Rust)
- 세션 (Valkey 8, Redis protocol)

세부 인증 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## Frontend 통합

- [SP6-i Frontend Integration](./frontend-integration.md) — 로컬 개발 / 디버깅 / 장애 대응

## 관련 ADR
- [ADR 0005 — Zitadel 인증](../adr/0005-auth-zitadel.md)

## 관련 컨벤션
- [에러 형식 컨벤션](../conventions/error-format.md)
