---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# compliance/

법적·규제·인증 컴플라이언스 SSOT.

## 책임 영역
- PIPA (개인정보보호법, 한국 필수)
- ISMS-P 인증 (Phase 3+ 후반, 매출 후)
- SOC 2 Type II (B2B 진출 시, Phase 4+)
- ISO 27001 (Phase 4+)
- 공공데이터 라이선스 (각 데이터셋별)
- Audit Log immutable (Cloudflare R2 bucket lock/retention)
- 데이터 retention 정책
- GDPR 호환 (글로벌 진출 시 활성화)
- 우 right to be forgotten (가입 탈퇴 시 데이터 삭제 또는 가명화)
- 법적 보유 (Legal Hold)
- 이용약관 / 개인정보처리방침 / 위치정보 동의

세부 컴플라이언스 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## 관련 ADR
- [ADR 0010 — 제품 범위 옵션 A](../adr/0010-scope-information-platform-option-a.md) (컴플라이언스 부담 낮춤)
- (Phase 3+ 인증 진입 시 추가 ADR)

## 관련 컨벤션
- [한국어 UI 작성 컨벤션](../conventions/ui-writing-korean.md) (사용자 동의 UI)
