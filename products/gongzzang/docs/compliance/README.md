---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# compliance/

법적·규제·인증 컴플라이언스 SSOT.

## 책임 영역

이 폴더가 앞으로 정본 문서를 둘 범위다. 아래에서 "구현됨"으로 적은 것 말고는 아직 정본 문서·코드가 없다.

- PIPA (개인정보보호법, 한국 필수)
- ISMS-P 인증 (매출 후)
- SOC 2 Type II (B2B 진출 시)
- ISO 27001
- 공공데이터 라이선스 (각 데이터셋별)
- Audit Log immutable — 구현됨: DB 역할 분리로 writer 의 `audit_log` UPDATE/DELETE 권한 박탈
  (`migrations/20260719000106_db_roles.sql`, CI `tests/migrations/test_v002_audit_immutable.sh`).
  R2 bucket lock/retention 은 미구현.
- 데이터 retention 정책
- GDPR 호환 (글로벌 진출 시 활성화)
- right to be forgotten (가입 탈퇴 시 데이터 삭제 또는 가명화)
- 법적 보유 (Legal Hold)
- 이용약관 / 개인정보처리방침 / 위치정보 동의

세부 컴플라이언스 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## 관련 ADR
- [ADR 0010 — 제품 범위 옵션 A](../adr/0010-scope-information-platform-option-a.md) (컴플라이언스 부담 낮춤)
- (인증 진입 시 추가 ADR)

## 관련 컨벤션
- [한국어 UI 작성 컨벤션](../conventions/ui-writing-korean.md) (사용자 동의 UI)
