---
status: current
owner: repository-maintainers
doc_type: README
last_reviewed: 2026-07-31
---

# 전역 조회용 레퍼런스

모노레포 전체에서 참조하는 조회용 사실을 모은다. 결정이 아니라 사실이며, 결정은
[전역 ADR](../adr/README.md)이, 실행 절차는 [전역 가이드](../guides/README.md)가 소유한다.

특정 플랫폼만 참조하는 사실은 해당 플랫폼 `docs/reference/`가 소유한다.

## 문서

- [디자인시스템 벤치마킹](./design-system-benchmarks.md) — 외부 디자인시스템의 공개 계약과 설계 선택

## 배치 기준

[ADR 0003](../adr/0003-docs-physical-taxonomy.md)의 공개 표준 골격 7종 중 `reference/`에 해당한다.
용어·스키마·레지스트리처럼 **조회해서 쓰는 사실**만 둔다. 시점 기록, 검토 메모, 계획 문서는 두지
않는다(가드 `scripts/guard/public-repository-safety.sh`가 차단한다).
