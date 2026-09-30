---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-09-30
---

# Foundation 스튜어드십

자동 도출이 가르지 못한 필지 계보 항목을 사람이 결정하는 경계다. 화면은 더니어가 맡고
(루트 ADR-0114), 이 영역은 결정의 규칙과 저장을 소유한다.

- 결정: [`docs/adr/0115`](../../../../docs/adr/0115-stewards-decide-lineage-items-through-one-api-and-decisions-fold-into-the-lakehouse.md)
- `stewardship-domain` — 순수 규칙: 근거 지문, 결정 검사, 두 사람 승인, 멱등 키, 맡기 만료
- `stewardship-application` — 저장소 포트(`LineageStewardshipStore`)
- `stewardship-infrastructure` — `PostgreSQL` 구현. 결정마다 항목을 잠그고 규칙을 다시 돈다
- 표: `migrations/20260930120000_lineage_stewardship.sql` — 결정·승인·접힘은 append-only, 자기 승인은 트리거가 거부
- 검증: `cargo test -p stewardship-domain`, Postgres 시험은 `-- --ignored`
