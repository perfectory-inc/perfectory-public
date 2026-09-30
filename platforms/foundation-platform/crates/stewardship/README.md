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

## API (`services/foundation-api`, 직원 토큰만)

| 경로 | 권한 | 하는 일 |
|---|---|---|
| `GET /catalog/v1/lineage-review/items` | `foundation.lineage:review` | 목록(상태·PNU 앞자리·미결정만·커서) |
| `GET /catalog/v1/lineage-review/items/{id}` | `review` | 후보·근거 지문·맡은 사람·결정 이력 |
| `POST .../items/{id}/claim`, `.../release` | `review` | 30분 맡기, 반납 |
| `POST .../items/{id}/decisions[?dry_run=true]` | `review` | 결정. `Idempotency-Key` 필수(미리 검사 제외) |
| `POST /catalog/v1/lineage-review/decisions/{id}/approval` | `foundation.lineage:adjudicate` | 두 사람이 필요한 결정의 승인·거절 |

역할(`identity-platform` 정책): `LINEAGE_STEWARD` = review, `LINEAGE_ADJUDICATOR` = review + adjudicate,
`MASTER_ADMIN` = 전부. 요청·응답 타입은 `foundation-contracts::lineage_review` 이고 더니어가 그대로 쓴다.
