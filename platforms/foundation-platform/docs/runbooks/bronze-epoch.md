---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-28
---

# Bronze 시기 — 실행 도장과 이름 바뀐 원천의 객체를 오늘의 키로

[루트 ADR-0626](../../../../docs/adr/0626-every-kind-of-data-name-has-one-home-every-other-spelling-is-generated-and-a-name-outside-the-rules-is-refused.md)
§4·§8 N2 와 그 2026-11-28 각주. `bronze_epoch` 작업(`definitions/bronze-epoch.definition.json`, 매시 50 분)은 발행기의
`rekey-bronze-epoch` 를 돌린다. 실행마다 남은 것만 한다.

| 걸음 | 무엇 | 어디 |
|---|---|---|
| 1 | 증거 목록(옛 키·새 키·크기·ETag·마지막 수정·SHA-256)을 만들기만으로 쓴다 | `control/evidence/r2_move/<날짜>-bronze-epoch.jsonl.gz` |
| 2 | 만들기만 복사, 새 키의 전체 바이트를 원장의 SHA-256 과 크기로 다시 해시, 새 키의 ETag 가 옛 것과 같다 | 같은 버킷 |
| 3 | 원천마다 한 트랜잭션: 원천 줄의 slug, 객체 줄의 키와 키에서 지은 칸을 제자리에서 고친다 | `catalog.source_catalog`, `catalog.bronze_object` (옛 줄은 `bronze_object_version`) |
| 4 | 옮긴 줄이 모두 새 키를 이름 대고 새 키가 목록의 크기·ETag 를 갖는지 확인하고 보고서를 쓴다 | `…-bronze-epoch.report.json` |
| 5 | 확인된 옛 키(와 잔해 `rtmolitkr__*`)를 버킷 잠금(`bronze/` 30 일)이 놓아주는 대로 지운다 | 다음 실행들이 이어서 |

실행의 마지막 줄 앞 `bronze-epoch-summary-json` 이 그 실행이 한 일(복사·옮긴 줄·지운 옛 키·잠금이 잡은 옛 키·다음 해제 시각)이다:
`journalctl -u foundation-bronze-epoch.service`.

## 실패할 때

| 메시지 | 뜻 | 할 일 |
|---|---|---|
| `a writer of an epoch source is running or ran within … minutes` | 옮길 원천의 쓰는 쪽이 방금 돌았다. 아무것도 옮기지 않았다 | 없음. 다음 실행이 다시 본다. 오래 이어지면 그 쓰는 쪽 작업이 정말 꺼졌는지(`orchestration/jobs.v1.json`) 본다 |
| `a source row already holds …` | 쓰는 쪽이 새 id 로 원천 줄을 먼저 만들었다(F4) | 시기를 멈추고 코디네이터에게: 두 원천 줄을 합치는 결정이 먼저다 |
| `…: R2 lists … bytes, the ledger …` | 원장과 R2 가 어긋난다 | 그 객체를 조사한다. 시기는 그 원천을 옮기지 않는다 |
| `… full-byte SHA-256 or size differs` | 복사본이 원본과 다르다 | 새 키는 그대로 두고(조사용) 코디네이터에게 |

## 되돌리기

복사본은 원장이 가리키기 전에는 아무도 읽지 않는다. 원장은 `catalog.bronze_object_version` 의 옛 줄(`prior_row`)과 증거 목록으로
되돌린다. 옛 키는 확인 뒤, 잠금이 놓아준 뒤에만 지우므로 그 사이에는 둘 다 있다.

## 끝

보고서가 있고 옛 키가 모두 지워지면 실행은 `unchanged` 다. 그때 작업을 끄고(enabled false), 그 릴리스가 산 뒤 지운다.
