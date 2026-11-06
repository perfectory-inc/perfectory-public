---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-04
---

# 미디어 대조기 — R2 의 사진과 Silver 색인이 어긋날 때

[루트 ADR-0387](../../../../docs/adr/0387-the-media-in-r2-and-the-silver-index-that-names-it-are-reconciled-daily.md).
`media_reconcile` 작업(`orchestration/jobs.v1.json`, 매일 06:15 UTC, 배포 뒤 한 번)은 `silver.onbid_real_estate_item_photo`
가 이름 대는 키 전부와 R2 `onbid/media/sha256/` 아래 객체 전부를 맞춘다. 무엇을 맞추는지는
`infra/lakehouse/contracts/data-go-kr-onbid.contract.json` 의 `media.reconcile` 이다. 아무것도 지우거나 고치지 않는다.

| 경보 | 뜻 | 먼저 볼 것 |
|---|---|---|
| `FoundationMediaObjectMissing` | 색인이 이름 대는 키가 R2 에 없다 — 필지 화면의 깨진 사진 | 마지막 보고서의 `missing`. 그 해시의 Bronze 줄과 수집 run.log 의 `onbid-media-summary-json` |
| `FoundationMediaObjectOrphaned` | 이름 대는 색인 줄이 없는 객체가 48시간 넘게 있다 | 마지막 보고서의 `orphans`. 색인 마지막 적재(`max(ingested_at_utc)`)가 멈췄는지, 수집 run 이 커밋 없이 끝났는지 |
| `FoundationMediaReconcileStale` | 30시간 비교가 없다 | `journalctl -u foundation-media-reconcile.service` |

보고서: `/var/lib/foundation-platform/media-reconcile/reports/<UTC>.json` (run 마다 하나, 덮어쓰지 않음). 지표:
`/var/lib/foundation-jobs/metrics/media-reconcile.prom`.

고아는 지우지 않는다. 같은 키가 여러 run 의 보고서에 계속 남고 원인(커밋 없는 run)이 확인된 뒤, 삭제는 사람의 결정으로
`delete-r2-candidates` 절차를 따른다.
