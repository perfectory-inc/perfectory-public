---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 산업입지정보시스템(ILIS) 산업단지 고시 첨부 — 수집·적재 런북

루트 ADR-0484 의 운영 절차다. 값(경로·상한·시간 예산·묶음 크기·파일 상한)은
`infra/lakehouse/contracts/industryland-or-kr.contract.json` 하나에 있다. 이 문서는 값을 다시 적지 않는다.

## 흐름

| 작업 | 단위 | 하는 일 |
|---|---|---|
| `industrial_complex_notice_collect` (매시, default_pool) | `foundation-industrial-complex-notice-collect.service` → `scripts/ops/industrial-complex-notice-collect.sh` | `collect-industrial-complex-notice-attachments`: 고시 목록 한 번, `silver.industrial_complex_notice_detail` 이 끝났다고(read) 하지 않는 고시를 새 것부터 계약 `notice_detail` 상한·시간 예산 안에서. 첨부는 형식에 상관없이 공용 미디어 저장소 `sha256/<해시>.<확장자>` 에 한 번(CreateOnly), 상세는 `commit_every` 건마다 Bronze 객체 하나 |
| `industrial_complex_notice_silver` (spark 1칸, 수집이 Bronze 를 바꾸면) | `foundation-industrial-complex-notice-silver-load.service` → `scripts/ops/industrial-complex-notice-silver-load.sh` | `plan-silver-load-objects` 가 읽은 기록 표가 아직 기록하지 않은 객체를 받고 `industryland_or_kr.py`: 첨부 색인 → 읽은 기록, 객체마다 커밋 |

상태: `/var/lib/foundation-platform/industrial-complex-notice/`(`journal.log`, 실행마다 `runs/<실행>/run.log`).
다음에 물을 고시는 매 실행 레이크하우스에서 계산한다. 다음 실행을 막는 상태 파일은 없다.

## 실패했을 때 (사람 할 일 없음)

- 상세 하나 실패는 그 고시의 읽은 기록이 `read` 가 아니게 남아 다음 실행이 다시 묻는다. 계약
  `stop_after_consecutive_failures` 번 연달아 실패하면 그 실행은 멈춘다(제공자 장애). 이미 커밋한 묶음은 Bronze 에 있다.
- 첨부 4xx 는 사라진 파일(끝남), 상한(`attachments.max_file_bytes`) 넘는 파일은 해시·크기만 Bronze 에 적고 저장하지 않으며 `oversize` 로 센다(끝남).
  그 밖의 받기 실패는 그 고시를 끝내지 않아 다음 실행이 다시 받는다.
- 상세를 하나도 못 읽은 실행만 실패로 끝나 `OnFailure` 가 주인(collection)을 부른다.
- Silver 적재가 `window_days` 보다 오래 안 실린 객체를 보면 실패해 알린다.

## 실행 한 번의 요약

`run.log` 의 `industrial-complex-notice-summary-json` 한 줄: 물은 고시 수, 첨부 `by_format`(저장했거나 이미 있던 것, 형식마다 수·바이트), `bytes_stored`,
`stored`·`deduplicated`·`gone`·`oversize`. 형식별 양은 Silver 로도 본다:

```sql
SELECT media_format, count(*), sum(size_bytes)
FROM r2.silver.industrial_complex_notice_attachment
GROUP BY media_format;
```
