---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-22
---

# 산업용지 분양·처분·공급 공고 — 수집·적재 런북

루트 ADR-0603 의 운영 절차다. 값(원천마다 목록·상세·첨부 규칙, 목록 지문 필드, 상한·시간 예산·묶음 크기·파일 상한)은
`infra/lakehouse/contracts/industrial-land-sale-notice.contract.json` 하나에 있다. 이 문서는 값을 다시 적지 않는다.

## 흐름

| 작업 | 단위 | 하는 일 |
|---|---|---|
| `industrial_land_sale_notice_ilis_collect` (매시, default_pool) | `foundation-industrial-land-sale-notice-collect@ilis.service` → `scripts/ops/industrial-land-sale-notice-collect.sh ilis` | ILIS 분양공고 목록 POST 하나(바뀌었으면 그 답을 Bronze 에), 같은 목록 지문으로 읽지 않은 공고의 상세·첨부 |
| `industrial_land_sale_notice_kicox_collect` (매시) | `…-collect@kicox.service` → `… kicox` | 한국산업단지공단 임대/분양 게시판의 목록 쪽을 새 것부터, 읽지 않은 글의 쪽·첨부 |
| `industrial_land_sale_notice_lh_collect` (꺼짐) | `…-collect@lh.service` → `… lh` | LH 토지 공고 목록(답을 Bronze 에), 공고마다 상세·공급정보·첨부. 사장의 data.go.kr 활용신청 전에는 78 로 끝난다 |
| `industrial_land_sale_notice_silver` (수집이 Bronze 를 바꾸면) | `foundation-industrial-land-sale-notice-silver-load.service` → `scripts/ops/industrial-land-sale-notice-silver-load.sh` | `plan-silver-load-objects` 가 세 원천의 상세 객체 중 읽은 기록 표가 기록하지 않은 것을 받고 `load-industrial-land-sale-notice-silver`(발행기 안의 DuckDB): 새것·바뀐 첨부 색인 → 읽은 기록, 객체마다 커밋 |

상태: `/var/lib/foundation-platform/industrial-land-sale-notice/`(`journal.log`, 실행마다 `runs/<원천>-<실행>/run.log`).
다음에 물을 공고는 매 실행 레이크하우스(`silver.industrial_land_sale_notice_read`)에서 계산한다. 다음 실행을 막는 상태 파일은 없다.

- 매시 실행은 최근 쪽(`list.recent_pages`)과 밀린 것만 걷는다. `full_walk_hour_utc` 시(UTC)에 시작한 실행은 목록 전체를 걷는다
  (요약의 `full_walk: true`). 그날 KICOX 실행은 목록 쪽을 97 번쯤 묻는다(정상).
- `gone` 은 제공자가 없다고 답한 공고다(4xx, ILIS 는 `code` 400·`result` null): 끝난 것으로 치고 다시 묻지 않는다. `failed`·`incomplete` 는
  `retry_after_hours` 뒤에 다시 묻는다.
- KICOX 의 글 번호와 첨부 번호는 쪽이 부르는 `onclick`(`bbsArticleDet('<글>')`, `fnFileDownload('<번호>', '<id>')`)에서 읽는다. 게시판 모양이
  바뀌면 목록이 비거나(`list_pages` 1, `asked` 0) 글 쪽이 "the page holds no post" 로 실패한다 — 그때 고칠 곳은 `kicox.rs` 의 `Patterns` 다.

## 실패했을 때 (사람 할 일 없음)

- 상세 하나 실패는 그 공고의 읽은 기록이 `failed` 로 남아 `retry_after_hours` 뒤의 실행이 다시 묻는다. `stop_after_consecutive_failures` 번 연달아
  실패하면 그 실행은 멈춘다(제공자 장애). 이미 커밋한 묶음은 Bronze 에 있다. 목록의 둘째 쪽부터 실패하면 걷기만 멈추고 읽은 것은 남는다.
- 첨부 4xx 는 사라진 파일(끝남), 상한 넘는 파일은 해시·크기만 적고 `oversize` 로 센다. 그 밖의 받기 실패는 그 공고를 끝내지 않는다.
- 상세를 하나도 못 읽은 실행만 실패로 끝나 `OnFailure` 가 주인(collection)을 부른다.
- LH 인스턴스가 78 로 끝나면 `journal.log` 의 `REFUSED` 줄이 게이트웨이의 거부 코드를 적는다. 사장의 활용신청(15058530·15057999·15056765)
  전에는 정상이고, 그 뒤라면 키 파일(`/etc/foundation-platform/data-go-kr.env`)을 본다.
- Silver 적재가 `window_days` 보다 오래 안 실린 객체를 보면 실패해 알린다.

## 실행 한 번의 요약

`run.log` 의 `industrial-land-sale-notice-summary-json` 한 줄: 원천, 걸은 목록 쪽 수, 제공자가 밝힌 총 건수, 이미 끝난 수, 물은 공고 수,
첨부 `by_format`·`bytes_stored`·`stored`·`deduplicated`·`gone`·`oversize`, 새로 쓴 목록 객체. 첫 채우기와 원천별 양은 Silver 로 본다:

```sql
SELECT r.notice_source, count(DISTINCT r.notice_id) AS notices, count(a.sha256) AS files, sum(a.size_bytes) AS bytes
FROM r2.silver.industrial_land_sale_notice_read r
LEFT JOIN r2.silver.industrial_land_sale_notice_attachment a USING (notice_source, notice_id)
GROUP BY r.notice_source;
```
