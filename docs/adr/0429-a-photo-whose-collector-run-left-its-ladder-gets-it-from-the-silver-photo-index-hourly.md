# ADR 0429: 수집 run 이 못 만든 사진 사다리는 Silver 사진 색인에서 매시 가져가 만든다 — 경매 사진도 공용 저장 하나

- Status: Accepted
- Date: 2026-11-06
- Builds on: [ADR-0427](./0427-a-photo-is-shown-from-a-rendition-ladder-made-once-from-its-stored-original.md) §3·§4·§7,
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md),
  [ADR-0352](./0352-court-auction-recrawl-is-computed-each-run-from-the-lakehouse-not-a-stored-queue.md),
  [ADR-0386](./0386-every-job-states-its-per-run-bound-in-one-vocabulary-and-its-measured-runs-fit-its-unit.md),
  ADR-0384(경매 사진, #1100)

## Context

ADR-0427 §7 은 경매 사진도 공매의 저장소·사다리를 쓴다고 정했다. 공매는 사다리가 미뤄진 회차를 미완료로 남겨 다음 run
(매시)이 다시 받는다(§4). 경매는 그 길을 쓸 수 없다:

- 물건상세를 다시 읽는 때는 매 run 계산하는 recrawl 규칙이 정한다(ADR-0352) — 사다리가 미뤄졌다고 사이트에 다시 묻는
  것은 예의(밤당 요청 상한)를 쓰는 일이다.
- 양이 다르다(계산): 경매 사건 2만 × 사진 ~11 ≈ **22만** 원천(#1100 계산), 원천 하나 ~4.5 초(ADR-0427 의 rav1e 추정)면
  CPU **~275 시간**. 수집 작업 한도 300 분 중 목록(실측 최장 78 분)·상세(1,800 × ~4 초 ≈ 120 분)를 빼면 사다리에 남는
  것은 ~30 분 ≈ 1,500 원천(동시 4) — 첫 바퀴만 ~150 밤이다.

## Decision

1. **저장은 하나다.** 내용주소 쓰기(키·형식 판정·크기 상한·중복·Cache-Control)는 원천 중립 모듈
   `media_store::store_bytes` 하나이고(#1105), 공매 다운로드와 경매 base64 사진이 모두 거기서 끝난다.
2. **수집 run 은 자기가 저장한 사진의 사다리를 먼저 만든다.** 경매 수집기는 그 밤의 Bronze 가 커밋된 뒤(사이트 요청
   없음) 그 밤 저장했거나 이미 있던 사진마다 `media_renditions::ensure_ladders` 를 부른다. 상한은 수집 계약
   `item_detail.photos.renditions`(원천 1,500·동시 4·1,800 초)이고, 실패는 밤을 깨지 않는다(manifest `item_detail.ladders`).
3. **남은 것은 백로그 작업이 가져간다.** 새 작업 `media_ladder_backlog`(매시 :35, 한도 40 분)가 공용 저장소의 쓰는 쪽
   계약 `data-go-kr-onbid.contract.json` `media.ladder_backlog.index_tables`(지금 `silver.court_auction_item_photo`)를
   읽어 같은 `ensure_ladders` 로 만든다. 상한 `media.ladder_backlog`(원천 1,500·동시 4·1,500 초).
   - 부분: 상태 파일이 표마다 읽은 Iceberg snapshot 과 마지막으로 가져간 `source_snapshot_id` 를, 그리고 아직 안 끝난
     원천을 적는다. snapshot 이 그대로인 표는 읽지 않고, 바뀐 표는 그 run 부터의 줄만 더한다(그 run 은 늦은 커밋을 위해
     다시; 이미 된 원천은 HEAD 한 번). 사다리 version 이 바뀌면 모든 표를 처음부터(ADR-0427 §3).
   - 공매 표는 넣지 않는다: 공매는 자기 길(§4)이 있고, 두 길이 같은 원천을 다투게 하지 않는다.
   - 쓰기는 사다리 객체(CreateOnly)뿐, 원본·색인은 건드리지 않는다(append-only).
   - ADR-0386 §3 대로 **꺼진 채로** 들어온다: 손으로 한 번 돌려 run 시간을 잰 뒤 켠다.
4. **화면도 하나다.** 공매·경매(이어서 매물)는 같은 `components/media/photo-grid.tsx`(`MediaStore` = 기반 URL + 접두사,
   AVIF→WebP `srcset`)를 쓴다(#1105). 출처 표시는 더하지 않는다(사장 결정, ADR-0428).

### 대안

| 대안 | 왜 아닌가 |
| --- | --- |
| 미뤄진 사건을 다음 밤 다시 읽기(공매 방식) | 사이트 요청을 사다리 때문에 쓴다; 22만 원천을 밤 30 분씩이면 ~150 밤 |
| 수집 run 안에서 다 만들기 | 첫 밤 ~19,800 원천 ≈ 6 시간 — 작업 한도 300 분을 넘는다 |
| 엣지 즉석 변환 | ADR-0427 표에서 기각(매달 과금, 정본 밖) |
| R2 목록으로 사다리 없는 원천 찾기 | 원천 하나에 7 객체 — 매 run 수백 LIST; 색인 snapshot 비교가 싸다 |

같은 꼴: 큰 회사들의 이미지 파이프라인은 업로드 경로에서 바로 만들 수 있는 만큼 만들고, 나머지는 큐(이벤트 로그)에서
비동기 작업자가 가져간다(Airbnb·Pinterest 의 썸네일 작업자, Thumbor 의 큐 모드). 여기서 큐는 append-only Silver 색인이다.

## Consequences

- **첫 바퀴(계산)**: 22만 원천 ÷ 시간당 ~1,300(1,500 초 × 4 ÷ 4.5 초) ≈ 170 run ≈ 7 일, CPU ~275 시간(ai-server 20 코어
  중 4). 그동안 사다리 없는 사진은 원천 파일로 보인다(ADR-0427 §3 의 오류 대체).
- **돈(계산)**: 저장 22만 × 사다리 ~0.2–0.4 MB ≈ 45–90 GB → R2 월 $0.7–1.4; 쓰기 22만 × 7 ≈ 154만 Class A ≈ $6.9 한 번;
  원본 읽기 22만 Class B ≈ $0.08; HEAD 는 run 당 ≤ 1,500. 사장 승인 범위(소액) 안이다.
- **지금 경매 사진은 0 장이다**: #1100 이 아직 병합되지 않아 운영에 저장된 경매 사진이 없다. 백로그는 #1100 과 이
  변경이 배포된 뒤의 첫 밤부터 의미가 있다.
- 미디어 대조기(ADR-0387)는 저장소에 사진을 담는 모든 원천의 색인을 본다(`media.reconcile.index_tables`: 공매·경매).
  막는 사고: 경매 사진이 같은 `sha256/` 에 쌓이면 공매 색인만 보는 대조기가 그것을 전부 고아로 세어 매일 경보한다.
  첫 적재 전이라 아직 없는 표는 아무 키도 대지 않는다(대조기가 그 때문에 실패하지 않는다).
- 검증하지 못한 것: rav1e 의 실제 속도·크기(첫 운영 run 의 `media-ladder-backlog-summary-json`), 경매 사진의 실제 픽셀
  크기(긴 변이 960 보다 작으면 단계가 겹쳐 더 빠르다).
