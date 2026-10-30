# ADR 0352: 법원경매 재수집 목록은 매 실행 lakehouse 에서 계산한다 — Postgres 대기열을 두지 않는다

- Status: Accepted
- Date: 2026-10-30
- Amends: [ADR-0290](./0290-court-auction-details-are-refetched-by-change-and-sale-date-and-photos-are-content-addressed.md) §4(우선순위 대기열)·§5(실패 장부)
- Relates to: [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)(행 버전), [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(부분 처리가 기본), [ADR-0318](./0318-the-remaining-silver-lanes-append-their-changed-keys-compared-bucket-by-bucket.md)(바뀐 키만 쌓기)
- 대체한 초안: PR #840(ADR-0304 로 제안됐으나 번호가 병합되지 않은 채 남았다)

## Context

ADR-0290 §4 는 상세 재수집 요청을 foundation Postgres 의 대기열 표에 `(점수, 다음 수집 시각)` 으로 쌓고 거기서
꺼내 받기로, §5 는 실패 key 를 그 대기열 옆 장부에 남기기로 정했다.

사장 질문(2026-10-22): "반드시 DB 에 저장해야 하나? 대기업식은?"

확인한 사실:

1. **대규모 크롤러가 프론티어를 영구 저장하는 까닭은 규모다.** Apache Nutch 의 CrawlDb, Google 의 프론티어 KV 는
   URL 마다 다음 수집 시각을 들고 있어야 한다 — 수십억 건이라 매번 전체를 다시 읽을 수 없기 때문이다.
2. **경매는 그 전제가 성립하지 않는다.** 날짜 창(±365일)을 준 물건 목록은 108,306 행이다(2026-10-21 실측,
   ADR-0290 §1). 받은 것은 이미 lakehouse(R2/Iceberg)의 `silver.court_auction_item` 에 내용·받은 시각·
   행 해시와 함께 쌓인다(2026-10-30 운영 Silver: 43,821 행, 10-19 밤 한 번 적재분).
3. **Postgres 에 재수집 상태를 영구로 두면 "lakehouse 가 중심" 원칙을 깬다.** 운영의 정본은 lakehouse 이고
   Postgres 는 거기서 재구성 가능해야 한다. 대기열 표가 "다음에 뭘 받을지"의 정본이 되면 lakehouse 로 되살릴 수
   없는 상태가 Postgres 에 생긴다.

## Decision

ADR-0290 §4·§5 를 다음으로 바꾼다. §1·§2·§3·§6–§10(범위·변경 감지·점수·주간 대조·사진·PNU·Silver·SSOT)은
그대로다.

1. **받을 목록은 저장하지 않고 매 실행 계산한다.** 별도 Postgres 대기열 표를 두지 않는다. 그날 받을 상세 목록은
   **새로 받은 목록 + Silver 의 지난 행 해시·마지막 수집일**로 ADR-0290 §3 의 점수를 그 자리에서 계산해 정한다.
   점수 높은 것부터 그날 예산(ADR-0300 H 의 실행당 상한)만큼 받는다. 같은 key 는 한 실행에서 한 번만 받는다.
   **시험:** 같은 lakehouse 상태를 두 번 계산하면 같은 목록·같은 순서가 나온다(결정적).
2. **실패 장부는 lakehouse 에 append-only 로 둔다.** 막힘이 아닌 오류로 끝내 못 받은 key 는 이유·시각·횟수와
   함께 lakehouse 에 쌓인다(전 데이터 append-only). 다음 실행이 목록을 계산할 때 이 장부를 함께 읽어 아직 안
   받힌 실패 key 를 먼저 넣는다. 장부 건수는 지표로 내보낸다. 성공하면 그 key 의 다음 append 가 해소를 적는다
   (덮어쓰지 않는다).
3. **이 계산이 생기기 전까지 목록 수집은 전수(full)로 남고, 그 사유는 작업 목록이 든다.**
   `court_auction_collect` 의 `full_tracked_by` 는 이 ADR 을 가리킨다: 사이트에 "바뀐 것만" 질의가 없어 목록은
   매 밤 끝까지 읽어야 하고(ADR-0281), 상세는 이 결정의 계산이 있어야 바뀐 것만 받는다.

## Consequences

- 상세 수집 구현에서 DB 마이그레이션(ADR-0290 의 옛 2c 단계)이 사라진다. 되돌리기 번거로운 지점 하나가 없어진다.
- "다음에 받을 목록"의 정본이 lakehouse 하나가 된다(SSOT) — 대기열 표와 lakehouse 가 어긋날 일이 없다.
- 매 실행 10만여 행의 지난 해시·마지막 수집일을 Silver 에서 읽는 비용이 든다. 한 표 한 번 읽기라 Spark 없이도
  가능한 크기다(실측은 상세 수집 첫 실행에서 잰다).
- 실행이 중간에 끊겨도 상태 손실이 없다 — 다음 실행이 lakehouse 에서 다시 계산한다. 영구 대기열이 주던
  "이어받기"를 계산이 대신한다.
- ADR-0290 에 이 개정을 가리키는 날짜 각주를 단다.

## Rejected

- **Redis/Kafka 프론티어:** 인프라를 새로 들인다. 10만 행 규모에는 과하다.
- **ADR-0290 §4 유지(Postgres 대기열):** 대규모 크롤러의 방식이지만 그 이유(전체를 못 읽음)가 우리에겐 없다.
  lakehouse 중심 원칙과도 어긋난다.
