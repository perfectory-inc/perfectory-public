# ADR 0379: 지적 판(edition)은 준비 판정을 통과한 승격으로만 서빙되고, 승격은 계약의 덧붙이기 원장에 남는다

- Status: Accepted
- Date: 2026-11-04
- Implements: [ADR-0291](./0291-the-served-cadastral-edition-is-operator-chosen-and-readiness-gated.md) §2(준비 판정)·§4(승격 관문)·§5(서빙판 단일 읽기), §1 의 기록 부분
- Builds on: [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)(판을 나란히 둔다), [ADR-0316](./0316-parcel-boundaries-are-partitioned-by-edition-and-range-ordered-by-pnu.md)(판별 파티션), [ADR-0209](./0209-the-data-flow-map-shows-the-edge-path-from-korea-and-the-ci-time-trend.md)(호스트가 쓴 파일을 API 가 그대로 보인다)

## Context

ADR-0291 은 "운영자가 판을 고르고, 판마다 데이터가 다 있는지 보이며, 준비 안 된 판은 서빙되지 않는다"를 정했지만
2026-11-04 main 에는 그 어느 것도 없었다. 실측(2026-11-04, Trino `parcel_boundaries$partitions`, 메타데이터만):

| 판 | Silver 행 | 계약 측정 | 리허설(타일 굽기 저널) |
|---|---|---|---|
| 202606 (서빙) | 39,861,511 | 2026-08-31 | — (서빙 중) |
| 202609 | 39,876,546 | 2026-10-05 | Gold 는 만들었으나 대조 관문 거부(10-15), bake 실패 |
| 202610 | 0 | 없음 | Gold 실패(10-16) |

- `served_edition` 은 여전히 손으로 고치는 글자였다. 바꾸는 PR 이 "그 판이 다 있나"를 보이지 않아도 막는 것이 없었다.
- 서빙판 읽기 규칙(§5)은 공매·필지패널·소유자만 지켰다. `address_pnu.lakehouse_inputs`(법원경매·공매 Silver·등록공장·
  place_identity 가 공유)와 실거래 Silver 적재(#988)는 `current_rows(parcel_boundaries)` 로 **모든 판**의 현재 행을 읽었다 —
  2026-10-21 공매 632 고아와 같은 버그 종류. 실거래는 그 때문에 매 run 7,974만 행(두 판)을 읽었다.

대기업 사례: Kargo(Akuity)는 "Freight(판)가 단계의 검증을 통과해야 승격되고, 승격은 Git 커밋으로 남는다"로, Argo CD·Flux 의
GitOps 는 원하는 상태를 Git 에 두고 사람의 승인을 PR 로 남긴다. Netflix·Airbnb 의 WAP(Write-Audit-Publish, 우리 ADR-0334)도
감사(audit)를 통과한 판만 게시한다. 우리는 이미 계약 JSON 이 정본이고 PR 이 승인이므로, 그 위에 관문과 원장을 얹는다.

## Decision

1. **준비 판정은 판마다 세 단계다.** `infra/lakehouse/spark/jobs/parcel_edition_readiness.py readiness` 가 계약과 호스트가 잰
   증거로 판마다 `source`(계약이 측정함 + 제공자가 추출 뒤 다시 올린 파일 없음) → `silver`(행이 서빙판의
   (1 − `readiness.max_row_loss_fraction`) 이상) → `rehearsal`(리허설 Gold 가 대조 관문 거부 없이 만들어지고 그 Gold 의 bake 통과)
   를 보고, 셋 다 통과해야 `ready`, 아니면 첫 실패 단계를 이름한다. 증거가 말하지 않는 단계는 실패다(안 잰 것은 준비 아님).
2. **증거는 각 단계에 답하는 가장 작은 읽기다(부분 처리).** `scripts/ops/parcel-edition-readiness.sh` 가 Trino 파티션 메타데이터
   (데이터 파일 0개 읽음, 1초 미만), 판 수집 작업이 남긴 최신 제공자 목록, 타일 굽기 저널만 읽어
   `/var/lib/foundation-platform/parcel-editions/readiness.json` 을 쓴다. 이 파일은 지금의 보기(통째 교체)이고, 기록은 아래 원장이다.
3. **`served_edition` 의 유일한 작성자는 `parcel_edition_readiness.py promote` 다.** 보고서가 그 판을 `ready` 라 하지 않거나
   (단계에서 다시 계산 — 손으로 고친 `ready` 는 통과 못 함), `readiness.max_report_age_hours`(48h)보다 오래됐거나, 다른 계약에서
   만들어졌으면(`contract_sha256`) 거부하고 계약을 건드리지 않는다. 통과하면 `served_edition` 을 옮기고 계약의 `promotions` 원장에
   누가·왜·언제·근거한 준비 판정을 **덧붙인다**. 그 변경은 지금처럼 PR 로 들어간다.
4. **원장이 서빙판을 정한다.** `vworld_parcel_editions.validate`(모든 읽는 쪽과 시험이 부른다)는 `served_edition` 이 원장 마지막
   항목과 다르거나, 원장이 앞 항목을 잇지 않거나(`from_edition`·시각), ADR-0291 이후 항목이 준비 판정을 싣지 않으면 거부한다.
   준비 판정 없는 항목은 첫 항목(202606, #339, 2026-10-05)만 허용한다. 가드 `parcel-edition-promotion-is-readiness-gated.sh` 와
   self-test(진짜 계약 사본에 단계 하나씩 빠진 판을 심어 거부·계약 불변을 확인, 손으로 옮긴 서빙판 거부 확인).
   막는 사고: 202606 이 넉 달 서빙되는 동안 손 편집 PR 하나면 리허설 실패한 202609 도 서빙될 수 있었다.
5. **서빙 우주는 `vworld_parcel_editions.served_rows` 하나로 읽는다.** `read_served_parcels` 도 그 위에 서고, `address_pnu`·실거래가
   이것으로 바뀐다. 가드 `parcel-reads-the-served-edition.sh`(AST)는 Spark 작업에서 `parcel_boundaries` 를 넘기는
   `current_rows`·`current_rows_of_versioned`·`current_row_latest` 호출을 거부한다. 막는 사고: 2026-10-21 공매 632 고아.
6. **배포 순서(관문은 한 배포 늦게 선다).** ① 이 변경: 관문은 계약·CI 안에 있어 **병합 순간** 선다(서빙판은 202606 그대로라 동작
   변화는 §5 두 읽기뿐). ② 배포 뒤 호스트에서 준비 판정을 처음 쓴다. ③ 판정 파일을 읽는 API·더니어 화면(후속 PR)을 배포한다.
   ④ 그 화면에서 `ready` 를 본 뒤에만 첫 `promote` PR. 화면에서 바로 옮기는 단추는 ADR-0291 §3 의 동시 재굽기 오케스트레이션이
   선 뒤에 연다 — 그 전에 옮기면 지도·패널·타일이 서로 다른 판을 서빙한다(§3 이 금지).

## Consequences

- "10월 판 다 있나?"가 숫자로 답해진다. 지금(11-04 계산): 202609 는 `rehearsal`, 202610 은 `source` 에서 멈춘다.
- 실거래 Silver 적재와 `address_pnu` 를 쓰는 네 생산자는 서빙판 하나만 읽는다: 필지 읽기가 7,974만 → 3,986만 행(파티션 가지치기,
  계산값). 서빙판에 없는 새 필지(202609 에만 있는 PNU)는 그 판이 승격될 때까지 PNU 미해결로 남는다 — 지도가 보이지 않는 필지에
  붙지 않는다는 뜻이고, 승격하면 다음 run 에 풀린다.
- 남는 일: 판정 주기 실행(작업 목록 등록), API `GET /data-catalog/v1/parcel-editions` 와 더니어 판 화면, §3 동시 재굽기.
