# ADR 0380: 새 지적 판은 스스로 준비에 다가간다 — 판정은 예약 작업이 바뀐 판만, 재업로드된 판은 다시 받는다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0379](./0379-a-cadastral-edition-is-served-only-through-a-readiness-gated-promotion-recorded-in-the-contract.md)(판별 준비 판정·승격 관문), [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)(판을 나란히 둔다), [ADR-0253](./0253-the-parcel-tile-bake-runs-in-two-halves-and-is-rehearsed-before-an-edition-is-served.md)(리허설), [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(부분 처리가 기본)

## Context

2026-11-04 판정: 202609 는 `rehearsal`, 202610 은 `source` 에서 멈춘다. 서버를 읽기만 하며 원인을 쟀다.

| 판 | 멈춘 곳 | 증거 | 근본 원인 |
|---|---|---|---|
| 202609 | 리허설 Gold | `parcel-tile-bake/20261015T150659Z/run.log`: `parcel_matching_gate.py` 가 `EditionError ... pass --allow-non-served-edition` 로 3초 만에 죽음. 저널은 이것을 "the matching gate refused" 로 적었다 | 당시 스크립트가 관문에 판 허용 플래그를 넘기지 않았고, 죽음을 거부로 읽었다 — #589(10-15) 가 고쳤다 |
| 202609 | 리허설 굽기 | `bake.log`: `maxzoom tiles miss 542 of the 39876546 served ids` | 542 필지는 면적 ~1e-12 도²(WKB 86 바이트 삼각형)의 화소 미만 조각이며 202606 에도 있다. ADR-0262(#618, 10-15)가 화소 미만 누락을 허용했다 |
| 202610 | 리허설 Gold | `20261016T033522Z/run.log`: `TABLE_OR_VIEW_NOT_FOUND silver.parcel_registry` | 그때 표가 없었다. 지금은 있다(`SHOW TABLES` 11-04) |
| 202610 | 원천 | 계약 항목에 `measured_at_utc` 없음; 작업 저널 11-01: `provider re-uploaded 272 file(s) of held edition 202610 after its extraction`(10-14~16) | ① `propose` 가 측정일을 싣지 않았다 ② 판을 "가진" 날은 재업로드를 경고만 하고 다시 받지 않았다 |
| 202610 | Silver 0행 | 계약 항목이 낡은 업로드(266 객체)를 말하므로 적재할 수 없다 | 위 ②의 결과. 적재는 런북 §4 수동 |

세 리허설 실패는 모두 main 에서 이미 고쳐졌는데, 리허설은 운영자만 돌리므로 고친 뒤 아무도 다시 돌리지 않았다.
판정도 운영자가 손으로 돌려야 했다(ADR-0379 남은 일).

## Decision

1. **판정은 예약 작업이다.** `parcel_edition_readiness`(jobs.v1.json, `skip-if-unchanged`, 매일 08:10 UTC, 상한 10분, 배포 직후 한 번)
   가 `scripts/ops/parcel-edition-readiness.sh` 를 돈다. **판마다 입력 지문**(`edition_inputs`: 그 판의 계약 항목, 서빙판과 그 행 수,
   관문 값, 그 판의 증거)을 남기고, 지문이 움직인 판만 다시 판정한다. 나머지 판의 줄은 그대로 둔다. 판정한 판이 없으면
   결과는 `unchanged`다. 입력은 메타데이터와 작은 파일뿐이다(파티션 1회 질의 수 초, 11-04 잰 값).
2. **아직 서빙하지 않는 판의 파일을 제공자가 갈면 다시 받는다.** `vworld-parcel-edition-collect.sh` 는 가진 판의 재업로드를
   경고만 했다. 이제는 그 판을 다시 받아 다시 제안한다(새 판과 같은 길: 측정, 제안, 종료 3, PR). Bronze 키가 내용이라
   앞 업로드는 남고 같은 파일은 건너뛴다. 서빙판은 다시 제안하지 않는다(서빙판을 옮기는 것은 승격, ADR-0379).
3. **제안은 측정일을 싣는다.** `propose` 가 `measured_at_utc`·`how_measured` 를 쓴다. 판정의 원천 단계가 읽는 값이다.
4. **남는 수동 단계와 그 순서.** Silver 적재(런북 §4: 변환 약 40분, 적재 약 20분, 16g 드라이버)와 리허설(Gold 약 14분, 굽기 약
   82분)은 무거운 Spark 다. 자동화하려면 단위·자격증명 묶음·spark 풀 계산을 손으로 한 번 감독해 재야 한다(ADR-0122 §4:
   켜는 것은 따로 하는 배포 단계). 그래서 이 ADR 은 판정이 "다음에 할 일"을 이름하게만 한다(첫 실패 단계). 판 하나가
   준비에 이르는 길은 다음과 같다.
   - (a) 재업로드 판 재수집·제안 → PR(자동, 이 ADR)
   - (b) Silver 적재(런북 §4)
   - (c) 리허설 Gold·굽기(런북 parcel-tile-bake §2)
   - (d) 판정 `ready`(자동, 이 ADR)
   - (e) 승격 요청(콘솔, ADR-0381)

## Consequences

- 202610: 다음 수집 실행이 272 파일을 다시 받아 측정일이 있는 항목을 제안한다. 그 PR 이 들어가면 원천 단계를 통과하고,
  이어서 Silver 적재를 기다린다.
- 202609: 막던 세 가지가 모두 고쳐졌다. 리허설 한 번(약 96분)이면 `rehearsal` 이 판정된다. 그 뒤 `ready` 면 승격할 수 있다.
- 비용: 재수집은 272 파일, 전국 약 12GB 내려받기(10-15 실측 규모). R2 쓰기는 바뀐 객체만이고 비용은 작다. 판정 작업은 하루
  수 초다.
- 남은 일: (b)(c)의 예약 작업화. 비활성으로 등록한 뒤 감독 실행으로 재고 켠다.
