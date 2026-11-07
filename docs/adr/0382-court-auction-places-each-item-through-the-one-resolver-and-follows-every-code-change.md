# ADR 0382: 법원경매는 물건마다 정본 해석기 하나로 필지를 잇고, 그 연결은 모든 법정동 코드 변경을 따라간다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md)(모든 필지 원천은 정본 해석기
  하나, 3단계 = 경매 수렴), [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md) §3
  (`views.changed_codes`), [ADR-0351](./0351-a-masked-lot-is-inferred-only-from-measured-evidence-and-every-link-follows-code-changes.md)
  §3(연결은 적재 때 한 번 정하고 끝내지 않는다), [ADR-0353](./0353-court-auction-is-a-parcel-kind-built-partially-and-baked-in-its-own-job.md)
  (경매 종류 레인)

## Context

법원경매 Silver(`courtauction_go_kr.py`)는 물건의 PNU 를 **주소 이름**으로만 찾았다. 행정구역 이름(시도·시군구·
읍면동·리)과 대표 지번을 `address_pnu` 가 법정동 이름 표에 맞춘다(ADR-0280 §4). 두 가지가 틀렸다.

1. **목록 행에는 코드가 이미 있다.** 2026-10-21 전수 목록 수집본(124,533 행, 노트북에서 읽기만)을 다시 쟀다.
   대표 필지의 법정동 코드가 네 필드(`daepyoSidoCd` 2자리 + `daepyoSiguCd` 3 + `daepyoDongCd` 3 + `daepyoRdCd` 2)에,
   대표 지번이 `daepyoLotno` 에 있다. 둘로 19자리 PNU 가 **121,963 행(97.9%)** 에서 바로 만들어진다. 못 만드는 행은
   코드가 빈 804 행(0.6%)과 지번이 산문인 1,766 행(1.4%, "외 6필지", "…도시개발구역5블록")이다.
   `srchHjguRdCd`·`srchHjguLotno` 는 사건의 지번 여러 개를 `^` 로 이은 목록이라(53,864 행) 대표 필지가 아니다.
2. **한 번 정하고 끝났다.** 적재 때 정한 PNU 가 Gold 까지 그대로 갔다. 법정동 코드가 바뀌면(분구·통합·번호 바꿈,
   `reference.legal_dong_code_change`) 이미 실린 물건은 옛 코드에 남는다. 사장 규칙(2026-10-31): 모든 데이터는
   법정동 코드 변경을 따라가야 한다. 실거래가 같은 구멍을 ADR-0351 §3 으로 막았다.

ADR-0288 §2 는 경매가 따로 쓰는 해석 길을 정본으로 수렴하라고 했다(3단계). 공매는 이미 코드 길(`ltnoPnu` →
`place_identity_resolver.resolve`)을 탄다(1단계 #820).

## Decision

1. **Silver: 코드 길이 먼저, 주소는 대체.** 물건 행마다 원천 코드 + 대표 지번으로 `source_code_pnu_raw` 를 만든다
   (`place_identity_resolver.code_lot_pnu`, 지번 문법은 `address_pnu.LOT` 그대로). 필드와 자리수는 계약
   `courtauction-go-kr.contract.json` `pnu_resolution.from_code` 에 있고 코드에는 없다.
   - 그 PNU 는 공매와 같은 `place_identity_resolver.resolve` 를 지난다: 옛 코드는 지금 코드로 옮기고, 지금 필지에
     있을 때만 `pnu` 가 된다. `pnu_basis` 는 `legal_dong_code`(코드 그대로) 또는 `legal_dong_code_superseded`
     (옮겨짐)다.
   - 코드 길이 못 잇는 행(코드 없음·산문 지번·필지 없음·분할 모호)만 지금의 주소 길(`address_pnu`, 정본 해석기의
     주소 길)로 간다. 둘 다 못 이으면 `pnu` 는 비고 이유가 남는다(추측 금지).
   - `source_code_pnu_raw` 는 원천의 증거로 Silver 에 남는다(공매의 `ltno_pnu_raw` 와 같은 자리).
2. **Gold: 짓기마다 지금 코드로 옮긴다.** `court_auction_silver_to_gold` 는 `reference.legal_dong_code_change` 를
   필수 입력으로 고정(pin)한다. 물건의 자리는 Silver 의 `pnu`, 그것이 비면 `source_code_pnu_raw` 이고, 짓기마다
   지금 쌍(`place_identity_resolver.crosswalk_of`, ADR-0329 의 대체 반영 view)으로 코드를 옮긴다
   (`carried_to_current`). 필지에 있는지는 여기서 보지 않는다: 이력 따라가기와 `servable`(ADR-0292)이 그 일을 한다.
3. **바뀐 코드 아래만 다시 짓는다(ADR-0312 §2).** 코드 변경 표가 바뀐 짓기는 `views.changed_codes` 가 내준 양쪽
   코드의 접두(`views.code_prefix`) 아래 물건만 영향 키에 넣는다(`code_change_keys`). 통째 입력이 아니다.
   작업 `gold_court_auction_rebuild` 는 파이프라인 그래프 간선 `reference-legal-dong-code-change-to-gold-parcel-court-auction`
   으로 그 표를 읽고, 그 표를 쓰는 `lineage_stewardship` 의 커밋이 이 작업을 시작한다(ADR-0333).
4. **시험이 고정하는 것.** 옛 코드의 원천 PNU 가 지금 필지로 이어지고 원천 코드는 증거로 남는다
   (`test_courtauction_go_kr.ChangesTest`). 짓기는 옛 코드 PNU 를 지금 코드로 옮기고, 새 쌍은 그 코드 아래 물건에만
   닿는다(`test_court_auction_gold.AffectedSparkTest`). 산문 지번·짧은 코드는 PNU 를 만들지 않는다
   (`test_place_identity_resolver.CodeLotPnu`).

## Consequences

- **연결률(계산).** 목록 행 기준 코드 길만으로 97.9%가 PNU 후보를 갖는다. 지금 주소 길은 Silver 43,821 행 중 PNU
  37,922 행(86.5%, 2026-10-19 Trino 읽기 전용 실측)이다. 필지에 실제로 있는지는 첫 적재 요약의 `with_pnu` 로 잰다.
- **한 번 늘어나는 Silver(계산).** 새 열과 바뀐 `pnu_basis` 로 다음 완전한 밤에 목록의 진행 물건(약 108,306 행)이
  한 번씩 새 판으로 쌓인다(append-only). Parquet 로 수십 MB, R2 월 1원 미만이다. 그 뒤로는 바뀐 물건만 쌓인다.
  옛 행은 고치지 않는다. 진행 물건은 매일 밤 목록에 다시 나오므로 따로 되돌려 채우지 않아도 첫 밤에 새 길을 탄다.
- **코드 변경이 오면** 바뀐 코드 아래 물건의 PNU 만 다시 지어진다. 변경 표는 몇천 행이라 짓기마다 통째 읽는다
  (필지 패널 Gold 와 같다).
- **공매 Gold 는 아직 짓기 때 옮기지 않는다.** 공매는 적재 때 정본 해석기를 거치지만 Gold 는 Silver 의 `pnu` 를 그대로
  쓴다. 같은 `carried_to_current` 로 묶는 일은 공매 레인의 다음 PR 몫이다.

## Rejected

- **주소 길만 계속:** 같은 행에 코드가 있는데 이름을 맞추면 옛 이름·오타·동명에서 틀린다. ADR-0288 §2 가 거부했다.
- **`srchHjguRdCd`·`srchHjguLotno`:** 사건 전체 지번 목록이라 행의 대표 필지가 아니다(측정 53,864 행이 `^` 목록).
- **적재 때 정한 PNU 를 그대로 서빙:** 사장 규칙(2026-10-31)과 ADR-0351 §3 이 금한다.
- **코드 변경 때 경매 Gold 통째 재생성:** 바뀐 코드 아래만 닿는다(ADR-0314). 통째로 할 이유가 없다.
