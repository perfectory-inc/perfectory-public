# ADR 0044: 사실의 이름을 단 컬럼은 그 사실을 담아야 한다

- Status: Accepted
- Date: 2026-08-18

## Context

`bronze/source=vworldkr__sandan_profile/…zip` 안 `TB_IRSTT_BASS_HIST.xlsx` 의 헤더는 **20칸**이다.
`profile_workbook_decoder.rs` 는 그중 10칸을 읽었고, 나머지 10칸은 화면까지 오지 못했다.
1,442행 전수를 세어 각 칸이 실제로 무엇을 담고 있는지 확인했다.

여덟 칸은 단지에 관한 사실이었고 이번 변경에서 계약에 올렸다.

```
lttot_sttus_nm    분양상태      1442/1442  100.00%   distinct=3
appn_basis_law    지정근거법    1442/1442  100.00%   distinct=48
make_procs_rt     조성진행률    1442/1442  100.00%   0.0 ~ 100.0
bsms_pd           사업기간      1441/1442   99.93%   1,440건이 "YYYY-MM~YYYY-MM"
devlop_mth        개발방식      1441/1442   99.93%   distinct=232
invite_upj        유치업종      1440/1442   99.86%   최대 485자
make_purps_cn     조성목적      1439/1442   99.79%   최대 521자
strwrk_de         착공일        1438/1442   99.72%   전부 yyyyMMdd
```

**나머지 두 칸은 올리지 않았다.** 이 ADR 은 그 두 건의 근거를 남기기 위해 존재한다. 근거가
코드 주석에만 있으면, 다음 사람이 20칸 중 18칸만 읽히는 것을 보고 "왜 이것만 빼먹었지" 하며
되살린다. 되살리는 쪽이 훨씬 쉽고, 되살리면 1,442곳 전부에 같은 거짓이 붙는다.

### `frst_regist_de` — 단지의 속성이 아니다

한글 이름은 "최초등록일"이다. 값은 1,442행 **전부 `45809`** 로 동일하다. 엑셀 일련값이고
`1899-12-30 + 45809일 = 2025-06-01` 이며, 이 스냅샷의 기준월 `sta_ym=202506` 의 첫날과 같다.

즉 이 칸은 **원천 시스템이 이 스냅샷 행을 기록한 날**이지 산업단지가 최초로 등록된 날이 아니다.
1964년에 지정된 단지와 2024년에 지정된 단지가 같은 값을 갖는다는 사실 자체가 그 증거다.
이름만 보고 `first_registered_date` 같은 컬럼에 실으면, 1,442곳 전부가 "2025-06-01 에 최초
등록되었다"는 없는 사실을 주장하게 된다. 스냅샷 기준월은 이미 `source_snapshot_id` 와
`valid_from_utc` 가 담고 있으므로, 이 칸이 실제로 말하는 것은 이미 계약 안에 있다.

### `rent_hsmp_se_code` — 원천이 비어 있다

1,442행 전부 빈 값이다. 채우는 생산자가 없는 칸을 만들면 영구 NULL 컬럼이 하나 늘 뿐이고,
그것이 [ADR-0040](./0040-a-column-no-producer-fills-cannot-be-required.md) 이 거부하는 형태다.
채우는 스냅샷이 나타나면 그때 올린다.

### 열거형과 자유 텍스트를 가르는 기준

`lttot_sttus_nm` 은 1,442행에 걸쳐 distinct 3 (`분양완료` 992 · `분양중` 294 · `분양계획` 156)
이다. 반면 `devlop_mth` 은 distinct 232, `appn_basis_law` 은 48 이며, **그 다양성은 뜻이 아니라
철자다**: `공영개발`(649) · `공영개발방식`(88) · `공영개발 방식`(8) 이 각각 다른 값으로 세어진다.
접미사와 띄어쓰기만 다른 같은 뜻이 흩어져 있다.

이것을 코드로 묶으면 없던 분류가 생긴다. 어떤 철자를 어느 코드에 붙일지는 별도의 근거가 필요한
결정이고, 잘못 묶으면 원문으로 되돌아갈 방법이 없다.

## Decision

1. **`frst_regist_de` 와 `rent_hsmp_se_code` 는 디코더가 읽지 않는다.** 두 이름은
   `profile_workbook_decoder.rs` 의 `REQUIRED_HEADERS` 에도 `OPTIONAL_HEADERS` 에도 넣지 않으며,
   같은 파일의 테스트 `the_two_excluded_provider_columns_stay_unread` 가 두 목록 어느 쪽에도
   나타나지 않음을 확인한다. 실패하면 막는 것: **모든 단지에 같은 날짜를 붙이는 컬럼이 계약에
   들어오는 것**, 그리고 **채우는 생산자가 없는 컬럼이 늘어나는 것.**
2. **원천 라벨을 열거형으로 정규화하려면 그 스냅샷 전수 계수가 있어야 한다.**
   `lttot_sttus_nm` 만 정규화한다 — `catalog_domain::IndustrialComplexLotSalesStatus` 와
   `INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES` 가 도메인을 소유하고, 계수는
   `COMPLEX_LOT_SALES_STATUS_LABELS_OBSERVED` 에 행수 주석으로 남는다. 매핑에 없는 라벨은
   행 번호와 함께 **실패**한다. `unknown` 멤버는 두지 않는다: 없는 값은 `null` 이고, 네 번째
   라벨은 숨길 자리가 아니라 실패할 사실이다.
3. **`appn_basis_law`·`devlop_mth`·`make_purps_cn`·`invite_upj` 는 원문 그대로 싣는다.**
   컬럼 이름에 `_raw` 접미사를 붙여 원문 보존이 계약임을 이름에서 읽히게 한다. 값 도메인 CHECK 를
   두지 않으며, 길이 제한도 두지 않는다 — 자르면 없던 사실이 생기고, 관측된 최대 길이는 521자다.
   정규화는 별도 근거가 생겼을 때의 별도 결정이다.
4. **`bsms_pd` 는 원문과 파생 두 달을 함께 싣는다.** `business_period_raw` 가 원문이고,
   `business_period_start_month` / `business_period_end_month` 는 `YYYY-MM~YYYY-MM` 이 파싱될 때만
   채워진다. 두 파생 컬럼은 **항상 함께 null 이거나 함께 값이 있다** — 한쪽만 있는 것은 원천이
   긋지 않은 경계를 긋는 것이다. Postgres CHECK
   `industrial_complex_business_period_months_together`, Spark 게이트
   `invalid_business_period_months_count`, 그리고 적재기의
   `validate_optional_business_period_months` 가 세 계층에서 같은 불변식을 강제한다.
   1,441건 중 1건(`2020-~2024-`)은 원문을 온전히 유지한 채 두 파생 컬럼이 null 이 되고,
   Bronze 내보내기 요약이 그런 행의 수와 단지 코드를 적는다.
5. **`make_procs_rt` 는 `numeric(5,2)` / `decimal(5,2)` 로 싣고 부동소수로 옮기지 않는다.**
   `59.9` 는 이진 부동소수로 정확히 표현되지 않으므로, `f64` 를 거치면 원천이 말하지 않은 숫자를
   다시 발행하게 된다. 이 워크스페이스에는 채택된 Rust decimal 타입이 없으므로
   ([technology-stack](../technology-stack.md) §1.1), Rust 는 정확한 십진 **텍스트**로 나르고
   Postgres 가 산술 도메인을 소유한다. `0` 은 값이다 — `준비중`·`보상중` 단지의 평균 조성진행률이
   정확히 0.0 이다 — 따라서 `official_area_sqm` 이 가진 `> 0` 게이트를 두지 않는다.
6. **`strwrk_de` 는 `appn_de` 와 같은 파서를 탄다.** 새 날짜 파서를 만들지 않는다. 두 파서는
   날짜가 무엇인지에 대해 의견이 갈릴 수 있는 두 자리다.
7. **여덟 칸은 `REQUIRED_HEADERS` 가 아니라 `OPTIONAL_HEADERS` 다.** 이번 스냅샷에는 여덟 개가
   모두 있지만, 필수로 박으면 다음 스냅샷에서 한 칸이 빠졌을 때 1,442건 전부가 못 들어온다.
   `rent_hsmp_se_code` 가 0% 인 것이 이 제공자가 컬럼을 비운다는 증거다.
   **다만 없는 것을 조용히 넘기지는 않는다**: 디코더가
   `DecodedProfileSheet::absent_optional_headers` 로 사라진 헤더 이름을 돌려주고, 내보내기 요약이
   그것을 적으며, `evidence_limitations` 에
   `the_worksheet_did_not_carry_every_optional_column_read` 가 붙는다. 실패하면 막는 것:
   **컬럼이 사라진 것과 셀이 비어 있는 것이 똑같이 "전부 null" 로 보이는 것.**
8. **살아 있는 Iceberg 표는 계약이 넓어져도 저절로 넓어지지 않는다.** `silver.industrial_complexes`
   도 `gold.complex_catalog` 와 같은 스키마 진화 단계를 거친다. 단계 자체는
   `platform_contracts.evolve_iceberg_table_to_contract` 하나이며 두 잡이 그것을 부른다 —
   같은 코드를 두 벌 두면 한쪽 표만 넓히는 일이 가능해진다.

## Consequences

- `silver.industrial_complexes` 와 `gold.complex_catalog` 가 각각 10칸 넓어지고,
  `catalog.industrial_complex` 도 같은 10칸을 nullable 로 얻는다. 여덟 개의 원천 칸이 10개의 계약
  칸이 되는 이유는 `bsms_pd` 하나가 원문 + 파생 2로 셋이 되기 때문이다.
- `GET /catalog/v1/complexes` 응답이 그만큼 넓어진다. OpenAPI 산출물과 gongzzang 소비자 핀 해시를
  같은 변경에서 갱신했다.
- **실 테이블 재실행이 필요하다.** Bronze JSONL 재생성 → Silver 재실행 → Gold 재실행 →
  canonical 적재 순서로 돌려야 새 칸이 실제로 채워진다. Silver 와 Gold 는 `overwrite` 로 돌린다:
  같은 `source_snapshot_id` 를 append 하면 `(official_complex_code, source_snapshot_id)` 유일성
  게이트가 그 자리에서 실패한다. Iceberg 의 overwrite 는 새 스냅샷을 만들 뿐이므로 이전 스냅샷은
  시간여행으로 남는다.
- `frst_regist_de` 를 되살리고 싶은 다음 사람은 이 ADR 을 대체하는 새 root ADR 을 써야 한다.
  그 ADR 은 "1,442행이 전부 같은 값이 아니게 되었다"는 새 계수를 근거로 가져와야 한다.
- 자유 텍스트 4칸의 정규화는 열려 있는 후속 작업이다. 원문이 계약으로 보존되어 있으므로 언제든
  근거를 갖춘 별도 결정으로 파생 컬럼을 더할 수 있고, 그때도 원문은 남는다.
