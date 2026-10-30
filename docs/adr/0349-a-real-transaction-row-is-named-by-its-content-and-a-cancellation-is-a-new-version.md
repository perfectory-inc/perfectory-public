# ADR 0349: 실거래 행은 내용으로 이름 붙이고, 계약해제는 지우지 않고 새 판으로 쌓는다

- Status: Accepted
- Date: 2026-10-30
- Builds on: [ADR-0348](./0348-real-transactions-come-from-the-data-go-kr-api-and-recent-months-are-asked-again.md)
  (수집 단위 = (데이터셋, 시군구, 달) 묶음, 다시 묻는 주기),
  [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md) (삭제 표지),
  [ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)
  (바뀐 키만 붙인다), [ADR-0328](./0328-a-compared-silver-table-orders-versions-by-edition-then-commit-instant.md)
  (같은 판을 다시 실으면 나중 적재가 이긴다), [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md)
  (정본 place-identity 해석기), [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md) §4
  (주소로 찾을 때 어느 단계가 찾았는지 남긴다)

## Context

RTMS 는 거래마다 번호를 주지 않는다. 같은 묶음을 다시 물으면(ADR-0348 §3) 그 사이 새 신고가 들고, 해제
(`cdealType`=`O`, `cdealDay`)가 붙고, 아파트는 등기일(`rgstDate`)이 채워진다. 드물게 행이 고쳐지거나 빠진다.
같은 날·같은 값의 거래가 한 묶음에 두 번 나올 수도 있다(같은 단지 같은 층 같은 면적).

연결 재료(2026-10-30 실측, ADR-0348 Context):

| 데이터셋 | 법정동 | 지번 |
|---|---|---|
| 아파트 매매 | `sggCd`+`umdCd`(10자리 코드) | `bonbun`·`bubun`·`landCd` |
| 그 밖의 11종 | `sggCd` + 동 이름 `umdNm`(예 `대곶면 상마리`) | `jibun`; 공장창고 일반건물 70%·토지 100%·상업업무 31% 가림(`3**`), 단독다가구 전월세는 지번 없음 |

## Decision

1. **표 하나: `silver.real_transaction`.** 12 데이터셋이 같은 열로(없는 열은 null) 들어간다. 원천 필드는 정해진
   열(금액 만원 단위·면적 m²·계약일·해제 등)과 함께, 받은 그대로 `provider_fields_json` 에 남는다(정렬된 JSON).
   표가 모르는 필드는 적재 요약의 `unrepresented_fields` 로 센다(ADR-0340 §2 와 같은 규칙).

2. **행의 이름(키) = (`dataset_slug`, `lawd_cd`, `deal_ym`, `row_key`).** `row_key` 는 해제·등기 필드
   (`cdealType`, `cdealDay`, `rgstDate`)를 뺀 원천 필드 전부의 sha256 앞 32자 + `-` + 같은 이름 안의 순번이다. 순번은
   같은 이름의 행들을 바뀌는 필드 순으로 세운 차례라서, 똑같은 두 거래 중 하나만 해제돼도 이름이 흔들리지 않는다.
   행의 내용이 바뀌면(금액 정정 등) 새 이름이 생기고 옛 이름은 아래 3번의 삭제 표지를 받는다.

3. **비교는 묶음마다, 완전한 묶음만.** 적재는 Bronze 객체 하나를 커밋 하나로 싣는다. 객체 안 묶음마다 그 묶음의
   현재 행(계약의 `current_row_latest`)과 견주어(`silver_release_compare.release_changes`) 새 이름·내용이 바뀐
   이름의 행을 붙이고, 묶음의 현재 행인데 이번 답에 없는 이름에 삭제 표지(`row_state = removed`)를 붙인다. 묶음은
   수집기가 `totalCount` 까지 다 받은 것만 Bronze 에 있으므로(ADR-0348 §2) 언제나 완전하다. 비어 있는 답(목록에서
   빠진 시군구의 마지막 질의)은 그 묶음의 행 전부를 내려놓는다. 현재 행은 객체가 가진 묶음의 행만 읽는다(표 전체를
   읽지 않는다: 부분 처리, ADR-0314).

4. **계약해제는 지우지 않는다.** 해제 필드가 붙은 행은 같은 이름의 새 판이다: `cancelled = true`,
   `cancel_date`, `cancel_type`. 읽는 쪽(Gold)이 해제 거래를 시세에서 뺄지 보일지 정한다. 원천이 행을 아예 빼면
   그것만 삭제 표지다. 판의 순서는 `[source_snapshot_id, ingested_at_utc]`(수집 run 의 이름, 적재 시각)라서 같은
   묶음을 다시 실으면 나중 적재가 이긴다(ADR-0328).

5. **PNU 와 법정동은 정본 해석기로(ADR-0288 §1).** 순서:
   - 법정동 10자리: 아파트 매매는 `sggCd+umdCd`. 그 밖은 `reference.legal_dong_code_snapshot` 의 가장 새 스냅숏에서
     (시군구 5자리, 시군구 이름 뒤의 동 이름)이 하나로 가리키는 코드(`존재` 먼저, 없으면 `폐지` 코드). 하나로 안
     모이면 `legal_dong_ambiguous`.
   - 후보 PNU: 법정동 + 산(`산` 또는 `landCd=2` → 2, 아니면 1) + 본번 4 + 부번 4. 가린 지번(`*`)은 후보가 없다:
     `pnu_unresolved_reason = jibun_masked`, `legal_dong_code` 는 남는다. 지번이 없는 원천은 `no_jibun`.
   - 그 후보를 `place_identity_resolver.resolve` 에 넘긴다: 옛 코드는 현재 코드로 옮겨지고(분구·통합), 현재 필지로
     있을 때만 `pnu` 가 된다(`place_identity_grade`). `legal_dong_code` 도 같은 crosswalk 로 현재 코드다.
   - 가린 지번으로 필지를 추정하지 않는다(면적·지목으로 맞추기 등). 국토교통부가 일부러 가린 것을 되돌리는 일이고,
     하려면 사장 결정이 먼저다(Consequences).

6. **연결률을 매 적재 보고하고 떨어지면 시끄럽게.** 적재 요약(`data-go-kr-rtms-summary-json`)에 데이터셋마다 행 수,
   `pnu` 가 선 행, 법정동만 선 행, 이유별 미연결 수를 적는다. 가리지 않은 지번인데 `pnu` 가 안 선 비율이
   `data-go-kr-rtms.contract.json` `silver_load.max_unmasked_unresolved_share` 를 넘으면 그 적재는 행을 다 싣고
   실패로 끝난다(경보, ADR-0288 §6).

## Consequences

- 시군구 목록(`lawd-set.json`, ADR-0348 §2)은 이 적재가 같은 스냅숏에서 쓴다: 하루 한 번(20시간 넘게 낡았을 때)
  Spark 를 띄워 쓰고, 쌓을 객체가 없으면 그것만 하고 끝난다.
- 연결률(2026-10-30 실측, 적재 전: API 2025-09, 무작위 30개 시군구, 이 모듈의 함수로):

  | 데이터셋 | 행 | 법정동 | PNU 후보(가리지 않은 지번) | 후보 중 현재 필지(12개 시군구, Trino) |
  |---|---:|---:|---:|---:|
  | 공장창고 | 171 | 100% | 70% | 14/14 |
  | 토지 | 2,597 | 100% | 0% (모두 가림) | – |
  | 상업업무용 | 834 | 100% | 76% | 189/190 |
  | 아파트 매매 | 8,200 | 100% | 100% | 3,745/3,757 |
  | 연립다세대 매매 | 1,813 | 100% | 100% | 489/489 |
  | 오피스텔 매매 | 1,096 | 100% | 100% | 475/475 |
  | 단독다가구 매매 | 379 | 100% | 0% (모두 가림) | – |

  공장창고·토지의 "내 필지 거래" 는 상당 부분 법정동까지만 이어지므로, 화면의 주된 쓰임은 같은 법정동의 비교
  거래다(Gold, 다음 결정). 전월세 4종은 재지 않았다(단독다가구 전월세는 지번이 없다).
- 사장 결정 대기: 가린 지번의 필지 추정(같은 법정동·본번 앞자리·대지면적·용도지역이 하나로 맞는 필지). 민간
  서비스들이 하는 일이지만 원천의 비식별 의도와 부딪힌다. 이 ADR 은 하지 않는다.
