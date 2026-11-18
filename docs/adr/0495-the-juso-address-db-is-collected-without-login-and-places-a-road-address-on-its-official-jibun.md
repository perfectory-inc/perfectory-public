# ADR 0495: 주소DB 는 로그인 없이 공개 내려받기로 받고, 도로명 주소를 그 공식 지번에 놓는다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md) §4(주소 → PNU),
  [ADR-0482](./0482-a-parcel-source-is-placed-on-the-served-editions-form-of-its-code-and-a-codeless-public-sale-by-its-address.md)(서빙판의 코드 형태),
  [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md) §3(법정동 코드 변경 따라가기),
  [ADR-0318](./0318-the-remaining-silver-lanes-append-their-changed-keys-compared-bucket-by-bucket.md)(바뀐 키만 쌓기),
  [ADR-0457](./0457-a-job-never-run-takes-its-first-run-unattended-bounded-by-its-unit-and-the-run-is-the-measurement.md)(첫 run 이 측정)
- 관련: [ADR-0475](./0475-the-juso-electronic-map-is-collected-every-day-from-the-official-daily-change-linkage-with-one-approval-key.md)(전자지도는 승인키 연계), ADR-0001(가드는 막는 사고를 밝힌다)

## Context

도로명 주소만 가진 행은 지금 건축물대장 표제부의 도로명 주소로만 PNU 에 놓인다(ADR-0280 §4). 표제부에 없는 건물,
표제부가 다른 형식으로 쓴 도로명, 둘 이상의 필지에 걸친 건물은 놓이지 않는다.

W65 측정(2026-11-11, 운영 Trino 읽기 전용 + 202610 주소DB 전체본):

| 방법 | 공매 정밀도 | 경매 정밀도 |
|---|---|---|
| 도로명 → 대표지번 | 287/287 = 100% | 2,547/2,573 = 99.0% |
| 지번(시군구 일치) | 7,555/7,564 = 99.9% | 788/788 = 100% |
| 지번(시군구 무시) | 50% | 18.5% |
| 건물명 | 28.6% | 0% |

도로명 → 지번으로 새로 놓이는 행은 경매 +1,702, 공매 +1,426 이다(W65 계산). 지번 쪽 차이는 대부분 우리 지번
해석기의 빈틈이라 juso 없이 고친다(별도 PR: "999외 9필지", 옛 구 이름).

주소DB 를 받는 길을 재었다(2026-11-11 실측):

- 제공처 내려받기 화면의 API 가 **로그인·키 없이** 답한다.
  - `POST business.juso.go.kr/api/jst/selectAttrbDBDwldList {rtlDtaDtlSn: 8, year, month, expand: Y}` → HTTP 200 JSON.
    그해 열두 달의 월전체(`allMonthFileList`)와 물은 달·그 전 달의 일변동(`dayFileList`)을 준다.
  - `GET /api/jst/download?regYmd&reqType&ctprvnCd&stdde&fileName&realFileName` → zip.
    202610 월전체 181,440,694 B(49 s), 20261029 일변동 18,583 B.
- 월전체 안: `주소_<시도>.txt`(642만 주소), `지번_<시도>.txt`(820만 지번, 대표 여부), `개선_도로명코드_전체분.txt`
  (37만 도로명). CP949, `|` 구분. 인천은 이미 제물포구·영종구·서해구·검단구, 법정동 코드는 현재 코드다.
- 일변동: `AlterD.JUSUMT.<날>.MATCHING_JUSO/JIBUN/ADDINFO/ROAD.TXT`, 같은 칸에 변경사유(31 신규·34 변경·63 폐지).

큰 곳의 방식: 변경 데이터 수집은 스냅샷 하나에 변경 로그를 잇는다(Debezium 의 initial snapshot + binlog, AWS DMS 의
full load + CDC). 소비자가 자기 오프셋을 가지고, 같은 위치를 다시 물으면 같은 것을 받는다(Kafka 소비자 그룹 오프셋).
주소 정규화는 공식 주소 원장에 맞춘다(USPS CASS 인증은 원장 일치만 인정한다; 이름 유사도는 쓰지 않는다).

## Decision

1. **원천은 공개 내려받기다.** 계약 `infra/lakehouse/contracts/juso-address-db.contract.json` 이 두 경로, 질문, 파일
   종류 두 가지, run 당 상한, 안의 파일 이름을 가진다. 다른 곳은 이 값을 다시 적지 않는다. 키·비밀은 없다.
2. **월전체는 한 번, 기초로.** 수집기(`collect-juso-address-db`)는 상태(`state.json`)에 기초가 없을 때만 가장 새 월전체를
   받는다(`jusogokr__address_db_monthly_full`). 다음 달 전체본을 다시 받지 않는다. 바뀐 것만 받는다(사장 원칙).
3. **일변동은 매일, 커서는 우리 것.** 커서는 기초 달 첫날부터 시작한다(한 달을 기초 위에 다시 재생; 같은 키의 같은 변경은
   같은 행으로 끝난다). run 은 달마다 목록을 한 번 묻고, 커서부터 어제(KST)까지의 일변동을 오래된 것부터
   `jusogokr__address_db_daily_change` 에 바이트 그대로 남긴다. 파일마다 커밋한 뒤에만 커서를 옮긴다. 목록에 없는 날은
   더 뒤의 날이 실린 뒤에만 지나간다. 객체 이름은 제공처가 저장한 파일 이름이라 두 번 받아도 같은 객체다.
4. **run 당 상한.** 일변동 62 개, 1 GB 중 먼저 닿는 것. 첫 run 은 기초 181 MB 와 약 41 일, 그 뒤는 날마다 하나다.
   작업 `juso_address_db`(매일 23:45 UTC, 첫 run 계산 600 s / 제한 3,600 s, `as_it_goes`, `retries: 0`, MemoryMax 1G).
5. **Silver 는 바뀐 키만 쌓는다.** `silver.juso_road_address` 는 도로명주소 관리번호마다 한 행: 도로명코드·도로명·
   시도/시군구 이름·지하 여부·건물 본번/부번과 대표지번(법정동 코드·산·본번·부번)으로 만든 PNU. 기초는 첫 적재 한 번
   (선언한 예외: 642만 행), 그 뒤는 일변동의 변경사유대로 바뀐 키와 삭제 표지만 쌓는다(ADR-0318).
6. **해석기는 하나, 단계는 셋.** `address_pnu.resolve` 의 순서: 지번 주소 → **juso 도로명** → 표제부 도로명.
   - juso 도로명: (시도, 시군구, 도로명, 건물 본번, 부번)이 가리키는 주소DB 행들의 대표지번 PNU 가 **하나일 때만** 놓는다.
     `pnu_basis = juso_road_address`. 둘 이상이면 놓지 않는다.
   - 시군구는 이름이 아니라 **코드로** 맞춘다. 주소의 시군구 이름을 법정동 코드표의 현재 이름, 그리고 기록된 코드
     쌍으로 이끈 폐지 이름(인천광역시 서구 → 서해구·검단구)으로 읽어 현재 시군구 코드의 집합으로 만든다. 주소DB 행의
     도로명코드 앞 다섯 자리가 그 집합에 있어야 한다. 시군구를 적지 않은 주소(세종)는 시도 안에서 하나일 때만 놓는다.
   - 쓰지 않는 것: 지번의 시군구 무시(정밀도 50%·18.5%), 건물명 맞추기(28.6%·0%).
   - 놓인 PNU 는 지번과 같은 검사를 지난다: 서빙 필지에 있거나, 그 코드의 앞선 형태가 하나만 있을 때(ADR-0482 §1).
7. **코드 변경을 따라간다.** 주소DB 의 코드는 제공처의 현재 코드다. 서빙판이 옛 코드를 쥔 동안은 앞선 형태로 찾고,
   서빙판이 바뀌면 같은 행이 다음 적재에서 새 형태에 놓인다. 원천 행은 ADR-0416 의 `views.changed_codes` 따라가기가
   같은 `resolve` 로 다시 놓는다. 적재 때 한 번 정하고 멈추지 않는다(사장 규칙 10-31).

## Consequences

- 사람 할 일 없음. 신청·키·창 없음.
- 비용(계산): 원천 0 원. R2 는 기초 181 MB + 하루 20–100 KB(연 약 0.2 GB 미만)라 월 1 센트 미만. Spark 기초 적재 한 번,
  그 뒤 하루 수천 행.
- 기대 효과(W65 계산, 오늘 행 기준): 경매 +1,702, 공매 +1,426 행. 실제 값은 배포 뒤 적재 요약의
  `resolved_by_grade`(`juso_road_address`)로 잰다.
- 가드: 수집기 시험(`juso_address_db_collect/tests.rs`)이 기초 한 번·날마다 한 번·커서는 커밋 뒤에만·상한에서 멈춤·
  zip 아닌 답 거부를 증명한다. 막는 사고: 실패한 날을 커서가 지나쳐 그날의 변경을 영원히 잃는 것, 매일 전체본을
  다시 받는 것.
- 건물DB·위치정보요약DB 는 받지 않는다. 같은 주소 키를 더 잘게 가질 뿐 PNU 연결을 더하지 않는다(W65 계산 ~0).
