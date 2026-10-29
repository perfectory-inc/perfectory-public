# ADR 0340: 온비드 API 의 모든 필드는 화면까지 가거나, 왜 아닌지 적는다 — 필드 인벤토리가 정본이다

- Status: Accepted
- Date: 2026-10-29
- Builds on: [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md)(공매가 필지 원천),
  [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md)(공매 종류),
  [ADR-0296](./0296-public-auction-serves-full-info-and-shares-one-content-addressed-media-store.md)(전체 정보·공용 미디어 저장소),
  [ADR-0298](./0298-content-addressed-media-is-a-write-once-store-not-a-per-object-bronze-ledger.md)(내용주소 미디어),
  [ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)(Silver 는 바뀐 키만),
  [ADR-0320](./0320-a-document-schema-bump-bakes-only-the-documents-it-changes.md)(선택 필드만 더하는 판),
  [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)(스키마 레지스트리)
- Amends: ADR-0296 §4 — 용도 이름은 별도 코드표(15157768) 수집이 아니라 응답에 이미 있는 이름(`cltrUsg*CtgrNm`)에서 온다

## Context

2026-10-29 공급자 스펙(data.go.kr 각 데이터셋 페이지의 swaggerJson, 활용가이드 01·04·07·11)과 우리 코드를 필드 하나씩 맞대 보았다(인벤토리 `data-go-kr-onbid.fields.json` 이 그 결과다).

| API | 필드 | 수집(Bronze) | 저장(Silver 열) | 서빙(문서) | 화면 |
|---|---:|---:|---:|---:|---:|
| 물건목록 getRlstCltrList2 (15157207) | 61 | 61 | 35 | 23 | 9 |
| 공고목록 getPbancList2 (15157216) | 20 | 20 | 0 | 0 | 0 |
| 공고 입찰결과목록 getPbancBidRsltList2 (15157222) | 18 | 18 | 0 | 0 | 0 |
| 물건 입찰결과상세 getCltrBidRsltDtl2 (15157254) | 51 | 51 | 19 | 0 | 0 |
| 물건상세 getRlstDtlInf2 (15157247) | 149 | 149 | 5 | 3 | 3 |
| 물건상세 입찰정보 getCltrBidInf2 (15157251) | 85 | 0 | 0 | 0 | 0 |
| 공고상세 getPbancDtlInf2 (15157218) | 30 | 0 | 0 | 0 | 0 |
| 물건 입찰결과목록 getCltrBidRsltList2 (15157252) | 39 | 0 | 0 | 0 | 0 |
| 공고상세 물건정보 getPbancCltrInf2 (15157220) | 31 | 0 | 0 | 0 | 0 |
| 공고상세 입찰정보 getPbancBidInf2 (15157256) | 26 | 0 | 0 | 0 | 0 |
| 공고 입찰결과상세 getPbancBidRsltDtl2 (15157258) | 54 | 0 | 0 | 0 | 0 |
| 코드 및 주소 getOnbidUsgCodeInfo·getOnbidDtlAddrInfo (15157768) | 8 | 0 | 0 | 0 | 0 |
| 합계 | 572 | 299 | 59 (+같은 값 213) | 26 | 12 |

(응답 봉투—결과코드·페이지—는 뺐다. "같은 값"은 다른 API 가 같은 키로 이미 싣는 필드다. 화면 12 는 9 개의 목록 값과 사진·감정평가서 링크·평가기관. 동산·자동차·유가증권 API 는 범위 밖이다.)

부동산 공매 API 13 개(15157768 은 두 operation) 중 새로 받을 것과 받지 않을 것:

| API | 다른 API 와 겹침 | 판단 |
|---|---|---|
| 15157251 물건상세 입찰정보 | 입찰방법 플래그 일부가 물건목록에 | **받는다**: 입찰보증금·잔대금 납부·입찰성립기준·제한경쟁 자격·제출서류·평가방식·이전 회차 결과·회차별 일정·개찰장소·매각결정일시는 여기에만 있다 |
| 15157218 공고상세 | 공고명·기관·방식 코드가 공고목록에 | **받는다**: 공고문 전문·첨부파일·참가자격·참가수수료·취소사유·관련공고·입찰금액 공개여부는 여기에만 있다 |
| 15157252 물건 입찰결과목록 | 부동산 필드 33/39 가 15157254·물건목록과 같다 | 받지 않는다(나머지 6 은 자동차) |
| 15157220 공고상세 물건정보 | 30/31 | 받지 않는다(물건주소 `cltrAdr` 는 물건상세 `zadrNm`·`cltrRadr` 가 싣는다) |
| 15157256 공고상세 입찰정보 | 26/26 | 받지 않는다(15157251 의 부분집합) |
| 15157258 공고 입찰결과상세 | 50/54 | 받지 않는다(나머지 4 는 자동차) |
| 15157768 용도 코드 | — | 지금은 받지 않는다: 모든 물건 응답이 용도 코드와 이름을 같이 준다. Silver 적재가 이름 없는 코드를 세고, 생기면 받는다 |
| 15157768 상세주소 | — | 받지 않는다: 검색용 주소 목록 |

세 가지가 보인다.

1. **받아 놓고 버린다.** 낙찰가·유효/무효 입찰자 수는 10-19 부터 Silver 에 쌓이지만 서빙되지 않는다. 물건상세 149 필드 중 Silver 에 닿는 것은 사진 해시와 평가기관 셋뿐이다: 부대조건·유의사항·공매재산명세서·임대차·점유·배분요구·감정평가 금액과 평가일이 Bronze 원문에만 있다. 공고 두 목록은 Silver 표가 없다.
2. **이미 있는 것을 따로 받으려 했다.** ADR-0296 §4 는 용도 이름을 위해 15157768 코드표 수집을 계획했지만, 모든 응답이 이름(`cltrUsgLclsCtgrNm` 등)을 같이 준다. 다른 코드도 마찬가지로 이름(`*Nm`)이 붙어 온다(처분방식·입찰방식·재산유형·입찰결과).
3. **아무도 몰랐다.** 어느 공급자 필드가 어디까지 가는지 적힌 곳이 없어, 위 둘은 실물 대조 전까지 보이지 않았다.

온비드 누리집 물건상세 화면과의 대조(세션 없이 상세 화면은 오류 페이지라 공개 가이드·안내 기준):

| 누리집 항목 | API |
|---|---|
| 공매재산명세서 | 물건상세 `papsInf[]`(처분청·배분요구종기·공매보증금·매각예정가격·말소제외권리·법정지상권·유의사항·조사일자 등) |
| 점유관계 / 임대차 | 물건상세 `ocpyRelList[]` / `leasInfList[]` |
| 배분요구종기 | 물건목록 `dtbtRqrEdtmCont`, `papsInf[].dtbtRqrEdtmYmd` |
| 부대조건 / 유의사항 | 물건상세 `icdlCdtnCont` / `pytnMtrsCont`, `papsInf[].pytnMtrsCont`, 입찰정보 `pytnMtrsCont`(평가방식) |
| 명도책임 | 물건목록·상세 `evcRsbyTrgtCont`(낙찰 후 인도/인수 책임) |
| 입찰이력(회차별) | 입찰정보 `prcnBidClgList[]`, 물건 입찰결과상세 |
| 공고문 / 공고 첨부 | 공고상세 `anncmAlCont`(전문) / `atchFileList[]` |
| 등기 주요정보 / 지적도 | 물건상세 `rgstPrmrInfList[]` / `lrmUrlAdrList`(등기부 원본 파일은 API 에 없다) |
| **담당자 연락처** | **없음** — 공고목록의 담당부점명(`rspbSbrNm`)뿐 |
| **조회수·관심 수** | **없음** |

## Decision

1. **필드 인벤토리가 정본이다.** `platforms/foundation-platform/infra/lakehouse/contracts/data-go-kr-onbid.fields.json` 이 API 마다 공급자 스펙의 모든 필드를 담고, 필드마다 수집(`bronze`)·저장(`silver`: 표.열)·서빙(`gold`: 문서 경로)·화면(`web`)·누리집 표시(`onbid_site`)를 적는다. 화면에 안 가는 필드는 `reason` 으로 어디서 왜 멈추는지 적고, 다른 API 가 같은 키로 이미 싣는 필드는 `same_as` 로 그쪽을 가리킨다. 누리집에만 있고 API 가 없는 항목은 `web_only` 에 적는다.

2. **가드 `scripts/guard/onbid-field-coverage.sh` 가 인벤토리를 계약에 대어 본다**(막는 사고: 위 Context 1·2 — 6 일 동안 서빙되지 않은 입찰결과, 응답에 이미 있는 이름을 위한 수집 계획). 실패 조건:
   - 화면에 안 가는데 `reason` 이 없다;
   - 사슬이 끊긴다: 화면엔 서빙이, 서빙엔 저장이, 저장엔 수집이 필요하다;
   - `silver` 가 lakehouse 계약에 없는 열, `gold` 가 가장 새 공매 문서 기록에 없는 경로, `web` 이 웹 문서 스키마와 패널이 둘 다 읽지 않는 잎을 가리킨다;
   - 거꾸로, 온비드 Silver 표의 내용 열이나 서빙 회차의 경로를 어느 필드도 주장하지 않고 `derived` 에도 없다(새 열이 인벤토리를 앞지르지 못한다);
   - `pii` 필드가 Silver 열을 주장한다.

   공급자 스펙이 바뀌면 `onbid-field-coverage.py refresh --check` 가 살아 있는 swaggerJson 과 대조한다(네트워크, CI 밖). 적재 때에도 Silver 적재는 인벤토리에 없는 응답 키를 실행 요약에 센다(2단계).

3. **수집을 닫는다(2단계).** 15157251(물건상세 입찰정보)과 15157218(공고상세)을 부른다. 입찰정보는 물건마다 한 번(ADR-0339 의 물건 단위와 같다: 회차별 일정 `cseqBidInfClgList` 와 이전 회차 결과 `prcnBidClgList` 가 한 응답에 다 있다), 공고상세는 공고관리번호마다 한 번, 공고목록에서 그 공고의 행이 바뀌면 다시. 둘 다 API 마다 자기 일일 상한·run 당 상한을 계약에 두고(쿼터는 API 별), 실패는 물건상세처럼 격리한다. 데이터 이용은 data.go.kr 활용신청이 API 마다 필요하다(운영자 1회).

4. **미디어는 미러하고, 링크하지 않는다.** 위치도(`lmapUrlAdrList`)·지적도(`lrmUrlAdrList`)·360도 사진(`poto360DgrUrlList`)·공고 첨부(`atchFileList[].urlAdr`)는 사진과 같은 KAMCO `commonImageView.do`/`dnldFile.do` 해시 URL 이다(가이드 04 샘플). ADR-0296 이 사진 주소만 서빙하는 길을 만료 때문에 기각했으므로 같은 이유로 링크하지 않고, ADR-0298 의 공용 내용주소 저장소에 같은 호스트 허용·바이트 판정·크기 상한으로 받는다. 동영상(`vdoUrlAdrList`)도 같은 길을 타되, 바이트 판정이 이미지·PDF 만 알므로 판정되지 않는 동영상은 저장하지 않고 서빙하지 않는다(실측으로 형식을 확인한 뒤 판정 목록을 넓힌다). 새 종류는 `photos` 가 아니라 새 선택 필드로 서빙한다: `photos[].media_kind` 를 닫힌 열거로 읽는 배포된 웹이 있어서다(#939 가 막는 사고와 같은 꼴).

5. **사인의 성명은 Bronze 밖으로 옮기지 않는다.** 임차인(`leasInfList[].cltrInprNm`)·권리자(`rgstPrmrInfList[].cltrInprNm`, `dtbtRqrMtrsList[].acpmPrptBondDclFlnm`)·점유자(`ocpyRelList[].acpmPrptOcpyIrpsFlnm`) 성명은 인벤토리에 `pii` 로 적고 Silver 가 버린다. 원문 Bronze 는 계보로 남는다. 구매자에게 필요한 것은 권리의 종류·날짜·금액이지 개인 이름이 아니다(개인정보 보호법의 최소처리, GDPR Art.5(1)(c) data minimisation). 감정평가사명(`apslApprNm`)은 공적 감정서의 서명자라 그대로 둔다.

6. **Silver(2단계).** 수집하는 모든 필드는 Silver 열이 된다. 필드→열 대응은 인벤토리의 `silver` 가 정본이고 적재가 그것을 읽는다(열 형은 lakehouse 계약에서). 중첩 목록은 공급자 키 그대로의 JSON 문자열 열(`*_json`)로 담는다. 새 표: 공고·공고 입찰결과·공고상세·공고 첨부·물건상세·물건 입찰정보. 모두 append-only, 바뀐 키만(ADR-0311). 기존 두 표에 열을 더하면 다음 전수 비교에서 열린 회차가 한 번 다시 쌓인다(선언한 예외, 비용은 그 PR 이 계산한다).

7. **서빙(3단계)은 선택 필드만 더하는 판이다(ADR-0320).** 회차에 이름(`*_nm`)·입찰 조건·공고·상세(명세서·임대차·점유·배분요구·정정·면적)·입찰정보·이전 회차 결과·감정평가 목록·새 미디어를 더한다. 판 번호는 ADR-0336 레지스트리가 병합돼 있으면 레지스트리 판으로, 아니면 `contracts/by-pnu-documents` 기록으로 올리고, 늦게 병합되는 쪽이 옮긴다.

8. **화면(4단계)은 공급자의 말을 쓴다.** 코드는 응답이 같이 주는 이름(`*Nm`)으로 보이고, 우리 말로 옮기지 않는다. 화면 항목 이름은 공급자 항목명(국문)을 쓴다.

9. **누리집 전용 항목은 받지 않는다.** 담당자 연락처·조회수·관심 수는 공개 API 가 없다. 누리집을 긁지 않는다(온비드 이용약관). 공개 API 가 생기면 인벤토리 `web_only` 에서 옮긴다.

## Consequences

- 새 필드가 Silver·서빙·화면 어디서 멈추든 가드가 이유를 요구한다. 공급자가 필드를 더하면 refresh 가 잡고, 적재 요약이 실물에서 센다.
- 2~4 단계는 각자 PR 이고, 각 PR 이 인벤토리의 주장을 그만큼 옮긴다.
- 공고상세·입찰정보 첫 수집은 열린 물건 수(~8,700)·공고 수만큼 호출이 늘고, 새 미디어는 R2 용량이 는다. 2단계 PR 이 계산하고 첫 수집에서 잰다.
- 운영자 일: data.go.kr 에서 15157251·15157218 활용신청.

## 참고한 사례

- Airbyte 스키마 변경 감지 — 원천에 새 열이 생기면 감지하고 전파/무시/정지를 정한다. <https://docs.airbyte.com/using-airbyte/schema-change-management>
- dbt model contracts·sources — 모델이 내놓는 열을 선언하고 다르면 빌드가 실패한다. <https://docs.getdbt.com/docs/collaborate/govern/model-contracts>
- OpenLineage column-level lineage — 열마다 어느 입력 열에서 왔는지 기록한다. <https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet>
- GDPR Art.5(1)(c) data minimisation, 개인정보 보호법 제3조(최소처리).
