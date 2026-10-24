# ADR 0296: 공매도 물건상세를 받아 사진·입찰결과·용도까지 서빙하고, 미디어는 경매와 한 content-addressed 저장소를 공유한다

- Status: Accepted
- Date: 2026-10-25
- Builds on: [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md)(공매·경매·공장이 필지 원천), [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md)(공매는 자기 Silver 로 만드는 필지 종류), [ADR-0290 §7](./0290-court-auction-details-are-refetched-by-change-and-sale-date-and-photos-are-content-addressed.md)(사진은 내용 해시로 한 번만 저장), [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md)(행 버전), [ADR-0292](./0292-public-sale-servable-gold-carries-only-host-panel-pnus.md)(서빙 gold 범위), [FP-ADR-0016](../../platforms/foundation-platform/docs/adr/0016-bronze-commit-protocol.md)(Bronze 쓰기 관문)
- Amends: ADR-0287 — 공매 서빙 문서(v1)에 사진·입찰결과·용도 이름이 없던 범위를 넓힌다

## Context

2026-10-25 실물 대조로 확인한, 지금 공매(온비드)의 상태:

1. **지금 공매 수집은 온비드 OpenAPI operation 4종뿐이다.** `public-source-endpoint-catalog.v1.json` 기준: 물건목록(`getRlstCltrList2`, 15157207)·공고목록(`getPbancList2`)·공고입찰결과목록(`getPbancBidRsltList2`)·물건입찰결과상세(`getCltrBidRsltDtl2`, 15157254). **물건상세(`getRlstDtlInf2`, 15157247)는 호출하지 않는다** — 그래서 사진·감정평가서 URL 이 Bronze·Silver 에 애초에 없다.
2. **서빙 by-PNU 문서(v1)는 회차마다 28칸이고 전부 표(숫자·텍스트)다.** 실서빙 실측: 사진·이미지·링크·설명 칸이 0. 사람이 보는 값(물건명·감정가·입찰가·입찰기간·면적) 외에 코드·온비드 번호가 섞여 있다.
3. **입찰결과(낙찰가·유효/무효 입찰자 수·개찰일시)는 이미 받아 Silver 에 있으나 서빙하지 않는다.** `getCltrBidRsltDtl2` → `silver.onbid_item_bid_result`(계약 `status: draft`, `declaredStatus: contract_only`). 읽는 by-PNU 소비 레인이 없어 섬으로 둔 상태(ADR-0287 §7: v1 은 열린 회차만).
4. **용도 대/중/소 이름은 코드만 있고 null 이다.** `onbid_public_auction_silver_to_gold.py` 는 `reference.onbid_usage_code` 가 채워졌을 때만 이름을 붙이고, 아니면 null 을 쓴다(지어내지 않음). 그 코드표는 data.go.kr **15157768**(차세대 온비드 코드·주소 조회서비스의 「온비드 용도 코드」)에서 와야 한다(ADR-0287 §4·Consequences). 아직 미수집.
5. **온비드 물건상세(`getRlstDtlInf2`) 응답에 사진·감정평가서 주소가 있다.** 응답의 사진 URL 목록(`potoUrlList`)·썸네일(`thnlImgUrlAdr`)·감정평가서 첨부(`apslEvlClgList`)에 KAMCO 파일 URL(`dnldFile.do`)이 담긴다. **실제 사진 보유율·평균 용량은 perfectory 첫 물건상세 수집에서 측정한다**(지금은 수집 자체를 안 해 수치가 없다 — 경매 ADR-0290 이 사진 99.5%·1.5~5MB 를 첫 수집에서 쟀던 것과 같은 방식).
6. **사진 바이너리는 API 가 아니라 KAMCO 공개 파일 URL 에서 받는다.** API 는 주소만 준다. 그 URL 은 해시(`hashCrpsNo`)가 붙어 **만료**될 수 있다 — 받자마자 저장하지 않으면 사라진다.
7. **perfectory 는 이미 "바이너리를 내용 해시로 R2 에 한 번만 담는" 기관을 갖고 있다.** 경매 사진이 쓰기로 한 [ADR-0290 §7](./0290-court-auction-details-are-refetched-by-change-and-sale-date-and-photos-are-content-addressed.md) — 접두어 아래 `sha256/<해시>.<확장자>`, FP-ADR-0016 관문, 중복제거, Bronze 는 `sha256:<해시>` 참조만 보관 — 이 그것이다(경매 쪽 구현은 아직 빌드 순서 ⑤로 미완). 브이월드 zip·건축물대장 zip 도 같은 `ObjectStorageService`/`bronze_committer` 로 R2 에 스트리밍된다.
8. **대기업·오픈소스 정석과 정합.** 미디어를 내용 해시로 저장·중복제거하는 것은 OCI/Docker 레이어·Nix store·Git·Perkeep(OSS)의 공통 방식이다. 외부 사진을 **다운로드→객체저장소+CDN**, 응답에서 원본 URL 을 우리 URL 로 치환하는 것도 확립된 패턴이다. 계약(ODCS, Linux Foundation Bitol)·medallion(Bronze/Silver/Gold)은 perfectory 가 이미 쓰는 표준이다.

즉 지금 공매는 "진행 중 회차의 핵심 숫자"라는 얇은 조각만 서빙하고, 사진은 아예 안 받으며, 받아 둔 입찰결과·코드표는 서빙에 닿지 않는다.

## Decision

1. **물건상세(`getRlstDtlInf2`, 15157247)를 받는다.** `public-source-endpoint-catalog.v1.json`(소스 SSOT)에 엔드포인트를 등록하고, 온비드 수집기가 **새로 생긴·바뀐 회차**에 대해 물건상세를 받는다(기존 온비드 증분 규칙 재사용, 전수는 첫 1회). 응답은 원문 그대로 Bronze 에 보존하되(ADR-0280 §5 raw lineage), 사진·첨부 URL 값은 §2 의 참조로 바꾼다.

2. **미디어 저장은 ADR-0290 §7 을 "경매·공매 공용 단일 메커니즘"으로 일반화한다(이 ADR 의 SSOT 핵심).** 사진·감정평가서는 소스별 저장소를 새로 만들지 않고, 계약의 미디어 접두어 아래 `sha256/<해시>.<확장자>` 로 한 번만 저장한다(FP-ADR-0016 관문, 내용 중복제거, 읽을 때 이름=체크섬 검증). **캡처 어댑터만 소스마다 다르다** — 경매는 base64(`picFile`) 디코드, 공매는 KAMCO 파일 URL 다운로드(만료 graceful: 못 받으면 미기록, 다음 수집 재시도). 저장소·키 규칙·형식 판정 목록은 한 곳(공용 미디어 계약)이다.

3. **Silver 에 공매 사진·감정평가서 표를 더한다.** 물건상세에서 (물건·순번·제목·해시·크기·형식·처음 본 시각)을 `silver.onbid_real_estate_item` 과 별도 사진 표에 ADR-0286 행 버전 규칙으로 쌓는다. 입찰결과 Silver(`silver.onbid_item_bid_result`)는 이미 있으므로 새로 받지 않는다.

4. **용도 코드 이름을 채운다.** 15157768(온비드 용도 코드)을 소량 수집해 `reference.onbid_usage_code` 를 채운다(ADR-0287 §4). data.go.kr 같은 키의 활용신청이 필요할 수 있다(운영자 1회). 이름이 없으면 지금처럼 null — 지어내지 않는다.

5. **공매 by-PNU 문서를 v2 로 올린다.** 회차 payload 에 **사진(해시→공개 URL)·입찰결과(낙찰가·입찰자 수·개찰일시)·용도 이름**을 더한다. 형식 변경이므로 ADR-0286 규칙에 따라 **전량 재굽기**하고, 사진 공개 URL 은 해시 기반 한 규칙으로 필지 엣지에서 서빙한다(새 호스트 없음). 열린 회차 외 범위(끝난 회차 포함 여부)는 v2 계약에서 정한다.

6. **웹 공매 패널에 사진 갤러리·감정평가서·입찰결과·용도를 더한다.** 패널은 서빙 문서만 읽는다(2026-10-24 추가된 공매 카테고리 확장).

7. **SSOT.** 소스=`public-source-endpoint-catalog.v1.json`, 표 스키마=ODCS 계약(Rust 도메인에서 생성), 미디어 접두어·형식 판정·캡처 어댑터 등급=공용 미디어 계약, 파이프라인 커버=`pipeline-graph.v1.json`(모든 새 표·서핑이 노드), 행 버전=ADR-0286, 수집 일정=작업 등록부. 한 사실은 한 곳.

8. **범위와 순서는 단계로 간다(규모가 크므로 지름길 없이).**
   - **A. 설계·계약**(비용 0, 가역): 이 ADR + 소스·표·미디어 계약 + pipeline-graph 갱신.
   - **B. 물건상세 수집 + 사진·첨부 R2 미러**(비용: 전수 수집 호출량·사진 다운로드·R2 용량 — 첫 수집에서 측정해 보고).
   - **C. Silver/Gold v2 + 서빙 v2 전량 재굽기**(운영자 cutover·창).
   - **D. 웹 패널 확장.**
   - 용도 코드표(§4)는 작은 별도 수집으로 병행.

## Consequences

- 첫 물건상세 전수 수집은 열린 회차 수만큼 호출이 늘고, 사진·PDF 다운로드·R2 저장은 **첫 수집에서 측정**(지어내지 않는다). 같은 사진은 다시 쌓이지 않는다(내용 해시).
- v2 전환은 전량 재굽기 + 공매 레인 운영자 cutover + 창 안에서.
- **경매 사진(ADR-0290 §7, 아직 미구현)도 이 공용 저장소를 그대로 쓴다** — 두 번째 사진 저장소를 만들지 않는다.
- 미서빙이던 입찰결과·용도 이름이 서빙에 닿는다 — "모든 정보"에 가까워진다.
- 사진 URL 만 서빙하고 파일을 안 받는 길은 KAMCO 해시 URL 만료로 깨지므로 택하지 않는다.

## Alternatives rejected

- **공매 전용 사진 저장소 신설** — SSOT 위반(미디어 저장이 두 곳). 기각, §2 로 공용화.
- **별도 코드베이스 모방** — perfectory 자체 ADR·계약으로만 설계한다.
- **사진 주소만 서빙(다운로드 안 함)** — 만료로 깨짐. 기각.

## Open (B 에서 측정·결정)

- 물건상세 응답의 실제 사진 보유율·평균 용량(perfectory 첫 수집에서 측정).
- 15157768 활용신청 필요 여부(운영자 1회).
- v2 가 끝난 회차·입찰결과를 어디까지 담을지(ADR-0287 §7 범위 재검토).
