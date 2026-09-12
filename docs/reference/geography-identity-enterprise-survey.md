---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-09-12
---

# 대기업 사례 전수조사 — 지리 정체성·코드 변경·필지 계보·파이프라인 SSOT

ADR-0103(장소의 정체성은 행정코드 변경보다 오래 산다)의 근거 자료. 우리 4대 문제 각각에 대해
실제 대기업/공공기관/표준/오픈소스 사례를 웹으로 조사(2026-09-12)하고, "무엇을 베낄지"를 적었다.
모든 주장은 아래 URL로 검증. (일부 PDF·차단 호스트는 인접 페이지로 교차확인 — 문서 끝 caveat 참조.)

## 우리 4대 문제 (정합성 감사 결과 요약)

1. **행정코드 변경이 정체성을 깬다** — 지도=29/46, HUB 건물=12(전남광주통합특별시). 심지어 HUB
   안에서도 표제부=29/46 vs 전유부·가격=12. `parcel_id = hash(pnu)`라 코드가 바뀌면 ID가 갈림.
2. **필지 자체가 분필/합필된다** — 땅이 쪼개지고 합쳐지면 옛 필지는 폐쇄, 새 필지 생성. PNU가 바뀜.
3. **파이프라인 SSOT가 낡음** — 그래프가 이름만 검사(coverage)하고 연결성(생산자/소비자)을 안 봄.
   canonical 표 2개 생산자 없음, gold 2개의 실제 by-PNU 서빙 소비자가 그래프에 빠짐.
4. **정체성이 가변값에서 파생** — 위 셋의 공통 뿌리. 산단 ID도 두 벌(v5/v7).

---

## 문제 1 — 행정코드 변경(통합·분리·개칭)을 어떻게 다루나

**가장 가까운 완결 청사진 — 영국 ONS GSS + CHD/RGC ★**
2011년 ONS는 계층적 "ONS 코드"를 **9자리 불투명 GSS 코드**로 교체(정확히 우리 이유). 규칙:
**실질 경계 변경 ⇒ 새 코드, 옛 코드는 폐기·재사용 금지, 순수 개칭은 코드 불변.** 이력은
**Code History Database(CHD)** = 살아있는 등록부(RGC) + 추가전용 이력(operative/terminated 날짜 +
predecessor/successor). 우리 "append-only + 현행 등록부" 지시와 정확히 일치.
- https://www.ons.gov.uk/methodology/geography/geographicalproducts/namescodesandlookups/codehistorydatabasechd
- https://en.wikipedia.org/wiki/GSS_coding_system
- 교훈(Democracy Club): 불투명 코드도 실질 경계 변경엔 새 코드가 나오므로 다운스트림은 **CHD로
  계보를 따라가야** 한다 — 불투명 ID는 *사고성* 파손을 막고, 크로스워크가 *실제* 통합을 나른다.
  https://democracyclub.org.uk/blog/2018/06/29/why-we-cant-rely-on-gss-codes-and-what-to-do-about-it/

**가장 가까운 오픈 데이터 모델 — Who's on First ★**
`wof:id`(불투명·영구·재사용 안 함). 통합/분리/개칭 = **새 레코드 + 양방향 포인터**
(`superseded_by`/`supersedes`) + `mz:is_current=0` + EDTF 유효기간, **삭제 안 함**. 부모도 안정
ID로 참조하므로 부모 통합이 자식을 고아로 안 만든다. CLI(`wof-deprecate-and-supersede`)까지.
- https://whosonfirst.org/docs/properties/wof/ · 데이터: https://github.com/whosonfirst-data/whosonfirst-data

**실패모드와 해법을 동시에 보여주는 — 미국 Census**
FIPS/GEOID(가변·계층·기하파생, 우리 법정동/PNU와 같은 함정) + **GNIS Feature ID(불투명·무의미)**.
"카운티 변경 로그"(옛코드→새코드+발효일, 예: Shannon 46113→Oglala Lakota 46102, 2015-05-01) +
**관계 파일**(분할=1:N 행, 합병=N:1 행). 교훈: 불투명 ID를 *주 키*로 삼아라(미국은 FIPS를 주 키로
남겨 지금도 변경 로그를 영원히 유지해야 함).
- https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html

**우리 아키텍처를 독립적으로 구현한 — Wikidata**
불투명 QID + 행정코드는 **속성**(P14697 한국 행정구역 분류 코드) + `replaces`/`replaced by`/
`separated from`(분할) + 유효기간(P580/P582). **실제 연기군(Q195890)→세종(Q20929) 통합이
이미 모델링**됨. 단 커뮤니티 관리라 정본이 아닌 상호운용 계층으로.
- https://www.wikidata.org/wiki/Property:P1366 · https://www.wikidata.org/wiki/Q20929

**크로스워크 파일 스키마의 표본 — Eurostat NUTS**: 버전쌍 대응표(분할 1:N, 합병 N:1) + 유효기간.
단 불투명 ID가 없어 정체성은 반면교사. https://ec.europa.eu/eurostat/web/nuts/correspondence-tables

**운영 패턴(권위 변경 피드 구독) — GeoNames**: 매일 `deletes/modifications` 피드. 우리 MOIS
행정표준코드 변경 수집의 운영 형태. https://download.geonames.org/export/dump/

**반면교사**: Google Place ID(바뀜, 크로스워크 없음, 재조회-후-희망) · OSM/Nominatim(재임포트 시
ID 재생성 = 우리 원래 버그 그대로). 재생성/기하파생 ID를 영속 키로 쓰지 말 것.

**우리가 베낄 것**: 불투명 코드 + 재사용 금지 + 개칭≠재코딩(ONS) · 양방향 supersedes + 유효기간 +
삭제금지(WOF) · 살아있는 등록부 + 추가전용 이력 2분할(ONS RGC/CHD) · 폐기코드 조회 시
현행으로 리다이렉트(Pelias/Foursquare) · 코드는 정체성이 아니라 속성(Overture Bridge, Wikidata).
**정본 값·발효일은 한국 MOIS 행정표준코드(getStanReginCdList)에서.**

---

## 문제 2 — 필지 분필/합필(땅 자체가 바뀜)

**표준 골격 — ISO 19152 LADM `VersionedObject` ★**
대리 OID + **양시간**(`beginLifespanVersion`/`endLifespanVersion` + 실세계 유효시간) + 폐쇄보존.
분할/합병 = 옛 SpatialUnit에 `endLifespanVersion` 찍어 이력으로, 새 것 생성. 오픈 프로파일
구현체(Asistente-LADM-COL)까지 존재.
- https://www.fig.net/resources/proceedings/fig_proceedings/fig2012/papers/ts04c/TS04C_vanoosterom_lemmen_et_al_6057.pdf
- https://github.com/SwissTierrasColombia/Asistente-LADM-COL

**가장 명료한 법적 계보 모델 — 뉴질랜드 LINZ ★**
필지 상태 `Current → Historic`(분할/합병 시), **"폐기되는 필지는 새 필지로 빈틈없이 전부 대체"**
규칙 → 계보 커버리지 100% 보장. 우리 분필/합필 의미와 거의 1:1.
- https://www.linz.govt.nz/guidance/survey/cadastral-survey-guidelines/parcels

**생산 국가 시스템의 predecessor 링크 — 네덜란드 Kadaster(BRK)**: LADM 기반, **객체가 이전 객체를
self-association**으로 가리킴. https://www.sciencedirect.com/science/article/pii/S0264837722001016

**제품화된 정답 — Esri ArcGIS Parcel Fabric**: 기록(record)-주도, 모든 법적 이벤트가 새 필지
생성 + 대체 필지를 **historic로(삭제 아님)**, 부모/자식(predecessor/successor) 관계 유지 →
"이 필지 언제 분할됐나" 즉답. https://www.esri.com/about/newsroom/arcnews/researching-parcel-history-in-seconds

**영구 대리키(부동산 데이터 기업)**: CoreLogic **CLIP** — "APN 변경·분할/합병·경계 이동을 관통해
영속"(우리 내부 안정 ID 전략의 정확한 상용판). ATTOM ID. Regrid **`ll_uuid`+`ll_stable_id`**
(분할/합병 시 새 UUID, 매치 타입+기하겹침으로 변화 탐지 — 필드 수준으로 가장 베끼기 쉬움).
- https://www.cotality.com/products/clip · https://support.regrid.com/parcel-data/regrid-id

**우리가 이미 가진 정본 소스 — 한국 MOLIT 변동연혁 ★**: `필지고유번호변동연혁`(data.go.kr 15122617)
= 옛→새 PNU 연결을 정부가 이미 공개. 분필/합필 계보를 추론하지 말고 받아쓸 후보. 그리고
우리 저장소엔 **토지이동이력(1.21억 행, ADR-0089/0090)** 이 이미 서빙 중 — 분할/합병 이벤트 포함.
- https://www.data.go.kr/data/15122617/fileData.do

**베낄 패턴(3요소 결합)**: 내부 안정 대리키(PNU는 속성으로 강등) + 양시간 버전 행(상태 enum
`current/historic`, 삭제금지=폐쇄대장) + **명시적 계보 DAG 표**`(predecessor, successor, event∈
{split,merge}, date, source)` — MOLIT 변동연혁/토지이동이력으로 시드, 없으면 기하겹침으로 보완.

---

## 문제 3 — "안정 ID + 크로스워크"가 정말 표준인가 (정체성 설계)

**우리 설계 = 대기업 표준의 교집합**임이 확인됨. 다섯 기둥 전부 정립된 표준에 대응.

**MDM 골든레코드 + 크로스워크(상용 표준)**: Reltio는 용어가 아예 **"crosswalk"**(createDate/
updateDate/**deleteDate**=유효기간) + 엔티티 URI + 읽기시점 survivorship = 우리 설계 그대로.
Informatica **XREF→ROWID_OBJECT**, SAP MDG **Key/Value Mapping**, Semarchy **Golden ID + MD 표**,
Stibo Match&Link. https://docs.reltio.com/en/objectives/model-data/data-modeling-at-a-glance/data-modeling-operation/define-crosswalks-for-data-sources/crosswalks

**이름 붙은 설계 패턴 — Kimball ★**: **durable "supernatural" key**("절대 안 변함" = 우리 안정 ID) +
**SCD Type 2**(effective/expiration + current flag = 유효기간) + **Type 7**(이중 FK: 안정키→현행,
대리키→이력 = "안정 ID로 조인, 현행 코드로 표시"). 오픈 구현: **dbt snapshots**(valid_from/valid_to).
- https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/natural-durable-supernatural-key/
- https://www.kimballgroup.com/2013/02/design-tip-152-slowly-changing-dimension-types-0-4-5-6-7/

**양시간 DB 기본기(SQL:2011)**: application-time 기간 표 + `WITHOUT OVERLAPS`. **PostgreSQL 18**
네이티브 지원(우리 스택에 직접 채택 가능), **XTDB**(양시간 기본), MariaDB/Oracle Temporal Validity/
Db2 BUSINESS_TIME. 변경 감지 feeder = **Debezium(CDC)**.
- https://www.postgresql.org/about/news/postgresql-18-released-3142/ · https://github.com/xtdb/xtdb

**현실의 결정적 증거 — 금융 FIGI/LEI ★**: **FIGI**(불변·재사용금지·폐기보존, 티커/CUSIP은 바뀌어도
FIGI 불변) + **GLEIF LEI**(영구 ID + 가변 속성 + **날짜 붙은 크로스워크 파일**을 공개 인프라로 발행:
ISIN/BIC/MIC→LEI). 우리 패턴이 전 지구적 공공 인프라로 이미 배포된 것.
- https://www.openfigi.com/about/overview · https://www.gleif.org/en/lei-data/lei-mapping

**오픈소스로 지을 수 있는 동형 — 의료 EMPI**: 각 병원 MRN 보존 + 엔터프라이즈 ID(golden) + 크로스
레퍼런스 + 결정/확률 매칭 + **스튜어드 검수 큐**(match-grade `possible`=사람 검토). IHE PIX/PDQ,
HL7 FHIR(`Patient.identifier`, `$match`, `Linkage type=historical`). 오픈: **JeMPI**, OpenCR, SanteMPI.
- https://github.com/jembi/JeMPI · https://profiles.ihe.net/ITI/TF/Volume1/ch-5.html

**베낄 것**: "durable/golden 안정 키 + source 코드로의 양시간 crosswalk" — MDM이 골든/xref 프레이밍,
Kimball이 durable-key/SCD 어휘, SQL:2011/PG18/XTDB가 시간 메커니즘, FIGI/LEI·EMPI가 실증. 스튜어드
검수는 EMPI match-grade 버킷을 그대로.

---

## 문제 4 — 파이프라인 SSOT & 생산자/소비자 연결성 강제

**우리 가드의 정답 버전 — dbt_project_evaluator ★**: 그래프를 표로 만들고 **CI 실패 규칙**으로
`Root Models`(부모 0 = **생산자 없음**), `Unused Sources`(자식 0 = **소비자 없음**), fanout,
문서/테스트 coverage까지 검사. 우리가 "이름만 검사"하던 것을 "연결성까지" 올리는 정확한 본보기.
- https://github.com/dbt-labs/dbt-project-evaluator
- 유사: dbt-checkpoint `check-model-parents-and-childs`(min/max 부모/자식 강제).

**그래프=현실을 구조적으로 보장**: **Dagster**(코드가 자산 그래프의 정본이라 낡을 수 없음) + asset
checks + FreshnessPolicy. **dbt exposures**로 서빙/gold 소비자를 1급 노드화(우리 빠진 by-PNU 소비자가
정확히 이걸로 해결). https://docs.dagster.io/guides/build/assets · https://docs.getdbt.com/docs/build/exposures

**런타임에서 계보 도출(대기업 공통)**: **OpenLineage**(LF AI, MS Purview 등 채택) + **Marquez**,
**DataHub**(LinkedIn, stateful ingestion으로 사라진 엔티티 감지 + freshness assertion), Apache Atlas.
Netflix/Uber/Airbnb/LinkedIn은 그래프를 손으로 관리하지 않고 **실행에서 도출**해 현실과 일치시킴.
- https://github.com/OpenLineage/OpenLineage · https://github.com/datahub-project/datahub

**베낄 가드 개선**: ① 연결성 단언(비-bronze는 생산자≥1, 비-serving은 소비자≥1) ② 서빙/gold 소비자를
exposure로 1급 노드화 ③ 실행에서 OpenLineage로 엣지 도출해 SSOT와 대사 ④ 그래프↔실물 양방향 대사 +
fail-safe 임계 ⑤ 데이터셋별 freshness 정책 ⑥ 도달성(모든 gold는 source에서 도달·serving까지 도달)
⑦ **위반 심어 거부 확인**(우리 "prove the check can fail" 규칙 그대로).

---

## 채택할 통합 패턴 (전 사례의 합집합)

1. **불투명 안정 ID를 주 키로** — 장소·필지에 UUID(또는 GNIS/GSS식 타입-프리픽스) 부여. PNU·법정동
   코드·산단코드는 전부 **속성**으로 강등. (GERS UUID·WOF·GNIS·GSS·CLIP·LEI·FIGI·Kimball durable key)
2. **양시간 크로스워크**(안정 ID ↔ 외부 코드, valid_from/valid_to, 삭제금지) — 살아있는 등록부 +
   추가전용 이력 2분할(ONS RGC/CHD), PG18 `WITHOUT OVERLAPS`로 구현, 정본은 MOIS 행정표준코드.
3. **필지 계보 DAG**(predecessor/successor + event + date) — MOLIT 변동연혁/토지이동이력으로 시드,
   상태 enum current/historic, 폐쇄보존(LADM/LINZ/Parcel Fabric).
4. **스튜어드 검수 큐** — 자동 해석 불가/모호를 격리, EMPI match-grade식 버킷, 결정은 데이터로 저장.
5. **연결성·freshness 강제 가드 + 런타임 계보 대사** — dbt_project_evaluator식 규칙 + OpenLineage 대사.
6. **개칭≠재코딩, 코드 재사용 금지, 폐기 코드 조회는 현행으로 리다이렉트**(ONS + Pelias/Foursquare).

이 여섯이 ADR-0103의 6기둥과 정렬된다. 즉 우리 방향은 지어낸 게 아니라 **ONS·WOF·Census·Wikidata
(지리) + LADM·LINZ·Esri·CLIP·MOLIT(필지) + MDM·Kimball·FIGI·LEI·EMPI(정체성) + dbt·OpenLineage·
Dagster(파이프라인)** 이 이미 실물로 운영하는 것의 합집합이다.

## 핵심 레포·URL 색인
지리: ONS CHD, [WOF](https://github.com/whosonfirst-data/whosonfirst-data), Census county-changes, Wikidata, NUTS, GeoNames, [Overture](https://github.com/OvertureMaps/data) ·
필지: [LADM-COL](https://github.com/SwissTierrasColombia/Asistente-LADM-COL), LINZ, Kadaster, Esri Parcel Fabric, Regrid, MOLIT 변동연혁 ·
정체성: Reltio/Informatica, Kimball, [XTDB](https://github.com/xtdb/xtdb), PG18, [OpenFIGI](https://github.com/OpenFIGI/api-examples), GLEIF, [JeMPI](https://github.com/jembi/JeMPI) ·
파이프라인: [dbt_project_evaluator](https://github.com/dbt-labs/dbt-project-evaluator), [OpenLineage](https://github.com/OpenLineage/OpenLineage), [DataHub](https://github.com/datahub-project/datahub), [Dagster](https://github.com/dagster-io/dagster)

## Sourcing caveats
- Netflix TechBlog·Airbnb Medium·일부 벤더 문서(Informatica/TIBCO/SAP)·OMG FIGI/Kimball Design Tip
  PDF는 자동 fetch가 막혀 검색 인덱스 발췌 + 인접 페이지로 교차확인함. ADR에 직접 인용 전 브라우저
  재확인 권장.
- GeoNames "ID 재사용 안 함/삭제 comment에 후속 id"는 포럼 요약 기반(일일 변경 피드·불투명ID분리는 1차확인).
- Overture GERS는 경계 변경 시 division ID 유지 여부를 문서가 명시 안 함(미확정).
- Wikidata는 커뮤니티 관리 — dong 단위 커버리지·특정 코드 값이 낡을 수 있어 정본 아닌 상호운용 계층으로.
