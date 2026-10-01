# ADR 0117: 메타데이터는 데이터셋마다 계약 하나, 사람이 보는 곳은 데이터 카탈로그 하나다

- Status: Accepted
- Date: 2026-10-01
- Builds on: [ADR-0086](./0086-the-pipeline-graph-names-every-dataset-once.md)(데이터셋을 한 번씩 명명),
  [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md)(버전은 계약 한 곳),
  [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)·[ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)(더니어·직원 로그인)

## Context

2026-09-30~10-01 실측이다.

- **"카탈로그"가 다섯 가지를 가리킨다.** R2 Data Catalog(Iceberg REST, 표의 현재 메타데이터 위치),
  Foundation 의 서빙 API(`/catalog/v1/...`, 산업단지·필지·건물 기준정보), 원천 목록
  (`public-source-endpoint-catalog.v1.json`), 참고 문서 폴더(`platforms/foundation-platform/docs/catalog/`),
  그리고 시험 설치한 DataHub. 같은 낱말이 뜻을 가려 주지 못해 대화와 문서가 계속 어긋났다.
- **데이터 흐름을 보여 주는 자리가 다섯이다.** 정본 `pipeline-graph.v1.json`(선언), 그것을 옮긴
  `docs/data-pipeline-map.md`(생성 문서), Foundation `GET /catalog/v1/pipeline-graph`(선언 + 실시간 점검 덧씌움),
  더니어 "데이터 흐름" 메뉴(그 API 를 그림), DataHub 시험 설치(선언을 OpenLineage 로 넣음). 앞의 넷은 모두 같은
  선언 하나를 다시 보여 줄 뿐이고, **실제 실행 기록(언제·몇 행·성공 여부·칸 단위 흐름)은 어디에도 없다.**
- **쓰이지 않는 메타데이터 표가 있다.** 운영 `foundation` DB 의 `catalog.lakehouse_data_asset`,
  `lakehouse_dataset_version`, `lakehouse_lineage_edge`, `lakehouse_quality_check`, `lakehouse_batch_run`,
  `lakehouse_object_artifact` 는 2026-07-19 스키마부터 있었으나 **여섯 표 모두 0행**이다. 쓰는 코드가 없다.
- **화면 글자를 원본 대신 짐작했다.** 산업단지 조성 상태 `operating` 을 "운영 중"으로 적었는데 원본(VWorld
  `make_sttus_nm`)은 "조성완료"였다. 적재가 원본 글자를 버리고 코드만 남긴 탓에 화면이 번역표를 따로 가졌다.
- 실제 사례(출처는 아래):
  - 업계는 **기술 카탈로그**(엔진이 표를 찾는 곳: Iceberg REST, Glue, Unity, Polaris)와 **데이터 카탈로그**(사람이
    데이터를 찾는 곳: DataHub, Atlan)를 앞말로 가른다.
  - 배민은 DataHub 위의 사내 서비스를 "데이터카탈로그"라 부르고 Spark·Hive·MySQL·Redash 를 연결했다. 쏘카는
    Amundsen 과 비교해 DataHub 를 골랐다. DataHub 는 LinkedIn 이 만들었다.
  - PayPal 의 데이터 계약 템플릿은 2023년 리눅스재단 Bitol 로 가 **ODCS**(Open Data Contract Standard, v3)가 됐다.
  - OpenLineage 는 Airflow 2.7 부터 내장이고 Spark·Flink·dbt 가 기록을 낸다. 사실상 표준이다.
  - Airbnb 는 중요한 데이터에 Midas 인증을, 모든 데이터에 매일 품질 점수를 매겨 카탈로그에 띄운다.
- 사용자 지시(2026-10-01): 전환 비용과 무관하게 명칭·기능 모두 대기업 수준으로, 명확하게.

## Decision

1. **공식 용어는 넷이다.** 문서·화면·코드 식별자는 이것만 쓴다.

   | 용어 | 실체 | 하는 일 |
   |---|---|---|
   | **레이크하우스 카탈로그** | Cloudflare R2 Data Catalog (Iceberg REST) | 엔진이 표의 현재 메타데이터를 찾는다 |
   | **데이터 카탈로그** | DataHub | 사람이 데이터를 찾고, 뜻·담당자·흐름·실행·품질을 본다 |
   | **기준정보 API** | Foundation 서빙 API (지금 `/catalog/v1/...`) | 서비스에 산업단지·필지·건물 기준정보를 준다 |
   | **데이터 계약** | 데이터셋마다 ODCS 파일 하나 | 이름·담당·칸·원천·주기·품질·상태의 약속 |

   원천 목록(`public-source-endpoint-catalog`)은 **원천 목록(registry)** 이라 부른다. "카탈로그"는 앞의 두 줄에만
   쓴다. 기준정보 API 의 경로는 `/master-data/v1/...` 로 바꾸고, 옛 경로는 소비자(공짱·더니어·타일)가 모두
   옮길 때까지 같은 처리기를 가리킨다(별도 작업, 이 ADR 이 결정).

2. **데이터 계약이 정본이다.** 데이터셋 하나당 ODCS v3 YAML 하나를
   `platforms/foundation-platform/contracts/data/` 에 둔다. 칸 목록·원천·상위 데이터셋·담당자(만드는 사람과
   쓰는 사람 둘)·갱신 주기 약속·품질 규칙·선언 상태(계획됨·없음 포함)가 여기 있다. `pipeline-graph.v1.json` 은
   계약에서 **생성되는 파생물**이 되고 손으로 고치지 않는다. ADR-0086 의 "모든 데이터셋을 한 번씩 명명"은 계약
   파일의 존재로 지킨다(가드는 계약 디렉터리를 센다).

3. **사람이 보는 곳은 데이터 카탈로그 하나다.** DataHub 에 네 가지가 들어간다.
   - 계약: 병합되면 CI 가 등록한다(설명·담당·용어·상태).
   - 레이크하우스 카탈로그: DataHub Iceberg 수집기가 매일 표·칸·스냅숏을 가져온다.
   - 기준정보 DB: DataHub Postgres 수집기가 서빙 표를 가져온다.
   - 실행 기록: 모든 적재 작업이 **OpenLineage** 로 보낸다(Spark 는 `openlineage-spark`, Rust 발행기는 HTTP).

   흐름·계보·실행을 보여 주는 화면을 다른 곳에 새로 만들지 않는다. 더니어는 데이터 카탈로그로 연결하고, 필요한
   요약만 DataHub API 에서 받아 그린다.

4. **로그인과 권한은 Zitadel 이다.** DataHub 는 직원 로그인 앱 목록
   (`staff-console-applications.v1.json`)의 한 줄로 OIDC 로그인하고, 미리 등록된 계정만 들어간다. 비밀번호
   로그인은 끈다. DataHub 권한은 Zitadel 역할에서 정한다.

5. **화면에 보이는 원본 값은 원본 글자로 싣는다.** 적재가 원본 글자를 코드로 바꿀 때 원본 글자 칸을 함께 남기고,
   API 는 둘 다 내준다. 화면은 번역표를 갖지 않는다. 업무 용어의 뜻은 DataHub 용어집에 적는다.

6. **품질은 계약의 규칙으로 잰다.** 행 수·신선도·칸 구조 규칙의 결과를 DataHub 에 싣고 데이터셋마다 품질 점수를
   보인다. 핵심 데이터셋은 **인증 등급**(설계 합의 → 제작 → 검토 → 인증)을 받는다.

7. **DataHub 는 저장소 코드로 설치한다.** 설정·버전·메모리 상한은 `platforms/foundation-platform/infra/datahub/`
   에 두고 배포 스크립트로 올린다. 포트는 루프백에만 연다. 시험 설치(ai-server `~/datahub-trial`)는 그 코드로
   바뀌면 지운다.

8. **걷어 낼 것과 그 조건.** 대체물이 운영에서 확인되면 같은 변경에서 지운다.

   | 걷어 낼 것 | 대체물 | 지우는 조건 |
   |---|---|---|
   | `docs/data-pipeline-map.md` 와 생성기 | 데이터 카탈로그 | 계약이 DataHub 에 등록됨 |
   | `GET /catalog/v1/pipeline-graph` 과 실시간 덧씌움 | OpenLineage 실행 기록 | 적재 작업 전부가 기록을 보냄 |
   | 더니어 "데이터 흐름" 메뉴 | 데이터 카탈로그 링크·요약 | 위 API 를 지울 때 |
   | `catalog.lakehouse_*` 빈 표 여섯 | 데이터 카탈로그 | 지금(0행, 쓰는 코드 없음) |
   | `pipeline-graph.v1.json` 손 편집 | 계약에서 생성 | 계약 전환 완료 |

## Consequences

- 새 데이터셋은 계약 파일 하나로 시작한다. 계약 없는 표는 CI 가 막는다.
- 이 결정의 실행 순서: ① DataHub Zitadel 로그인 ② 계약 형식과 생성기 ③ Iceberg·Postgres 수집 ④ OpenLineage 발신
  ⑤ 걷어 내기 ⑥ 기준정보 API 경로 이전. 적재 예약·재시도를 맡을 실행 도구, 공용 Kafka, 서버 프로그램 버전의 한
  곳 관리는 별도 ADR 로 정한다(서버 메모리가 그 결정의 실제 제약이다).
- DataHub 는 7개 컨테이너(실측 약 4GB)를 더 운영하게 한다. 백업·버전 올리기를 배포 스크립트가 맡는다.
- 사례 출처: [배민](https://techblog.woowahan.com/21434/), [쏘카](https://tech.socarcorp.kr/data/2022/02/25/data-discovery-platform-01.html),
  [LinkedIn DataHub](https://www.linkedin.com/blog/engineering/archive/data-hub),
  [ODCS](https://bitol-io.github.io/open-data-contract-standard/v3.0.0/home/),
  [OpenLineage Airflow](https://openlineage.io/docs/integrations/airflow/),
  [Airbnb 품질](https://medium.com/airbnb-engineering/data-quality-at-airbnb-870d03080469),
  [기술·데이터 카탈로그 구분](https://www.hpcwire.com/bigdatawire/2024/07/03/data-catalogs-vs-metadata-catalogs-whats-the-difference/).

---

2026-10-01 각주: §3·§4 의 "사람이 보는 곳" 은 [ADR-0119](./0119-staff-read-the-data-catalog-in-dawneer-through-foundation.md) 가 더니어로 구체화했다(DataHub 화면은 관리자 전용).
