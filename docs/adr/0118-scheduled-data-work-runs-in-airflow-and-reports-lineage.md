# ADR 0118: 예약된 데이터 작업은 Airflow 가 돌리고, 실행마다 계보를 남기며, 서버 프로그램도 버전·메모리를 한 곳에서 정한다

- Status: Accepted
- Date: 2026-10-01
- Builds on: [ADR-0117](./0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md)(데이터 카탈로그 하나),
  [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md)(버전은 계약 한 곳),
  [ADR-0077](./0077-the-pipe-looks-at-its-sources-every-day.md)(매일 원천 확인)

## Context

2026-10-01 ai-server 실측이다.

- **예약 작업은 systemd 타이머 여섯이다**: 원천 확인(`foundation-source-sweep`), 이벤트 발행(`outbox-publish`),
  지도 편집 접기 둘(`map-edit-fold@admin`, `@complex`), 계보 검토(`lineage-stewardship`), DB 백업. 실행 방법은
  서버의 손수 짠 스크립트에 흩어져 있고, 재시도·의존 순서·실행 이력은 각자 다르며, 무엇을 읽고 썼는지는 남지 않는다.
  큰 적재(Spark)는 사람이 서버에서 직접 띄운다.
- **Kafka 는 코드에만 있다.** Foundation 이벤트 발행기는 `rdkafka` 로 짜였고 Intelligence 에는 Redpanda 구성이
  있지만, 운영 서버에 Kafka 는 돌지 않는다. 발행을 기다리는 이벤트가 쌓인다. DataHub 도 Kafka 를 쓴다.
- **서버 프로그램 버전은 compose 파일마다 따로 적혀 있다.** ADR-0097 의 계약(`tools/technology-versions.contract.json`)
  은 JS 매니페스트만 다룬다. Zitadel·PostgreSQL·Trino·Spark·Martin·DataHub 의 이미지는 각 compose 에 있다.
- **메모리 상한이 없는 컨테이너가 있다.** Trino(`compose.lakehouse.yml`)는 컨테이너 상한 0(무제한)에
  `-XX:MaxRAMPercentage=80` 이라 장비 62GB 의 80%까지 잡을 수 있고, 쉬는 중에도 27.7GB 를 쥐고 있었다.
  같은 장비에서 Spark 적재와 DataHub(실측 약 4GB)가 돈다.
- 사례: Airflow 는 Airbnb 가 만들어 가장 널리 쓰이고, 2.7 부터 OpenLineage 를 내장했다(3.x 유지, 최신 3.3.2).
  Netflix 는 규모 때문에 자체 도구(Maestro)를 만들었다. Kafka 는 LinkedIn 이 만들었고 4.x 부터 ZooKeeper 가
  없다(KRaft). 배민의 DataHub 도 Kafka 를 쓴다.

## Decision

1. **예약된 데이터 작업은 Apache Airflow 3 이 돌린다.** 원천 확인·이벤트 발행·지도 편집 접기·계보 검토·Spark
   적재는 Airflow DAG 가 되고, 해당 systemd 타이머와 서버 손수 스크립트는 DAG 가 운영에서 한 번 성공한 뒤 지운다.
   DAG 는 저장소 `platforms/foundation-platform/orchestration/dags/` 에 있고 배포가 나른다. 재시도·의존 순서·
   실행 이력·알림은 Airflow 가 맡는다. DB 백업은 데이터 작업이 아니라 기반 설비이므로 systemd 에 남는다.

2. **모든 실행이 계보를 남긴다.** Airflow 의 OpenLineage 제공자가 작업마다 실행 기록을 데이터 카탈로그로 보낸다.
   Spark 적재는 `openlineage-spark` 로 표·칸 단위 입출력을 보내고 부모 실행(DAG)을 가리킨다. Rust 발행기는 같은
   양식을 HTTP 로 보낸다. 입력·출력을 알리지 않는 작업은 병합하지 않는다.

3. **Kafka 는 하나를 같이 쓴다.** Apache Kafka 4.x(KRaft, 단일 브로커로 시작)를 Foundation 이벤트와 DataHub 가
   함께 쓴다. 토픽 이름은 `<도메인>.<대상>.<사건>.v<판>` 이다. Intelligence 의 Redpanda 구성은 이 Kafka 로 옮긴다.

4. **품질 검사는 계약이 정하고 Airflow 가 돌린다.** 데이터 계약(ADR-0117 §2)의 규칙(행 수·신선도·칸 구조)을
   적재 뒤 작업이 재고, 결과를 데이터 카탈로그에 싣는다. 어긋나면 다음 단계가 멈추고 알림이 간다.

5. **서버 프로그램도 버전은 한 곳이다.** `tools/technology-versions.contract.json` 에 컨테이너 이미지(태그와
   digest)를 더하고, compose 파일은 그 값을 쓴다. 가드가 compose 의 이미지 줄과 계약을 대조한다.

6. **모든 컨테이너는 메모리 상한을 갖는다.** 상한은 배포 설정에 적고, 장비 합계가 물리 메모리를 넘지 않게 예산표를
   둔다. JVM 은 상한 안에서만 비율로 잡는다. Trino 는 실측 필요량에 맞춰 상한을 건다.

7. **자리는 ai-server 다.** 위 예산표로 들어가지 않으면 이 부분만 다른 장비로 옮기는 결정을 새 ADR 로 한다.

## Consequences

- 운영 컨테이너가 늘어난다(Airflow 스케줄러·API·DB, Kafka). 대신 실행 방법이 저장소 코드가 되어, 서버 손수
  스크립트가 정본이던 부채가 닫힌다.
- 실행 순서: ① 메모리 예산표와 Trino 상한 ② 이미지 버전 계약·가드 ③ Kafka ④ Airflow 설치(Zitadel 로그인)
  ⑤ 타이머 여섯 중 다섯을 DAG 로 옮기며 하나씩 타이머 제거 ⑥ Spark 적재를 DAG 로.
- 출처: [OpenLineage Airflow](https://openlineage.io/docs/integrations/airflow/),
  [Airflow 3.3.2](https://github.com/apache/airflow/releases),
  [Kafka 4.x](https://kafka.apache.org/blog/2026/05/30/apache-kafka-4.2.1-release-announcement/),
  [Netflix Maestro](https://blog.bytebytego.com/p/how-netflix-orchestrates-millions),
  [배민 DataHub](https://techblog.woowahan.com/21434/).

---

> **개정 각주(2026-10-03, 검토한 대안 보완):** 이 결정은 검토한 대안을 적지 않았다.
>
> **검토한 대안: Dagster.** 이전 조사(`docs/reference/geography-identity-enterprise-survey.md`)가 "코드가 자산 그래프의 정본"이라는
> 점을 들어 추천했다. 작업이 아니라 데이터 결과물(Silver·Gold·타일 판)을 단위로 다루므로, 개념은 이 저장소의 레이크하우스 구조와
> 더 맞는다.
>
> **지금 고르지 않은 이유.**
> - 대규모 운영 사례와 계보 연동(OpenLineage 내장, DataHub)은 Airflow 쪽이 더 넓다.
> - ADR-0122로 DAG는 systemd 서비스를 시작만 하는 얇은 층이다. 작업 로직은 저장소 코드에 있어 DAG 없이도 시험할 수 있다. 그래서
>   Dagster의 주된 이점(입출력 함수로서의 시험)이 지금은 작다.
>
> **다시 검토하는 조건.** 하나라도 생기면 다시 본다.
> - 결과물 사이 의존이 수십 개를 넘어 DAG 순서 관리가 어려워진다.
> - 운영에 배포해야만 드러나는 오케스트레이션 결함이 반복된다. 2026-09-30~10-01에는 세 건이었다: 첫 작업만 동기화,
>   배포 순서, 실행 권한.
> - 여러 팀이 같은 데이터를 나눠 만든다.
>
> **옮길 때의 방식.** Airflow를 유지한 채 Dagster를 점진적으로 붙인다(Mapbox 사례, `dagster-airlift`). 한 번에 대체하지 않는다.
> 출처: [Dagster at Mapbox](https://dagster.io/customers/incremental-adoption-mapbox), [Dagster customers](https://dagster.io/customers).
