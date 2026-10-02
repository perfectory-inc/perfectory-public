---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-02
---

# ADR 0129: 작업용 Spark는 종료되고 메모리 예산은 실제 실행 형태를 센다

- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md),
  [ADR-0127](./0127-floor-duckdb-staging-preserves-normalization-and-native-resource-contracts.md)

## 원인과 불변식

`compose.lakehouse.yml`의 Spark는 `sleep infinity`와 자동 재시작으로 계속 떠 있었다.
그러나 실제 적재기와 예약 작업은 `docker compose run --rm spark`로 새 컨테이너를 만든다.
기존 메모리 가드는 Spark의 상한을 상시 서비스 한 개로만 계산해 추가 작업을 빠뜨렸다.
FLOOR의 native overlay도 호스트 예산에 배치되지 않았다.

상시 서비스와 실행 중인 작업의 합계를 실제 실행 형태로 계산해야 한다. native 메모리 수치는
기존 engine contract가 계속 소유한다. 가드를 통과시키려고 수치를 복사하거나 overlay를 검사에서
제외해서는 안 된다.

## 결정

1. Spark는 작업용 서비스로 유지하며 `restart: "no"`를 사용한다. 기본 명령은
   `spark-submit --version`으로 끝난다. 실제 작업은 기존 `compose run --rm`이 명령을 지정한다.
   Trino의 상시 조회 서비스는 유지한다.
2. 호스트 예산의 compute 묶음이 기본 Compose와 native overlay, 관련 profile을 함께 검사한다.
   native 메모리 변수는 `tools/host-memory-budget.contract.json`의 명시적 참조를 통해
   `lakehouse-engine.contract.json`의 `execution_profile.memory_mib`에서 읽는다.
   외부 셸 환경, 기본값을 가진 치환, 누락·잘못된 참조를 상한으로 인정하지 않는다.
3. 기존 Airflow `spark` pool의 한 슬롯이 예약된 데이터 작업을 직렬 실행한다. FLOOR를 연결할
   때는 native 준비·변환과 Spark 적재 전체가 같은 슬롯에 있어야 한다. 예산 가드 자체가
   직접 실행한 임의의 작업이나 다른 호스트 프로그램의 동시 실행까지 막는 장치는 아니다.
4. 배포 시 이전에 떠 있던 Spark 대기 컨테이너도 새 정의로 재생성해야 한다. 코드와 예산 검증이
   통과했다는 이유만으로 기존 운영 컨테이너가 이미 종료됐다고 보고하지 않는다.

## 대안과 검증

[Docker Compose 공식 run 문서](https://docs.docker.com/reference/cli/docker/compose/run/)의
일회성 컨테이너 실행과 명령 덮어쓰기를 그대로 사용한다. 별도 작업 실행기나 메모리 제어 서비스를
추가하지 않는다. 대기 컨테이너를 남긴 채 계산에서만 빼는 방법은 기각한다. `exec`로 모든 작업을
한 컨테이너에 넣는 방법은 기존 작업별 종료·정리 경계를 바꾸므로 기각한다.

실제 Compose 렌더 결과의 메모리 상한을 공통 계약과 대조하고, 기본 Spark 명령의 종료를 확인한다.
가드는 참조 원본 값 변경이 합계를 바꾸는지와 잘못된 참조가 실패하는지를 검증한다. 상한의 실제
합계는 가드가 계산하며 이 ADR에 두 번째 숫자 목록을 만들지 않는다. 운영 설치와 FLOOR 예약
실행 확인은 별도 완료 증거가 필요하다.
