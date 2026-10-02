---
status: accepted
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-10-02
---

# ADR-0132 — 새 버전은 켜져 있던 자동작업을 조용히 제거하지 않는다

## 원인과 불변식

FLOOR 자동수집이 동작하는 서버에 해당 변경을 포함하지 않은 버전이 활성화됐다.
systemd 유닛과 Airflow의 과거 DAG 기록은 남았지만 새 버전에는 실행 파일·설정·job이
없었다. 새 버전의 jobs 파일만 검사하면 빠진 작업 자체를 알 수 없었다.

일반 활성화는 기존 enabled 작업의 식별자를 보존해야 한다. 의도적으로 끄는 경우는
같은 jobs 정본에 `enabled:false`로 명시한다. 복구용 `rollback`은 별도 동작이다.

## 결정

- `foundation-release.sh`의 `activate_release`가 링크를 바꾸기 전에 현재·후보 버전의
  `orchestration/jobs.v1.json`을 비교한다. 기존 enabled job의 `id`가 후보에 없으면
  거부하며 `current`, `previous` 링크를 유지한다.
- 정본의 `enabled:false` 전환은 허용한다. 중복 job 식별자·잘못된 enabled 값은 거부한다.
  orchestration 도입 전 버전에서의 최초 설치는 이전 jobs 파일이 없을 수 있다.
- 작업 목록이나 별도의 허용 레지스트리를 추가하지 않는다. 기존 두 release의 정본을
  비교하는 제어 검사만 추가한다. 명시적 `rollback`의 복구 의미는 유지한다.
- 소스 통합과 활성화 검증은 별개다. 구현을 main에 보존해야 다음 정상 배포에도 남는다.
  과거 installer를 직접 호출하는 행위까지 이 새 검사로 차단했다고 주장하지 않는다.

## 근거와 대안

Airflow의 [DAG 중지·비활성화·삭제](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#dag-pausing-deactivation-and-deletion)는
파일 상태와 실행 이력의 차이를 설명한다. UI에 이름이 남았다는 사실을 runnable 증거로
사용하지 않는다. 기존 `airflow-runtime.sh`의 파싱·활성화 확인과 실제 실행을 사용한다.

새 스케줄러나 배포 프레임워크를 도입할 문제는 아니다. 기존 JSON 정본과 표준 Python으로
누락 여부만 검사한다. 작업별 파일명을 installer에 나열하는 대안은 또 다른 목록을 만들고,
이전 파일을 새 release로 자동 복사하는 대안은 서로 다른 버전의 소스를 섞어 기각한다.

## 검증

작업·jobs 파일 누락, 중복 식별자, 잘못된 enabled 값에서 링크 불변을 검사한다.
명시적 disable, 최초 설치, 전용 rollback의 기존 동작도 함께 검사한다.
실제 서버 검증과 미완료 범위는 [운영 준비 작업 목록](../roadmap/production-readiness.md)에 남긴다.
