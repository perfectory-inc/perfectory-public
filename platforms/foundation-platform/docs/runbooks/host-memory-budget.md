---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-01
---

# ai-server 메모리 예산

[ADR-0118](../../../../docs/adr/0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §6:
모든 컨테이너는 상한을 갖고, 상한의 합이 물리 메모리(62GB)를 넘지 않는다. 2026-10-01 실측이다.

| 묶음 | 상한 | 실측 사용 | 상한 위치 |
|---|---|---|---|
| Trino | 20GB (힙 약 17GB, 질의 풀 12GB, 질의당 10GB) | 쉬는 중 27.7GB → 상한 후 기동 직후 0.7GB | `compose.lakehouse.yml`, `infra/lakehouse/trino/config.properties` |
| 데이터 카탈로그(DataHub) 7개 | 8.4GB | 약 4.5GB | `infra/datahub/compose.override.yml` |
| Spark 적재 | 작업마다 드라이버·실행기 메모리 인자 | 작업에 따라 다름 | 적재 스크립트 |
| Foundation·신원·Zitadel·지도 서빙 등 16개 | **없음** | 합계 약 0.5GB | 각 compose — 상한 추가가 남은 일 |

- Trino 의 가장 큰 질의는 7.0GB 를 잡았다(2026-09-25, 보관된 100개 중). 질의당 10GB 는 그 위에 여유를 둔 값이다.
  더 큰 질의가 막히면 상한과 질의 한도를 함께 올리고 이 표를 고친다.
- 상한이 없는 컨테이너 16개는 지금 합계 0.5GB 를 쓰지만, 상한이 없으면 한 컨테이너의 누수가 장비 전체를
  멈춘다. compose 별로 상한을 붙이고 이 표의 마지막 줄을 없앤다.
