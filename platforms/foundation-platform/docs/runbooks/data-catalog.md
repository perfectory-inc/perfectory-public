---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-01
---

# 데이터 카탈로그(DataHub) 운영

[ADR-0117](../../../../docs/adr/0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md)이
정한 데이터 카탈로그의 저장소다.
직원은 더니어의 데이터 카탈로그 메뉴로 읽고(ADR-0119, Foundation `/data-catalog/v1/*` 경유), DataHub 웹 화면은
플랫폼 관리자만 SSH 터널로 쓴다. Foundation API 는 공유 네트워크 `metadata-shared` 에서 `datahub-gms` 로 닿는다.
Foundation 과 같은 장비에서 돈다.

## 무엇이 어디 있나

| 무엇 | 위치 |
|---|---|
| 업스트림 compose(이미지 digest 고정) | `infra/datahub/compose.yml` |
| 우리 설정(포트·비밀값·로그인·메모리) | `infra/datahub/compose.override.yml` |
| 실행 스크립트 | `scripts/deploy/datahub-runtime.sh` |
| 비밀값·상태 | `DATAHUB_RUNTIME_DIR`(기본 `/var/lib/foundation-platform/datahub`, 0700). 운영 계정이 그 디렉터리에 쓸 수 없는 장비에서는 이 변수로 운영 계정의 홈 아래 0700 디렉터리를 가리킨다 |
| Zitadel 로그인 앱 | identity-platform `config/staff-console-applications.v1.json` 의 `datahub` 줄 |

웹 화면은 `127.0.0.1:19002`, GMS API 는 `127.0.0.1:18095` 에만 열린다. 직원은 SSH 터널로 들어온다.

## 처음 올리기

1. identity-platform 에서 `infra/zitadel/configure-zitadel.sh` 를 실행한다. `datahub` 앱이 생기고 client
   id·secret 이 `/etc/identity-platform/secrets/datahub-oidc-client.env`(0600)에 쓰인다.
2. `scripts/deploy/datahub-runtime.sh init-secrets` — 비밀값 파일을 한 번 만든다. 화면에 찍지 않는다.
3. `scripts/deploy/datahub-runtime.sh up -d` — UI·GMS 응답과 "로그인이 Zitadel 로 넘어감"까지 확인하고
   끝난다. 하나라도 아니면 실패로 끝난다.
4. `scripts/deploy/datahub-runtime.sh provision <직원 이메일>` — 미리 등록된 계정만 들어올 수 있다
   (Zitadel 로그인만으로는 접근이 아니다).

## 실물 수집

`scripts/deploy/datahub-runtime.sh ingest lakehouse-iceberg` 가 `infra/datahub/recipes/lakehouse-iceberg.yml` 로
레이크하우스 카탈로그의 모든 표(이름·칸·타입·스냅숏)를 읽어 온다. 행은 읽지 않는다. 레시피의 `${변수}` 는 실행하는
프로세스의 환경에서 이름으로만 넘기며 값을 찍지 않는다(R2 reader 키·카탈로그 토큰). 매일 실행은 Airflow 가 맡는다
(ADR-0118); 그 전까지는 손으로 돌린다.

선언 흐름(`seed_declared_lineage.py`)은 레이크하우스 표를 같은 `iceberg` 엔티티에 붙인다 — 설명·선언 상태는
사람 편집 설명칸에, 흐름은 작업(job)으로. 그래서 한 표에 계획(설명·흐름)과 실물(칸·스냅숏)이 함께 보인다.

2026-10-01 첫 수집: 표 38개, 실패 0. 계획과 실물의 차이:

| 차이 | 표 |
|---|---|
| 계획에 "implemented" 인데 레이크하우스에 없음 | `silver.parcel_registry`, `silver.parcel_lineage`, `gold.lineage_review_queue`, `reference.legal_dong_code`, `reference.sigungu_canonical_crosswalk` |
| 계획에 계약만 있음(정상) | `gold.complex_spatial_locator`, `silver.complex_parcel_memberships` |
| 레이크하우스에 있는데 계획에 없음 | 시험 흔적 8개(`*_smoke`, `*_probe`, `dist_probe.*`, `sail_probe.*`), `gold.building_resources`, `gongzzang_silver.court_auction_property` |

## 데이터 계약 등록

`contracts/data/*.odcs.yaml`(ADR-0123, 생성물)을 등록한다. 계약 파일이 바뀐 릴리스를 배포한 뒤 돌린다.

```bash
bash /opt/foundation-platform/current/scripts/deploy/datahub-runtime.sh ingest data-contracts
```

DataHub 의 `odcs` 수집기가 계약마다 `odcs` 플랫폼에 논리 데이터셋(계약 id 가 이름)을 만들고, 품질 규칙마다
검사 항목(assertion)과 표 구조 검사를 붙이고, 같은 이름의 `iceberg` 표에 연결한다. 수집기는 공식 ODCS
JSON 스키마로 계약을 검증한다(`validation_errors` 가 0 이어야 한다). 계약에서 빠진 표는 다음 실행에서 지워진다.

2026-10-01 첫 등록: 계약 35개, 실제 표 연결 35개(모두 실재 확인), 검사 항목 148개 + 표 구조 35개, 검증 오류 0.

- DataHub 1.7 은 ODCS v3.1 까지 읽는다(v3.2 계약은 "Unsupported apiVersion" 으로 하나도 들어가지 않는다).
- 수집 결과의 절반 이상이 바뀌면 DataHub 가 옛 항목 삭제를 한 번 미룬다(`fail_safe_threshold`). 이름 규칙을 바꾼
  직후라면 한 번 더 돌리면 정리된다.

## 품질 검사

Airflow 작업 `foundation_data_quality`(매일 07:20 UTC, `scripts/ops/data-quality-check.py`)가 계약에서 만든
검사 항목 중 기계가 판정할 수 있는 둘을 레이크하우스에 돌리고 결과를 각 검사 항목에 기록한다.

| 검사 | 판정 |
|---|---|
| 허용 값(FIELD_VALUES) | 값이 있고 `validValues` 에 없는 행이 하나라도 있으면 실패 |
| 표 구조(DATA_SCHEMA) | 계약의 칸이 실제 표에 하나라도 없으면 실패 |

글로 적은 규칙은 돌리지 않고 결과도 적지 않는다. 레이크하우스에 아직 없는 표는 실패가 아니라 "absent" 로 센다
(지도의 implemented 는 코드가 있다는 뜻). 실패가 하나라도 있으면 작업이 실패로 끝나 알림이 간다.

2026-10-01 첫 실행: 계약 35개, 통과 31, 실패 1, 표 없음 7. 실패는 `gold.parcel_panel` 에 계약의 `row_digest`
(ADR-0099, 9/10 추가)와 `attached_via_json`(ADR-0113 §6, 9/30 추가)이 없는 것 — 운영 표가 9/9 이후 다시 만들어지지
않았다.

## 확인

- 비밀번호 로그인은 꺼져 있다: `POST /logIn` 은 어떤 비밀번호로도 400 이다.
- 컨테이너마다 메모리 상한이 있다(합계 약 8.4GB, 실측 사용 약 4.5GB).

## 버전 올리기

`compose.yml` 을 새 판의 업스트림 파일로 바꾸고, 새 이미지를 받아 digest 로 다시 고정한 뒤 `up -d` 한다.
`system-update` 컨테이너가 저장소 구조를 맞춘다.
