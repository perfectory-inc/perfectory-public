---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# R2와 레이크하우스 실제 연결 확인

이 확인 절차는 개발용 원격 Cloudflare R2와 R2 Data Catalog에 실제로 연결하는 선택 실행 절차다. MinIO, LocalStack, 파일 저장소를 대신 사용하지 않는다. 파일 저장소는 단위 테스트 전용이다.

## 확인되는 것

1. Foundation 런타임이 관리된 개발용 R2 버킷을 가리키는지 확인한다.
2. 전용 임시 객체를 R2에 쓰고, 읽고, 삭제한다.
3. R2 객체 목록 조회가 실제로 되는지 확인한다.
4. R2 Data Catalog에서 지정된 Iceberg 표의 현재 스냅샷을 읽는다.

이 절차의 통과는 Spark가 Silver/Gold 표를 쓰고 Trino가 조회했다는 뜻이 아니다. 그 쓰기·읽기 왕복은 별도 증명 단계다. 또한 운영·스테이징 자격 증명으로 실행하지 않는다.

## 필요한 값

```text
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=              # 또는 ACCOUNT_ID
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_RUNTIME_ENV=local
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET=foundation-platform-lakehouse-dev
FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM=1
FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION=0
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=r2_data_catalog
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=foundation-platform
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN=
```

`FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET`은 실수로 다른 버킷에 쓰지 않도록 실제 버킷 이름과 같아야 한다. 엔드포인트와 계정 ID 중 하나는 반드시 있어야 한다. 스크립트는 기본적으로 `local:developer` 또는 `ci:ci`만 허용하고, 운영 버킷은 별도 승인값이 있을 때만 허용한다. 값이 빠졌으면 성공으로 처리하지 않는다.

스크립트는 기본적으로 `platforms/foundation-platform/.env.local`을 읽는다. 다른 파일을 쓰려면 `FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE`에 경로를 지정한다. 이미 셸에 같은 변수가 있으면 셸 값이 우선된다. `production + developer` 프로필은 기본 거부되며, 사전 출시 공유 운영 버킷을 직접 검증할 때만 셸에서 `FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION=1`과 기존 `FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1`을 함께 명시한다. 운영 버킷 검증은 임시 객체를 쓰고 읽고 삭제하므로, 이 승인값은 일회성 실행에만 설정한다.

## 실행

저장소 루트에서 실행한다.

```bash
bash scripts/verify/foundation-r2-lakehouse-live.sh
```

자격 증명이 없을 때는 이름이 표시된 오류와 종료 코드 2가 나와야 한다. 자격 증명을 우연히 사용하지 않았는지 먼저 확인하려면 다음을 실행한다.

```bash
bash scripts/guard/foundation-r2-lakehouse-live-self-test.sh
```

이 자체 검사는 네트워크에 연결하지 않고, 엔드포인트·자격 증명·카탈로그 공급자 누락을 모두 실패로 판정하는지 확인한다.

## 증거 해석

- `PASS (Cloudflare R2 and R2 Data Catalog)`: R2 쓰기/읽기/삭제, 목록 조회, 카탈로그 스냅샷 읽기가 모두 성공했다.
- 자체 검사 통과: 자격 증명 없는 CI가 실제 확인을 건너뛰지 않고 차단하는지만 증명한다.
- 어느 결과도 전국 수집 완료, Postgres 원장 전체 일치, Silver/Gold 승격, 운영 전환을 승인하지 않는다.

실제 자격 증명은 저장소나 로그에 기록하지 않는다. CI에서 실행할 경우 보호된 비운영 환경의 시크릿만 주입하고, 운영 버킷 이름은 사용하지 않는다.
