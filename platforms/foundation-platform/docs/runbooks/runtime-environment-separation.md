---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# 런타임 환경 분리

이 런북은 ADR 0029의 운영자용 절차다.

## 환경 변수

모든 운영 Foundation publisher 프로세스는 다음을 설정해야 한다.

```dotenv
FOUNDATION_PLATFORM_RUNTIME_ENV=local|ci|staging|production
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer|ci|service
FOUNDATION_PLATFORM_PRELAUNCH_SHARED=0|1
```

publisher는 값이 없거나 알 수 없는 값을 거부한다. `prod`, `dev` 또는 자유 형식 이름을 사용하지
않는다. `PRELAUNCH_SHARED=1`은 production을 명시적으로 대상으로 하는 개발자 프로세스에서만
허용한다.

## R2 설정

개발 환경도 MinIO가 아니라 Cloudflare R2를 사용한다.

```dotenv
FOUNDATION_PLATFORM_RUNTIME_ENV=local
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
FOUNDATION_PLATFORM_PRELAUNCH_SHARED=0
FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev
```

버킷 범위 R2 토큰을 사용한다. 운영 자격증명은 아래의 명시적인 임시 출시 전 예외에서만
`.env.local`에 허용한다.

### 현재 출시 전 예외

제품이 아직 출시되지 않았으므로 현재 비공개 `.env.local`은
`FOUNDATION_PLATFORM_RUNTIME_ENV=production`으로 기존 운영 R2/Data Catalog를 의도적으로
선택한다. 이것은 개발자 컴퓨터에서 실행하는 명시적인 production 모드이며 `local`의 별칭이
아니다. 외부 출시 뒤에는 이 예외를 사용하지 말고 먼저 `runtime=local`과 전용 개발 버킷으로
전환한다. 실제 운영 endpoint를 비공개 운영 설정으로 준비하기 전까지 Postgres·Valkey·Kafka·
Identity·compute는 local로 유지한다.

현재 비공개 출시 전 프로필은 다음과 같다.

```dotenv
FOUNDATION_PLATFORM_RUNTIME_ENV=production
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1
FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-prod
```

예상 버킷 이름은 다음과 같다.

```text
local       foundation-platform-lakehouse-dev (remote R2 development bucket)
ci          foundation-platform-lakehouse-ci
staging     foundation-platform-lakehouse-staging
production  foundation-platform-lakehouse-prod
```

## Redpanda/Karapace

Kafka/Avro 계약 작업을 할 때만 로컬 C2 fixture를 시작한다.

```bash
docker compose -f platforms/intelligence-platform/docker/c2-event-backbone.compose.yml up -d
```

broker와 registry 환경 변수를 명시해 실시간 테스트를 실행한다. 이 작업은 Foundation 운영
토픽을 발행하지 않는다. 운영 broker 선택과 producer 배선은 별도로 관리한다.

## 자격증명 없는 검증

일반 Cargo 검증과 단위 테스트는 자격증명 없이 유지해야 한다. mock·파일·logging 어댑터를
사용할 수 있지만 R2나 Kafka 연결을 증명하지는 않는다.

보호된 실시간 smoke job은 전용 CI/staging 자격증명을 제공하고 일치하는 런타임 환경을
설정해야 한다. 명시적으로 필요한 backend가 없으면 실시간 smoke job은 실패해야 하며,
없는 서비스를 성공한 soft skip으로 바꾸면 안 된다.

## 안전 확인

실시간 수집 또는 publisher 실행 전에 다음을 확인한다.

1. 런타임 환경이 명시되어 있다.
2. 실행 컨텍스트가 명시되어 있으며 developer→production에는 출시 전 플래그가 필요하다.
3. local/CI 제한 테스트 외부에서는 Bronze와 Catalog driver가 `r2`다.
4. `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET`이 환경별 버킷과 일치한다.
5. R2 토큰이 해당 버킷으로 범위가 제한되어 있다.
6. staging/production에서는 fixture 토픽·로컬 파일 root·logging 어댑터·프로세스 로컬 상태를
   사용하지 않는다.

Bronze publisher는 명령 시작뿐 아니라 쓰기 경계에서도 이를 강제한다. 모든 실시간 객체 저장소
어댑터는 사전 점검이 포함된 공통
`live_write_bronze_object_storage_from_env` 또는
`live_write_bronze_streaming_object_storage_from_env` helper로만 만든다. backend-profile guard는
이 helper를 우회하는 새 ingest 호출자를 거부한다. 이는 향후 수집 명령을 위한 심층 방어이며,
공급자 다운로드 전에 실행하는 앞단 사전 점검을 대체하지 않는다.
