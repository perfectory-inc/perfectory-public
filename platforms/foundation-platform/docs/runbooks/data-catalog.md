---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-01
---

# 데이터 카탈로그(DataHub) 운영

[ADR-0117](../../../../docs/adr/0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md)이
정한 "사람이 보는 곳 하나"다. Foundation 과 같은 장비에서 돈다.

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

## 확인

- 비밀번호 로그인은 꺼져 있다: `POST /logIn` 은 어떤 비밀번호로도 400 이다.
- 컨테이너마다 메모리 상한이 있다(합계 약 8.4GB, 실측 사용 약 4.5GB).

## 버전 올리기

`compose.yml` 을 새 판의 업스트림 파일로 바꾸고, 새 이미지를 받아 digest 로 다시 고정한 뒤 `up -d` 한다.
`system-update` 컨테이너가 저장소 구조를 맞춘다.
