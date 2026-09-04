---
status: current
owner: identity-platform
doc_type: runbook
last_reviewed: 2026-09-04
---

# LAN 호스트에서 신원 스택 기동

결정은 root ADR-0080 이 소유한다: issuer 는 `http://127.0.0.1:18453` 하나이고, 모든
소비자는 자기 루프백에서 그 주소에 닿는다. 이 문서는 빈 호스트에서 보호 라우트 200
까지의 순서와, 각 단계가 실패하는 알려진 방식이다.

## 지형

| 것 | 위치 |
| --- | --- |
| Zitadel(발급자) | compose 프로젝트 `zitadel-platform`, 호스트 127.0.0.1:18453 |
| identity-api | compose 프로젝트 `identity-platform-runtime`, 호스트 127.0.0.1:18082 |
| 공유망 | `identity-shared` (외부 네트워크; 래퍼들이 없으면 만든다) |
| env 파일 | `/etc/identity-platform/{zitadel.env,runtime.env}` (0640) |
| 바인딩 파일 | `/etc/identity-platform/workload-principal-bindings.json` |
| 래퍼 | `scripts/deploy/{zitadel-runtime.sh,identity-runtime.sh}` |

## 0. env 파일

`infra/zitadel/.env.example` 과 `.env.example` 을 본떠 두 env 파일을 만든다.
`runtime.env` 에는 identity compose 의 필수 비밀 5개 외에 다음을 적는다:

```
IDENTITY_DB_PORT=15437
IDENTITY_API_PORT=18082
IDENTITY_ZITADEL_ISSUER_URL=http://127.0.0.1:18453
IDENTITY_API_AUDIENCE=<Zitadel 프로젝트 ID — 2단계에서 얻는다>
IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS_FILE=/etc/identity-platform/workload-principal-bindings.json
```

## 1. Zitadel

첫 부팅 **전에** PAT 볼륨 소유자를 맞춘다 — 안 하면 setup 이 `permission denied` 로
죽고 PAT 는 영영 안 쓰인다:

```
docker volume create zitadel-platform_zitadel-init-pat
docker run --rm -v zitadel-platform_zitadel-init-pat:/v alpine:3.20 chown 1000:1000 /v
scripts/deploy/zitadel-runtime.sh up -d
curl -sf http://127.0.0.1:18453/debug/healthz   # 될 때까지; 첫 init 은 수 분
```

부트스트랩 PAT 를 한 번 읽어 호스트 비밀 자리로 옮긴다:

```
docker run --rm -v zitadel-platform_zitadel-init-pat:/v:ro alpine:3.20 cat /v/pat \
  > /etc/identity-platform/secrets/zitadel-bootstrap-pat   # 0400
```

## 2. Zitadel 구성 (PAT 로 API 호출)

전부 `-H "Authorization: Bearer $(cat …/zitadel-bootstrap-pat)"` 로
`http://127.0.0.1:18453` 에 건다.

1. **프로젝트 생성** `POST /management/v1/projects` `{"name":"perfectory"}` —
   응답의 `id` 가 **audience 값**이다. `runtime.env` 의 `IDENTITY_API_AUDIENCE` 와
   foundation 의 `FOUNDATION_PLATFORM_ZITADEL_AUDIENCE` 에 적는다.
2. **principal_kind 액션**: foundation-api 와 identity-api 는 이 클레임이 없는
   토큰을 거부한다. 액션 본문과 플로 연결의 정본은
   `platforms/foundation-platform/scripts/smoke/identity-foundation-signed-oidc.sh`
   (`principalKind` 로 검색)이다 — `POST /management/v1/actions` 로 만들고, 플로
   유형 2(토큰 보완)의 트리거 5(pre access token creation)에 **POST** 로 붙인다
   (`curl --data-binary … /management/v1/flows/2/trigger/5`). 실측 함정 둘:
   `-X PUT` 은 405 인데 `&&` 사슬 속에서는 조용히 사라진다 — 붙인 뒤
   `GET /management/v1/flows/2` 로 `triggerActions` 에 실제로 실렸는지 본다.
   그리고 붙이기 전에 발급된 토큰에는 클레임이 없다 — 판정은 항상 새 토큰으로.
3. **워크로드 기계 사용자**: `POST /management/v1/users/machine`
   (`userName: gongzzang-api`), 이어서
   `PUT /management/v1/users/{id}/machine` 으로 `accessTokenType` 을
   `ACCESS_TOKEN_TYPE_JWT` 로 — 기본값은 불투명 토큰이라 JWKS 검증이 불가능하다.
   `PUT /management/v1/users/{id}/secret` 으로 client_credentials 자격을 받는다.
   응답의 사용자 `id` (숫자 subject) 를 기록한다.

## 3. identity 스택

바인딩 파일에 2-3단계의 실제 subject 를 적는다 (capabilities 는 파일에 없다 —
provisioner 의 정책 산출물이 `gongzzang-api → foundation.catalog:read` 를 소유한다):

```json
{ "schema_version": "identity.workload-principal-bindings.v1",
  "bindings": [ { "service_slug": "gongzzang-api", "zitadel_subject": "<subject>" } ] }
```

이미지를 만들고 사슬을 올린다. **`identity-api` 를 지목한다** — 정책 워커는
ADR-0080 §5 로 기동하지 않으며, `up` 에 이름 없이 올리면 워커의 필수 env 가 없어
전체가 멈춘다:

```
scripts/deploy/identity-runtime.sh build identity-api
scripts/deploy/identity-runtime.sh up -d --wait identity-api
curl -sf http://127.0.0.1:18082/healthz && curl -sf http://127.0.0.1:18082/readyz
```

`readyz` 503 은 대개 DB 아니면 issuer 도달 실패다 — 사이드카가 함께 떴는지
(`docker ps | grep zitadel-loopback`) 먼저 본다.

## 4. foundation 전환

`/etc/foundation-platform/recovery.env` 의 자리표시자 세 칸을 실값으로 바꾸고
(`ZITADEL_ISSUER_URL=http://127.0.0.1:18453`, `IDENTITY_API_BASE_URL=http://127.0.0.1:18082`,
`FOUNDATION_PLATFORM_ZITADEL_AUDIENCE=<프로젝트 ID>`) 릴리스로 재기동한다:

```
sudo foundation-release.sh migrate
```

## 5. 판정 — 통과와 거부를 둘 다 본다

```
TOKEN=$(curl -sf http://127.0.0.1:18453/oauth/v2/token \
  -d grant_type=client_credentials -d client_id=<clientId> -d client_secret=<secret> \
  -d "scope=openid urn:zitadel:iam:org:project:id:<프로젝트ID>:aud" | jq -r .access_token)
# 통과: 200 과 본문
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer ${TOKEN}" \
  "http://127.0.0.1:18080/catalog/v1/parcels/by-pnu/<실측 PNU>"
# 거부도 실증한다: 토큰 없이 같은 요청 → 401, 남의 audience 토큰 → 401
```

셋 다 기대값이어야 완료다. 200 만 보고 끝내면 "아무 토큰이나 통과"를 놓친다.
