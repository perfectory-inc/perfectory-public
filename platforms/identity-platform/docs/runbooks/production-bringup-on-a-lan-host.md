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
포트는 적지 않는다 — 결정 상수는 `config/identity-runtime-endpoints.contract.json`
하나가 소유하고 래퍼가 도출한다(root ADR-0081). `runtime.env` 에는 identity compose
의 필수 비밀 5개 외에 다음을 적는다:

```
IDENTITY_DB_PORT=15437
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

## 2. Zitadel 구성 — 멱등 스크립트 (root ADR-0081)

```
infra/zitadel/configure-zitadel.sh --emit-bindings /etc/identity-platform/workload-principal-bindings.json
```

프로젝트·principal_kind 액션·플로 연결·기계 사용자(정책 산출물의 slug 전부)를
"없으면 만들고 있으면 통과"로 보장하고, 실측 subject 로 바인딩 문서까지 쓴다.
출력의 `project id=` 가 **audience 값**이다 — `runtime.env` 의
`IDENTITY_API_AUDIENCE` 와 foundation 의 `FOUNDATION_PLATFORM_ZITADEL_AUDIENCE`
에 적는다. 두 번 돌려 전 행 `exists` 인지 보는 것이 구성 검증이다.

스크립트 밖에 남는 운영자 행위는 **비밀 발급**뿐이다: 자격이 필요한 서비스만
`PUT /management/v1/users/{subject}/secret` 으로 client_credentials 를 받아
`/etc/identity-platform/secrets/` 에 둔다(예: gongzzang-api). 실측 함정: 플로에
붙이기 **전에** 발급된 토큰에는 클레임이 없다 — 판정은 항상 새 토큰으로.

## 3. identity 스택

바인딩 파일은 2단계가 이미 썼다(capabilities 는 파일에 없다 — provisioner 의 정책
산출물이 `gongzzang-api → foundation.catalog:read` 를 소유한다).

이미지를 만들고 사슬을 올린다. 정책 워커는 ADR-0080 §5 로 기동하지 않는다 —
compose 에서 `policy-worker` 프로필 뒤에 있어 기본 `up` 에서 빠진다:

```
scripts/deploy/identity-runtime.sh build identity-api
scripts/deploy/identity-runtime.sh up -d --wait identity-api zitadel-loopback
curl -sf http://127.0.0.1:18082/healthz && curl -sf http://127.0.0.1:18082/readyz
```

**사이드카는 반드시 이름으로 부른다.** compose 는 의존 화살표 방향으로만 끌어올리므로
`up identity-api` 는 api 에 의존하는 사이드카를 안 띄운다 — 첫 기동에서 이대로
`readyz` 200 인데 판정만 500 이 났다(JWKS 를 자기 루프백에서 못 가져옴). 확인은
`docker ps | grep zitadel-loopback`.

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
