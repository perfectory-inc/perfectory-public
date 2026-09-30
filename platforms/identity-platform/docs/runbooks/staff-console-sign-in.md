---
status: current
owner: identity-platform
doc_type: runbook
last_reviewed: 2026-09-30
---

# 직원 콘솔(더니어) 로그인 준비

루트 [ADR-0116](../../../../docs/adr/0116-dawneer-keeps-staff-tokens-on-its-rust-server.md) 의 첫 판: 더니어는 직원
노트북에서 돌고, 운영 서버(ai-server)의 Zitadel 과 Foundation API 에 SSH 터널로 붙는다.

## 1. 터널 (노트북, 쓰는 동안 열어 둔다)

```bash
ssh -N -i ~/.ssh/perfectory_ai_server \
  -L 18453:127.0.0.1:18453 \
  -L 18080:127.0.0.1:18080 \
  <운영자>@<운영 서버>
```

운영 서버 주소와 계정은 운영 인벤토리에 있고 저장소에는 적지 않는다.

포트를 바꾸지 않는다. 토큰의 발급자(`iss`)가 `http://127.0.0.1:18453` 이어야 운영 API 가 받는다.

## 2. Zitadel 앱 (한 번, 서버)

`infra/zitadel/configure-zitadel.sh` 가 `config/staff-console-applications.v1.json` 의 앱을 만든다. 두 번째 실행부터는
`exists` 만 찍힌다. client id 와 secret 은 `/etc/identity-platform/secrets/dawneer-oidc-client.env`(0600)에만 있다.
노트북으로 옮길 때는 복사하되 저장소 안에 두지 않는다.

## 3. 내 계정 (한 번, 본인이 직접)

1. 터널을 연 채로 브라우저에서 `http://127.0.0.1:18453/ui/console` 을 연다.
2. 인스턴스 관리자로 로그인한다. 아이디·비밀번호는 서버의 `/etc/identity-platform/zitadel.env`
   (`ZITADEL_ADMIN_USERNAME`, `ZITADEL_ADMIN_PASSWORD`)에 있다 — 본인이 직접 읽는다. 에이전트에게 보여 주지 않는다.
3. Users → New 로 본인 계정(사람)을 만들고 비밀번호를 정한다.
4. 만든 사용자의 **User ID** 를 적어 둔다. 이것이 토큰의 `sub` 이고 비밀이 아니다.

## 4. 직원 행과 역할 (한 번, 서버)

첫 `MASTER_ADMIN` 은 신원 API 가 시작할 때 만든다. 이미 `MASTER_ADMIN` 이 있으면 아무 일도 하지 않는다.

```bash
# /etc/identity-platform/runtime.env 에 세 줄을 넣고
IDENTITY_BOOTSTRAP_ADMIN_ZITADEL_SUBJECT=<3.4 의 User ID>
IDENTITY_BOOTSTRAP_ADMIN_EMAIL=<본인 메일>
IDENTITY_BOOTSTRAP_ADMIN_DISPLAY_NAME=<이름>
# 신원 API 를 다시 올린다
cd ~/identity-platform/current && bash scripts/deploy/identity-runtime.sh up -d --no-deps identity-api
```

그 뒤의 직원은 `MASTER_ADMIN` 이 `POST /identity/v1/staff/{staff_id}/roles` 로 역할을 준다
(`LINEAGE_STEWARD` 검토, `LINEAGE_ADJUDICATOR` 검토 + 승인).

## 5. 확인

더니어에 로그인해서 필지 검토 목록이 열리면 끝이다. 401 이면 토큰(발급자·audience·`principal_kind`), 403 이면 역할을 본다.
