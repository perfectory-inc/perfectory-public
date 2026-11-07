# ADR 0434: 더니어는 데이터 호스트에서 돌고 Cloudflare Tunnel·Access 뒤에서 열린다 — Zitadel 로그인 화면은 더니어가 자기 주소에서 중계한다

- Status: Accepted
- Date: 2026-11-07
- Revises: [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md) §5(첫 판은 노트북에서 돈다)와 그 Consequences 의
  "공개 접속(Cloudflare Access 등)과 https issuer 는 이 ADR 밖이다"
- Builds on: [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md)(더니어는 틀만),
  [ADR-0080](./0080-the-issuer-answers-on-every-loopback.md)(issuer 는 루프백, 공개 issuer 는 후속 결정),
  [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md)(서버는 빌드하지 않는다),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(런타임 비밀 계약),
  [ADR-0433](./0433-a-by-pnu-cut-over-and-a-full-bake-are-approved-in-the-console-and-the-host-runs-them-in-the-operator-window.md)(승인 단추)

## Context

ADR-0433(#1112–#1115, 병합)으로 by-PNU cut-over 와 전량 굽기는 더니어의 단추 하나로 승인된다. 그러나 더니어는 아직 노트북에서만 돈다
(ADR-0116 §5). 사장이 단추를 누르려면 노트북에서 SSH 터널을 열고(`-L 18453 -L 18080`), 서버의 OIDC client secret 을 노트북으로 복사하고,
`cargo run` 해야 한다. 휴대폰에서는 아예 안 된다. 그래서 단추가 SSH 명령을 실제로는 대신하지 못한다.

2026-11-07 실측(읽기만):

| 무엇 | 지금 |
|---|---|
| 데이터 호스트(ai-server)의 공개 통로 | 없다. `cloudflared` 도, 공개 포트도 없다(ADR-0080 의 진술 그대로) |
| Zitadel | v2.65.1, issuer `http://127.0.0.1:18453`(루프백), `ExternalSecure=false` |
| Foundation API 가 받는 issuer | 하나(`ZITADEL_ISSUER_URL`). 토큰의 `iss` 가 다르면 401 |
| Cloudflare 계정 | `perfectory.io` 영역(Free). Worker 배포 토큰은 Tunnel 읽기는 되고 Access 는 403 — Zero Trust 를 쓸 권한이 없다 |
| 이미 있는 엣지 | Worker 게이트웨이 여섯(ADR-0175), 프로필 게이트웨이 `profiles.gongzzang.app`. 모두 R2 를 읽는 Worker 이고 서버에 닿지 않는다 |

밖에서 쓰는 직원 화면을 대기업은 이렇게 연다.
- Google BeyondCorp: 사내 앱은 VPN 이 아니라 **신원 인식 프록시(IAP)** 뒤에 둔다. 요청마다 사람·기기를 확인하고, 앱은 공개 포트를 갖지 않는다.
- Cloudflare 자신과 많은 회사가 같은 모양을 **Cloudflare Access + Tunnel** 로 한다. `cloudflared` 가 서버에서 바깥으로만 연결을 열고(들어오는
  포트 0), Access 가 엣지에서 사람을 확인한 요청만 그 연결로 보낸다. Zero Trust Free 는 50명까지 0원이다.
- 앱 자신의 로그인은 그대로 둔다(심층 방어). Zitadel 은 자기 로그인 화면을 다른 주소에서 중계하는 길을 문서로 둔다: 프록시가
  `x-zitadel-public-host` 에 공개 주소를 싣고, 인스턴스는 그 주소를 **trusted domain** 으로 갖는다(Zitadel "Login App — OIDC proxy").

## 비교

| | A. 더니어를 데이터 호스트에서, Tunnel + Access | B. 더니어를 Worker/Pages 로, API 만 Tunnel | C. 공개 issuer(ADR-0080 §6) | D. VPN(Tailscale 등) |
|---|---|---|---|---|
| BFF(토큰 보관) | 지금의 Rust 서버 그대로(ADR-0116 §1) | Rust BFF 를 Worker 로 다시 짓거나(wasm) TS 로 바꿔야 한다(ADR-0114 §4 와 충돌). 토큰이 엣지로 나간다 | A 와 같음 | 그대로 |
| Zitadel·Foundation 에 닿는 길 | 호스트 루프백(지금 노트북이 터널로 하던 것과 같은 주소) | Tunnel 로 Zitadel·Foundation 둘 다 공개해야 한다 | Zitadel 을 공개 | VPN |
| 토큰 `iss` | 바뀌지 않는다(코드 교환이 루프백) | 바뀐다 | 바뀐다 — Foundation·모든 워크로드의 검증을 함께 옮겨야 한다 | 그대로 |
| 휴대폰 | 브라우저만 | 브라우저만 | 브라우저만 | 앱 설치·기기 등록 |
| 공개되는 면 | 더니어 한 주소(Access 뒤). Zitadel 은 로그인 경로 여섯만, 더니어를 거쳐 | 더니어 + Zitadel + API | Zitadel 전체 | 없음(사설망) |
| 배포 길 | 서명된 릴리스의 이미지(ADR-0276) — 새 배포 길 없음 | Worker 자동배포(ADR-0175)에 BFF 추가 + 서버 터널 | — | — |

**A 를 고른다.** B 는 토큰을 엣지로 옮기고 Tunnel 로 내부 API 둘을 공개해야 하며, BFF 를 다시 짓는다. C 는 issuer 하나를 믿는 모든
검증(Foundation·identity 워크로드)을 함께 옮기는 별도 결정이다(ADR-0080 §6) — A 는 그것 없이 된다. D 는 신원이 아니라 망으로 막고,
휴대폰에 앱과 기기 등록이 필요하다.

## Decision

1. **더니어는 데이터 호스트에서 돈다.** `dawneer-server` 는 호스트 루프백 `127.0.0.1:3120` 에서 듣고, Zitadel(`127.0.0.1:18453`)과
   Foundation API(`127.0.0.1:18080`)를 루프백으로 부른다. 이미지는 CI 가 서명한 릴리스에 든다(ADR-0276: 서버는 빌드하지 않는다).
   main 이 바뀌면 다른 런타임 서비스처럼 릴리스가 따라간다. 공개 주소는 `https://console.perfectory.io` 하나다.

2. **브라우저는 Zitadel 에 직접 가지 않는다. 더니어가 로그인 화면을 자기 주소에서 중계한다.**
   - 중계하는 경로는 여섯뿐이다: `/oauth/v2/authorize`(와 그 `/callback`), `/ui/login`(과 그 아래), `/oidc/v1/end_session`.
     Zitadel 의 관리 콘솔(`/ui/console`)과 API 는 중계하지 않는다 — 호스트 루프백에만 있다.
   - 중계는 Zitadel 의 문서화된 프록시 계약을 따른다: `Host` 는 인스턴스(issuer)의 것, `x-zitadel-public-host` 는 더니어의 주소,
     `x-forwarded-proto` 는 그 scheme. 인스턴스는 그 주소를 trusted domain 으로 갖는다 — `configure-zitadel.sh` 가
     `staff-console-applications.v1.json` 의 https redirect URI 의 호스트를 모두 trusted domain 으로 맞춘다(목록은 그 파일 하나).
   - 더니어의 세션 쿠키는 Zitadel 에 넘기지 않는다. Zitadel 화면의 보안 머리글은 그 화면의 것을 그대로 둔다(더니어의 CSP 를 덮으면
     로그인 스크립트가 막힌다).
   - **토큰은 중계로 나오지 않는다.** 코드 교환·갱신·userinfo 는 더니어 서버가 issuer 루프백에 직접 한다. 그래서 토큰의 `iss` 는
     지금과 같은 `http://127.0.0.1:18453` 이고 Foundation 의 검증은 바뀌지 않는다. discovery 의 `authorization_endpoint`·
     `end_session_endpoint` 만 더니어 주소로 옮긴다.
   - Zitadel 앱 `dawneer` 의 redirect 는 `https://console.perfectory.io/auth/callback` 이고 개발 모드(http 허용)는 끈다. 노트북 루프백
     주소는 은퇴한다 — 노트북에는 client secret 을 더 복사하지 않는다.

3. **두 겹의 신원.**
   - 바깥: **Cloudflare Access**(self-hosted 앱 `console.perfectory.io`). 허용 정책은 이메일 목록이고 로그인 방법은 일회용 PIN(메일)이다.
     세션 24시간. 목록에 없는 사람은 엣지에서 막혀 더니어에 요청이 닿지 않는다. 이메일 목록은 공개 저장소에 적지 않는다 — 소유자가
     Access 정책에 넣는다.
   - 안: **Zitadel** 비밀번호 + **2차 인증 필수**(`staff-login-policy.v1.json` `force_mfa=true`). 2차 인증이 없는 직원은 다음 로그인에서
     OTP 앱 또는 보안 키 등록을 요구받는다. 권한(역할)은 지금처럼 신원 플랫폼이 판정한다.
   - 서버에는 들어오는 포트가 생기지 않는다. `cloudflared` 는 바깥으로만 연결을 연다.

4. **Tunnel 은 엣지가 설정을 갖고, 서버는 토큰 하나만 갖는다.** Cloudflare 가 설정을 관리하는 터널(`config_src=cloudflare`)이고,
   공개 호스트 `console.perfectory.io` → `http://127.0.0.1:3120` 하나, 나머지는 404 다. 서버의 `cloudflared` 는 터널 토큰 하나로
   돈다. 토큰은 런타임 비밀 계약의 선택 그룹 `console-tunnel` 파일(root 0600)에만 있고, 파일이 없으면 `cloudflared` 를 띄우지 않는다
   (ADR-0175 §7 의 켜고 끄기와 같다). 노트북에는 새 장기 비밀이 생기지 않는다.

5. **감사는 지금처럼 쌓이기만 한다.** 승인·결정은 Foundation API 를 거쳐 그 주인의 append-only 기록에 남는다(ADR-0433 §2). 더니어는
   기록을 갖지 않는다. 엣지(Access 로그인·차단)와 Zitadel(로그인·2차 인증 이벤트)은 각자 자기 감사 기록을 쌓는다.

6. **휴대폰.** 더니어 화면은 휴대폰 폭(360 CSS px)에서 가로 스크롤 없이 승인 탭까지 쓸 수 있어야 한다. Zitadel 로그인 화면은 Zitadel
   의 것(반응형)이다.

7. **엣지 설정의 코드화는 ADR-0423 의 기계에 붙는다.** Tunnel·DNS·Access 앱·정책은 OpenTofu `cloudflare/cloudflare` 프로바이더로
   ADR-0423 과 같은 상태 버킷·적용 단계에서 관리하는 것이 목표다. ADR-0423(제안)의 기계가 아직 없으므로, 첫 판은 소유자가 아래 한 번의
   설정으로 만들고, 그 기계가 생기면 `tofu import` 로 옮긴다. 엣지 설정은 이 ADR 이 적은 값(호스트 하나, 서비스 하나, 정책 하나)이 전부다.

## 소유자가 한 번 하는 일

PowerShell 기준이고 명령에 `$` 가 없다. 비밀값은 소유자만 보고, 에이전트에게는 "했다"만 알린다.

1. **Zero Trust 켜기.** Cloudflare 대시보드 → **Zero Trust** → 팀 이름 정하기 → **Free** 요금제(50명까지 0원).
2. **Access 앱.** Zero Trust → **Access → Applications → Add an application → Self-hosted** → 이름 `dawneer`, 도메인
   `console.perfectory.io`, 세션 24h → 정책 `staff` (Action **Allow**, Include **Emails** = 본인 메일) → 로그인 방법 **One-time PIN**.
3. **터널.** Zero Trust → **Networks → Tunnels → Create a tunnel → Cloudflared** → 이름 `perfectory-console` → 설치 화면의 토큰을
   복사(명령은 실행하지 않는다) → **Public Hostname**: `console` . `perfectory.io`, Service `HTTP` `127.0.0.1:3120` → 저장.
4. **토큰을 서버에.** PowerShell 에서(토큰은 화면·기록에 남지 않게 붙여 넣고 Enter, 그다음 Ctrl+D):
   `ssh -t ai-server "umask 077; cat > /tmp/console-tunnel.env; sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set console-tunnel /tmp/console-tunnel.env; rm -f /tmp/console-tunnel.env"`
   붙여 넣을 한 줄: `TUNNEL_TOKEN=<복사한 토큰>`.
5. **Zitadel 맞추기.** `ssh -t ai-server "cd ~/identity-platform/current && bash infra/zitadel/configure-zitadel.sh"` — `updated console app
   dawneer`, `created trusted domain console.perfectory.io`, `updated organization login policy` 가 찍힌다.
6. **확인(휴대폰).** `https://console.perfectory.io` → Access 메일 PIN → Zitadel 로그인 → 처음 한 번 OTP 앱 등록 → 더니어 → 승인 탭.

## Consequences

- 사장은 휴대폰 브라우저로 승인 단추를 누른다. SSH·터널·노트북 비밀이 필요 없다.
- 비용: Zero Trust Free(≤50명) 0원, Tunnel 0원, DNS 0원. 호스트: 더니어 컨테이너(상한 32 MiB, 실측 기대 약 10 MiB)와 `cloudflared`(상한 64 MiB). 호스트 메모리 예산(62 GiB)이 꽉 차 있어 신원 다리의 socat 두 개 상한을 64→32 MiB(실측 약 1 MiB)로 줄여 자리를 냈다 — 예산 62.00 / 62.00 GiB.
- 세션 저장소는 아직 메모리다(ADR-0116 §7). 한 호스트·한 프로세스라 맞지만, 더니어 컨테이너가 바뀌는 배포마다 더니어 세션이 끝난다.
  Access(24h)와 Zitadel 의 SSO 쿠키는 남으므로 다시 들어오는 데는 탭 하나(필요하면 2차 인증)다. 여러 벌로 뜨면 Postgres 로 옮긴다.
- 더니어 서버는 Access 의 JWT(`Cf-Access-Jwt-Assertion`)를 아직 직접 확인하지 않는다. 원점(origin)은 루프백에만 있고 공개 통로는 Tunnel
  하나라서, Access 를 거치지 않은 요청은 호스트 안에서만 온다. 원점 확인(Cloudflare 권고)은 후속이다.
- `force_mfa` 는 조직 정책이라 DataHub·Airflow 로그인에도 2차 인증이 걸린다(같은 직원, 같은 조직).
- 노트북 실행(ADR-0116 §5)은 은퇴한다. 개발은 로컬 Zitadel 과 `bff.rs` 의 가짜 issuer 로 한다.
- 남는 부채: 엣지 설정 코드화(§7, ADR-0423 의 기계), Access JWT 원점 확인, 세션 저장소의 영속화.
- 출처: [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/),
  [Access self-hosted applications](https://developers.cloudflare.com/cloudflare-one/applications/configure-apps/self-hosted-public-app/),
  [Zero Trust plans](https://www.cloudflare.com/plans/zero-trust-services/),
  [Zitadel — Login app OIDC proxy·Trusted Domains](https://zitadel.com/docs/guides/integrate/login-ui/login-app),
  Zitadel v2.65.1 `internal/api/http/middleware/origin_interceptor.go`(공개 호스트·scheme 을 읽는 순서), Google BeyondCorp(“BeyondCorp: A New
  Approach to Enterprise Security”, 2014).
