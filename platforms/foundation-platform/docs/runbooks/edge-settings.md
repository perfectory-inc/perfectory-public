---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 더니어 엣지 설정 — Access·Tunnel·DNS 를 호스트가 코드로 맞춘다

[루트 ADR-0436](../../../../docs/adr/0436-the-consoles-cloudflare-edge-is-opentofu-applied-by-the-host-with-the-existing-account-token-and-the-allowlist-is-the-identity-staff-list.md).
`foundation-edge-settings.service` 가 `infra/edge/main.tf` 를 OpenTofu 로 적용한다. 평상시 사람이 하는 일은 없다.

## 무엇이 언제 도나

- 호스트가 main 을 돌면 `foundation-autodeploy.sh` 가 Worker 배포와 함께 이 유닛을 시작한다(Worker 배포 뒤에 선다).
- 모듈·`compose.console.yml`·`staff-console-applications.v1.json`·직원 목록·계정 id 가 마지막 성공과 같고 24시간이 안 지났으면 아무것도
  하지 않는다. 그 밖에는 `tofu apply` 한 번. 대시보드에서 바꾼 것은 다음 적용이 되돌린다.
- 터널 토큰이 바뀌면 `console-tunnel` 파일에 쓰고 `foundation-release.sh console` 로 더니어와 `cloudflared` 를 띄운다.

## 직원 넣고 빼기

Access 허용 목록 = `identity.staff` 중 역할을 하나 이상 가진 사람의 메일. 역할을 주거나(`POST /identity/v1/staff/{id}/roles`) 모두
거두면 다음 틱에 엣지가 따라간다. 대시보드에서 메일을 넣지 않는다 — 다음 적용이 지운다.

## 확인 (읽기만)

```bash
journalctl -u foundation-edge-settings.service --since -1d --no-pager | tail
systemctl show -p Result foundation-edge-settings.service
```

`applied <commit> (N staff on the allowlist)` 가 성공이다. 실패 기록은 `/data/foundation-platform/edge-settings/failed.json`
(같은 입력으로 세 번 실패하면 멈춘다; 입력이 바뀌거나 그 파일을 지우면 다시 한다).

## 끄기

`/etc/foundation-platform/edge-settings.off` 를 만들면 엣지 적용만, `autodeploy.off` 면 배포 전체가 멈춘다.

## 상태와 passphrase

- 상태: R2 `foundation-platform-edge-settings-state-prod` / `edge/terraform.tfstate`, 잠금은 같은 키의 `.tflock`(조건부 PUT).
- 암호화 passphrase: `/etc/foundation-platform/edge-settings.env`(root 0600), 실행기가 처음 한 번 만든다.
- **passphrase 를 잃었을 때**: 상태를 읽을 수 없다. 상태 객체를 지우고(`edge/terraform.tfstate`) passphrase 줄을 지우면 다음 적용이
  새 passphrase 로 다시 만든다. 조직은 `import` 블록이 다시 받는다. 터널·앱·정책·DNS 는 이미 있으므로 새로 만들다가 이름 충돌로 실패한다 —
  그때는 운영자가 한 번 `tofu import` 로 받는다(리소스 id 는 대시보드나 API 목록에서): 사고 대응이고 평상시 단계가 아니다.

## 토큰

Worker 배포와 같은 계정 토큰(`cloudflare-deploy` 그룹)이다. 교체가 필요할 때의 권한 목록과 서버에 넣는 길은 ADR-0436 "소유자가 하는 일"에 있다.

## 서울 앞문 (CloudFront, 루트 ADR-0461)

[루트 ADR-0461](../../../../docs/adr/0461-korean-visitors-enter-through-a-cloudfront-front-door-in-seoul-in-a-separate-aws-account-applied-by-the-host.md).

같은 유닛이 콘솔 엣지 뒤에 `infra/cdn/main.tf` 를 적용한다. 정본은 `config/cdn-front-door.contract.json`.
지금(2026-11-10) 문 셋 `tiles`·`catalog`·`web_preview` 가 모두 `answered_by: cloudfront` 다. 전달 경로·압축·캐시 수명의
모양은 [전달 계층](../../../../docs/architecture/edge-delivery.md).

- **꺼져 있음**: `/etc/foundation-platform/aws-cdn.env` 에 별도 AWS 계정의 적용 키가 없으면 로그에 `front door off` 한 줄뿐이다.
  키를 넣는 길은 ADR-0461 "소유자가 하는 일" 2.
- **성공**: `applied <commit> (front door)` 와 문마다 `front door <문>: dXXXX.cloudfront.net`. 기록은
  `/data/foundation-platform/edge-settings/cdn/applied.json`·`failed.json`(콘솔과 따로).
- **전환**: 계약의 문 `answered_by` 를 `cloudfront` 로 바꾸는 PR 하나(문 하나씩). Worker 배포가 공개 이름을 Worker 도메인에서 빼고, 이 적용이
  DNS-only CNAME 을 만든다.
- **되돌리기**: 그 줄을 `cloudflare` 로 되돌리는 PR. 급하면 Cloudflare 대시보드 → Workers → 그 Worker → Settings → Domains & Routes →
  Add → Custom domain 에 공개 이름(예: `tiles.perfectory.io`) — 기존 CNAME 을 덮고 1분 안에 Cloudflare 가 다시 답한다. 그 뒤 계약도
  되돌린다(안 그러면 다음 적용이 CNAME 을 다시 만든다).
- **실측**(읽기만, 노트북):
  `python platforms/foundation-platform/scripts/ops/korea-edge-measure.py --sample <샘플.json> --cloudfront tiles=<배포이름> --cloudfront catalog=<배포이름> --globalping 12`
  샘플은 저장소 밖 로컬 파일(`{"tiles": ["/<source>/<z>/<x>/<y>"], "catalog": ["/parcels/by-pnu/<pnu>"]}`). 전환된 문은
  `--cloudfront` 가 필요 없다.
- **캐시 수명**: 불변 URL(`immutable`)은 1년, 매니페스트·색인(`no-cache`)은 매번, 같은 주소의 답이 바뀌는 by-PNU 문서·패치 타일은
  브라우저 1시간·CloudFront `s-maxage=60`(루트 ADR-0470, 가드 `orchestration/tests/test_cdn_shared_cache_lifetime.py`). 무효화는 쓰지
  않는다. 발행 직후 문 뒤의 옛 답은 최대 약 2분(CloudFront 60 s + Worker 매니페스트 60 s).
- **압축 확인은 `Accept-Encoding` 을 붙여서**: `curl -s -o /dev/null -D - -H 'Accept-Encoding: br, gzip' https://tiles.perfectory.io/<타일>`
  을 두 번(Miss → Hit), 둘 다 `Content-Encoding: br|gzip` 이어야 한다. 헤더 없이 보낸 `curl` 은 압축을 원하지 않는다는 뜻이라 identity
  로 답하는 것이 옳다(2026-11-09 의 "압축 없음 539,454 B" 오판, ADR-0470 Context).
- **비용 알림**: 예산 `perfectory-cdn-free-tier` 가 실제 지출 $0.01 을 넘으면 직원 메일로 온다. 오면 콘솔 Billing → Bills 에서 서비스별로 본다.
