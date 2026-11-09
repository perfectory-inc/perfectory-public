# ADR 0460: 한국 방문자는 서울의 CloudFront 앞문으로 들어온다 — 별도 AWS 계정, 호스트가 OpenTofu 로 적용, 되돌리기는 DNS 한 줄

- Status: Accepted
- Date: 2026-11-09
- Builds on: [ADR-0436](./0436-the-consoles-cloudflare-edge-is-opentofu-applied-by-the-host-with-the-existing-account-token-and-the-allowlist-is-the-identity-staff-list.md)(엣지 설정은
  OpenTofu, 호스트가 적용, 상태는 R2 에 잠금·암호화),
  [ADR-0423](./0423-github-repository-settings-are-opentofu-state-in-r2-applied-by-the-host-with-a-scoped-token.md)(범위 좁은 키 하나),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)·[ADR-0247](./0247-credential-variable-names-are-derived-from-the-cloudflare-token-name.md)(런타임 비밀 계약),
  [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md)(타일 Worker 의 캐시 헤더),
  [ADR-0249](./0249-a-temporary-read-only-web-preview-lives-in-one-free-worker.md)(임시 웹 미리보기)

## Context

W43(2026-11-09 08:35–09:10Z) 이 원인을 쟀다. 코드가 아니라 **우리 Free 존에 배정된 Cloudflare 주소 대역**이다.

| 측정(노트북 KT AS4766, 각 5회) | 경로 | 입장 | TTFB | 전체 |
|---|---|---|---|---|
| parcels z14 큰 타일 683,030 B(캐시 적중) | 존 주소 104.21.x | LAX | 0.91–3.15 s | 6.9–18.7 s |
| 〃 | 다른 Cloudflare 주소 104.18.x | ICN | 0.06–0.26 s | 0.08–0.29 s |
| by-PNU 문서 883 B | 존 주소 | LAX | 0.98–2.73 s | 같음 |
| 〃 | 104.18.x | ICN | 0.10–0.14 s | 같음 |

Globalping 한국 탐침 12개: KT → LAX(TTFB 359–1,052 ms), SK브로드밴드 → HKG(66–264 ms), LG U+ → HKG/NRT, 데이터센터 회선만
ICN. 같은 탐침에서 **CloudFront 는 12/12 가 ICN80**(4–44 ms), Bunny 는 11/12 가 KR1. Pro 요금제는 2026 커뮤니티 보고상 KT 경로를 고치지
못했고, 서울 입장을 보장하는 것은 Enterprise(견적) 뿐이다. 이 ADR 을 쓰며 다시 쟀다(2026-11-09 12:1xZ, 노트북 KT):
`catalog.perfectory.io/parcels/by-pnu/_capabilities` 72 B 가 존 주소로 LAX, TTFB 0.86–1.54 s; 같은 요청을 `--connect-to` 로 ICN 주소에
붙이면 0.095–0.117 s. Globalping KR 6개: KT 1개 LAX 425 ms, 데이터센터 5개 ICN 62–67 ms.

사장은 CloudFront 쪽으로 기울었다(W44 지시). 조건: **gongzzang 의 AWS 계정은 건드리지 않는다**, 새 별도 계정이 생기기 전에는 아무것도
만들거나 적용하지 않는다, 무료 한도에서 비용 알림, 되돌리기는 DNS 를 Cloudflare 로 돌리는 것.

대기업은 이렇게 한다. 여러 CDN 을 앞에 세우고 원점은 그대로 두는 것(multi-CDN, origin 분리)은 Netflix·Shopify·Wikimedia 가 쓰는 표준
모양이다. CDN 을 Terraform 으로 관리하고 병합이 곧 적용인 것은 GitLab 의 공개 인프라 저장소가 그 예다. AWS 는 CloudFront 앞에 다른 CDN 을
원점으로 두는 구성을 custom origin 으로 지원하고, 캐시는 원점의 `Cache-Control` 을 따르게 하는 것(`min_ttl=0`)을 권한다. 별도 계정은 AWS
Organizations 의 "작업마다 계정을 나눈다"(blast radius) 권고 그대로다.

## 비교

| | A. CloudFront 별도 계정, 호스트가 OpenTofu | B. Bunny | C. Cloudflare Enterprise |
|---|---|---|---|
| 한국 입장(실측) | ICN80 12/12 | KR1 11/12, JP1 1 | ICN(cloudflare.com) |
| 비용 | 상시 무료 1 TB·1천만 요청/월, 넘으면 한국 $0.120/GB + $0.012/만 요청 | $0.03/GB, 최소 $1/월 | 견적(2017 서울 $5,000+/월, 2026 미확인) |
| 코드로 관리 | `hashicorp/aws` 프로바이더(공식) | 커뮤니티 프로바이더 | 이미 있음 |
| 우리 실행기와 | ADR-0436 실행기를 그대로 넓힌다 | 같음 | 변화 없음 |
| 사장 단계 | 계정 1, 키 1 | 계정 1, 키 1 | 영업 계약 |

**A 를 고른다.** B 는 2순위로 남긴다(같은 계약 모양에 프로바이더만 바꾸면 된다). C 는 병행해서 견적만 받는다(사장 결정).

## Decision

1. **계약 하나.** `platforms/foundation-platform/config/cdn-front-door.contract.json` 이 앞문의 정본이다. 문(door) 셋:
   - `tiles` — `vector_tile_gateway`(공개 `tiles.perfectory.io`), 원점 `tiles-origin.perfectory.io`.
   - `catalog` — `parcel_by_pnu_gateway`(공개 `catalog.perfectory.io`), 원점 `catalog-origin.perfectory.io`.
   - `web_preview` — 임시 웹 미리보기(ADR-0249), 공개 `web-preview.perfectory.io`, 원점은 그 Worker 의 workers.dev 주소(계정의
     workers.dev 하위 도메인은 적용 때 API 로 읽는다, 저장소에 적지 않는다).
   공개 주소는 R2 연결 계약(`r2-connections.contract.json`, 정의에서 생성)에서 읽고 복사하지 않는다. `gongzzang.app` 별칭은 이 결정 밖이다
   (그 존은 손대지 않는다).
2. **원점은 별도 호스트 이름이다.** 공개 이름이 CloudFront 를 가리키면 원점이 같은 이름일 수 없다(자기 자신을 부른다; 모듈의
   precondition 이 막는다). 타일·필지 Worker 의 wrangler 렌더러가 계약의 `origin_hostname` 을 custom domain 으로 붙인다 — 배포가 붙이는
   도메인은 계약이 이름한 것과 정확히 같다는 기존 규칙 그대로. CloudFront 는 AWS 서울 리전 망에서 원점을 부르므로 데이터센터 회선처럼
   Cloudflare ICN 으로 들어간다(위 실측의 데이터센터 행).
3. **캐시는 원점이 정한다.** 캐시 정책 하나(`perfectory-origin-cache-control`): `min_ttl=0`, `default_ttl=0`, `max_ttl=31536000`.
   불변 주소(타일·by-PNU 세대·미디어·프로필의 `public, max-age=31536000, immutable`)는 1년, 매니페스트·색인(`no-cache, max-age=0`)은
   매번 다시 묻고, 패치 타일·by-PNU 최신 문서(`max-age=3600`)는 한 시간이다. `Cache-Control` 이 없는 답은 저장하지 않는다. 값을 HCL 에
   복사하지 않으므로 계약(r2-connections)의 캐시 값이 바뀌어도 앞문은 따라간다. 키에는 `Origin`·`Access-Control-Request-Method`·
   `Access-Control-Request-Headers`(Worker 가 `Vary: Origin` 이고 목록 밖 Origin 을 403 하므로), 쿠키·쿼리는 없음(타일 Worker 는 쿼리가
   있으면 404, by-PNU 는 쿼리를 받지 않는다). 미리보기는 Next.js 앱이라 통과(Managed-CachingDisabled + AllViewerExceptHostHeader)하고
   `/_next/static/*`(내용 해시 이름)만 캐시한다.
4. **전송.** `PriceClass_200`(한국을 포함하는 가장 싼 등급), HTTP/2·HTTP/3, IPv6, 압축(brotli·gzip, Accept-Encoding 정규화 — Worker 가
   이미 gzip 으로 준 답은 그대로), 뷰어 TLS 1.2_2021 SNI, 원점 HTTPS 전용 TLS 1.2, 원점 keep-alive 60 s(쿼터 요청 없는 최대),
   읽기 30 s. 모든 답에 `Server-Timing`(PoP·적중)을 붙인다 — 검증이 읽는다.
5. **인증서·DNS.** 문마다 ACM 인증서(us-east-1, DNS 검증). 검증 레코드는 기존 Cloudflare 존에 DNS-only 로 만든다. 공개 이름의
   전환 레코드도 같은 존의 **DNS-only CNAME → 배포 이름**(TTL 60)이다. Route 53 은 쓰지 않는다.
6. **스위치와 되돌리기.** 문마다 `answered_by`:
   - `cloudflare`(지금 `tiles`·`catalog`) — 공개 이름은 Worker custom domain 그대로. 배포·인증서는 미리 만들어지고 공개 이름으로 TLS 를
     받으므로 전환 전에 `--connect-to` 로 실물을 잰다.
   - `cloudfront` — 렌더러가 공개 이름을 Worker 도메인에서 빼고(Wrangler 의 비대화식 배포는 설정에 없는 custom domain 을 지운다,
     `override_scope`), Worker 배포 **뒤에** 서는 엣지 적용이 DNS-only CNAME 을 만든다.
   - **되돌리기 = 그 줄을 `cloudflare` 로.** 다음 Worker 배포가 공개 이름을 custom domain 으로 되찾고(비대화식 Wrangler 는
     `override_existing_dns_record` 로 CNAME 을 덮는다), 엣지 적용은 사라진 레코드를 상태에서 잊는다. 급할 때는 대시보드 Workers → 그
     Worker → Domains → Add 한 번이 같은 일을 1분 안에 한다(사고 대응).
   - 전환 PR 은 문 하나씩, 계정·키가 생기고 첫 적용·실측이 끝난 뒤. 전환 전에 바꿀 것(아래 Consequences): by-PNU 카나리·지연 관문이 버전 고정 헤더로
     읽는 주소를 원점 이름으로.
7. **비용 알림.** AWS Budgets 하나(`perfectory-cdn-free-tier`, 월 $1.00): **실제 지출 $0.01 초과**(AWS 의 zero-spend 템플릿)와 예측
   100% 초과에 메일. 받는 사람은 콘솔 허용 목록과 같은 신원 플랫폼 직원 목록(역할 있는 사람, 최대 10)이다 — 메일 주소를 저장소에 두지 않는다.
8. **적용은 호스트가 한다(ADR-0436 실행기를 넓힌다).** `foundation-edge-settings.service` 의 `edge_settings.py` 가 콘솔 엣지 뒤에
   `infra/cdn` 을 같은 규칙으로 적용한다: 바뀐 것만(모듈·앞문 계약·문이 읽는 공개 이름·직원 목록·키 id 해시), 하루 한 번 표류 되돌리기,
   같은 입력 세 번 실패 뒤 대기, 기록은 따로(`/data/foundation-platform/edge-settings/cdn/`). 상태는 같은 R2 버킷의 `cdn/terraform.tfstate`,
   같은 passphrase 로 암호화. 콘솔 쪽 실패가 앞문을 막지 않고 그 반대도 아니다. AWS 쪽은 별도 계정의 키로, Cloudflare 존은 기존 계정
   토큰(DNS 편집, ADR-0436 실측)으로.
   - **키가 없으면 아무것도 하지 않는다.** 런타임 비밀 그룹 `aws-cdn`(`/etc/foundation-platform/aws-cdn.env`, root 0600, 선택)에
     `FOUNDATION_PLATFORM_AWS_CDN_ACCESS_KEY_ID`·`…_SECRET_ACCESS_KEY` 가 생기기 전까지 로그에 "front door off" 한 줄뿐이다. 실행기가
     파일을 직접 읽어 OpenTofu 변수로 넘긴다 — `AWS_*` 환경 이름은 R2 상태 버킷의 것으로 남는다.
   - 키의 권한은 `infra/cdn/apply-policy.json` 뿐이다: CloudFront 배포·캐시/응답 헤더 정책, us-east-1 의 ACM, 이름이 정해진 예산 하나.
     IAM·S3·EC2·결제 쓰기는 없다.
9. **검증.** `scripts/ops/korea-edge-measure.py --sample <로컬 파일> [--cloudfront 문=배포이름] [--globalping 12]` 가 같은 읽기를
   Cloudflare 경로(원점 이름)와 CloudFront 경로(공개 이름, 전환 전에는 `--connect-to` 로 배포에 붙인다)로 재고, 입장 PoP(`x-amz-cf-pop` /
   `cf-ray` 끝)·적중·인코딩·연결·TTFB·전체·처리량을 표로 낸다. Globalping 한국 탐침(계정 없음)으로 통신사별 입장과 첫 바이트도 낸다.
   샘플 파일(W43 의 parcels z14 타일·zoning z11 타일·by-PNU 문서 하나)은 실제 PNU 를 담으므로 저장소에 두지 않는다.

## 소유자가 하는 일 (두 번, 한 번씩)

1. **AWS 계정 만들기.** <https://signup.aws.amazon.com> → 새 이메일 주소(공짱 계정과 다른 주소)로 가입 → 결제 수단(카드) 등록 → 본인 확인
   → 지원 플랜 **Basic(무료)**. 루트 계정에 MFA 를 켠다(오른쪽 위 계정 이름 → Security credentials → Assign MFA). 공짱 AWS 계정에는
   아무것도 하지 않는다.
2. **적용 키 하나 만들어 서버에 넣기.**
   1. 콘솔 위 검색 → **IAM** → 왼쪽 **Policies** → **Create policy** → **JSON** 탭 → 저장소의
      `platforms/foundation-platform/infra/cdn/apply-policy.json` 내용을 그대로 붙여 넣기 → 이름 `perfectory-cdn-apply` → Create.
   2. 왼쪽 **Users** → **Create user** → 이름 `perfectory-cdn-apply`(콘솔 접근 없음) → **Attach policies directly** →
      `perfectory-cdn-apply` 선택 → Create.
   3. 만든 사용자 → **Security credentials** → **Create access key** → 용도 **Other** → 만들기. 화면의 두 값을 아래 3번에 바로 붙이고
      창을 닫는다(채팅에 붙이지 않는다).
   4. 서버에 넣는 길은 이미 있는 런타임 비밀 경로 하나다. 값은 채팅·명령줄·셸 기록에 남지 않는다. PowerShell 에서 그대로:
      `ssh -t ai-server "umask 077; cat > /tmp/aws-cdn.env; sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set aws-cdn /tmp/aws-cdn.env; rm -f /tmp/aws-cdn.env"`
      붙여 넣을 두 줄:
      `FOUNDATION_PLATFORM_AWS_CDN_ACCESS_KEY_ID=<Access key>`
      `FOUNDATION_PLATFORM_AWS_CDN_SECRET_ACCESS_KEY=<Secret access key>` → Enter → Ctrl+D.
   5. "만들었다"고만 알린다. 그 다음은 에이전트가 한다: 이 PR 을 준비 상태로 → 병합 → 호스트가 배포·인증서·예산을 만든다(전환 0) →
      실측 → 문 하나씩 전환 PR.

## Consequences

- 계정·키가 생기기 전에는 이 PR 은 draft 로 남는다. 병합되면 Cloudflare 쪽에서 원점 이름 둘(custom domain)이 생기고, 키가 있으면 AWS 쪽에
  배포 셋·인증서 셋·캐시 정책·응답 헤더 정책·예산 하나, Cloudflare 존에 검증 레코드 셋과 `web-preview` CNAME 하나가 생긴다. 공개
  `tiles`·`catalog` 의 경로는 전환 PR 전까지 바뀌지 않는다.
- 첫 계획(2026-11-09, 노트북, OpenTofu 1.13.9 `tofu test` 모의 프로바이더로 이 계약과 생성된 R2 계약을 읽어): 배포 3, 인증서 3, 검증 레코드 3,
  전환 레코드 1(`web_preview`), 캐시 정책 1, 응답 헤더 정책 1, 예산 1. `tofu validate` 통과. 실계정 계획은 키가 생긴 뒤의 첫 적용이다.
- **비용(계산).** 출시 전 트래픽(W43: 측정 못 함, 분석 토큰 미사용)으로 CloudFront 상시 무료 1 TB·1천만 요청 안이면 $0/월. 넘으면
  한국 $0.120/GB + HTTPS $0.012/만 요청 — 예: 2 TB·2천만 요청이면 (1,024 GB × $0.120) + (1,000만 × $0.0000012) ≈ $122.9 + $12 = 약
  $135/월(정액 Pro $15/월 50 TB 가 그 전에 유리하다 — 그때 다시 본다). ACM 공인 인증서 $0, Budgets(동작 없는 예산) $0. 첫 1센트에 메일이 온다.
  Cloudflare 쪽: 원점 custom domain $0; CloudFront 가 캐시하므로 Worker 요청은 줄어든다.
- **전환 전에 할 일(전환 PR 의 조건).**
  - by-PNU 카나리·지연 관문(`worker_autodeploy.py`, `by-pnu-lane-cutover.sh`, 발행기의 health/latency/monitor)은 공개 이름에
    `Cloudflare-Workers-Version-Overrides` 를 붙여 버전을 고정해 읽는다. CloudFront 는 그 헤더를 키에도 원점에도 넘기지 않으므로 전환된 문에서는
    캐시된 답을 받는다 — 그 읽기는 원점 이름으로 옮긴다. 사용자 경험을 재는 모니터(엣지 탐침)는 공개 이름 그대로가 옳다.
  - 미리보기는 빌드 때 자기 주소(`deploy.sh` 의 `preview_origin`)를 앱에 굽는다. 미리보기가 `web-preview.perfectory.io` 로 열리게 하려면
    `deploy.sh` 가 앞문 계약의 이름을 쓰게 하고 그 Origin 을 Worker CORS 목록에 더해야 한다(지금은 workers.dev 주소 그대로 — 앞문은 서지만
    앱 안의 엣지 읽기는 아직 workers.dev 로 간다).
- **위험.** (1) Cloudflare 의 봇 차단이 AWS 주소를 막으면 원점 읽기가 실패한다 — 첫 적용 뒤 검증 스크립트가 바로 드러낸다. (2) 전환 순간
  Worker 배포가 공개 도메인을 지운 뒤 엣지 적용이 CNAME 을 만들 때까지(같은 배포 틱, 분 단위) 새로 묻는 사람은 이름을 못 찾는다 — 출시 전
  트래픽이라 감수하고, 문 하나씩 한다. (3) 키 하나가 앞문 전체를 바꿀 수 있다; 권한은 앞문 자원뿐이고 별도 계정이라 공짱과 섞이지 않는다.
  IAM Roles Anywhere(X.509)로 장기 키를 없애는 것은 CA 운영이 필요해 지금은 하지 않는다 — 키 교체 때 다시 본다.
- 출처: [CloudFront 요금](https://aws.amazon.com/cloudfront/pricing/),
  [가격 등급](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/PriceClass.html),
  [캐시 정책과 원점 헤더](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/controlling-the-cache-key.html),
  [원점 keep-alive](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/DownloadDistValuesOrigin.html#DownloadDistValuesOriginKeepaliveTimeout),
  [압축](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/ServingCompressedFiles.html),
  [Server-Timing](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/understanding-response-headers-policies.html#server-timing-header),
  [zero-spend 예산](https://docs.aws.amazon.com/cost-management/latest/userguide/budget-templates.html),
  [AWS 다계정 권고](https://docs.aws.amazon.com/whitepapers/latest/organizing-your-aws-environment/benefits-of-using-multiple-aws-accounts.html),
  [Wrangler custom domain](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/), W43 보고(2026-11-09).
