# ADR 0249: 임시 읽기 전용 웹 미리보기는 무료 Worker 하나에 산다

- Status: Accepted
- Date: 2026-10-14

## Context

사장은 노트북에서 아무것도 띄우지 않고 우리 파이프라인 데이터(지도 타일, 필지·건물 패널)를 공짱 화면으로 보고 싶다.
이 미리보기는 임시이며 나중에 Cloudflare 에서 내린다. 조건은 돈 0(무료 플랜), AWS 운영 공짱 무접촉, 운영 신원
무사용, 남는 것 없이 지우는 명령 하나다.

실측으로 확인한 제약:

1. 화면은 모두 Zitadel 로그인과 Redis 세션을 요구한다. Workers 무료 플랜에 Redis 는 없고 운영 신원은 쓰지 않는다.
2. 정적 export 는 안 된다: `proxy.ts`, route handler, `force-dynamic` 레이아웃이 있다. 남는 길은
   `@opennextjs/cloudflare` 로 Workers 에 올리는 것이다. OpenNext 는 Windows 에서 `sharp` 를 묶지 못해 빌드는 Linux 에서 한다.
3. 지도가 그릴 단위를 알려주는 `GET /catalog/v1/vector-tiles/runtime-manifest` 는 데이터 호스트 안의
   foundation-api 만 답한다(공개 엣지는 `foundation-runtime-manifest-edge-policy.v1.json` 에서 planned).
4. 공개 엣지(tiles·catalog·buildings·unit-prices·map-edits)는 허용 Origin 밖이면 403 이다. Origin 이 없으면 200 이다.
5. 매물 목록·마커는 Rust API 가 필요하다. 미리보기에는 없다.
6. 네이버 지도 키는 등록된 Web 서비스 URL 에서만 인증된다(지금 키는 localhost 만).

## Decision

1. **앱 변경은 플래그 하나다.** `NEXT_PUBLIC_GONGZZANG_READ_ONLY_PREVIEW=1` 로 빌드했을 때만 `proxy.ts` 가 세션·속도
   제한(둘 다 Redis)에 닿지 않고 지도 페이지(`/listings`) 하나를 열며, 그 밖의 경로는 404 다. 인증 레이아웃도
   같은 조건에서만 세션을 건너뛴다(`apps/web/lib/preview.ts`). Next 는 `NEXT_PUBLIC_*` 를 빌드 때 박으므로 이 값이
   없는 운영 빌드에는 우회로가 없다. 신원·세션 값은 `.invalid` 호스트와 일회용 난수뿐이다.
   같은 플래그로 `instrumentation.ts` 는 OpenTelemetry SDK 를 띄우지 않는다. 미리보기에는 수집기가 없고, Next 16 은
   instrumentation 을 미들웨어 묶음에 넣으므로 SDK(gRPC 포함)를 빼야 무료 한도에 들어간다. 실측 압축 크기: 그대로
   3,522 KiB, minify 3,095 KiB(둘 다 한도 3,072 KiB 초과), minify+SDK 제외 2,499 KiB. 플래그는 함수 호출이 아니라
   `process.env` 비교로 적어야 번들러가 접어서 SDK import 를 지운다(함수로 감쌌을 때는 3,098 KiB 로 그대로였다).
2. **Cloudflare 에 만드는 것은 Worker `gongzzang-web-preview` 하나다**(workers.dev, 무료). KV·D1·R2·시크릿·커스텀
   도메인·라우트는 없다. 운영 게이트웨이 설정도 바꾸지 않는다: 브라우저는 미리보기에 같은 출처로 묻고, Worker 가
   Origin 없이 엣지에 중계한다(`scripts/web-preview/edge-relay.mjs`, GET·HEAD 만, 쿠키·Origin 은 넘기지 않는다).
   엣지 호스트는 `r2-connections.contract.json` 의 `public_hostname` 이고 시험이 어긋남을 잡는다.
3. **타일 목록은 배포 때 한 번 찍는다.** `deploy.sh` 가 데이터 호스트에서 runtime-manifest 를 받아 스키마 2 를
   확인하고, 각 단위의 타일 주소를 `/edge/tiles` 로 바꿔 정적 자산으로 올린다. 릴리스는 불변이라 스냅샷은 유효하다.
   지도가 옛 릴리스를 보이면 다시 배포한다.
4. **경로는 하나다.** `products/gongzzang/scripts/web-preview/deploy.sh [ref]`(기본 `origin/main`)가 빌드(Docker 안
   Linux), 무료 한도 3 MiB 압축 크기 검사, 배포, 실주소 확인까지 한다. `teardown.sh` 가 Worker 를 지우고
   API·주소 양쪽에서 남은 것이 없음을 확인한다. 인증은 실행하는 사람의 wrangler 로그인이다. 저장소에도 GitHub 에도
   Cloudflare 토큰을 새로 두지 않는다. PR 마다 띄우는 CI 는 만들지 않는다(임시용이고 CI 시간 예산을 쓰지 않는다).
5. OpenNext·wrangler 는 빌드 컨테이너 안에서만 설치한다. `apps/web/package.json` 과 락파일은 그대로다.

## 증명 (2026-10-14)

- `deploy.sh`: 압축 2,499 KiB 로 배포. `/listings` 200, 매니페스트 200(세 단위 모두 `/edge/tiles`), 행정경계 타일
  중계 200(MVT), 필지·건물 by-PNU 중계 200, `/profile`·`/api/proxy/*` 404. 브라우저에서 `?p=parcel:<pnu>.summary`
  로 필지 패널(지목·용도지역·공시지가)이 실데이터로 그려짐. 네이버 인증은 URL 등록 전이라 401.
- `teardown.sh`: Worker 삭제 후 API 에 Worker·KV·D1·R2 흔적 없음, 주소 404. 다시 배포해 미리보기를 살려 둠.
- 크기 한도 검사는 실제로 두 번 배포를 막았다(3,522·3,095 KiB).

## Consequences

- 미리보기는 지도 폴리곤과 필지·건물·호 가격 패널을 보여 준다. 매물 목록·마커·로그인·알림은 비어 있거나 404 다.
- 네이버 콘솔에 미리보기 주소를 등록하기 전에는 배경지도가 인증 실패로 사라지고 그 위의 폴리곤도 그려지지 않는다.
  패널은 주소의 `?p=` 로 열 수 있다. 등록과 해제는 사장 몫이다(`teardown.sh` 가 마지막에 알려 준다).
- Workers 무료 한도(하루 10만 요청, 요청당 CPU 10 ms)는 혼자 보는 미리보기에 충분하다. 타일 중계도 요청으로 센다.
- 내릴 때 남는 것: 네이버 콘솔의 URL 한 줄(사람이 지움), 그리고 이 ADR·스크립트·플래그 분기 코드. 코드를 지울 때는
  `lib/preview.ts`, `proxy.ts` 의 `readOnlyPreviewProxy`, 레이아웃·`instrumentation.ts` 의 분기, `scripts/web-preview/`, 시험 둘을 함께 지운다.
