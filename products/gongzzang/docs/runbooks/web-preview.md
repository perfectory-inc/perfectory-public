---
status: current
owner: gongzzang-제품
doc_type: runbook
last_reviewed: 2026-10-14
---

# 임시 웹 미리보기 (읽기 전용)

결정: 루트 [ADR-0249](../../../../docs/adr/0249-a-temporary-read-only-web-preview-lives-in-one-free-worker.md).
Cloudflare 무료 플랜의 Worker `gongzzang-web-preview` 하나가 공짱 지도 페이지를 로그인 없이 보여 준다.
데이터는 공개 엣지(타일·필지·건물·호 가격·지도 편집)에서 온다. 매물 목록·마커·로그인은 없다.

## 올리기

```sh
npx wrangler login        # 한 번, 실행하는 사람의 Cloudflare 계정
products/gongzzang/scripts/web-preview/deploy.sh             # origin/main 을 올린다
products/gongzzang/scripts/web-preview/deploy.sh <git-ref>   # 다른 커밋
```

필요한 것: git, docker, node(npx), curl, 데이터 호스트 ssh(`ai-server`). 네이버 지도 client id 는 이
체크아웃의 `apps/web/.env.local` 에서 읽는다(없으면 `NAVER_MAPS_CLIENT_ID` 를 준다). 스크립트는 다음을 한다.

1. 계정의 workers.dev 서브도메인에서 주소를 정한다: `https://gongzzang-web-preview.<서브도메인>.workers.dev`.
2. `git archive` 한 트리를 Docker(Linux) 안에서 빌드한다. OpenNext·wrangler 는 컨테이너 안에서만 설치한다.
3. 데이터 호스트의 runtime-manifest 를 찍어 정적 자산으로 넣는다(타일 주소는 `/edge/tiles` 로).
4. 압축 크기가 무료 한도 3 MiB 를 넘으면 배포하지 않고 멈춘다.
5. 배포하고 `/listings` 200, 매니페스트 200, 엣지 중계 200, `/profile`·`/api/*` 404 를 확인한다.

지도가 옛 릴리스를 보이면(새 타일 발행 뒤) 같은 명령을 다시 돌린다.

## 네이버 지도 (사람이 한 번)

네이버 클라우드 콘솔 → Services → AI·NAVER API → Maps → Application → 공짱 웹 앱 → 수정 →
Web 서비스 URL 에 미리보기 주소를 추가한다. 등록 전에는 배경지도가 인증 실패로 사라지고 폴리곤도 그려지지 않는다.
패널은 주소의 `?p=` 로 열 수 있다.

## 내리기

```sh
products/gongzzang/scripts/web-preview/teardown.sh
```

Worker 를 지우고(버전·정적 자산·변수·workers.dev 주소가 함께 사라진다), Cloudflare API 에서 Worker·KV·D1·R2 에
이름이 남지 않았는지, 주소가 404 인지 확인한다. 하나라도 남으면 `LEFT:` 를 찍고 실패한다. 다시 돌려도 안전하다.
끝으로 네이버 콘솔의 Web 서비스 URL 한 줄을 사람이 지운다.
