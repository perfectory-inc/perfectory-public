---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# 부하 시나리오

이 k6 시나리오는 성능·스테이징 용량을 찾는 운영자 도구다. `apps/`, `services/`, `crates/`,
`packages/`에서 가져오지 않는다. 시나리오 목록·SLO는 [`scenarios.v1.json`](scenarios.v1.json)이 정본이고,
증거 위치·승인 대상·측정 용량은 비공개 운영자 입력이다([`docs/testing/load.md`](../../docs/testing/load.md)).

각 시나리오는 `k6 run --summary-export`로 실행한다.

```bash
test -n "$LOAD_EVIDENCE_DIR"
TARGET_BASE_URL=https://<승인된-대상> \
  k6 run --summary-export "$LOAD_EVIDENCE_DIR/k6-summary.json" \
  tests/load/scenarios/api-read-mix.js
```

- 대상은 `TARGET_BASE_URL`로만 받는다(필수). 커밋된 `defaultTargetBaseUrl`은 라우팅할 수 없는
  `.invalid` 주소이고, `gongzzang.com`과 그 하위 도메인은 `lib/env.js`가 거부한다.
- `LOAD_ENVIRONMENT`는 `perf`(기본)·`staging`·`local`·`ci` 중 하나, `LOAD_PROFILE` 기본은 `smoke`.
- 인증된 API 읽기 경로는 runner 환경에 `LOAD_AUTH_BEARER_TOKEN`을 설정한다. bearer 토큰을 workflow
  입력이나 커밋 파일에 넣지 않는다.
- marker 실행은 perf 데이터셋의 알려진 fixture 해시를 `LOAD_FILTER_HASH`(필요하면 `LOAD_FILTER_HASH_MISS`)로
  줄 수 있다. miss 경로는 기본적으로 같은 유효 해시를 쓰고 요청 타일만 바꾼다.
