---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# 부하 시나리오

이 k6 시나리오는 성능·스테이징 용량을 찾는 운영자 도구다. `apps/`, `services/`, `crates/`,
`packages/`에서 가져오지 않는다.

각 시나리오는 `k6 run --summary-export`로 실행한다. 증거 위치,
approved targets, and measured capacity bindings are private operator inputs
(see `docs/testing/load.md`).

Example:

```bash
test -n "$LOAD_EVIDENCE_DIR"
k6 run --summary-export "$LOAD_EVIDENCE_DIR/k6-summary.json" \
  tests/load/scenarios/api-read-mix.js
```

커밋된 대상은 의도적으로 라우팅할 수 없다. 실행에는 비공개 실행기가 제공하는 승인된
target host supplied by the private load runner through
`LOAD_APPROVED_TARGET_HOSTS`, using comma-separated hostnames without scheme,
path, port, query, or credentials.

인증된 API 읽기 경로는 runner에 `LOAD_AUTH_BEARER_TOKEN`을 설정한다.
environment. Do not put bearer tokens in workflow inputs or committed files.

marker 실행은 `LOAD_FILTER_HASH`를 설정하고 필요하면 `LOAD_FILTER_HASH_MISS`도 설정한다.
to known fixture hashes from the perf dataset. The default miss path reuses the
same valid hash and changes only the requested tile.
