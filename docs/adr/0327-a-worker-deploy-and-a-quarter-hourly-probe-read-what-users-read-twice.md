# ADR 0327: Worker 배포와 15분 탐침은 사용자가 읽는 것을 GET 으로 두 번 읽고, 시험은 운영 바인딩을 모두 묶는다

- Status: Accepted
- Date: 2026-10-29
- Builds on: [ADR-0175](./0175-workers-deploy-themselves-after-the-host-deploys-main.md)(Worker 자동 배포·스모크·되돌리기), [ADR-0196](./0196-a-worker-deploy-creates-a-new-worker-and-weighs-latency-against-the-serving-version.md), [ADR-0151](./0151-building-packs-serve-each-document-whole.md)·[ADR-0160](./0160-one-by-pnu-gateway-source-serves-both-lanes.md)(매시 서빙 모니터), [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md), [ADR-0157](./0157-a-canary-step-counts-its-own-answers-and-cpu-is-bound-by-measurement.md)(버전 헤더), [ADR-0297](./0297-parcel-media-captures-originals-first-and-pre-generates-display-sizes.md)(필지 엣지 미디어)
- 관련 사건: 2026-10-29 — catalog.perfectory.io(`foundation-parcel-gateway`)가 공매 사진을 첫 GET 뒤로 모두 500 으로 답했다(`TypeError: Can't modify immutable headers`, `withVersion` 이 캐시 적중 응답의 불변 헤더에 버전 헤더를 씀). 수정은 #927. 소유자가 웹 미리보기의 깨진 그림을 보고서야 알았다.

## Context

세 구멍이 겹쳤다.

1. **변경 뒤 확인이 HEAD 였다.** `serveMedia` 는 GET 만 엣지 캐시(`caches.default`)를 본다. HEAD 와 첫 GET 은 캐시를
   지나지 않으므로 통과했다.
2. **시험이 운영과 다른 Worker 를 띄웠다.** 운영 `wrangler.*.jsonc` 는 `version_metadata` 를 묶는데, 게이트웨이의
   Miniflare 시험 6개 파일은 각자 손으로 바인딩을 적었고 아무도 그것을 묶지 않았다. 버전이 없으면 `withVersion` 은
   헤더를 쓰지 않으므로, 캐시 적중 + 버전 헤더 경로를 탄 시험이 하나도 없었다.
3. **사용자 요청을 흉내 내는 상시 탐침이 없었다.** Worker 자동 배포의 스모크(ADR-0175)는 capabilities 를 읽고
   레인의 서빙 모니터를 돌린다. 모니터는 표본 PNU 를 **한 번씩** 읽어 R2 의 문서와 대조하는 흰 상자 검사이고,
   미디어는 읽지 않는다. 매시 모니터도 같다. 엣지 사본에서 읽은 두 번째 답, 그리고 사진은 아무도 보지 않았다.

검토한 기존 장치(전 트리 조사, §새 메커니즘 전에 증명):

| 후보 | 하는 일 | 이 구멍에 그대로 못 쓰는 이유 |
|---|---|---|
| `foundation-by-pnu-serving-monitor@<lane>`(매시, Rust) | 표본 20개를 한 번씩 읽고 R2 문서와 대조, 신선도 | 흰 상자(R2 읽기·문서 형식 판정): 문서 버전이 바뀌면 실패한다(10-29 지금 공매 레인이 `...v1 document` 로 매시 실패). 배포를 막는 조건이 되면 사용자와 무관한 이유로 배포가 멈춘다. 10개 인스턴스를 15분으로 당기면 R2 읽기·Slack 이 4배 |
| `foundation-service-checks`(매시, ADR-0209) | 호스트명의 한국발 엣지 경로·CI 추세를 파일로 | 레인·표본을 모르고, 결과를 foundation-api 가 서빙하는 파일로 쓴다. 주기가 1시간 |
| Prometheus/Alertmanager | foundation-api 지표, 노드 텍스트 파일 | 탐침 실행기가 없다(blackbox_exporter 없음). 표본·해시를 실행 시 끌어오는 탐침은 대상 고정 목록과 맞지 않는다 |
| `worker_autodeploy.py` 의 스모크·되돌리기 | 업로드 → 100% → 스모크 → 실패 시 옛 버전 100% | **확장한다**: 되돌리기·재시도·알림이 이미 있다. 빠진 것은 "사용자처럼 두 번 읽기" 하나 |

대기업 사례: Google SRE 의 black-box monitoring(사용자 관점의 외부 탐침이 증상을 잡는다, *Monitoring Distributed
Systems*), AWS CloudWatch Synthetics canary(배포 파이프라인이 canary 경보에 묶여 자동 롤백), Cloudflare 자신의
Worker gradual deployments + 버전별 지표. 공통 보장만 가져온다: **사용자 경로를 그대로 밟는 외부 탐침, 배포 직후와
상시 두 번, 실패하면 직전 버전으로.**

## Decision

1. **탐침 하나, 두 자리.** `scripts/ops/by-pnu-edge-probe.py`(표준 라이브러리, 자격 증명 없음)가 by-PNU Worker 하나가
   서빙하는 모든 레인(자기 레인과 그 Worker 가 호스트하는 종류 레인, ADR-0193 §4)을 GET 으로 읽는다. 문서는 레인의
   게이트 표본에서 404 가 아닌 첫 항목을 `reads`(계약, 2)번 더 읽어 매번 200·레인의 content type·비지 않은 JSON
   객체를 요구한다. 필지 엣지에서는 공매 레인 표본 문서가 이름한 첫 사진(`rounds[].photos[]` 의 `sha256`·
   `media_format`)을 두 번 읽어 200·확장자의 content type·**바이트의 sha256 = 이름의 해시**를 요구한다.
   첫 읽기는 이미 엣지 사본일 수 있고(사본은 모든 버전이 공유) 두 번째는 늘 사본이다.
2. **배포 스모크.** `worker_autodeploy.py` 가 by-PNU Worker 의 미리보기와 운영 각각에서 capabilities 가 새 버전을
   말한 뒤, 서빙 모니터 앞에서 이 탐침을 그 호스트명에 돌린다(`systemd-run`, `foundation-platform` 계정,
   탐침 유닛의 환경 파일만). 실패는 기존 경로 그대로: 그 대상을 직전 버전 100% 로 되돌리고(미리보기가 실패하면
   운영은 업로드조차 안 한다), 유닛 실패가 Slack 에 간다. 탐침 파일은 `TOOL_FILES` 에 있어 탐침을 고친 커밋은
   포기한 Worker 를 다시 시도하게 한다.
3. **상시 탐침.** `foundation-by-pnu-edge-probe.timer` 가 15분마다(매시 07·22·37·52분, 모니터 :17·점검 :41 과
   겹치지 않게) 계약의 by-PNU Worker 셋을 운영 호스트명에서 탐침한다. 실패는 `OnFailure=foundation-unit-failed@`
   → Slack, `Documentation=` 은 [worker-autodeploy 런북 §4](../../platforms/foundation-platform/docs/runbooks/worker-autodeploy.md).
   데이터 작업이 아닌 호스트 점검이므로 `jobs.v1.json` 작업이 아니라 systemd 타이머다(ADR-0118 §1, 모니터와 같음).
   처리하는 데이터가 없어 ADR-0314 의 scope 선언 대상이 아니다.
4. **표본은 실행 시 호스트에서.** PNU·해시는 저장소에 없다(공개 저장소). 레인의 표본은 컷오버 때 호스트에 쓴
   게이트 표본(`FOUNDATION_PLATFORM_<LANE>_BY_PNU_SERVING_MONITOR_SAMPLE_PATH`, 모니터와 같은 파일)이고, 사진
   해시는 엣지가 서빙한 문서에서 읽는다. 표본 변수가 없는 레인은 아직 컷오버 전이라 건너뛰고 그렇다고 말한다.
   아무 레인도 읽지 못한 실행은 실패다. 표본 문서가 사진을 하나도 이름하지 않으면 사용자가 닿을 사진이 없으므로
   미디어 읽기를 건너뛰고 말한다.
5. **정책은 계약 한 곳.** `config/r2-connections.contract.json` `by_pnu_section_packs.edge_probe`: `reads`,
   `read_timeout_seconds`, `user_agent`(분석에서 합성 트래픽을 가를 수 있게), `scan_documents`(끝난 공매처럼 사라진
   표본 항목을 넘는 상한), `media.documents_lane`. 탐침 유닛은 `config/runtime-secrets.contract.json` 에 모니터
   파일 네 그룹만 선택 그룹으로 둔다 — 비밀 파일은 읽지 않는다.
6. **시험은 운영이 묶는 것을 모두 묶는다.** 게이트웨이 시험은 `test/miniflare.ts` 의 `startGateway(lane, …)` 로만
   Worker 를 띄운다. 그것은 `wrangler.<lane>.jsonc`(그리고 `env`)에서 R2·`version_metadata`·`vars` 와 `define`·
   호환 날짜를 끌어오고, 설정 밖에서 운영이 두는 CORS 변수(계약의 `allowed_origins_binding`)를 더한다. 손 목록은
   없다. 설정에 하네스가 묶지 못하는 바인딩 종류(KV·D1·서비스 등)가 생기면 하네스가 거부한다. 일부러 빼는 것은
   `without: [이름]` 으로 이름을 대야 하며, 운영 바인딩이 아닌 이름은 거부한다.

가드가 막는 실제 사고(루트 ADR-0001 가드 정책): **2026-10-29 캐시된 공매 사진이 모두 500 이었는데, 시험은 운영의
버전 바인딩 없이 돌았고 변경 확인은 HEAD 였으며 사용자 요청을 흉내 내는 탐침이 없어 소유자가 화면에서 찾았다.**
- `test/harness-parity.test.ts`: 모든 `wrangler.*.jsonc`·env 의 바인딩이 하네스에 있음, 모르는 바인딩 종류 거부,
  어떤 시험 파일도 하네스 없이 Miniflare 를 띄우지 않음(포트 재시도 시험 하나만 예외, 게이트웨이가 아니다).
- `orchestration/tests/test_by_pnu_edge_probe.py`: 두 번째 GET 만 500 인 사진(이 사고), 해시가 다른 바이트, 두 번째
  읽기만 깨진 문서가 탐침을 실패시킨다.
- `orchestration/tests/test_worker_autodeploy.py`: 탐침이 모니터 앞에 돌고, 실패하면 미리보기·운영을 되돌린다.

## Consequences

- by-PNU Worker 배포 한 번에 대상마다 GET 이 약 40번(필지: 레인 8개 × 3 + 사진 탐색 ≤20 + 2) 더해진다. 수 초.
- 상시 탐침: 15분마다 약 45 요청 → 하루 약 4,300, 한 달 약 13만 Worker 요청. Workers Paid 포함량(월 1,000만)
  안이고 R2 는 엣지 사본이 없을 때만 읽으며 egress 는 0. 비용 0원(계산).
- 장애가 이어지면 Slack 이 15분마다 한 번 온다. 사용자 경로 장애이므로 받아들인다; 묶어 줄이려면 Alertmanager 로
  옮기는 것이 다음 단계다(지금은 하지 않는다 — 탐침 실행기가 없다).
- 첫 읽기가 엣지 사본인지 원본인지는 밖에서 강제할 수 없다(질의 문자열은 404 로 거절된다). 원본 경로는 매시
  모니터의 표본 읽기와 `startGateway` 로 띄운 Miniflare 시험이 맡는다.
- 이 PR 이 `r2-connections.contract.json` 을 바꾸므로 병합 뒤 자동 배포가 by-PNU Worker 셋을 다시 올리며, 그때부터
  새 스모크가 적용된다.
