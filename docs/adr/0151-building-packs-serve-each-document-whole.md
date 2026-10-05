# ADR 0151: 건물 묶음은 PNU 문서를 통째로 담고, Worker 는 그 바이트를 그대로 낸다

- Status: Accepted
- Date: 2026-10-05
- Amends: [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md) (서빙 형태: 항목별 조각을 Worker 가 합치는 것 → 문서 통째)

## Context

ADR-0147 의 건물 레인은 문서를 항목 넷(`buildings`·`floors`·`units`·`unit_prices`)으로 나눠 묶고, Worker 가
요청마다 네 묶음에서 조각을 읽어 gzip 을 풀고 JSON 으로 합쳐 다시 직렬화했다. 관문 (나)(2026-10-05, 표본 10,000,
ai-server 에서 순차)는 두 가지로 실패했다.

1. **503 276건.** 원인은 Cloudflare 계정이 **Workers Free**(요청당 CPU 10ms)라는 것이다. 근거:
   - Workers 분석(GraphQL `workersInvocationsAdaptive`): 미리보기 Worker 의 `exceededResources` 325건, 그
     CPU p50 이 정확히 10,000µs(오류 1102 → HTTP 503). 모두 06:25–06:36Z, 탐침의 503 시간대와 같다.
     성공한 요청의 CPU 는 p50 8.0ms·p99 19.8ms 였다(Free 는 isolate 마다 가끔의 초과를 봐주다가 끊는다).
   - `wrangler versions upload` 에 `limits.cpu_ms` 를 주면 "CPU limits are not supported for the Free plan
     [code 100328]" 로 거부된다(아무것도 올라가지 않았다).
   - 운영 객체 경로의 CPU 는 p50 1.95ms 다(R2 GET 하나, 본문 통과).
2. **첫 조회가 느렸다**(p50 +212ms, p95 +361ms, 한도 +50ms). 큰 묶음은 머리 읽기 + 범위 읽기 두 번을 R2 에
   다녀왔다. 이 계정(Free 존)의 한국 트래픽은 Cloudflare **LAX** 로 들어오고(ai-server·노트북 모두 `cf-ray …-LAX`),
   버킷은 APAC 이라 R2 왕복 하나가 LAX 에서 약 160–215ms 다.

CPU 를 줄이려고 잰 것(미리보기, 같은 배치, 600건씩, Workers 분석):

| 변형 | CPU p50 | CPU p99 |
|---|---:|---:|
| 기존 코드(머리 + 범위, 머리 엣지 캐시) | 10.1ms | 37.5ms |
| 256KiB 이하 묶음 통째 읽기 + 엣지 캐시 | 10.3–11.8ms | 24–35ms |
| 통째 읽기, 엣지 캐시 쓰기 없음 | 8.0ms | 27.1ms |

- 실제 문서 299건으로 잰 Worker 의 JS 일(조각 넷 gunzip·파싱·합치기·재직렬화·해시): p50 0.75ms, **p99 15ms, 최대
  73ms**(호가 수천인 아파트 문서). p50 은 R2·캐시·스트림 배관, p99 는 큰 문서의 JSON 왕복이다.
- 그래서 항목 넷을 Worker 가 합치는 한 Free 의 10ms 안에 p99 를 둘 방법이 없다. 묶음 크기 한도를 바꿔도 CPU 는
  움직이지 않았다(위 표).

## Decision

1. **건물 레인의 묶음은 항목 하나 `documents` 다.** 한 PNU 의 항목은 그 PNU 의 서빙 문서 전체이고, 바이트는 객체
   레인이 쓰던 문서와 같다(gzip 한 덩어리). 계약 `building_by_pnu_gateway.section_packs.sections` 가
   `["documents"]`, 기준 항목도 `documents` 다. 묶음 형식(v1), 세대·패치·`patch_floor`, manifest 의
   `section_packs` 블록(schema 3), 관문 (가)·(나)·(다)는 그대로다. 항목 목록만 바뀐다.
2. **Worker 는 그 gzip 덩어리를 풀지도 파싱하지도 않고 `Content-Encoding: gzip` 으로 낸다.** 클라이언트가 gzip 을
   받지 않으면(드묾) 그때만 Worker 가 푼다. `ETag` 는 묶음의 R2 `etag` 와 그 덩어리의 위치다(키가 create-only 라 그
   바이트를 정확히 가리킨다). PNU 마다의 엣지 응답 캐시는 묶음 경로에서 쓰지 않는다(묶음 사본이 R2 왕복을 이미 없앤다).
3. **읽기 경로**(계약 `by_pnu_section_packs.read_path`):
   - 계약의 `whole_pack_max_bytes`(256KiB) 이하 묶음은 머리를 찾는 GET 한 번으로 통째 읽고 그 바이트에서 문서를
     꺼낸다(R2 한 번). 큰 묶음은 머리까지만 읽고 끊은 뒤 범위 읽기를 `onlyIf.etagMatches` 로 한다. 근거: g1 측정
     (묶음 76,040개)에서 묶음의 93.1%, 문서 읽기의 62.3% 가 256KiB 이하다.
   - 묶음 사본은 isolate 메모리(바이트 예산 `memory_budget_bytes`, LRU)와 엣지 캐시(`immutable`)에 둔다. 엣지
     사본은 따옴표 붙은 `ETag` 를 지니고, 머리 사본은 그 태그로만 범위 읽기와 합친다. 캐시 식별자에 태그를 넣지
     않는 이유: 조회가 태그를 알기 전에 일어나므로, 넣으면 조회 앞에 R2 왕복이 하나 더 생긴다.
   - R2 의 일시 오류는 `r2_attempts` 번까지, 남은 `r2_deadline_ms` 안에서, 전체 지터 지수 백오프로 다시 묻는다.
     묶음이 형식·태그와 어긋나면 다시 묻지 않는다. 503 은 불일치이거나 기한 안에 R2 가 답하지 않은 경우뿐이다.
   - 미리보기는 `Server-Timing`(R2 대기·시도 수, 항목마다 묶음이 어디서 왔는지, 전체)과 구조화된 로그를 낸다.
4. **관문 (나)는 Worker CPU 도 본다.** 탐침은 계약의 `probe_concurrency`(8)개를 동시에 읽되 한 건씩 시간을 재고,
   법정동의 첫 읽기(차가움)와 나머지(따뜻함)를 나눠 적고, 실패를 쪽·종류별로 센다. 한도는 차가운 읽기에 건다. 탐침
   구간의 미리보기 Worker CPU 를 Workers 분석에서 읽어 `exceededResources` 0, p99 ≤ `worker_cpu_p99_max_ms`(5ms,
   Free 한도의 절반)여야 통과다. 분석 자격증명이 없으면 증거에 CPU 가 없고 관문은 열리지 않는다.
5. **미리보기는 별도 Worker 와 별도 주소다.** `foundation-building-gateway-preview` 를 `buildings-preview.perfectory.io`
   (사용자 지정 도메인)에 붙인다. 계약 `section_packs.preview_worker` 가 정본이고 `wrangler.jsonc` 의 `env.preview`
   는 그 투영이다. 운영 Worker 의 이름·주소와 겹치면 생성이 거부된다.

## Consequences

- **패널에 새 칸을 더하면 건물 레인 묶음 전체를 다시 굽는다**: 법정동 약 19,010개 → R2 Class A 약 19,010회(약
  0.09달러), 굽기 약 40분(2026-10-04 측정의 4항목 굽기 2,329초 기준). 필지 레인을 같은 형태로 옮기면 약 20,000개다.
  ADR-0147 이 기대한 "한 항목만 다시 굽기"는 없어진다. 객체 방식(건물 5.94M 회, 약 27달러, 7시간 이상)보다는 여전히
  수백 배 싸다.
- 매일 패치는 바뀐 PNU 의 문서 통째를 패치 묶음에 쓴다(변경 PNU 만). ADR-0147 의 항목별 패치보다 바이트가 조금
  늘지만 요청 수는 같다.
- Worker 의 묶음 경로 CPU 는 객체 경로와 같은 일(R2 GET 하나 또는 둘, 바이트 통과)이 된다.
- 4항목 g1 묶음(`serving/buildings/packs/{buildings,floors,units,unit_prices}/g1/`)은 서빙에 쓰이지 않는다. 지우지
  않는다(create-only, 이력).
- **한국 사용자 지연은 Worker 코드로 풀리지 않는다.** Free·Pro 존의 한국 트래픽은 ICN 이 아닌 해외 PoP 로 들어오고
  (Cloudflare 커뮤니티 공식 답변, 한국 대역폭 단가: Cloudflare 블로그 "Bandwidth Costs Around the World"), ICN
  진입은 Enterprise 에서만 보장된다. Argo Smart Routing 은 Cloudflare→원본 구간을 고르는 것이라 R2 바인딩과 진입
  PoP 에는 해당이 없다. Workers Paid 는 CPU 한도를 바꿀 뿐 진입 PoP 를 바꾸지 않는다. Worker 배치(`placement`)는
  Free 에서도 되며, Worker 를 버킷 가까이(측정: `aws:ap-northeast-1` → `remote-NRT`, R2 왕복 p50 76ms) 옮겨
  R2 왕복을 줄인다. 운영 Worker 의 배치 변경은 별도 결정이다.
