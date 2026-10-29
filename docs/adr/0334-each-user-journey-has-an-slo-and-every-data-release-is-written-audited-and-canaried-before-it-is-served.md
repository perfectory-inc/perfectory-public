# ADR 0334: 사용자 여정마다 SLO 와 오류 예산을 두고, 모든 데이터 발행은 쓰기·검사·카나리를 거쳐 서빙된다

- Status: Proposed
- Date: 2026-10-29
- Builds on: [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md)(서빙 신선도 예산), [ADR-0327](./0327-a-worker-deploy-and-a-quarter-hourly-probe-read-what-users-read-twice.md)(엣지 탐침), [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md)(Gold 의 WAP), [ADR-0157](./0157-a-canary-step-counts-its-own-answers-and-cpu-is-bound-by-measurement.md)·[ADR-0196](./0196-a-worker-deploy-creates-a-new-worker-and-weighs-latency-against-the-serving-version.md)(Worker 카나리), [ADR-0242](./0242-a-superseded-pack-generation-is-deleted-a-week-after-its-successor-went-live.md)(옛 세대 보관), [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md)(위생 B SLO), [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)(스키마 레지스트리), [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(자산 파티션)
- 관련 사건: 사용자(소유자)가 먼저 찾은 버그 셋 — 2026-10-29 공매 사진 엑박(캐시 적중 GET 이 모두 500, #927), 2026-10-29 거래이력 영향 PNU 1,599개가 낡은 문서로 남음(ADR-0328), 2026-10-24~28 공매가 화면에 안 보임(서빙 10~12일 정지, ADR-0306). 세 번 모두 작업·모니터는 초록이었다.

## Context

지금 있는 확인 장치와 그것이 보는 것:

| 장치 | 보는 것 | 사용자 여정과의 거리 |
|---|---|---|
| `slo-policy.v1.example.json`·`slo-alert-policy.md` | API readiness·5xx·수집·Gold 신선도 | API·적재 중심. 지도·패널·사진 여정 없음, **오류 예산 없음** |
| 매시 서빙 모니터(ADR-0151/0160/0306) | 표본 문서 = R2 문서, manifest 나이 | 흰 상자. 스키마가 바뀌면 사용자와 무관하게 실패(10-29) |
| 15분 엣지 탐침(ADR-0327, #928) | 레인 표본 GET 두 번, 사진 해시 | **검은 상자**, 사용자 경로. 결과는 Slack 뿐, 시계열·예산 없음 |
| Worker 배포 카나리(ADR-0157/0196) | 새 Worker 판의 CPU·5xx | 코드 배포만. **데이터 발행에는 카나리가 없다** |
| Gold WAP(ADR-0321) | 전량 빌드를 그림자 가지에 쓰고 바뀐 키만 main 으로 | Gold 까지만. 서빙 발행(manifest)은 검사 없이 바로 |
| 단면 팩 cut-over 게이트(ADR-0147 §6) | 첫 팩 발행 때만 같음(가)·실서빙 표본(나) | **첫 발행 한 번**. 일상 패치에는 없다 |
| 타일 WAP(`spatial_tile_publication_wap.py`) | 타일 판 | 타일만 |

측정: Prometheus 에 노드·엣지 사용자 지표가 없다(`node_cpu_seconds_total` 없음, 탐침 결과는 파일·Slack). 그래서 "지난
28일 사용자가 공매 패널을 성공적으로 연 비율"을 지금은 아무도 답할 수 없다.

대기업 사례:
- **Google SRE**(SRE Book ch.4, Workbook ch.2·5): 사용자 여정(CUJ)마다 SLI·SLO, 오류 예산 정책(예산이 바닥나면 기능
  배포 동결), 다중 창 소진율 경보(1h/5m 14.4배, 6h/30m 6배). 데이터 파이프라인은 신선도·정확성·완전성이 SLI.
- **Netflix WAP**(Iceberg Write-Audit-Publish): 쓰기는 보이지 않는 가지에, 검사 통과 뒤에만 게시.
- **데이터 카나리**: LinkedIn·Uber 의 섀도 읽기(새 결과를 실서빙과 나란히 읽어 대조), 구성 배포와 같은 점진 노출.

## Decision

1. **여정이 SLO 의 단위다.** 정본은 기존 `slo-policy.v1.example.json` 을 **계약으로 승격한
   `config/slo-policy.contract.json`**(v2, `journeys` 추가)이다. 새 파일을 하나 더 만드는 것이 아니라 예제를 계약으로
   옮기고 예제는 지운다. 첫 여정:

   | 여정 | 밟는 것 | SLI | SLO(출시 전 시작값, 28일) |
   |---|---|---|---|
   | J1 지도 보기 | 타일 Worker | 타일 GET 성공률, p95 | 99.5%, 400ms |
   | J2 필지 패널 | 필지 host + 종류 레인 6 | 문서 GET 성공·스키마 유효(ADR-0336) | 99.5% |
   | J3 건물 패널 | 건물 레인(페이지 포함) | 같음 | 99.5% |
   | J4 공매 보기 | 공매 레인 + 필지 엣지 미디어 | 문서 성공 + **사진 바이트 해시 일치** | 99.5%, 사진 99.9% |
   | J5 단가 | 단가 레인 | 문서 성공 | 99.5% |
   | J6 변경 반영 | Silver 변경 → 서빙 문서 | **바뀐 PNU 가 서빙되기까지의 시간**(ADR-0312 바뀐 PNU 기록 → manifest 패치) | 바뀐 PNU 의 99% 가 36시간 안 |
   | J7 신선도 | 레인 manifest | `published_at_utc` 나이(ADR-0306) | 48시간 안 99% |

   J6 이 1,599 PNU 사고를 잡는다: 문서가 있고 200 이어도 "바뀐 키가 반영되지 않음"은 실패다.
2. **측정은 둘이다.** (a) 합성: ADR-0327 탐침을 여정별로 확장하고 결과를 노드 익스포터 텍스트 파일로 내보내 Prometheus 가
   시계열로 가진다(새 탐침 실행기 없음). (b) 실사용: Workers analytics 의 호스트명별 상태 코드(이미 게이트가 읽는 토큰,
   ADR-0157)와 웹 미리보기의 RUM. 운영 공짱(AWS)은 건드리지 않는다.
3. **오류 예산 정책.** 여정의 28일 예산이 바닥나면 그 여정의 레인·Worker·웹 화면에 대한 기능 변경은 자동 배포에서
   보류되고(신뢰성 수정은 통과), 소진율 경보는 Alertmanager 다중 창 규칙(14.4×/1h, 6×/6h)이다. 보류 판정은 배포가
   이미 읽는 계약 한 곳(위 계약)에서 온다.
4. **모든 데이터 발행은 WAP 다.** Gold(ADR-0321)와 타일에 이미 있는 것을 by-PNU 서빙·미디어로 넓힌다:
   - **Write**: 패치·전량 모두 새 manifest 를 `manifest.candidate` 로 쓴다. 문서·팩 객체는 이미 세대·내용 이름이라
     실서빙과 섞이지 않는다.
   - **Audit**: (가) 레지스트리 스키마 검증(ADR-0336)을 바뀐 문서 표본에, (나) 수 불변식(바뀐 PNU 수 = 패치 수,
     ADR-0312), (다) **섀도 읽기** — 미리보기 Worker 가 후보 manifest 로 같은 표본을 읽고, 바뀌지 않은 PNU 는 실서빙과
     바이트 같음, 바뀐 PNU 는 Gold 와 같음. 지금 cut-over 게이트(ADR-0147 §6 가·나)를 **모든 발행**의 검사로 만든다.
   - **Publish**: 후보를 manifest 로 원자 교체. 되돌리기는 이전 manifest 로의 교체(옛 세대는 ADR-0242 대로 7일 보관).
5. **데이터 카나리.** 레인 Worker 는 `PNU 해시 < canary_ppm` 인 요청에 후보 manifest 를 쓴다(계약 `canary_ppm`,
   기본 5%·30분). 그 동안 J2~J5 의 SLI 를 후보와 실서빙으로 나눠 비교하고, 후보 쪽이 예산 소진율 기준을 넘으면 후보를
   버린다. 응답 헤더가 어느 manifest 였는지 적는다(ADR-0157 의 버전 헤더 확장).
6. **여정 범위는 빠짐없이.** `by_pnu_lanes` 의 모든 레인, 모든 엣지 호스트명, 웹의 by-PNU 화면은 어느 여정엔가 속해야
   한다(가드).

## 이행(한 여정씩)

| 단계 | 내용 | 끝났다는 증거 |
|---|---|---|
| 1 (첫 조각) | J4 공매: 계약 승격(공매 여정 한 줄), 탐침 결과의 텍스트 파일 지표, 28일 SLI 대시보드·소진율 경보 | "지난 28일 공매 성공률"이 숫자로 나온다 |
| 2 | 공매 레인 발행을 candidate → audit(섀도 읽기) → publish 로 | 깨진 후보(시험: 스키마 위반 문서)가 서빙되지 않음 |
| 3 | J6 변경 반영 SLI(바뀐 PNU → 서빙 지연) | 1,599 사고를 재현한 시험에서 경보 |
| 4 | 데이터 카나리를 공매 → 단가 → 종류 레인 → 필지·건물 순으로 | 레인마다 카나리 판정 기록 |
| 5 | 오류 예산 정책을 자동 배포에 연결, J1 타일·웹 RUM | 예산 바닥 시 기능 배포 보류(시험) |

## 가드(ADR-0001)

- **모든 레인·호스트명은 여정에 속한다**: 막는 사고 — 2026-10-24~28 공매가 화면에 없었는데 어느 여정 지표도 그 레인을
  보지 않은 일.
- **manifest 는 후보를 거쳐서만 바뀐다**(발행기의 쓰기 경로 시험): 막는 사고 — 10-29 #929 전체 세대가 잘못된 기록을 검사
  없이 바로 실 manifest 에 쓴 일.
- **J6 SLI**: 막는 사고 — 10-29 1,599 PNU 가 200 으로 낡은 문서를 내며 모든 검사를 통과한 일.

## 기각한 대안

- **흰 상자 모니터를 더 촘촘히**: 10-29 에 스키마 변경만으로 매시 실패했다. 사용자 결과가 아니라 내부 형식을 보는 검사는
  배포 게이트가 되면 거짓 경보로 배포를 막는다. 흰 상자는 진단용으로 남고, 게이트는 검은 상자 SLI 가 맡는다.
- **외부 SaaS 합성 모니터링(Checkly·Datadog Synthetics)**: 월 수만 원~수십만 원, 표본 PNU 를 외부에 둬야 한다. 탐침은
  이미 있다(ADR-0327). 시계열·예산만 더한다.
- **카나리 없이 WAP 만**: 검사는 표본이고 사용자 분포를 다 덮지 못한다. 카나리는 실사용 분포의 5% 로 남은 위험을 잰다.

## Consequences

- 데이터 발행 한 번이 30분 늦게 전부에 닿는다(카나리 창). J7 신선도 예산(48h) 안이다.
- 후보 manifest 와 카나리 판정이 R2 읽기·Worker 요청을 조금 늘린다(레인당 하루 수천 요청, Workers Paid 포함량 안,
  계산).
- 출시 때 SLO 시작값을 28일 실측으로 다시 정한다(지금 값은 측정 전 시작값이다).
