# ADR 0568: foundation-api 는 malloc 아레나를 둘로 묶고, 표 메타데이터는 몇 개씩만 읽는다

- Status: Accepted
- Date: 2026-11-19
- Builds on: [ADR-0504](./0504-one-query-cannot-take-trino-past-its-container-cap-and-trino-follows-the-release.md)(Trino 의 같은 아레나 증가에 `MALLOC_ARENA_MAX=4`),
  [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)(상한은 측정에서 나온다),
  [ADR-0195](./0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md)(운영 지도가 세 저장소를 읽어 둔다)

## Context

**사고 (W87 측정 중 발견).** 상주 서비스 `foundation-platform-runtime-foundation-api-1`(`mem_limit: 512m`,
`oom_score_adj: -900`)이 2026-11-14..18 에 커널 OOM 으로 69 번 죽었다(하루 8·10·16·21·14 번, `RestartCount=69`).
죽을 때마다 `anon-rss` 는 520 MB 안팎이었다. 사용자 쪽 영향(Prometheus, 11-13..18):

| 무엇 | 잰 값 |
|---|---:|
| `up{job="foundation-api"} == 0`(분) | 134 분 / 5 일 |
| `/metrics` 408(스크레이프 시간 초과) | 257 |
| `/readyz` 503 | 202 |
| `FoundationContainerMemoryNearCap` 이 이 컨테이너로 울린 표본 | 14 일 내내(ticket, 아무도 고치지 않음) |

**무엇이 자랐나.** cAdvisor 일별 최대 working set 은 11-04 301 MB, 11-09 389 MB, 11-10 494 MB, 11-11 부터 매일 512 MB
상한이다. 같은 기간 API 가 받은 요청은 `/readyz`·`/metrics`·`/catalog/v1/vector-tiles/runtime-manifest`(4 일 1,122 건)뿐이고,
RSS 는 재시작 뒤 15 분 안에 110 MB 에서 500 MB 로 오르내린다 — 요청이 아니라 뒤에서 도는 일이 잡고 놓지 않는 메모리다.
CPU 0%, 로그 30 분 무소식인 상태에서 `/proc/1/smaps` 는 64 MB 경계에서 시작하는 익명 매핑 7 개가 23–62 MB 씩(합 245 MB)
들고 있음을 보였다. glibc malloc 의 스레드별 아레나다: 기본 상한은 8 × 코어(ai-server 20 코어 = 160), API 는 tokio 일꾼
20 개 + 1 = 21 스레드, 아레나는 해제된 조각을 맨 위가 아니면 운영체제에 돌려주지 않는다.

뒤에서 도는 일은 운영 지도의 읽기(`facts::read`, ADR-0195)다. `/metrics` 가 30 초마다 다음 읽기를 깨우고, 읽기는 낡은 표
166 개의 Iceberg 메타데이터를 **한꺼번에**(`join_all`) 받아 파싱한다. 11-09 뒤로 그 표들의 커밋이 크게 늘었다(Trino
`$snapshots`: 전체 7,695 개 중 4,874 개가 11-09 이후, `silver.real_transaction` 1,701, 산단 고시 두 표 0 → 891). 한 번의
폭발이 21 개 아레나에 흩어져 각자의 최고 수위로 남고, 수위가 자료와 함께 올라 512 MB 에 닿았다. 2026-11-11 Trino 가 같은
원인으로 16g 를 넘었다(ADR-0504: 유휴 아레나 164 개 = 2.5 GB).

## Decision

1. **아레나 상한.** compose `foundation-api` 환경(같은 앵커를 쓰는 `foundation-api-smoke` 도)에
   `MALLOC_ARENA_MAX=2`. 이 서비스의 일은 뒤의 읽기 하나와 드문 요청이라 동시 할당이 적고, 아레나 둘이면 경합보다 수위가
   문제다. 상한이 있는 컨테이너 안의 glibc 프로세스에 아레나 상한을 두는 것은 Hadoop(`HADOOP-7154`, 4)·Heroku(Ruby, 2)·
   ADR-0504(Trino, 4)의 방식이다.
2. **폭발을 줄인다.** 운영 지도의 표 읽기는 `TABLE_READS_AT_ONCE = 4` 개씩(`buffer_unordered`)만 동시에 받는다. 한 읽기가
   들 수 있는 메타데이터가 표 수가 아니라 4 개 몫으로 묶인다. 결과는 표 이름으로 모으므로 순서는 상관없다.
3. **할당기 교체는 하지 않는다.** jemalloc(`tikv-jemallocator`: TiKV·Databend·Materialize)은 감쇠로 메모리를 돌려주지만,
   빌드에 C 툴체인과 MSVC 제외 분기를 더하고 서비스마다 다른 할당기를 둔다. 같은 glibc 손잡이가 이미 이 저장소의 결정
   (ADR-0504)이므로 그것을 넓힌다. 배포 뒤 측정이 다시 상한 가까이 가면 그때 할당기를 다시 본다.
4. **상한은 측정으로 다시 정한다.** 512m 는 그대로 둔다. 배포 뒤 48 시간의 cAdvisor working set 최고점과 OOM 0 을 확인하고,
   최고점 × 1.5(ADR-0455)가 512m 를 넘으면 `docker-compose.yml` 과 호스트 예산(`tools/host-memory-budget.contract.json`)을 함께
   바꾸는 PR 로 올린다.

## Consequences

- API 의 메모리 수위가 스레드 수와 함께 곱해지지 않는다. 운영 지도 읽기는 표를 4 개씩 받아 조금 늦어질 수 있다(166 표,
  표마다 수십 ms; 읽기는 백그라운드라 응답은 기다리지 않는다).
- 사고 자체의 기록(시각·영향)은 비공개 `perfectory-ops` 의 사후 검토서로 간다. 이 ADR 은 오래 가는 결정만 남긴다.
- 상한 가까이에서 몇 주를 울린 ticket 경보가 아무도 고치지 않은 채 죽음으로 이어졌다. 상한 근접이 주인을 부르는 규칙은
  W87 의 한도 결정(측정으로 정하는 상한, 근접 경보)의 몫이다.
