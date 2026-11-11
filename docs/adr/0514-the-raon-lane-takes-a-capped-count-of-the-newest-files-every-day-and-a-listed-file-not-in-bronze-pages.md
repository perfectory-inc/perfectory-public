# ADR 0514: RAON 레인은 매일 공급자 갱신일이 새것부터 정한 개수를 스스로 받고, 목록에 있는데 Bronze 에 없는 파일은 72시간 뒤 알린다

- Status: Accepted
- Date: 2026-11-11
- Supersedes: [ADR-0170](./0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md) §9(상한 0 에서 시작, 운영자의
  첫 감독 실행 뒤 측정한 상한을 적는다)와 "첫 감독 실행" 절 — 레인 자체(§1~§8)는 그대로
- Builds on: [ADR-0444](./0444-a-by-pnu-lane-is-stale-when-it-is-behind-its-inputs-not-when-its-manifest-is-old.md)(레인은 입력보다
  뒤처졌을 때 묵은 것), [ADR-0443](./0443-each-collected-source-declares-its-providers-release-cadence-and-the-ingestion-alerts-hold-it-to-that.md)(원천별
  공개 주기), [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(규칙은 promtool 로 시험)
- 관련 사건: 2026-11-11 W70 측정 — parcel-zoning 레인 manifest 가 24.4일(10-17 발행) 묵었고, 그 입력 silver.land_use_plan 은
  2026-06-09 판이었다. 아무 경보도 울리지 않았다.

## Context

2026-11-11 08:25Z 운영 측정(읽기 전용: node-exporter textfile, Trino `$snapshots`, 운영 원장 `catalog.bronze_object_member`):

| 무엇 | 값 |
|---|---|
| parcel-zoning 레인 manifest 나이 / `foundation_by_pnu_lane_behind_seconds` | 2,106,062 s (24.4일) / 0 |
| gold.parcel_zoning 이 고정한 silver.land_use_plan 판 | 2026-10-15 커밋, 판 `20260609` |
| Bronze 에 있는 용도지역지구(AL_D155) 판 | `20260711`·`0826`·`0909`·`1009`·`1109` 가 모두 시도 13~15 개 — 경기(41)·경북(47) 없음 |
| `20260609` 까지의 판 | 시도 17 개 전부 |
| 매일 훑기 journal 의 raon 줄(11-01~11-11) | `raon deferred=44~49 listed_bytes=37~41 GB budget=0 status=lane-off` — 용도지역지구 26·용도구역 17·토지소유 6 |
| `selection_archive_new_bytes_budget` | 0 (ADR-0170 §9, 2026-10-09 부터) |
| RAON 레인 실행 기록(`/data/foundation-platform/source-sweep/raon/runs`) | 없음 — 10-11 빌드 맥락만 남음 |

경기·경북 파일은 500 MB 를 넘어 공급자가 RAON 에이전트로만 준다. ADR-0170 §9 는 레인을 상한 0(꺼짐)으로 두고 운영자의 첫
감독 실행이 상한을 정하게 했다. 그 실행은 33일 동안 오지 않았다. Silver 갱신은 시도를 모두 덮는 가장 새 판만 올리므로
(ADR-0169) 7월 이후 판은 하나도 올라가지 못했다. 사용자는 2026-06 의 용도지역을 보고 있었다.

경보가 없던 이유는 세 겹이다. ADR-0444 의 서빙 지연은 Silver 가 움직인 뒤부터 센다 — Silver 가 멈춰 있으면 0 이다.
ADR-0443 의 수집 신선도는 원천 단위(`vworldkr__land_use_plan`)로 Bronze 의 마지막 수집을 재는데, 같은 원천의 다른
시도 파일이 매일 들어와 신선했다. 매일 훑기의 "레인 꺼짐" 은 슬랙 ℹ️ 한 줄이었고 실패가 아니었다. 같은 꺼짐이 용도구역
지도 타일(zoning_tile_bake 의 `pending_regions`, RAON 전용)과 토지소유도 막고 있었다.

검토한 사례: Kafka consumer lag(Burrow)은 "아직 처리 안 된 가장 오래된 메시지의 나이" 를 잰다 — 여기서 메시지는 공급자가
목록에 올린 파일이다. dbt source freshness 는 원천의 `loaded_at` 을 보지만, 같은 원천 안의 일부가 빠진 것은 보지 못한다(이번
구멍). Kubernetes workqueue 와 Airflow 의 백필은 밀린 일을 한 번에 다 하지 않고 한 회차의 양을 정해 매 회차 잇는다.
사장 지시(2026-11-07): 사장 손 단계와 고정 시간 창을 두지 않는다. 사장 승인(2026-10-30): 모든 미수집 원천 수집.

## Decision

1. **레인은 사람의 첫 실행 없이 매일 돈다.** 카탈로그 `daily_collections.source_sweep` 에 한 실행이 받을 파일 수
   `selection_archive_max_files_per_run`(8)을 두고, `selection_archive_new_bytes_budget` 을 그 수 × 2 GiB(17,179,869,184)로
   둔다 — Bronze 가 가진 가장 큰 그런 파일(1,598,299,302 바이트, 2026-11-11 측정)보다 크므로 상한은 그보다 큰 파일만
   거부한다. 8 개는 최대 16 GiB, 11-11 VWorld 레인이 받은 속도(약 2 MB/s)로 약 2.3 시간으로 훑기의 480 분 안에 든다. 밀린
   49 개(41 GB)는 7일에 빠진다. 0 은 여전히 레인 꺼짐이다. `raon-large-files.sh` 가 카탈로그의 개수를 읽는다(운영자의
   `FOUNDATION_RAON_LARGE_FILES_MAX_FILES` 는 그 실행만 바꾼다).
2. **새것부터 받는다.** `plan-provider-acquisition-jobs` 는 후보를 공급자 갱신일(`updated_at`) 내림차순으로 놓고 앞에서
   그 수만큼 받는다(날짜 없는 것은 끝, 같은 날은 목록 순서). Silver 는 가장 새 온전한 판을 올리므로 옛 달부터 받으면
   아무도 안 올리는 판만 채운다. 옛 판도 결국 받는다(append-only).
3. **목록에 있는데 Bronze 에 없는 파일을 잰다.** 매일 훑기는 실행 끝에(실패한 실행도) `scripts/ops/source_sweep_metrics.py`
   로 `/var/lib/foundation-jobs/metrics/source-sweep.prom` 을 쓴다: 원천마다 VWorld 레인이 미룬 선택 묶음 중 이 실행의
   RAON 레인이 커밋하지 않은 것의 수(`foundation_source_sweep_unfetched_files`), 바이트, 그중 가장 오래된 공급자 갱신일
   (`foundation_source_sweep_unfetched_oldest_listed_timestamp_seconds`, 날짜가 없으면 0 — 모르면 가장 오래된 것으로 본다,
   ADR-0444 §2 와 같은 이유), 그리고 쓴 시각.
4. **규칙 둘.** `FoundationSourceFilesUnfetched`(page, `serving-freshness`): 그 갱신일이 72 시간보다 오래면 — 매일 실행이
   세 번 받을 기회를 가졌다. `FoundationSourceSweepMetricsSilent`(ticket): 쓴 시각이 50 시간보다 오래거나 한 번도 쓰지
   않은 채 이틀. 둘 다 `foundation-source-sweep.rules.test.yml` 이 promtool 로 시험한다.

가드가 막는 실제 사고(루트 ADR-0001): **2026-10-09~11-11 RAON 레인이 꺼진 채 49 개 파일이 Bronze 에 오지 않았고, 경기·경북이
빠진 용도지역지구 판 다섯 달 때문에 parcel-zoning 이 2026-06 판을 경보 없이 서빙했다.** 시험:
`test_daily_source_sweep_command.py`(못 받은 파일과 그 날짜, 꺼진 레인, 날짜 없는 파일, 실패한 실행, 유닛 쓰기 경로, 카탈로그
상한 ≥ 개수 × 가장 큰 파일), `test_raon_large_files_command.py`(카탈로그 개수가 계획에 간다), `provider_acquisition_plan`
Rust 시험(새것부터), promtool 시험.

## Consequences

- 다음 매일 훑기(배포 뒤 03:40 UTC)부터 RAON 레인이 처음으로 돈다. 이미지 빌드·브라우저 수집이 이 호스트에서 처음이므로
  실패할 수 있다; 그 실패는 훑기의 `raon` 레인 실패(🔴, 유닛 실패)와 위 page 로 보인다. 조용히 꺼져 있는 것보다 낫다.
- 처음 7일 동안은 밀린 옛 판 때문에 `FoundationSourceFilesUnfetched` 가 울린 채로 있다 — 사실이 그렇다. 최신 판의 경기·경북은
  첫날 받으므로 silver.land_use_plan(20261109)·gold.parcel_zoning·parcel-zoning 레인은 기존 자동 경로(Silver 갱신 → Gold kind
  재생성 → 굽기, ADR-0333 §7)로 따라간다.
- 비용: R2 저장 41 GB × $0.015/GB·월 ≈ $0.62/월, 내려받기 무료(공공 원천). 호스트 데이터 디스크 스풀은 파일마다 지운다.
- 첫 실행이 잰 속도로 개수를 올리는 것은 측정 PR 의 몫이다(ADR-0386 의 실행 시간 기록과 함께).
- 새 유닛·새 타이머는 없다. 훑기 유닛의 쓰기 경로에 metrics 디렉터리 하나가 더해진다.

## Alternatives rejected

- **운영자가 첫 감독 실행을 하도록 다시 요청** — 33일 동안 오지 않았고 사장 지시(11-07)는 사람 단계를 없애라는 것이다.
- **밀린 것을 한 실행에 전부(상한 = 41 GB)** — 측정 없는 첫 실행이 5시간을 넘을 수 있어 훑기의 다른 레인과 480 분을 다툰다.
  정한 개수로 매일 잇는 것이 부분 처리 원칙(ADR-0314)과 맞다.
- **Silver 레인마다 "공급자 최신 판 대비 지연" 을 Spark·카탈로그에서 계산** — 판을 정하는 규칙(시도 완전성)은 Silver 갱신에만
  있다. 빠진 것은 결국 "목록에 있는데 Bronze 에 없는 파일" 이므로 그것을 훑기가 직접 센다.
- **ADR-0443 의 원천 나이를 파일 단위로 바꾸기** — 원천의 다른 파일이 신선한 것도 사실이고, 그 규칙은 수집기 정지를 잡는다.
  빠진 파일은 별도 신호다.
