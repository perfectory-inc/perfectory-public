# ADR 0207: 데이터 흐름 화면은 한국에서 서비스 주소까지의 경로와 PR CI 시간 추이를 보인다

- Status: Proposed
- Date: 2026-10-13
- Related: [ADR-0195](./0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md) (데이터 흐름 지도),
  [ADR-0203](./0203-pull-request-ci-has-one-wall-clock-budget.md) (PR CI 벽시계 예산),
  [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §1 (호스트 점검은 systemd 타이머)

## Context

두 숫자가 화면에 없어 매번 손으로 쟀다.

- **한국 방문자가 들어가는 Cloudflare 위치.** 2026-10-12 부터 한국 방문이 서울(ICN)이 아니라 SEA·LAX 로
  들어가 모든 주소가 p50 2.5–3 s 로 느려졌고, 이를 코드 회귀로 오인해 Worker 배포가 세 번 물러났다. 2026-10-13
  노트북(한국)에서 `/cdn-cgi/trace` 를 10번씩 물으니 catalog·buildings·tiles 여섯 주소가 전부 LHR/AMS 로
  들어갔고(p50 746–1,036 ms, p95 1,742–3,869 ms), 같은 시각 `cloudflare.com` 은 ICN 이었다.
- **PR CI 대기 시간 추이.** ADR-0203 의 주간 워크플로가 재지만 결과는 실행 요약과 7일짜리 아티팩트에만
  있다. 아티팩트 내려받기는 공개 저장소여도 인증이 필요하다(익명 401 확인).

## Decision

1. **호스트 점검 하나.** `infra/systemd/foundation-service-checks.service`/`.timer`(매시 41분, 데이터 작업이
   아니므로 ADR-0118 §1 대로 systemd 타이머, `OnFailure` 는 Slack)가 `scripts/ops/service-checks.py`
   (표준 라이브러리만)를 돌려 파일 하나 `/var/lib/foundation-platform/service-checks/service-checks.json` 을
   원자적으로 쓴다. 릴리스가 유닛을 설치·활성화하고 디렉터리를 만든다.
2. **경로 점검(edge).** 대상 주소는 `config/r2-connections.contract.json` 의 `parcel_by_pnu_gateway`·
   `building_by_pnu_gateway`·`vector_tile_gateway` 의 `public_hostname` 과 별칭에서 읽는다(여기 다시 적지
   않는다). 주소마다 `/cdn-cgi/trace` 를 새 연결로 10번 물어 응답 시간의 p50/p95(최근접 순위, ADR-0203 과
   같은 정의)와 가장 많이 나온 `colo` 를 남긴다. 이 경로는 Cloudflare 가장자리가 답하므로 Worker 가 돌지
   않고 과금도 없다. 재는 것은 들어가는 길이지 Worker·R2 시간이 아니다(그것은 by-PNU 서빙 모니터 몫).
   `cloudflare.com` 의 `colo` 를 기준으로 함께 남겨, 우리 주소만 서울을 벗어나면 요금제의 경로 배정이
   원인임을 화면이 말한다.
3. **CI 추이(ci).** 계산은 ADR-0203 의 `scripts/ci/ci_time_budget.py` 를 그대로 쓴다(`fetch_runs`·
   `group_attempts`·`weekly_trend`·`load_budget`). 이 파일은 플랫폼 릴리스 트리 밖이라 배포가 유지하는 제어
   체크아웃(`/opt/perfectory-control/current`)에서 읽는다. GitHub 실행 API 는 익명이고 호스트의 시간당
   60회는 자동 배포와 나눠 쓴다. 그래서 UTC 하루치 실행을 그날이 끝나고 3시간 뒤 **한 번만** 가져와 파일
   옆(`ci-runs/`)에 두고, 매번 `/rate_limit`(횟수에 안 셈)을 물어 남은 횟수가 자동 배포 몫 30 + 하루치 20
   미만이면 다음 시간으로 미룬다. 주는 그 주 7일과 다음 월요일이 다 모였을 때만 보인다. 첫 8주 채우기는 몇
   시간 걸리고, 그 뒤로는 하루 몇 번이다. 붙여 둔 날 파일은 실행 API 에서 다시 만들 수 있는 캐시다.
4. **서빙.** foundation-api 가 `FOUNDATION_PLATFORM_SERVICE_CHECKS_PATH`(`compose.orchestration-bridge.yml`
   이 읽기 전용으로 마운트)의 파일을 `GET /data-catalog/v1/service-checks` 로 그대로 돌려준다. 파일이 없거나
   모양이 다르면 500 이 아니라 `error` 칸으로 말한다. 더니어는 이 경로를 중계하고 데이터 흐름 화면의
   「서비스 점검」 탭에 주소별 colo·p50·p95 와 한 줄, 주별 CI p50/p90 막대와 예산선을 보인다.

## Consequences

- 한국 경로 문제는 코드 문제와 화면에서 구분된다. 자동 판정·경보는 하지 않는다: 경로를 바꾸려면 유료
  요금제가 필요하고 그것은 사장 결정이다.
- 호스트가 한 시간에 약 61번(주소 6개 × 10 + 기준 1) 가장자리에 묻는다. 비용 없음.
- 주간 워크플로와 호스트가 같은 계산을 각자 돌린다. 계산 코드는 하나(ADR-0203 스크립트)라 숫자가 갈리지
  않는다.

## Rejected

- **아티팩트를 내려받기**: 인증 토큰이 필요하다. 새 비밀을 호스트에 두지 않는다.
- **워크플로가 결과를 저장소 브랜치·Pages 에 쓰기**: 공개 워크플로는 `contents: read` 만 허용된다
  (`scripts/guard/check-workflow-policy.sh`).
- **8주를 한 번에 다시 재기**: 2026-10-12 하루만 PR 실행 1,608건이다. 익명 60회로는 한 번에 못 끝내고 자동
  배포의 몫까지 먹는다.
- **blackbox exporter 등 새 서비스**: 표준 라이브러리 스크립트 하나로 충분하다.
