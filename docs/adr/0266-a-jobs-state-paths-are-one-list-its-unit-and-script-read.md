# ADR 0266: 작업의 상태 경로는 목록 하나이고, 유닛과 스크립트가 그것을 읽는다

- Status: Accepted
- Date: 2026-10-16
- Builds on: [ADR-0243](./0243-the-parcel-registry-is-registered-region-by-region-by-a-scheduled-job.md), ADR-0265 (PR #645, 대조 관문이 원장을 기다린다)

## Context

필지 ID 원장 작업 `parcel_registry`는 시작하자마자 죽었다.

```
mkdir: cannot create directory /var/lib/foundation-platform/map-edit-fold/lakehouse/parcel-registry: Read-only file system
```

그래서 `silver.parcel_registry`가 한 번도 생기지 않았다.

- 유닛 `foundation-parcel-registry.service`는 `ProtectSystem=strict`이고, `ReadWritePaths`에
  `/var/lib/foundation-platform/lineage-stewardship`가 있다.
- 스크립트의 기본값도 그 아래(`lineage-stewardship/lakehouse`)다.
- 그런데 유닛이 함께 읽는 `map-edit-fold.env`가 `FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT`를 접기 작업의 폴더로 정한다.
  스크립트는 그 값을 먼저 읽는다. 그래서 샌드박스 밖에 쓰려 했다.

같은 변수를 환경 파일에서 읽는 작업 스크립트는 넷이었다. `map-edit-fold.sh`, `admin-boundary-release.sh`,
`complex-boundary-release.sh`, `parcel-registry-cycle.sh`다. 앞의 셋은 우연히 그 값이 자기 샌드박스 안이었다.

경로가 적힌 곳은 셋이었다. 유닛의 `ReadWritePaths`, 스크립트의 기본값, 그리고 저장소 밖 호스트 환경 파일의 값이다.
셋이 서로 맞는지 보는 곳이 없었다.

## Decision

1. **목록은 `orchestration/jobs.v1.json`의 `state_paths` 하나다.** 작업마다 쓰는 절대 경로를 적는다.
   `ProtectSystem=strict` 유닛의 `ReadWritePaths`는 이 목록에서 만든다(`job_specs.py --write-units`). 유닛을 함께 쓰는
   작업들은 같은 목록을 적는다. systemd가 만드는 `StateDirectory`는 따로 둔다.
2. **스크립트는 상태 루트를 그 목록에서 받는다.** `scripts/ops/job-state-root.sh`의 `job_state_root <작업 id>`가
   `job_specs.py --state-root`로 첫 경로를 알려 준다. 위 네 스크립트가 그렇게 바뀐다. 이름 붙은 덮어쓰기 변수
   (`FOUNDATION_<작업>_LAKEHOUSE_STATE_ROOT` 따위)는 시험용이고, 어떤 환경 파일도 담지 않는다.
3. **검사 `state_path_problems()`가 모든 작업에서 거부한다**(`--check-units`, `test_job_specs`).
   - `state_paths`와 다른 유닛 `ReadWritePaths`
   - 같은 유닛을 쓰면서 목록이 다른 작업
   - 유닛의 환경 파일이 담는 변수에서 상태 루트(`*STATE_ROOT*="${원천:-기본값}"`)를 읽는 스크립트
   - 쓸 수 있는 곳(`state_paths`와 `StateDirectory`) 밖의 글자 기본값
   - 시험이 그 각각을 심어서 거부되는 것을 보인다.

## Consequences

- 원장 작업의 감독 첫 실행이 샌드박스 안에서 돌 수 있다.
- 유닛 샌드박스를 바꾸려면 `state_paths`를 고치고 `--write-units`를 돌린다. 손으로 고친 유닛은 시험이 막는다.
- 호스트 `map-edit-fold.env`의 `FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT`는 이제 네 스크립트의 상태 루트를 정하지 않는다.
  그 값은 여전히 Spark 컨테이너의 마운트에 쓰이는데, 스크립트가 자기 경로로 명시해 넘긴다. 다른 작업이 그 값을 다시 읽으면
  검사가 막는다.
