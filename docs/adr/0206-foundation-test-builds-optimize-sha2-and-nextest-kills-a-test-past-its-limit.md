# ADR 0206: Foundation 시험 빌드는 sha2 를 최적화하고, 제한 시간을 넘긴 시험은 nextest 가 이름으로 죽인다

- Status: Accepted
- Date: 2026-10-13
- Amends: [ADR-0004](./0004-verification-ssot.md) "Foundation 외부 라이브러리의 빌드 프로필" (외부 의존성의
  최적화 수준은 바꾸지 않는다 → `sha2` 하나만 바꾼다)

## Context

Foundation `Rust fmt, unit tests, clippy` 잡의 시험 실행에서 가장 긴 바이너리는
`foundation-outbox-publisher` 의 라이브러리 시험(167개)이다. 2026-10-13 merge_group 실행에서 106초,
두 번째로 긴 `foundation-api` 의 `deploy_contract`(26개)가 42초, 나머지 바이너리는 모두 3초 미만이었다.

라이브러리 시험을 cargo-nextest 로 시험마다 재었다(노트북, 4 동시).

| 시험 | 시간 |
|---|---|
| `building_register_floor_silver_export::committed_inputs::tests::completed_handoff_reuses_exact_bytes_and_rejects_changed_artifacts` | 89.2초 |
| `…floor_scratch::tests::interleaved_rows_preserve_exact_export_bytes` | 23.1초 |
| `…interleaved_group_tests::exact_zip_interleaved_building_matches_full_input_handoff_and_context` | 13.0초 |
| `…interleaved_group_tests::later_building_witness_resolves_earlier_proposal_across_interleaving` | 12.6초 |
| `…interleaved_group_tests::multiple_attics_and_positive_first_title_duplicate_keep_full_group_proposals` | 12.0초 |
| `…tests::exports_building_register_floor_bronze_json_to_silver_handoff` | 11.0초 |

모두 층(FLOOR) Silver 내보내기를 부른다. 행 두 개짜리 시험 하나를 단계별로 재니 내보내기 자체는 16ms 였고,
준비 요약(`ready_summary::prepare`)에서 12.7초를 썼다. 요약의 재사용 증거가 **생산자 실행 파일**
(`current_exe`)의 SHA-256 을 담는데, 시험에서 그 실행 파일은 시험 바이너리(노트북 80MB)다. `sha2` 가 dev
프로필(`opt-level = 0`)로 빌드되어 초당 6MB 로 해시했다. 89초짜리 시험은 내보내기를 약 열 번 부른다.

같은 `sha2 = 0.10.9` 로 같은 80MB 를 해시했다: 최적화 없음 6.57초, `sha2` 만 `opt-level = 3`(호출하는
쪽은 그대로 0) 0.33초.

DuckDB 를 먼저 의심했다(층 내보내기의 임시 저장소). `libduckdb-sys` 만 `opt-level = 1` 로 올려 다시 쟀으나
시험 시간은 그대로였다(19.4·14.2·13.5초). 원인이 아니어서 채택하지 않았다.

`deploy_contract` 의 42초는 `postgres_recovery_contract_is_encrypted_bounded_and_rehearsable` 하나가
`scripts/deploy/foundation-release-test.sh` 를 돌리는 시간이다(4코어 서버 실측 37초, `python3` 실행 약 385회).
배포 스크립트 자체의 리허설이라 이 결정의 범위 밖이다. nextest 아래에서는 다른 시험과 동시에 돌므로 벽시계의
하한(약 40초)이 될 뿐 더해지지 않는다.

## Decision

1. Foundation 작업 공간 `Cargo.toml` 에 `[profile.dev.package.sha2] opt-level = 3` 을 둔다. 바뀌는 것은
   `sha2` 의 최적화 수준 하나다. 디버그 정보·단언·정수 넘침 검사와 다른 의존성은 ADR-0004 그대로다. `check`
   와 `test` 는 같은 dev 프로필을 쓰므로 두 그래프의 설정은 계속 같다.
2. Foundation 작업 공간 `.config/nextest.toml` 의 기본 프로필은 시험 하나를 60초(20초 × 3)에 죽이고 그
   시험 이름으로 실패시킨다. 위 리허설 시험 하나만 자기 이름의 재정의로 120초(30초 × 4)를 받는다. 다른 시험의
   한도는 올라가지 않는다. `cargo xtask verify foundation` 이 nextest 로 시험을 돌리면(ADR-0205) 이 설정을 읽는다.
3. 증명: `sha2` 가 최적화되지 않은 빌드에서 이 설정으로 돌리면 89초짜리 시험이
   `TIMEOUT [60.343s] …completed_handoff_reuses_exact_bytes_and_rejects_changed_artifacts` 로 실패했다.

## Consequences

- `sha2` 의 프로필이 바뀌어 rust-cache 키가 한 번 바뀐다. 다음 main 실행이 의존성을 다시 빌드해 저장한다.
- 시험 하나가 어떤 이유로든 1분을 넘으면, 잡 시간을 조용히 늘리는 대신 이름이 찍힌 실패가 된다.
  한도를 올리려면 그 시험의 이름으로 재정의를 더하고 이유를 적는다.
- 생산자 증거의 의미(실행 파일 전체 바이트의 SHA-256)는 바뀌지 않는다.
