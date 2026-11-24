# ADR 0610: Foundation 시험 빌드는 번들 DuckDB 를 opt-level 1 로 짓고, DuckDB 단계 시험은 기본 제한 시간으로 돌아간다

- Status: Accepted
- Date: 2026-11-23
- Amends: [ADR-0206](./0206-foundation-test-builds-optimize-sha2-and-nextest-kills-a-test-past-its-limit.md) §1(최적화하는
  의존성은 `sha2` 하나), [ADR-0004](./0004-verification-ssot.md) "Foundation 외부 라이브러리의 빌드 프로필"(최적화 수준)
- Related: [ADR-0294](./0294-ci-builds-share-a-compiler-cache-on-r2.md)(R2 의 sccache)

## Context

`foundation-publisher-silver` 의 단계 시험은 행 SQL 을 번들 DuckDB 에서 계획한다. 그 SQL 은 열마다 매크로가 여러 겹이다.
dev 프로필(`opt-level = 0`)은 DuckDB 의 C++ 도 최적화 없이 지었다. 그래서 시험 몇 개가 nextest 의 기본 한도(20 초 × 3)를
넘어 자기 이름의 재정의를 받았다. 하나는 온비드 단계의 역할별 시험(2026-11-19), 다른 하나는 실거래 Gold 문서 시험과
법원경매 물건 검색 시험(2026-11-22)이다.

C++ 는 `libduckdb-sys` 의 build script 가 `cc` 로 짓는다.

- `cc` 는 `-O` 를 Cargo 가 주는 `OPT_LEVEL` 에서 읽는다. build script 를 돌리는 단위의 프로필은 그 패키지의 것이다. 그래서
  `[profile.dev.package.libduckdb-sys]` 의 `opt-level` 하나가 C++ 의 최적화 수준이다.
- `DEBUG` 는 디버그 정보가 켜져 있으면 true 다(ADR-0004 의 `debug = 1`). 그러면 build script 가 `-DDEBUG` 를 정의해
  DuckDB 의 단언(`D_ASSERT`)이 켜진다. `opt-level` 은 이것을 바꾸지 않는다.
- `cc` 1.4.5 는 `RUSTC_WRAPPER` 가 sccache 이면 C/C++ 컴파일러 앞에도 그것을 붙인다. 그래서 DuckDB 의 목적 파일도 R2 의
  sccache(ADR-0294)에 쌓인다. 어떤 플래그로 처음 짓는 빌드만 C++ 를 정말 컴파일한다.

#1652 에서 쟀다(2026-11-23, `ubuntu-24.04` 4 vCPU, Foundation `Rust fmt, unit tests, clippy` 잡). 모든 crate 가 돌았다.
기준(-O0)의 시험 시간은 같은 시각의 #1629 merge-queue 실행이다. 시험 집합이 같다(579 개).

| | 지금(-O0) | opt-level 1 | opt-level 3 |
|---|---|---|---|
| C++ 빌드: 그 플래그로 처음(sccache 에 없음) | 재지 않음 | 18m28s(run 45099699318) | 23m44s(run 45096294896) |
| C++ 빌드: sccache 에 있음, rust-cache 에 없음 | 3m17s(run 45094312623) | 5m43s(run 45102864371) | — |
| `foundation-publisher-silver` 시험 합(579 개) | 857.9 초(run 45093857958) | 221.6 초 / 225.6 초 | — |
| 가장 긴 시험(온비드 물건 상세) | 81.5 초 | 13.3 초 / 12.4 초 | — |
| 20 초를 넘은 시험 | 7 개 | 0 개(가장 긴 것 17.3 초) | — |
| nextest 전체 벽시계(3,123 개) | 257.7 초 | 86.6 초 / 88.7 초 | — |
| 잡 | 11m10s | 27m06s(처음) / 13m36s(sccache 에 있음) | 30 분에 취소 |

opt-level 3 의 첫 빌드는 C++ 만 23m44s 였다. 잡은 시험을 30 초 돌린 뒤 30 분 한도에 취소됐다. ADR-0004 는 CI 의 시간 제한이
"캐시가 없는 최초 네이티브 컴파일과 전체 검사"를 담아야 한다고 정했다. 3 은 그 안에 들지 않는다. DuckDB 판이나 도구 사슬을
올리는 PR 마다 그렇다. merge queue 의 실행도 main 의 옛 캐시에서 시작하므로 마찬가지다.

## Decision

1. Foundation 작업 공간 `Cargo.toml` 에 `[profile.dev.package.libduckdb-sys] opt-level = 1` 을 둔다. 바뀌는 것은
   `libduckdb-sys` 의 최적화 수준 하나다. 디버그 정보, `DEBUG`(DuckDB 의 단언), 정수 넘침 검사와 다른 의존성은 ADR-0004
   그대로다. `check` 와 `test` 는 같은 dev 프로필을 쓰므로 두 그래프의 설정은 계속 같다.
2. 3 은 택하지 않는다. 그 플래그의 첫 빌드가 잡의 30 분을 넘는다(위). 시험 시간으로 얻는 것은 1 에서 이미 대부분이다.
   잡의 한도를 올리는 것은 택하지 않는다. 첫 빌드가 아닌 모든 실행에는 필요 없는 시간이다.
3. `.config/nextest.toml` 의 DuckDB 단계 재정의 둘(온비드 단계의 역할별 시험, 실거래 Gold 문서 시험과 법원경매 물건 검색
   시험)을 지운다. 그 시험들은 기본 한도(20 초 × 3)로 돌아간다. opt-level 1 에서 그중 가장 긴 것이 13.3 초였다.
   한도를 다시 올리려면 ADR-0206 의 규칙대로 그 시험의 이름으로 재정의를 더하고 이유를 적는다.

## Consequences

- main 의 rust-cache 가 opt-level 1 의 DuckDB 를 담은 뒤에는 C++ 를 짓지 않는다. 이 crate 를 돌리는 실행마다 nextest 벽시계가
  약 170 초 준다.
- 프로필이 바뀌어 rust-cache 키가 한 번 바뀐다. main 이 새 캐시를 저장하기 전까지, 캐시에 이 DuckDB 가 없는 실행은
  sccache 에서 C++ 를 받는다. 3m17s 대신 5m43s 다.
- DuckDB 판이나 도구 사슬을 올리면 첫 빌드가 C++ 를 다시 컴파일한다. opt-level 1 에서 약 18 분이고, 잡은 30 분 안에 든다
  (27m06s). 그 빌드가 sccache 를 채우므로 다음 실행부터는 다시 5–6 분이다.
- 노트북은 target 디렉터리마다 한 번 opt-level 1 로 짓는다.
- 시험
  - CI 가 판정한다. 재정의를 지운 시험이 60 초를 넘으면 그 이름으로 실패한다(ADR-0206 §3 과 같은 증명).
  - 시험 내용과 시험 집합은 바뀌지 않는다.
