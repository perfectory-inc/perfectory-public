---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-28
---

# ADR 0004: 검증 단일 진실 원천(`cargo xtask verify`)

## 배경

모노레포의 첫 실제 CI 실행에서 반복적인 실패 유형이 드러났다. 로컬에서는 성공한
검증이 CI에서 실패했고, 한 CI 작업을 고치면 다음 실패가 나타났다. 근본 원인은
“영역을 어떻게 검증하는가”에 대한 단일 정의가 없었던 것이다. 같은 fmt·clippy·test
로직이 다섯 곳에 수작업으로 작성되어 서로 달라졌다.

| 검증기 | `--all-features` | `--locked` | 네이티브 의존성(cmake) |
|---|---|---|---|
| `scripts/verify/cargo-verify.sh`(로컬) | 사용 | 사용 | cmake+sasl 설치 |
| `foundation-ci.yml` | 사용 | 사용 | 없음 |
| `gongzzang-ci.yml` | 사용 | 미사용 | 없음 |
| `identity-ci.yml` | 불일치 | 미사용 | 없음 |
| `intelligence-ci.yml` | **미사용** | 미사용 | **없음**(rdkafka가 cmake 필요) |

각 위치에서 “성공”의 의미가 달랐기 때문에 로컬 성공이 CI 성공을 보장하지 못했다.
clippy 플래그 차이, rdkafka용 cmake 누락, 서로 다른 테스트 범위가 모두 이 틈에서
발생했다. 이는 루트 `AGENTS.md`의 최상위 원칙인 근본 원인 제거·SSOT·재발 방지에
어긋난다.

## 결정

**영역 검증의 정의는 `cargo xtask verify <area>` 하나뿐이다.**

- 루트 `.cargo/config.toml` 별칭으로 실행하는 독립 Rust 크레이트 `tools/xtask`가
  영역별 네이티브 의존성과 하나의 fmt/clippy/test 정책을 소유한다.
- 로컬(`scripts/verify/cargo-verify.sh`, Docker 내부)과 CI(`.github/workflows/*-ci.yml`
  Rust 작업)는 모두 `cargo xtask verify <area>`를 호출한다. 어느 쪽도 cargo 명령을
  직접 조합하지 않는다.
- 정본 정책은 변형 하나만 둔다.
  - `cargo fmt --all -- --check`
  - `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`
  - `cargo test --locked --workspace --all-features`
    (gongzzang은 workspace 전체에서 DB 기능 테스트를 제외한 뒤 persistence 크레이트의
    비DB 모음을 별도로 실행한다. 이 2단계 계약은 YAML이 아니라 xtask에 있다.)
  - 영역별 네이티브 의존성도 xtask가 선언한다. 현재 `intelligence`만 rdkafka용
    cmake+libsasl2가 필요하며, Debian 계열에서는 apt로 설치하고 root가 아니면 sudo를
    사용한다.
- **가드:** `scripts/guard/no-adhoc-cargo-lint.sh`는 `cargo xtask` 밖에서 raw
  `cargo clippy` 또는 `cargo fmt`를 포함한 workflow를 실패시킨다. 드리프트가 다시
  들어올 수 없다.

Rust로 구현한 이유는 언어 정책과 대규모 저장소의 검증 사례(oxidecomputer/omicron의
`cargo xtask`)에 맞추기 위해서다. `xtask`는 표준 라이브러리만 사용하는 무의존성
도구다.

## 범위(단계별)

- **1단계(이 ADR):** 장애를 일으킨 fmt/clippy/test 3종을 통합한다. 모든 영역의
  Rust 품질 작업이 대상이다.
- **2단계(후속):** `xtask verify --full`이 임시 Postgres를 띄워 `--ignored` DB 통합
  테스트와 Compose smoke까지 조정하도록 확장한다. 그러면 로컬 하네스가 CI 전체 표면을
  실행해 현재의 범위 차이를 닫는다. 그 전까지 DB 통합·Compose·frontend 작업은 기존
  명령을 유지하며 가드 예외로 둔다.

## 결과

- 새 영역을 추가하거나 검증 정책을 바꿀 때 `tools/xtask`의 데이터 한 줄만 수정하면
  모든 소비자가 함께 바뀐다. YAML을 여러 곳 수정할 필요가 없고 드리프트도 없다.
- `cargo xtask verify all`로 전체 모노레포의 Rust 품질 검증을 로컬에서 재현할 수 있다.
- 독립 크레이트 `xtask`는 전역적으로 유일한 이름을 사용하며 어느 영역 workspace에도
  속하지 않는다. 저장소 루트에서 실행한다.
