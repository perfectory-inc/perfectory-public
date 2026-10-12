# ADR 0197: CI 는 고정 릴리스 바이너리를 캐시·재시도·체크섬을 가진 한 개의 내려받기 도구로만 받는다

- Status: Accepted
- Date: 2026-10-12
- Amends: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (cargo-deny 설치가 쓰는 내려받기)

## Context

2026-10-12 하루에 머지 큐에서 PR 이 세 번 떨어졌고, 셋 다 코드가 아니라 GitHub 릴리스 CDN 의 순간 오류였다.

1. Identity `Rust quality`: 저장소 가드 안의 actionlint 내려받기가 `curl: (22) ... 504`.
2. Foundation `Verify exact static-release executable toolchain`: Martin 압축본이 `HTTP Error 500`.
3. 같은 날 앞서 #424 에서 Identity 가 같은 모양으로 한 번 더.

전수 조사(워크플로 12개와 그것이 부르는 `scripts/`)에서 잡 실행 중에 고정 릴리스 자산을 받는 곳은 여섯이었다.

| 무엇 | 어디서 | 재시도 | 캐시 |
| --- | --- | --- | --- |
| actionlint | `scripts/ci/actionlint.sh` (가드 체인, 모든 `cargo xtask verify` 잡) | 없음 | 러너 `/tmp` 뿐(잡마다 비어 있음) |
| gitleaks | `scripts/ci/gitleaks-scan.sh` | 없음 | 없음 |
| OSV-Scanner | `scripts/ci/osv-vulnerability-gate.sh` | 없음 | 없음 |
| cargo-deny | `scripts/ci/install-cargo-deny.sh` | curl `--retry 5`(대기 1–16초) | 없음 |
| syft | `foundation-ci.yml` 인라인 | 없음 | 없음 |
| Martin·pmtiles | `scripts/tiles/static_release_toolchain_contract.py` | 없음 | 없음 |

같은 일(“고정된 파일을 받아 체크섬을 확인한다”)이 여섯 벌로 있었고, 재시도가 있는 것은 하나, 캐시는 없었다.
URL 모양도 각 스크립트 안에 흩어져 있었다(syft 는 핀까지 워크플로 안에 있었다).

툴체인 설치(rustup)와 패키지 레지스트리(crates.io, npm, apt, 컨테이너 이미지)는 이 결정의 대상이 아니다.
rustup·cargo 는 ADR-0155 의 `scripts/ci/rust-setup.sh` 가 이미 재시도·백오프를 갖고, 나머지는 각 도구가
스스로 재시도한다. 릴리스 호스트에서만 도는 이미지 빌드의 내려받기(`Dockerfile.lakehouse-control` 의
cargo-chef, RAON 에이전트 `.deb`)도 CI 잡이 아니므로 여기서 다루지 않는다.

## Decision

1. **내려받기 도구는 `scripts/ci/fetch_pinned.py` 하나다.** 셸 스크립트·워크플로·파이썬 설치기는 모두 이것을
   부른다(`tool <이름> --output <경로>`, 또는 모듈의 `fetch(url, sha256)`).
2. **캐시는 내용 주소다.** `~/.cache/perfectory/pinned/<sha256>/<자산 이름>`(`PERFECTORY_PINNED_CACHE` 로 바꿀 수
   있다). 들어올 때와 읽을 때마다 핀과 대조하고, 어긋난 캐시는 버리고 다시 받는다.
3. **찬 캐시 채우기는 재시도한다.** 연결 오류·시간 초과·잘린 본문·HTTP 408/425/429/5xx 는 5, 15, 45, 90초를
   기다리며 최대 다섯 번 시도한다(약 2분 30초). 404·403 처럼 다시 해도 같은 오류는 즉시 실패한다.
4. **체크섬 불일치는 공급망 게이트다.** 재시도하지 않고, 캐시에 남기지 않고, 즉시 실패한다. 이 성질은 보안
   게이트이므로 “재시도 후 성공”과 섞지 않는다.
5. **핀은 그대로 한 곳, URL 모양도 한 곳.** 버전과 SHA-256 은 여전히 각 `tools/*.env`(syft 는 새
   `tools/syft.env`)와 정적 릴리스 계약에 있다. 자산 URL 모양은 `tools/pinned-artifacts.json` 에만 있다.
6. **CI 는 그 캐시를 `actions/cache` 로 나른다.** 고정 바이너리를 받는 잡 13개는 checkout 직후 같은
   `Restore pinned release binaries` 단계를 둔다(접두사 복원이라 키에 해시가 없다). 저장은 main 과 매일 예약
   실행만 한다: 리눅스는 `required/repository` 잡, 윈도는 Foundation 의 윈도 툴체인 잡이 `prefetch --prune` 으로
   그 플랫폼의 모든 핀을 채우고 `pinned-release-binaries-v1-<OS>-<핀 파일 해시>` 로 저장한다. PR·머지 큐는 읽기만
   한다(ADR-0155 의 main 전용 저장과 같다). 저장소 캐시 한도(10GB·7일) 안에서 리눅스 약 150MB, 윈도 약 50MB다.
7. **가드: `scripts/guard/pinned-downloads.sh`**(자체 시험 `pinned-downloads-self-test.sh`). 막는 실제 사고:
   새 스크립트가 다시 `curl` 로 릴리스 자산을 받거나, 새 잡이 캐시 없이 내려받아 CDN 오류 한 번에 PR 이
   떨어지는 것. 규칙 둘 — `releases/download` 는 레지스트리와 그것이 가리키는 계약 밖의 스크립트·워크플로에
   나오면 안 되고, 내려받기 도구를 (직접 또는 그것을 부르는 스크립트나 `cargo xtask verify` 로) 부르는 잡은
   그보다 앞서 정확히 같은 복원 단계를 가져야 한다. 자체 시험은 실제 HTTP 서버로 500·504 뒤 성공과 캐시
   적중(망 0회), 변조 본문 거부, 404 즉시 실패, 잘린 본문 재시도를 확인하고, 가드에 위반 넷을 심어 거부를
   확인한다.

검토한 대안:

- **우리 R2 나 GitHub 릴리스에 거울을 둔다.** 거울을 채우고 갱신하는 일과 자격 증명이 새로 생긴다. GitHub
  릴리스 거울은 같은 CDN 이라 이번 오류를 피하지 못한다. 캐시(Azure 저장소)는 다른 경로이고 비용이 없다.
- **curl `--retry` 만 붙인다.** 재시도는 남지만 여섯 벌의 사본과 캐시 없음은 그대로다. 매 잡이 여전히 망에
  닿는다.
- **잡별 캐시 키.** 잡 수만큼 같은 파일을 저장해 한도를 낭비하고, 한 잡이 먼저 저장한 키에 다른 잡의 파일이
  빠지는 문제가 생긴다. 내용 주소 디렉터리 하나를 플랫폼별로 한 번 채우는 쪽이 단순하다.
- **로컬 복합 액션으로 복원 단계를 한 번만 적는다.** 저장소 정책이 로컬 액션을 허용하지 않는다
  (`scripts/guard/check-workflow-policy.sh`). 그래서 같은 단계를 13번 적고, 가드가 전부 한 글자도 다르지 않음을
  확인한다.

## Consequences

- 캐시가 따뜻한 잡은 고정 바이너리 때문에 망에 닿지 않는다. 찬 캐시(핀을 올린 PR, 7일 무사용 뒤)에서는 약
  2분 30초까지 재시도한 뒤에만 실패한다.
- 핀을 올리면 그 PR 은 새 파일을 재시도와 함께 받고, 병합 뒤 main 이 새 키로 저장한다. 접두사 복원이라 다른
  도구의 파일은 이전 캐시에서 그대로 온다.
- 로컬(노트북)에서도 같은 캐시를 써서, 가드를 두 번째 돌릴 때부터 actionlint 를 받지 않는다. 검증 이미지는
  `PERFECTORY_PINNED_CACHE=/opt/perfectory-pinned` 에 actionlint 를 미리 채운다.
- 남은 망 의존: 컨테이너 이미지 당겨오기, apt, crates.io, npm, rustup. 각자 재시도는 있으나 캐시는 없다. 이들이
  PR 을 떨어뜨리는 일이 측정되면 별도 결정으로 다룬다.
