# ADR 0620: AWS 이미지는 GitHub 의 arm 러너에서 arm64 로 짓고, 릴리스 워크플로의 이름 있는 둘째 잡이 서명한다

- Status: Accepted
- Date: 2026-11-25
- Amends: [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md) §3 의 "서명하는 잡은 하나". 이제 같은 워크플로의 이름 있는 잡 둘이다.
  ADR-0276 의 나머지는 그대로다. 데이터 호스트의 릴리스가 무엇이고 어떻게 검증되는지는 바뀌지 않는다.
- 함께 읽는 것: 루트 ADR-0619 §7(이미지는 GHCR 의 공개 패키지에서, digest 로). ADR-0616 §2-1(코드 위치 이미지가 발행기를 함께 싣는다).

## Context

**사장 결정(2026-11-25): AWS 의 계산은 ARM(Graviton)이다.** AWS 에서 도는 이미지는 linux/arm64 로 짓는다.
ai-server 가 아직 돌리는 이미지는 amd64 도 함께 둔다. 도구는 GitHub 의 무료 arm 러너에서 짓는다. CI 비용은 0 이다.

**지금의 릴리스 빌드(ADR-0276).** `foundation-release-build.yml` 의 `build` 잡 하나가 main 의 커밋마다 릴리스를 짓는다(ubuntu-24.04, x86).
그 잡이 Sigstore 로 서명하고 `foundation-release` 패키지에 올린다. 정책(`tools/conftest/workflow/contracts.rego`)은 두 가지를 막는다.

- 서명하는 잡(`id-token: write`)이 둘 이상인 워크플로.
- 패키지를 쓰는 잡(`packages: write`)이 서명 잡이 아닌 것.

믿는 러너는 `ubuntu-24.04` 와 `windows-2022` 뿐이다. 릴리스의 이미지는 모두 amd64 하나짜리다.
호스트의 승인(`foundation-release-admission.py`)은 그 모양을 전제한다(이미지마다 매니페스트 하나, 정해진 이미지 이름).

**재 본 것.**

- 발행기의 Rust 빌드는 번들 DuckDB(C++)와 librdkafka·BoringSSL 을 짓는다. 릴리스 잡에서 31 분이고, 그중 cargo-chef 의 의존성 층이 25 분이다(ADR-0276 §14).
  에뮬레이션(QEMU)으로 arm64 를 지으면 몇 배가 든다. 그래서 arm64 는 arm 러너에서 그대로 짓는다.
- 기본 이미지(`rust:1.99.1-bookworm`, `debian:bookworm-slim`, `python:3.14.10-slim-trixie`, `moby/buildkit:v0.33.0`)의 고정 digest 는 모두 arm64 를 품은 색인이다(2026-11-25 레지스트리에서 읽음).
- cargo-chef v0.1.78 은 aarch64 musl 판을 낸다. 체크섬은 공식 `.sha256` 과 같다.
- DuckDB v2.0.0 의 확장 넷(httpfs·avro·iceberg·spatial)은 linux_arm64 판이 있다.
  파일 끝의 메타데이터가 v2.0.0·linux_arm64 와, 계약의 x86 판과 같은 `extension_version` 을 적는다.
- Dagster 코드 위치의 의존성은 모두 cp314 manylinux aarch64 휠이 있다(`uv pip compile --only-binary :all:` 를 두 플랫폼에 대해 돌림).

## Decision

### 1. 무엇을 짓나

| 이미지 | 플랫폼 | 어디서 | 태그(`foundation-release` 패키지) |
|---|---|---|---|
| 발행기 | linux/amd64 + linux/arm64 의 색인 | amd64 = 릴리스 잡이 지은 그 매니페스트(같은 바이트), arm64 = 아래 잡 | `<commit>-aws-publisher` |
| 코드 위치(`orchestration/dagster/Dockerfile.aws`) | linux/arm64 만 | 아래 잡 | `<commit>-aws-code-location` |

코드 위치는 AWS 만 돌린다. 그래서 amd64 를 짓지 않는다. ai-server 의 Dagster 는 지금의 이미지 그대로다.
발행기의 amd64 를 다시 짓지 않는 것은 ai-server 가 돌리는 것과 AWS 의 amd64 가 같은 바이트여야 하기 때문이다.
코드 위치에는 그 커밋의 arm64 발행기와, 계약이 `linux-aarch64` 로 고정한 확장 파일이 발행기 옆에 든다(`duckdb_extensions::release_directory`).
의존성은 `requirements-aws.txt` 다. 두 플랫폼의 해시를 가진 잠금(`uv pip compile --universal --generate-hashes`)이다.

### 2. 누가 짓고 서명하나

`foundation-release-build.yml` 에 잡 `aws-images` 를 더한다. 이 잡은 `build` 가 끝난 뒤 `ubuntu-24.04-arm` 에서 돈다.

1. 발행기를 linux/arm64 로 짓는다. 릴리스 빌드와 같은 `build_image`, 같은 Dockerfile, 같은 빌드 인자다. BuildKit 캐시는 `buildcache-publisher-arm64` 다.
2. 코드 위치를 짓는다.
3. 그 이미지 안의 발행기가 망 없이 제 확장을 싣는지 본다(`verify-lakehouse-engine-extensions`). Dagster 를 import 해 본다.
4. 매니페스트를 digest 그대로 올리고 다시 읽어 견준다.
5. 발행기 색인을 `docker buildx imagetools create` 로 만든다.
6. 두 digest 를 `actions/attest-build-provenance` 로 서명한다.

빌드 스크립트는 `scripts/deploy/aws-images-build.py` 하나다. 이 스크립트는 릴리스 빌드의 모듈을 그대로 불러 쓴다.

서명하는 잡은 이제 둘이다. 정책은 이름으로 정한다. `release_signing_jobs := {"build", "aws-images"}` 밖의 이름이 서명하면 거부한다.
둘 다 같은 워크플로라 Sigstore 인증서의 신원(`…/foundation-release-build.yml@refs/heads/main`)이 같다. 릴리스를 검증하는 쪽이 보는 신원은 그대로다.
`aws-images` 는 `build` 뒤에 돈다. 그래서 릴리스와 호스트 배포를 늦추지 않는다. 실패해도 릴리스는 이미 올라가 있다.

`ubuntu-24.04-arm` 을 믿는 러너에 더한다(GitHub 호스팅, 공개 저장소에 무료).

### 3. 발행기 Dockerfile 하나가 두 플랫폼을 짓는다

`Dockerfile.lakehouse-control` 은 cargo-chef 의 두 아카이브를 각자의 체크섬으로 받는다. `TARGETARCH` 가 가리키는 것만 푼다.
그 밖의 값이면 빌드가 멈춘다. 단계 이름에 변수를 쓰는 `FROM chef-${TARGETARCH}` 는 `check-container-runtime-policy.sh` 가 거부한다(고정 digest 가 없는 FROM).
그래서 이 모양을 쓰지 않는다.

확장 계약(`duckdb-extensions.contract.json`)은 배포판마다 `linux-x86_64` 와 `linux-aarch64` 를 고정한다.
하나뿐이던 `platform` 칸은 지운다. 발행기는 제 플랫폼 키(`<os>-<arch>`)로 고르고, 없으면 고정된 키들을 이름 대며 거부한다.

## Consequences

- AWS 의 태스크 정의는 `<commit>-aws-code-location` 의 digest 를 가리킨다(A1). 서명은 `gh attestation verify oci://…` 로 확인된다.
- main 의 릴리스 커밋마다 arm 러너 잡이 하나 더 돈다. 처음 한 번은 캐시가 없어 의존성 층부터 짓는다. 시간은 첫 run 에서 잰다.
- 캐시 태그와 AWS 이미지 태그가 `foundation-release` 패키지에 더해진다. 새 패키지를 만들지 않는다. 새 GHCR 패키지는 비공개로 시작하고, 공개로 바꾸는 API 가 없다.
- 호스트의 릴리스 승인과 매니페스트는 바뀌지 않는다. AWS 이미지는 `manifest.json` 의 `images` 에 들지 않는다.

## 기각한 대안

- **`build` 가 arm64 잡을 기다린다(서명 잡 하나 유지).** 릴리스와 모든 호스트 배포가 arm64 빌드만큼 늦어진다.
- **QEMU 로 x86 러너에서 arm64.** Rust·C++ 빌드가 몇 배 느리다.
- **x86 에서 교차 컴파일.** C/C++ 의존성(DuckDB·librdkafka·BoringSSL·libclang bindgen)마다 교차 도구가 든다.
- **별도 워크플로가 서명.** 인증서의 신원이 둘이 되고, 검증하는 쪽이 둘을 알아야 한다.
- **코드 위치도 amd64 로.** 그것을 돌리는 곳이 없다.

| 구분 | 내용 |
|---|---|
| 기본 기능 | GitHub 의 arm64 러너, BuildKit `--platform`·이름 있는 빌드 문맥(`oci-layout://`), `imagetools create`, `attest-build-provenance` |
| 임시 | 발행기 색인의 amd64(ai-server 가 릴리스를 돌리는 동안) |
| 운영 | `aws-images` 잡, `aws-images-build.py`, `Dockerfile.aws`, `requirements-aws.txt`, 정책의 `release_signing_jobs` |

2026-11-26 개정 주(W107). 결정은 그대로다. 본문이 코드와 다르던 곳과 더한 것:

- §1: 이미지는 셋이다. 런타임(`services/foundation-api/Dockerfile` 의 compose 빌드, linux/arm64 만, `<commit>-aws-runtime`)이
  foundation-migrate 를 싣는다(ADR-0616 A1b). 플랫폼마다의 매니페스트도 `<commit>-aws-<이름>-arm64` 로 올라간다.
- §2: 서명하는 주제에 기록(`aws-images.json`)이 더해지고, 서명 뒤에 그 기록이 `<commit>-aws-images` 로 올라간다. 적용은 그것 하나를
  입력으로 받는다(ADR-0616 §2-4).
- §2: 이 잡의 고정 내려받기 캐시는 제 키(`pinned-release-binaries-v1-<os>-<arch>`)를 쓰고 main 에서 스스로 채운다. 두 러너가 같은
  `Linux` 키를 써서 x86 캐시를 받았고, 그 안에 없는 aarch64 파일을 run 마다 망에서 받았다(ADR-0559 §6).
- §2 의 skopeo 는 그대로 둔다. `docker buildx imagetools` 는 로컬 OCI 레이아웃의 매니페스트를 바이트 그대로 올리지 못한다.
- §1(2026-11-26, W107 의 둘째): 넷째 이미지는 Dagster 의 것(`orchestration/dagster/Dockerfile.aws-dagster`, linux/arm64 만)이다. 웹서버와
  데몬이 돌리고 위치의 코드는 없다. 태그는 `aws-dagster-<입력 넷의 해시>` 이고, 그 태그가 패키지에 없을 때만 짓는다. 그래서 코드 릴리스는
  그 digest 를 바꾸지 않는다(ADR-0616 §2-2). 코드 위치 이미지는 위치의 gRPC 코드 서버와 run 태스크가 돌린다.
