---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-21
---

# 호스트 크레이트 시험 — 하나뿐인 시험 이미지·공유 컴파일 캐시

[루트 ADR-0601](../../../../docs/adr/0601-the-verification-image-is-the-one-test-image-and-reaches-the-data-host-in-the-signed-release.md).
데이터 호스트(ai-server)에서 Rust·Python 시험은 `perfectory-verify:local` 에서만 돌린다. 그 이미지는
`tools/verify-image/Dockerfile` 하나로 정의되고, 서명된 릴리스에 실려 배포가 태그를 붙인다. 맨 `rust` 이미지에서 apt 를 깔거나
이미지를 손으로 짓지 않는다.

## 확인

```bash
docker image inspect --format '{{.Id}} {{.Created}}' perfectory-verify:local
```

태그가 없으면 그 호스트의 릴리스가 이 이미지를 싣기 전이다(배포 `tag-compose-images` 가 붙인다).

## 실행

작업 트리를 호스트의 자기 scratch 디렉터리에 두고, 대상 볼륨은 실행마다 자기 것을 쓰고 끝나면 지운다(공유 대상은 다른 작업의
낡은 크레이트를 내준다). 컴파일 캐시는 볼륨 하나 `perfectory-sccache` 를 모두가 나눈다.

```bash
docker run --rm --network none \
  -e RUSTC_WRAPPER=sccache -e CARGO_INCREMENTAL=0 \
  -v perfectory-sccache:/sccache \
  -v perfectory-cargo-registry:/usr/local/cargo/registry \
  -v "<작업 디렉터리>:/work" -v "<내 대상 볼륨>:/work/platforms/foundation-platform/target" \
  -w /work/platforms/foundation-platform \
  perfectory-verify:local cargo test -p <크레이트>
```

- `RUSTC_WRAPPER` 는 이미지에 없다: `cargo-verify.sh` 가 증분 빌드로 돌게 두려는 것이다. `CARGO_INCREMENTAL=0` 은 CI 의
  `scripts/ci/rust-setup.sh` 값이고, sccache 는 증분 단위를 캐시하지 못한다.
- 레지스트리가 비어 있으면 첫 실행만 `--network none` 을 빼고 `cargo fetch` 를 한 번 한다.
- `SCCACHE_DIR=/sccache`, `SCCACHE_CACHE_SIZE=20G` 는 이미지가 싣는다. 캐시 크기는 `docker system df -v | grep perfectory-sccache`.

## 이미지 입력을 바꾸는 브랜치

`perfectory-verify:local` 은 배포된 main 의 환경이다. 브랜치가 `apt_deps`, `rust-toolchain.toml`, `tools/sccache.env`,
`tools/container-images.env` 의 기반, `tools/verify-image/Dockerfile` 을 바꾸면 그 환경은 아직 없다. 그때만 자기 작업 트리에서
임시 태그를 짓고(`docker build -f tools/verify-image/Dockerfile -t perfectory-verify:<작업 ID> .`), 시험이 끝나면
`docker image rm perfectory-verify:<작업 ID>` 로 지운다.

## 바꾸기

apt 목록은 `tools/xtask/src/main.rs` 의 `apt_deps`, 도구체인은 `rust-toolchain.toml`, 기반은 `tools/container-images.env`,
sccache 는 `tools/sccache.env` 에서 바꾼다. Dockerfile 이 다른 값을 가지면 `technology-version-consistency` 가 실패한다.
