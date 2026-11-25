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

작업 트리와 대상, 컴파일 캐시는 모두 시스템 디스크(SSD)의 자기 scratch 아래 bind mount 로 둔다. Docker 의 이름 있는 볼륨은
`/data/docker`, 곧 데이터 디스크(HDD, `config/host-layout.contract.json` `docker.data_root`)에 있다. 2026-11-21 에 그 디스크의
I/O 압력은 64% 였다(단가 Spark 이주 중). 그 위의 대상과 sccache 는 모든 작업의 읽기·쓰기를 같은 바늘에 줄 세웠다.

- 대상은 실행마다 자기 것을 쓰고, 끝나면 지운다. 공유 대상은 다른 작업의 낡은 크레이트를 내준다.
- 컴파일 캐시는 `~/claude-scratch/sccache` 하나를 모두가 나눈다. 이미지가 정한 상한은 `SCCACHE_CACHE_SIZE=20G` 다.

시작 전에 SSD 의 여유를 본다. 한 크레이트의 대상과 sccache 가 더 쓸 몫을 빼고도 30 GiB 가 남아야 시작한다: 운영 Postgres 와
DuckDB 단계의 몫이다(단계의 임시 파일 상한은 DuckDB 의 기본값, 처음 쏟을 때 그 디스크 남은 자리의 90% 다).
2026-11-21 ai-server 의 SSD(`/`, `sdb`)는 217 GB 중 82 GB 가 비어 있었다. 그 몫을 빼면 시험에 쓸 몫은 약 52 GB 로, 대상 둘과
sccache 하나가 들어간다. 슬롯(`hostslot.sh`)은 한 번에 하나다. 그래도 이 대조는 DuckDB 단계와 같은 디스크를 나누기 때문에 둔다.

```bash
w=<작업 ID>; scratch=~/claude-scratch
mkdir -p "${scratch}/${w}-target" "${scratch}/sccache"
gib=$((1024 * 1024 * 1024))
reserve=$((30 * gib))
cache="$(du -sb "${scratch}/sccache" | cut -f1)"
# 한 크레이트의 대상(어림 20 GiB)과, sccache 가 상한 20 GiB 까지 더 쓸 몫
need=$((20 * gib + (cache < 20 * gib ? 20 * gib - cache : 0)))
free="$(df --output=avail -B1 "${scratch}" | tail -1)"
if (( free - need < reserve )); then echo "SSD free ${free} - ${need} is under the reserve ${reserve}; wait" >&2; exit 75; fi

docker run --rm --network none \
  -e RUSTC_WRAPPER=sccache -e CARGO_INCREMENTAL=0 \
  -v "${scratch}/sccache:/sccache" \
  -v perfectory-cargo-registry:/usr/local/cargo/registry \
  -v "<작업 디렉터리>:/work" -v "${scratch}/${w}-target:/work/platforms/foundation-platform/target" \
  -w /work/platforms/foundation-platform \
  perfectory-verify:local sh -c "cargo test -p <크레이트>; s=\$?; chown -R $(id -u):$(id -g) target /sccache; exit \$s"
rm -rf "${scratch}/${w}-target"
```

- `<작업 디렉터리>` 도 `~/claude-scratch/<작업 ID>` 아래에 둔다(SSD).
- 이미지는 root 로 돈다. bind mount 에 쓴 파일은 끝에 자기 계정으로 돌려줘야 `rm -rf` 와 다음 실행의 `du` 가 된다.
- 레지스트리 볼륨은 그대로 둔다. 읽기뿐이고 실행 첫머리에 한 번 읽는다.
- `RUSTC_WRAPPER` 는 이미지에 없다: `cargo-verify.sh` 가 증분 빌드로 돌게 두려는 것이다. `CARGO_INCREMENTAL=0` 은 CI 의
  `scripts/ci/rust-setup.sh` 값이고, sccache 는 증분 단위를 캐시하지 못한다.
- 레지스트리가 비어 있으면 첫 실행만 `--network none` 을 빼고 `cargo fetch` 를 한 번 한다.
- `SCCACHE_DIR=/sccache`, `SCCACHE_CACHE_SIZE=20G` 는 이미지가 싣는다. 캐시 크기는 `du -sh ~/claude-scratch/sccache` 로 본다.
- 옛 볼륨 `perfectory-sccache`(HDD)는 더 쓰지 않는다. 비우는 일은 쓰는 작업이 없을 때 한다(`docker volume rm perfectory-sccache`).

## 이미지 입력을 바꾸는 브랜치

`perfectory-verify:local` 은 배포된 main 의 환경이다. 브랜치가 `apt_deps`, `rust-toolchain.toml`, `tools/sccache.env`,
`tools/container-images.env` 의 기반, `tools/verify-image/Dockerfile` 을 바꾸면 그 환경은 아직 없다. 그때만 자기 작업 트리에서
임시 태그를 짓고(`docker build -f tools/verify-image/Dockerfile -t perfectory-verify:<작업 ID> .`), 시험이 끝나면
`docker image rm perfectory-verify:<작업 ID>` 로 지운다.

## 바꾸기

apt 목록은 `tools/xtask/src/main.rs` 의 `apt_deps`, 도구체인은 `rust-toolchain.toml`, 기반은 `tools/container-images.env`,
sccache 는 `tools/sccache.env` 에서 바꾼다. Dockerfile 이 다른 값을 가지면 `technology-version-consistency` 가 실패한다.
