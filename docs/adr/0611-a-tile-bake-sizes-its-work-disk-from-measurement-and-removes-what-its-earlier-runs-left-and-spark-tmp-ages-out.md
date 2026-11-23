# ADR 0611: 타일 굽기는 작업 디스크를 잰 값으로 요구하고 앞선 run 이 남긴 것을 스스로 치운다 — Spark 의 /tmp 도 나이로 사라진다

- Status: Accepted
- Date: 2026-11-23
- Amends: [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §3(작업 디스크의 배수),
  [ADR-0198](./0198-server-scratch-journal-and-unused-images-age-out-by-themselves.md) §1(나이로 사라지는 트리에 `spark-tmp` 를 더한다)
- Related: [ADR-0602](./0602-the-served-parcel-boundaries-are-built-in-duckdb-inside-the-publisher-one-sido-at-a-time-and-kept-as-row-versions.md)(서빙본 Gold 를 발행기가 쓰는 전환),
  [ADR-0493](./0493-the-national-parcel-bake-runs-unattended-a-run-stops-between-partitions-at-its-budget-and-the-next-continues.md)(시도 조각 캐시)

## Context

2026-11-23 05:22Z, 필지 굽기(`foundation-parcel-tile-bake@bake`)가 시작 검사에서 멈췄다: "작업 디스크에 134 GB 가 남았고,
25 GB 인계물에는 200 GB 가 필요하다". 데이터 디스크(`/data`, 1,968 GB)는 94% 였다. 측정한 사실이다.

1. **배수 8 은 처음 추정 그대로였다.** 계약(`config/tile-bake-containers.contract.json` `work_disk`)이 스스로 "첫 전국 굽기가 다시
   재는 추정"이라 적었다. 2026-10-13 조각 없이 잰 전국 굽기의 정점은 43.7 GB(1.75 배, 런북 `parcel-tile-bake.md` §4)였지만 배수는
   바뀌지 않았다. 지금은 시도 조각으로 굽는다(ADR-0316, ADR-0493). Prometheus 의 `/data` 남은 바이트를 run 시작부터 가장 낮은 때까지 보면(호스트 전체라 위쪽
   한계다), 25,055,575,250 바이트 202609 인계물에 시도 조각을 구운 세 run 이 26.7·27.3·25.0 GB(11-13 04:04Z, 11-19 02:39Z,
   04:22Z), 인계물을 시도 CSV 로 나누기만 한 run 이 16.8 GB, 합치기만 한 run 들이 10.1~14.5 GB 였다. 인계물의 1.09 배 이하다.
   빈 캐시에서 굽는다면 11-19 run 전에 이미 있던 조각 판 3.75 GB 를 더해 약 31 GB, 1.24 배로 계산된다.
2. **앞선 run 의 작업 폴더가 남았다.** run 은 `<unit>-<uuid>` 를 만들고 끝날 때 지운다. 11-19 06:18Z 시간 한도에 systemd 가 죽인
   run(타일 합치기 중)과, 11-19 03:47Z 예산에 이르러 끝났으나 다음 시도의 GDAL 컨테이너가 아직 그 폴더에 쓰던 run 의 폴더가
   남아 32 GB(공유 하드링크를 뺀 값)를 쥐고 있었다. 코드는 "작업 폴더의 어떤 것도 다시 쓰이지 않는다"고 적는다.
3. **다시 읽히지 않는 인계물이 남았다.** 굽기 스크립트는 run 폴더를 증거로 남긴다. 그 안의 `served/`(인계물 25 GB)도 함께 남아, 상태
   파일(`ready`, `rehearsal-ready`)이 이름 대지 않는 두 run(10-15, 11-13)의 인계물 50.1 GB 가 있었다. 증거(run.log, 요약, 판정)는
   KB~MB 다. 그 둘은 Spark 컨테이너가 uid 185 로 썼기에 굽기 unit 의 계정은 지울 수 없다.
4. **Spark 의 /tmp 가 쌓였다.** 모든 Spark 컨테이너가 `/data/foundation-platform/spark-tmp` 를 /tmp 로 쓴다(host-layout
   `spark_tmp`). 정상 종료한 JVM 은 자기 block manager 를 지우지만 죽은 것은 못 지운다. 11-11 부터의 block manager 203개, 72.1 GB.
   한 Spark run 이 그 안에서 쓰고 있었다.

## Decision

1. **배수는 잰 값에서 온다: 2.** 조각으로 굽는 run 의 잰 최대 1.09 배, 빈 캐시 계산 1.24 배의 1.6 배이고, 조각 없이 굽는 unit 의
   10-13 실측 1.75 배보다도 크다. 바닥(`min_free_bytes` 2 GiB)은 그대로다.
   근거 숫자는 계약의 `work_disk.reason` 에 둔다.
2. **한 unit 의 굽기는 작업 루트에서 한 번에 하나이고, 앞선 run 이 남긴 폴더를 먼저 치운다.** `bake-lakehouse-tiles` 는
   `<작업 루트>/<unit>.lock` 을 기다리지 않고 잡고(잡혀 있으면 거부) run 이 끝날 때까지 쥔다. 잠금은 프로세스와 함께 풀리므로 죽은
   run 은 잠금을 남기지 않는다. 쥔 채로 `<unit>-<uuid>` 모양의 폴더를 모두 지운다 — 그 unit 의 다른 run 이 살아 있을 수 없다. 캐시
   (`<unit>-partitions`, `<unit>-feature-index`)는 그 모양이 아니라 남는다. 그다음에 남은 공간을 잰다
   (`lakehouse_tile_bake_work.rs`).
3. **상태 파일이 이름 대지 않는 인계물은 지운다.** 필지 굽기 스크립트는 반쪽마다 잠금을 쥔 뒤, `ready`·`rehearsal-ready` 가 가리키지
   않는 run 의 `served/` 를 지우고 나머지(로그·요약·판정)는 증거로 둔다. 자기 계정이 지우지 못하는 것(전환 전 Spark 가 쓴 것)은
   레이크하우스 쓰기 검사 컨테이너(`lakehouse-target-init`, busybox, uid 185)가 그 사용자로 지운다. 못 지운 것은 저널에 남기고 run 은
   계속한다. 전환(ADR-0602) 뒤에는 발행기가 굽기 계정으로 쓰므로 보통 경로는 `rm` 이다.
4. **`spark-tmp` 는 2일 동안 고쳐지지 않으면 사라진다.** `infra/host/tmpfiles.d/foundation-spark-tmp.conf` 의
   `e /data/foundation-platform/spark-tmp - - - mM:2d` 를 OS 의 일일 정리가 집행한다(ADR-0198 §1 과 같은 장치). run 의 파일은 그
   unit 의 시간 한도보다 오래되지 않고, 어느 작업의 한도도 멈춤 유예와 합쳐 하루를 넘지 않는다(가장 긴 것은 spark-small 단계를 가진
   by-PNU 서빙 굽기의 21시간). `test_host_retention.py` 가 나이를 모든 작업의 한도보다 길게 지킨다.
5. 시험: Rust 시험이 잠금(같은 unit 둘째 run 거부, 다른 unit 허용, 놓으면 다시 잡힘)과 치우기(그 unit 의 run 폴더만, 캐시·다른 unit·같은
   이름의 파일은 남김)를 본다. `test_parcel_tile_bake.py` 가 이름 없는 인계물 둘이 — 하나는 다른 계정의 것처럼 지울 수 없게 — 사라지고
   증거와 이름 있는 인계물은 남는 것을 본다. `test_host_retention.py` 는 리눅스 CI 에서 실제 `systemd-tmpfiles --clean` 으로 사흘 전
   block manager 는 지워지고 가장 긴 한도만큼 된 것은 남는 것을 본다.

검토한 대안:

- **배수를 그대로 두고 공간을 비운다.** 비워도 배수가 틀린 것은 남아, 25 GB 인계물이 200 GB 를 요구하는 동안 디스크가 어느 정도
  차면 다시 멈춘다.
- **작업 폴더마다 잠금 파일을 두고 잡히지 않은 것만 지운다.** 같은 unit 의 두 run 이 함께 도는 경우를 남기지만, 두 run 은 조각
  캐시를 함께 쓰므로 그 경우 자체가 안전하지 않다. unit 잠금 하나가 둘 다 막는다.
- **호스트 보존 unit(`foundation-host-retention`, ADR-0198 §3)이 인계물을 지운다.** 그 unit 은 굽기 상태 파일과 잠금을 몰라, Gold 반쪽이
  새 인계물을 쓰는 동안 그것을 이름 없는 것으로 볼 수 있다. 데이터의 주인이 자기 잠금 아래에서 지운다.
- **tmpfiles 의 나이로 인계물을 지운다.** 지금 기다리는 인계물도 며칠씩 고쳐지지 않는다(11-20 것이 11-23 에도 쓰인다). 나이는 쓰임을
  말하지 않는다.

## Consequences

- 측정(2026-11-23): 이 결정 전 `/data` 에서 W86 의 측정 스크래치 77.7 GB 를 그 주인이 지웠고(115.2 → 192.9 GB), 배포 뒤 첫 굽기
  시작이 남은 작업 폴더 32 GB 와 이름 없는 인계물 50.1 GB 를, 첫 일일 정리가 `spark-tmp` 의 2일 넘은 것을 지운다.
- 25 GB 인계물의 굽기는 50 GB 가 남아 있으면 시작한다.
- 남은 일: 용도지역 굽기의 run 폴더(`tile-bake/lakehouse/zoning-tile-bake`, 3.3 GB 씩 일곱, 23.2 GB)도 같은 규칙을 따를 수 있다 —
  그 주인에게 제안한다. 예산에 이른 run 이 도구 컨테이너가 쓰는 폴더를 지우려다 실패하는 것은 2 의 치우기가 다음 run 에 거둔다.
