# ADR 0598: 운영 Postgres 는 데이터 디렉터리와 WAL 을 시스템 SSD 에, 대용량 표공간은 데이터 디스크에 둔다

- Status: Accepted
- Date: 2026-11-21
- Amends: [ADR-0241](./0241-the-host-keeps-the-os-on-its-system-disk-and-docker-on-its-data-disk.md)(데이터와 함께 자라는 것은 데이터 디스크로; 운영 Postgres `pgdata` 볼륨도 그때 Docker 와 함께 옮겨졌다)
- Builds on: ADR-0082(대용량 투영은 데이터 볼륨 밖의 표공간), [ADR-0597](./0597-a-deploy-is-done-when-every-step-passed-and-a-failed-one-is-finished-from-the-step-that-failed.md)(배포 실패의 원인 측정)
- 승인: 사장, 2026-11-20 약 19:00Z(pg_default+WAL 0.8 GB 를 SSD 로, `parcel_bulk` 112 GB 는 HDD 에)

## Context

**측정(ai-server 읽기 전용, 2026-11-20).**

| 무엇 | 값 |
|---|---|
| 데이터 디스크 `sda` | TOSHIBA HDWD120, 회전(ROTA=1), 1.8 TB, Docker data-root·Spark·OCR·tablespace 가 함께 쓴다 |
| 시스템 디스크 `sdb` | WDC SSD(ROTA=0), `/` 217 GB 중 약 101 GB 여유 |
| I/O pressure | `full avg300` 47.5%, 10:28Z iowait 7.1 코어(바쁜 CPU 8.7/20) |
| `pg_default`+`pg_global` | 관계 568 MB, `pg_wal` 160 MB, 데이터 디렉터리 전체 775 MB |
| `parcel_bulk` | 112 GB(`serving_postgis`), `/data/foundation-postgres-tablespaces` |
| 연결 | 100 중 5 사용, `track_io_timing` off |
| WAL 동기화 비용(`pg_test_fsync -s 2`, 19:33Z, load 12–20, 같은 시각 두 디스크) | `fdatasync` HDD 272,459 µs/op(3.7 ops/s), SSD 687 µs/op(1,455 ops/s); `open_datasync` 241,355 µs 대 691 µs; `fsync` 695,853 µs 대 2,006 µs |

같은 staging database 단계(빈 DB 에 스키마 사슬)가 11-19..20 에 17 s 부터 300 s 넘게까지 흔들렸고, 배포의 API 첫 연결은
요청 예산 500 ms 안에 열리지 않았다(ADR-0597). 마이그레이션, 카탈로그, 모든 커밋의 WAL fsync 는 `pg_default` 와 `pg_wal` 에서
일어나는데, 그 둘이 Spark 스크래치와 같은 회전 디스크에 있다. 커밋 하나가 WAL 을 `fdatasync`(Linux 기본 `wal_sync_method`)하는 데 HDD 에서 약 0.27 초, SSD 에서 약 0.0007 초가 든다(약 400 배).

ADR-0241 이 옮긴 까닭은 크기였다(그때 `pgdata` 20 GB, 시스템 디스크 92%). 지금 자라는 것은 `parcel_bulk` 이고 그것은 이미
표공간으로 나뉘어 있다. 데이터 디렉터리는 1 GB 아래다.

## Decision

1. **어디에.** 데이터 디렉터리(`pg_default`, `pg_global`, `pg_wal`)는 `config/host-layout.contract.json` `postgres_data.path`
   (`/var/lib/foundation-postgres/data`, 시스템 SSD)에 둔다. `parcel_bulk` 은 데이터 디스크에 그대로 둔다. 대용량 투영은 앞으로도
   표공간에 만든다(ADR-0082). 기본 표공간에 큰 표를 만드는 것은 이 결정을 어기는 것이다.
2. **한 변수, 한 결정자.** `docker-compose.yml` 과 `compose.recovery.yml` 의 데이터 디렉터리 마운트는 모두
   `${FOUNDATION_PLATFORM_POSTGRES_DATA:-pgdata}` 다. 고정된 Docker Compose v5.1.0 에서 측정했다: 값이 없으면 이름 있는 볼륨
   `pgdata`, 절대 경로면 bind 다. 값은 `scripts/deploy/postgres-data-source.sh` 하나가 정한다. 근거는
   `postgres_data.moved_marker` 가 있느냐 하나뿐이다. 운영 프로젝트의 compose 호출자(`foundation-runtime.sh`,
   `run-postgres-backup.sh`)가 그것을 읽고, 복원 연습은 자기 프로젝트와 볼륨을 쓰며 변수를 지운다. 빈 디렉터리를 bind 하면 빈
   클러스터가 시작되므로, 표식은 배포만 쓴다.
3. **한 번의 이전(one-off, `scripts/deploy/postgres-data-move.sh`).** 배포 4단계 `migrate` 가 사슬 전에 한 번 한다. 표식이 있거나
   볼륨에 클러스터가 없으면 아무것도 하지 않는다.
   - 하는 일: 몇 가지 수를 읽고(마이그레이션 원장, `catalog`·`audit` 의 모든 표), Postgres 를 `stop -t 300` 으로 멈춘다.
     `pg_controldata` 가 `shut down` 인지 확인하고 `rsync -a --numeric-ids` 로 복사한다(uid 70 유지).
   - 확인: 체크섬 비교 `rsync -n -c` 가 차이 0, 복사본의 `pg_controldata` 가 원본과 같음, 모든 `pg_tblspc` 링크가 같음.
   - 표식을 쓰고 Postgres 를 복사본으로 다시 띄운다. 수는 이전보다 작지 않아야 하고, 마이그레이션 수는 같아야 한다.
   - 하나라도 어긋나면 표식을 지우고 손대지 않은 볼륨으로 다시 띄우며, 배포는 77 로 멈춘다(ADR-0597 §4: 잡힌 DAG 는 재시도까지 멈춤).
   - 옛 볼륨은 fallback 으로 남긴다. 그것을 지우는 정리 PR 이 이 one-off 와 그 호출을 함께 지운다.
4. **이전 뒤 전체 백업.** 이전이 끝나면 `full-backup-requested` 를 남기고 `foundation-postgres-backup.service` 를 시작한다.
   `run-postgres-backup.sh` 는 요청이 있으면 `--type=full` 로 하고, 성공한 뒤에만 요청을 지운다. 클러스터의 system identifier 는
   그대로이므로 기존 백업과 WAL 보관은 이어진다. 전체 백업은 새 기준점이다.
5. **디스크를 잰다.** `track_io_timing=on`, `track_wal_io_timing=on`(`compose.recovery.yml`). PostgreSQL 17 문서에 따르면 이 둘은
   `pg_stat_io` 의 read/write/fsync 시간과 `pg_stat_wal` 의 write/sync 시간을 채운다. 시계 비용이 큰 플랫폼이 있다는 경고와
   `pg_test_timing` 으로 재라는 권고도 있다. 이전 전후의 fsync·커밋 지연은 이 값으로 보고한다.

## Consequences

- 이전하는 배포에서 Postgres 는 멈춤부터 healthy 까지 내려가 있다(계산: 0.78 GB 복사와 재시작, 1–3 분). 그동안 API 와 DB 를
  쓰는 작업은 실패하거나 다시 시도한다. 시간은 `postgres-data-move done` 줄이 말한다.
- 시스템 디스크에 1 GB 남짓과 WAL(`max_wal_size` 1 GB)이 더해진다. 여유 101 GB 에 비하면 작다. 10-02 의 루트 디스크 사고는
  로그가 원인이었고, 그것은 로그 상한이 막는다.
- 대용량 투영의 읽기는 여전히 HDD 에 있다. 이번 결정은 배포 사슬, 카탈로그, 커밋의 지연만 다룬다.
