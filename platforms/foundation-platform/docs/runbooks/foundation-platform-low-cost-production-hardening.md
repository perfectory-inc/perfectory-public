---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Foundation Platform 저비용 운영 강화 런북

## 목적

이 런북은 foundation-platform의 저비용 운영 강화 기준선을 기록한다. M3.2 전환 완료 증거를
대체하지 않는다. 용량 주장을 추정치가 아니라 측정한 부하 테스트 산출물에 근거하게 하는 것이 목적이다.

## 복구 설계

Cloudflare R2는 S3 버킷 버전 관리를 제공하지 않는다. 따라서 Foundation Platform은 서로 다른 저장
계약에 서로 다른 복구 제어를 적용한다.

- 관리되는 레이크하우스 Bronze 접두사는 변경 불가 원자료 증거다. 저장소에 포함된
  `bronze-raw-30-days` Bucket Lock policy to the configured lakehouse bucket and read it back before
  allowing live collection writes.
- PostgreSQL 복구는
  `FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET`. Do not Bucket Lock the whole pgBackRest repository because
  pgBackRest updates metadata such as `backup.info` and `archive.info`. Its controls are a dedicated
  bucket and credentials, client-side AES-256-CBC encryption, continuous WAL archiving, 35-day
  full-backup retention, and restore rehearsal.

공식 Cloudflare CLI로 설정된 레이크하우스 버킷에 저장소의 잠금 선언을 적용하고 즉시 확인한다.

```bash
CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=... \
  wrangler r2 bucket lock set "$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET" \
  --file infra/cloudflare/foundation-platform-lakehouse-prod.bucket-lock.json --force
CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=... \
  wrangler r2 bucket lock list "$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"
```

완료된 백업 객체와 변경 가능한 저장소 메타데이터를 먼저 물리적으로 분리한 접두사로 옮기고
백업·만료 전체 테스트를 통과하기 전에는 pgBackRest 저장소에 잠금을 추가하지 않는다.

## 기준 부하 명령

API가 준비 상태를 보고한 뒤 읽기 경로 k6 smoke를 실행한다.

```bash
FOUNDATION_PLATFORM_API_URL=http://localhost:8080 \
FOUNDATION_PLATFORM_LOAD_DURATION=5m \
FOUNDATION_PLATFORM_LOAD_READ_RPS=20 \
FOUNDATION_PLATFORM_LOAD_HEALTH_RPS=5 \
k6 run --summary-export target/load/summary.json scripts/load/foundation-read-smoke.js
```

 k6 실행은 `target/load` 아래에 JSON 증거를 쓴다.

## 실행 없이 검증

k6가 설치되지 않은 환경에서는 부하를 실행하지 않고 스크립트만 검증한다.

```bash
k6 inspect scripts/load/foundation-read-smoke.js
```

스크립트 구문과 시나리오 해석만 확인하며 용량을 주장할 수 있는 검사는 아니다.

## 초기 목표

- `/healthz`가 `200`을 반환한다.
- 부하 시작 전에 `/readyz`가 `200`을 반환한다.
- hot read p95가 500ms 미만이다.
- hot read p99가 1500ms 미만이다.
- 실패 요청률이 1% 미만이다.
- overload는 process 붕괴가 아니라 제한된 `429`·`503`·timeout 응답으로 열화되어야 한다.

## 용량 주장 형식

증거가 있을 때만 다음 형식으로 용량을 주장한다.

```text
instance type X, PostgreSQL 17 설정 Y, Valkey 8 설정 Z에서 foundation-platform이 D 기간 동안
N read RPS를 처리했고 p95 <= A ms, p99 <= B ms, error rate <= C%, restart 없음, OOM 없음,
DB saturation 없음이어야 한다.
```

## 필수 증거

- k6 summary JSON from `target/load`.
- 같은 시간 구간의 platform log
- 요청 수·error rate·latency·DB pressure·Valkey 상태·outbox 상태를 담은 `/metrics` scrape
- 배포 target 정보: host type, CPU, memory, PostgreSQL/Valkey limit, commit SHA

## 프로세스 관리자 가드

운영을 주장하기 전 재시작·종료 동작이 명시된 프로세스 관리자를 사용한다.

- 실패 시 backoff를 두고 재시작한다. 빠른 무한 재시작 loop는 사용하지 않는다.
- startup timeout을 설정하고 `/readyz`가 `200`이 아니면 배포를 실패시킨다.
- process 종료 전에 in-flight 요청이 끝날 수 있도록 graceful stop timeout을 둔다.
- Git 밖에서 관리하는 environment file로 환경변수를 읽는다.
- log를 journald, Docker log 또는 보존되는 다른 sink로 보낸다.
- rollout note에 배포 commit SHA, container digest, environment file version을 기록한다.

## PostgreSQL 백업 정책

복구 image는 `infra/postgres/Dockerfile.recovery`에 고정하고 pgBackRest 2.58.0을 사용한다.
`compose.recovery.yml`은 `archive_timeout=60s`로 synchronous WAL archive push를 켜고 application
host 밖 R2에 repository를 둔다.

- Full backup: 일요일 또는 유효한 full backup이 없을 때
- Differential backup: 이틀마다
- Schedule: local server 시각 매일 02:15, 최대 15분 random delay
- Retention: full backup은 35일 복구 가능, 필요한 WAL은 pgBackRest가 보존
- Encryption: Git 밖에 둔 passphrase를 사용하는 pgBackRest AES-256-CBC
- Access: credential은 전용 recovery bucket 범위여야 하며 lakehouse writer와 공유하지 않는다.
- RPO 목표: 최대 5분. 설정된 60초 archive timeout은 더 엄격하고 네트워크·알림 지연 여유가 있다.
- RTO 목표: 현재 배포 등급에서 최대 2시간
- Valkey는 폐기 가능한 cache/idempotency 상태이며 정본 데이터로 복구하지 않는다.

릴리스는 `/opt/foundation-platform/releases/<git-sha>` 아래에 설치한다. 배포 진입점은
`/opt/foundation-platform/current`를 원자적으로 전환하고 이전 대상을 `previous`에 기록한다.
변경 가능한 복구 증거는 변경 불가 릴리스 밖의 `/var/lib/foundation-platform/recovery`에 둔다.

```bash
release_id="$(git rev-parse HEAD)"
git archive --format=tar.gz --output="/tmp/foundation-${release_id}.tar.gz" "${release_id}"
sudo FOUNDATION_PLATFORM_RELEASE_ROOT=/opt/foundation-platform \
  FOUNDATION_PLATFORM_STATE_ROOT=/var/lib/foundation-platform \
  scripts/deploy/foundation-release.sh install \
    "${release_id}" "/tmp/foundation-${release_id}.tar.gz"
```

같은 릴리스 진입점은 변경 불가 소스 트리 밖에 레이크하우스 계산 상태를 준비한다.

- `/var/lib/foundation-platform/lakehouse`
- `/var/lib/foundation-platform/remote-lakehouse`

두 디렉터리는 설정된 레이크하우스 runtime UID/GID(기본 `185:185`)가 소유하고
`compose.lakehouse.yml`이 마운트한다. 릴리스는 `/opt/foundation-platform/releases/<git-sha>/target`
아래에 변경 가능한 Spark·레이크하우스 출력을 절대 쓰지 않는다.

`current`가 의도한 정확한 commit을 가리키고 `/etc/foundation-platform/recovery.env`가 mode
`0600`으로 존재한 뒤에만 scheduler를 설치한다. 해당 file에
`FOUNDATION_RECOVERY_EVIDENCE_DIR=/var/lib/foundation-platform/recovery`를 설정하거나 같은
systemd default를 사용한다.

```bash
sudo install -o root -g root -m 0644 \
  infra/systemd/foundation-postgres-backup.service \
  /etc/systemd/system/foundation-postgres-backup.service
sudo install -o root -g root -m 0644 \
  infra/systemd/foundation-postgres-backup.timer \
  /etc/systemd/system/foundation-postgres-backup.timer
sudo systemctl daemon-reload
sudo systemctl enable --now foundation-postgres-backup.timer
sudo systemctl start foundation-postgres-backup.service
systemctl show foundation-postgres-backup.timer -p ActiveState -p NextElapseUSecRealtime
journalctl -u foundation-postgres-backup.service --since today
```

timer를 운영 상태로 인정하기 전에 첫 서비스 실행이 성공적으로 끝나야 한다.

롤백은 어느 릴리스도 변경하지 않고 심볼릭 링크만 전환한다. 전환 후 영향을 받는 서비스를 재시작하고
준비 상태 검사를 다시 실행한다.

```bash
sudo FOUNDATION_PLATFORM_RELEASE_ROOT=/opt/foundation-platform \
  FOUNDATION_PLATFORM_STATE_ROOT=/var/lib/foundation-platform \
  /opt/foundation-platform/current/scripts/deploy/foundation-release.sh rollback
readlink /opt/foundation-platform/current
```

## 운영 Compose 진입점

모든 운영 런타임 Compose 작업은 `scripts/deploy/foundation-runtime.sh`를 거쳐야 한다. 진입점은
항상 `docker-compose.yml`과 `compose.recovery.yml`을 합치므로 API·관측성 서비스를 시작해도 복구
설정 PostgreSQL 이미지가 로컬 개발 이미지로 바뀌거나 WAL 보관이 꺼지지 않는다.

```bash
cd /opt/foundation-platform/current
sudo scripts/deploy/foundation-runtime.sh up -d --build \
  postgres valkey foundation-api alertmanager prometheus
sudo scripts/deploy/foundation-runtime.sh ps
```

레이크하우스 서비스도 같은 복구 안전 wrapper를 사용하지만 별도 Compose 프로젝트로 실행한다. 계산을
API·데이터베이스 런타임과 독립적으로 운영하기 위해서다.

```bash
sudo env FOUNDATION_PLATFORM_COMPOSE_PROJECT=foundation-platform-compute \
  scripts/deploy/foundation-runtime.sh --profile lakehouse-query up -d trino
sudo env FOUNDATION_PLATFORM_COMPOSE_PROJECT=foundation-platform-compute \
  scripts/deploy/foundation-runtime.sh --profile lakehouse-batch up -d spark
```

제어된 API 장애 알림 리허설에도 같은 진입점을 사용한다.

```bash
sudo scripts/deploy/foundation-runtime.sh stop foundation-api
sudo scripts/deploy/foundation-runtime.sh up -d foundation-api
```

복구가 켜진 운영에서 루트 Compose 파일만 단독 호출하지 않는다. 루트 파일은 로컬 개발 계약이기도
해서 표준 PostGIS 이미지를 의도적으로 사용한다. pgBackRest, `archive_mode=on`, 외부 저장소 설정은
복구 overlay가 추가한다.

## 복구 리허설

전용 빈 repository prefix와 임시 암호화 passphrase로 격리 리허설을 실행한다.

```bash
run_id="$(date -u +%Y%m%d%H%M%S)"
FOUNDATION_RECOVERY_RUN_ID="${run_id}" \
FOUNDATION_RECOVERY_EVIDENCE_DIR="target/recovery/${run_id}" \
  scripts/recovery/postgres-restore-drill.sh
```

드릴은 정확한 bootstrap·migration·runtime-grant·finalize chain을 수행하고 전체 암호화 백업을
encrypted backup; writes a marker after the full backup; creates a named PostgreSQL restore point;
archives the WAL; restores into a new volume; promotes the restored database; reruns migrations; and
proves both application tables and the post-backup marker are readable.

## 증거 처리

모든 복구 리허설은 schema version, run identifier, 시작·종료 시각, 이름이 지정된 restore
point, migration 수, read/PITR 검증 결과와 최종 결과를 비공개 운영 증거 저장소에 기록해야
한다. 같은 run에 배포·rollback commit 식별자, container digest, timer 상태, backup 식별자,
RPO/RTO 시간, alert 전환, daemon 재시작 결과, bucket 접근 성공·실패 probe를 함께 보존한다.
이 값, 실제 resource binding, token 이름, account 식별자와 host inventory는 공개 소스
저장소에 커밋하지 않는다.

복구 게이트는 대상 환경의 비공개 증거가 다음을 모두 입증할 때만 완료다.

- `pgbackrest check`, full 또는 differential backup, WAL archival, 격리 volume 복원
- PITR 후 application read와 post-backup marker
- immutable release directory 밖에서 active/enabled scheduling과 성공 실행
- recovery credential이 전용 bucket만 접근하고 lakehouse bucket은 거부되는지
- 제어된 service·daemon 복구가 필요한 모든 health/readiness check를 반환하는지
- 관측한 RPO와 RTO가 선언한 목표 안에 있는지

공식 참고 문서:

- Cloudflare R2 Bucket Locks: https://developers.cloudflare.com/r2/buckets/bucket-locks/
- Cloudflare R2 lock API: https://developers.cloudflare.com/api/resources/r2/subresources/buckets/subresources/locks/
- pgBackRest user guide: https://pgbackrest.org/user-guide.html
