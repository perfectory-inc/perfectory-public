# ADR 0177: 새 릴리스는 운영 전환 전에 같은 서버에서 이름으로 격리된 스테이징 스모크를 통과해야 한다

- Status: Accepted
- Date: 2026-10-10
- Amends: [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)(배포 순서에 단계 3b 추가),
  FP-ADR-0029(런타임 환경 분리)의 `staging` 버킷 `foundation-platform-lakehouse-staging` — 스테이징은 운영 버킷을
  쓰고 키 접두사로 격리한다.
- Builds on: [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(환경 파일 계약),
  [ADR-0173](./0173-a-failed-post-deploy-run-does-not-pause-the-schedule.md)(전환 전에 멈춘 배포는 DAG 를 돌려준다),
  [ADR-0175](./0175-workers-deploy-themselves-after-the-host-deploys-main.md)(Worker 는 preview 먼저)

## Context

2026-10-09/10 운영 사고 셋은 모두 CI 를 통과했다. CI 는 가짜(로컬 S3 대역, 고정 인벤토리, 가짜 계정)로 돈다.

1. 내용 주소 스풀 업로드가 실제 R2 에 첫 파일을 올리다 패닉했다(`unfold` 가 끝난 뒤 다시 폴링됨, PR #390).
2. 전부 아니면 전무인 바이트 예산이 실제 VWorld 밀린 분량을 매일 거부했다(ADR-0172).
3. Worker 자동 배포가 map-edit 에서 실패했다: wrangler 는 실제 계정에서 D1 id 를 요구했다(ADR-0175 수정).

셋 다 새 릴리스를 실제 입력 몇 개에 돌려 봤으면 전환 전에 드러났다. 소유자 결정(2026-10-10): 운영 배포 앞에
스테이징 단계를 둔다. 비용은 거의 0, 새 서버 없음, 소유자는 명령을 치지 않는다(배포가 한다).

현행 실물:

- 운영 Rust 코드의 S3 클라이언트는 `R2ObjectStorage::from_config` 하나뿐이다(`crates/foundation-outbox`).
- `FOUNDATION_PLATFORM_RUNTIME_ENV` 는 이미 모든 발행기 명령이 싣는 값이고 `staging` 이 정의돼 있었지만
  존재하지 않는 별도 버킷을 가리켰다.
- `foundation_admin` 접속 주소는 스크립트 아홉 곳이 `/foundation` 을 박아 각자 조립했다.
- 마이그레이션은 런타임 이미지(`foundation-migrate`)에 컴파일되고, 부트스트랩·마감 SQL 은 `DATABASE foundation` 을
  글자로 적었다.
- 호스트 메모리는 61/62 GB 가 예약돼 있다. Spark 적재는 스모크에 넣을 수 없다.

## Decision

1. **어디서: 같은 서버, 같은 계정, 이름으로 격리.**
   - R2: 레이크하우스 버킷의 `staging/` 아래. 스위치는 하나, `FOUNDATION_PLATFORM_RUNTIME_ENV=staging` 이다.
     R2 클라이언트를 만들 때 이 값을 읽어 `R2KeyNamespace` 를 정하고(`crates/foundation-outbox/src/object_storage/r2/namespace.rs`),
     클라이언트의 요청 파이프라인 한 곳에서 처리한다: 재시도 루프 전에 객체 경로·목록의 `prefix`/`start-after`·복사
     원본 헤더에 접두사를 붙이고, 매 시도 전송 직전에 실제 요청을 다시 검사해 스테이징이면 `staging/` 밖을,
     운영이면 `staging/` 안을 거부한다. 호출부가 키를 바꾸지 않으므로 빠뜨릴 호출부가 없고, 다시 쓰기를 우회한
     새 호출부도 자기 이름공간 밖에는 쓸 수 없다. 목록은 논리 키로 돌려준다(운영 목록에는 스테이징 객체가 보이지
     않는다). `staging/` 로 시작하는 논리 키는 양쪽에서 거부한다. `RuntimeEnvironment::Staging` 의 버킷은 운영 버킷이다.
   - Postgres: 같은 런타임 Postgres 의 `foundation_staging` 데이터베이스. 이름은 `scripts/ops/database-url.sh` 한 곳이
     정하고(`staging` 일 때만 `foundation_staging`, 그 밖에는 `foundation` — R2 와 같은 규칙), 아홉 스크립트의 복제
     조립을 이 함수로 바꿨다. 부트스트랩·마감 SQL 은 psql 의 `:"DBNAME"`(연결된 데이터베이스)을 쓴다.
   - Cloudflare: ADR-0175 의 preview Worker. 이미 preview 먼저, 그 스모크가 실패하면 운영에 가지 않는다
     (`test_a_preview_smoke_that_fails_never_reaches_production`). 호스트 게이트에는 넣지 않는다: Worker 배포의 입력은
     호스트가 돌리는 커밋의 제어 체크아웃이고, Worker 실패가 데이터 호스트 릴리스를 막을 이유가 없다.
2. **언제:** `foundation-deploy.sh` 의 단계 3b — 승인 빌드(2)와 FLOOR 설정(3) 뒤, 전환(4) 전 —
   `foundation-release.sh staging-smoke <sha>` 가 설치됐지만 아직 current 가 아닌 릴리스의
   `scripts/ops/staging-smoke.sh` 를 임시 유닛(`foundation-staging-smoke`, User=foundation-platform, 환경 파일은
   런타임 비밀 계약의 run `staging-smoke`: recovery·source-sweep·lakehouse-reader)으로 돌린다. 실패하면 배포가 거기서
   멈춘다: 새 릴리스는 아무것도 활성화되지 않고, EXIT 트랩이 돌리던 릴리스에 DAG 를 돌려주며(ADR-0173 의
   before-switch), 자동 배포는 `failed/<sha>` 를 남기고 한 번 exit 1 로 알린다. 더 새 main 커밋이 다음 시도다.
3. **무엇을(가볍게, Spark 없음), 각 단계 상한은 `config/staging-gate.contract.json`:**
   `clear`(이전 실행의 `staging/` 비우기, 새 발행기 명령 `clear-staging-namespace`, 스테이징 밖에서는 거부) →
   `image`(릴리스의 런타임 이미지를 `foundation-platform-staging-runtime:local` 이름으로 빌드; 운영의 `foundation-platform-runtime:local` 을
   옮기지 않는다) →
   `database`(`foundation_staging` 을 지우고 만든 뒤 릴리스의 부트스트랩·마이그레이션·권한·마감 — 빈 DB 에서 스키마
   전체) → `plan`(허브·VWorld 계획과 VWorld 파일 인벤토리, 실제 제공자, 읽기 전용) → 표본(가장 작은 파일 둘 +
   멀티파트 임계 이상 가장 작은 하나, RAON 선택 묶음 제외) → `ingest`(일일 수집과 **같은** VWorld 레인 설정,
   `scripts/ops/vworld-sweep-lane.sh` 로 한 벌) → `measure`(읽기 전용 키로 bronze-object-members). 스테이징 클라이언트는
   계약의 `multipart_threshold_bytes`(32 MiB)부터 멀티파트로 가므로 수십 MB 파일이 10-10 패닉 경로를 지난다(운영에서는
   이 변수를 거부). 각 검사는 일어난 양을 센다: 표본이 다 착지하지 않거나, 측정 수가 착지 수와 다르거나, 제공자가
   아무것도 내놓지 않으면 실패다.
4. **보존:** 한 실행분. 매 스모크가 시작할 때 `staging/` 를 비우고 `foundation_staging` 을 새로 만든다. 마지막 실행의
   객체와 행은 다음 배포까지 조사용으로 남는다. "N 일보다 오래된 것" 대신 이것을 택한 이유: N 일 안의 두 번째 실행은
   같은 내용 주소 키를 만나 "새로 쓰기" 대신 "이미 있음" 복구 경로를 타서, 지키려던 경로를 시험하지 못한다.
5. **끄는 법:** `/etc/foundation-platform/staging-gate.off` 가 있으면 단계 3b 는 그렇다고 한 줄 쓰고 지나간다.
   `autodeploy.off` 는 배포 전체를 끈다. 새 자격 증명은 없다: 기존 환경 파일만 쓰며, `recovery` 그룹의 holds 에
   이미 들어 있던 `FOUNDATION_MIGRATOR_PASSWORD`·`FOUNDATION_API_PASSWORD` 를 명시했다.

검사: `crates/foundation-outbox` 의 `namespace_tests.rs`(키 빌더, 전송 직전 검사, `from_config` 가 만든 실제 클라이언트를
루프백 S3 에 — 인터셉터를 빼면 넷이 실패함을 확인), `orchestration/tests/test_staging_smoke.py`(대역 발행기·docker 로
전 단계, 표본 규칙, 실패 다섯 갈래, `database-url.sh`), `test_foundation_autodeploy.py`(스모크 통과 → 전환 진행, 실패 →
전환 없음·DAG 복귀; 유닛 시간 상한이 스모크 상한을 포함).

## Consequences

- 시간: 스모크 자체는 목표 10 분 미만(추정: 비우기 수 초, 빈 DB 스키마 1 분 안팎, 계획·인벤토리 1–3 분, 표본 수집
  ≤ 0.6 GB 1–3 분, 측정 수 초 — 첫 실행이 실측한다). 이미지 빌드는 새 비용이 아니라 앞당김이다: 같은 Dockerfile·같은
  컨텍스트라 단계 4 의 `migrate` 빌드가 캐시를 맞는다(첫 실행에서 확인할 것). 유닛 상한 4500 초.
- 비용: 새 인프라 0. R2 저장은 한 실행분(표본 ≤ 약 0.6 GB)이라 월 0.01 달러 미만(0.015 달러/GB-월), 요청은 배포당
  수십 건으로 무료 구간 안, 이그레스 없음. 배포마다 VWorld 에서 ≤ 0.6 GB 를 내려받는다.
- 위험: 제공자(VWorld·허브)가 멈추면 스모크가 실패해 배포가 막힌다 — 의도된 보수 쪽이며, 끄는 스위치와 다음 커밋이
  길이다. foundation-platform 계정이 docker 그룹이라는 전제(Spark 작업과 같다). Spark·Iceberg·타일·Worker 경로는
  시험하지 않는다.
- 첫 실행: 이 변경을 담은 커밋의 배포는 **이전** 제어 체크아웃의 `foundation-deploy.sh` 로 돌므로 스모크가 없다.
  그 다음 main 커밋의 배포부터 단계 3b 가 돈다. 소유자가 할 일은 없다. 첫 실행 뒤 볼 것: 배포 로그의
  `staging-smoke: passed release=… clear=…s image=…s …` 한 줄과 단계 4 빌드 시간(캐시 적중 여부).
