# ADR 0569: 플랫폼의 모든 변경은 직원 계정을 열쇠로 한 해시 사슬 감사 사건 하나로 남는다 — 더니어가 보이고, 코드에 든 설정은 더니어가 PR 로 제안한다

- Status: Proposed
- Date: 2026-11-19
- Revises: [ADR-0434](./0434-dawneer-is-served-from-the-data-host-behind-cloudflare-access-and-a-tunnel-and-relays-zitadels-sign-in-pages.md)
  §5(「더니어는 기록을 갖지 않는다. 주인마다 자기 기록」 → 기록의 정본은 여전히 Foundation 이지만, 모든 변경 기록이 **한 표**로 모이고 더니어가 그 표를 보인다)
- Builds on: [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md) §3(더니어는 틀만, 쓰기는 주인의 API),
  [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)(BFF·허용 목록 중계·CSRF),
  [ADR-0165](./0165-dawneer-shows-operations-status-from-the-data-catalog-and-the-job-list.md)(운영 현황 화면),
  [ADR-0282](./0282-operator-switches-are-issued-by-foundation-api-and-land-as-files-the-runner-reads.md)(운영자 원장 `catalog.operator_action`),
  [ADR-0433](./0433-a-by-pnu-cut-over-and-a-full-bake-are-approved-in-the-console-and-the-host-runs-them-in-the-operator-window.md)(cut-over·전량 굽기 승인),
  [ADR-0436](./0436-the-consoles-cloudflare-edge-is-opentofu-applied-by-the-host-with-the-existing-account-token-and-the-allowlist-is-the-identity-staff-list.md)(허용 목록 = 신원 플랫폼 직원 목록),
  [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)(지도 편집 D1),
  [ADR-0423](./0423-github-repository-settings-are-opentofu-state-in-r2-applied-by-the-host-with-a-scoped-token.md)(저장소 설정 = 코드),
  [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)(자동 배포)

## Context

사장(2026-11-18): "모든 기록은 더니어에, 계정별로. 대기업은 어떻게 하나?" 지금 플랫폼을 바꾸는 길은 여섯이고, 기록은 여섯 곳에
서로 다른 모양으로 흩어져 있다. 2026-11-19 실측(읽기만):

| 변경의 길 | 지금 기록이 있는 곳 | 실측 | 누가(열쇠) |
|---|---|---|---|
| 더니어 단추(작업 run-now·pause·cut-over·전량 굽기, ADR-0282·0433) | Foundation Postgres `catalog.operator_action`(append-only 트리거) + `switches/operator-actions.log` | **0줄**. 단추가 생긴 뒤 한 번도 쓰이지 않았다 | `identity.staff.id`(UUID) |
| 호스트 셸의 운영자 명령(`by-pnu-pack-operator.sh` 등, sudo) | journal 태그 `foundation-operator-start` | 10-01 이후 **183줄** | 유닉스 계정 |
| PR 병합(코드·설정·계약) | GitHub | 병합 PR **1,267개**(07-23 이후), 최근 300개 작성자·병합자 모두 같은 계정 하나 | GitHub 로그인 |
| 자동 배포 | 호스트 `/var/lib/perfectory/autodeploy/{deployed,failed,refused}/<sha>`(시각 한 줄) | deployed **439**·failed **53**·refused **8**(10-07 이후) | 없음(시스템) |
| 지도 편집(ADR-0112) | D1 `map_edit` → Iceberg `silver.map_edit_ledger` | **0줄**. `editor` 는 공유 쓰기 토큰을 든 호출자가 **스스로 적는 글자**다(검증된 신원이 아니다) | 자기 신고 |
| 저장소 설정(ADR-0423)·직원 역할 | PR + 호스트 적용 unit journal / identity `outbox_event` | 역할 부여 1건(`identity.staff.role_assigned.v1`), 직원 1명 | 섞임 |

- GitHub 조직 감사 로그 API(`/orgs/perfectory-inc/audit-log`)는 **404**다. 조직 요금제가 Free 이고, 그 API 는 Enterprise Cloud 에만 있다. 그래서
  GitHub 에만 기록을 두는 길은 처음부터 일부만 덮는다.
- `catalog.operator_action` 의 권한: `foundation_api` 에 `UPDATE`·`DELETE` 권한이 있고 트리거만 막는다. 표 주인(`foundation_migrator`)과 DB 관리자는
  트리거를 끄고 고칠 수 있으며, 고쳤다는 흔적은 남지 않는다.
- 사장의 단일 계정이 지금은 모든 변경을 하지만, 직원이 늘면 "누가 이 한도를 바꿨나"는 여섯 곳을 손으로 맞춰야 답이 나온다.

## 업계 비교

| | 사건 하나의 모양 | 누가 | 무엇을(전·후) | 보관 | 변조 증거 |
|---|---|---|---|---|---|
| **AWS CloudTrail** | `eventTime`·`eventSource`·`eventName`·`requestParameters`·`responseElements`·`errorCode`·`eventID`·`readOnly`·`resources[]` | `userIdentity`(`type`·`principalId`·`arn`·`sessionContext.attributes.mfaAuthenticated`·`invokedBy`·`onBehalfOf`) | 요청·응답 | 이벤트 기록 90일, trail·event data store 로 그 이상 | 파일마다 SHA-256, 매시 digest 파일(RSA 서명, **앞 digest 의 서명을 담아 사슬**) |
| **Google Cloud Audit Logs** | `serviceName`·`methodName`·`resourceName`·`status`·`request`·`response`·`resourceOriginalState` | `authenticationInfo.principalEmail`·`requestMetadata.callerIp` | `resourceOriginalState`(전)·`request`(후) | **Admin Activity 는 끌 수 없고** `_Required` 버킷 400일, 설정 불가 | 사건은 불변 |
| **GitHub 감사 로그** | `action`(`repo.create`, `pull_request.merge` 꼴 `영역.동사`)·`created_at`·`repo`·`operation`(create·modify·remove·access·…) | `actor`·`user`·`actor_location` | 동작별 필드 | 180일 | — |
| **Stripe Events** | `id`·`type`·`created`·`data.object`·`request.id`·`request.idempotency_key` | 계정·API 키 | `data.object`(후)·갱신 사건의 `previous_attributes`(전) | API 30일 | — |
| **SOC 2 CC8.1** | "변경을 승인·설계·개발·구성·문서화·시험·승인·구현한다" — 변경마다 요청자·승인·시험·배포가 **한 줄로 이어져** 추적되어야 한다 | 요청자와 승인자 | 무엇이 바뀌었나 | 감사 기간 내내 | — |

공통점: (1) 사건은 **주체·시각·동작(`영역.동사`)·대상·요청/결과**의 고정 틀이고, 원천(서비스)마다 틀을 바꾸지 않는다. (2) 관리 동작의 기록은 **끌 수 없고**(GCP
Admin Activity) 덧붙이기만 된다. (3) 변조는 **사슬로 된 digest 를 바깥에 두어** 드러낸다(CloudTrail). (4) 한 변경의 요청→승인→적용→결과가 **하나의 id 로
이어진다**(CloudTrail `sharedEventID`, Stripe `request.id`, SOC 2 의 추적). 그래서 모양은 CloudTrail·GCP 의 틀, 동작 이름은 GitHub 의 `영역.동사`,
전·후는 GCP `resourceOriginalState`·Stripe `previous_attributes` 의 짝, 무결성은 CloudTrail 의 digest 사슬에서 가져온다. 집에서 만든 모양을 쓰지 않는다.

## Decision

### 1. 더니어는 운영자 변경의 정문이고, 적용은 이미 검토된 길로만 한다

| 바꾸려는 것 | 더니어가 하는 일 | 적용하는 길 | 기록 |
|---|---|---|---|
| 코드·설정에 든 값(작업의 메모리·시간 한도 `orchestration/jobs.v1.json`, 호스트 예산 `tools/host-memory-budget.contract.json`, 계약 파일) — **첫째는 자원 한도** | 「변경 제안」 양식: 대상·새 값·이유 | Foundation API 가 GitHub App 으로 **그 직원 대신 PR** 을 연다 → CI 관문(호스트 예산 가드 등) → 승인 → merge queue → 자동 배포 | 제안·PR·검사·승인·병합·배포가 한 `change_id` |
| 실행 동작(run-now·pause·stop·cut-over·전량 굽기·판 승격) | 지금 단추 그대로 | ADR-0282·0433 의 원장 + `switches/` 파일 | 원장 줄과 **같은 트랜잭션**의 감사 사건 |
| 지도 편집, 원천 켜기·끄기 | 다음 단계에서 같은 정문으로 옮긴다(§7) | 편집은 Foundation API 가 신원을 붙여 D1 에, 원천 켜기는 `jobs.v1.json` 의 PR | 같은 표 |

더니어는 호스트·systemd·DB 에 직접 쓰지 않는다(ADR-0114 §3 유지). 더니어가 하는 것은 Foundation API 를 부르는 것뿐이다.

### 2. 감사 사건 하나의 모양 — Foundation Postgres `audit.event`

새 서비스·새 DB 는 없다. 운영자 원장이 이미 사는 운영 Postgres(`foundation`, ai-server, pgBackRest 백업)에 스키마 `audit` 을 둔다(ADR-0434 §5 의 "주인의
append-only 기록"과 같은 곳). 열:

| 열 | 뜻 | 본뜬 것 |
|---|---|---|
| `seq bigint` | 사슬 순서(1부터, 빈칸 없음) | CloudTrail digest 순서 |
| `event_id uuid` | 사건 id | `eventID` |
| `occurred_at`·`recorded_at` | 일이 일어난 시각 / 이 표에 들어온 시각(가져온 사건은 다르다) | `eventTime` |
| `source` | `dawneer`·`github`·`merge_queue`·`deploy`·`host_command`·`map_edit`·`repo_settings`·`identity` | `eventSource`·`serviceName` |
| `source_event_id` | 원천 안의 id(PR 번호·커밋·journal cursor·원장 id). `(source, source_event_id)` 유일 → 가져오기를 두 번 해도 한 줄 | `requestID` |
| `action` | `영역.동사`: `change.proposed`, `pull_request.merged`, `merge_queue.removed`, `deploy.succeeded`, `operator_action.run_now`, `host_command.cutover`, `map_edit.upsert`, `audit.exported` … 어휘는 계약 파일 하나 | GitHub `action` |
| `operation` | `create`·`modify`·`remove`·`execute`·`access` | GitHub `operation` |
| `actor_kind` | `staff`·`service`·`system`·`unlinked` | `userIdentity.type` |
| `actor_staff_id`·`actor_subject`·`actor_name` | `identity.staff.id`, 그 Zitadel `sub`, 그때의 표시 이름. 메일은 적지 않는다 | `principalId` |
| `actor_external` | 원천의 계정(`github:<login>`, `unix:<user>`) | `userName`·`invokedBy` |
| `actor_verified` | 원천이 신원을 검증했는가(지도 편집의 자기 신고 `editor` 는 `false`) | — |
| `on_behalf_of` | GitHub App 이 낸 PR 처럼 대리 실행일 때 실행 주체 | `onBehalfOf` |
| `object_type`·`object_id` | `job:onbid_silver_load`, `file:tools/host-memory-budget.contract.json#/…`, `pr:1410`, `commit:<sha>`, `lane:parcel-ownership` | `resourceName`·`resources[]` |
| `before`·`after` `jsonb` | 바뀐 값의 전·후(바뀐 키만). 실행 동작은 `after` 에 요청 인자 | `resourceOriginalState`·`previous_attributes` |
| `reason` | 사람의 이유(자유 글, 300자) | — |
| `applied_via` | `{kind: pr|approval|host_command|deploy|direct, ref}` | — |
| `result`·`error` | `requested`·`succeeded`·`failed`·`refused`·`expired` 와 이유 | `errorCode`·`status` |
| `change_id` | 한 변경의 사건들을 잇는 id(제안→PR→병합→배포) | `sharedEventID`·Stripe `request.id` |
| `prev_hash`·`row_hash` | §4 | CloudTrail digest 사슬 |

`actor` 는 **직원 계정이 열쇠다.** GitHub 로그인·유닉스 계정은 신원 플랫폼의 새 표 `identity.staff_external_account(staff_id, provider ∈ {github, unix},
external_id, linked_at, linked_by)`(덧붙이기만, `MASTER_ADMIN` 이 신원 API 로 잇는다)로 직원에 잇는다. 잇지 못한 주체는 버리지 않고 `actor_kind=unlinked`
로 남기고, 나중에 이어지면 화면이 그 연결로 묶어 보인다(행은 고치지 않는다).

### 3. 쓰는 길은 함수 하나다 — 표에 직접 `INSERT`·`UPDATE`·`DELETE` 권한이 없다

- 모든 쓰기는 `audit.append(...)`(SECURITY DEFINER) 하나로 한다. 함수는 `pg_advisory_xact_lock` 으로 줄을 세우고, 마지막 줄의 `row_hash` 를 `prev_hash`
  로 받아 새 줄의 해시를 계산해 넣는다.
- 역할: `foundation_api`(더니어 동작·제안·읽기)와 새 `foundation_audit_importer`(가져오기 작업)는 `audit.append` 의 `EXECUTE` 와 `SELECT` 만 갖는다.
  표의 `INSERT`·`UPDATE`·`DELETE`·`TRUNCATE` 권한은 아무 앱 역할에도 없다. 트리거가 표 주인의 `UPDATE`·`DELETE`·`TRUNCATE` 도 거부한다(ADR-0282 와 같은 틀).
- 더니어 동작은 원장 줄(`catalog.operator_action`)과 감사 사건을 **같은 트랜잭션**에 쓴다(같은 DB). 감사가 안 써지면 동작도 없다 — GCP Admin Activity 처럼
  끌 수 없다.

### 4. 변조 증거 — 해시 사슬과 바깥의 잠긴 digest

- `row_hash = SHA-256(prev_hash ‖ 정규화한 행 JSON(RFC 8785 JCS))`. 첫 줄의 `prev_hash` 는 32바이트 0.
- 매일 00:10 UTC 작업 `audit_digest` 가 (a) 마지막 digest 이후 줄의 사슬을 다시 계산해 맞는지 보고, (b) `{from_seq, to_seq, count, head_hash,
  previous_digest_sha256}` 를 **R2 `foundation-platform-postgres-recovery-prod` 의 `audit-digests/YYYY/MM/DD.json`** 에 쓴다. 그 접두사에 R2 bucket lock
  (보존 400일)을 건다 — 그 안의 객체는 계정 토큰으로도 지우거나 덮어쓸 수 없다. 새 버킷·새 비밀은 없다(백업 쓰기 열쇠가 이미 호스트에 있다).
- 사슬이 어긋나면 작업은 실패하고(OnFailure → 기존 알림), 그 seq 범위를 적는다. DB 관리자가 트리거를 끄고 줄을 고친 뒤 사슬을 다시 계산해도, 이미 잠긴
  digest 의 `head_hash` 와 다르므로 드러난다(CloudTrail 의 log file validation 과 같은 원리).

### 5. 보관

**지우지 않는다**(모든 데이터는 덧붙이기만, 사장 지시). 최소 보장은 GCP `_Required` 와 같은 400일이고, 그 뒤도 지우지 않는다. 계산값: 지금 속도(배포 하루
약 10, PR 하루 약 10, 호스트 명령 하루 약 4, 가져오기 사건 전부 합쳐 하루 100줄 미만)로 줄 하나 2 KiB 이면 1년 약 70 MiB — 운영 DB(112 GB)의 0.06%다. 백업은
기존 pgBackRest 가 함께 가져간다.

### 6. 다른 곳에서 일어난 변경을 가져온다 — 원천마다 자기 몫만, 커서로

가져오기는 작업 목록(`jobs.v1.json`)의 작업 하나 `audit_import` 이고 15분마다 돈다. 원천마다 `audit.import_cursor(source, cursor, advanced_at)` 를
갖고, **지난 커서 이후의 것만** 읽는다(전량 다시 읽지 않는다). 같은 사건을 두 번 읽어도 `(source, source_event_id)` 가 막는다.

| 원천 | 읽는 것 | 커서 | 주체 |
|---|---|---|---|
| `github` | 병합된 PR: 번호·제목·작성자·승인 리뷰·병합자·병합 커밋·바뀐 파일 목록. 커밋 꼬리말의 `Dawneer-Change`·`Requested-By` | PR `updated_at` | 작성자·병합자를 `github` 연결로 |
| `merge_queue` | PR 타임라인 `ADDED_TO_MERGE_QUEUE_EVENT`·`REMOVED_FROM_MERGE_QUEUE_EVENT`(누가·왜) | 같은 PR 커서 | 같음 |
| `deploy` | `/var/lib/perfectory/autodeploy/{deployed,failed,refused}/<sha>`, Worker 배포 상태 | 파일 시각 | `system`, `change_id` 는 그 커밋이 담은 PR 의 것 |
| `host_command` | journal `-t foundation-operator-start` 와 운영자 계정의 sudo 명령줄(`_COMM=sudo`) | journal cursor | `unix` 연결 |
| `map_edit` | 지도 편집 게이트웨이의 원장 내보내기(이미 있는 경로) | `change_seq` | 자기 신고 `editor` → `actor_verified=false` |
| `repo_settings` | ADR-0423 적용 unit 의 결과, 저장소 ruleset 기록(`/repos/{o}/{r}/rulesets/{id}/history`, Free 에서도 된다) | ruleset version id | GitHub 연결 |
| `identity` | 신원 `outbox_event`(역할 부여·회수, 외부 계정 연결) | `occurred_at`·event id | 부여한 직원 |

GitHub 읽기는 §8 의 GitHub App 설치 토큰으로 한다(GraphQL 은 인증이 필요하다). App 이 생기기 전에는 `github` 만 익명 REST(공개 저장소, 시간당 60회 — 하루 PR
약 10개면 충분)로 돌고, `merge_queue`·`repo_settings` 는 `waiting_for_credential` 로 화면에 보인다.

호스트 쪽 원천(배포 상태, journal)을 읽기 위해 작업 unit 은 `systemd-journal` 그룹과 `/var/lib/perfectory/autodeploy` 읽기만 갖는다. 쓰기는 `audit.append` 뿐이다.

로그인(Cloudflare Access·Zitadel)은 변경이 아니라 접근이므로 이 표 밖이다(GCP 의 Data Access 와 Admin Activity 를 나누는 것과 같다). 그 기록은 각자의
감사 기록에 남는다(ADR-0434 §5). 단, 감사 기록을 **내보낸 것**은 `audit.exported` 로 남긴다.

### 7. 더니어 화면 — 계정별·대상별·시간별, 내보내기

- 메뉴 **감사 기록**(`#/audit`). 거르기: 직원(연결된 외부 계정 포함), 대상(`object_type`·`object_id` 앞부분), 원천, 동작, 결과, 기간. 줄을 펼치면 전·후 차이,
  이유, 적용 길(PR·승인·호스트 명령 링크), 같은 `change_id` 의 사건 사슬이 보인다.
- 직원 페이지(`#/audit/actor/<staff_id>`), 대상 페이지(`#/audit/object/<type>/<id>`) — "이 작업의 한도는 누가 언제 왜 바꿨나" 가 한 화면이다.
- 내보내기: 지금 거른 결과를 JSON Lines 또는 CSV 로(서버가 흘려 보내고, 줄마다 `seq`·`row_hash` 포함, 끝에 그 범위의 digest 위치). 내보내기 자체가 사건이다.
- Foundation API: `GET /v1/audit/events`(커서 페이지, 최대 500), `GET /v1/audit/events/export`, `GET /v1/audit/import-status`. 범위
  `operations.audit.read`. 더니어 허용 목록에 GET 만 연다. 계약 `docs/openapi/audit.v1.json`, 화면 타입은 그것에서 생성한다.
- 지도 편집과 원천 켜기도 같은 정문으로 옮긴다: 지도 편집은 Foundation API 가 직원 신원을 붙여 게이트웨이에 쓰고(`actor_verified=true`), 원천 켜기는 §8 의 제안.

### 8. 「변경 제안」 — 코드에 든 설정은 직원 대신 PR 로

- `POST /v1/changes`(범위 `operations.changes.propose`, CSRF, `Idempotency-Key`): 본문 `{target, pointer, new_value, reason}`. `target` 은 계약
  파일 `config/console-changes.contract.json` 의 목록에 있는 파일·JSON Pointer 문법만(첫째: `jobs.v1.json` 의 작업 한도, `host-memory-budget.contract.json`).
  API 는 지금 main 의 파일을 읽어 `before` 를 정하고, 같은 값이면 거부한다.
- API 가 GitHub App 설치 토큰으로 가지 `change/<change_id>` 에 한 파일만 바꾼 커밋을 만들고 PR 을 연다. 커밋 꼬리말 `Dawneer-Change: <change_id>`,
  `Requested-By: <표시 이름> <staff_id>`, PR 본문에 대상·전·후·이유. 사람의 GitHub 계정이나 토큰은 쓰지 않는다(CloudTrail `onBehalfOf` 처럼 App 이 실행 주체,
  직원이 요청 주체).
- CI 의 모든 필수 검사(호스트 예산 가드 포함)가 그대로 돈다. 더니어는 PR 의 검사 상태를 보인다.
- **승인**: `POST /v1/changes/{id}/approve`(범위 `operations.changes.approve`)가 검사가 다 초록일 때만 App 으로 merge queue 에 넣는다. 요청자와 승인자가 같으면
  `self_approved=true` 로 남는다(직원이 한 명인 동안의 예외를 숨기지 않는다 — SOC 2 의 직무 분리를 나중에 켤 자리: 계약의 `require_distinct_approver`).
- 병합·배포는 §6 의 가져오기가 같은 `change_id` 로 잇는다. 화면의 한 줄: 제안됨 → PR #N → 검사 초록 → 승인 → 병합 → 배포됨(커밋) → 다음 run 이 새 한도로 돎.
- App 의 개인 키는 런타임 비밀 계약([ADR-0153](./0153-runtime-secrets-have-one-contract.md))의 새 그룹 `console-github-app`(root 0600, foundation-api 에만 마운트)에 있다. 저장소에는 없다.

## 버린 대안

| 대안 | 기각 이유 |
|---|---|
| **더니어가 호스트·systemd 에 직접 쓴다**(한도를 drop-in 으로, 원천을 파일로) | 검토(CI·호스트 예산 가드)를 건너뛴다. 코드와 호스트가 갈라지고(다음 배포가 되돌린다), API 에 root 쓰기 길이 생긴다 — ADR-0433 이 버린 "API 에 sudo 경로"와 같다 |
| **기록은 GitHub 에만**(PR 이 곧 감사) | 실행 동작·호스트 명령·배포·지도 편집이 GitHub 에 없다. 조직 감사 로그 API 는 Free 요금제에서 404(실측), 화면 기록도 180일이다. 신원이 Zitadel 직원이 아니라 GitHub 계정이다 |
| **원천마다 자기 기록, 더니어가 매번 여섯 곳을 모아 보인다** | 원천마다 모양·보관이 달라 계정별 질의가 안 되고, 원천이 지운 것은 사라진다. 변조 증거가 없다 |
| **새 감사 서비스·로그 저장소**(OpenSearch·Loki·DataHub 감사) | 새 서비스는 이 결정이 금한다. 질의 규모(하루 100줄)가 Postgres 로 충분하다. 해시 사슬·digest 는 표 하나로 된다 |
| **표의 트리거만으로 append-only**(ADR-0282 그대로) | 표 주인·DB 관리자가 트리거를 끄고 고치면 흔적이 없다(지금의 `catalog.operator_action`). 사슬과 바깥의 잠긴 digest 가 있어야 드러난다 |
| **Iceberg(레이크하우스)에 감사 표** | 동작과 같은 트랜잭션에 못 쓴다(동작은 Postgres). 화면의 짧은 질의에 Trino 를 탄다 |
| **더니어 서버 자신의 DB** | ADR-0114 §3: 더니어는 도메인 데이터를 갖지 않는다. 쓰기는 주인의 API |

## Consequences

- 단계(작은 PR, 앞에 기대는 것은 draft):
  1. 이 ADR.
  2. 저장소 + 화면: 마이그레이션(`audit.event`·`audit.append`·`audit.import_cursor`·역할·트리거), 사건 어휘 계약, 운영자 원장과 같은 트랜잭션의 쓰기,
     읽기·내보내기 API(+OpenAPI), 더니어 `#/audit`. 기존 `catalog.operator_action` 줄은 0이라 옮길 것이 없다.
  3. 가져오기 1: `deploy`·`host_command`(호스트 원천, 작업 `audit_import`). 첫 실행은 10-07 이후의 deployed 439·failed 53·refused 8, journal 183줄을 한 번
     가져온다(선언된 예외: 원천의 지금까지 전부, 약 700줄, 수 초).
  4. 가져오기 2: `github`(익명 REST, 병합 PR 1,267개를 페이지 100으로 13회), `identity`(+`staff_external_account`), `map_edit`.
  5. digest 작업 + R2 bucket lock + 사슬 검증 알림.
  6. 「변경 제안」: 자원 한도부터. W87 의 한도 화면(측정값·권장값) 옆에 "이 값으로 제안" 단추를 붙인다.
  7. App 이 생긴 뒤: `merge_queue`·`repo_settings`.
- **소유자가 하는 일(한 번):** GitHub App 만들기. 조직 `perfectory-inc` 에 App `perfectory-console`, 권한 Contents·Pull requests **Read and write**,
  Metadata·Checks **Read**, 설치는 `perfectory-public` 하나. 개인 키를 손으로 옮기지 않도록 GitHub App manifest 흐름(`POST /app-manifests/{code}/conversions`)
  을 더니어의 설정 화면 단추 하나로 만들고, 변환 결과(키)는 호스트가 바로 런타임 비밀 파일로 받는다 — 그 단추를 누르는 것이 소유자의 유일한 손이다.
  App 이 없는 동안 1–5 는 된다(제안·병합 대기열·ruleset 만 기다린다).
- 신원: `identity.staff_external_account` 에 사장의 GitHub 로그인과 ai-server 유닉스 계정을 한 번 잇는다(신원 API, 직원 본인 계정 정보뿐 — 공개 저장소에는
  이름·메일을 적지 않는다).
- 비용: 돈 0(R2 객체 하루 하나, 수 KB; 새 서비스 없음). 호스트: 15분마다 가져오기 한 번(목표 상한 256 MiB, 5분), digest 하루 한 번. 호스트 메모리 예산에
  `audit_import` 의 상한을 더한다(W87 의 예산 가드가 판정).
- 남는 일: 지도 편집 게이트웨이의 공유 쓰기 토큰 → 직원 신원(§7, 별도 PR), Cloudflare Access·Zitadel 로그인 기록을 접근 감사로 가져올지는 따로 판단,
  직원이 둘 이상이 되면 `require_distinct_approver` 켜기.
- 출처: [CloudTrail record contents](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html),
  [userIdentity](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-user-identity.html),
  [log file integrity validation](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-validation-intro.html),
  [Cloud Audit Logs](https://cloud.google.com/logging/docs/audit), [AuditLog](https://cloud.google.com/logging/docs/audit/api/ref/rest/Shared.Types/AuditLog),
  [Logging quotas(`_Required` 400일)](https://cloud.google.com/logging/quotas),
  [GitHub 조직 감사 로그](https://docs.github.com/en/organizations/keeping-your-organization-secure/managing-security-settings-for-your-organization/reviewing-the-audit-log-for-your-organization),
  [Stripe Event object](https://docs.stripe.com/api/events/object), AICPA Trust Services Criteria(2017) CC8.1,
  [R2 bucket locks](https://developers.cloudflare.com/r2/buckets/bucket-locks/), RFC 8785(JSON Canonicalization Scheme).
