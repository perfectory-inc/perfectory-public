# ADR 0569: 플랫폼의 모든 변경은 직원 계정을 열쇠로 한 해시 사슬 감사 사건 하나로 남는다 — 더니어가 보이고, 코드에 든 설정은 더니어가 PR 로 제안한다

- Status: Accepted; §3 의 쓰는 이 잠금·READ COMMITTED 조건·503 길과 §4 의 줄마다 사슬(`prev_hash`·`row_hash`)·digest 모양은 Superseded by [ADR-0584](./0584-the-audit-record-is-witnessed-by-its-hourly-digest-over-ordered-rows-not-chained-row-by-row.md) (2026-11-19, 소유자 승인). 나머지는 그대로다
- Date: 2026-11-19
- Revises: [ADR-0434](./0434-dawneer-is-served-from-the-data-host-behind-cloudflare-access-and-a-tunnel-and-relays-zitadels-sign-in-pages.md) §5
  ("승인·결정은 Foundation API 를 거쳐 그 주인의 append-only 기록에 남는다. 더니어는 기록을 갖지 않는다.")
  — **유지**: 더니어는 기록을 갖지 않고 정본은 Foundation 이다. **바뀜**: 주인마다 흩어진 기록 대신 모든 변경이 Foundation 의 표 하나
  (`audit.event`)에 모이고, 더니어가 그 표를 보인다. 로그인(Access·Zitadel)은 §5 그대로 각자의 기록에 남는다.
  [ADR-0282](./0282-operator-switches-are-issued-by-foundation-api-and-land-as-files-the-runner-reads.md) §2
  — **유지**: `catalog.operator_action` 과 트리거. **바뀜**: `foundation_api` 의 그 표 `UPDATE`·`DELETE`·`TRUNCATE` 권한을 회수한다.
  `consumed_at`·`outcome` 을 채우는 코드는 지금 저장소에 없다(2026-11-19 실측, 시험만 쓴다). 채우는 문이 생기면 그 두 열만
  열 단위 `GRANT UPDATE (consumed_at, outcome)` 로 준다. 원장의 모든 줄과 응답은 트리거로 감사 사건이 된다(§3).
- Builds on: [ADR-0114](./0114-dawneer-is-the-staff-console-built-new-in-the-monorepo.md) §3(더니어는 틀만, 쓰기는 주인의 API),
  [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)(BFF·허용 목록 중계·CSRF),
  [ADR-0165](./0165-dawneer-shows-operations-status-from-the-data-catalog-and-the-job-list.md)(운영 현황 화면),
  [ADR-0433](./0433-a-by-pnu-cut-over-and-a-full-bake-are-approved-in-the-console-and-the-host-runs-them-in-the-operator-window.md)(cut-over·전량 굽기 승인),
  [ADR-0436](./0436-the-consoles-cloudflare-edge-is-opentofu-applied-by-the-host-with-the-existing-account-token-and-the-allowlist-is-the-identity-staff-list.md)(허용 목록 = 직원 목록),
  [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)(지도 편집),
  [ADR-0423](./0423-github-repository-settings-are-opentofu-state-in-r2-applied-by-the-host-with-a-scoped-token.md)(저장소 설정 = 코드),
  [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md)(자동 배포),
  [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md)(Sigstore·cosign 이 이미 호스트에 있다),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)(런타임 비밀 계약)

## Context

사장(2026-11-18): "모든 기록은 더니어에, 계정별로. 대기업은 어떻게 하나?" 지금 플랫폼을 바꾸는 길은 여럿이고, 기록은 곳마다 다른 모양으로
흩어져 있다. 2026-11-19 실측(읽기만, ai-server 와 GitHub):

| 변경의 길 | 지금 기록이 있는 곳 | 실측 | 누가(열쇠) |
|---|---|---|---|
| 더니어 단추(run-now·pause·stop·cut-over·전량 굽기, ADR-0282·0433) | Foundation Postgres `catalog.operator_action`(트리거로 append-only) + `switches/operator-actions.log` | **0줄** — 단추가 생긴 뒤 한 번도 쓰이지 않았다 | `identity.staff.id` |
| 더니어·API 의 지도 편집(ADR-0112) | Foundation API `routes/catalog/map_edits.rs` 가 직원 신원(`editor = StaffId(principal)`)을 붙여 D1 에 쓴다 → 접을 때 Iceberg `silver.map_edit_ledger` | `silver.map_edit_ledger` **0줄**. 접힌 D1 행은 지워진다 | `identity.staff.id` |
| 호스트 셸의 운영자 명령(`by-pnu-pack-operator.sh` 등, sudo) | journal 태그 `foundation-operator-start`(본문의 `who=`) | 10-01 이후 **183줄** | 유닉스 계정(글자) |
| PR 병합(코드·설정·계약) | GitHub | 병합 PR **1,267개**(07-23 이후). 최근 300개는 작성자·병합자 모두 같은 계정 | GitHub 계정 |
| 자동 배포 | `/var/lib/perfectory/autodeploy/{deployed,failed,refused}/<sha>` | deployed **439**·failed **53**·refused **8**(10-07 이후). deployed 파일 내용은 시각 한 줄, refused 는 거부 이유 | 없음(시스템) |
| 저장소 설정(ADR-0423)·직원 역할 | PR + 호스트 적용 unit journal / identity `outbox_event` | 역할 부여 1건(`identity.staff.role_assigned.v1`), 직원 1명 | 섞임 |

- GitHub 조직 감사 로그 API(`/orgs/perfectory-inc/audit-log`)는 **404** 다. 조직 요금제가 Free 이고 그 API 는 Enterprise Cloud 전용이다.
- 저장소 규칙: `main-ruleset.json` 의 `required_approving_review_count` 는 0, 병합은 squash 이고 커밋 메시지는 PR 본문이다
  (`repository-settings.json` `squash_merge_commit_message: PR_BODY`). `non-main-branch-firewall.json` 은 bypass 계정 말고는 main 밖 가지를 만들지 못하게 한다.
- `catalog.operator_action` 에서 `foundation_api` 는 `UPDATE`·`DELETE` 권한을 갖고 트리거만 그것을 막는다. 표 주인과 DB 관리자는 트리거를 끄고 고칠 수
  있으며, 고친 흔적은 남지 않는다.
- 지금은 사장의 계정 하나가 모든 변경을 한다. 직원이 늘면 "누가 이 한도를 바꿨나"에 답하려면 여러 곳을 손으로 맞춰야 한다.

## 업계 비교

| | 사건 하나의 모양 | 누가 | 무엇을(전·후) | 보관 | 변조 증거 |
|---|---|---|---|---|---|
| **AWS CloudTrail** | `eventTime`·`eventSource`·`eventName`·`requestParameters`·`responseElements`·`errorCode`·`eventID`·`readOnly`·`resources[]`·`sharedEventID` | `userIdentity`(`type`·`principalId`·`sessionContext.attributes.mfaAuthenticated`·`invokedBy`·`onBehalfOf`) | 요청·응답 | 이벤트 기록 90일, 그 이상은 trail·event data store | 파일마다 SHA-256, 매시 digest(RSA 서명, **앞 digest 의 서명을 담은 사슬**) |
| **Google Cloud Audit Logs** | `serviceName`·`methodName`·`resourceName`·`status`·`request`·`response`·`resourceOriginalState` | `authenticationInfo.principalEmail`·`requestMetadata.callerIp` | `resourceOriginalState`(전)·`request`(후) | **Admin Activity 는 끌 수 없다**. `_Required` 버킷 400일, 설정 불가 | 사건은 불변 |
| **GitHub 감사 로그** | `action`(`repo.create`·`pull_request.merge` 꼴 `영역.동사`)·`created_at`·`repo`·`operation`(create·modify·remove·access·…) | `actor`·`user`·`actor_location` | 동작별 필드 | 180일 | — |
| **Stripe Events** | `id`·`type`·`created`·`data.object`·`request.id`·`request.idempotency_key` | 계정·API 키 | `data.object`(후), 갱신 사건의 `previous_attributes`(전) | API 30일 | — |
| **SOC 2 CC8.1** | 변경은 승인·설계·개발·구성·문서화·시험·승인·구현된다. 요청에서 배포까지 **한 줄로 추적**된다 | 요청자와 승인자 | 무엇이 바뀌었나 | 감사 기간 내내 | — |
| **Sigstore Rekor**(투명성 기록) | 서명된 해시 항목 | 서명 키 | — | 공개, 지울 수 없음(Merkle 트리) | 포함 증명·일관성 증명 |

우리가 가져오는 것:

| 가져오는 것 | 출처 |
|---|---|
| 사건의 고정 틀(주체·시각·동작·대상·요청과 결과). 원천마다 틀을 바꾸지 않는다 | CloudTrail, GCP |
| 끌 수 없는 관리 동작 기록 | GCP Admin Activity |
| 동작 이름 `영역.동사`, `operation` 분류 | GitHub |
| 전·후 값의 짝 | GCP `resourceOriginalState`, Stripe `previous_attributes` |
| 한 변경을 잇는 id | CloudTrail `sharedEventID`, Stripe `request.id`, SOC 2 의 추적 |
| 매시 digest 사슬 | CloudTrail |
| 호스트가 지울 수 없는 바깥 증인 | Rekor. ADR-0276 이 이미 Sigstore 를 쓴다 |

## 위협 모형(무엇을 잡고 무엇을 못 잡나)

| 누가 고치려 하나 | 잡는가 | 어떻게 |
|---|---|---|
| 앱 역할(`foundation_api`, 가져오기) | 막는다 | 표에 쓰기 권한이 없고 `audit.append` 만 부른다. 함수가 원천과 역할을 맞춘다(§3) |
| 표 주인·DB 관리자(호스트 root 아님) | 드러난다 | 트리거를 끄고 줄을 고친 뒤 사슬을 다시 계산해도, 이미 Rekor 에 올라간 digest 와 다르다(§4) |
| 호스트 root·Cloudflare 계정 토큰을 가진 자 | **마지막 증인 이후 1시간 안의 줄만** 몰래 고칠 수 있다. 앞으로의 증인을 멈출 수는 있다 | Rekor 항목은 누구도 지우지 못한다. 증인이 2시간 넘게 없으면 알림이 울린다. 단, root 는 알림도 끌 수 있다 — 이것이 이 설계의 한계다 |
| R2 bucket lock 만으로 막기 | 못 잡는다 | lock 은 버킷 설정이다. R2 편집 권한이 있는 토큰(호스트가 ADR-0436 으로 이미 가졌다)이면 지울 수 있다. 그래서 쓰지 않는다 |

## Decision

### 1. 더니어는 운영자 변경의 정문이고, 적용은 이미 검토된 길로만 한다

| 바꾸려는 것 | 더니어가 하는 일 | 적용하는 길 | 감사 |
|---|---|---|---|
| 코드·설정에 든 값. **첫째는 자원 한도**: `platforms/foundation-platform/orchestration/jobs.v1.json` 의 작업 한도, `tools/host-memory-budget.contract.json` | 「변경 제안」 양식: 대상·새 값·이유 | Foundation API 가 GitHub App 으로 **그 직원을 대신해 draft PR** 을 연다. 그 뒤는 CI 관문(호스트 예산 가드 등) → **사람이 GitHub 에서 ready** → merge queue → 자동 배포(§8) | 제안·PR·병합·배포가 한 `change_id` |
| 실행 동작(run-now·pause·stop·cut-over·전량 굽기·판 승격) | 지금 단추 그대로 | ADR-0282·0433 의 원장 + `switches/` 파일 | 원장 트리거가 같은 트랜잭션에(§3) |
| 지도 편집 | 지금 API 그대로(이미 직원 신원) | `map_edits.rs` → D1 | 그 요청 안에서 요청·결과(§3) |
| 원천 켜기·끄기 | 「변경 제안」 | `jobs.v1.json` 의 `enabled` PR | 첫 칸과 같음 |

더니어는 호스트·systemd·DB·GitHub 에 직접 쓰지 않는다(ADR-0114 §3 유지). 더니어는 Foundation API 를 부를 뿐이다.

### 2. 감사 사건 하나의 모양 — Foundation Postgres `audit.event`

새 서비스·새 DB 는 없다. 운영자 원장이 이미 사는 운영 Postgres(`foundation`, ai-server, pgBackRest 백업)에 스키마 `audit` 을 둔다.
열과 제약은 [부록 A](#부록-a-ddl-초안) 의 DDL 이다. 뜻:

| 열 | 뜻 | 본뜬 것 |
|---|---|---|
| `seq` | 사슬 순서. 1부터 빈칸 없이 `max+1`(시퀀스를 쓰지 않는다 — 되돌린 트랜잭션이 번호를 먹는다) | CloudTrail digest 순서 |
| `event_id` | 사건 id | `eventID` |
| `occurred_at`·`recorded_at` | 일어난 시각 / 이 표에 들어온 시각. 가져온 사건은 둘이 다르다 | `eventTime` |
| `source` | `dawneer`·`github`·`merge_queue`·`deploy`·`host_command`·`repo_settings`·`identity`·`audit` | `eventSource` |
| `source_event_id` | 원천 안에서 **동작 하나**의 id(§6 의 표). `(source, source_event_id)` 가 유일해 두 번 가져와도 한 줄이다 | `requestID` |
| `action`·`operation` | `영역.동사`(`change.proposed`·`pull_request.merged`·`deploy.succeeded`·`operator_action.run_now`·`host_command.cutover`·`map_edit.upsert`·`audit.exported`·`audit.import_gap` …)와 `create`·`modify`·`remove`·`execute`·`access` | GitHub |
| `actor_kind` | `staff`·`service`·`system`·`unlinked` | `userIdentity.type` |
| `actor_staff_id`·`actor_subject` | `identity.staff.id` 와 그 Zitadel `sub`. **이름·메일은 적지 않는다** — 화면이 읽을 때 신원 API 에서 붙인다 | `principalId` |
| `actor_external` | 원천의 불변 id: `github:<숫자 user id>`, `unix:<uid>@<호스트>`. 로그인 이름처럼 바뀌는 값은 쓰지 않는다 | `invokedBy` |
| `actor_verified` | 원천이 그 신원을 검증했는가 | — |
| `on_behalf_of` | 대리 실행일 때 실행 주체(GitHub App 이 낸 PR 이면 `github-app:<app id>`) | `onBehalfOf` |
| `object_type`·`object_id` | `job`·`onbid_public_auction_silver`, `file`·`tools/host-memory-budget.contract.json#/…`, `pull_request`·`1410`, `commit`·`<sha>`, `lane`·`parcel-ownership` | `resourceName` |
| `before`·`after` | 바뀐 키의 전·후. 실행 동작이면 `after` 에 요청 인자 | `resourceOriginalState`·`previous_attributes` |
| `reason` | 사람이 적은 이유(300자) | — |
| `applied_via_kind`·`applied_via_ref` | `pull_request`·`approval`·`host_command`·`deploy`·`api` 와 그 번호나 id | — |
| `result`·`error` | `requested`·`succeeded`·`failed`·`refused`·`expired` 와 그 이유 | `errorCode`·`status` |
| `change_id` | 한 변경의 사건을 잇는 id(§6) | `sharedEventID` |
| `prev_hash`·`row_hash` | §4 | digest 사슬 |

**주체의 열쇠는 직원 계정이다.** 원천의 계정은 신원 플랫폼의 새 표 `identity.staff_external_account` 로 직원에 잇는다. 열은
`staff_id, provider ∈ {github, unix}, external_id, valid_from, valid_to, linked_by` 이고 덧붙이기만 한다: 연결을 끊으면 `valid_to` 가 든 새 줄이 생긴다.
잇는 것은 `MASTER_ADMIN` 이 신원 API(`POST /identity/v1/staff/{id}/external-accounts`)로 한다. 사건은 **그 `occurred_at` 에 유효했던 연결**로 풀이하고,
행 자체는 고치지 않는다. 잇지 못한 주체는 `unlinked` 로 남고 버리지 않는다. 신원 사건 `identity.staff.external_account_linked.v1`·`…_unlinked.v1` 을 더한다.

### 3. 쓰는 길은 함수 하나다

- `audit.append(...)` 는 SECURITY DEFINER 이고 `search_path` 를 `audit, pg_temp` 로 고정한다.
  - `transaction_isolation` 이 `read committed` 가 아니면 거부한다. REPEATABLE READ 에서는 스냅샷이 잠금보다 먼저 잡혀, 두 줄이 같은 `prev_hash` 를 받는다.
  - 상수 키의 `pg_advisory_xact_lock` 을 잡은 뒤 마지막 줄을 읽고, `seq = 마지막 + 1`, `prev_hash = 마지막 row_hash` 로 해시를 계산해 넣는다.
- **원천은 호출자가 정하지 않는다.** 함수가 `session_user` 로 원천을 제한한다. `foundation_api` 는 `dawneer` 만 쓰고, 그 줄은 `actor_kind = staff`,
  `actor_verified = true` 다(API 가 토큰을 검증했다). 새 역할 `foundation_audit_importer` 는 `dawneer` 를 쓸 수 없다. 그 역할의 `actor_verified` 는 계약이 검증 원천으로 적은
  원천(`github`·`identity`)에서만 참이다.
- 권한: 두 역할은 `audit.append`·`audit.advance_cursor` 의 `EXECUTE` 와 `audit` 표의 `SELECT` 만 갖는다. 어떤 앱 역할에도 표의 `INSERT`·`UPDATE`·`DELETE`·`TRUNCATE` 는
  없다. 트리거가 `UPDATE`·`DELETE`·`TRUNCATE` 를 거부하지만, 표 주인과 DB 관리자는 트리거를 끌 수 있다 — 그것을 잡는 것은 §4 다. 역할·스키마는
  기존 기동 SQL(`bootstrap-foundation.sql`·`grant-foundation-runtime.sql`·`finalize-foundation.sql`)이 만들고 굳힌다. `compose-smoke.sh` 가 권한 거부를 확인한다.
- **더니어 동작은 같은 트랜잭션이다.** `catalog.operator_action` 의 AFTER INSERT 트리거와 `consumed_at` 을 채우는 AFTER UPDATE 트리거가 `audit.append` 를
  부른다. 원장 줄이 있으면 감사 줄도 있고, 감사를 못 쓰면 원장 줄도 없다(GCP Admin Activity 처럼 끌 수 없다). 스위치 파일 쓰기와 Airflow 시작은 커밋 뒤에 있으므로,
  API 가 그 결과를 `succeeded`·`failed` 줄로 하나 더 남긴다. 지도 편집도 같다: 요청 줄을 쓰고, D1 의 답을 결과 줄로 남긴다.
- 잠금 다툼: 가져오기는 트랜잭션 하나에 50줄까지 쓴다. API 쪽은 `lock_timeout = 2s` 이다. 넘으면 동작이 503 으로 끝나고 원장 줄도 없다.

### 4. 변조 증거 — 해시 사슬과 지울 수 없는 바깥 증인

- `row_hash = sha256(prev_hash ‖ convert_to(canon(row), 'UTF8'))`. 첫 줄의 `prev_hash` 는 32바이트 0이다. `canon(row)` 는 SQL 함수 하나로 정한다.
  - `seq`·`row_hash` 를 뺀 모든 열의 `jsonb_build_object(...)::text` 를 쓴다. Postgres 의 jsonb 출력은 같은 값이면 같은 글자다.
  - 시각은 UTC `YYYY-MM-DD"T"HH24:MI:SS.US"Z"` 이고, NULL 은 JSON `null`, bytea 는 소문자 hex 다.
  - 같은 함수를 시험(정해 둔 입력과 기대 해시)이 고정한다. 계산은 DB 안에서만 하고, 다른 언어로 베끼지 않는다.
- **매시 digest.** 작업 `audit_digest`(작업 목록에 선언, 매시 5분)가 마지막 digest 이후 줄의 사슬을 다시 계산한다. 그 범위 첫 줄의 `prev_hash` 가 앞 digest 의
  `head_hash` 와 같아야 한다. 같으면 `{from_seq, to_seq, count, head_hash, previous_digest_sha256}` 를 `audit.digest`(같은 append-only 규칙)에 쓴다.
- **증인은 Rekor 다.** 그 digest 의 SHA-256 을 고정 판 cosign(ADR-0276 의 `tools/pinned-artifacts.json`)의 `sign-blob --key` 로 서명해 공개 Rekor 에
  올리고, 받은 `logIndex` 와 포함 증명을 `audit.digest` 에 함께 둔다.
  - 올라가는 것은 해시뿐이다. 개인 정보는 없다.
  - 서명 키는 호스트가 처음 한 번 만들어 새 런타임 비밀 그룹 `audit-witness`(root 0600)에 둔다. 공개 키는 저장소의 `config/audit-witness.pub` 이다.
    누구든 그 공개 키로 Rekor 를 찾아 digest 를 대조할 수 있다.
- **검증.** 매시 검증은 범위만 본다. 매주 일요일에 처음부터 전체 사슬을 다시 계산해 모든 digest 와 Rekor 항목을 대조한다.
  - 어긋나거나, 증인이 2시간 넘게 없거나, Rekor 에 못 올리면 작업이 실패한다. 실패는 OnFailure → 기존 알림으로 가고, 그 seq 범위를 적는다.

### 5. 보관

**지우지 않는다**(모든 데이터는 덧붙이기만 한다, 사장 지시). 최소 보장은 GCP `_Required` 와 같은 400일이고, 그 뒤에도 지우지 않는다.
계산값: 지금 속도로 가져오는 사건까지 합쳐 하루 100줄 미만이다. 줄 하나를 2 KiB 로 잡으면 1년에 약 70 MiB 이고, 운영 DB(112 GB)의 0.06% 다.
백업은 기존 pgBackRest 가 함께 가져간다.
지울 수 없으므로 **비밀과 인자는 넣지 않는다**. 이름·메일은 열에 없다. 호스트 명령은 허용 목록의 명령 이름과 선언된 인자(레인·세대)만 남긴다(§6).

### 6. 다른 곳에서 일어난 변경을 가져온다 — 원천마다 자기 몫만

가져오기는 작업 목록의 작업 `audit_import` 하나이고 15분마다 돈다. 원천마다 `audit.import_cursor` 를 갖고, `audit.advance_cursor` 로 그 원천의 줄과 같은
트랜잭션에서 커서를 민다.
- 커서는 **겹치게** 읽는다(지난 커서에서 한 구간 뒤로). 중복은 `(source, source_event_id)` 가 지운다.
- 커서가 쓸 수 없게 되면 `audit.import_gap` 사건을 남기고 실패해 알린다. 예: journal 이 지워져 cursor 가 사라졌다.

| 원천 | 읽는 것 | `source_event_id` | 커서 | 주체 |
|---|---|---|---|---|
| `github` | PR(병합·닫힘), 승인 리뷰, 바뀐 파일 목록. GraphQL 로 한 질의에 PR 50개 | `pr:<n>:opened`·`pr:<n>:review:<id>`·`pr:<n>:merged`·`pr:<n>:closed` | `updatedAt`, 1시간 겹침 | `github:<user id>`, 검증됨 |
| `merge_queue` | 같은 질의의 타임라인 `ADDED_TO_MERGE_QUEUE_EVENT`·`REMOVED_FROM_MERGE_QUEUE_EVENT` | `pr:<n>:mq:<node id>` | 같음 | 같음 |
| `deploy` | `autodeploy/{deployed,failed,refused}/` **전부를 매번**(약 500개 파일, 1초 미만). 시각은 deployed·failed 파일의 내용, refused 는 파일 시각. 그 배포가 담은 PR 들은 호스트의 git 미러(`control-source.git`)에서 `이전 deployed..target` 범위로 읽는다 | `deploy:<state>:<sha>`, 담은 PR 마다 `deploy:<state>:<sha>:pr:<n>` | 없음(전부 다시, 중복 제거) | `system` |
| `host_command` | journal 중 `SYSLOG_IDENTIFIER=foundation-operator-start` 이고 **`_UID=0`** 인 줄(아무 사용자의 `logger -t` 위조를 거른다). 주체는 본문의 `who=`(sudo 의 `SUDO_UID`) | `journal:<boot id>:<seqnum>` | journal cursor | `unix:<uid>@ai-server` |
| `repo_settings` | ADR-0423 적용 unit 의 결과, ruleset 기록(`/repos/{o}/{r}/rulesets/{id}/history`) | `ruleset:<id>:<version id>` | version id | GitHub 연결 |
| `identity` | 신원 API 의 새 읽기 경로 `GET /identity/v1/audit-feed?after=<event_id>`(outbox 를 `occurred_at, event_id` 순으로, 10분 겹침) | `identity:<event_id>` | 같음 | 부여한 직원 |

- **`change_id`.**
  - 더니어 제안: 제안 사건이 PR 번호를 갖는다. 가져오기는 PR 번호로 잇는다. 커밋 꼬리말·PR 본문은 정보일 뿐이고 증거로 쓰지 않는다(누구나 고칠 수 있다).
  - 더니어 밖의 PR: `change_id = uuid_v5(고정 이름공간, "pr:<n>")` 다.
  - 배포: 한 배포가 여러 PR 을 담으면 PR 마다 배포 사건 하나를 남긴다.
- 가져오기는 GitHub 를 §8 의 App 설치 토큰으로만 읽는다(시간당 5,000회; GraphQL 은 인증이 필요하다). 익명 REST 는 쓰지 않는다.
  익명 한도(IP 당 시간당 60회)는 `foundation-autodeploy.sh` 가 이미 쓰고 있다. 첫 가져오기는 병합 PR 1,267개를 50개씩 26질의로 읽는다.
  App 이 생기기 전에는 `github`·`merge_queue`·`repo_settings` 가 화면에 `waiting_for_credential` 로 보인다(`audit.import_cursor.state`).
- 작업 unit 이 갖는 것은 셋뿐이다: `SupplementaryGroups=systemd-journal`, `ReadOnlyPaths=` 배포 상태와 git 미러, 그리고 DB 역할 `foundation_audit_importer`.
  상한은 `MemoryMax=256M`·`RuntimeMaxSec=600` 이고, 그 합이 호스트 메모리 예산 계약에 들어간다.
- 첫 가져오기는 선언된 예외로, 원천의 지금까지 전부를 읽는다. 배포 약 500줄, journal 183줄, PR 1,267개(PR 하나에 사건 2–4개)다.

로그인(Cloudflare Access·Zitadel)은 변경이 아니라 접근이므로 이 표 밖이다. GCP 가 Data Access 를 Admin Activity 와 나누는 것과 같다.
단, 감사 기록을 **내보낸 것**은 `audit.exported` 로 남긴다.

### 7. 더니어 화면과 API

- 메뉴 **감사 기록**(`#/audit`).
  - 거르기: 직원(연결된 외부 계정 포함), 대상(`object_type`·`object_id` 앞부분), 원천, 동작, 결과, 기간.
  - 줄을 펼치면 전·후 차이, 이유, 적용 길(PR·승인·호스트 명령 링크), 같은 `change_id` 의 사건 사슬이 보인다.
- 직원 페이지(`#/audit/actor/<staff_id>`)와 대상 페이지(`#/audit/object/<type>/<id>`). "이 작업의 한도는 누가 언제 왜 바꿨나"가 한 화면에 나온다.
- 내보내기: 지금 거른 결과를 JSON Lines 나 CSV 로 흘려 보낸다. 줄마다 `seq`·`row_hash` 가 있고, 끝에 그 범위를 덮는 digest 와 Rekor `logIndex` 가 붙는다.
- API: Foundation 데이터 카탈로그 경로 아래에 둔다. 더니어의 `DATA_CATALOG_ROUTES` 에 더하고, 계약은 기존 `docs/openapi/data-catalog.v1.json` 을 늘린다.
  - `GET /data-catalog/v1/audit/events`: `seq` 커서 페이지(최대 500). 시간으로 거르되 순서는 `seq` 다 — 늦게 가져온 사건도 빠지지 않는다.
  - `GET /data-catalog/v1/audit/events/export`
  - `GET /data-catalog/v1/audit/import-status`
- 권한은 `(resource, action)` 짝이다(`config/identity-route-policies.contract.json` + 신원의 역할 부여).
  - `foundation.operations`·`audit_read`
  - `foundation.operations`·`change_propose`
  - `identity.staff_external_account`·`link`

### 8. 「변경 제안」 — 코드에 든 설정은 직원 대신 draft PR 로

- `POST /data-catalog/v1/changes`(`change_propose`, CSRF, `Idempotency-Key` 는 `lineage_review.rs` 의 처리를 그대로 쓴다)
  - 본문은 `{target, pointer, new_value, reason}` 이다.
  - `target`·`pointer` 문법과 값의 모양은 계약 `config/console-changes.contract.json` 의 목록에 든 것만 받는다. 첫 목록은 작업 한도와 호스트 예산이다.
  - API 는 지금 main 의 파일을 읽어 `before` 를 정한다. 같은 값이면 거부한다.
- API 가 GitHub App 설치 토큰으로 PR 을 연다.
  - 가지 `change/<change_id>` 에 **그 파일 하나만** 바꾼 커밋을 만들고 **draft** PR 을 연다.
  - PR 본문에는 대상·전·후·이유와 `Requested-By: <staff_id>` 가 들어간다. squash 병합이면 PR 본문이 커밋 메시지가 되므로 main 에도 남는다.
  - 사람의 GitHub 계정이나 토큰은 쓰지 않는다. App 이 실행 주체(`on_behalf_of`)이고, 직원이 요청 주체다.
- **병합은 사람이 한다.** App 은 PR 을 열 수만 있고 ready·승인·대기열 넣기를 하지 않는다.
  - CI 의 필수 검사(호스트 예산 가드 포함)가 그대로 돌고, 사람이 GitHub 에서 ready 로 바꾸면 기존 keeper 가 대기열에 넣는다.
  - 그래서 API 가 털려도 할 수 있는 것은 draft PR 을 여는 것뿐이고, 자동 배포까지는 닿지 않는다.
  - 가져오기는 병합된 `change/*` PR 의 diff 가 제안의 파일·pointer·값과 다르면 `change.diverged` 사건을 남기고 알린다(승인 뒤 누가 가지에 덧푸시한 경우).
- 가지 규칙(ADR-0423 의 코드로, Rego 정책 통과):
  - `non-main-branch-firewall.json` 의 대상에서 `refs/heads/change/**` 를 뺀다.
  - 새 ruleset 은 `change/**` 를 만들고 푸시할 수 있는 bypass 를 이 App 하나로 둔다.
- App 의 개인 키(PEM)는 한 줄 base64 로 새 런타임 비밀 그룹 `console-github-app`(root 0600)에 두고, foundation-api 에만 넘긴다. 저장소에는 없다.
- 화면의 한 줄은 이렇게 이어진다: 제안됨 → draft PR #N → 검사 초록 → (GitHub 에서) ready → 병합 → 배포됨(커밋) → 다음 run 이 새 한도로 돈다.

## 하지 않는 것

- 읽기 접근(조회·Trino 질의)의 감사, 로그인 감사. 변경만 다룬다.
- 고객 제품(공짱)의 감사 — 공짱은 자기 `audit_log` 가 있다(그 crate `audit-log-domain`).
- 직무 분리(요청자 ≠ 승인자)의 강제. 직원이 한 명인 동안은 GitHub 의 ready 를 누른 계정이 기록될 뿐이다. 둘이 되면 저장소 규칙의 리뷰 수로 켠다.
- 전문 검색 색인 같은 로그 분석. 하루 100줄 규모라 Postgres 질의로 충분하다.

## 버린 대안

| 대안 | 기각 이유 |
|---|---|
| **더니어가 호스트·systemd 에 직접 쓴다**(한도는 drop-in, 원천은 파일) | 검토(CI·호스트 예산 가드)를 건너뛴다. 코드와 호스트가 갈라져 다음 배포가 되돌린다. API 에 root 쓰기 길이 생긴다 — ADR-0433 이 버린 "API 에 sudo 경로"와 같다 |
| **기록은 GitHub 에만**(PR 이 곧 감사) | 실행 동작·호스트 명령·배포·지도 편집이 GitHub 에 없다. 조직 감사 로그 API 는 Free 에서 404(실측)이고, 화면의 기록은 180일이다. 신원이 Zitadel 직원이 아니라 GitHub 계정이다 |
| **원천마다 자기 기록, 더니어가 매번 모아 보인다** | 원천마다 모양·보관이 달라 계정별 질의가 안 된다. 원천이 지운 것은 사라진다(접힌 D1 행). 변조 증거가 없다 |
| **새 감사 서비스·로그 저장소**(OpenSearch·Loki·DataHub 감사) | 이 결정이 새 서비스를 금한다. 하루 100줄 규모는 Postgres 로 충분하다 |
| **트리거만으로 append-only**(ADR-0282 그대로) | 표 주인·DB 관리자가 트리거를 끄고 고치면 흔적이 없다 |
| **digest 를 R2 bucket lock 에** | lock 은 R2 편집 권한으로 지울 수 있고, 호스트가 그 권한을 가졌다. 백업 버킷은 pgBackRest 때문에 lock 을 금한다(`postgres_recovery.rs` 시험). 그 버킷의 쓰기 열쇠는 DB 관리자 비밀번호와 한 그룹이다 |
| **Iceberg 에 감사 표** | 동작과 같은 트랜잭션에 쓸 수 없다(동작은 Postgres 에 있다). 화면의 짧은 질의가 Trino 를 타게 된다 |
| **더니어 서버 자신의 DB** | ADR-0114 §3: 더니어는 도메인 데이터를 갖지 않는다 |
| **주체 열쇠를 GitHub 로그인·메일로** | 로그인은 바뀌고 다시 쓰인다. 메일은 개인 정보이고, 지울 수 없는 표에 들어간다. 직원 계정(`identity.staff.id`·Zitadel `sub`)이 사장이 말한 "계정"이다 |
| **기계 사용자 계정·개인 토큰으로 PR** | 사람 한 명에 묶인 장기 토큰이고, 권한이 그 사람 전체다. App 은 저장소 하나·권한 몇 개로 좁고, 토큰이 한 시간짜리다 |
| **App 이 승인·대기열 넣기까지** | API 가 털리면 자동 배포(ADR-0159)를 거쳐 호스트 코드가 된다. 사람의 ready 를 남긴다 |

## Consequences

- 단계(작은 PR, 앞에 기대는 것은 draft). 마이그레이션이 API 보다 먼저 배포된다.

  | # | PR | 파일(주요) | 시험 |
  |---|---|---|---|
  | 1 | 이 ADR | — | — |
  | 2 | 저장소 | 마이그레이션(부록 A), 기동 SQL 셋, 어휘 계약 `config/audit-events.contract.json`(동작·원천·역할별 허용 원천·검증 원천; SQL CHECK·Rust enum 과 대조하는 계약 시험), `operator_action` 트리거, `DELETE`·`TRUNCATE` 회수 | 권한 거부, 동시 append 에 빈칸·같은 `prev_hash` 없음, 되돌린 트랜잭션은 줄 없음, 고정 해시 벡터, 원장 줄 = 감사 줄 |
  | 3 | 읽기 API + 더니어 `#/audit` | `data_catalog` 경로, OpenAPI, `relay.rs`, 화면 | 경로 권한, seq 페이지, 내보내기 끝의 digest |
  | 4 | 가져오기 1: `deploy`·`host_command` | 작업 목록의 `audit_import`, unit, 예산 | 다시 돌리면 0줄, 위조 태그(`_UID≠0`) 거름, 지워진 cursor → `import_gap` |
  | 5 | digest + Rekor 증인 + 검증 | 작업 `audit_digest`, `audit-witness` 그룹, `config/audit-witness.pub` | 줄 하나 고친 사본에서 검증 실패 |
  | 6 | 신원: `staff_external_account`·feed·이벤트 둘 | identity 마이그레이션·API | 유효 구간 풀이 |
  | 7 | App 이 생긴 뒤: `github`·`merge_queue`·`repo_settings` | 가져오기, 가지 규칙(ADR-0423) | 1,267개 첫 가져오기 26질의, `change.diverged` |
  | 8 | 「변경 제안」: 자원 한도부터 | `changes` 경로, `console-changes.contract.json`, 화면 | 같은 값 거부, 한 파일만 바뀐 PR |

  되돌리기: 2–5 는 작업을 끄면 멈추고 표는 남는다(지우지 않는다). 8 은 계약 목록을 비우면 제안이 닫힌다.
- 비용: 돈 0(Rekor 공개 인스턴스, 하루 24 항목). 호스트에서는 15분마다 가져오기 한 번(상한 256 MiB), 매시 digest 한 번이다. W87 의 예산 가드가 상한 합을 판정한다.
- 남는 일:
  - 직원이 둘이 되면 리뷰 수 1 이상으로 직무 분리를 켠다.
  - 로그인을 접근 감사로 가져올지 따로 정한다.
  - GitHub 연결의 소유 증명(App 의 사용자 OAuth)은 직원이 늘 때 한다.

## 소유자가 한 번 하는 일

PowerShell 기준이고, 명령에 `$` 가 없다. 1–6단계는 이 일이 없어도 된다.

1. GitHub → 조직 `perfectory-inc` → Settings → Developer settings → **GitHub Apps → New GitHub App**.
   - 이름 `perfectory-console`, Webhook 끔.
   - Repository permissions: **Contents: Read and write**, **Pull requests: Read and write**, **Checks: Read**, **Administration: Read**(ruleset 기록), Metadata: Read.
   - 권한 이름은 첫 연기 시험(PR 하나 열기·ruleset 기록 읽기)으로 확인하고, 모자라면 그 PR 이 적는다.
2. **Install App** → Only select repositories → `perfectory-public`.
3. **Generate a private key**(PEM 이 내려받아진다). App id 와 설치 id 를 적는다.
4. 서버에 넣는 길은 기존 런타임 비밀 경로 하나다. 값은 채팅·셸 기록에 남지 않는다:
   `ssh -t ai-server "umask 077; cat > /tmp/gh-app.env; sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set console-github-app /tmp/gh-app.env; rm -f /tmp/gh-app.env"`
   - 붙여 넣을 세 줄: `FOUNDATION_CONSOLE_GITHUB_APP_ID=<id>`, `FOUNDATION_CONSOLE_GITHUB_APP_INSTALLATION_ID=<id>`,
     `FOUNDATION_CONSOLE_GITHUB_APP_PRIVATE_KEY_B64=<PEM 파일을 base64 한 줄로>` → Enter → Ctrl+D.
   - base64 한 줄은 PowerShell 에서 `[Convert]::ToBase64String([IO.File]::ReadAllBytes("내려받은.pem"))` 로 만든다.
5. 사장 본인의 GitHub 계정과 ai-server 유닉스 계정을 직원 계정에 잇는 것은 에이전트가 신원 API 로 한다. 공개 저장소에는 적지 않는다.

## 부록 A. DDL 초안

```sql
CREATE SCHEMA audit;
CREATE TABLE audit.event (
    seq               bigint      PRIMARY KEY CHECK (seq >= 1),
    event_id          uuid        NOT NULL UNIQUE,
    occurred_at       timestamptz NOT NULL,
    recorded_at       timestamptz NOT NULL DEFAULT now(),
    source            text        NOT NULL,   -- CHECK 는 어휘 계약에서 생성
    source_event_id   text        NOT NULL CHECK (length(source_event_id) BETWEEN 1 AND 200),
    action            text        NOT NULL CHECK (action ~ '^[a-z_]+\.[a-z_]+$'),
    operation         text        NOT NULL CHECK (operation IN ('create','modify','remove','execute','access')),
    actor_kind        text        NOT NULL CHECK (actor_kind IN ('staff','service','system','unlinked')),
    actor_staff_id    uuid,
    actor_subject     text,
    actor_external    text        CHECK (actor_external ~ '^(github:[0-9]+|unix:[0-9]+@[a-z0-9-]+|github-app:[0-9]+)$'),
    actor_verified    boolean     NOT NULL,
    on_behalf_of      text,
    object_type       text        NOT NULL,
    object_id         text        NOT NULL,
    before            jsonb       CHECK (before IS NULL OR jsonb_typeof(before) = 'object'),
    after             jsonb       CHECK (after  IS NULL OR jsonb_typeof(after)  = 'object'),
    reason            text        CHECK (reason IS NULL OR (length(btrim(reason)) BETWEEN 1 AND 300 AND reason !~ '[[:cntrl:]]')),
    applied_via_kind  text        CHECK (applied_via_kind IN ('pull_request','approval','host_command','deploy','api')),
    applied_via_ref   text,
    result            text        NOT NULL CHECK (result IN ('requested','succeeded','failed','refused','expired')),
    error             text,
    change_id         uuid,
    prev_hash         bytea       NOT NULL CHECK (length(prev_hash) = 32),
    row_hash          bytea       NOT NULL UNIQUE CHECK (length(row_hash) = 32),
    UNIQUE (source, source_event_id),
    CHECK ((actor_kind = 'staff') = (actor_staff_id IS NOT NULL))
);
CREATE INDEX event_actor_idx  ON audit.event (actor_staff_id, seq DESC);
CREATE INDEX event_object_idx ON audit.event (object_type, object_id, seq DESC);
CREATE INDEX event_change_idx ON audit.event (change_id) WHERE change_id IS NOT NULL;
CREATE INDEX event_time_idx   ON audit.event (occurred_at);

CREATE TABLE audit.digest (
    from_seq bigint NOT NULL, to_seq bigint NOT NULL, count bigint NOT NULL,
    head_hash bytea NOT NULL, previous_digest_sha256 bytea, digest_sha256 bytea NOT NULL UNIQUE,
    rekor_log_index bigint, rekor_inclusion_proof jsonb, made_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (to_seq), CHECK (from_seq <= to_seq)
);
CREATE TABLE audit.import_cursor (            -- 원천마다 줄을 덧붙인다; 지금 값은 원천의 마지막 줄
    source text NOT NULL, cursor text NOT NULL,
    state text NOT NULL CHECK (state IN ('running','waiting_for_credential','gap')),
    advanced_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (source, advanced_at)
);
-- audit.append(...), audit.advance_cursor(source, cursor, state), audit.canon(row) 는 SECURITY DEFINER,
-- SET search_path = audit, pg_temp. 세 표 모두 BEFORE UPDATE OR DELETE / BEFORE TRUNCATE 트리거가 거부한다.
```

## 출처

[CloudTrail record contents](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html),
[userIdentity](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-user-identity.html),
[log file integrity validation](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-validation-intro.html),
[Event history 90일](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/view-cloudtrail-events.html),
[Cloud Audit Logs](https://cloud.google.com/logging/docs/audit), [AuditLog](https://cloud.google.com/logging/docs/audit/api/ref/rest/Shared.Types/AuditLog),
[Logging quotas(`_Required` 400일)](https://cloud.google.com/logging/quotas),
[GitHub 조직 감사 로그](https://docs.github.com/en/organizations/keeping-your-organization-secure/managing-security-settings-for-your-organization/reviewing-the-audit-log-for-your-organization),
[Stripe Event object](https://docs.stripe.com/api/events/object), AICPA Trust Services Criteria(2017) CC8.1,
[Sigstore Rekor](https://docs.sigstore.dev/logging/overview/), [R2 bucket locks](https://developers.cloudflare.com/r2/buckets/bucket-locks/),
[GitHub App permissions](https://docs.github.com/en/rest/authentication/permissions-required-for-github-apps).
