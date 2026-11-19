# ADR 0584: 감사 기록은 줄마다 사슬로 잇지 않고, 매시 digest 가 순서대로 놓인 줄들을 해시해 증인에 올린다

- Status: Accepted (2026-11-19, 소유자 승인 — brief 「Reinvention and tool-underuse inventory」 §5.3 결정 6)
- Date: 2026-11-19
- Supersedes (일부): [ADR-0569](./0569-every-change-to-the-platform-is-one-hash-chained-audit-event-keyed-by-the-staff-account-and-dawneer-shows-it-and-proposes-config-changes-as-pull-requests.md)
  §3 의 쓰는 이 잠금(`pg_advisory_xact_lock`)·READ COMMITTED 조건·`lock_timeout` 503 길, §4 의 줄마다 사슬(`prev_hash`·`row_hash`)과 digest 의 모양,
  §7 의 `seq` 커서(내보내기의 `row_hash` 포함).
  **그대로**: §1·§2(사건의 모양, 주체의 열쇠는 직원 계정), §3 의 문 하나(`audit.append` 가 호출자의 원천을 맞춘다)·권한·트리거·원장 트리거,
  §4 의 증인(Rekor, cosign, `audit-witness` 그룹, 매시·매주 검증), §5·§6, §7 의 화면·거르기·권한, §8.

## Context

ADR-0569 는 감사 사건마다 `prev_hash`(앞 줄의 `row_hash`)를 두고 `row_hash = sha256(prev_hash ‖ canon(row))` 로 줄을 이었다. 그 사슬이 빈칸 없이 이어지려면
쓰는 이가 한 번에 하나여야 해서, 상수 키의 transaction advisory lock 을 잡고, REPEATABLE READ 를 거부하고(스냅샷이 잠금보다 먼저 잡혀 두 줄이 같은
`prev_hash` 를 받는다), API 는 `lock_timeout = 2s` 를 넘으면 동작을 503 으로 끝냈다.

같은 ADR 의 위협 모형이 이미 답을 적어 두었다(§「위협 모형」):

| 누가 고치려 하나 | 무엇이 잡나 |
|---|---|
| 앱 역할 | 권한 — 표에 쓰기 권한이 없고 `audit.append` 만 부른다 |
| 표 주인·DB 관리자 | **Rekor 에 올라간 digest** — 트리거를 끄고 줄을 고친 뒤 **사슬을 다시 계산해도** digest 와 다르다 |
| 호스트 root | 마지막 증인 이후 1시간 안의 줄만 — 사슬도 같다 |

줄마다 사슬은 어느 행에서도 증거가 되지 못한다: 줄을 고칠 수 있는 자는 사슬도 다시 계산할 수 있고, 그것을 드러내는 것은 바깥 증인뿐이다.
사슬이 산 것은 쓰는 이 하나(잠금), 격리 수준 하나, 그리고 잠금을 기다리다 거부되는 운영자 동작(503)이다. 하루 100줄이 안 되는 기록이다
(2026-11-19 운영 `audit.event` 0줄, 운영자 원장 0줄, 호스트 명령 저널 183줄 누적).

업계도 같다. CloudTrail 은 줄을 사슬로 잇지 않는다: 로그 파일마다 SHA-256 을 두고, 매시 digest 파일이 그 시간의 파일 해시들과 앞 digest 의 서명을 담는다.
사슬은 digest 사이에만 있다(AWS CloudTrail User Guide, 「CloudTrail log file integrity validation」).

잠금이 없으면 `seq` 는 커밋 때가 아니라 넣을 때 받는다. 그래서 "어디까지 덮었나"를 번호로 말할 수 없다: 103 을 받은 트랜잭션이 105 보다 늦게 커밋되면,
105 까지 읽은 독자(그리고 105 까지 덮은 digest)는 103 을 영영 놓친다. 트랜잭션 id 의 하한(`pg_snapshot_xmin`)으로 자르면 빈칸은 없지만, 그 하한은
클러스터 전체에서 가장 오래 열린 트랜잭션이라 긴 적재 하나가 그동안의 모든 감사 줄을 증인 밖에 둔다.

## Decision

### 1. 줄은 해시를 갖지 않는다. 쓰는 데 잠금이 없다

- `audit.event` 에서 `prev_hash`·`row_hash` 를 지운다. `seq` 는 identity 다: 줄의 순서(해시 안의 순서, 화면의 정렬)일 뿐 커서가 아니다. 되돌린 트랜잭션이나
  이미 있는 `(source, source_event_id)` 는 번호를 쓰고 빈칸을 남긴다. digest 는 번호를 세지 않고 줄을 해시하므로 빈칸은 증거와 상관없다.
- 같은 사건을 두 번 가져오면 한 줄이다: `INSERT … ON CONFLICT (source, source_event_id) DO NOTHING`. 고쳐 쓰기(`DO UPDATE`)는 없다.
  같은 열쇠에 다른 내용이 오면 **먼저 온 것이 남고** 뒤의 것은 조용히 버려진다(전과 같다; 원천의 사건 id 는 내용마다 하나라는 가정).
- `audit.append` 는 호출자의 원천을 맞춘 뒤 줄 하나를 넣는다. 잠금도 격리 수준 조건도 없다. 그래서 API 에 잠금 시한과 503 길이 없다.
- 줄마다 `txid xid8 = pg_current_xact_id()`(쓴 트랜잭션의 id)를 둔다. savepoint 안에서도 바깥 트랜잭션의 id 다.

### 2. digest 는 그것이 만들어진 스냅샷이다

- digest 하나는 `to_snapshot`(만든 때의 `pg_current_snapshot()`)과 `from_snapshot`(앞 digest 의 `to_snapshot`, 첫 digest 는 없음)을 갖고,
  **`to_snapshot` 이 보는 트랜잭션이 쓴 줄 가운데 `from_snapshot` 이 보지 않은 줄**을 덮는다(`audit.rows_between`, `pg_visible_in_snapshot`).
- 스냅샷이 보는 트랜잭션의 집합은 한번 잡히면 바뀌지 않는다(PostgreSQL 17 「Transaction ID and Snapshot Information Functions」). 그래서
  증인에 올라간 범위에 줄이 나중에 들어올 수 없고, 스냅샷은 시간이 갈수록 더 많이 보므로 범위들은 겹치지도 비지도 않는다.
  긴 트랜잭션은 **자기 줄만** 붙잡는다: 그것이 열려 있는 동안 커밋된 다른 줄은 이번 digest 에, 그것의 줄은 커밋된 뒤의 digest 에 들어간다. 잠금이 필요 없다.
- 범위의 해시는 `audit.range_sha256` = `audit.rows_sha256`: 그 줄들의 `audit.canon`(모든 열, `seq`·`txid` 포함, jsonb 한 객체의 글자)을 `seq` 순서로
  한 줄씩 이은 것의 SHA-256 이다. 줄 하나를 고치거나, 지우거나, 끼워 넣거나, 순서를 바꾸면 그 범위의 해시가 바뀐다.
- `audit.digest` = `{digest_no, from_snapshot, to_snapshot, row_count, rows_sha256, previous_digest_sha256, digest_sha256, made_at}`, 덧붙이기만 한다.
  `digest_sha256` 은 `audit.digest_sha256` 이 정한 바이트 — `{schema: "audit.digest.v1", digest_no, from_snapshot, to_snapshot, row_count, rows_sha256,
  previous_digest_sha256}` 한 jsonb 객체의 글자(스냅샷은 글자꼴, 해시는 소문자 hex, 첫 digest 의 빈 값은 null)의 SHA-256 이다. `made_at` 과 증인은 밖이다.
- 트리거 `audit.digest_follows` 가 넣을 때마다 본다: 번호가 앞 것 + 1, `from_snapshot` 이 앞 것의 `to_snapshot`, `previous_digest_sha256` 이 앞 것의
  `digest_sha256`, `to_snapshot` 이 보는 트랜잭션이 모두 끝났고(지어낸 미래 스냅샷 거부) 앞 것보다 뒤로 가지 않으며, `row_count`·`rows_sha256`·`digest_sha256`
  이 실제 줄과 맞는다. 사슬은 digest 사이에만 있고, DB 가 그것을 지킨다.
- 증인은 따로 `audit.digest_witness`(`digest_no`, `rekor_log_index`, `rekor_inclusion_proof`, 덧붙이기만)에 둔다: digest 를 먼저 저장하고 올린다. 올리기가
  실패해 다시 하면 같은 digest 를 다시 올린다(Rekor 는 같은 항목을 409 와 있는 항목의 위치로 답한다) — 한 범위에 digest 가 둘 생기지 않는다.
- 서명·증인·검증은 ADR-0569 §4 그대로다: `digest_sha256` 을 cosign `sign-blob` 으로 서명해 공개 Rekor 에 올린다. 매시 검증은 새 digest 만, 매주 일요일에는
  모든 digest 를 처음부터 다시 계산해 Rekor 항목과 대조한다.

### 3. 읽는 쪽의 커서도 스냅샷이다 (ADR-0569 §7 의 `seq` 커서를 바꾼다)

- 따라 읽는 쪽(감사 피드, 내보내기)의 커서는 `pg_snapshot` 의 글자다: "지난번 스냅샷 이후로 보이게 된 줄" = `audit.rows_between(cursor, pg_current_snapshot())`,
  응답과 함께 새 커서(`pg_current_snapshot()`)를 돌려준다. digest 와 같은 규칙이라 늦게 커밋된 줄도 빠지지 않는다.
- 화면의 목록은 `seq` 로 정렬해 보여 줄 뿐 "여기까지 다 봤다"를 약속하지 않는다. 내보내기의 줄에는 `row_hash` 대신 `seq`·`txid` 가 있고,
  끝에 그 줄들을 덮는 digest 들과 Rekor `logIndex` 가 붙는다.

### 4. 지우는 것과 시험

- 마이그레이션 `20261119054701_the_audit_chain_is_witnessed_by_its_digest_not_chained_row_by_row.sql`: 두 열과 `audit.chain`·`audit.row_hash` 를 지우고,
  내부 단계를 `audit.record` 로, `audit.digest` 를 새 모양으로(옛 모양의 digest 가 한 줄이라도 있으면 거부한다 — 운영은 0줄), `audit.digest_witness`·
  `audit.rows_between`·`audit.range_sha256`·`audit.digest_sha256`·`audit.digest_follows` 를 만든다.
- `catalog-infrastructure/tests/audit_event_record.rs`(옛 `audit_event_chain.rs`): 고정 벡터(줄 하나의 canon 해시, 두 줄 해시, 빈 범위),
  긴 트랜잭션은 자기 줄만 붙잡고 만든 범위는 바뀌지 않음, savepoint 의 줄은 바깥 트랜잭션과 함께 덮임, 범위 해시 = 그 줄들의 해시,
  digest 가 앞 것을 따르고 줄과 맞음(번호 건너뛰기·틀린 개수·틀린 자기 해시·미래 스냅샷·앞 것 연결 없음 거부), 열린 쓰는 이가 다른 쓰는 이를 막지 않음,
  동시 80줄이 모두 한 번씩, REPEATABLE READ 에서도 쓴다, 고쳐 쓰기·지우기·남의 원천 거부.
- `compose-smoke.sh` 는 `audit.record` 가 있고 어떤 앱 역할도 그것을 부를 수 없음을 본다.

## Consequences

- 운영자 동작이 감사 잠금 때문에 503 으로 끝나는 일이 없다. 가져오기의 50줄 트랜잭션이 API 를 기다리게 하지 않는다.
- 변조 증거는 줄마다 대신 범위마다다. 위협 모형의 세 줄이 그대로다: 호스트 root 가 고칠 수 있는 것은 마지막 증인 이후의 줄뿐이고, 그 창은 한 시간이다 —
  다만 그때 **아직 커밋되지 않은** 트랜잭션의 줄은 커밋된 뒤의 첫 digest 까지 증인이 없다(긴 트랜잭션 안의 감사 줄만 해당).
- 한 줄만 따로 떼어 "이 줄이 사슬에 있었다"를 보일 수는 없다. 그 범위의 모든 줄(내보내기)이 있어야 한다. 하루 100줄 미만이라 범위는 작다.
- 트랜잭션 id 는 한 클러스터 안에서만 순서가 있다. 덤프를 다른 클러스터에 복원하면 id 가 처음부터 다시 시작해 다음 digest 의 스냅샷이 앞 것보다 뒤로 가므로
  트리거가 거부하고 digest 작업이 실패해 경보가 난다(조용히 덮지 않고 시끄럽게 멈춘다). 이전(예: AWS 이전)은 옮기기 전 마지막 digest 를 증인에 올리고
  새 클러스터에서 사슬을 어떻게 잇는지 정하는 별도 결정이 필요하다. 시점 복원(PITR)은 같은 id 를 다시 쓰므로 Rekor 에 같은 `digest_no` 가 둘 보인다 —
  검증자는 그것을 되돌림의 증거로 보고하며, 어느 것도 조용히 고르지 않는다.
- 이 PR 뒤에 다시 쓰는 것: 읽기 API(#1448)는 `prev_hash`·`row_hash` 를 내지 않고, 피드·내보내기 커서를 §3 의 스냅샷으로 한다. digest 작업(#1451)은
  `pg_current_snapshot()` 으로 범위를 잡고 digest 를 넣은 뒤 올리고 증인을 넣는다. 가져오기(#1449)는 바뀌는 것이 없다(`audit.append` 의 인자가 같다).

## 출처

- ADR-0569 §「위협 모형」, §3, §4, §7(이 저장소).
- AWS CloudTrail User Guide, 「Validating CloudTrail log file integrity」·「CloudTrail digest file structure」: 시간마다 digest, 그 안에 로그 파일 해시와 앞 digest 서명.
- PostgreSQL 17 문서 「System Information Functions — Transaction ID and Snapshot Information Functions」: `pg_current_xact_id()`, `pg_current_snapshot()`,
  `pg_visible_in_snapshot()`, `pg_snapshot_xmin()`·`pg_snapshot_xmax()`·`pg_snapshot_xip()`.
- PostgreSQL 17 문서 「INSERT — ON CONFLICT Clause」: `DO NOTHING` 은 UPDATE 트리거를 부르지 않는다.
- Sigstore Rekor API `createLogEntry`: 같은 항목이 이미 있으면 409 와 그 항목의 위치(중복 항목을 만들지 않음).
- 검증: 2026-11-19 노트북의 일회용 `postgres:17-alpine` 에서 이 마이그레이션의 digest 부분을 그대로 올려, 긴 트랜잭션·만든 범위 불변·다섯 결함 거부·빈 범위·
  savepoint 를 돌려 봤다(컨테이너는 지웠다). 저장소의 시험은 CI 의 Postgres 에서 돈다.
