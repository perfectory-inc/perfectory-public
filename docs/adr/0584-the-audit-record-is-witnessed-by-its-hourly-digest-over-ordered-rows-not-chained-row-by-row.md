# ADR 0584: 감사 기록은 줄마다 사슬로 잇지 않고, 매시 digest 가 순서대로 놓인 줄들을 해시해 증인에 올린다

- Status: Accepted (2026-11-19, 소유자 승인 — brief 「Reinvention and tool-underuse inventory」 §5.3 결정 6)
- Date: 2026-11-19
- Supersedes (일부): [ADR-0569](./0569-every-change-to-the-platform-is-one-hash-chained-audit-event-keyed-by-the-staff-account-and-dawneer-shows-it-and-proposes-config-changes-as-pull-requests.md)
  §3 의 쓰는 이 잠금(`pg_advisory_xact_lock`)·READ COMMITTED 조건·`lock_timeout` 503 길, §4 의 줄마다 사슬(`prev_hash`·`row_hash`)과 digest 의 모양.
  **그대로**: §1·§2(사건의 모양, 주체의 열쇠는 직원 계정), §3 의 문 하나(`audit.append` 가 호출자의 원천을 맞춘다)·권한·트리거·원장 트리거,
  §4 의 증인(Rekor, cosign, `audit-witness` 그룹, 매시·매주 검증), §5~§8.

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

## Decision

### 1. 줄은 해시를 갖지 않는다. 쓰는 데 잠금이 없다

- `audit.event` 에서 `prev_hash`·`row_hash` 를 지운다. `seq` 는 identity 다: 줄의 순서이고 화면의 쪽 나누기 열쇠다. 되돌린 트랜잭션이나
  이미 있는 `(source, source_event_id)` 는 번호를 쓰고 빈칸을 남긴다. digest 는 번호를 세지 않고 줄을 해시하므로 빈칸은 증거와 상관없다.
- 같은 사건을 두 번 가져오면 한 줄이다: `INSERT … ON CONFLICT (source, source_event_id) DO NOTHING`. 고쳐 쓰기(`DO UPDATE`)는 없다.
- `audit.append` 는 호출자의 원천을 맞춘 뒤 줄 하나를 넣는다. 잠금도 격리 수준 조건도 없다. 그래서 API 에 잠금 시한과 503 길이 없다.
- 줄마다 `txid xid8 = pg_current_xact_id()`(쓴 트랜잭션의 id)를 둔다.

### 2. digest 는 끝난 트랜잭션의 줄만 덮는다

- digest 하나는 `txid` 가 `[from_txid, to_txid)` 인 줄들이다. `to_txid` 는 digest 를 만드는 순간의 `pg_snapshot_xmin(pg_current_snapshot())` 이고,
  `from_txid` 는 앞 digest 의 `to_txid`(첫 digest 는 `0`)다.
- `pg_snapshot_xmin` 보다 작은 id 의 트랜잭션은 모두 끝났다(커밋 또는 되돌림, PostgreSQL 17 문서 「Transaction ID and Snapshot Information Functions」).
  그래서 증인에 올라간 범위에 줄이 나중에 들어올 수 없다. 늦게 커밋되는 긴 트랜잭션의 줄은 다음 digest 가 덮는다. 이 성질에 잠금이 필요 없다.
- 범위의 해시는 `audit.rows_sha256`: 그 줄들의 `audit.canon`(모든 열, `seq`·`txid` 포함, jsonb 한 객체의 글자)을 `seq` 순서로 한 줄씩 이은 것의 SHA-256 이다.
  줄 하나를 고치거나, 지우거나, 끼워 넣거나, 순서를 바꾸면 그 범위의 해시가 바뀐다.
- `audit.digest` 는 `{digest_no, from_txid, to_txid, row_count, rows_sha256, previous_digest_sha256, digest_sha256, rekor_log_index, rekor_inclusion_proof}` 이고
  덧붙이기만 한다. 첫 digest 만 `previous_digest_sha256` 이 없다. digest 끼리는 `previous_digest_sha256` 로 잇는다 — 사슬은 여기에만 있다.
- 증인·검증은 ADR-0569 §4 그대로다: digest 의 SHA-256 을 cosign `sign-blob` 으로 서명해 공개 Rekor 에 올린다. 매시 검증은 새 범위만, 매주 일요일에는 모든 digest 를
  처음부터 다시 계산해 Rekor 항목과 대조한다. 내보내기 끝의 digest 로 누구든 같은 범위를 다시 해시해 볼 수 있다.

### 3. 지우는 것과 시험

- 마이그레이션 `20261119054701_the_audit_chain_is_witnessed_by_its_digest_not_chained_row_by_row.sql`: 두 열과 `audit.chain`·`audit.row_hash` 를 지우고,
  내부 단계를 `audit.record` 로, `audit.digest` 를 새 모양으로 만든다(옛 모양의 digest 가 한 줄이라도 있으면 거부한다 — 운영은 0줄).
- `catalog-infrastructure/tests/audit_event_record.rs`(옛 `audit_event_chain.rs`): 고정 벡터(줄 하나의 canon 해시, 두 줄 범위 해시, 빈 범위),
  열린 쓰는 이가 다른 쓰는 이를 막지 않음, 열린 트랜잭션의 줄은 digest 경계 아래에 오지 않음, 동시 80줄이 모두 한 번씩, REPEATABLE READ 에서도 쓴다,
  고쳐 쓰기·지우기·남의 원천 거부.
- `compose-smoke.sh` 는 `audit.record` 가 있고 어떤 앱 역할도 그것을 부를 수 없음을 본다.

## Consequences

- 운영자 동작이 감사 잠금 때문에 503 으로 끝나는 일이 없다. 가져오기의 50줄 트랜잭션이 API 를 기다리게 하지 않는다.
- 변조 증거는 줄마다 대신 범위마다다. 증인 이후의 변조를 드러내는 힘은 같다(위협 모형의 세 줄이 그대로다). 마지막 증인 이후의 줄은 전에도 지금도 증인이 없다.
- 한 줄만 따로 떼어 "이 줄이 사슬에 있었다"를 보일 수는 없다. 그 범위의 모든 줄(내보내기)이 있어야 한다. 하루 100줄 미만이라 범위는 작다.
- 이 PR 뒤에 다시 쓰는 것: 읽기 API(#1448)는 `prev_hash`·`row_hash` 를 내지 않고, 내보내기 끝에 그 줄들을 덮는 digest 를 붙인다. digest 작업(#1451)은 §2 의 경계로
  범위를 잡는다. 가져오기(#1449)는 바뀌는 것이 없다(`audit.append` 의 인자가 같다).

## 출처

- ADR-0569 §「위협 모형」, §3, §4(이 저장소).
- AWS CloudTrail User Guide, 「Validating CloudTrail log file integrity」·「CloudTrail digest file structure」: 시간마다 digest, 그 안에 로그 파일 해시와 앞 digest 서명.
- PostgreSQL 17 문서 「System Information Functions — Transaction ID and Snapshot Information Functions」: `pg_current_xact_id()`, `pg_current_snapshot()`, `pg_snapshot_xmin()`.
- PostgreSQL 17 문서 「INSERT — ON CONFLICT Clause」: `DO NOTHING` 은 UPDATE 트리거를 부르지 않는다.
