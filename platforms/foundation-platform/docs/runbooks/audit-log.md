---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-19
---

# 감사 기록 (`audit.event`, `foundation-audit-import.service`)

결정: 루트 ADR-0569. 플랫폼의 모든 변경을 직원 계정을 열쇠로 한 해시 사슬 한 표에 남기고, 더니어 `#/audit` 가 보인다.

## 무엇이 도는가

| 조각 | 위치 |
|---|---|
| 표 | Foundation Postgres `audit.event`(사슬), `audit.digest`(증인), `audit.import_cursor`(원천 커서), `audit.source`(원천마다 쓰는 역할). 모두 덧붙이기만 한다 |
| 쓰는 문 | `audit.append`(원천은 `session_user` 로 제한)·`audit.advance_cursor`. 앱 역할에는 표의 INSERT·UPDATE·DELETE·TRUNCATE 가 없다 |
| 더니어 동작 | `catalog.operator_action` 의 트리거가 같은 트랜잭션에 요청·답을, API 가 커밋 뒤 스위치 결과를 쓴다 |
| 가져오기 | `foundation-outbox-publisher import-audit-events`, 작업 `audit_import`(`definitions/audit.definition.json`), 15분마다 |
| 어휘 | `config/audit-events.contract.json` — 원천·동작·호스트 명령 허용 목록 |
| 화면·API | 더니어 `#/audit`, `GET /data-catalog/v1/audit/events`·`/export`·`/import-status`(권한 `foundation.operations:audit_read`) |

## 가져오기가 읽는 것

| 원천 | 읽는 것 | 줄 하나 |
|---|---|---|
| `deploy` | `/var/lib/perfectory/autodeploy/{deployed,failed,refused}/` 전부(매번, 약 500개) | 상태 파일 하나, 그리고 그 배포가 담은 PR 마다 하나(`control-source.git` 의 first-parent 범위, 제목 끝의 `(#N)`) |
| `host_command` | journal `SYSLOG_IDENTIFIER=foundation-operator-start _UID=0`, 지난 커서 뒤 | 시작 요청 하나. 동작·대상·선언된 인자만 남기고 이유는 journal 에만 둔다 |
| `github`·`merge_queue`·`repo_settings` | GitHub App 이 생긴 뒤(ADR-0569 §8) | 지금은 `waiting_for_credential` |

가져오기는 호스트의 DB 자격(`recovery.env`)으로 연결한 뒤 `SET SESSION AUTHORIZATION foundation_audit_importer` 로 쓴다.
그 역할은 로그인하지 못하고, `audit.append` 가 그 역할에 허락한 원천만 받는다.

## 매시 digest 와 Rekor 증인

`foundation-audit-digest.service`(작업 `audit_digest`, 매시 5분)는 세 걸음이다.

| 걸음 | 누가 | 하는 일 |
|---|---|---|
| `audit-digest.sh prepare` | 서비스 사용자 | `audit.next_digest()`: 지난 digest 뒤의 줄이 그 head 에서 이어지는지 확인하고 digest 글을 `$RUNTIME_DIRECTORY/digest.txt` 에 쓴다 |
| `audit-witness.sh`(컨트롤 checkout) | root | 증인 키(`/var/lib/perfectory/audit-witness/cosign.key`, 0600, 처음 한 번 만든다)와 고정 판 cosign 으로 서명해 공개 Rekor 에 올리고 번들을 받는다 |
| `audit-digest.sh record` | 서비스 사용자 | `audit.record_digest(...)`: digest 를 다시 계산해 같을 때만 번들과 함께 `audit.digest` 에 남긴다 |

월요일 00:05 KST 에는 `audit.verify_all()` 로 seq 1 부터 전부, 그리고 모든 번들을 Rekor 와 대조한다.
digest 없이 두 시간 넘은 줄이 있으면 가져오기 작업이 실패해 알린다(증인이 멈췄다는 뜻).

누구나 하는 대조: 공개 키(`/var/lib/perfectory/audit-witness/cosign.pub`, 저장소의 `config/audit-witness.pub`)로
`cosign verify-blob --key cosign.pub --bundle <번들> <digest 글>`. digest 글은 `audit.digest_text(from_seq, to_seq, count, head_hash, previous_digest_sha256)` 다.

## 확인

```bash
# 원천마다 최신 커서(읽기만)
docker exec -i foundation-platform-runtime-postgres-1 sh -c 'psql -X -At -U "$POSTGRES_USER" -d foundation' <<'SQL'
SELECT DISTINCT ON (source) source, state, advanced_at FROM audit.import_cursor ORDER BY source, advanced_at DESC;
SELECT source, count(*), max(occurred_at) FROM audit.event GROUP BY source ORDER BY source;
SQL
journalctl -u foundation-audit-import.service --since -1h --no-pager | tail
```

## 실패하면

| 증상 | 뜻 | 할 일 |
|---|---|---|
| `the host command journal has a gap` | 지난 커서가 journal 에 없다(회전·재부팅). 그 사이 줄은 사슬에 없다 | 사슬에 `audit.import_gap` 이 남았다. 다음 실행은 journal 처음부터 다시 읽고, 이미 있는 줄은 건너뛴다. 사라진 기간을 사후 기록에 적는다 |
| `role ... may not write audit source ...` | 가져오기가 남의 원천을 쓰려 했다 | 코드 결함이다. 원천 표(`audit.source`)·계약을 고치지 말고 코드를 고친다 |
| `the audit chain is broken in seq ...` | 그 범위의 줄이 사슬을 이루지 않는다(누가 트리거를 끄고 고쳤다) | 증인은 그 범위를 올리지 않았다. 고치지 말고 사고로 다룬다: 마지막 증인 digest 와 Rekor 항목이 원래 값이다 |
| `does not verify against Rekor` | 남긴 digest 와 Rekor 항목이 다르다 | 사고로 다룬다. 사슬과 digest 를 고치지 않는다 |
| `rows have waited more than two hours for a witnessed digest` | 매시 증인이 멈췄다 | `journalctl -u foundation-audit-digest.service` 로 이유를 본다(Rekor 연결, cosign 받기) |
| `lock timeout` | 다른 쓰기가 사슬 잠금을 오래 잡았다 | 다음 실행이 이어 간다. 계속되면 오래 도는 트랜잭션을 찾는다 |

줄은 고치거나 지우지 않는다. 잘못 들어간 줄은 그 줄을 바로잡는 새 사건으로 남긴다.
